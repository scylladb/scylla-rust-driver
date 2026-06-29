use crate::errors::{ClusterStateTokenError, ConnectionPoolError};
use crate::network::{Connection, ConnectivityChangeEvent, PoolConfig, VerifiedKeyspaceName};
use crate::observability::metrics::Metrics;
use crate::policies::host_filter::HostFilter;
use crate::routing::locator::ReplicaLocator;
use crate::routing::locator::tablets::{RawTablet, Tablet, TabletsInfo};
use crate::routing::partitioner::{PartitionerName, calculate_token_for_partition_key};
use crate::routing::{Shard, Token};
use crate::utils::safe_format::IteratorSafeFormatExt;

use crate::frame::response::result::TableSpec;
use crate::serialize::row::{RowSerializationContext, SerializeRow, SerializedValues};
use itertools::Itertools;
use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::sync::mpsc;
use tracing::{debug, warn};
use uuid::Uuid;

use super::metadata::{Keyspace, Metadata, Strategy};
use super::node::{Node, NodeRef};

/// Represents the state of the cluster, including known nodes, keyspaces, and replica locator.
///
/// It is immutable after creation, and is replaced atomically upon a metadata refresh.
/// Can be accessed through [Session::get_cluster_state()](crate::client::session::Session::get_cluster_state).
#[derive(Clone)]
pub struct ClusterState {
    /// All nodes known to be part of the cluster, accessible by their host ID.
    /// Often refered to as "topology metadata".
    pub(crate) known_nodes: HashMap<Uuid, Arc<Node>>, // Invariant: nonempty after Cluster::new()

    /// Contains the same set of nodes as `known_nodes`.
    ///
    /// Introduced to fix the bug that zero-token nodes were missing from
    /// `ClusterState::get_nodes_info()` slice, because the slice was borrowed
    /// from `ReplicaLocator`, which only contains nodes with some tokens assigned.
    // TODO: in 2.0, make `get_nodes_info()` return `Iterator` instead of a slice.
    // Then, remove this field.
    pub(crate) all_nodes: Vec<Arc<Node>>,

    /// All keyspaces in the cluster, accessible by their name.
    /// Often refered to as "schema metadata".
    pub(crate) keyspaces: HashMap<String, Keyspace>,

    /// The entity which provides a way to find the set of owning nodes (+shards, in case of ScyllaDB)
    /// for a given (token, replication strategy, table) tuple.
    /// It relies on both topology and schema metadata.
    pub(crate) locator: ReplicaLocator,

    /// The name of the cluster, as reported by the `cluster_name` column in `system.local`.
    pub(crate) cluster_name: Option<String>,
}

/// Enables printing [ClusterState] struct in a neat way, skipping the clutter involved by
/// [ClusterState::ring] being large and [Self::keyspaces] debug print being very verbose by default.
pub(crate) struct ClusterStateNeatDebug<'a>(pub(crate) &'a Arc<ClusterState>);
impl std::fmt::Debug for ClusterStateNeatDebug<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let cluster_state = &self.0;

        let ring_printer = {
            struct RingSizePrinter(usize);
            impl std::fmt::Debug for RingSizePrinter {
                fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                    write!(f, "<size={}>", self.0)
                }
            }
            RingSizePrinter(cluster_state.locator.ring().len())
        };

        f.debug_struct("ClusterState")
            .field("known_nodes", &cluster_state.known_nodes)
            .field("ring", &ring_printer)
            .field("keyspaces", &cluster_state.keyspaces.keys())
            .finish_non_exhaustive()
    }
}

impl ClusterState {
    pub(crate) async fn wait_until_all_pools_are_initialized(&self) {
        for node in self.locator.unique_nodes_in_global_ring().iter() {
            node.wait_until_pool_initialized().await;
        }
    }

    /// Triggers immediate pool refills for given nodes. This resets exponential
    /// backoff for those nodes, so they will be retried immediately instead of
    /// waiting for the next retry timeout.
    ///
    /// Suitable, among others, for nodes whose client routes were added or updated.
    pub(super) fn trigger_pool_refills_for_hosts(&self, host_ids: impl Iterator<Item = Uuid>) {
        for host_id in host_ids {
            if let Some(node) = self.known_nodes.get(&host_id) {
                debug!(
                    host_id = %host_id,
                    "Triggering immediate pool refill for relevant Node"
                );
                node.trigger_pool_refill();
            }
        }
    }

    /// Triggers an immediate pool refill for the node with the given broadcast
    /// address. Used when a `STATUS_CHANGE UP` event hints that a node is back
    /// and its pool (likely in exponential backoff) should retry immediately.
    pub(super) fn trigger_pool_refill_for_addr(&self, addr: SocketAddr) {
        for node in self.known_nodes.values() {
            if node.address.into_inner() == addr {
                debug!(
                    address = %addr,
                    host_id = %node.host_id,
                    "STATUS_CHANGE UP: triggering immediate pool refill"
                );
                node.trigger_pool_refill();
                return;
            }
        }
        debug!(
            address = %addr,
            "STATUS_CHANGE UP: no known node with this address"
        );
    }

    /// Creates new ClusterState using information about topology held in `metadata`.
    /// Uses provided `known_nodes` hashmap to recycle nodes if possible.
    #[allow(clippy::too_many_arguments)]
    // This allow(clippy::type_complexity) is here because I can't satisfy borrow checker while
    // having the closure type be a type alias.
    #[allow(clippy::type_complexity)]
    pub(crate) async fn new(
        metadata: Metadata,
        pool_config: &PoolConfig,
        known_nodes: &HashMap<Uuid, Arc<Node>>,
        // Takes old and new known_nodes maps as arguments.
        handle_topology_changes: &mut (
                 dyn FnMut(&HashMap<Uuid, Arc<Node>>, &HashMap<Uuid, Arc<Node>>) + Send + Sync
             ),
        used_keyspace: &Option<VerifiedKeyspaceName>,
        host_filter: Option<&dyn HostFilter>,
        connectivity_events_sender: &mpsc::UnboundedSender<ConnectivityChangeEvent>,
        mut tablets: TabletsInfo,
        old_keyspaces: &HashMap<String, Keyspace>,
        metrics: &Metrics,
    ) -> Self {
        // Create new updated known_nodes and ring
        let mut new_known_nodes: HashMap<Uuid, Arc<Node>> =
            HashMap::with_capacity(metadata.peers.len());
        let mut ring: Vec<(Token, Arc<Node>)> = Vec::new();

        for peer in metadata.peers {
            // Take existing Arc<Node> if possible, otherwise create new one.
            let peer_host_id = peer.host_id;
            let is_enabled = host_filter.is_none_or(|f| f.accept(&peer));
            let (peer_endpoint, peer_tokens) = peer.into_peer_endpoint_and_tokens();

            let node = match (is_enabled, known_nodes.get(&peer_host_id)) {
                // If the node is disabled, then we never want to have a connection pool to it.
                // If it already existed, and was disabled, and all parameters (dc, rack, address) match
                // then we can reuse the object.
                // Otherwise we need to create a new one.
                (false, Some(node))
                    if !node.is_enabled()
                        && node.datacenter == peer_endpoint.datacenter
                        && node.rack == peer_endpoint.rack
                        && node.address == peer_endpoint.address =>
                {
                    Arc::clone(node)
                }
                (false, _) => Arc::new(Node::new_disabled(peer_endpoint)),
                // Changing rack/datacenter but not ip address seems improbable
                // so we can just create new node and connections in such case.
                // Here we allow preserving the Node object in full if all attributes (dc, rack, address)
                // match. If only address is different, we recreate Node object but share the same underlying pool.
                //
                // IMPORTANT EDGE CASE: The old Node object might have been disabled. We know that the new Node object
                // must be enabled, so there is no point in using the old one then.
                (true, Some(node))
                    if node.is_enabled()
                        && node.datacenter == peer_endpoint.datacenter
                        && node.rack == peer_endpoint.rack =>
                {
                    if node.address == peer_endpoint.address {
                        Arc::clone(node)
                    } else {
                        // If IP changes, the Node struct is recreated, but the underlying pool is preserved and notified about the IP change.
                        Arc::new(Node::inherit_with_ip_changed(node, peer_endpoint))
                    }
                }
                (true, _) => Arc::new(Node::new(
                    peer_endpoint,
                    pool_config,
                    connectivity_events_sender.clone(),
                    used_keyspace.clone(),
                    metrics.clone(),
                )),
            };

            new_known_nodes.insert(peer_host_id, Arc::clone(&node));

            for token in peer_tokens {
                ring.push((token, Arc::clone(&node)));
            }
        }

        handle_topology_changes(known_nodes, &new_known_nodes);

        let keyspaces: HashMap<String, Keyspace> = metadata
            .keyspaces
            .into_iter()
            .filter_map(|(ks_name, ks)| match ks {
                Ok(ks) => Some((ks_name, ks)),
                Err(e) => {
                    if let Some(old_ks) = old_keyspaces.get(&ks_name) {
                        warn!(
                            "Encountered an error while processing\
                            metadata of keyspace \"{ks_name}\": {e}.\
                            Re-using older version of this keyspace metadata"
                        );
                        Some((ks_name, old_ks.clone()))
                    } else {
                        warn!(
                            "Encountered an error while processing metadata\
                            of keyspace \"{ks_name}\": {e}.\
                            No previous version of this keyspace metadata found, so it will not be\
                            present in ClusterState until next refresh."
                        );
                        None
                    }
                }
            })
            .collect();

        // Tablets maintenance.
        {
            let removed_nodes = {
                let mut removed_nodes = HashSet::new();
                for old_peer in known_nodes {
                    if !new_known_nodes.contains_key(old_peer.0) {
                        removed_nodes.insert(*old_peer.0);
                    }
                }

                removed_nodes
            };

            let recreated_nodes = {
                let mut recreated_nodes = HashMap::new();
                for (old_peer_id, old_peer_node) in known_nodes {
                    if let Some(new_peer_node) = new_known_nodes.get(old_peer_id)
                        && !Arc::ptr_eq(old_peer_node, new_peer_node)
                    {
                        recreated_nodes.insert(*old_peer_id, Arc::clone(new_peer_node));
                    }
                }

                recreated_nodes
            };

            tablets.perform_maintenance(
                &keyspaces,
                &removed_nodes,
                &new_known_nodes,
                &recreated_nodes,
            )
        }

        let (locator, keyspaces) = tokio::task::spawn_blocking(move || {
            let keyspace_strategies = keyspaces
                .values()
                .filter(|ks| !ks.tablet_based)
                .map(|ks| &ks.strategy);
            let locator = ReplicaLocator::new(ring.into_iter(), keyspace_strategies, tablets);
            (locator, keyspaces)
        })
        .await
        .unwrap();

        ClusterState {
            all_nodes: new_known_nodes.values().cloned().collect(),
            known_nodes: new_known_nodes,
            keyspaces,
            locator,
            cluster_name: metadata.cluster_name,
        }
    }

    /// Returns the name of the cluster, as reported by the `cluster_name` column in `system.local`.
    pub fn cluster_name(&self) -> &str {
        self.cluster_name.as_deref().unwrap_or("")
    }

    /// Access keyspace details collected by the driver.
    pub fn get_keyspace(&self, keyspace: impl AsRef<str>) -> Option<&Keyspace> {
        self.keyspaces.get(keyspace.as_ref())
    }

    /// Returns an iterator over keyspaces.
    pub fn keyspaces_iter(&self) -> impl Iterator<Item = (&str, &Keyspace)> {
        self.keyspaces.iter().map(|(k, v)| (k.as_str(), v))
    }

    /// Access details about nodes known to the driver
    pub fn get_nodes_info(&self) -> &[Arc<Node>] {
        &self.all_nodes
    }

    /// Access details about specific node known to the driver, querying by host id.
    pub fn get_node_by_host_id(&self, host_id: Uuid) -> Option<NodeRef<'_>> {
        self.known_nodes.get(&host_id)
    }

    /// Compute token of a table partition key
    ///
    /// `partition_key` argument contains the values of all partition key
    /// columns. You can use both unnamed values like a tuple (e.g. `(1, 5, 5)`)
    /// or named values (e.g. struct that derives `SerializeRow`), as you would
    /// when executing a request. No additional values are allowed besides values
    /// for primary key columns.
    pub fn compute_token(
        &self,
        keyspace: &str,
        table: &str,
        partition_key: &dyn SerializeRow,
    ) -> Result<Token, ClusterStateTokenError> {
        let Some(table) = self
            .keyspaces
            .get(keyspace)
            .and_then(|k| k.tables.get(table))
        else {
            return Err(ClusterStateTokenError::UnknownTable {
                keyspace: keyspace.to_owned(),
                table: table.to_owned(),
            });
        };
        let values = SerializedValues::from_serializable(
            &RowSerializationContext::from_specs(table.pk_column_specs.as_slice()),
            partition_key,
        )?;
        let partitioner = table
            .partitioner
            .as_deref()
            .and_then(PartitionerName::from_str)
            .unwrap_or_default();
        calculate_token_for_partition_key(&values, &partitioner)
            .map_err(ClusterStateTokenError::TokenCalculation)
    }

    /// Access to replicas owning a given token
    pub fn get_token_endpoints(
        &self,
        keyspace: &str,
        table: &str,
        token: Token,
    ) -> Vec<(Arc<Node>, Shard)> {
        let table_spec = TableSpec::borrowed(keyspace, table);
        self.get_token_endpoints_iter(&table_spec, token)
            .map(|(node, shard)| (node.clone(), shard))
            .collect()
    }

    pub(crate) fn get_token_endpoints_iter(
        &self,
        table_spec: &TableSpec,
        token: Token,
    ) -> impl Iterator<Item = (NodeRef<'_>, Shard)> + Clone + use<'_> {
        let keyspace = self.keyspaces.get(table_spec.ks_name());
        let strategy = keyspace
            .map(|k| &k.strategy)
            .unwrap_or(&Strategy::LocalStrategy);
        let replica_set = self
            .replica_locator()
            .replicas_for_token(token, strategy, None, table_spec);

        replica_set.into_iter()
    }

    /// Access to replicas owning a given partition key (similar to `nodetool getendpoints`)
    ///
    /// `partition_key` argument contains the values of all partition key
    /// columns. You can use both unnamed values like a tuple (e.g. `(1, 5, 5)`)
    /// or named values (e.g. struct that derives `SerializeRow`), as you would
    /// when executing a request. No additional values are allowed besides values
    /// for primary key columns.
    pub fn get_endpoints(
        &self,
        keyspace: &str,
        table: &str,
        partition_key: &dyn SerializeRow,
    ) -> Result<Vec<(Arc<Node>, Shard)>, ClusterStateTokenError> {
        let token = self.compute_token(keyspace, table, partition_key)?;
        Ok(self.get_token_endpoints(keyspace, table, token))
    }

    /// Access replica location info
    pub fn replica_locator(&self) -> &ReplicaLocator {
        &self.locator
    }

    /// Returns nonempty iterator (over nodes) of iterators (over shards).
    ///
    /// External iterator iterates over nodes.
    /// Internal iterator iterates over working connections to all shards of given node.
    pub(crate) fn iter_working_connections_per_node(
        &self,
    ) -> Result<
        impl Iterator<Item = (Uuid, impl Iterator<Item = Arc<Connection>> + use<>)> + use<'_>,
        ConnectionPoolError,
    > {
        // The returned iterator is nonempty by nonemptiness invariant of `self.known_nodes`.
        assert!(!self.known_nodes.is_empty());
        let nodes_iter = self.known_nodes.values();
        let mut connection_pool_per_node_iter = nodes_iter.map(|node| {
            node.get_working_connections()
                .map(|pool| (node.host_id, pool))
        });

        // First we try to find the first working pool of connections.
        // If none is found, return error.
        let first_working_pool_or_error: Result<(Uuid, Vec<Arc<Connection>>), ConnectionPoolError> =
            connection_pool_per_node_iter
                .by_ref()
                .find_or_first(Result::is_ok)
                .expect("impossible: known_nodes was asserted to be nonempty");

        // We have:
        // 1. either consumed the whole iterator without success and got the first error,
        //    in which case we propagate it;
        // 2. or found the first working pool of connections.
        let first_working_pool: (Uuid, Vec<Arc<Connection>>) = first_working_pool_or_error?;

        // We retrieve connection pools for remaining nodes (those that are left in the iterator
        // once the first working pool has been found).
        let remaining_pools_iter = connection_pool_per_node_iter;
        // Errors (non-working pools) are filtered out.
        let remaining_working_pools_iter = remaining_pools_iter.filter_map(Result::ok);

        // First pool is chained with the rest.
        // Then, pools are made iterators, so now we have `impl Iterator<Item = (Uuid, impl Iterator<Item = Arc<Connection>>)>`.
        Ok(std::iter::once(first_working_pool)
            .chain(remaining_working_pools_iter)
            .map(|(host_id, pool)| (host_id, IntoIterator::into_iter(pool))))
        // By an invariant `self.known_nodes` is nonempty, so the returned iterator
        // is nonempty, too.
    }

    /// Returns nonempty iterator of working connections to all shards.
    pub(crate) fn iter_working_connections_to_shards(
        &self,
    ) -> Result<impl Iterator<Item = Arc<Connection>> + use<'_>, ConnectionPoolError> {
        self.iter_working_connections_per_node()
            .map(|outer_iter| outer_iter.flat_map(|(_, inner_iter)| inner_iter))
    }

    /// Returns nonempty iterator of working connections to all nodes.
    pub(crate) fn iter_working_connections_to_nodes(
        &self,
    ) -> Result<impl Iterator<Item = Arc<Connection>> + use<'_>, ConnectionPoolError> {
        // The returned iterator is nonempty by nonemptiness invariant of `self.known_nodes`.
        assert!(!self.known_nodes.is_empty());
        let nodes_iter = self.known_nodes.values();
        let mut single_connection_per_node_iter =
            nodes_iter.map(|node| node.get_random_connection());

        // First we try to find the first working connection.
        // If none is found, return error.
        let first_working_connection_or_error: Result<Arc<Connection>, ConnectionPoolError> =
            single_connection_per_node_iter
                .by_ref()
                .find_or_first(Result::is_ok)
                .expect("impossible: known_nodes was asserted to be nonempty");

        // We have:
        // 1. either consumed the whole iterator without success and got the first error,
        //    in which case we propagate it;
        // 2. or found the first working connection.
        let first_working_connection: Arc<Connection> = first_working_connection_or_error?;

        // We retrieve single random connections for remaining nodes (those that are left in the iterator
        // once the first working connection has been found). Errors (non-working connections) are filtered out.
        let remaining_connection_iter = single_connection_per_node_iter.filter_map(Result::ok);

        // Connections to the remaining nodes are chained to the first working connection.
        Ok(std::iter::once(first_working_connection).chain(remaining_connection_iter))
        // The returned iterator is nonempty, because it returns at least `first_working_pool`.
    }

    #[cfg(test)]
    fn known_nodes(&self) -> &HashMap<Uuid, Arc<Node>> {
        &self.known_nodes
    }

    pub(super) fn update_tablets(&mut self, raw_tablets: Vec<(TableSpec<'static>, RawTablet)>) {
        let replica_translator = |uuid: Uuid| self.known_nodes.get(&uuid).cloned();

        for (table, raw_tablet) in raw_tablets.into_iter() {
            // Should we skip tablets that belong to a keyspace not present in
            // self.keyspaces? The keyspace could have been, without driver's knowledge:
            // 1. Dropped - in which case we'll remove its info soon (when refreshing
            // topology) anyway.
            // 2. Created - no harm in storing the info now.
            //
            // So I think we can safely skip checking keyspace presence.
            let tablet = match Tablet::from_raw_tablet(raw_tablet, replica_translator) {
                Ok(t) => t,
                Err((t, f)) => {
                    debug!(
                        "Nodes ({}) that are replicas for a tablet {{ks: {}, table: {}, range: [{}. {}]}} not present in current ClusterState.known_nodes. \
                       Skipping these replicas until topology refresh",
                        f.iter().safe_format(", "),
                        table.ks_name(),
                        table.table_name(),
                        t.range().0.value(),
                        t.range().1.value()
                    );
                    t
                }
            };
            self.locator.tablets.add_tablet(table, tablet);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cluster::metadata::{Metadata, Peer};
    use crate::cluster::node::NodeAddr;
    use crate::policies::host_filter::HostFilter;
    use crate::routing::locator::tablets::TabletsInfo;
    use crate::test_utils::setup_tracing;

    use std::collections::{HashMap, HashSet};
    use std::net::SocketAddr;
    use std::sync::Arc;

    fn make_addr(id: u16) -> NodeAddr {
        NodeAddr::Translatable(SocketAddr::from(([255, 255, 255, id as u8], id)))
    }

    fn make_peer(
        host_id: Uuid,
        address: NodeAddr,
        datacenter: Option<&str>,
        rack: Option<&str>,
    ) -> Peer {
        Peer {
            host_id,
            address,
            tokens: vec![Token::new(1)],
            datacenter: datacenter.map(String::from),
            rack: rack.map(String::from),
        }
    }

    fn make_metadata(peers: Vec<Peer>) -> Metadata {
        Metadata {
            peers,
            keyspaces: HashMap::new(),
            client_routes_updated_hosts: HashSet::new(),
            cluster_name: Some("Test Cluster".into()),
        }
    }

    /// A host filter that rejects peers whose address is in the reject set.
    struct AddrRejectFilter {
        rejected: HashSet<NodeAddr>,
    }

    impl AddrRejectFilter {
        fn rejecting(addrs: impl IntoIterator<Item = NodeAddr>) -> Self {
            Self {
                rejected: addrs.into_iter().collect(),
            }
        }
    }

    impl HostFilter for AddrRejectFilter {
        fn accept(&self, peer: &Peer) -> bool {
            !self.rejected.contains(&peer.address)
        }
    }

    /// Helper: build a ClusterState from metadata, old known_nodes, and an optional host filter.
    async fn build_cluster_state(
        metadata: Metadata,
        known_nodes: &HashMap<Uuid, Arc<Node>>,
        host_filter: Option<&dyn HostFilter>,
    ) -> ClusterState {
        let (tx, _rx) = mpsc::unbounded_channel();
        ClusterState::new(
            metadata,
            &Default::default(),
            known_nodes,
            &mut |_, _| (),
            &None,
            host_filter,
            &tx,
            TabletsInfo::new(),
            &HashMap::new(),
            &Default::default(),
        )
        .await
    }

    // Node's address changes so that host filter no longer rejects it.
    // The node should become enabled, and the Node object must NOT be reused.
    #[tokio::test]
    async fn node_included_after_ip_change_not_filtered_anymore() {
        setup_tracing();

        let host_id = Uuid::new_v4();
        let addr_rejected = make_addr(1);
        let addr_accepted = make_addr(2);
        let filter = AddrRejectFilter::rejecting([addr_rejected]);

        // Build initial state: peer at addr_rejected ⇒ disabled.
        let initial_metadata = make_metadata(vec![make_peer(
            host_id,
            addr_rejected,
            Some("dc1"),
            Some("r1"),
        )]);
        let initial_state =
            build_cluster_state(initial_metadata, &HashMap::new(), Some(&filter)).await;
        let old_node = initial_state.known_nodes().get(&host_id).unwrap().clone();
        assert!(
            !old_node.is_enabled(),
            "node should be disabled when its address is rejected"
        );

        // Refresh: same host_id moves to addr_accepted ⇒ should be enabled.
        let new_metadata = make_metadata(vec![make_peer(
            host_id,
            addr_accepted,
            Some("dc1"),
            Some("r1"),
        )]);
        let new_state =
            build_cluster_state(new_metadata, initial_state.known_nodes(), Some(&filter)).await;
        let new_node = new_state.known_nodes().get(&host_id).unwrap();

        assert!(
            new_node.is_enabled(),
            "node should be enabled after address change passes the filter"
        );
        assert!(
            !Arc::ptr_eq(&old_node, new_node),
            "Node object must NOT be reused when transitioning from disabled to enabled"
        );
    }

    // Node's address changes so that host filter now rejects it.
    // The node should become disabled, and the Node object must NOT be reused.
    #[tokio::test]
    async fn node_filtered_out_after_ip_change() {
        setup_tracing();

        let host_id = Uuid::new_v4();
        let addr_accepted = make_addr(1);
        let addr_rejected = make_addr(2);
        let filter = AddrRejectFilter::rejecting([addr_rejected]);

        // Build initial state: peer at addr_accepted ⇒ enabled.
        let initial_metadata = make_metadata(vec![make_peer(
            host_id,
            addr_accepted,
            Some("dc1"),
            Some("r1"),
        )]);
        let initial_state =
            build_cluster_state(initial_metadata, &HashMap::new(), Some(&filter)).await;
        let old_node = initial_state.known_nodes().get(&host_id).unwrap().clone();
        assert!(
            old_node.is_enabled(),
            "node should be enabled when its address is accepted"
        );

        // Refresh: same host_id moves to addr_rejected ⇒ should be disabled.
        let new_metadata = make_metadata(vec![make_peer(
            host_id,
            addr_rejected,
            Some("dc1"),
            Some("r1"),
        )]);
        let new_state =
            build_cluster_state(new_metadata, initial_state.known_nodes(), Some(&filter)).await;
        let new_node = new_state.known_nodes().get(&host_id).unwrap();

        assert!(
            !new_node.is_enabled(),
            "node should be disabled after address change hits the filter"
        );
        assert!(
            !Arc::ptr_eq(&old_node, new_node),
            "Node object must NOT be reused when transitioning from enabled to disabled"
        );
    }

    // A disabled node whose attributes (dc, rack, address) have not changed
    // should reuse the same Node object (Arc::ptr_eq).
    #[tokio::test]
    async fn disabled_node_unchanged_attributes_reuses_object() {
        setup_tracing();

        let host_id = Uuid::new_v4();
        let addr = make_addr(1);
        let filter = AddrRejectFilter::rejecting([addr]);

        // Build initial state: peer at addr ⇒ disabled.
        let initial_metadata =
            make_metadata(vec![make_peer(host_id, addr, Some("dc1"), Some("r1"))]);
        let initial_state =
            build_cluster_state(initial_metadata, &HashMap::new(), Some(&filter)).await;
        let old_node = initial_state.known_nodes().get(&host_id).unwrap().clone();
        assert!(!old_node.is_enabled(), "node should be disabled");

        // Refresh with identical attributes, still filtered out.
        let new_metadata = make_metadata(vec![make_peer(host_id, addr, Some("dc1"), Some("r1"))]);
        let new_state =
            build_cluster_state(new_metadata, initial_state.known_nodes(), Some(&filter)).await;
        let new_node = new_state.known_nodes().get(&host_id).unwrap();

        assert!(!new_node.is_enabled(), "node should still be disabled");
        assert!(
            Arc::ptr_eq(&old_node, new_node),
            "Node object should be reused when disabled node's attributes haven't changed"
        );
    }
}
