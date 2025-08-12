use crate::errors::{ClusterStateTokenError, ConnectionPoolError};
use crate::network::{Connection, PoolConfig, VerifiedKeyspaceName};
#[cfg(feature = "metrics")]
use crate::observability::metrics::Metrics;
use crate::policies::host_filter::HostFilter;
use crate::routing::locator::ReplicaLocator;
use crate::routing::locator::tablets::{RawTablet, Tablet, TabletsInfo};
use crate::routing::partitioner::{PartitionerName, calculate_token_for_partition_key};
use crate::routing::{Shard, Token};
use crate::utils::safe_format::IteratorSafeFormatExt;

use itertools::Itertools;
use scylla_cql::frame::response::result::TableSpec;
use scylla_cql::serialize::row::{RowSerializationContext, SerializeRow, SerializedValues};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
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
    pub(crate) known_peers: HashMap<Uuid, Arc<Node>>, // Invariant: nonempty after Cluster::new()

    /// Contains the same set of nodes as `known_peers`.
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
            .field("known_peers", &cluster_state.known_peers)
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

    /// Creates new ClusterState using information about topology held in `metadata`.
    /// Uses provided `known_peers` hashmap to recycle nodes if possible.
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn new(
        metadata: Metadata,
        pool_config: &PoolConfig,
        known_peers: &HashMap<Uuid, Arc<Node>>,
        used_keyspace: &Option<VerifiedKeyspaceName>,
        host_filter: Option<&dyn HostFilter>,
        mut tablets: TabletsInfo,
        old_keyspaces: &HashMap<String, Keyspace>,
        #[cfg(feature = "metrics")] metrics: &Arc<Metrics>,
    ) -> Self {
        // Create new updated known_peers and ring
        let mut new_known_peers: HashMap<Uuid, Arc<Node>> =
            HashMap::with_capacity(metadata.peers.len());
        let mut ring: Vec<(Token, Arc<Node>)> = Vec::new();

        for peer in metadata.peers {
            // Take existing Arc<Node> if possible, otherwise create new one
            // Changing rack/datacenter but not ip address seems improbable
            // so we can just create new node and connections then
            let peer_host_id = peer.host_id;
            let peer_address = peer.address;
            let peer_tokens;

            let node: Arc<Node> = match known_peers.get(&peer_host_id) {
                Some(node) if node.datacenter == peer.datacenter && node.rack == peer.rack => {
                    let (peer_endpoint, tokens) = peer.into_peer_endpoint_and_tokens();
                    peer_tokens = tokens;
                    if node.address == peer_address {
                        Arc::clone(node)
                    } else {
                        // If IP changes, the Node struct is recreated, but the underlying pool is preserved and notified about the IP change.
                        Arc::new(Node::inherit_with_ip_changed(node, peer_endpoint))
                    }
                }
                _ => {
                    let is_enabled = host_filter.is_none_or(|f| f.accept(&peer));
                    let (peer_endpoint, tokens) = peer.into_peer_endpoint_and_tokens();
                    peer_tokens = tokens;
                    Arc::new(Node::new(
                        peer_endpoint,
                        pool_config,
                        used_keyspace.clone(),
                        is_enabled,
                        #[cfg(feature = "metrics")]
                        Arc::clone(metrics),
                    ))
                }
            };

            new_known_peers.insert(peer_host_id, Arc::clone(&node));

            for token in peer_tokens {
                ring.push((token, Arc::clone(&node)));
            }
        }

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
                            present in ClusterData until next refresh."
                        );
                        None
                    }
                }
            })
            .collect();

        {
            let removed_nodes = {
                let mut removed_nodes = HashSet::new();
                for old_peer in known_peers {
                    if !new_known_peers.contains_key(old_peer.0) {
                        removed_nodes.insert(*old_peer.0);
                    }
                }

                removed_nodes
            };

            let table_predicate = |spec: &TableSpec| {
                if let Some(ks) = keyspaces.get(spec.ks_name()) {
                    ks.tables.contains_key(spec.table_name())
                } else {
                    false
                }
            };

            let recreated_nodes = {
                let mut recreated_nodes = HashMap::new();
                for (old_peer_id, old_peer_node) in known_peers {
                    if let Some(new_peer_node) = new_known_peers.get(old_peer_id) {
                        if !Arc::ptr_eq(old_peer_node, new_peer_node) {
                            recreated_nodes.insert(*old_peer_id, Arc::clone(new_peer_node));
                        }
                    }
                }

                recreated_nodes
            };

            tablets.perform_maintenance(
                &table_predicate,
                &removed_nodes,
                &new_known_peers,
                &recreated_nodes,
            )
        }

        let (locator, keyspaces) = tokio::task::spawn_blocking(move || {
            let keyspace_strategies = keyspaces.values().map(|ks| &ks.strategy);
            let locator = ReplicaLocator::new(ring.into_iter(), keyspace_strategies, tablets);
            (locator, keyspaces)
        })
        .await
        .unwrap();

        ClusterState {
            all_nodes: new_known_peers.values().cloned().collect(),
            known_peers: new_known_peers,
            keyspaces,
            locator,
        }
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
        impl Iterator<Item = (Uuid, impl Iterator<Item = Arc<Connection>> + use<>)> + '_,
        ConnectionPoolError,
    > {
        // The returned iterator is nonempty by nonemptiness invariant of `self.known_peers`.
        assert!(!self.known_peers.is_empty());
        let nodes_iter = self.known_peers.values();
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
                .expect("impossible: known_peers was asserted to be nonempty");

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
        // By an invariant `self.known_peers` is nonempty, so the returned iterator
        // is nonempty, too.
    }

    /// Returns nonempty iterator of working connections to all shards.
    pub(crate) fn iter_working_connections_to_shards(
        &self,
    ) -> Result<impl Iterator<Item = Arc<Connection>> + '_, ConnectionPoolError> {
        self.iter_working_connections_per_node()
            .map(|outer_iter| outer_iter.flat_map(|(_, inner_iter)| inner_iter))
    }

    /// Returns nonempty iterator of working connections to all nodes.
    pub(crate) fn iter_working_connections_to_nodes(
        &self,
    ) -> Result<impl Iterator<Item = Arc<Connection>> + '_, ConnectionPoolError> {
        // The returned iterator is nonempty by nonemptiness invariant of `self.known_peers`.
        assert!(!self.known_peers.is_empty());
        let nodes_iter = self.known_peers.values();
        let mut single_connection_per_node_iter =
            nodes_iter.map(|node| node.get_random_connection());

        // First we try to find the first working connection.
        // If none is found, return error.
        let first_working_connection_or_error: Result<Arc<Connection>, ConnectionPoolError> =
            single_connection_per_node_iter
                .by_ref()
                .find_or_first(Result::is_ok)
                .expect("impossible: known_peers was asserted to be nonempty");

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

    pub(super) fn update_tablets(&mut self, raw_tablets: Vec<(TableSpec<'static>, RawTablet)>) {
        let replica_translator = |uuid: Uuid| self.known_peers.get(&uuid).cloned();

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
                        "Nodes ({}) that are replicas for a tablet {{ks: {}, table: {}, range: [{}. {}]}} not present in current ClusterState.known_peers. \
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
