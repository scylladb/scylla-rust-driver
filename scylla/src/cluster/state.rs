use crate::cluster::metadata::update::{FetchedKeyspace, SchemaUpdate};
use crate::cluster::metadata::{Peer, SingleKeyspaceMetadataError};
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

use super::metadata::{Keyspace, Metadata, Strategy, Table};
use super::node::{Node, NodeRef};

/// Helper struct to group parameters that are only needed to
/// construct `Node` objects.
pub(crate) struct NodeConfig {
    // Config for `NodeConnectionPool` for all new nodes.
    pub(crate) pool_config: PoolConfig,
    // Keyspace send in "USE <keyspace name>" when opening each connection.
    pub(crate) used_keyspace: Option<VerifiedKeyspaceName>,
    // Sender part of that channel to pass to `PoolRefiller`s.
    pub(crate) connectivity_events_sender: mpsc::UnboundedSender<ConnectivityChangeEvent>,
    // Metrics passed to each new `NodeConnectionPool`.
    pub(crate) metrics: Metrics,
}

/// The nodes known to be part of the cluster. Often refered to as "topology metadata".
///
/// Both collections hold the same set of nodes and are rebuilt together on
/// every topology change.
#[derive(Clone)]
pub(crate) struct Topology {
    /// All nodes known to be part of the cluster, accessible by their host ID.
    pub(crate) known_nodes: HashMap<Uuid, Arc<Node>>, // Invariant: nonempty after Cluster::new()

    /// Contains the same set of nodes as `known_nodes`.
    ///
    /// Introduced to fix the bug that zero-token nodes were missing from
    /// `ClusterState::get_nodes_info()` slice, because the slice was borrowed
    /// from `ReplicaLocator`, which only contains nodes with some tokens assigned.
    // TODO: in 2.0, make `get_nodes_info()` return `Iterator` instead of a slice.
    // Then, remove this field.
    pub(crate) all_nodes: Vec<Arc<Node>>,
}

impl Topology {
    pub(crate) fn new(known_nodes: HashMap<Uuid, Arc<Node>>) -> Self {
        Self {
            all_nodes: known_nodes.values().cloned().collect(),
            known_nodes,
        }
    }

    /// Whether `known_nodes` holds exactly the `Node` objects of `self`.
    ///
    /// Pointer identity is enough: `ClusterState::calculate_new_topology`
    /// reuses a `Node` object only if nothing about the node changed.
    fn has_same_nodes(&self, known_nodes: &KnownNodes) -> bool {
        self.known_nodes.len() == known_nodes.len()
            && known_nodes.iter().all(|(host_id, node)| {
                self.known_nodes
                    .get(host_id)
                    .is_some_and(|own_node| Arc::ptr_eq(own_node, node))
            })
    }

    /// Whether some enabled `Node` object is present in both topologies.
    pub(crate) fn shares_enabled_node_with(&self, other: &Topology) -> bool {
        self.known_nodes.iter().any(|(host_id, node)| {
            node.is_enabled()
                && other
                    .known_nodes
                    .get(host_id)
                    .is_some_and(|other_node| Arc::ptr_eq(node, other_node))
        })
    }
}

/// Represents the state of the cluster, including known nodes, keyspaces, and replica locator.
///
/// It is immutable after creation, and is replaced atomically upon a metadata refresh.
/// Can be accessed through [Session::get_cluster_state()](crate::client::session::Session::get_cluster_state).
#[derive(Clone)]
pub struct ClusterState {
    /// Shared, not copied, when a new `ClusterState` is derived from this one
    /// without a topology change (e.g. on a tablet update).
    pub(crate) topology: Arc<Topology>,

    /// All keyspaces in the cluster, accessible by their name.
    /// Often refered to as "schema metadata".
    ///
    /// Shared, not copied, when a new `ClusterState` is derived from this one
    /// without a schema change (e.g. on a tablet update).
    pub(crate) keyspaces: Arc<HashMap<String, Arc<Keyspace>>>,

    /// The entity which provides a way to find the set of owning nodes (+shards, in case of ScyllaDB)
    /// for a given (token, replication strategy, table) tuple.
    /// It relies on both topology and schema metadata.
    pub(crate) locator: ReplicaLocator,

    /// The name of the cluster, as reported by the `cluster_name` column in `system.local`.
    pub(crate) cluster_name: Option<Arc<str>>,
}

impl std::fmt::Debug for ClusterState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let ring_printer = {
            struct RingSizePrinter(usize);
            impl std::fmt::Debug for RingSizePrinter {
                fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                    write!(f, "<size={}>", self.0)
                }
            }
            RingSizePrinter(self.locator.ring().len())
        };

        f.debug_struct("ClusterState")
            .field("known_nodes", &self.topology.known_nodes)
            .field("ring", &ring_printer)
            .field("keyspaces", &self.keyspaces.keys())
            .finish_non_exhaustive()
    }
}

type KnownNodes = HashMap<Uuid, Arc<Node>>;
type Ring = Vec<(Token, Arc<Node>)>;

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
            if let Some(node) = self.topology.known_nodes.get(&host_id) {
                debug!(
                    host_id = %host_id,
                    "Triggering immediate pool refill for relevant Node"
                );
                node.trigger_pool_refill();
            }
        }
    }

    /// Finds the known node whose address equals `addr`, if any.
    ///
    /// `STATUS_CHANGE` events carry the node's broadcast address. For a node learnt from
    /// metadata that is exactly what [`Node::address`] holds
    /// ([`NodeAddr::Translatable`], i.e. before any address translation), so the
    /// comparison is right. In the current implementation, `Node` object always uses this variant.
    fn node_by_broadcast_address(&self, addr: SocketAddr) -> Option<&Arc<Node>> {
        self.topology
            .known_nodes
            .values()
            .find(|node| node.address.into_inner() == addr)
    }

    /// Triggers an immediate pool refill for the node with the given broadcast
    /// address. Used when a `STATUS_CHANGE UP` event hints that a node is back
    /// and its pool (likely in exponential backoff) should retry immediately.
    pub(super) fn trigger_pool_refill_for_addr(&self, addr: SocketAddr) {
        let Some(node) = self.node_by_broadcast_address(addr) else {
            debug!(
                address = %addr,
                "STATUS_CHANGE UP: no known node with this address"
            );
            return;
        };
        debug!(
            address = %addr,
            host_id = %node.host_id,
            "STATUS_CHANGE UP: triggering immediate pool refill"
        );
        node.trigger_pool_refill();
    }

    /// Triggers an immediate keepalive request on all connections to the node with the
    /// given broadcast address.
    ///
    /// Used when a `STATUS_CHANGE DOWN` event hints that the node's connections are
    /// likely defunct, so the driver probes them immediately instead of waiting for the
    /// next keepalive interval tick. If the probe fails, the connections are closed and
    /// the node stops being targeted by the load balancing policy; if it succeeds, the
    /// node is likely still alive and keeps being targeted.
    pub(super) fn trigger_keepalive_for_addr(&self, addr: SocketAddr) {
        let Some(node) = self.node_by_broadcast_address(addr) else {
            debug!(
                address = %addr,
                "STATUS_CHANGE DOWN: no known node with this address"
            );
            return;
        };
        debug!(
            address = %addr,
            host_id = %node.host_id,
            "STATUS_CHANGE DOWN: triggering immediate keepalive"
        );
        node.trigger_keepalive();
    }

    pub(crate) async fn new(
        metadata: Metadata,
        node_config: &NodeConfig,
        host_filter: Option<&dyn HostFilter>,
    ) -> Self {
        let (new_known_nodes, ring) =
            Self::calculate_new_topology(metadata.peers, &HashMap::new(), node_config, host_filter);

        let keyspaces = Arc::new(Self::resolve_metadata_keyspaces(
            metadata.keyspaces,
            &HashMap::new(),
        ));

        let mut tablets = TabletsInfo::new();
        // Perform maintenance to create empty entries for all tablets-based tables.
        Self::perform_tablets_maintenance(
            &mut tablets,
            &HashMap::new(),
            &new_known_nodes,
            &keyspaces,
        );

        let (locator, keyspaces) = Self::calculate_new_locator(keyspaces, ring, tablets).await;

        ClusterState {
            topology: Arc::new(Topology::new(new_known_nodes)),
            keyspaces,
            locator,
            cluster_name: metadata.cluster_name.map(Arc::from),
        }
    }

    /// Creates new ClusterState using information about topology and schema held
    /// in `metadata`. Uses `self` to reuse data when possible.
    pub(crate) async fn new_updated(
        &self,
        metadata: Metadata,
        node_config: &NodeConfig,
        host_filter: Option<&dyn HostFilter>,
    ) -> Self {
        let keyspaces = Arc::new(Self::resolve_metadata_keyspaces(
            metadata.keyspaces,
            &self.keyspaces,
        ));

        let (new_known_nodes, ring) = Self::calculate_new_topology(
            metadata.peers,
            &self.topology.known_nodes,
            node_config,
            host_filter,
        );

        let mut tablets = self.locator.tablets.clone();
        Self::perform_tablets_maintenance(
            &mut tablets,
            &self.topology.known_nodes,
            &new_known_nodes,
            &keyspaces,
        );

        let (locator, keyspaces) = Self::calculate_new_locator(keyspaces, ring, tablets).await;

        ClusterState {
            topology: Arc::new(Topology::new(new_known_nodes)),
            keyspaces,
            locator,
            cluster_name: metadata.cluster_name.map(Arc::from),
        }
    }

    /// Creates a new `ClusterState` from partially fetched metadata, reusing
    /// everything of `self` that the partial fetches did not read - the
    /// counterpart of [`new_updated`](Self::new_updated) for partial fetches.
    ///
    /// `peers` is the peer list read by a partial topology fetch and `schema`
    /// the per-keyspace schema read by a partial schema fetch; `None` means the
    /// aspect was not re-read, so the one of `self` stands. The cluster name is
    /// never re-read by a partial fetch - it is fixed for the lifetime of a
    /// cluster.
    pub(crate) async fn new_with_partial_changes(
        &self,
        peers: Option<Vec<Peer>>,
        schema: Option<SchemaUpdate>,
        node_config: &NodeConfig,
        host_filter: Option<&dyn HostFilter>,
    ) -> Self {
        // The topology is unchanged if it was not re-read, or was re-read
        // and turned out identical. Either way it is shared with `self`. The
        // ring in place is exactly the (token, node) list that it implies, so
        // it is reused as is.
        let (topology, ring) = peers
            .and_then(|peers| self.updated_topology(peers, node_config, host_filter))
            .unwrap_or_else(|| {
                (
                    Arc::clone(&self.topology),
                    self.locator.ring().iter().cloned().collect(),
                )
            });

        let keyspaces = match schema {
            Some(schema) => Arc::new(self.updated_keyspaces(schema)),
            // The schema is unchanged, so it is shared with `self`.
            None => Arc::clone(&self.keyspaces),
        };

        let mut tablets = self.locator.tablets.clone();
        Self::perform_tablets_maintenance(
            &mut tablets,
            &self.topology.known_nodes,
            &topology.known_nodes,
            &keyspaces,
        );

        let (locator, keyspaces) = Self::calculate_new_locator(keyspaces, ring, tablets).await;

        ClusterState {
            topology,
            keyspaces,
            locator,
            cluster_name: self.cluster_name.clone(),
        }
    }

    /// The topology that `peers` describe, with its ring, reusing the `Node`
    /// objects of `self` where possible - or `None` if it is exactly the
    /// topology of `self`: the same `Node` objects, holding the same tokens.
    ///
    /// That is what the fetch triggered by a STATUS_CHANGE event usually
    /// yields, as a node going down or up changes nothing about the topology.
    fn updated_topology(
        &self,
        peers: Vec<Peer>,
        node_config: &NodeConfig,
        host_filter: Option<&dyn HostFilter>,
    ) -> Option<(Arc<Topology>, Ring)> {
        let (new_known_nodes, mut ring) = Self::calculate_new_topology(
            peers,
            &self.topology.known_nodes,
            node_config,
            host_filter,
        );

        // The ring of `self` is sorted by token, so the new one is sorted too
        // to be compared with it entry by entry. `ReplicaLocator::new` would
        // sort it anyway.
        ring.sort_by_key(|(token, _)| *token);

        // The ring holds only nodes with tokens, so the nodes are compared
        // separately: a zero-token node may have appeared or gone.
        let unchanged = self.topology.has_same_nodes(&new_known_nodes) && self.has_same_ring(&ring);
        (!unchanged).then(|| (Arc::new(Topology::new(new_known_nodes)), ring))
    }

    /// Whether `sorted_ring` holds exactly the entries of the ring of `self`:
    /// the same `Node` objects at the same tokens.
    fn has_same_ring(&self, sorted_ring: &Ring) -> bool {
        let ring = self.locator.ring();
        ring.len() == sorted_ring.len()
            && ring
                .iter()
                .zip(sorted_ring)
                .all(|((token, node), (new_token, new_node))| {
                    token == new_token && Arc::ptr_eq(node, new_node)
                })
    }

    /// Applies a partial schema update onto the schema metadata of `self`:
    /// replaces the metadata of the re-read keyspaces and removes those that
    /// ceased to exist, keeping every keyspace the update does not mention.
    fn updated_keyspaces(&self, schema: SchemaUpdate) -> HashMap<String, Arc<Keyspace>> {
        let mut new_keyspaces = HashMap::clone(&self.keyspaces);

        for (name, keyspace) in schema.keyspaces {
            // A keyspace whose fresh metadata turned out inconsistent keeps its
            // previous version, as after a full fetch - hence the resolution
            // step, shared with `new_updated`.
            let resolved = match keyspace {
                FetchedKeyspace::Present(keyspace) => {
                    Self::resolve_metadata_keyspace(&name, keyspace, &self.keyspaces)
                }
                FetchedKeyspace::Absent => None,
            };

            match resolved {
                Some(keyspace) => new_keyspaces.insert(name, keyspace),
                None => new_keyspaces.remove(&name),
            };
        }

        new_keyspaces
    }

    /// Creates a new topology (`Node` objects and token ring) from metadata peers,
    /// and previous topology.
    ///
    /// Previous topology is used to reuse connection pools and `Node` objects when possible.
    fn calculate_new_topology(
        peers: Vec<Peer>,
        known_nodes: &KnownNodes,
        node_config: &NodeConfig,
        host_filter: Option<&dyn HostFilter>,
    ) -> (KnownNodes, Ring) {
        // Create new updated known_nodes and ring
        let mut new_known_nodes: KnownNodes = HashMap::with_capacity(peers.len());
        let mut ring: Ring = Vec::new();

        for peer in peers {
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
                    &node_config.pool_config,
                    node_config.connectivity_events_sender.clone(),
                    node_config.used_keyspace.clone(),
                    node_config.metrics.clone(),
                )),
            };

            new_known_nodes.insert(peer_host_id, Arc::clone(&node));

            for token in peer_tokens {
                ring.push((token, Arc::clone(&node)));
            }
        }

        (new_known_nodes, ring)
    }

    /// Handles single-keyspace errors by reusing old `Keyspace` objects for such
    /// broken keyspaces and warns about it.
    fn resolve_metadata_keyspaces(
        meta_keyspaces: HashMap<String, Result<Arc<Keyspace>, SingleKeyspaceMetadataError>>,
        old_keyspaces: &HashMap<String, Arc<Keyspace>>,
    ) -> HashMap<String, Arc<Keyspace>> {
        meta_keyspaces
            .into_iter()
            .filter_map(|(ks_name, ks)| {
                Self::resolve_metadata_keyspace(&ks_name, ks, old_keyspaces).map(|ks| (ks_name, ks))
            })
            .collect()
    }

    /// [`resolve_metadata_keyspaces`](Self::resolve_metadata_keyspaces) for a
    /// single keyspace. `None` means there is no metadata to be had for it,
    /// neither fresh nor previous, so it belongs in no `ClusterState`.
    fn resolve_metadata_keyspace(
        ks_name: &str,
        ks: Result<Arc<Keyspace>, SingleKeyspaceMetadataError>,
        old_keyspaces: &HashMap<String, Arc<Keyspace>>,
    ) -> Option<Arc<Keyspace>> {
        match ks {
            Ok(ks) => Some(ks),
            Err(e) => {
                if let Some(old_ks) = old_keyspaces.get(ks_name) {
                    warn!(
                        "Encountered an error while processing \
                        metadata of keyspace \"{ks_name}\": {e}. \
                        Re-using older version of this keyspace metadata"
                    );
                    Some(old_ks.clone())
                } else {
                    warn!(
                        "Encountered an error while processing metadata \
                        of keyspace \"{ks_name}\": {e}. \
                        No previous version of this keyspace metadata found, so it will not be \
                        present in ClusterState until next refresh."
                    );
                    None
                }
            }
        }
    }

    fn perform_tablets_maintenance(
        tablets: &mut TabletsInfo,
        old_known_nodes: &KnownNodes,
        new_known_nodes: &KnownNodes,
        keyspaces: &HashMap<String, Arc<Keyspace>>,
    ) {
        let removed_nodes = {
            let mut removed_nodes = HashSet::new();
            for old_peer in old_known_nodes {
                if !new_known_nodes.contains_key(old_peer.0) {
                    removed_nodes.insert(*old_peer.0);
                }
            }

            removed_nodes
        };

        let recreated_nodes = {
            let mut recreated_nodes = HashMap::new();
            for (old_peer_id, old_peer_node) in old_known_nodes {
                if let Some(new_peer_node) = new_known_nodes.get(old_peer_id)
                    && !Arc::ptr_eq(old_peer_node, new_peer_node)
                {
                    recreated_nodes.insert(*old_peer_id, Arc::clone(new_peer_node));
                }
            }

            recreated_nodes
        };

        tablets.perform_maintenance(keyspaces, &removed_nodes, new_known_nodes, &recreated_nodes)
    }

    async fn calculate_new_locator(
        keyspaces: Arc<HashMap<String, Arc<Keyspace>>>,
        ring: Ring,
        tablets: TabletsInfo,
    ) -> (ReplicaLocator, Arc<HashMap<String, Arc<Keyspace>>>) {
        tokio::task::spawn_blocking(move || {
            let keyspace_strategies = keyspaces
                .values()
                .filter(|ks| !ks.tablet_based)
                .map(|ks| &ks.strategy);
            let locator = ReplicaLocator::new(ring.into_iter(), keyspace_strategies, tablets);
            (locator, keyspaces)
        })
        .await
        .unwrap()
    }

    /// Returns the name of the cluster, as reported by the `cluster_name` column in `system.local`.
    pub fn cluster_name(&self) -> &str {
        self.cluster_name.as_deref().unwrap_or("")
    }

    /// Access keyspace details collected by the driver.
    pub fn get_keyspace(&self, keyspace: impl AsRef<str>) -> Option<&Keyspace> {
        self.keyspaces.get(keyspace.as_ref()).map(Arc::as_ref)
    }

    /// Returns an iterator over keyspaces.
    pub fn keyspaces_iter(&self) -> impl Iterator<Item = (&str, &Keyspace)> {
        self.keyspaces.iter().map(|(k, v)| (k.as_str(), v.as_ref()))
    }

    /// Access details about nodes known to the driver
    pub fn get_nodes_info(&self) -> &[Arc<Node>] {
        &self.topology.all_nodes
    }

    /// Access details about specific node known to the driver, querying by host id.
    pub fn get_node_by_host_id(&self, host_id: Uuid) -> Option<NodeRef<'_>> {
        self.topology.known_nodes.get(&host_id)
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
        let table_meta = self.lookup_table_meta(keyspace, table)?;

        let values = SerializedValues::from_serializable(
            &RowSerializationContext::from_specs(table_meta.pk_column_specs.as_slice()),
            partition_key,
        )?;

        // Delegate actual token calculation to centralized helper.
        self.do_compute_token(table_meta, &values)
    }

    fn do_compute_token(
        &self,
        table: &Table,
        serialized_partition_key: &SerializedValues,
    ) -> Result<Token, ClusterStateTokenError> {
        let partitioner = table
            .partitioner
            .as_deref()
            .and_then(PartitionerName::from_str)
            .unwrap_or_default();
        calculate_token_for_partition_key(serialized_partition_key, &partitioner)
            .map_err(ClusterStateTokenError::TokenCalculation)
    }

    /// Helper: lookup table metadata or return `UnknownTable` error.
    fn lookup_table_meta(
        &self,
        keyspace: &str,
        table: &str,
    ) -> Result<&Table, ClusterStateTokenError> {
        self.keyspaces
            .get(keyspace)
            .and_then(|k| k.tables.get(table))
            .ok_or_else(|| ClusterStateTokenError::UnknownTable {
                keyspace: keyspace.to_string(),
                table: table.to_string(),
            })
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
        // The returned iterator is nonempty by nonemptiness invariant of `self.topology.known_nodes`.
        assert!(!self.topology.known_nodes.is_empty());
        let nodes_iter = self.topology.known_nodes.values();
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
        // By an invariant `self.topology.known_nodes` is nonempty, so the returned iterator
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
        // The returned iterator is nonempty by nonemptiness invariant of `self.topology.known_nodes`.
        assert!(!self.topology.known_nodes.is_empty());
        let nodes_iter = self.topology.known_nodes.values();
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
        &self.topology.known_nodes
    }

    /// Returns a copy of `self` with `raw_tablets` recorded, or `None` if every
    /// one of them is already recorded exactly as given, so that there is nothing
    /// new to publish.
    ///
    /// The `None` case is common: every in-flight request to a not yet learned
    /// tablet triggers feedback for it, so the same tablet tends to arrive many
    /// times in a row. A repeat is detected on the raw payload, before its
    /// replicas are resolved and before `self` is cloned, so it costs neither
    /// allocation nor a new `ClusterState`.
    pub(crate) fn with_updated_tablets(
        &self,
        raw_tablets: Vec<(TableSpec<'static>, RawTablet)>,
    ) -> Option<ClusterState> {
        let replica_translator = |uuid: Uuid| self.topology.known_nodes.get(&uuid).cloned();

        // Cloned lazily, by the first tablet that turns out to change anything.
        let mut new_state: Option<ClusterState> = None;

        for (table, raw_tablet) in raw_tablets {
            // Compared with the state as updated by the batch so far, not with
            // `self`: an earlier tablet of the batch may have replaced this one.
            let current = new_state.as_ref().unwrap_or(self);
            if current.locator.tablets.contains(&table, &raw_tablet) {
                continue;
            }

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

            new_state
                .get_or_insert_with(|| self.clone())
                .locator
                .tablets
                .add_tablet(table, tablet);
        }

        new_state
    }
}

/// Additional API for interop-based code.
#[cfg(all(scylla_unstable, feature = "unstable-csharp-rs"))]
impl ClusterState {
    /// Compute token for an externally-provided **serialized** partition key.
    ///
    /// For typical Rust code that starts from strongly-typed key values, prefer
    /// [`ClusterState::compute_token`], which takes typed values and handles their
    /// serialization into [`SerializedValues`] for you.
    ///
    /// This is a lower-level variant of [`ClusterState::compute_token`] intended for interop
    /// and FFI use, when the caller already has a [`SerializedValues`] representing
    /// the partition key.
    ///
    /// # Partition key format
    ///
    /// `serialized_partition_key` must contain **exactly** the serialized values of
    /// the table’s partition key columns:
    ///
    /// - Values must be provided in the same order as the partition key columns are
    ///   defined in the table schema (e.g. `PRIMARY KEY ((k1, k2), ...)` means `k1`,
    ///   then `k2`).
    /// - Each value must be serialized using the CQL type of the corresponding
    ///   partition key column, using the standard CQL binary encoding (including
    ///   length prefixes for composite keys).
    /// - Only partition key columns are allowed; clustering or regular columns must
    ///   not be included.
    /// - Partition key components must not be null or unset.
    ///
    /// This method performs no reordering or validation beyond basic structural
    /// checks. If the provided values do not exactly match the table’s partition key
    /// definition in order, type, or encoding, token computation will produce
    /// incorrect results.
    ///
    /// # Usage notes
    ///
    /// Use this method when you already have a `SerializedValues` representation of
    /// the partition key, for example:
    ///
    /// - from another language / binding,
    /// - from code that manually constructs `SerializedValues` using
    ///   [`RowSerializationContext`] and the [`SerializeRow`] trait.
    ///
    /// # Correctness validation
    ///
    /// Type validation at this stage would be difficult, as it would likely require deserializing
    /// each value to check its type against the table schema. Therefore, it is the caller’s
    /// responsibility to ensure that the provided serialized values are correct for the target
    /// table’s partition key.
    pub fn compute_token_preserialized(
        &self,
        keyspace: &str,
        table: &str,
        serialized_partition_key: &SerializedValues,
    ) -> Result<Token, ClusterStateTokenError> {
        let table_meta = self.lookup_table_meta(keyspace, table)?;
        self.do_compute_token(table_meta, serialized_partition_key)
    }

    /// Compute token using a specific partitioner, bypassing table metadata lookup.
    ///
    /// See [`ClusterState::compute_token_preserialized`] for details on
    /// the `serialized_partition_key` format.
    ///
    /// This is useful when the table metadata is not available but the partitioner
    /// is known (or default Murmur3 is desired).
    /// This is the case for the following (Obsolete) C# Driver API:
    /// ```csharp
    /// public ICollection<HostShard> GetReplicas(string keyspaceName, byte[] partitionKey);
    /// ```
    /// Once this API is removed, this method can be removed as well.
    pub fn compute_token_preserialized_with_partitioner(
        &self,
        partitioner: &PartitionerName,
        serialized_partition_key: &SerializedValues,
    ) -> Result<Token, ClusterStateTokenError> {
        calculate_token_for_partition_key(serialized_partition_key, partitioner)
            .map_err(ClusterStateTokenError::TokenCalculation)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cluster::metadata::{Metadata, Peer};
    use crate::cluster::node::NodeAddr;
    use crate::policies::host_filter::HostFilter;
    use crate::routing::locator::tablets::RawTablet;
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
            client_routes: None,
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

    /// Helper: a `NodeConfig` whose connectivity events go nowhere.
    fn make_node_config() -> NodeConfig {
        let (tx, _rx) = mpsc::unbounded_channel();
        NodeConfig {
            pool_config: Default::default(),
            used_keyspace: None,
            connectivity_events_sender: tx,
            metrics: Default::default(),
        }
    }

    /// Helper: build a ClusterState from metadata and an optional host filter.
    async fn new_cluster_state(
        metadata: Metadata,
        host_filter: Option<&dyn HostFilter>,
    ) -> ClusterState {
        ClusterState::new(metadata, &make_node_config(), host_filter).await
    }

    /// Helper: build a ClusterState from metadata, old state, and an optional host filter.
    async fn update_cluster_state(
        previous: &ClusterState,
        metadata: Metadata,
        host_filter: Option<&dyn HostFilter>,
    ) -> ClusterState {
        previous
            .new_updated(metadata, &make_node_config(), host_filter)
            .await
    }

    /// Helper: apply partially fetched metadata onto `previous`.
    async fn partially_update_cluster_state(
        previous: &ClusterState,
        peers: Option<Vec<Peer>>,
        schema: Option<SchemaUpdate>,
    ) -> ClusterState {
        previous
            .new_with_partial_changes(peers, schema, &make_node_config(), None)
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
        let initial_state = new_cluster_state(initial_metadata, Some(&filter)).await;
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
        let new_state = update_cluster_state(&initial_state, new_metadata, Some(&filter)).await;
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
        let initial_state = new_cluster_state(initial_metadata, Some(&filter)).await;
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
        let new_state = update_cluster_state(&initial_state, new_metadata, Some(&filter)).await;
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
        let initial_state = new_cluster_state(initial_metadata, Some(&filter)).await;
        let old_node = initial_state.known_nodes().get(&host_id).unwrap().clone();
        assert!(!old_node.is_enabled(), "node should be disabled");

        // Refresh with identical attributes, still filtered out.
        let new_metadata = make_metadata(vec![make_peer(host_id, addr, Some("dc1"), Some("r1"))]);
        let new_state = update_cluster_state(&initial_state, new_metadata, Some(&filter)).await;
        let new_node = new_state.known_nodes().get(&host_id).unwrap();

        assert!(!new_node.is_enabled(), "node should still be disabled");
        assert!(
            Arc::ptr_eq(&old_node, new_node),
            "Node object should be reused when disabled node's attributes haven't changed"
        );
    }

    #[tokio::test]
    async fn shares_enabled_node_with_detects_carried_over_nodes() {
        setup_tracing();

        let host_id = Uuid::new_v4();
        let peer = || make_peer(host_id, make_addr(1), Some("dc1"), Some("r1"));
        let state = new_cluster_state(make_metadata(vec![peer()]), None).await;

        // The same peer again: the node object is reused.
        let same = update_cluster_state(&state, make_metadata(vec![peer()]), None).await;
        assert!(state.topology.shares_enabled_node_with(&same.topology));

        // A peer with a new host id, as after dummy initial metadata.
        let other_peer = make_peer(Uuid::new_v4(), make_addr(1), Some("dc1"), Some("r1"));
        let replaced = update_cluster_state(&state, make_metadata(vec![other_peer]), None).await;
        assert!(!state.topology.shares_enabled_node_with(&replaced.topology));

        // A shared node that is disabled does not count.
        let filter = AddrRejectFilter::rejecting([make_addr(1)]);
        let disabled = new_cluster_state(make_metadata(vec![peer()]), Some(&filter)).await;
        let same_disabled =
            update_cluster_state(&disabled, make_metadata(vec![peer()]), Some(&filter)).await;
        assert!(
            !disabled
                .topology
                .shares_enabled_node_with(&same_disabled.topology)
        );
    }

    // A re-read peer list that describes the current topology exactly must
    // share it; one that differs in any way must build a new one.
    #[tokio::test]
    async fn partial_topology_update_shares_unchanged_topology() {
        setup_tracing();

        let host_ids = [Uuid::new_v4(), Uuid::new_v4()];
        let peer = |host_id: Uuid, addr: u16, tokens: Vec<i64>| Peer {
            tokens: tokens.into_iter().map(Token::new).collect(),
            ..make_peer(host_id, make_addr(addr), Some("dc1"), Some("r1"))
        };
        let peers = || {
            vec![
                peer(host_ids[0], 1, vec![1, 3]),
                peer(host_ids[1], 2, vec![2, 4]),
            ]
        };
        let state = new_cluster_state(make_metadata(peers()), None).await;

        let same = partially_update_cluster_state(&state, Some(peers()), None).await;
        assert!(
            Arc::ptr_eq(&state.topology, &same.topology),
            "the same peers again must share the topology"
        );

        let mut reordered = peers();
        reordered.reverse();
        let same = partially_update_cluster_state(&state, Some(reordered), None).await;
        assert!(
            Arc::ptr_eq(&state.topology, &same.topology),
            "the order of peers does not matter"
        );

        let mut token_moved = peers();
        token_moved[0].tokens[1] = Token::new(5);
        let changed = partially_update_cluster_state(&state, Some(token_moved), None).await;
        assert!(
            !Arc::ptr_eq(&state.topology, &changed.topology),
            "a node holding a different token changes the topology"
        );

        let mut added = peers();
        added.push(peer(Uuid::new_v4(), 3, vec![]));
        let changed = partially_update_cluster_state(&state, Some(added), None).await;
        assert!(
            !Arc::ptr_eq(&state.topology, &changed.topology),
            "a new node, even without tokens, changes the topology"
        );

        let mut removed = peers();
        removed.pop();
        let changed = partially_update_cluster_state(&state, Some(removed), None).await;
        assert!(
            !Arc::ptr_eq(&state.topology, &changed.topology),
            "a node gone changes the topology"
        );

        let mut moved = peers();
        moved[0].address = make_addr(9);
        let changed = partially_update_cluster_state(&state, Some(moved), None).await;
        assert!(
            !Arc::ptr_eq(&state.topology, &changed.topology),
            "a node with a new address changes the topology"
        );
    }

    // Tablet feedback that repeats what the state already holds must not
    // produce a new state; feedback that changes anything must.
    #[tokio::test]
    async fn tablet_update_is_skipped_when_nothing_changes() {
        setup_tracing();

        let host_id = Uuid::new_v4();
        let metadata = make_metadata(vec![make_peer(
            host_id,
            make_addr(1),
            Some("dc1"),
            Some("r1"),
        )]);
        let state = new_cluster_state(metadata, None).await;

        let table = TableSpec::borrowed("ks", "t").into_owned();
        let tablet = |first, last, shard, version| {
            (
                table.clone(),
                RawTablet::new_for_test(first, last, vec![(host_id, shard)], version),
            )
        };

        let state = state
            .with_updated_tablets(vec![tablet(0, 100, 0, Some(1))])
            .expect("a new tablet changes the state");

        assert!(
            state
                .with_updated_tablets(vec![tablet(0, 100, 0, Some(1))])
                .is_none(),
            "the same tablet again changes nothing"
        );
        assert!(
            state
                .with_updated_tablets(vec![tablet(0, 100, 0, Some(1)), tablet(0, 100, 0, Some(1))])
                .is_none(),
            "nor does a whole batch of it"
        );

        // Within one batch, a tablet is compared with the state as updated by
        // the batch so far: the overlapping tablet replaces the known one, and
        // the known one, arriving again, must replace it back.
        let reordered = state
            .with_updated_tablets(vec![
                tablet(50, 150, 0, Some(9)),
                tablet(0, 100, 0, Some(1)),
            ])
            .expect("the overlapping tablet changes the state");
        assert!(
            reordered
                .with_updated_tablets(vec![tablet(0, 100, 0, Some(1))])
                .is_none(),
            "the known tablet was applied last and is present"
        );
        assert!(
            reordered
                .with_updated_tablets(vec![tablet(50, 150, 0, Some(9))])
                .is_some(),
            "the overlapping tablet was replaced back"
        );

        // Any difference does change the state: version, shard, or range.
        for changed in [
            tablet(0, 100, 0, Some(2)),
            tablet(0, 100, 1, Some(1)),
            tablet(0, 50, 0, Some(1)),
        ] {
            assert!(state.with_updated_tablets(vec![changed]).is_some());
        }

        // A batch mixing a known tablet with a new one is applied.
        let state = state
            .with_updated_tablets(vec![
                tablet(0, 100, 0, Some(1)),
                tablet(101, 200, 0, Some(3)),
            ])
            .expect("the new tablet changes the state");
        assert!(
            state
                .with_updated_tablets(vec![tablet(101, 200, 0, Some(3))])
                .is_none()
        );
    }
}
