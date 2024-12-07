use crate::network::{Connection, PoolConfig, VerifiedKeyspaceName};
use crate::prepared_statement::TokenCalculationError;
use crate::routing::{Shard, Token};
use crate::transport::errors::{BadQuery, QueryError};
use crate::transport::host_filter::HostFilter;
use crate::transport::locator::tablets::{RawTablet, Tablet, TabletsInfo};
use crate::transport::locator::ReplicaLocator;
use crate::transport::partitioner::calculate_token_for_partition_key;
use crate::transport::partitioner::PartitionerName;

use itertools::Itertools;
use scylla_cql::frame::response::result::TableSpec;
use scylla_cql::types::serialize::row::SerializedValues;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tracing::debug;
use uuid::Uuid;

use super::metadata::{Keyspace, Metadata, Strategy};
use super::node::{Node, NodeRef};

#[derive(Clone)]
pub struct ClusterState {
    pub(crate) known_peers: HashMap<Uuid, Arc<Node>>, // Invariant: nonempty after Cluster::new()
    pub(crate) keyspaces: HashMap<String, Keyspace>,
    pub(crate) locator: ReplicaLocator,
}

/// Enables printing [ClusterState] struct in a neat way, skipping the clutter involved by
/// [ClusterState::ring] being large and [Self::keyspaces] debug print being very verbose by default.
pub(crate) struct ClusterStateNeatDebug<'a>(pub(crate) &'a Arc<ClusterState>);
impl std::fmt::Debug for ClusterStateNeatDebug<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let cluster_data = &self.0;

        f.debug_struct("ClusterState")
            .field("known_peers", &cluster_data.known_peers)
            .field("ring", {
                struct RingSizePrinter(usize);
                impl std::fmt::Debug for RingSizePrinter {
                    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(f, "<size={}>", self.0)
                    }
                }
                &RingSizePrinter(cluster_data.locator.ring().len())
            })
            .field("keyspaces", &cluster_data.keyspaces.keys())
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
    pub(crate) async fn new(
        metadata: Metadata,
        pool_config: &PoolConfig,
        known_peers: &HashMap<Uuid, Arc<Node>>,
        used_keyspace: &Option<VerifiedKeyspaceName>,
        host_filter: Option<&dyn HostFilter>,
        mut tablets: TabletsInfo,
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
                        node.clone()
                    } else {
                        // If IP changes, the Node struct is recreated, but the underlying pool is preserved and notified about the IP change.
                        Arc::new(Node::inherit_with_ip_changed(node, peer_endpoint))
                    }
                }
                _ => {
                    let is_enabled = host_filter.map_or(true, |f| f.accept(&peer));
                    let (peer_endpoint, tokens) = peer.into_peer_endpoint_and_tokens();
                    peer_tokens = tokens;
                    Arc::new(Node::new(
                        peer_endpoint,
                        pool_config.clone(),
                        used_keyspace.clone(),
                        is_enabled,
                    ))
                }
            };

            new_known_peers.insert(peer_host_id, node.clone());

            for token in peer_tokens {
                ring.push((token, node.clone()));
            }
        }

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
                if let Some(ks) = metadata.keyspaces.get(spec.ks_name()) {
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

        let keyspaces = metadata.keyspaces;
        let (locator, keyspaces) = tokio::task::spawn_blocking(move || {
            let keyspace_strategies = keyspaces.values().map(|ks| &ks.strategy);
            let locator = ReplicaLocator::new(ring.into_iter(), keyspace_strategies, tablets);
            (locator, keyspaces)
        })
        .await
        .unwrap();

        ClusterState {
            known_peers: new_known_peers,
            keyspaces,
            locator,
        }
    }

    /// Access keyspaces details collected by the driver
    /// Driver collects various schema details like tables, partitioners, columns, types.
    /// They can be read using this method
    pub fn get_keyspace_info(&self) -> &HashMap<String, Keyspace> {
        &self.keyspaces
    }

    /// Access details about nodes known to the driver
    pub fn get_nodes_info(&self) -> &[Arc<Node>] {
        self.locator.unique_nodes_in_global_ring()
    }

    /// Compute token of a table partition key
    pub fn compute_token(
        &self,
        keyspace: &str,
        table: &str,
        partition_key: &SerializedValues,
    ) -> Result<Token, BadQuery> {
        let partitioner = self
            .keyspaces
            .get(keyspace)
            .and_then(|k| k.tables.get(table))
            .and_then(|t| t.partitioner.as_deref())
            .and_then(PartitionerName::from_str)
            .unwrap_or_default();

        calculate_token_for_partition_key(partition_key, &partitioner).map_err(|err| match err {
            TokenCalculationError::ValueTooLong(values_len) => {
                BadQuery::ValuesTooLongForKey(values_len, u16::MAX.into())
            }
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
    ) -> impl Iterator<Item = (NodeRef<'_>, Shard)> {
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
    pub fn get_endpoints(
        &self,
        keyspace: &str,
        table: &str,
        partition_key: &SerializedValues,
    ) -> Result<Vec<(Arc<Node>, Shard)>, BadQuery> {
        let token = self.compute_token(keyspace, table, partition_key)?;
        Ok(self.get_token_endpoints(keyspace, table, token))
    }

    /// Access replica location info
    pub fn replica_locator(&self) -> &ReplicaLocator {
        &self.locator
    }

    /// Returns nonempty iterator of working connections to all shards.
    pub(crate) fn iter_working_connections(
        &self,
    ) -> Result<impl Iterator<Item = Arc<Connection>> + '_, QueryError> {
        // The returned iterator is nonempty by nonemptiness invariant of `self.known_peers`.
        assert!(!self.known_peers.is_empty());
        let mut peers_iter = self.known_peers.values();

        // First we try to find the first working pool of connections.
        // If none is found, return error.
        let first_working_pool = peers_iter
            .by_ref()
            .map(|node| node.get_working_connections())
            .find_or_first(Result::is_ok)
            .expect("impossible: known_peers was asserted to be nonempty")?;

        let remaining_pools_iter = peers_iter
            .map(|node| node.get_working_connections())
            .flatten_ok()
            .flatten();

        Ok(first_working_pool.into_iter().chain(remaining_pools_iter))
        // By an invariant `self.known_peers` is nonempty, so the returned iterator
        // is nonempty, too.
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
                    debug!("Nodes ({}) that are replicas for a tablet {{ks: {}, table: {}, range: [{}. {}]}} not present in current ClusterState.known_peers. \
                       Skipping these replicas until topology refresh",
                       f.iter().format(", "), table.ks_name(), table.table_name(), t.range().0.value(), t.range().1.value());
                    t
                }
            };
            self.locator.tablets.add_tablet(table, tablet);
        }
    }
}
