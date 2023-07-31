/// Cluster manages up to date information and connections to database nodes
use crate::frame::response::event::{Event, StatusChangeEvent};
use crate::frame::value::ValueList;
use crate::routing::Token;
use crate::transport::host_filter::HostFilter;
use crate::transport::{
    connection::{Connection, VerifiedKeyspaceName},
    connection_pool::PoolConfig,
    errors::QueryError,
    node::Node,
    partitioner::PartitionerName,
    topology::{Keyspace, Metadata, MetadataReader},
};

use arc_swap::ArcSwap;
use bytes::{BufMut, Bytes, BytesMut};
use futures::future::join_all;
use futures::{future::RemoteHandle, FutureExt};
use itertools::Itertools;
use scylla_cql::errors::BadQuery;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tracing::instrument::WithSubscriber;
use tracing::{debug, warn};
use uuid::Uuid;

use super::node::NodeAddr;

#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct ContactPoint {
    pub address: SocketAddr,
    pub datacenter: Option<String>,
}

use super::locator::ReplicaLocator;
use super::topology::Strategy;

/// Cluster manages up to date information and connections to database nodes.
/// All data can be accessed by cloning Arc<ClusterData> in the `data` field
pub(crate) struct Cluster {
    // `ArcSwap<ClusterData>` is wrapped in `Arc` to support sharing cluster data
    // between `Cluster` and `ClusterWorker`
    data: Arc<ArcSwap<ClusterData>>,

    refresh_channel: tokio::sync::mpsc::Sender<RefreshRequest>,
    use_keyspace_channel: tokio::sync::mpsc::Sender<UseKeyspaceRequest>,

    _worker_handle: RemoteHandle<()>,
}

/// Enables printing [Cluster] struct in a neat way, by skipping the rather useless
/// print of channels state and printing [ClusterData] neatly.
pub(crate) struct ClusterNeatDebug<'a>(pub(crate) &'a Cluster);
impl<'a> std::fmt::Debug for ClusterNeatDebug<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let cluster = self.0;
        f.debug_struct("Cluster")
            .field("data", &ClusterDataNeatDebug(&cluster.data.load()))
            .finish_non_exhaustive()
    }
}

#[derive(Clone, Debug)]
pub struct Datacenter {
    pub nodes: Vec<Arc<Node>>,
    pub rack_count: usize,
}

#[derive(Clone)]
pub struct ClusterData {
    pub(crate) known_peers: HashMap<Uuid, Arc<Node>>, // Invariant: nonempty after Cluster::new()
    pub(crate) keyspaces: HashMap<String, Keyspace>,
    pub(crate) locator: ReplicaLocator,
}

/// Enables printing [ClusterData] struct in a neat way, skipping the clutter involved by
/// [ClusterData::ring] being large and [Self::keyspaces] debug print being very verbose by default.
pub(crate) struct ClusterDataNeatDebug<'a>(pub(crate) &'a Arc<ClusterData>);
impl<'a> std::fmt::Debug for ClusterDataNeatDebug<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let cluster_data = &self.0;

        f.debug_struct("ClusterData")
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

// Works in the background to keep the cluster updated
struct ClusterWorker {
    // Cluster data to keep updated:
    cluster_data: Arc<ArcSwap<ClusterData>>,

    // Cluster connections
    metadata_reader: MetadataReader,
    pool_config: PoolConfig,

    // To listen for refresh requests
    refresh_channel: tokio::sync::mpsc::Receiver<RefreshRequest>,

    // Channel used to receive use keyspace requests
    use_keyspace_channel: tokio::sync::mpsc::Receiver<UseKeyspaceRequest>,

    // Channel used to receive server events
    server_events_channel: tokio::sync::mpsc::Receiver<Event>,

    // Keyspace send in "USE <keyspace name>" when opening each connection
    used_keyspace: Option<VerifiedKeyspaceName>,

    // The host filter determines towards which nodes we should open
    // connections
    host_filter: Option<Arc<dyn HostFilter>>,
}

#[derive(Debug)]
struct RefreshRequest {
    response_chan: tokio::sync::oneshot::Sender<Result<(), QueryError>>,
}

#[derive(Debug)]
struct UseKeyspaceRequest {
    keyspace_name: VerifiedKeyspaceName,
    response_chan: tokio::sync::oneshot::Sender<Result<(), QueryError>>,
}

impl Cluster {
    pub(crate) async fn new(
        initial_peers: Vec<ContactPoint>,
        pool_config: PoolConfig,
        keyspaces_to_fetch: Vec<String>,
        fetch_schema_metadata: bool,
        host_filter: Option<Arc<dyn HostFilter>>,
    ) -> Result<Cluster, QueryError> {
        let (refresh_sender, refresh_receiver) = tokio::sync::mpsc::channel(32);
        let (use_keyspace_sender, use_keyspace_receiver) = tokio::sync::mpsc::channel(32);
        let (server_events_sender, server_events_receiver) = tokio::sync::mpsc::channel(32);

        let mut metadata_reader = MetadataReader::new(
            initial_peers,
            pool_config.connection_config.clone(),
            pool_config.keepalive_interval,
            server_events_sender,
            keyspaces_to_fetch,
            fetch_schema_metadata,
            &host_filter,
        );

        let metadata = metadata_reader.read_metadata(true).await?;
        let cluster_data = ClusterData::new(
            metadata,
            &pool_config,
            &HashMap::new(),
            &None,
            host_filter.as_deref(),
        )
        .await;
        cluster_data.wait_until_all_pools_are_initialized().await;
        let cluster_data: Arc<ArcSwap<ClusterData>> =
            Arc::new(ArcSwap::from(Arc::new(cluster_data)));

        let worker = ClusterWorker {
            cluster_data: cluster_data.clone(),

            metadata_reader,
            pool_config,

            refresh_channel: refresh_receiver,
            server_events_channel: server_events_receiver,

            use_keyspace_channel: use_keyspace_receiver,
            used_keyspace: None,

            host_filter,
        };

        let (fut, worker_handle) = worker.work().remote_handle();
        tokio::spawn(fut.with_current_subscriber());

        let result = Cluster {
            data: cluster_data,
            refresh_channel: refresh_sender,
            use_keyspace_channel: use_keyspace_sender,
            _worker_handle: worker_handle,
        };

        Ok(result)
    }

    pub(crate) fn get_data(&self) -> Arc<ClusterData> {
        self.data.load_full()
    }

    pub(crate) async fn refresh_metadata(&self) -> Result<(), QueryError> {
        let (response_sender, response_receiver) = tokio::sync::oneshot::channel();

        self.refresh_channel
            .send(RefreshRequest {
                response_chan: response_sender,
            })
            .await
            .expect("Bug in Cluster::refresh_metadata sending");
        // Other end of this channel is in ClusterWorker, can't be dropped while we have &self to Cluster with _worker_handle

        response_receiver
            .await
            .expect("Bug in Cluster::refresh_metadata receiving")
        // ClusterWorker always responds
    }

    pub(crate) async fn use_keyspace(
        &self,
        keyspace_name: VerifiedKeyspaceName,
    ) -> Result<(), QueryError> {
        let (response_sender, response_receiver) = tokio::sync::oneshot::channel();

        self.use_keyspace_channel
            .send(UseKeyspaceRequest {
                keyspace_name,
                response_chan: response_sender,
            })
            .await
            .expect("Bug in Cluster::use_keyspace sending");
        // Other end of this channel is in ClusterWorker, can't be dropped while we have &self to Cluster with _worker_handle

        response_receiver.await.unwrap() // ClusterWorker always responds
    }

    /// Returns nonempty list of working connections to all shards
    pub(crate) async fn get_working_connections(&self) -> Result<Vec<Arc<Connection>>, QueryError> {
        let cluster_data: Arc<ClusterData> = self.get_data();
        let peers = &cluster_data.known_peers;

        let mut result: Vec<Arc<Connection>> = Vec::with_capacity(peers.len());

        let mut last_error: Option<QueryError> = None;

        for node in peers.values() {
            match node.get_working_connections() {
                Ok(conns) => result.extend(conns),
                Err(e) => last_error = Some(e),
            }
        }

        if result.is_empty() {
            return Err(last_error.unwrap()); // By invariant peers is nonempty
        }

        Ok(result)
    }
}

impl ClusterData {
    // Updates information about rack count in each datacenter
    fn update_rack_count(datacenters: &mut HashMap<String, Datacenter>) {
        for datacenter in datacenters.values_mut() {
            datacenter.rack_count = datacenter
                .nodes
                .iter()
                .filter_map(|node| node.rack.as_ref())
                .unique()
                .count();
        }
    }

    pub(crate) async fn wait_until_all_pools_are_initialized(&self) {
        for node in self.locator.unique_nodes_in_global_ring().iter() {
            node.wait_until_pool_initialized().await;
        }
    }

    /// Creates new ClusterData using information about topology held in `metadata`.
    /// Uses provided `known_peers` hashmap to recycle nodes if possible.
    pub(crate) async fn new(
        metadata: Metadata,
        pool_config: &PoolConfig,
        known_peers: &HashMap<Uuid, Arc<Node>>,
        used_keyspace: &Option<VerifiedKeyspaceName>,
        host_filter: Option<&dyn HostFilter>,
    ) -> Self {
        // Create new updated known_peers and ring
        let mut new_known_peers: HashMap<Uuid, Arc<Node>> =
            HashMap::with_capacity(metadata.peers.len());
        let mut ring: Vec<(Token, Arc<Node>)> = Vec::new();
        let mut datacenters: HashMap<String, Datacenter> = HashMap::new();
        let mut all_nodes: Vec<Arc<Node>> = Vec::with_capacity(metadata.peers.len());

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

            if let Some(dc) = &node.datacenter {
                match datacenters.get_mut(dc) {
                    Some(v) => v.nodes.push(node.clone()),
                    None => {
                        let v = Datacenter {
                            nodes: vec![node.clone()],
                            rack_count: 0,
                        };
                        datacenters.insert(dc.clone(), v);
                    }
                }
            }

            for token in peer_tokens {
                ring.push((token, node.clone()));
            }

            all_nodes.push(node);
        }

        Self::update_rack_count(&mut datacenters);

        let keyspaces = metadata.keyspaces;
        let (locator, keyspaces) = tokio::task::spawn_blocking(move || {
            let keyspace_strategies = keyspaces.values().map(|ks| &ks.strategy);
            let locator = ReplicaLocator::new(ring.into_iter(), keyspace_strategies);
            (locator, keyspaces)
        })
        .await
        .unwrap();

        ClusterData {
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

    /// Access datacenter details collected by the driver
    /// Returned `HashMap` is indexed by names of datacenters
    pub fn get_datacenters_info(&self) -> HashMap<String, Datacenter> {
        self.locator
            .datacenter_names()
            .iter()
            .map(|dc_name| {
                let nodes = self
                    .locator
                    .unique_nodes_in_datacenter_ring(dc_name)
                    .unwrap()
                    .to_vec();
                let rack_count = nodes.iter().map(|node| node.rack.as_ref()).unique().count();

                (dc_name.clone(), Datacenter { nodes, rack_count })
            })
            .collect()
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
        partition_key: impl ValueList,
    ) -> Result<Token, BadQuery> {
        let partitioner = self
            .keyspaces
            .get(keyspace)
            .and_then(|k| k.tables.get(table))
            .and_then(|t| t.partitioner.as_deref())
            .and_then(PartitionerName::from_str)
            .unwrap_or_default();
        let serialized_values = partition_key.serialized()?;
        // Null values are skipped in computation; null values in partition key are unsound,
        // but it is consistent with computation of prepared statements token.
        let serialized_pk = match serialized_values.len() {
            0 => None,
            1 => serialized_values
                .iter()
                .next()
                .unwrap()
                .map(Bytes::copy_from_slice),
            _ => {
                let mut buf = BytesMut::new();
                for value in serialized_values.iter().flatten() {
                    let value_size = value
                        .len()
                        .try_into()
                        .map_err(|_| BadQuery::ValuesTooLongForKey(value.len(), u16::MAX.into()))?;
                    buf.put_u16(value_size);
                    buf.extend_from_slice(value);
                    buf.put_u8(0);
                }
                Some(buf.into())
            }
        };
        Ok(partitioner.hash(&serialized_pk.unwrap_or_default()))
    }

    /// Access to replicas owning a given token
    pub fn get_token_endpoints(&self, keyspace: &str, token: Token) -> Vec<Arc<Node>> {
        self.get_token_endpoints_iter(keyspace, token)
            .cloned()
            .collect()
    }

    pub(crate) fn get_token_endpoints_iter(
        &self,
        keyspace: &str,
        token: Token,
    ) -> impl Iterator<Item = &Arc<Node>> {
        let keyspace = self.keyspaces.get(keyspace);
        let strategy = keyspace
            .map(|k| &k.strategy)
            .unwrap_or(&Strategy::LocalStrategy);
        let replica_set = self
            .replica_locator()
            .replicas_for_token(token, strategy, None);

        replica_set.into_iter()
    }

    /// Access to replicas owning a given partition key (similar to `nodetool getendpoints`)
    pub fn get_endpoints(
        &self,
        keyspace: &str,
        table: &str,
        partition_key: impl ValueList,
    ) -> Result<Vec<Arc<Node>>, BadQuery> {
        Ok(self.get_token_endpoints(
            keyspace,
            self.compute_token(keyspace, table, partition_key)?,
        ))
    }

    /// Access replica location info
    pub fn replica_locator(&self) -> &ReplicaLocator {
        &self.locator
    }
}

impl ClusterWorker {
    pub(crate) async fn work(mut self) {
        use tokio::time::Instant;

        let refresh_duration = Duration::from_secs(60); // Refresh topology every 60 seconds
        let mut last_refresh_time = Instant::now();

        loop {
            let mut cur_request: Option<RefreshRequest> = None;

            // Wait until it's time for the next refresh
            let sleep_until: Instant = last_refresh_time
                .checked_add(refresh_duration)
                .unwrap_or_else(Instant::now);

            let sleep_future = tokio::time::sleep_until(sleep_until);
            tokio::pin!(sleep_future);

            tokio::select! {
                _ = sleep_future => {},
                recv_res = self.refresh_channel.recv() => {
                    match recv_res {
                        Some(request) => cur_request = Some(request),
                        None => return, // If refresh_channel was closed then cluster was dropped, we can stop working
                    }
                }
                recv_res = self.server_events_channel.recv() => {
                    if let Some(event) = recv_res {
                        debug!("Received server event: {:?}", event);
                        match event {
                            Event::TopologyChange(_) => (), // Refresh immediately
                            Event::StatusChange(status) => {
                                // If some node went down/up, update it's marker and refresh
                                // later as planned.

                                match status {
                                    StatusChangeEvent::Down(addr) => self.change_node_down_marker(addr, true),
                                    StatusChangeEvent::Up(addr) => self.change_node_down_marker(addr, false),
                                }
                                continue;
                            },
                            _ => continue, // Don't go to refreshing
                        }
                    } else {
                        // If server_events_channel was closed, than TopologyReader was dropped,
                        // so we can probably stop working too
                        return;
                    }
                }
                recv_res = self.use_keyspace_channel.recv() => {
                    match recv_res {
                        Some(request) => {
                            self.used_keyspace = Some(request.keyspace_name.clone());

                            let cluster_data = self.cluster_data.load_full();
                            let use_keyspace_future = Self::handle_use_keyspace_request(cluster_data, request);
                            tokio::spawn(use_keyspace_future.with_current_subscriber());
                        },
                        None => return, // If use_keyspace_channel was closed then cluster was dropped, we can stop working
                    }

                    continue; // Don't go to refreshing, wait for the next event
                }
            }

            // Perform the refresh
            debug!("Requesting topology refresh");
            last_refresh_time = Instant::now();
            let refresh_res = self.perform_refresh().await;

            // Send refresh result if there was a request
            if let Some(request) = cur_request {
                // We can ignore sending error - if no one waits for the response we can drop it
                let _ = request.response_chan.send(refresh_res);
            }
        }
    }

    fn change_node_down_marker(&mut self, addr: SocketAddr, is_down: bool) {
        let cluster_data = self.cluster_data.load_full();

        // We need to iterate through the whole map here, but there will rarely more than ~100 nodes,
        // and changes of down marker are infrequent enough to afford this. As an important tradeoff,
        // we only keep one hashmap of known_peers, which is indexed by host IDs for node identification.
        let node = match cluster_data
            .known_peers
            .values()
            .find(|&peer| peer.address == NodeAddr::Translatable(addr))
        {
            Some(node) => node,
            None => {
                warn!("Unknown node address {}", addr);
                return;
            }
        };

        node.change_down_marker(is_down);
    }

    async fn handle_use_keyspace_request(
        cluster_data: Arc<ClusterData>,
        request: UseKeyspaceRequest,
    ) {
        let result = Self::send_use_keyspace(cluster_data, &request.keyspace_name).await;

        // Don't care if nobody wants request result
        let _ = request.response_chan.send(result);
    }

    async fn send_use_keyspace(
        cluster_data: Arc<ClusterData>,
        keyspace_name: &VerifiedKeyspaceName,
    ) -> Result<(), QueryError> {
        let mut use_keyspace_futures = Vec::new();

        for node in cluster_data.known_peers.values() {
            let fut = node.use_keyspace(keyspace_name.clone());
            use_keyspace_futures.push(fut);
        }

        let use_keyspace_results: Vec<Result<(), QueryError>> =
            join_all(use_keyspace_futures).await;

        // If there was at least one Ok and the rest were IoErrors we can return Ok
        // keyspace name is correct and will be used on broken connection on the next reconnect

        // If there were only IoErrors then return IoError
        // If there was an error different than IoError return this error - something is wrong

        let mut was_ok: bool = false;
        let mut io_error: Option<Arc<std::io::Error>> = None;

        for result in use_keyspace_results {
            match result {
                Ok(()) => was_ok = true,
                Err(err) => match err {
                    QueryError::IoError(io_err) => io_error = Some(io_err),
                    _ => return Err(err),
                },
            }
        }

        if was_ok {
            return Ok(());
        }

        // We can unwrap io_error because use_keyspace_futures must be nonempty
        Err(QueryError::IoError(io_error.unwrap()))
    }

    async fn perform_refresh(&mut self) -> Result<(), QueryError> {
        // Read latest Metadata
        let metadata = self.metadata_reader.read_metadata(false).await?;
        let cluster_data: Arc<ClusterData> = self.cluster_data.load_full();

        let new_cluster_data = Arc::new(
            ClusterData::new(
                metadata,
                &self.pool_config,
                &cluster_data.known_peers,
                &self.used_keyspace,
                self.host_filter.as_deref(),
            )
            .await,
        );

        new_cluster_data
            .wait_until_all_pools_are_initialized()
            .await;

        self.update_cluster_data(new_cluster_data);

        Ok(())
    }

    fn update_cluster_data(&mut self, new_cluster_data: Arc<ClusterData>) {
        self.cluster_data.store(new_cluster_data);
    }
}
