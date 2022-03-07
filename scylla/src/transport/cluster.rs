use crate::frame::response::event::{Event, StatusChangeEvent};
/// Cluster manages up to date information and connections to database nodes
use crate::routing::Token;
use crate::transport::connection::{Connection, VerifiedKeyspaceName};
use crate::transport::connection_pool::PoolConfig;
use crate::transport::errors::QueryError;
use crate::transport::node::Node;
use crate::transport::topology::{Keyspace, Metadata, MetadataReader};

use arc_swap::ArcSwap;
use futures::future::join_all;
use futures::{future::RemoteHandle, FutureExt};
use itertools::Itertools;
use std::collections::{BTreeMap, HashMap};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tracing::{debug, warn};

/// Cluster manages up to date information and connections to database nodes.
/// All data can be accessed by cloning Arc<ClusterData> in the `data` field
pub struct Cluster {
    // `ArcSwap<ClusterData>` is wrapped in `Arc` to support sharing cluster data
    // between `Cluster` and `ClusterWorker`
    data: Arc<ArcSwap<ClusterData>>,

    refresh_channel: tokio::sync::mpsc::Sender<RefreshRequest>,
    use_keyspace_channel: tokio::sync::mpsc::Sender<UseKeyspaceRequest>,

    _worker_handle: RemoteHandle<()>,
}

#[derive(Clone)]
pub struct Datacenter {
    pub nodes: Vec<Arc<Node>>,
    pub rack_count: usize,
}

#[derive(Clone)]
pub struct ClusterData {
    pub(crate) known_peers: HashMap<SocketAddr, Arc<Node>>, // Invariant: nonempty after Cluster::new()
    pub(crate) ring: BTreeMap<Token, Arc<Node>>, // Invariant: nonempty after Cluster::new()
    pub(crate) keyspaces: HashMap<String, Keyspace>,
    pub(crate) all_nodes: Vec<Arc<Node>>,
    pub(crate) datacenters: HashMap<String, Datacenter>,
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
    pub async fn new(
        initial_peers: &[SocketAddr],
        pool_config: PoolConfig,
        fetch_schema_metadata: bool,
    ) -> Result<Cluster, QueryError> {
        let cluster_data = Arc::new(ArcSwap::from(Arc::new(ClusterData {
            known_peers: HashMap::new(),
            ring: BTreeMap::new(),
            keyspaces: HashMap::new(),
            all_nodes: Vec::new(),
            datacenters: HashMap::new(),
        })));

        let (refresh_sender, refresh_receiver) = tokio::sync::mpsc::channel(32);
        let (use_keyspace_sender, use_keyspace_receiver) = tokio::sync::mpsc::channel(32);
        let (server_events_sender, server_events_receiver) = tokio::sync::mpsc::channel(32);

        let worker = ClusterWorker {
            cluster_data: cluster_data.clone(),

            metadata_reader: MetadataReader::new(
                initial_peers,
                pool_config.connection_config.clone(),
                pool_config.keepalive_interval,
                server_events_sender,
                fetch_schema_metadata,
            ),
            pool_config,

            refresh_channel: refresh_receiver,
            server_events_channel: server_events_receiver,

            use_keyspace_channel: use_keyspace_receiver,
            used_keyspace: None,
        };

        let (fut, worker_handle) = worker.work().remote_handle();
        tokio::spawn(fut);

        let result = Cluster {
            data: cluster_data,
            refresh_channel: refresh_sender,
            use_keyspace_channel: use_keyspace_sender,
            _worker_handle: worker_handle,
        };

        result.refresh_metadata().await?;

        Ok(result)
    }

    pub fn get_data(&self) -> Arc<ClusterData> {
        self.data.load_full()
    }

    pub async fn refresh_metadata(&self) -> Result<(), QueryError> {
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

    pub async fn use_keyspace(
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
    pub async fn get_working_connections(&self) -> Result<Vec<Arc<Connection>>, QueryError> {
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
    /// Returns an iterator to the sequence of ends of vnodes, starting at the vnode in which t
    /// lies and going clockwise. Returned sequence has the same length as ring.
    pub(crate) fn ring_range<'a>(&'a self, t: &Token) -> impl Iterator<Item = Arc<Node>> + 'a {
        let before_wrap = self.ring.range(t..).map(|(_token, node)| node.clone());
        let after_wrap = self.ring.values().cloned();

        before_wrap.chain(after_wrap).take(self.ring.len())
    }

    // Updates information about rack count in each datacenter
    fn update_rack_count(datacenters: &mut HashMap<String, Datacenter>) {
        for datacenter in datacenters.values_mut() {
            datacenter.rack_count = datacenter
                .nodes
                .iter()
                .filter_map(|node| node.rack.clone())
                .unique()
                .count();
        }
    }

    pub(crate) async fn wait_until_all_pools_are_initialized(&self) {
        for node in self.all_nodes.iter() {
            node.wait_until_pool_initialized().await;
        }
    }

    /// Creates new ClusterData using information about topology held in `metadata`.
    /// Uses provided `known_peers` hashmap to recycle nodes if possible.
    pub(crate) fn new(
        metadata: Metadata,
        pool_config: &PoolConfig,
        known_peers: &HashMap<SocketAddr, Arc<Node>>,
        used_keyspace: &Option<VerifiedKeyspaceName>,
    ) -> Self {
        // Create new updated known_peers and ring
        let mut new_known_peers: HashMap<SocketAddr, Arc<Node>> =
            HashMap::with_capacity(metadata.peers.len());
        let mut ring: BTreeMap<Token, Arc<Node>> = BTreeMap::new();
        let mut datacenters: HashMap<String, Datacenter> = HashMap::new();
        let mut all_nodes: Vec<Arc<Node>> = Vec::with_capacity(metadata.peers.len());

        for peer in metadata.peers {
            // Take existing Arc<Node> if possible, otherwise create new one
            // Changing rack/datacenter but not ip address seems improbable
            // so we can just create new node and connections then
            let node: Arc<Node> = match known_peers.get(&peer.address) {
                Some(node) if node.datacenter == peer.datacenter && node.rack == peer.rack => {
                    node.clone()
                }
                _ => Arc::new(Node::new(
                    peer.address,
                    pool_config.clone(),
                    peer.datacenter,
                    peer.rack,
                    used_keyspace.clone(),
                )),
            };

            new_known_peers.insert(peer.address, node.clone());

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

            for token in peer.tokens {
                ring.insert(token, node.clone());
            }

            all_nodes.push(node);
        }

        Self::update_rack_count(&mut datacenters);

        ClusterData {
            known_peers: new_known_peers,
            ring,
            keyspaces: metadata.keyspaces,
            all_nodes,
            datacenters,
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
    pub fn get_datacenters_info(&self) -> &HashMap<String, Datacenter> {
        &self.datacenters
    }

    /// Access ring details collected by the driver
    pub fn get_ring_info(&self) -> &BTreeMap<Token, Arc<Node>> {
        &self.ring
    }

    /// Access details about nodes known to the driver
    pub fn get_nodes_info(&self) -> &Vec<Arc<Node>> {
        &self.all_nodes
    }
}

impl ClusterWorker {
    pub async fn work(mut self) {
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
                            tokio::spawn(use_keyspace_future);
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

        let node = match cluster_data.known_peers.get(&addr) {
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
        let metadata = self.metadata_reader.read_metadata().await?;
        let cluster_data: Arc<ClusterData> = self.cluster_data.load_full();

        let new_cluster_data = Arc::new(ClusterData::new(
            metadata,
            &self.pool_config,
            &cluster_data.known_peers,
            &self.used_keyspace,
        ));

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
