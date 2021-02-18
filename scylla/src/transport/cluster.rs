/// Cluster manages up to date information and connections to database nodes
use crate::routing::Token;
use crate::transport::connection::{Connection, ConnectionConfig, VerifiedKeyspaceName};
use crate::transport::errors::QueryError;
use crate::transport::node::{Node, NodeConnections};
use crate::transport::topology::{Keyspace, TopologyReader};

use futures::future::join_all;
use futures::{future::RemoteHandle, FutureExt};
use std::collections::{BTreeMap, HashMap};
use std::net::SocketAddr;
use std::sync::{Arc, RwLock};

/// Cluster manages up to date information and connections to database nodes.
/// All data can be accessed by cloning Arc<ClusterData> in the `data` field
pub struct Cluster {
    data: Arc<RwLock<Arc<ClusterData>>>,

    refresh_channel: tokio::sync::mpsc::Sender<RefreshRequest>,
    use_keyspace_channel: tokio::sync::mpsc::Sender<UseKeyspaceRequest>,

    _worker_handle: RemoteHandle<()>,
}

pub struct ClusterData {
    pub known_peers: HashMap<SocketAddr, Arc<Node>>, // Invariant: nonempty after Cluster::new()
    pub ring: BTreeMap<Token, Arc<Node>>,            // Invariant: nonempty after Cluster::new()
    pub keyspaces: HashMap<String, Keyspace>,
    pub all_nodes: Vec<Arc<Node>>,
    pub datacenters: HashMap<String, Vec<Arc<Node>>>,
}

// Works in the background to keep the cluster updated
struct ClusterWorker {
    // Cluster data to keep updated:
    cluster_data: Arc<RwLock<Arc<ClusterData>>>,

    // Cluster connections
    topology_reader: TopologyReader,
    connection_config: ConnectionConfig,

    // To listen for refresh requests
    refresh_channel: tokio::sync::mpsc::Receiver<RefreshRequest>,

    // Channel used to receive use keyspace requests
    use_keyspace_channel: tokio::sync::mpsc::Receiver<UseKeyspaceRequest>,

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
        connection_config: ConnectionConfig,
    ) -> Result<Cluster, QueryError> {
        let cluster_data = Arc::new(RwLock::new(Arc::new(ClusterData {
            known_peers: HashMap::new(),
            ring: BTreeMap::new(),
            keyspaces: HashMap::new(),
            all_nodes: Vec::new(),
            datacenters: HashMap::new(),
        })));

        let (refresh_sender, refresh_receiver) = tokio::sync::mpsc::channel(32);

        let (use_keyspace_sender, use_keyspace_receiver) = tokio::sync::mpsc::channel(32);

        let worker = ClusterWorker {
            cluster_data: cluster_data.clone(),

            topology_reader: TopologyReader::new(initial_peers, connection_config.clone()),
            connection_config,

            refresh_channel: refresh_receiver,

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

        result.refresh_topology().await?;

        Ok(result)
    }

    pub fn get_data(&self) -> Arc<ClusterData> {
        self.data.read().unwrap().clone()
    }

    pub async fn refresh_topology(&self) -> Result<(), QueryError> {
        let (response_sender, response_receiver) = tokio::sync::oneshot::channel();

        self.refresh_channel
            .send(RefreshRequest {
                response_chan: response_sender,
            })
            .await
            .expect("Bug in Cluster::refresh_topology sending");
        // Other end of this channel is in ClusterWorker, can't be dropped while we have &self to Cluster with _worker_handle

        response_receiver
            .await
            .expect("Bug in Cluster::refresh_topology receiving")
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

        // Takes result of ConnectionKeeper::get_connection() and pushes it onto result list or sets last_error
        let mut push_to_result = |get_conn_res: Result<Arc<Connection>, QueryError>| {
            match get_conn_res {
                Ok(conn) => result.push(conn),
                Err(e) => last_error = Some(e),
            };
        };

        for node in peers.values() {
            let connections: Arc<NodeConnections> = node.connections.read().unwrap().clone();

            match &*connections {
                NodeConnections::Single(conn_keeper) => {
                    push_to_result(conn_keeper.get_connection().await)
                }
                NodeConnections::Sharded { shard_conns, .. } => {
                    for conn_keeper in shard_conns {
                        push_to_result(conn_keeper.get_connection().await);
                    }
                }
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
    pub fn ring_range<'a>(&'a self, t: &Token) -> impl Iterator<Item = Arc<Node>> + 'a {
        let before_wrap = self.ring.range(t..).map(|(_token, node)| node.clone());
        let after_wrap = self.ring.values().cloned();

        before_wrap.chain(after_wrap).take(self.ring.len())
    }
}

impl ClusterWorker {
    pub async fn work(mut self) {
        use tokio::time::{Duration, Instant};

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
                recv_res = self.use_keyspace_channel.recv() => {
                    match recv_res {
                        Some(request) => {
                            self.used_keyspace = Some(request.keyspace_name.clone());

                            let cluster_data = self.cluster_data.read().unwrap().clone();
                            let use_keyspace_future = Self::handle_use_keyspace_request(cluster_data, request);
                            tokio::spawn(use_keyspace_future);
                        },
                        None => return, // If use_keyspace_channel was closed then cluster was dropped, we can stop working
                    }

                    continue; // Don't go to refreshing, wait for the next event
                }
            }

            // Perform the refresh
            last_refresh_time = Instant::now();
            let refresh_res = self.perform_refresh().await;

            // Send refresh result if there was a request
            if let Some(request) = cur_request {
                // We can ignore sending error - if no one waits for the response we can drop it
                let _ = request.response_chan.send(refresh_res);
            }
        }
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

        // If there was at least one Ok and the rest were IOErrors we can return Ok
        // keyspace name is correct and will be used on broken connection on the next reconnect

        // If there were only IOErrors then return IOError
        // If there was an error different than IOError return this error - something is wrong

        let mut was_ok: bool = false;
        let mut io_error: Option<Arc<std::io::Error>> = None;

        for result in use_keyspace_results {
            match result {
                Ok(()) => was_ok = true,
                Err(err) => match err {
                    QueryError::IOError(io_err) => io_error = Some(io_err),
                    _ => return Err(err),
                },
            }
        }

        if was_ok {
            return Ok(());
        }

        // We can unwrap io_error because use_keyspace_futures must be nonempty
        return Err(QueryError::IOError(io_error.unwrap()));
    }

    async fn perform_refresh(&mut self) -> Result<(), QueryError> {
        // Read latest TopologyInfo
        let topo_info = self.topology_reader.read_topology_info().await?;

        // Create new updated known_peers and ring
        let mut new_known_peers: HashMap<SocketAddr, Arc<Node>> =
            HashMap::with_capacity(topo_info.peers.len());
        let mut new_ring: BTreeMap<Token, Arc<Node>> = BTreeMap::new();
        let mut new_datacenters: HashMap<String, Vec<Arc<Node>>> = HashMap::new();

        let cluster_data: Arc<ClusterData> = self.cluster_data.read().unwrap().clone();

        for peer in topo_info.peers {
            // Take existing Arc<Node> if possible, otherwise create new one
            // Changing rack/datacenter but not ip address seems improbable
            // so we can just create new node and connections then
            let node: Arc<Node> = match cluster_data.known_peers.get(&peer.address) {
                Some(node) if node.datacenter == peer.datacenter && node.rack == peer.rack => {
                    node.clone()
                }
                _ => Arc::new(Node::new(
                    peer.address,
                    self.connection_config.clone(),
                    peer.datacenter,
                    peer.rack,
                    self.used_keyspace.clone(),
                )),
            };

            new_known_peers.insert(peer.address, node.clone());

            if let Some(dc) = &node.datacenter {
                match new_datacenters.get_mut(dc) {
                    Some(v) => v.push(node.clone()),
                    None => {
                        let v = vec![node.clone()];
                        new_datacenters.insert(dc.clone(), v);
                    }
                }
            }

            for token in peer.tokens {
                new_ring.insert(token, node.clone());
            }
        }

        let all_nodes = new_known_peers
            .values()
            .cloned()
            .collect::<Vec<Arc<Node>>>();

        let mut new_cluster_data: Arc<ClusterData> = Arc::new(ClusterData {
            known_peers: new_known_peers,
            ring: new_ring,
            keyspaces: topo_info.keyspaces,
            all_nodes,
            datacenters: new_datacenters,
        });

        // Update current cluster_data
        // Use std::mem::swap to avoid dropping large structures with active write lock
        let mut cluster_data_lock = self.cluster_data.write().unwrap();

        std::mem::swap(&mut *cluster_data_lock, &mut new_cluster_data);

        drop(cluster_data_lock);

        Ok(())
    }
}
