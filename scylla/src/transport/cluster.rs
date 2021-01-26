/// Cluster manages up to date information and connections to database nodes
use crate::routing::Token;
use crate::transport::connection::{Connection, ConnectionConfig};
use crate::transport::errors::QueryError;
use crate::transport::node::{Node, NodeConnections};
use crate::transport::topology::{Keyspace, TopologyReader};

use futures::{future::RemoteHandle, FutureExt};
use std::collections::{BTreeMap, HashMap};
use std::net::SocketAddr;
use std::sync::{Arc, RwLock};

/// Cluster manages up to date information and connections to database nodes.
/// All data can be accessed by cloning Arc<ClusterData> in the `data` field
pub struct Cluster {
    data: Arc<RwLock<Arc<ClusterData>>>,

    refresh_channel: tokio::sync::mpsc::Sender<RefreshRequest>,
    _worker_handle: RemoteHandle<()>,
}

pub struct ClusterData {
    pub known_peers: HashMap<SocketAddr, Arc<Node>>, // Invariant: nonempty after Cluster::new()
    pub ring: BTreeMap<Token, Arc<Node>>,            // Invariant: nonempty after Cluster::new()
    pub keyspaces: HashMap<String, Keyspace>,
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
}

#[derive(Debug)]
struct RefreshRequest {
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
        })));

        let (refresh_sender, refresh_receiver) = tokio::sync::mpsc::channel(32);

        let worker = ClusterWorker {
            cluster_data: cluster_data.clone(),

            topology_reader: TopologyReader::new(initial_peers, connection_config.clone()),
            connection_config,

            refresh_channel: refresh_receiver,
        };

        let (fut, worker_handle) = worker.work().remote_handle();
        tokio::spawn(fut);

        let result = Cluster {
            data: cluster_data,
            refresh_channel: refresh_sender,
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
            match &*node.connections.read().await {
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

    async fn perform_refresh(&mut self) -> Result<(), QueryError> {
        // Read latest TopologyInfo
        let topo_info = self.topology_reader.read_topology_info().await?;

        // Create new updated known_peers and ring
        let mut new_known_peers: HashMap<SocketAddr, Arc<Node>> =
            HashMap::with_capacity(topo_info.peers.len());
        let mut new_ring: BTreeMap<Token, Arc<Node>> = BTreeMap::new();

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
                )),
            };

            new_known_peers.insert(peer.address, node.clone());

            for token in peer.tokens {
                new_ring.insert(token, node.clone());
            }
        }

        let mut new_cluster_data: Arc<ClusterData> = Arc::new(ClusterData {
            known_peers: new_known_peers,
            ring: new_ring,
            keyspaces: topo_info.keyspaces,
        });

        // Update current cluster_data
        // Use std::mem::swap to avoid dropping large structures with active write lock
        let mut cluster_data_lock = self.cluster_data.write().unwrap();

        std::mem::swap(&mut *cluster_data_lock, &mut new_cluster_data);

        drop(cluster_data_lock);

        Ok(())
    }
}
