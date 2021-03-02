/// Node represents a cluster node along with it's data and connections
use crate::routing::{Shard, ShardInfo, Token};
use crate::transport::connection::VerifiedKeyspaceName;
use crate::transport::connection::{Connection, ConnectionConfig};
use crate::transport::connection_keeper::{ConnectionKeeper, ShardInfoSender};
use crate::transport::errors::QueryError;
use futures::future::join_all;

use futures::{future::RemoteHandle, FutureExt};
use rand::Rng;
use std::net::SocketAddr;
use std::sync::{Arc, RwLock};

/// Node represents a cluster node along with it's data and connections
pub struct Node {
    pub address: SocketAddr,
    pub datacenter: Option<String>,
    pub rack: Option<String>,

    pub connections: Arc<RwLock<Arc<NodeConnections>>>,

    use_keyspace_channel: tokio::sync::mpsc::Sender<UseKeyspaceRequest>,

    _worker_handle: RemoteHandle<()>,
}

pub enum NodeConnections {
    /// Non shard-aware ex. a Cassandra node connection
    Single(ConnectionKeeper),
    /// Shard aware Scylla node connections
    Sharded {
        shard_info: ShardInfo,
        /// shard_conns always contains shard_info.nr_shards ConnectionKeepers
        shard_conns: Vec<ConnectionKeeper>,
    },
}

// Works in the background to detect ShardInfo changes and keep node connections updated
struct NodeWorker {
    node_conns: Arc<RwLock<Arc<NodeConnections>>>,
    node_addr: SocketAddr,
    connection_config: ConnectionConfig,

    shard_info_sender: ShardInfoSender,
    shard_info_receiver: tokio::sync::watch::Receiver<Option<ShardInfo>>,

    // Channel used to receive use keyspace requests
    use_keyspace_channel: tokio::sync::mpsc::Receiver<UseKeyspaceRequest>,

    // Keyspace send in "USE <keyspace name>" when opening each connection
    used_keyspace: Option<VerifiedKeyspaceName>,
}

#[derive(Debug)]
struct UseKeyspaceRequest {
    keyspace_name: VerifiedKeyspaceName,
    response_chan: tokio::sync::oneshot::Sender<Result<(), QueryError>>,
}

impl Node {
    /// Creates new node which starts connecting in the background
    /// # Arguments
    ///
    /// `address` - address to connect to
    /// `compression` - preferred compression to use
    /// `datacenter` - optional datacenter name
    /// `rack` - optional rack name
    pub fn new(
        address: SocketAddr,
        connection_config: ConnectionConfig,
        datacenter: Option<String>,
        rack: Option<String>,
        keyspace_name: Option<VerifiedKeyspaceName>,
    ) -> Self {
        let (shard_info_sender, shard_info_receiver) = tokio::sync::watch::channel(None);

        let shard_info_sender = Arc::new(std::sync::Mutex::new(shard_info_sender));

        let (use_keyspace_sender, use_keyspace_receiver) = tokio::sync::mpsc::channel(32);

        let connections = Arc::new(RwLock::new(Arc::new(NodeConnections::Single(
            ConnectionKeeper::new(
                address,
                connection_config.clone(),
                None,
                Some(shard_info_sender.clone()),
                keyspace_name.clone(),
            ),
        ))));

        let worker = NodeWorker {
            node_conns: connections.clone(),
            node_addr: address,
            connection_config,
            shard_info_sender,
            shard_info_receiver,
            use_keyspace_channel: use_keyspace_receiver,
            used_keyspace: keyspace_name,
        };

        let (fut, worker_handle) = worker.work().remote_handle();
        tokio::spawn(fut);

        Node {
            address,
            datacenter,
            rack,
            connections,
            use_keyspace_channel: use_keyspace_sender,
            _worker_handle: worker_handle,
        }
    }

    /// Get connection which should be used to connect using given token
    pub async fn connection_for_token(&self, token: Token) -> Result<Arc<Connection>, QueryError> {
        let connections: Arc<NodeConnections> = self.connections.read().unwrap().clone();

        match &*connections {
            NodeConnections::Single(conn_keeper) => conn_keeper.get_connection().await,
            NodeConnections::Sharded {
                shard_info,
                shard_conns,
            } => {
                let shard: Shard = shard_info.shard_of(token);
                // It's guaranteed by NodeWorker that for each shard < shard_info.nr_shards
                // there is an entry for this shard in connections map
                shard_conns[shard as usize].get_connection().await
            }
        }
    }

    /// Get random connection
    pub async fn random_connection(&self) -> Result<Arc<Connection>, QueryError> {
        let connections: Arc<NodeConnections> = self.connections.read().unwrap().clone();

        match &*connections {
            NodeConnections::Single(conn_keeper) => conn_keeper.get_connection().await,
            NodeConnections::Sharded {
                shard_info,
                shard_conns,
            } => {
                let shard = rand::thread_rng().gen_range(0..shard_info.nr_shards as u32);
                // It's guaranteed by NodeWorker that shard_conns.len() == shard_info.nr_shards
                shard_conns[shard as usize].get_connection().await
            }
        }
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
            .expect("Bug in Node::use_keyspace sending");
        // Other end of this channel is in NodeWorker, can't be dropped while we have &self to Node with _worker_handle

        response_receiver.await.unwrap() // NodeWorker always responds
    }
}

impl NodeWorker {
    pub async fn work(mut self) {
        let mut cur_shard_info: Option<ShardInfo> = self.shard_info_receiver.borrow().clone();

        loop {
            tokio::select! {
                // Wait for current shard_info to change
                changed_res = self.shard_info_receiver.changed() => {
                    // We own one sending end of this channel so it can't return None
                    changed_res.expect("Bug in NodeWorker::work")
                    // Then go to resharding update
                },
                // Wait for a use_keyspace request
                recv_res = self.use_keyspace_channel.recv() => {
                    match recv_res {
                        Some(request) => {
                            self.used_keyspace = Some(request.keyspace_name.clone());

                            let node_conns = self.node_conns.read().unwrap().clone();
                            let use_keyspace_future = Self::handle_use_keyspace_request(node_conns, request);
                            tokio::spawn(use_keyspace_future);
                        },
                        None => return,
                    }

                    continue; // Don't go to resharding update, wait for the next event
                },
            }

            let new_shard_info: Option<ShardInfo> = self.shard_info_receiver.borrow().clone();

            // See if the node has resharded
            match (&cur_shard_info, &new_shard_info) {
                (Some(cur), Some(new)) => {
                    if cur.nr_shards == new.nr_shards && cur.msb_ignore == new.msb_ignore {
                        // Nothing chaged, go back to waiting for a change
                        continue;
                    }
                }
                (None, None) => continue, // Nothing chaged, go back to waiting for a change
                _ => {}
            }

            cur_shard_info = new_shard_info;

            // We received updated node ShardInfo
            // Create new node connections. It will happen rarely so we can probably afford it
            // TODO: Maybe save some connections instead of recreating?
            let new_connections: NodeConnections = match &cur_shard_info {
                None => NodeConnections::Single(ConnectionKeeper::new(
                    self.node_addr,
                    self.connection_config.clone(),
                    None,
                    Some(self.shard_info_sender.clone()),
                    self.used_keyspace.clone(),
                )),
                Some(shard_info) => {
                    let mut connections: Vec<ConnectionKeeper> =
                        Vec::with_capacity(shard_info.nr_shards as usize);

                    for shard in 0..shard_info.nr_shards {
                        let mut cur_conn_shard_info = shard_info.clone();
                        cur_conn_shard_info.shard = shard;
                        let cur_conn = ConnectionKeeper::new(
                            self.node_addr,
                            self.connection_config.clone(),
                            Some(cur_conn_shard_info),
                            Some(self.shard_info_sender.clone()),
                            self.used_keyspace.clone(),
                        );

                        connections.push(cur_conn);
                    }

                    NodeConnections::Sharded {
                        shard_info: shard_info.clone(),
                        shard_conns: connections,
                    }
                }
            };

            let mut new_connections_to_swap = Arc::new(new_connections);

            // Update node.connections
            // Use std::mem::swap to minimalize time spent holding write lock
            let mut node_conns_lock = self.node_conns.write().unwrap();
            std::mem::swap(&mut *node_conns_lock, &mut new_connections_to_swap);
            drop(node_conns_lock);
        }
    }

    async fn handle_use_keyspace_request(
        node_conns: Arc<NodeConnections>,
        request: UseKeyspaceRequest,
    ) {
        let result = Self::send_use_keyspace(node_conns, &request.keyspace_name).await;

        // Don't care if nobody wants request result
        let _ = request.response_chan.send(result);
    }

    async fn send_use_keyspace(
        node_conns: Arc<NodeConnections>,
        keyspace_name: &VerifiedKeyspaceName,
    ) -> Result<(), QueryError> {
        let mut use_keyspace_futures = Vec::new();

        match &*node_conns {
            NodeConnections::Single(conn_keeper) => {
                let fut = conn_keeper.use_keyspace(keyspace_name.clone());
                use_keyspace_futures.push(fut);
            }
            NodeConnections::Sharded { shard_conns, .. } => {
                for conn_keeper in shard_conns {
                    let fut = conn_keeper.use_keyspace(keyspace_name.clone());
                    use_keyspace_futures.push(fut);
                }
            }
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
}
