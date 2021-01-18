/// Node represents a cluster node along with it's data and connections
use crate::routing::{Shard, ShardInfo, Token};
use crate::transport::connection::{Connection, ConnectionConfig};
use crate::transport::connection_keeper::{ConnectionKeeper, ShardInfoSender};
use crate::transport::errors::QueryError;

use futures::{future::RemoteHandle, FutureExt};
use rand::Rng;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::sync::RwLock as AsyncRwLock;

/// Node represents a cluster node along with it's data and connections
pub struct Node {
    pub address: SocketAddr,
    pub datacenter: Option<String>,
    pub rack: Option<String>,

    pub connections: Arc<AsyncRwLock<NodeConnections>>,

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
    node_conns: Arc<AsyncRwLock<NodeConnections>>,
    node_addr: SocketAddr,
    connection_config: ConnectionConfig,

    shard_info_sender: ShardInfoSender,
    shard_info_receiver: tokio::sync::watch::Receiver<Option<ShardInfo>>,
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
    ) -> Self {
        let (shard_info_sender, shard_info_receiver) = tokio::sync::watch::channel(None);

        let shard_info_sender = Arc::new(std::sync::Mutex::new(shard_info_sender));

        let connections = Arc::new(AsyncRwLock::new(NodeConnections::Single(
            ConnectionKeeper::new(
                address,
                connection_config.clone(),
                None,
                Some(shard_info_sender.clone()),
            ),
        )));

        let worker = NodeWorker {
            node_conns: connections.clone(),
            node_addr: address,
            connection_config,
            shard_info_sender,
            shard_info_receiver,
        };

        let (fut, worker_handle) = worker.work().remote_handle();
        tokio::spawn(fut);

        Node {
            address,
            datacenter,
            rack,
            connections,
            _worker_handle: worker_handle,
        }
    }

    /// Get connection which should be used to connect using given token
    pub async fn connection_for_token(&self, token: Token) -> Result<Arc<Connection>, QueryError> {
        let connections = &*self.connections.read().await;

        match connections {
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
        let connections = &*self.connections.read().await;

        match connections {
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
}

impl NodeWorker {
    pub async fn work(mut self) {
        let mut cur_shard_info: Option<ShardInfo> = self.shard_info_receiver.borrow().clone();

        loop {
            // Wait for current shard_info to change
            // We own one sending end of this channel so it can't return None
            self.shard_info_receiver
                .changed()
                .await
                .expect("Bug in NodeWorker::work");
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
            let mut new_connections: NodeConnections = match &cur_shard_info {
                None => NodeConnections::Single(ConnectionKeeper::new(
                    self.node_addr,
                    self.connection_config.clone(),
                    None,
                    Some(self.shard_info_sender.clone()),
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
                        );
                        connections.push(cur_conn);
                    }

                    NodeConnections::Sharded {
                        shard_info: shard_info.clone(),
                        shard_conns: connections,
                    }
                }
            };

            // Update node.connections
            // Use std::mem::swap to minimalize time spent holding write lock
            let mut node_conns_lock = self.node_conns.write().await;
            std::mem::swap(&mut *node_conns_lock, &mut new_connections);
            drop(node_conns_lock);
        }
    }
}
