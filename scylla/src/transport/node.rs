use uuid::Uuid;

/// Node represents a cluster node along with it's data and connections
use crate::routing::{Shard, Sharder, Token};
use crate::transport::connection::Connection;
use crate::transport::connection::VerifiedKeyspaceName;
use crate::transport::connection_pool::{NodeConnectionPool, PoolConfig};
use crate::transport::errors::QueryError;

use std::fmt::Display;
use std::net::IpAddr;
use std::{
    hash::{Hash, Hasher},
    net::SocketAddr,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
};

use super::topology::{PeerEndpoint, UntranslatedEndpoint};

/// This enum is introduced to support address translation only upon opening a connection,
/// as well as to cope with a bug present in older Cassandra and Scylla releases.
/// The bug involves misconfiguration of rpc_address and/or broadcast_rpc_address
/// in system.local to 0.0.0.0. Mitigation involves replacing the faulty address
/// with connection's address, but then that address must not be subject to `AddressTranslator`,
/// so we carry that information using this enum. Address translation is never performed
/// on `Untranslatable` variant.
#[non_exhaustive]
#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq, PartialOrd, Ord)]
pub enum NodeAddr {
    /// Fetched in Metadata with `query_peers()` (broadcast by a node itself).
    Translatable(SocketAddr),
    /// Built from control connection's address upon `query_peers()` in order to mitigate the bug described above.
    Untranslatable(SocketAddr),
}

impl NodeAddr {
    pub(crate) fn into_inner(self) -> SocketAddr {
        match self {
            NodeAddr::Translatable(addr) | NodeAddr::Untranslatable(addr) => addr,
        }
    }
    pub(crate) fn inner_mut(&mut self) -> &mut SocketAddr {
        match self {
            NodeAddr::Translatable(addr) | NodeAddr::Untranslatable(addr) => addr,
        }
    }
    pub fn ip(&self) -> IpAddr {
        self.into_inner().ip()
    }
    pub fn port(&self) -> u16 {
        self.into_inner().port()
    }
}

impl Display for NodeAddr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.into_inner())
    }
}

/// Node represents a cluster node along with it's data and connections
///
/// Note: if a Node changes its broadcast address, then it is not longer
/// represented by the same instance of Node struct, but instead
/// a new instance is created (for implementation reasons).
#[derive(Debug)]
pub struct Node {
    pub host_id: Uuid,
    pub address: NodeAddr,
    pub datacenter: Option<String>,
    pub rack: Option<String>,

    // If the node is filtered out by the host filter, this will be None
    pool: Option<NodeConnectionPool>,

    down_marker: AtomicBool,
}

/// A way that Nodes are often passed and accessed in the driver's code.
pub type NodeRef<'a> = &'a Arc<Node>;

impl Node {
    /// Creates new node which starts connecting in the background
    /// # Arguments
    ///
    /// `address` - address to connect to
    /// `compression` - preferred compression to use
    /// `datacenter` - optional datacenter name
    /// `rack` - optional rack name
    pub(crate) fn new(
        peer: PeerEndpoint,
        pool_config: PoolConfig,
        keyspace_name: Option<VerifiedKeyspaceName>,
        enabled: bool,
    ) -> Self {
        let host_id = peer.host_id;
        let address = peer.address;
        let datacenter = peer.datacenter.clone();
        let rack = peer.rack.clone();
        let pool = enabled.then(|| {
            NodeConnectionPool::new(UntranslatedEndpoint::Peer(peer), pool_config, keyspace_name)
        });

        Node {
            host_id,
            address,
            datacenter,
            rack,
            pool,
            down_marker: false.into(),
        }
    }

    /// Recreates a Node after it changes its IP, preserving the pool.
    ///
    /// All settings except address are inherited from `node`.
    /// The underlying pool is preserved and notified about the IP change.
    /// # Arguments
    ///
    /// `node` - previous definition of that node
    /// `address` - new address to connect to
    pub(crate) fn inherit_with_ip_changed(node: &Node, endpoint: PeerEndpoint) -> Self {
        let address = endpoint.address;
        if let Some(ref pool) = node.pool {
            pool.update_endpoint(endpoint);
        }
        Self {
            address,
            down_marker: false.into(),
            datacenter: node.datacenter.clone(),
            rack: node.rack.clone(),
            host_id: node.host_id,
            pool: node.pool.clone(),
        }
    }

    pub fn sharder(&self) -> Option<Sharder> {
        self.pool.as_ref()?.sharder()
    }

    pub(crate) fn shard_for_token(&self, token: Token) -> Option<Shard> {
        self.sharder().map(|sharder| sharder.shard_of(token))
    }

    /// Get connection which should be used to connect using given token
    /// If this connection is broken get any random connection to this Node
    pub(crate) async fn connection_for_token(
        &self,
        token: Token,
    ) -> Result<Arc<Connection>, QueryError> {
        self.get_pool()?.connection_for_token(token)
    }

    /// Get random connection
    pub(crate) async fn random_connection(&self) -> Result<Arc<Connection>, QueryError> {
        self.get_pool()?.random_connection()
    }

    pub fn is_down(&self) -> bool {
        self.down_marker.load(Ordering::Relaxed)
    }

    /// Returns a boolean which indicates whether this node was is enabled.
    /// Only enabled nodes will have connections open. For disabled nodes,
    /// no connections will be opened.
    pub fn is_enabled(&self) -> bool {
        self.pool.is_some()
    }

    pub(crate) fn change_down_marker(&self, is_down: bool) {
        self.down_marker.store(is_down, Ordering::Relaxed);
    }

    pub(crate) async fn use_keyspace(
        &self,
        keyspace_name: VerifiedKeyspaceName,
    ) -> Result<(), QueryError> {
        if let Some(pool) = &self.pool {
            pool.use_keyspace(keyspace_name).await?;
        }
        Ok(())
    }

    pub(crate) fn get_working_connections(&self) -> Result<Vec<Arc<Connection>>, QueryError> {
        self.get_pool()?.get_working_connections()
    }

    pub(crate) async fn wait_until_pool_initialized(&self) {
        if let Some(pool) = &self.pool {
            pool.wait_until_initialized().await;
        }
    }

    fn get_pool(&self) -> Result<&NodeConnectionPool, QueryError> {
        self.pool.as_ref().ok_or_else(|| {
            QueryError::IoError(Arc::new(std::io::Error::new(
                std::io::ErrorKind::Other,
                "No connections in the pool: the node has been disabled \
                by the host filter",
            )))
        })
    }
}

impl PartialEq for Node {
    fn eq(&self, other: &Self) -> bool {
        self.host_id == other.host_id
    }
}

impl Eq for Node {}

impl Hash for Node {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.host_id.hash(state);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    impl Node {
        pub(crate) fn new_for_test(
            address: NodeAddr,
            datacenter: Option<String>,
            rack: Option<String>,
        ) -> Self {
            Self {
                host_id: Uuid::new_v4(),
                address,
                datacenter,
                rack,
                pool: None,
                down_marker: false.into(),
            }
        }
    }
}
