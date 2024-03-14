use tokio::net::lookup_host;
use tracing::warn;
use uuid::Uuid;

/// Node represents a cluster node along with it's data and connections
use crate::routing::{Shard, Sharder};
use crate::transport::connection::Connection;
use crate::transport::connection::VerifiedKeyspaceName;
use crate::transport::connection_pool::{NodeConnectionPool, PoolConfig};
use crate::transport::errors::QueryError;

use std::fmt::Display;
use std::io;
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

        // We aren't interested in the fact that the pool becomes empty, so we immediately drop the receiving part.
        let (pool_empty_notifier, _) = tokio::sync::broadcast::channel(1);
        let pool = enabled.then(|| {
            NodeConnectionPool::new(
                UntranslatedEndpoint::Peer(peer),
                pool_config,
                keyspace_name,
                pool_empty_notifier,
            )
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

    /// Get a connection targetting the given shard
    /// If such connection is broken, get any random connection to this `Node`
    pub(crate) async fn connection_for_shard(
        &self,
        shard: Shard,
    ) -> Result<Arc<Connection>, QueryError> {
        self.get_pool()?.connection_for_shard(shard)
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

/// Describes a database server known on `Session` startup.
///
/// The name derives from SessionBuilder's `known_node()` family of methods.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Debug)]
#[non_exhaustive]
pub enum KnownNode {
    Hostname(String),
    Address(SocketAddr),
    #[cfg(feature = "cloud")]
    CloudEndpoint(CloudEndpoint),
}

/// Describes a database server in the serverless Scylla Cloud.
#[cfg(feature = "cloud")]
#[non_exhaustive]
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Debug)]
pub struct CloudEndpoint {
    pub hostname: String,
    pub datacenter: String,
}

/// Describes a database server known on Session startup, with already resolved address.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct ResolvedContactPoint {
    pub address: SocketAddr,
    pub datacenter: Option<String>,
}

// Resolve the given hostname using a DNS lookup if necessary.
// The resolution may return multiple IPs and the function returns one of them.
// It prefers to return IPv4s first, and only if there are none, IPv6s.
pub(crate) async fn resolve_hostname(hostname: &str) -> Result<SocketAddr, io::Error> {
    let mut ret = None;
    let addrs: Vec<SocketAddr> = match lookup_host(hostname).await {
        Ok(addrs) => addrs.collect(),
        // Use a default port in case of error, but propagate the original error on failure
        Err(e) => lookup_host((hostname, 9042)).await.or(Err(e))?.collect(),
    };
    for a in addrs {
        match a {
            SocketAddr::V4(_) => return Ok(a),
            _ => {
                ret = Some(a);
            }
        }
    }

    ret.ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::Other,
            format!("Empty address list returned by DNS for {}", hostname),
        )
    })
}

/// Transforms the given [`KnownNode`]s into [`ContactPoint`]s.
///
/// In case of a hostname, resolves it using a DNS lookup.
/// In case of a plain IP address, parses it and uses straight.
pub(crate) async fn resolve_contact_points(
    known_nodes: &[KnownNode],
) -> (Vec<ResolvedContactPoint>, Vec<String>) {
    // Find IP addresses of all known nodes passed in the config
    let mut initial_peers: Vec<ResolvedContactPoint> = Vec::with_capacity(known_nodes.len());

    let mut to_resolve: Vec<(&String, Option<String>)> = Vec::new();
    let mut hostnames: Vec<String> = Vec::new();

    for node in known_nodes.iter() {
        match node {
            KnownNode::Hostname(hostname) => {
                to_resolve.push((hostname, None));
                hostnames.push(hostname.clone());
            }
            KnownNode::Address(address) => initial_peers.push(ResolvedContactPoint {
                address: *address,
                datacenter: None,
            }),
            #[cfg(feature = "cloud")]
            KnownNode::CloudEndpoint(CloudEndpoint {
                hostname,
                datacenter,
            }) => to_resolve.push((hostname, Some(datacenter.clone()))),
        };
    }
    let resolve_futures = to_resolve.iter().map(|(hostname, datacenter)| async move {
        match resolve_hostname(hostname).await {
            Ok(address) => Some(ResolvedContactPoint {
                address,
                datacenter: datacenter.clone(),
            }),
            Err(e) => {
                warn!("Hostname resolution failed for {}: {}", hostname, &e);
                None
            }
        }
    });
    let resolved: Vec<_> = futures::future::join_all(resolve_futures).await;
    initial_peers.extend(resolved.into_iter().flatten());

    (initial_peers, hostnames)
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
