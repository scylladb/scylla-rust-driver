use tokio::net::{ToSocketAddrs, lookup_host};
use tracing::warn;
use uuid::Uuid;

use crate::errors::{ConnectionPoolError, DnsLookupError, UseKeyspaceError};
use crate::network::VerifiedKeyspaceName;
use crate::network::{Connection, ConnectivityChangeEvent};
use crate::network::{NodeConnectionPool, PoolConfig};
use crate::observability::metrics::Metrics;
/// Node represents a cluster node along with it's data and connections
use crate::routing::{Shard, Sharder};

use std::fmt::Display;
use std::net::IpAddr;
#[cfg(test)]
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use std::{
    hash::{Hash, Hasher},
    net::SocketAddr,
    sync::Arc,
};

use crate::cluster::metadata::{PeerEndpoint, UntranslatedEndpoint};

/// This enum is introduced to support address translation only upon opening a connection.
///
/// Address translation is never performed on `Untranslatable` variant, which is intended for
/// contact points. The `Translatable` variant is used for addresses broadcast by nodes themselves.
///
/// Historically, this enum had another use: to cope with a bug present in older Cassandra and ScyllaDB
/// releases: <https://github.com/scylladb/scylladb/issues/11201>. The bug involved misconfiguration
/// of rpc_address and/or broadcast_rpc_address in system.local to 0.0.0.0. Mitigation involved
/// replacing the faulty address with connection's address, but then that address had to not be subject
/// to `AddressTranslator`, so we carried that information using this enum.
#[non_exhaustive]
#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq, PartialOrd, Ord)]
pub enum NodeAddr {
    /// Fetched in Metadata with `query_peers()` (broadcast by a node itself).
    Translatable(SocketAddr),
    /// Stores contact points, because they are provided as already translated addresses.
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
    /// IP address of the node.
    ///
    /// Keep in mind that this discards the information about whether the address is translatable or not.
    /// Don't be surprised if you get a `Translatable` address here and won't be able to reach a node using it,
    /// because the node might be reachable through a different address, which must be obtained by translation.
    pub fn ip(&self) -> IpAddr {
        self.into_inner().ip()
    }

    /// Port of the node.
    pub fn port(&self) -> u16 {
        self.into_inner().port()
    }
}

impl Display for NodeAddr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.into_inner())
    }
}

/// Node represents a cluster node along with its data and connections
///
/// Note: if a Node changes its broadcast address, then it is not longer
/// represented by the same instance of Node struct, but instead
/// a new instance is created (for implementation reasons).
#[derive(Debug)]
pub struct Node {
    /// Unique identifier of the node.
    pub host_id: Uuid,
    /// Address of the node, which is used to connect to it.
    /// This address is either the one broadcast by the node itself
    /// (`NodeAddr::Translatable`) or the one used to connect to it
    /// in the first place if it's a contact point (`NodeAddr::Untranslatable`).
    pub address: NodeAddr,
    /// Datacenter of the node, if known.
    pub datacenter: Option<String>,
    /// Rack of the node, if known.
    pub rack: Option<String>,

    /// Connection pool for this node.
    ///
    /// If the node is filtered out by the host filter, this will be [None].
    pool: Option<NodeConnectionPool>,

    // In unit tests Node objects are mocked, and don't have real connection
    // pools. We want DefaultPolicy to use is_connected to filter out nodes,
    // but it would mean that all nodes would be filtered out in unit tests.

    // This field allows using is_enabled as a result of is_connected. Tests can
    // utilize this to simulate node being connected.
    #[cfg(test)]
    enabled_as_connected: AtomicBool,
}

/// A way that Nodes are often passed and accessed in the driver's code.
pub type NodeRef<'a> = &'a Arc<Node>;

impl Node {
    /// Creates a new node which starts connecting in the background.
    pub(crate) fn new(
        peer: PeerEndpoint,
        pool_config: &PoolConfig,
        connectivity_events_sender: tokio::sync::mpsc::UnboundedSender<ConnectivityChangeEvent>,
        keyspace_name: Option<VerifiedKeyspaceName>,
        metrics: Metrics,
    ) -> Self {
        let host_id = peer.host_id;
        let address = peer.address;
        let datacenter = peer.datacenter.clone();
        let rack = peer.rack.clone();

        // We aren't interested in the fact that the pool becomes empty, so we immediately drop the receiving part.
        let (pool_empty_notifier, _) = tokio::sync::mpsc::channel(1);
        let pool = NodeConnectionPool::new(
            UntranslatedEndpoint::Peer(peer),
            pool_config,
            Some((host_id, connectivity_events_sender)),
            keyspace_name,
            pool_empty_notifier,
            metrics,
        );

        Node {
            host_id,
            address,
            datacenter,
            rack,
            pool: Some(pool),
            #[cfg(test)]
            enabled_as_connected: AtomicBool::new(false),
        }
    }

    pub(crate) fn new_disabled(peer: PeerEndpoint) -> Self {
        let host_id = peer.host_id;
        let address = peer.address;
        let datacenter = peer.datacenter.clone();
        let rack = peer.rack.clone();

        Node {
            host_id,
            address,
            datacenter,
            rack,
            pool: None,
            #[cfg(test)]
            enabled_as_connected: AtomicBool::new(false),
        }
    }

    /// Recreates a Node after it changes its IP, preserving the pool.
    ///
    /// All settings except address are inherited from `node`.
    /// The underlying pool is preserved and notified about the IP change.
    /// # Arguments
    ///
    /// - `node` - previous definition of that node
    /// - `address` - new address to connect to
    pub(crate) fn inherit_with_ip_changed(node: &Node, endpoint: PeerEndpoint) -> Self {
        let address = endpoint.address;
        if let Some(ref pool) = node.pool {
            pool.update_endpoint(endpoint);
        }
        Self {
            address,
            datacenter: node.datacenter.clone(),
            rack: node.rack.clone(),
            host_id: node.host_id,
            pool: node.pool.clone(),
            #[cfg(test)]
            enabled_as_connected: AtomicBool::new(node.enabled_as_connected.load(Ordering::SeqCst)),
        }
    }

    /// Retrieves the sharder for this node, if it has one.
    ///
    /// If the node is disabled (i.e., it has no connection pool),
    /// or the node is not sharded (i.e., it's not a ScyllaDB node), this will return `None`.
    ///
    /// If the node [is enabled](Self::is_enabled) and does not have a sharder,
    /// this means it's not a ScyllaDB node.
    pub fn sharder(&self) -> Option<Sharder> {
        self.pool.as_ref()?.sharder()
    }

    /// Get a connection targetting the given shard
    /// If such connection is broken, get any random connection to this `Node`
    pub(crate) async fn connection_for_shard(
        &self,
        shard: Shard,
    ) -> Result<Arc<Connection>, ConnectionPoolError> {
        self.get_pool()?.connection_for_shard(shard)
    }

    /// Returns true if the driver has any open connections in the pool for this
    /// node.
    pub fn is_connected(&self) -> bool {
        #[cfg(test)]
        if self.enabled_as_connected.load(Ordering::SeqCst) {
            return self.is_enabled();
        }
        let Ok(pool) = self.get_pool() else {
            return false;
        };
        pool.is_connected()
    }

    /// Returns a boolean which indicates whether this node was is enabled.
    /// Only enabled nodes will have connections open. For disabled nodes,
    /// no connections will be opened.
    pub fn is_enabled(&self) -> bool {
        self.pool.is_some()
    }

    /// Signals the node's connection pool to retry connecting immediately,
    /// resetting its exponential backoff.
    ///
    /// This is a no-op if the node has no pool (disabled by host filter).
    pub(crate) fn trigger_pool_refill(&self) {
        if let Some(pool) = &self.pool {
            pool.trigger_immediate_refill();
        }
    }

    pub(crate) async fn use_keyspace(
        &self,
        keyspace_name: VerifiedKeyspaceName,
    ) -> Result<(), UseKeyspaceError> {
        if let Some(pool) = &self.pool {
            pool.use_keyspace(keyspace_name).await?;
        }
        Ok(())
    }

    pub(crate) fn get_working_connections(
        &self,
    ) -> Result<Vec<Arc<Connection>>, ConnectionPoolError> {
        self.get_pool()?.get_working_connections()
    }

    pub(crate) fn get_random_connection(&self) -> Result<Arc<Connection>, ConnectionPoolError> {
        self.get_pool()?.random_connection()
    }

    pub(crate) async fn wait_until_pool_initialized(&self) {
        if let Some(pool) = &self.pool {
            pool.wait_until_initialized().await;
        }
    }

    fn get_pool(&self) -> Result<&NodeConnectionPool, ConnectionPoolError> {
        self.pool
            .as_ref()
            .ok_or(ConnectionPoolError::NodeDisabledByHostFilter)
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
    /// A node identified by its hostname.
    Hostname(String),
    /// A node identified by its IP address + a port.
    Address(SocketAddr),
}

/// Describes a database server known on Session startup, with already resolved address.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct ResolvedContactPoint {
    pub(crate) address: SocketAddr,
}

/// Performs a DNS lookup with provided optional timeout.
async fn lookup_host_with_timeout(
    host: impl ToSocketAddrs,
    hostname_resolution_timeout: Option<Duration>,
) -> Result<impl Iterator<Item = SocketAddr>, DnsLookupError> {
    if let Some(timeout) = hostname_resolution_timeout {
        match tokio::time::timeout(timeout, lookup_host(host)).await {
            Ok(res) => res.map_err(|io_err| DnsLookupError::IoError(Arc::new(io_err))),
            // Elapsed error from tokio library does not provide any context.
            Err(_) => Err(DnsLookupError::Timeout(timeout.as_millis())),
        }
    } else {
        lookup_host(host)
            .await
            .map_err(|io_err| DnsLookupError::IoError(Arc::new(io_err)))
    }
}

// Resolve the given hostname using a DNS lookup if necessary.
// The resolution may return multiple IPs, all of which are returned in the
// order provided by the resolver (without deduplication or reordering).
pub(crate) async fn resolve_hostname(
    hostname: &str,
    hostname_resolution_timeout: Option<Duration>,
) -> Result<Vec<SocketAddr>, DnsLookupError> {
    // When passing String to `lookup_host`, it expects it to be in the form "hostname:port".
    // If it is not, error will be returned immediately. In this case, we want to perform
    // check with (hostname, default_port) with the same timeout.
    // If the first check ended with timeout, there is no point in second check, because
    // reason for failure is not connected to the lack of default port.
    // There may be other errors than timeout and invalid value, but I don't really see
    // any harm in trying again in such cases.
    let addrs = match lookup_host_with_timeout(hostname, hostname_resolution_timeout).await {
        Ok(addrs) => itertools::Either::Left(addrs),
        Err(DnsLookupError::Timeout(t)) => return Err(DnsLookupError::Timeout(t)),
        // Use a default port in case of error, but propagate the original error on failure
        Err(e) => {
            let addrs = lookup_host_with_timeout((hostname, 9042), hostname_resolution_timeout)
                .await
                .or(Err(e))?;
            itertools::Either::Right(addrs)
        }
    };

    let addrs: Vec<SocketAddr> = addrs.collect();
    if addrs.is_empty() {
        Err(DnsLookupError::EmptyAddressListForHost(hostname.into()))
    } else {
        Ok(addrs)
    }
}

/// Removes duplicate contact points that share the same socket address.
///
/// The same address may be reached via multiple hostnames, or via both a
/// literal [`KnownNode::Address`] and a hostname. Deduplication avoids creating
/// redundant control-connection endpoints, which would otherwise waste retry
/// attempts and bias the random initial-endpoint choice.
///
/// Note that the obtained order (sorted by address) is irrelevant,
/// because the driver randomizes the order of contact points before trying to connect.
fn dedup_contact_points(mut peers: Vec<ResolvedContactPoint>) -> Vec<ResolvedContactPoint> {
    peers.sort_unstable();
    peers.dedup();

    peers
}

/// Transforms the given [`KnownNode`]s into [`ResolvedContactPoint`]s.
///
/// In case of a hostname, resolves it using a DNS lookup, producing one
/// [`ResolvedContactPoint`] per resolved address.
/// In case of a plain IP address, parses it and uses straight.
pub(crate) async fn resolve_contact_points(
    known_nodes: &[KnownNode],
    hostname_resolution_timeout: Option<Duration>,
) -> (Vec<ResolvedContactPoint>, Vec<String>) {
    resolve_contact_points_inner(known_nodes, async move |hostname| {
        resolve_hostname(&hostname, hostname_resolution_timeout).await
    })
    .await
}

/// Inner implementation of [`resolve_contact_points`], generic over the DNS
/// resolver so that it can be tested deterministically without real DNS lookups.
///
/// Each hostname is expanded to *all* of its resolved addresses, so a single
/// [`KnownNode::Hostname`] may produce multiple [`ResolvedContactPoint`]s.
/// The resulting contact points are deduplicated by address
/// (see [`dedup_contact_points`]).
async fn resolve_contact_points_inner(
    known_nodes: &[KnownNode],
    // This is generic only to allow mocking in unit tests; the real resolver is `resolve_hostname`.
    resolve: impl AsyncFn(String) -> Result<Vec<SocketAddr>, DnsLookupError>,
) -> (Vec<ResolvedContactPoint>, Vec<String>) {
    // Find IP addresses of all known nodes passed in the config
    let mut initial_peers: Vec<ResolvedContactPoint> = Vec::with_capacity(known_nodes.len());

    let mut to_resolve: Vec<&str> = Vec::new();
    let mut hostnames: Vec<String> = Vec::new();

    for node in known_nodes.iter() {
        match node {
            KnownNode::Hostname(hostname) => {
                to_resolve.push(hostname.as_str());
                hostnames.push(hostname.clone());
            }
            KnownNode::Address(address) => {
                initial_peers.push(ResolvedContactPoint { address: *address })
            }
        };
    }
    let resolve_futures =
        to_resolve
            .into_iter()
            .map(async |hostname| match resolve(hostname.to_string()).await {
                Ok(addresses) => addresses
                    .into_iter()
                    .map(|address| ResolvedContactPoint { address })
                    .collect::<Vec<_>>(),
                Err(e) => {
                    warn!("Hostname resolution failed for {}: {}", hostname, &e);
                    Vec::new()
                }
            });
    let resolved: Vec<Vec<ResolvedContactPoint>> = futures::future::join_all(resolve_futures).await;
    initial_peers.extend(resolved.into_iter().flatten());

    let initial_peers = dedup_contact_points(initial_peers);

    (initial_peers, hostnames)
}

#[cfg(test)]
mod tests {
    use super::*;

    impl Node {
        pub(crate) fn new_for_test(
            id: Option<Uuid>,
            address: Option<NodeAddr>,
            datacenter: Option<String>,
            rack: Option<String>,
        ) -> Self {
            Self {
                host_id: id.unwrap_or(Uuid::new_v4()),
                address: address.unwrap_or(NodeAddr::Translatable(SocketAddr::from((
                    [255, 255, 255, 255],
                    0,
                )))),
                datacenter,
                rack,
                pool: None,
                enabled_as_connected: AtomicBool::new(false),
            }
        }

        pub(crate) fn use_enabled_as_connected(&self) {
            self.enabled_as_connected.store(true, Ordering::SeqCst);
        }
    }

    use std::net::{Ipv4Addr, Ipv6Addr};

    fn v4(last_octet: u8, port: u16) -> SocketAddr {
        SocketAddr::new(IpAddr::V4(Ipv4Addr::new(10, 0, 0, last_octet)), port)
    }

    fn v6(port: u16) -> SocketAddr {
        SocketAddr::new(IpAddr::V6(Ipv6Addr::LOCALHOST), port)
    }

    // A single hostname resolving to multiple addresses must produce one
    // ResolvedContactPoint per address. Duplicate addresses are removed.
    #[tokio::test]
    async fn contact_points_expand_hostname_to_all_addresses() {
        let known = vec![KnownNode::Hostname("multi.example:9042".to_string())];
        // Resolver returns a duplicate and mixed families in arbitrary order.
        let resolved = vec![v4(1, 9042), v6(9042), v4(1, 9042), v4(2, 9042)];
        let resolved_addrs = resolved.clone();

        let (peers, hostnames) = resolve_contact_points_inner(&known, |_host| {
            let resolved_addrs = resolved_addrs.clone();
            async move { Ok(resolved_addrs) }
        })
        .await;

        let expected = [v4(1, 9042), v6(9042), v4(2, 9042)];
        assert_eq!(peers.len(), expected.len());
        for ex in expected {
            assert!(peers.contains(&ResolvedContactPoint { address: ex }));
        }
        assert_eq!(hostnames, vec!["multi.example:9042".to_string()]);
    }

    // Literal addresses are used as-is.
    // The `hostnames` output must list only the hostnames.
    #[tokio::test]
    async fn contact_points_mix_addresses_and_hostnames() {
        let literal = v4(100, 9042);
        let known = vec![
            KnownNode::Address(literal),
            KnownNode::Hostname("host.example:9042".to_string()),
        ];
        let host_addrs = vec![v4(1, 9042), v6(9042)];
        let resolved_addrs = host_addrs.clone();

        let (peers, hostnames) = resolve_contact_points_inner(&known, |_host| {
            let resolved_addrs = resolved_addrs.clone();
            async move { Ok(resolved_addrs) }
        })
        .await;

        let expected = [literal, v4(1, 9042), v6(9042)];
        assert_eq!(peers.len(), expected.len());
        for ex in expected {
            assert!(peers.contains(&ResolvedContactPoint { address: ex }))
        }
        assert_eq!(hostnames, vec!["host.example:9042".to_string()]);
    }

    // The same address reached via a literal contact point and via multiple
    // hostnames must appear only once.
    #[tokio::test]
    async fn contact_points_dedup_across_sources() {
        let shared = v4(1, 9042);
        let a_only = v6(9042);
        let b_only = v4(2, 9042);
        let known = vec![
            KnownNode::Address(shared),
            KnownNode::Hostname("a.example:9042".to_string()),
            KnownNode::Hostname("b.example:9042".to_string()),
        ];

        let (peers, _hostnames) = resolve_contact_points_inner(&known, async |host| {
            if host.starts_with('a') {
                Ok(vec![shared, a_only])
            } else {
                Ok(vec![shared, b_only])
            }
        })
        .await;

        let expected = [shared, a_only, b_only];
        assert_eq!(peers.len(), expected.len());
        for ex in expected {
            assert!(peers.contains(&ResolvedContactPoint { address: ex }))
        }
    }

    // A hostname whose resolution fails is skipped, while others are kept.
    #[tokio::test]
    async fn contact_points_skip_failed_resolution() {
        let known = vec![
            KnownNode::Hostname("good.example:9042".to_string()),
            KnownNode::Hostname("bad.example:9042".to_string()),
        ];
        let good = v4(1, 9042);

        let (peers, hostnames) = resolve_contact_points_inner(&known, async move |host| {
            if host.starts_with("good") {
                Ok(vec![good])
            } else {
                Err(DnsLookupError::Timeout(1))
            }
        })
        .await;

        let expected = [good];
        assert_eq!(peers.len(), expected.len());
        for ex in expected {
            assert!(peers.contains(&ResolvedContactPoint { address: ex }))
        }
        assert_eq!(
            hostnames,
            vec![
                "good.example:9042".to_string(),
                "bad.example:9042".to_string()
            ]
        );
    }

    // If all hostnames fail to resolve and there are no literal addresses,
    // the resulting contact point list is empty.
    #[tokio::test]
    async fn contact_points_all_failed_yields_empty() {
        let known = vec![
            KnownNode::Hostname("a.example:9042".to_string()),
            KnownNode::Hostname("b.example:9042".to_string()),
        ];

        let (peers, hostnames) =
            resolve_contact_points_inner(
                &known,
                |_host| async move { Err(DnsLookupError::Timeout(1)) },
            )
            .await;

        assert!(peers.is_empty());
        assert_eq!(
            hostnames,
            vec!["a.example:9042".to_string(), "b.example:9042".to_string()]
        );
    }

    // Duplicate addresses are removed.
    #[test]
    fn dedup_contact_points_removes_duplicate_addresses() {
        let a = v4(1, 9042);
        let b = v6(9042);
        let c = v4(2, 9042);
        let peers = vec![
            ResolvedContactPoint { address: a },
            ResolvedContactPoint { address: b },
            ResolvedContactPoint { address: a }, // duplicate of `a`
            ResolvedContactPoint { address: c },
            ResolvedContactPoint { address: b }, // duplicate of `b`
        ];

        let dedupped = dedup_contact_points(peers);
        assert_eq!(dedupped.len(), [a, b, c].len());
        for addr in [a, b, c] {
            assert!(dedupped.contains(&ResolvedContactPoint { address: addr }));
        }
    }

    // An empty input yields an empty output.
    #[test]
    fn dedup_contact_points_empty() {
        assert!(dedup_contact_points(Vec::new()).is_empty());
    }
}
