use super::errors::NewSessionError;
use super::load_balancing::LoadBalancingPolicy;
use super::session::{Session, SessionConfig};
use super::Compression;
use std::net::SocketAddr;

/// SessionBuilder is used to create new Session instances
/// # Example
///
/// ```
/// # use scylla::{Session, SessionBuilder};
/// # use scylla::transport::Compression;
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let session: Session = SessionBuilder::new()
///     .known_node("127.0.0.1:9042")
///     .compression(Some(Compression::Snappy))
///     .build()
///     .await?;
/// # Ok(())
/// # }
/// ```
pub struct SessionBuilder {
    pub config: SessionConfig,
}

impl SessionBuilder {
    /// Creates new SessionBuilder with default configuration
    /// # Default configuration
    /// * Compression: None
    ///
    pub fn new() -> Self {
        SessionBuilder {
            config: SessionConfig::new(),
        }
    }

    /// Add a known node with a hostname
    /// # Examples
    /// ```
    /// # use scylla::{Session, SessionBuilder};
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let session: Session = SessionBuilder::new().known_node("127.0.0.1:9042").build().await?;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// ```
    /// # use scylla::{Session, SessionBuilder};
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let session: Session = SessionBuilder::new().known_node("db1.example.com").build().await?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn known_node(mut self, hostname: impl AsRef<str>) -> Self {
        self.config.add_known_node(hostname);
        self
    }

    /// Add a known node with an IP address
    /// # Example
    /// ```
    /// # use scylla::{Session, SessionBuilder};
    /// # use std::net::{SocketAddr, IpAddr, Ipv4Addr};
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let session: Session = SessionBuilder::new()
    ///     .known_node_addr(SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 9042))
    ///     .build()
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn known_node_addr(mut self, node_addr: SocketAddr) -> Self {
        self.config.add_known_node_addr(node_addr);
        self
    }

    /// Add a list of known nodes with hostnames
    /// # Example
    /// ```
    /// # use scylla::{Session, SessionBuilder};
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let session: Session = SessionBuilder::new()
    ///     .known_nodes(&["127.0.0.1:9042", "db1.example.com"])
    ///     .build()
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn known_nodes(mut self, hostnames: &[impl AsRef<str>]) -> Self {
        self.config.add_known_nodes(hostnames);
        self
    }

    /// Add a list of known nodes with IP addresses
    /// # Example
    /// ```
    /// # use scylla::{Session, SessionBuilder};
    /// # use std::net::{SocketAddr, IpAddr, Ipv4Addr};
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let addr1 = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(172, 17, 0, 3)), 9042);
    /// let addr2 = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(172, 17, 0, 4)), 9042);
    ///
    /// let session: Session = SessionBuilder::new()
    ///     .known_nodes_addr(&[addr1, addr2])
    ///     .build()
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn known_nodes_addr(mut self, node_addrs: &[SocketAddr]) -> Self {
        self.config.add_known_nodes_addr(node_addrs);
        self
    }

    /// Set preferred Compression algorithm.
    /// The default is no compression.
    /// If it is not supported by database server Session will fall back to no encryption.
    ///
    /// # Example
    /// ```
    /// # use scylla::{Session, SessionBuilder};
    /// # use scylla::transport::Compression;
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let session: Session = SessionBuilder::new()
    ///     .known_node("127.0.0.1:9042")
    ///     .compression(Some(Compression::Snappy))
    ///     .build()
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn compression(mut self, compression: Option<Compression>) -> Self {
        self.config.compression = compression;
        self
    }

    /// Set the nodelay TCP flag.
    /// The default is false.
    ///
    /// # Example
    /// ```
    /// # use scylla::{Session, SessionBuilder};
    /// # use scylla::transport::Compression;
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let session: Session = SessionBuilder::new()
    ///     .known_node("127.0.0.1:9042")
    ///     .tcp_nodelay(true)
    ///     .build()
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn tcp_nodelay(mut self, nodelay: bool) -> Self {
        self.config.tcp_nodelay = nodelay;
        self
    }

    /// Set keyspace to be used on all connections.  
    /// Each connection will send `"USE <keyspace_name>"` before sending any requests.  
    /// This can be later changed with [`Session::use_keyspace`]
    ///
    /// # Example
    /// ```
    /// # use scylla::{Session, SessionBuilder};
    /// # use scylla::transport::Compression;
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let session: Session = SessionBuilder::new()
    ///     .known_node("127.0.0.1:9042")
    ///     .use_keyspace("my_keyspace_name")
    ///     .build()
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn use_keyspace(mut self, keyspace_name: impl Into<String>) -> Self {
        self.config.used_keyspace = Some(keyspace_name.into());
        self
    }

    /// Set the load balancing policy
    /// The default is Token-aware Round-robin.
    ///
    /// # Example
    /// ```
    /// # use scylla::{Session, SessionBuilder};
    /// # use scylla::transport::Compression;
    /// # use scylla::transport::load_balancing::RoundRobinPolicy;
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let session: Session = SessionBuilder::new()
    ///     .known_node("127.0.0.1:9042")
    ///     .load_balancing(Box::new(RoundRobinPolicy::new()))
    ///     .build()
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn load_balancing(mut self, policy: Box<dyn LoadBalancingPolicy>) -> Self {
        self.config.load_balancing = policy;
        self
    }

    /// Builds the Session after setting all the options
    ///
    /// # Example
    /// ```
    /// # use scylla::{Session, SessionBuilder};
    /// # use scylla::transport::Compression;
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let session: Session = SessionBuilder::new()
    ///     .known_node("127.0.0.1:9042")
    ///     .compression(Some(Compression::Snappy))
    ///     .build() // Turns SessionBuilder into Session
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn build(self) -> Result<Session, NewSessionError> {
        Session::connect(self.config).await
    }
}

/// Creates a [`SessionBuilder`] with default configuration, same as [`SessionBuilder::new`]
impl Default for SessionBuilder {
    fn default() -> Self {
        SessionBuilder::new()
    }
}

#[cfg(test)]
mod tests {
    use super::SessionBuilder;
    use crate::transport::load_balancing::RoundRobinPolicy;
    use crate::transport::session::KnownNode;
    use crate::transport::Compression;
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};

    #[test]
    fn default_session_builder() {
        let builder = SessionBuilder::new();

        assert!(builder.config.known_nodes.is_empty());
        assert_eq!(builder.config.compression, None);
    }

    #[test]
    fn add_known_node() {
        let mut builder = SessionBuilder::new();

        builder = builder.known_node("test_hostname");

        assert_eq!(
            builder.config.known_nodes,
            vec![KnownNode::Hostname("test_hostname".into())]
        );
        assert_eq!(builder.config.compression, None);
    }

    #[test]
    fn add_known_node_addr() {
        let mut builder = SessionBuilder::new();

        let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(172, 17, 0, 3)), 1357);
        builder = builder.known_node_addr(addr);

        assert_eq!(builder.config.known_nodes, vec![KnownNode::Address(addr)]);
        assert_eq!(builder.config.compression, None);
    }

    #[test]
    fn add_known_nodes() {
        let mut builder = SessionBuilder::new();

        builder = builder.known_nodes(&["test_hostname1", "test_hostname2"]);

        assert_eq!(
            builder.config.known_nodes,
            vec![
                KnownNode::Hostname("test_hostname1".into()),
                KnownNode::Hostname("test_hostname2".into())
            ]
        );
        assert_eq!(builder.config.compression, None);
    }

    #[test]
    fn add_known_nodes_addr() {
        let mut builder = SessionBuilder::new();

        let addr1 = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(172, 17, 0, 3)), 1357);
        let addr2 = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(172, 17, 0, 4)), 9090);

        builder = builder.known_nodes_addr(&[addr1, addr2]);

        assert_eq!(
            builder.config.known_nodes,
            vec![KnownNode::Address(addr1), KnownNode::Address(addr2)]
        );
        assert_eq!(builder.config.compression, None);
    }

    #[test]
    fn compression() {
        let mut builder = SessionBuilder::new();
        assert_eq!(builder.config.compression, None);

        builder = builder.compression(Some(Compression::LZ4));
        assert_eq!(builder.config.compression, Some(Compression::LZ4));

        builder = builder.compression(Some(Compression::Snappy));
        assert_eq!(builder.config.compression, Some(Compression::Snappy));

        builder = builder.compression(None);
        assert_eq!(builder.config.compression, None);
    }

    #[test]
    fn tcp_nodelay() {
        let mut builder = SessionBuilder::new();
        assert_eq!(builder.config.tcp_nodelay, false);

        builder = builder.tcp_nodelay(true);
        assert_eq!(builder.config.tcp_nodelay, true);

        builder = builder.tcp_nodelay(false);
        assert_eq!(builder.config.tcp_nodelay, false);
    }

    #[test]
    fn load_balancing() {
        let mut builder = SessionBuilder::new();
        assert_eq!(
            builder.config.load_balancing.name(),
            "TokenAwarePolicy{child_policy: RoundRobinPolicy}".to_string()
        );

        builder = builder.load_balancing(Box::new(RoundRobinPolicy::new()));
        assert_eq!(
            builder.config.load_balancing.name(),
            "RoundRobinPolicy".to_string()
        );
    }

    #[test]
    fn all_features() {
        let mut builder = SessionBuilder::new();

        let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(172, 17, 0, 3)), 8465);
        let addr1 = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(172, 17, 0, 3)), 1357);
        let addr2 = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(172, 17, 0, 4)), 9090);

        builder = builder.known_node("hostname_test");
        builder = builder.known_node_addr(addr);
        builder = builder.known_nodes(&["hostname_test1", "hostname_test2"]);
        builder = builder.known_nodes_addr(&[addr1, addr2]);
        builder = builder.compression(Some(Compression::Snappy));
        builder = builder.tcp_nodelay(true);
        builder = builder.load_balancing(Box::new(RoundRobinPolicy::new()));

        assert_eq!(
            builder.config.known_nodes,
            vec![
                KnownNode::Hostname("hostname_test".into()),
                KnownNode::Address(addr),
                KnownNode::Hostname("hostname_test1".into()),
                KnownNode::Hostname("hostname_test2".into()),
                KnownNode::Address(addr1),
                KnownNode::Address(addr2),
            ]
        );

        assert_eq!(builder.config.compression, Some(Compression::Snappy));
        assert_eq!(builder.config.tcp_nodelay, true);
        assert_eq!(
            builder.config.load_balancing.name(),
            "RoundRobinPolicy".to_string()
        );
    }
}
