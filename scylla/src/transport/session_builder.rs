use super::errors::NewSessionError;
use super::load_balancing::LoadBalancingPolicy;
use super::session::{Session, SessionConfig};
use super::Compression;
use crate::transport::retry_policy::RetryPolicy;
use std::net::SocketAddr;
use std::sync::Arc;

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
    ///     .use_keyspace("my_keyspace_name", false)
    ///     .build()
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn use_keyspace(mut self, keyspace_name: impl Into<String>, case_sensitive: bool) -> Self {
        self.config.used_keyspace = Some(keyspace_name.into());
        self.config.keyspace_case_sensitive = case_sensitive;
        self
    }

    /// Set the load balancing policy
    /// The default is Token-aware Round-robin.
    ///
    /// # Example
    /// ```
    /// # use scylla::{Session, SessionBuilder};
    /// # use scylla::transport::load_balancing::RoundRobinPolicy;
    /// # use std::sync::Arc;
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let session: Session = SessionBuilder::new()
    ///     .known_node("127.0.0.1:9042")
    ///     .load_balancing(Arc::new(RoundRobinPolicy::new()))
    ///     .build()
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn load_balancing(mut self, policy: Arc<dyn LoadBalancingPolicy>) -> Self {
        self.config.load_balancing = policy;
        self
    }

    /// Sets the [`RetryPolicy`] to use by default on queries
    /// The default is [DefaultRetryPolicy](crate::transport::retry_policy::DefaultRetryPolicy)
    /// It is possible to implement a custom retry policy by implementing the trait [`RetryPolicy`]
    ///
    /// # Example
    /// ```
    /// # use scylla::{Session, SessionBuilder};
    /// # use scylla::transport::Compression;
    /// use scylla::transport::retry_policy::DefaultRetryPolicy;
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let session: Session = SessionBuilder::new()
    ///     .known_node("127.0.0.1:9042")
    ///     .retry_policy(Box::new(DefaultRetryPolicy::new()))
    ///     .build()
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```    
    pub fn retry_policy(mut self, retry_policy: Box<dyn RetryPolicy + Send + Sync>) -> Self {
        self.config.retry_policy = retry_policy;
        self
    }

    /// Decide if session should use TLS.
    /// The default is false.
    /// Has to be used with tls_certificate_path.
    ///
    /// # Example
    /// ```
    /// # use scylla::{Session, SessionBuilder};
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let session: Session = SessionBuilder::new()
    ///     .known_node("127.0.0.1:9042")
    ///     .use_tls(true)
    ///     .tls_certificate_path("certificates/tls.pem")
    ///     .build()
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ``` 
    pub fn use_tls(mut self, use_tls: bool) -> Self {
        self.config.use_tls = use_tls;
        self
    }

    /// Provide TLS Certificate Path for our connection
    /// Argument should be a path to your .pem file.
    ///
    /// # Example
    /// ```
    /// # use scylla::{Session, SessionBuilder};
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let session: Session = SessionBuilder::new()
    ///     .known_node("127.0.0.1:9042")
    ///     .use_tls(true)
    ///     .tls_certificate_path("certificates/tls.pem")
    ///     .build()
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn tls_certificate_path(mut self, tls_certificate_path: String) -> Self {
        self.config.tls_certificate_path = Some(tls_certificate_path);
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
    use std::sync::Arc;

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

        builder = builder.compression(Some(Compression::Lz4));
        assert_eq!(builder.config.compression, Some(Compression::Lz4));

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

        builder = builder.load_balancing(Arc::new(RoundRobinPolicy::new()));
        assert_eq!(
            builder.config.load_balancing.name(),
            "RoundRobinPolicy".to_string()
        );
    }

    #[test]
    fn use_keyspace() {
        let mut builder = SessionBuilder::new();
        assert_eq!(builder.config.used_keyspace, None);
        assert_eq!(builder.config.keyspace_case_sensitive, false);

        builder = builder.use_keyspace("ks_name_1", true);
        assert_eq!(builder.config.used_keyspace, Some("ks_name_1".to_string()));
        assert_eq!(builder.config.keyspace_case_sensitive, true);

        builder = builder.use_keyspace("ks_name_2", false);
        assert_eq!(builder.config.used_keyspace, Some("ks_name_2".to_string()));
        assert_eq!(builder.config.keyspace_case_sensitive, false);
    }

    #[test]
    fn use_tls() {
        let mut builder = SessionBuilder::new();
        assert_eq!(builder.config.use_tls, false);

        builder = builder.use_tls(true);
        assert_eq!(builder.config.use_tls, true);

        builder = builder.use_tls(false);
        assert_eq!(builder.config.use_tls, false);
    }

    #[test]
    fn tls_certificate_path() {
        let mut builder = SessionBuilder::new();
        assert_eq!(builder.config.tls_certificate_path, None);

        let host = "localhost".to_string();
        builder = builder.tls_certificate_path(host.clone());
        assert_eq!(builder.config.tls_certificate_path, Some(host));

        let host = "127.0.0.1".to_string();
        builder = builder.tls_certificate_path(host.clone());
        assert_eq!(builder.config.tls_certificate_path, Some(host));
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
        builder = builder.load_balancing(Arc::new(RoundRobinPolicy::new()));
        builder = builder.use_keyspace("ks_name", true);
        builder = builder.use_tls(true);

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

        assert_eq!(builder.config.used_keyspace, Some("ks_name".to_string()));

        assert_eq!(builder.config.keyspace_case_sensitive, true);
        assert_eq!(builder.config.use_tls, true);
    }
}
