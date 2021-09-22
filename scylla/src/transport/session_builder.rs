//! SessionBuilder provides an easy way to create new Sessions

use super::errors::NewSessionError;
use super::load_balancing::LoadBalancingPolicy;
use super::session::{Session, SessionConfig};
use super::speculative_execution::SpeculativeExecutionPolicy;
use super::Compression;
use crate::transport::retry_policy::RetryPolicy;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use crate::transport::errors::QueryError;
use crate::QueryResult;
#[cfg(feature = "ssl")]
use openssl::ssl::SslContext;

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

    #[cfg(test)]
    pub async fn new_for_test() -> Session {
        let uri = std::env::var("SCYLLA_URI").unwrap_or_else(|_| "127.0.0.1:9042".to_string());
        let session = SessionBuilder::new()
            .known_node(uri)
            .build()
            .await
            .expect("Could not create session");

        session
            .query("CREATE KEYSPACE IF NOT EXISTS ks WITH REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor' : 1}", &[])
            .await
            .expect("Could not create keyspace");

        session
            .query(
                "CREATE TABLE IF NOT EXISTS ks.test_table (a int primary key, b int)",
                &[],
            )
            .await
            .expect("Could not create table");

        session
            .use_keyspace("ks", false)
            .await
            .expect("Could not set keyspace");

        session
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
    /// The default is true.
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

    /// Set username and password for authentication.  
    /// If the database server will require authentication  
    ///
    /// # Example
    /// ```
    /// # use scylla::{Session, SessionBuilder};
    /// # use scylla::transport::Compression;
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let session: Session = SessionBuilder::new()
    ///     .known_node("127.0.0.1:9042")
    ///     .use_keyspace("my_keyspace_name", false)
    ///     .user("cassandra", "cassandra")
    ///     .build()
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn user(mut self, username: impl Into<String>, passwd: impl Into<String>) -> Self {
        self.config.auth_username = Some(username.into());
        self.config.auth_password = Some(passwd.into());
        self
    }

    /// Set the delay for schema agreement check. How often driver should ask if schema is in agreement
    /// The default is 200 miliseconds.
    ///
    /// # Example
    /// ```
    /// # use scylla::{Session, SessionBuilder};
    /// # use std::time::Duration;
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let session: Session = SessionBuilder::new()
    ///     .known_node("127.0.0.1:9042")
    ///     .schema_agreement_interval(Duration::from_secs(5))
    ///     .build()
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn schema_agreement_interval(mut self, timeout: Duration) -> Self {
        self.config.schema_agreement_interval = timeout;
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

    /// Set the speculative execution policy
    /// The default is None
    /// # Example
    /// ```
    /// # extern crate scylla;
    /// # use scylla::Session;
    /// # use std::error::Error;
    /// # async fn check_only_compiles() -> Result<(), Box<dyn Error>> {
    /// use std::{sync::Arc, time::Duration};
    /// use scylla::{
    ///     Session,
    ///     SessionBuilder,
    ///     transport::speculative_execution::SimpleSpeculativeExecutionPolicy
    /// };
    ///
    /// let policy = SimpleSpeculativeExecutionPolicy {
    ///     max_retry_count: 3,
    ///     retry_interval: Duration::from_millis(100),
    /// };
    ///
    /// let session: Session = SessionBuilder::new()
    ///     .known_node("127.0.0.1:9042")
    ///     .speculative_execution(Arc::new(policy))
    ///     .build()
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn speculative_execution(mut self, policy: Arc<dyn SpeculativeExecutionPolicy>) -> Self {
        self.config.speculative_execution_policy = Some(policy);
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

    /// ssl feature
    /// Provide SessionBuilder with SslContext from openssl crate that will be
    /// used to create an ssl connection to the database.
    /// If set to None SSL connection won't be used.
    /// Default is None.
    ///
    /// # Example
    /// ```
    /// # use std::fs;
    /// # use std::path::PathBuf;
    /// # use scylla::{Session, SessionBuilder};
    /// # use openssl::ssl::{SslContextBuilder, SslVerifyMode, SslMethod, SslFiletype};
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let certdir = fs::canonicalize(PathBuf::from("./examples/certs/scylla.crt"))?;
    /// let mut context_builder = SslContextBuilder::new(SslMethod::tls())?;
    /// context_builder.set_certificate_file(certdir.as_path(), SslFiletype::PEM)?;
    /// context_builder.set_verify(SslVerifyMode::NONE);
    ///
    /// let session: Session = SessionBuilder::new()
    ///     .known_node("127.0.0.1:9042")
    ///     .ssl_context(Some(context_builder.build()))
    ///     .build()
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    #[cfg(feature = "ssl")]
    pub fn ssl_context(mut self, ssl_context: Option<SslContext>) -> Self {
        self.config.ssl_context = ssl_context;
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
    pub async fn build(&self) -> Result<Session, NewSessionError> {
        Session::connect(self.config.clone()).await
    }

    /// Changes connection timeout
    /// The default is 5 seconds.
    /// If it's higher than underlying os's default connection timeout it won't effect.
    ///
    /// # Example
    /// ```
    /// # use scylla::{Session, SessionBuilder};
    /// # use std::time::Duration;
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let session: Session = SessionBuilder::new()
    ///     .known_node("127.0.0.1:9042")
    ///     .connection_timeout(Duration::from_secs(30))
    ///     .build() // Turns SessionBuilder into Session
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn connection_timeout(mut self, duration: std::time::Duration) -> Self {
        self.config.connect_timeout = duration;
        self
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
        assert!(builder.config.tcp_nodelay);

        builder = builder.tcp_nodelay(false);
        assert!(!builder.config.tcp_nodelay);

        builder = builder.tcp_nodelay(true);
        assert!(builder.config.tcp_nodelay);
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
        assert!(!builder.config.keyspace_case_sensitive);

        builder = builder.use_keyspace("ks_name_1", true);
        assert_eq!(builder.config.used_keyspace, Some("ks_name_1".to_string()));
        assert!(builder.config.keyspace_case_sensitive);

        builder = builder.use_keyspace("ks_name_2", false);
        assert_eq!(builder.config.used_keyspace, Some("ks_name_2".to_string()));
        assert!(!builder.config.keyspace_case_sensitive);
    }

    #[test]
    fn connection_timeout() {
        let mut builder = SessionBuilder::new();
        assert_eq!(
            builder.config.connect_timeout,
            std::time::Duration::from_secs(5)
        );

        builder = builder.connection_timeout(std::time::Duration::from_secs(10));
        assert_eq!(
            builder.config.connect_timeout,
            std::time::Duration::from_secs(10)
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
        builder = builder.load_balancing(Arc::new(RoundRobinPolicy::new()));
        builder = builder.use_keyspace("ks_name", true);

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
        assert!(builder.config.tcp_nodelay);
        assert_eq!(
            builder.config.load_balancing.name(),
            "RoundRobinPolicy".to_string()
        );

        assert_eq!(builder.config.used_keyspace, Some("ks_name".to_string()));

        assert!(builder.config.keyspace_case_sensitive);
    }
}
