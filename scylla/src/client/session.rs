//! `Session` is the main object used in the driver.\
//! It manages all connections to the cluster and allows to execute CQL requests.

use super::execution_profile::{ExecutionProfile, ExecutionProfileHandle, ExecutionProfileInner};
use super::pager::{PreparedPagerConfig, QueryPager};
use super::{Compression, PoolSize, SelfIdentity, WriteCoalescingDelay};
use crate::authentication::AuthenticatorProvider;
#[cfg(feature = "unstable-cloud")]
use crate::cloud::CloudConfig;
#[cfg(feature = "unstable-cloud")]
use crate::cluster::node::CloudEndpoint;
use crate::cluster::node::{InternalKnownNode, KnownNode, NodeRef};
use crate::cluster::{Cluster, ClusterNeatDebug, ClusterState};
use crate::errors::{
    BadQuery, BrokenConnectionError, ExecutionError, MetadataError, NewSessionError,
    PagerExecutionError, PrepareError, RequestAttemptError, RequestError, SchemaAgreementError,
    TracingError, UseKeyspaceError,
};
use crate::frame::response::result;
use crate::network::tls::TlsProvider;
use crate::network::{Connection, ConnectionConfig, PoolConfig, VerifiedKeyspaceName};
use crate::observability::driver_tracing::RequestSpan;
use crate::observability::history::{self, HistoryListener};
#[cfg(feature = "metrics")]
use crate::observability::metrics::Metrics;
use crate::observability::tracing::TracingInfo;
use crate::policies::address_translator::AddressTranslator;
use crate::policies::host_filter::HostFilter;
use crate::policies::load_balancing::{self, RoutingInfo};
use crate::policies::retry::{RequestInfo, RetryDecision, RetrySession};
use crate::policies::speculative_execution;
use crate::policies::timestamp_generator::TimestampGenerator;
use crate::response::query_result::{MaybeFirstRowError, QueryResult, RowsError};
use crate::response::{
    Coordinator, NonErrorQueryResponse, PagingState, PagingStateResponse, QueryResponse,
};
use crate::routing::partitioner::PartitionerName;
use crate::routing::{Shard, ShardAwarePortRange};
use crate::statement::batch::batch_values;
use crate::statement::batch::{Batch, BatchStatement};
use crate::statement::prepared::{PartitionKeyError, PreparedStatement};
use crate::statement::unprepared::Statement;
use crate::statement::{Consistency, PageSize, StatementConfig};
use arc_swap::ArcSwapOption;
use futures::future::join_all;
use futures::future::try_join_all;
use itertools::Itertools;
use scylla_cql::frame::response::NonErrorResponse;
use scylla_cql::serialize::batch::BatchValues;
use scylla_cql::serialize::row::{SerializeRow, SerializedValues};
use std::borrow::Borrow;
use std::future::Future;
use std::net::{IpAddr, SocketAddr};
use std::num::NonZeroU32;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::timeout;
#[cfg(feature = "unstable-cloud")]
use tracing::warn;
use tracing::{Instrument, debug, error, trace, trace_span};
use uuid::Uuid;

pub(crate) const TABLET_CHANNEL_SIZE: usize = 8192;

const TRACING_QUERY_PAGE_SIZE: i32 = 1024;

/// `Session` manages connections to the cluster and allows to execute CQL requests.
pub struct Session {
    cluster: Cluster,
    default_execution_profile_handle: ExecutionProfileHandle,
    schema_agreement_interval: Duration,
    #[cfg(feature = "metrics")]
    metrics: Arc<Metrics>,
    schema_agreement_timeout: Duration,
    schema_agreement_automatic_waiting: bool,
    refresh_metadata_on_auto_schema_agreement: bool,
    keyspace_name: Arc<ArcSwapOption<String>>,
    tracing_info_fetch_attempts: NonZeroU32,
    tracing_info_fetch_interval: Duration,
    tracing_info_fetch_consistency: Consistency,
}

/// This implementation deliberately omits some details from Cluster in order
/// to avoid cluttering the print with much information of little usability.
impl std::fmt::Debug for Session {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut d = f.debug_struct("Session");
        d.field("cluster", &ClusterNeatDebug(&self.cluster))
            .field(
                "default_execution_profile_handle",
                &self.default_execution_profile_handle,
            )
            .field("schema_agreement_interval", &self.schema_agreement_interval);

        #[cfg(feature = "metrics")]
        d.field("metrics", &self.metrics);

        d.field(
            "auto_await_schema_agreement_timeout",
            &self.schema_agreement_timeout,
        )
        .field(
            "schema_agreement_automatic_waiting",
            &self.schema_agreement_automatic_waiting,
        )
        .field(
            "refresh_metadata_on_auto_schema_agreement",
            &self.refresh_metadata_on_auto_schema_agreement,
        )
        .field("keyspace_name", &self.keyspace_name)
        .field(
            "tracing_info_fetch_attempts",
            &self.tracing_info_fetch_attempts,
        )
        .field(
            "tracing_info_fetch_interval",
            &self.tracing_info_fetch_interval,
        )
        .field(
            "tracing_info_fetch_consistency",
            &self.tracing_info_fetch_consistency,
        )
        .finish()
    }
}

/// Represents a TLS context used to configure TLS connections to DB nodes.
/// Abstracts over various TLS implementations, such as OpenSSL and Rustls.
#[derive(Clone)] // Cheaply clonable - reference counted.
#[non_exhaustive]
pub enum TlsContext {
    /// TLS context backed by OpenSSL 0.10.
    #[cfg(feature = "openssl-010")]
    OpenSsl010(openssl::ssl::SslContext),
    /// TLS context backed by Rustls 0.23.
    #[cfg(feature = "rustls-023")]
    Rustls023(Arc<rustls::ClientConfig>),
}

#[cfg(feature = "openssl-010")]
impl From<openssl::ssl::SslContext> for TlsContext {
    fn from(value: openssl::ssl::SslContext) -> Self {
        TlsContext::OpenSsl010(value)
    }
}

#[cfg(feature = "rustls-023")]
impl From<Arc<rustls::ClientConfig>> for TlsContext {
    fn from(value: Arc<rustls::ClientConfig>) -> Self {
        TlsContext::Rustls023(value)
    }
}

/// Configuration options for [`Session`].
/// Can be created manually, but usually it's easier to use
/// [SessionBuilder](super::session_builder::SessionBuilder)
#[derive(Clone)]
#[non_exhaustive]
pub struct SessionConfig {
    /// List of database servers known on Session startup.
    /// Session will connect to these nodes to retrieve information about other nodes in the cluster.
    /// Each node can be represented as a hostname or an IP address.
    pub known_nodes: Vec<KnownNode>,

    /// A local ip address to bind all driver's TCP sockets to.
    ///
    /// By default set to None, which is equivalent to:
    /// - `INADDR_ANY` for IPv4 ([`Ipv4Addr::UNSPECIFIED`][std::net::Ipv4Addr::UNSPECIFIED])
    /// - `in6addr_any` for IPv6 ([`Ipv6Addr::UNSPECIFIED`][std::net::Ipv6Addr::UNSPECIFIED])
    pub local_ip_address: Option<IpAddr>,

    /// Specifies the local port range used for shard-aware connections.
    ///
    /// By default set to [`ShardAwarePortRange::EPHEMERAL_PORT_RANGE`].
    pub shard_aware_local_port_range: ShardAwarePortRange,

    /// Preferred compression algorithm to use on connections.
    /// If it's not supported by database server Session will fall back to no compression.
    pub compression: Option<Compression>,

    /// Whether to set the nodelay TCP flag.
    pub tcp_nodelay: bool,

    /// TCP keepalive interval, which means how often keepalive messages
    /// are sent **on TCP layer** when a connection is idle.
    /// If `None`, no TCP keepalive messages are sent.
    pub tcp_keepalive_interval: Option<Duration>,

    /// Handle to the default execution profile, which is used
    /// for all statements that do not specify an execution profile.
    pub default_execution_profile_handle: ExecutionProfileHandle,

    /// Keyspace to be used on all connections.
    /// Each connection will send `"USE <keyspace_name>"` before sending any requests.
    /// This can be later changed with [`Session::use_keyspace`].
    pub used_keyspace: Option<String>,

    /// Whether the keyspace name is case-sensitive.
    /// This is used to determine how the keyspace name is sent to the server:
    /// - if case-insensitive, it is sent as-is,
    /// - if case-sensitive, it is enclosed in double quotes.
    pub keyspace_case_sensitive: bool,

    /// TLS context used configure TLS connections to DB nodes.
    pub tls_context: Option<TlsContext>,

    /// Custom authenticator provider to create an authenticator instance
    /// upon session creation.
    pub authenticator: Option<Arc<dyn AuthenticatorProvider>>,

    /// Timeout for establishing connections to a node.
    ///
    /// If it's higher than underlying os's default connection timeout, it won't have
    /// any effect.
    pub connect_timeout: Duration,

    /// Size of the per-node connection pool, i.e. how many connections the driver should keep to each node.
    /// The default is `PerShard(1)`, which is the recommended setting for ScyllaDB clusters.
    pub connection_pool_size: PoolSize,

    /// If true, prevents the driver from connecting to the shard-aware port, even if the node supports it.
    /// Generally, this options is best left as default (false).
    pub disallow_shard_aware_port: bool,

    ///  Timestamp generator used for generating timestamps on the client-side
    ///  If None, server-side timestamps are used.
    pub timestamp_generator: Option<Arc<dyn TimestampGenerator>>,

    /// If empty, fetch all keyspaces
    pub keyspaces_to_fetch: Vec<String>,

    /// If true, full schema is fetched with every metadata refresh.
    pub fetch_schema_metadata: bool,

    /// Custom timeout for requests that query metadata.
    pub metadata_request_serverside_timeout: Option<Duration>,

    /// Interval of sending keepalive requests.
    /// If `None`, keepalives are never sent, so `Self::keepalive_timeout` has no effect.
    pub keepalive_interval: Option<Duration>,

    /// Controls after what time of not receiving response to keepalives a connection is closed.
    /// If `None`, connections are never closed due to lack of response to a keepalive message.
    pub keepalive_timeout: Option<Duration>,

    /// How often the driver should ask if schema is in agreement.
    pub schema_agreement_interval: Duration,

    /// Controls the timeout for waiting for schema agreement.
    /// This works both for manual awaiting schema agreement and for
    /// automatic waiting after a schema-altering statement is sent.
    pub schema_agreement_timeout: Duration,

    /// Controls whether schema agreement is automatically awaited
    /// after sending a schema-altering statement.
    pub schema_agreement_automatic_waiting: bool,

    /// If true, full schema metadata is fetched after successfully reaching a schema agreement.
    /// It is true by default but can be disabled if successive schema-altering statements should be performed.
    pub refresh_metadata_on_auto_schema_agreement: bool,

    /// The address translator is used to translate addresses received from ScyllaDB nodes
    /// (either with cluster metadata or with an event) to addresses that can be used to
    /// actually connect to those nodes. This may be needed e.g. when there is NAT
    /// between the nodes and the driver.
    pub address_translator: Option<Arc<dyn AddressTranslator>>,

    /// The host filter decides whether any connections should be opened
    /// to the node or not. The driver will also avoid filtered out nodes when
    /// re-establishing the control connection.
    pub host_filter: Option<Arc<dyn HostFilter>>,

    /// If the driver is to connect to ScyllaCloud, there is a config for it.
    #[cfg(feature = "unstable-cloud")]
    pub cloud_config: Option<Arc<CloudConfig>>,

    /// If true, the driver will inject a delay controlled by [`SessionConfig::write_coalescing_delay`]
    /// before flushing data to the socket.
    /// This gives the driver an opportunity to collect more write requests
    /// and write them in a single syscall, increasing the efficiency.
    ///
    /// However, this optimization may worsen latency if the rate of requests
    /// issued by the application is low, but otherwise the application is
    /// heavily loaded with other tasks on the same tokio executor.
    /// Please do performance measurements before committing to disabling
    /// this option.
    pub enable_write_coalescing: bool,

    /// Controls the write coalescing delay (if enabled).
    ///
    /// This option has no effect if [`SessionConfig::enable_write_coalescing`] is false.
    ///
    /// This option is [`WriteCoalescingDelay::SmallNondeterministic`] by default.
    pub write_coalescing_delay: WriteCoalescingDelay,

    /// Number of attempts to fetch [`TracingInfo`]
    /// in [`Session::get_tracing_info`]. Tracing info
    /// might not be available immediately on queried node - that's why
    /// the driver performs a few attempts with sleeps in between.
    pub tracing_info_fetch_attempts: NonZeroU32,

    /// Delay between attempts to fetch [`TracingInfo`]
    /// in [`Session::get_tracing_info`]. Tracing info
    /// might not be available immediately on queried node - that's why
    /// the driver performs a few attempts with sleeps in between.
    pub tracing_info_fetch_interval: Duration,

    /// Consistency level of fetching [`TracingInfo`]
    /// in [`Session::get_tracing_info`].
    pub tracing_info_fetch_consistency: Consistency,

    /// Interval between refreshing cluster metadata. This
    /// can be configured according to the traffic pattern
    /// for e.g: if they do not want unexpected traffic
    /// or they expect the topology to change frequently.
    pub cluster_metadata_refresh_interval: Duration,

    /// Driver and application self-identifying information,
    /// to be sent to server in STARTUP message.
    pub identity: SelfIdentity<'static>,
}

impl SessionConfig {
    /// Creates a [`SessionConfig`] with default configuration
    /// # Default configuration
    /// * Compression: None
    /// * Load balancing policy: Token-aware Round-robin
    ///
    /// # Example
    /// ```
    /// # use scylla::client::session::SessionConfig;
    /// let config = SessionConfig::new();
    /// ```
    pub fn new() -> Self {
        SessionConfig {
            known_nodes: Vec::new(),
            local_ip_address: None,
            shard_aware_local_port_range: ShardAwarePortRange::EPHEMERAL_PORT_RANGE,
            compression: None,
            tcp_nodelay: true,
            tcp_keepalive_interval: None,
            schema_agreement_interval: Duration::from_millis(200),
            default_execution_profile_handle: ExecutionProfile::new_from_inner(Default::default())
                .into_handle(),
            used_keyspace: None,
            keyspace_case_sensitive: false,
            tls_context: None,
            authenticator: None,
            connect_timeout: Duration::from_secs(5),
            connection_pool_size: Default::default(),
            disallow_shard_aware_port: false,
            timestamp_generator: None,
            keyspaces_to_fetch: Vec::new(),
            fetch_schema_metadata: true,
            metadata_request_serverside_timeout: Some(Duration::from_secs(2)),
            keepalive_interval: Some(Duration::from_secs(30)),
            keepalive_timeout: Some(Duration::from_secs(30)),
            schema_agreement_timeout: Duration::from_secs(60),
            schema_agreement_automatic_waiting: true,
            address_translator: None,
            host_filter: None,
            refresh_metadata_on_auto_schema_agreement: true,
            #[cfg(feature = "unstable-cloud")]
            cloud_config: None,
            enable_write_coalescing: true,
            write_coalescing_delay: WriteCoalescingDelay::SmallNondeterministic,
            tracing_info_fetch_attempts: NonZeroU32::new(10).unwrap(),
            tracing_info_fetch_interval: Duration::from_millis(3),
            tracing_info_fetch_consistency: Consistency::One,
            cluster_metadata_refresh_interval: Duration::from_secs(60),
            identity: SelfIdentity::default(),
        }
    }

    /// Adds a known database server with a hostname.
    /// If the port is not explicitly specified, 9042 is used as default
    /// # Example
    /// ```
    /// # use scylla::client::session::SessionConfig;
    /// let mut config = SessionConfig::new();
    /// config.add_known_node("127.0.0.1");
    /// config.add_known_node("db1.example.com:9042");
    /// ```
    pub fn add_known_node(&mut self, hostname: impl AsRef<str>) {
        self.known_nodes
            .push(KnownNode::Hostname(hostname.as_ref().to_string()));
    }

    /// Adds a known database server with an IP address
    /// # Example
    /// ```
    /// # use scylla::client::session::SessionConfig;
    /// # use std::net::{SocketAddr, IpAddr, Ipv4Addr};
    /// let mut config = SessionConfig::new();
    /// config.add_known_node_addr(SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 9042));
    /// ```
    pub fn add_known_node_addr(&mut self, node_addr: SocketAddr) {
        self.known_nodes.push(KnownNode::Address(node_addr));
    }

    /// Adds a list of known database server with hostnames.
    /// If the port is not explicitly specified, 9042 is used as default
    /// # Example
    /// ```
    /// # use scylla::client::session::SessionConfig;
    /// # use std::net::{SocketAddr, IpAddr, Ipv4Addr};
    /// let mut config = SessionConfig::new();
    /// config.add_known_nodes(&["127.0.0.1:9042", "db1.example.com"]);
    /// ```
    pub fn add_known_nodes(&mut self, hostnames: impl IntoIterator<Item = impl AsRef<str>>) {
        for hostname in hostnames {
            self.add_known_node(hostname);
        }
    }

    /// Adds a list of known database servers with IP addresses
    /// # Example
    /// ```
    /// # use scylla::client::session::SessionConfig;
    /// # use std::net::{SocketAddr, IpAddr, Ipv4Addr};
    /// let addr1 = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(172, 17, 0, 3)), 9042);
    /// let addr2 = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(172, 17, 0, 4)), 9042);
    ///
    /// let mut config = SessionConfig::new();
    /// config.add_known_nodes_addr(&[addr1, addr2]);
    /// ```
    pub fn add_known_nodes_addr(
        &mut self,
        node_addrs: impl IntoIterator<Item = impl Borrow<SocketAddr>>,
    ) {
        for address in node_addrs {
            self.add_known_node_addr(*address.borrow());
        }
    }
}

/// Creates default [`SessionConfig`], same as [`SessionConfig::new`]
impl Default for SessionConfig {
    fn default() -> Self {
        Self::new()
    }
}

pub(crate) enum RunRequestResult<ResT> {
    IgnoredWriteError,
    Completed(ResT),
}

/// Represents a CQL session, which can be used to communicate
/// with the database
impl Session {
    /// Sends a request to the database and receives a response.\
    /// Executes an unprepared CQL statement without paging, i.e. all results are received in a single response.
    ///
    /// This is the easiest way to execute a CQL statement, but performance is worse than that of prepared statements.
    ///
    /// It is discouraged to use this method with non-empty values argument ([`SerializeRow::is_empty()`]
    /// trait method returns false). In such case, statement first needs to be prepared (on a single connection), so
    /// driver will perform 2 round trips instead of 1. Please use [`Session::execute_unpaged()`] instead.
    ///
    /// As all results come in one response (no paging is done!), the memory footprint and latency may be huge
    /// for statements returning rows (i.e. SELECTs)! Prefer this method for non-SELECTs, and for SELECTs
    /// it is best to use paged requests:
    /// - to receive multiple pages and transparently iterate through them, use [query_iter](Session::query_iter).
    /// - to manually receive multiple pages and iterate through them, use [query_single_page](Session::query_single_page).
    ///
    /// See [the book](https://rust-driver.docs.scylladb.com/stable/statements/unprepared.html) for more information
    /// # Arguments
    /// * `statement` - statement to be executed, can be just a `&str` or the [`Statement`] struct.
    /// * `values` - values bound to the statement, the easiest way is to use a tuple of bound values.
    ///
    /// # Examples
    /// ```rust
    /// # use scylla::client::session::Session;
    /// # use std::error::Error;
    /// # async fn check_only_compiles(session: &Session) -> Result<(), Box<dyn Error>> {
    /// // Insert an int and text into a table.
    /// session
    ///     .query_unpaged(
    ///         "INSERT INTO ks.tab (a, b) VALUES(?, ?)",
    ///         (2_i32, "some text")
    ///     )
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    /// ```rust
    /// # use scylla::client::session::Session;
    /// # use std::error::Error;
    /// # async fn check_only_compiles(session: &Session) -> Result<(), Box<dyn Error>> {
    ///
    /// // Read rows containing an int and text.
    /// // Keep in mind that all results come in one response (no paging is done!),
    /// // so the memory footprint and latency may be huge!
    /// // To prevent that, use `Session::query_iter` or `Session::query_single_page`.
    /// let query_rows = session
    ///     .query_unpaged("SELECT a, b FROM ks.tab", &[])
    ///     .await?
    ///     .into_rows_result()?;
    ///
    /// for row in query_rows.rows()? {
    ///     // Parse row as int and text.
    ///     let (int_val, text_val): (i32, &str) = row?;
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub async fn query_unpaged(
        &self,
        statement: impl Into<Statement>,
        values: impl SerializeRow,
    ) -> Result<QueryResult, ExecutionError> {
        self.do_query_unpaged(&statement.into(), values).await
    }

    /// Queries a single page from the database, optionally continuing from a saved point.
    ///
    /// It is discouraged to use this method with non-empty values argument ([`SerializeRow::is_empty()`]
    /// trait method returns false). In such case, CQL statement first needs to be prepared (on a single connection), so
    /// driver will perform 2 round trips instead of 1. Please use [`Session::execute_single_page()`] instead.
    ///
    /// # Arguments
    ///
    /// * `statement` - statement to be executed
    /// * `values` - values bound to the statement
    /// * `paging_state` - previously received paging state or [PagingState::start()]
    ///
    /// # Example
    ///
    /// ```rust
    /// # use scylla::client::session::Session;
    /// # use std::error::Error;
    /// # async fn check_only_compiles(session: &Session) -> Result<(), Box<dyn Error>> {
    /// use std::ops::ControlFlow;
    /// use scylla::response::PagingState;
    ///
    /// // Manual paging in a loop, unprepared statement.
    /// let mut paging_state = PagingState::start();
    /// loop {
    ///    let (res, paging_state_response) = session
    ///        .query_single_page("SELECT a, b, c FROM ks.tbl", &[], paging_state)
    ///        .await?;
    ///
    ///    // Do something with a single page of results.
    ///    for row in res
    ///        .into_rows_result()?
    ///        .rows::<(i32, &str)>()?
    ///    {
    ///        let (a, b) = row?;
    ///    }
    ///
    ///    match paging_state_response.into_paging_control_flow() {
    ///        ControlFlow::Break(()) => {
    ///            // No more pages to be fetched.
    ///            break;
    ///        }
    ///        ControlFlow::Continue(new_paging_state) => {
    ///            // Update paging state from the response, so that query
    ///            // will be resumed from where it ended the last time.
    ///            paging_state = new_paging_state;
    ///        }
    ///    }
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub async fn query_single_page(
        &self,
        statement: impl Into<Statement>,
        values: impl SerializeRow,
        paging_state: PagingState,
    ) -> Result<(QueryResult, PagingStateResponse), ExecutionError> {
        self.do_query_single_page(&statement.into(), values, paging_state)
            .await
    }

    /// Execute an unprepared CQL statement with paging\
    /// This method will query all pages of the result\
    ///
    /// Returns an async iterator (stream) over all received rows\
    /// Page size can be specified in the [`Statement`] passed to the function
    ///
    /// It is discouraged to use this method with non-empty values argument ([`SerializeRow::is_empty()`]
    /// trait method returns false). In such case, statement first needs to be prepared (on a single connection), so
    /// driver will initially perform 2 round trips instead of 1. Please use [`Session::execute_iter()`] instead.
    ///
    /// See [the book](https://rust-driver.docs.scylladb.com/stable/statements/paged.html) for more information.
    ///
    /// # Arguments
    /// * `statement` - statement to be executed, can be just a `&str` or the [`Statement`] struct.
    /// * `values` - values bound to the statement, the easiest way is to use a tuple of bound values.
    ///
    /// # Example
    ///
    /// ```rust
    /// # use scylla::client::session::Session;
    /// # use std::error::Error;
    /// # async fn check_only_compiles(session: &Session) -> Result<(), Box<dyn Error>> {
    /// use futures::stream::StreamExt;
    ///
    /// let mut rows_stream = session
    ///    .query_iter("SELECT a, b FROM ks.t", &[])
    ///    .await?
    ///    .rows_stream::<(i32, i32)>()?;
    ///
    /// while let Some(next_row_res) = rows_stream.next().await {
    ///     let (a, b): (i32, i32) = next_row_res?;
    ///     println!("a, b: {}, {}", a, b);
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub async fn query_iter(
        &self,
        statement: impl Into<Statement>,
        values: impl SerializeRow,
    ) -> Result<QueryPager, PagerExecutionError> {
        self.do_query_iter(statement.into(), values).await
    }

    /// Execute a prepared statement. Requires a [PreparedStatement]
    /// generated using [`Session::prepare`](Session::prepare).\
    /// Performs an unpaged request, i.e. all results are received in a single response.
    ///
    /// As all results come in one response (no paging is done!), the memory footprint and latency may be huge
    /// for statements returning rows (i.e. SELECTs)! Prefer this method for non-SELECTs, and for SELECTs
    /// it is best to use paged requests:
    /// - to receive multiple pages and transparently iterate through them, use [execute_iter](Session::execute_iter).
    /// - to manually receive multiple pages and iterate through them, use [execute_single_page](Session::execute_single_page).
    ///
    /// Prepared statements are much faster than unprepared statements:
    /// * Database doesn't need to parse the statement string upon each execution (only once)
    /// * They are properly load balanced using token aware routing
    ///
    /// > ***Warning***\
    /// > For token/shard aware load balancing to work properly, all partition key values
    /// > must be sent as bound values
    /// > (see [performance section](https://rust-driver.docs.scylladb.com/stable/statements/prepared.html#performance)).
    ///
    /// See [the book](https://rust-driver.docs.scylladb.com/stable/statements/prepared.html) for more information.
    ///
    /// # Arguments
    /// * `prepared` - the prepared statement to execute, generated using [`Session::prepare`](Session::prepare)
    /// * `values` - values bound to the statement, the easiest way is to use a tuple of bound values
    ///
    /// # Example
    /// ```rust
    /// # use scylla::client::session::Session;
    /// # use std::error::Error;
    /// # async fn check_only_compiles(session: &Session) -> Result<(), Box<dyn Error>> {
    /// use scylla::statement::prepared::PreparedStatement;
    ///
    /// // Prepare the statement for later execution
    /// let prepared: PreparedStatement = session
    ///     .prepare("INSERT INTO ks.tab (a) VALUES(?)")
    ///     .await?;
    ///
    /// // Execute the prepared statement with some values, just like an unprepared statement.
    /// let to_insert: i32 = 12345;
    /// session.execute_unpaged(&prepared, (to_insert,)).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn execute_unpaged(
        &self,
        prepared: &PreparedStatement,
        values: impl SerializeRow,
    ) -> Result<QueryResult, ExecutionError> {
        self.do_execute_unpaged(prepared, values).await
    }

    /// Executes a prepared statement, restricting results to single page.
    /// Optionally continues fetching results from a saved point.
    ///
    /// # Arguments
    ///
    /// * `prepared` - a statement prepared with [prepare](crate::client::session::Session::prepare)
    /// * `values` - values bound to the statement
    /// * `paging_state` - continuation based on a paging state received from a previous paged query or None
    ///
    /// # Example
    ///
    /// ```rust
    /// # use scylla::client::session::Session;
    /// # use std::error::Error;
    /// # async fn check_only_compiles(session: &Session) -> Result<(), Box<dyn Error>> {
    /// use std::ops::ControlFlow;
    /// use scylla::statement::unprepared::Statement;
    /// use scylla::response::{PagingState, PagingStateResponse};
    ///
    /// let paged_prepared = session
    ///     .prepare(
    ///         Statement::new("SELECT a, b FROM ks.tbl")
    ///             .with_page_size(100.try_into().unwrap()),
    ///     )
    ///     .await?;
    ///
    /// // Manual paging in a loop, prepared statement.
    /// let mut paging_state = PagingState::start();
    /// loop {
    ///     let (res, paging_state_response) = session
    ///         .execute_single_page(&paged_prepared, &[], paging_state)
    ///         .await?;
    ///
    ///    // Do something with a single page of results.
    ///    for row in res
    ///        .into_rows_result()?
    ///        .rows::<(i32, &str)>()?
    ///    {
    ///        let (a, b) = row?;
    ///    }
    ///
    ///     match paging_state_response.into_paging_control_flow() {
    ///         ControlFlow::Break(()) => {
    ///             // No more pages to be fetched.
    ///             break;
    ///         }
    ///         ControlFlow::Continue(new_paging_state) => {
    ///             // Update paging continuation from the paging state, so that query
    ///             // will be resumed from where it ended the last time.
    ///             paging_state = new_paging_state;
    ///         }
    ///     }
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub async fn execute_single_page(
        &self,
        prepared: &PreparedStatement,
        values: impl SerializeRow,
        paging_state: PagingState,
    ) -> Result<(QueryResult, PagingStateResponse), ExecutionError> {
        self.do_execute_single_page(prepared, values, paging_state)
            .await
    }

    /// Execute a prepared statement with paging.\
    /// This method will query all pages of the result.\
    ///
    /// Returns an async iterator (stream) over all received rows.\
    /// Page size can be specified in the [PreparedStatement] passed to the function.
    ///
    /// See [the book](https://rust-driver.docs.scylladb.com/stable/statements/paged.html) for more information.
    ///
    /// # Arguments
    /// * `prepared` - the prepared statement to execute, generated using [`Session::prepare`](Session::prepare)
    /// * `values` - values bound to the statement, the easiest way is to use a tuple of bound values
    ///
    /// # Example
    ///
    /// ```rust
    /// # use scylla::client::session::Session;
    /// # use futures::StreamExt as _;
    /// # use std::error::Error;
    /// # async fn check_only_compiles(session: &Session) -> Result<(), Box<dyn Error>> {
    /// use scylla::statement::prepared::PreparedStatement;
    ///
    /// // Prepare the statement for later execution
    /// let prepared: PreparedStatement = session
    ///     .prepare("SELECT a, b FROM ks.t")
    ///     .await?;
    ///
    /// // Execute the statement and receive all pages
    /// let mut rows_stream = session
    ///    .execute_iter(prepared, &[])
    ///    .await?
    ///    .rows_stream::<(i32, i32)>()?;
    ///
    /// while let Some(next_row_res) = rows_stream.next().await {
    ///     let (a, b): (i32, i32) = next_row_res?;
    ///     println!("a, b: {}, {}", a, b);
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub async fn execute_iter(
        &self,
        prepared: impl Into<PreparedStatement>,
        values: impl SerializeRow,
    ) -> Result<QueryPager, PagerExecutionError> {
        self.do_execute_iter(prepared.into(), values).await
    }

    /// Execute a batch statement\
    /// Batch contains many `unprepared` or `prepared` statements which are executed at once\
    /// Batch doesn't return any rows.
    ///
    /// Batch values must contain values for each of the statements.
    ///
    /// Avoid using non-empty values ([`SerializeRow::is_empty()`] return false) for unprepared statements
    /// inside the batch. Such statements will first need to be prepared, so the driver will need to
    /// send (numer_of_unprepared_statements_with_values + 1) requests instead of 1 request, severly
    /// affecting performance.
    ///
    /// See [the book](https://rust-driver.docs.scylladb.com/stable/statements/batch.html) for more information
    ///
    /// # Arguments
    /// * `batch` - [Batch] to be performed
    /// * `values` - List of values for each statement, it's the easiest to use a tuple of tuples
    ///
    /// # Example
    /// ```rust
    /// # use scylla::client::session::Session;
    /// # use std::error::Error;
    /// # async fn check_only_compiles(session: &Session) -> Result<(), Box<dyn Error>> {
    /// use scylla::statement::batch::Batch;
    ///
    /// let mut batch: Batch = Default::default();
    ///
    /// // A statement with two bound values
    /// batch.append_statement("INSERT INTO ks.tab(a, b) VALUES(?, ?)");
    ///
    /// // A statement with one bound value
    /// batch.append_statement("INSERT INTO ks.tab(a, b) VALUES(3, ?)");
    ///
    /// // A statement with no bound values
    /// batch.append_statement("INSERT INTO ks.tab(a, b) VALUES(5, 6)");
    ///
    /// // Batch values is a tuple of 3 tuples containing values for each statement
    /// let batch_values = ((1_i32, 2_i32), // Tuple with two values for the first statement
    ///                     (4_i32,),       // Tuple with one value for the second statement
    ///                     ());            // Empty tuple/unit for the third statement
    ///
    /// // Run the batch
    /// session.batch(&batch, batch_values).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn batch(
        &self,
        batch: &Batch,
        values: impl BatchValues,
    ) -> Result<QueryResult, ExecutionError> {
        self.do_batch(batch, values).await
    }

    /// Estabilishes a CQL session with the database
    ///
    /// Usually it's easier to use [SessionBuilder](crate::client::session_builder::SessionBuilder)
    /// instead of calling `Session::connect` directly, because it's more convenient.
    /// # Arguments
    /// * `config` - Connection configuration - known nodes, Compression, etc.
    ///   Must contain at least one known node.
    ///
    /// # Example
    /// ```rust
    /// # use std::error::Error;
    /// # async fn check_only_compiles() -> Result<(), Box<dyn Error>> {
    /// use scylla::client::session::{Session, SessionConfig};
    /// use scylla::cluster::KnownNode;
    ///
    /// let mut config = SessionConfig::new();
    /// config.known_nodes.push(KnownNode::Hostname("127.0.0.1:9042".to_string()));
    ///
    /// let session: Session = Session::connect(config).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn connect(config: SessionConfig) -> Result<Self, NewSessionError> {
        let known_nodes = config.known_nodes;

        #[cfg(feature = "unstable-cloud")]
        let cloud_known_nodes: Option<Vec<InternalKnownNode>> =
            if let Some(ref cloud_config) = config.cloud_config {
                let cloud_servers = cloud_config
                    .get_datacenters()
                    .iter()
                    .map(|(dc_name, dc_data)| {
                        InternalKnownNode::CloudEndpoint(CloudEndpoint {
                            hostname: dc_data.get_server().to_owned(),
                            datacenter: dc_name.clone(),
                        })
                    })
                    .collect();
                Some(cloud_servers)
            } else {
                None
            };

        #[cfg(not(feature = "unstable-cloud"))]
        let cloud_known_nodes: Option<Vec<InternalKnownNode>> = None;

        #[allow(clippy::unnecessary_literal_unwrap)]
        let known_nodes = cloud_known_nodes
            .unwrap_or_else(|| known_nodes.into_iter().map(|node| node.into()).collect());

        // Ensure there is at least one known node
        if known_nodes.is_empty() {
            return Err(NewSessionError::EmptyKnownNodesList);
        }

        let (tablet_sender, tablet_receiver) = tokio::sync::mpsc::channel(TABLET_CHANNEL_SIZE);

        #[allow(unused_labels)] // Triggers when `cloud` feature is disabled.
        let address_translator = 'translator: {
            #[cfg(feature = "unstable-cloud")]
            if let Some(translator) = config.cloud_config.clone() {
                if config.address_translator.is_some() {
                    // This can only happen if the user builds SessionConfig by hand, as SessionBuilder in cloud mode prevents setting custom AddressTranslator.
                    warn!(
                        "Overriding user-provided AddressTranslator with Scylla Cloud AddressTranslator due \
                            to CloudConfig being provided. This is certainly an API misuse - Cloud \
                            may not be combined with user's own AddressTranslator."
                    )
                }

                break 'translator Some(translator as Arc<dyn AddressTranslator>);
            }

            config.address_translator
        };

        let tls_provider = 'provider: {
            #[cfg(feature = "unstable-cloud")]
            if let Some(cloud_config) = config.cloud_config {
                if config.tls_context.is_some() {
                    // This can only happen if the user builds SessionConfig by hand, as SessionBuilder in cloud mode prevents setting custom TlsContext.
                    warn!(
                        "Overriding user-provided TlsContext with Scylla Cloud TlsContext due \
                            to CloudConfig being provided. This is certainly an API misuse - Cloud \
                            may not be combined with user's own TLS config."
                    )
                }

                let provider = TlsProvider::new_cloud(cloud_config);
                break 'provider Some(provider);
            }
            if let Some(tls_context) = config.tls_context {
                // To silence warnings when TlsContext is an empty enum (tls features are disabled).
                // In such case, TlsProvider is uninhabited.
                #[allow(unused_variables)]
                let provider = TlsProvider::new_with_global_context(tls_context);
                #[allow(unreachable_code)]
                break 'provider Some(provider);
            }
            None
        };

        let connection_config = ConnectionConfig {
            local_ip_address: config.local_ip_address,
            shard_aware_local_port_range: config.shard_aware_local_port_range,
            compression: config.compression,
            tcp_nodelay: config.tcp_nodelay,
            tcp_keepalive_interval: config.tcp_keepalive_interval,
            timestamp_generator: config.timestamp_generator,
            tls_provider,
            authenticator: config.authenticator,
            connect_timeout: config.connect_timeout,
            event_sender: None,
            default_consistency: Default::default(),
            address_translator,
            write_coalescing_delay: config
                .enable_write_coalescing
                .then_some(config.write_coalescing_delay),
            keepalive_interval: config.keepalive_interval,
            keepalive_timeout: config.keepalive_timeout,
            tablet_sender: Some(tablet_sender),
            identity: config.identity,
        };

        let pool_config = PoolConfig {
            connection_config,
            pool_size: config.connection_pool_size,
            can_use_shard_aware_port: !config.disallow_shard_aware_port,
        };

        #[cfg(feature = "metrics")]
        let metrics = Arc::new(Metrics::new());

        let cluster = Cluster::new(
            known_nodes,
            pool_config,
            config.keyspaces_to_fetch,
            config.fetch_schema_metadata,
            config.metadata_request_serverside_timeout,
            config.host_filter,
            config.cluster_metadata_refresh_interval,
            tablet_receiver,
            #[cfg(feature = "metrics")]
            Arc::clone(&metrics),
        )
        .await?;

        let default_execution_profile_handle = config.default_execution_profile_handle;

        let session = Self {
            cluster,
            default_execution_profile_handle,
            schema_agreement_interval: config.schema_agreement_interval,
            #[cfg(feature = "metrics")]
            metrics,
            schema_agreement_timeout: config.schema_agreement_timeout,
            schema_agreement_automatic_waiting: config.schema_agreement_automatic_waiting,
            refresh_metadata_on_auto_schema_agreement: config
                .refresh_metadata_on_auto_schema_agreement,
            keyspace_name: Arc::new(ArcSwapOption::default()), // will be set by use_keyspace
            tracing_info_fetch_attempts: config.tracing_info_fetch_attempts,
            tracing_info_fetch_interval: config.tracing_info_fetch_interval,
            tracing_info_fetch_consistency: config.tracing_info_fetch_consistency,
        };

        if let Some(keyspace_name) = config.used_keyspace {
            session
                .use_keyspace(keyspace_name, config.keyspace_case_sensitive)
                .await?;
        }

        Ok(session)
    }

    async fn do_query_unpaged(
        &self,
        statement: &Statement,
        values: impl SerializeRow,
    ) -> Result<QueryResult, ExecutionError> {
        let (result, paging_state_response) = self
            .query(statement, values, None, PagingState::start())
            .await?;
        if !paging_state_response.finished() {
            error!(
                "Unpaged unprepared query returned a non-empty paging state! This is a driver-side or server-side bug."
            );
            return Err(ExecutionError::LastAttemptError(
                RequestAttemptError::NonfinishedPagingState,
            ));
        }
        Ok(result)
    }

    async fn do_query_single_page(
        &self,
        statement: &Statement,
        values: impl SerializeRow,
        paging_state: PagingState,
    ) -> Result<(QueryResult, PagingStateResponse), ExecutionError> {
        self.query(
            statement,
            values,
            Some(statement.get_validated_page_size()),
            paging_state,
        )
        .await
    }

    /// Sends a request to the database.
    /// Optionally continues fetching results from a saved point.
    ///
    /// This is now an internal method only.
    ///
    /// Tl;dr: use [Session::query_unpaged], [Session::query_single_page] or [Session::query_iter] instead.
    ///
    /// The rationale is that we believe that paging is so important concept (and it has shown to be error-prone as well)
    /// that we need to require users to make a conscious decision to use paging or not. For that, we expose
    /// the aforementioned 3 methods clearly differing in naming and API, so that no unconscious choices about paging
    /// should be made.
    async fn query(
        &self,
        statement: &Statement,
        values: impl SerializeRow,
        page_size: Option<PageSize>,
        paging_state: PagingState,
    ) -> Result<(QueryResult, PagingStateResponse), ExecutionError> {
        let execution_profile = statement
            .get_execution_profile_handle()
            .unwrap_or_else(|| self.get_default_execution_profile_handle())
            .access();

        let statement_info = RoutingInfo {
            consistency: statement
                .config
                .consistency
                .unwrap_or(execution_profile.consistency),
            serial_consistency: statement
                .config
                .serial_consistency
                .unwrap_or(execution_profile.serial_consistency),
            ..Default::default()
        };

        let span = RequestSpan::new_query(&statement.contents);
        let span_ref = &span;
        let (run_request_result, coordinator): (
            RunRequestResult<NonErrorQueryResponse>,
            Coordinator,
        ) = self
            .run_request(
                statement_info,
                &statement.config,
                execution_profile,
                |connection: Arc<Connection>,
                 consistency: Consistency,
                 execution_profile: &ExecutionProfileInner| {
                    let serial_consistency = statement
                        .config
                        .serial_consistency
                        .unwrap_or(execution_profile.serial_consistency);
                    // Needed to avoid moving query and values into async move block
                    let values_ref = &values;
                    let paging_state_ref = &paging_state;
                    async move {
                        if values_ref.is_empty() {
                            span_ref.record_request_size(0);
                            connection
                                .query_raw_with_consistency(
                                    statement,
                                    consistency,
                                    serial_consistency,
                                    page_size,
                                    paging_state_ref.clone(),
                                )
                                .await
                                .and_then(QueryResponse::into_non_error_query_response)
                        } else {
                            let prepared = connection.prepare(statement).await?;
                            let serialized = prepared.serialize_values(values_ref)?;
                            span_ref.record_request_size(serialized.buffer_size());
                            connection
                                .execute_raw_with_consistency(
                                    &prepared,
                                    &serialized,
                                    consistency,
                                    serial_consistency,
                                    page_size,
                                    paging_state_ref.clone(),
                                )
                                .await
                                .and_then(QueryResponse::into_non_error_query_response)
                        }
                    }
                },
                &span,
            )
            .instrument(span.span().clone())
            .await?;

        let response = match run_request_result {
            RunRequestResult::IgnoredWriteError => NonErrorQueryResponse {
                response: NonErrorResponse::Result(result::Result::Void),
                tracing_id: None,
                warnings: Vec::new(),
            },
            RunRequestResult::Completed(response) => response,
        };

        let (result, paging_state_response) =
            response.into_query_result_and_paging_state(coordinator)?;
        span.record_result_fields(&result);

        Ok((result, paging_state_response))
    }

    async fn handle_set_keyspace_response(
        &self,
        response: &NonErrorQueryResponse,
    ) -> Result<(), UseKeyspaceError> {
        if let Some(set_keyspace) = response.as_set_keyspace() {
            debug!(
                "Detected USE KEYSPACE query, setting session's keyspace to {}",
                set_keyspace.keyspace_name
            );
            self.use_keyspace(set_keyspace.keyspace_name.clone(), true)
                .await?;
        }

        Ok(())
    }

    async fn handle_auto_await_schema_agreement(
        &self,
        response: &NonErrorQueryResponse,
        coordinator_id: Uuid,
    ) -> Result<(), ExecutionError> {
        if self.schema_agreement_automatic_waiting {
            if response.as_schema_change().is_some() {
                self.await_schema_agreement_with_required_node(Some(coordinator_id))
                    .await?;
            }

            if self.refresh_metadata_on_auto_schema_agreement
                && response.as_schema_change().is_some()
            {
                self.refresh_metadata().await?;
            }
        }

        Ok(())
    }

    async fn do_query_iter(
        &self,
        statement: Statement,
        values: impl SerializeRow,
    ) -> Result<QueryPager, PagerExecutionError> {
        let execution_profile = statement
            .get_execution_profile_handle()
            .unwrap_or_else(|| self.get_default_execution_profile_handle())
            .access();

        if values.is_empty() {
            QueryPager::new_for_query(
                statement,
                execution_profile,
                self.cluster.get_state(),
                #[cfg(feature = "metrics")]
                Arc::clone(&self.metrics),
            )
            .await
            .map_err(PagerExecutionError::NextPageError)
        } else {
            // Making QueryPager::new_for_query work with values is too hard (if even possible)
            // so instead of sending one prepare to a specific connection on each iterator query,
            // we fully prepare a statement beforehand.
            let prepared = self.prepare_nongeneric(&statement).await?;
            let values = prepared.serialize_values(&values)?;
            QueryPager::new_for_prepared_statement(PreparedPagerConfig {
                prepared,
                values,
                execution_profile,
                cluster_state: self.cluster.get_state(),
                #[cfg(feature = "metrics")]
                metrics: Arc::clone(&self.metrics),
            })
            .await
            .map_err(PagerExecutionError::NextPageError)
        }
    }

    /// Prepares a statement on the server side and returns a prepared statement,
    /// which can later be used to perform more efficient requests.
    ///
    /// The statement is prepared on all nodes. This function finishes once all nodes respond
    /// with either success or an error.
    ///
    /// Prepared statements are much faster than unprepared statements:
    /// * Database doesn't need to parse the statement string upon each execution (only once)
    /// * They are properly load balanced using token aware routing
    ///
    /// > ***Warning***\
    /// > For token/shard aware load balancing to work properly, all partition key values
    /// > must be sent as bound values
    /// > (see [performance section](https://rust-driver.docs.scylladb.com/stable/statements/prepared.html#performance))
    ///
    /// See [the book](https://rust-driver.docs.scylladb.com/stable/statements/prepared.html) for more information.
    /// See the documentation of [`PreparedStatement`].
    ///
    /// # Arguments
    /// * `statement` - statement to prepare, can be just a `&str` or the [`Statement`] struct.
    ///
    /// # Example
    /// ```rust
    /// # use scylla::client::session::Session;
    /// # use std::error::Error;
    /// # async fn check_only_compiles(session: &Session) -> Result<(), Box<dyn Error>> {
    /// use scylla::statement::prepared::PreparedStatement;
    ///
    /// // Prepare the statement for later execution
    /// let prepared: PreparedStatement = session
    ///     .prepare("INSERT INTO ks.tab (a) VALUES(?)")
    ///     .await?;
    ///
    /// // Execute the prepared statement with some values, just like an unprepared statement.
    /// let to_insert: i32 = 12345;
    /// session.execute_unpaged(&prepared, (to_insert,)).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn prepare(
        &self,
        statement: impl Into<Statement>,
    ) -> Result<PreparedStatement, PrepareError> {
        let statement = statement.into();
        self.prepare_nongeneric(&statement).await
    }

    // Introduced to avoid monomorphisation of this large function.
    async fn prepare_nongeneric(
        &self,
        statement: &Statement,
    ) -> Result<PreparedStatement, PrepareError> {
        let cluster_state = self.get_cluster_state();

        // Start by attempting preparation on a single (random) connection to every node.
        {
            let mut connections_to_nodes = cluster_state.iter_working_connections_to_nodes()?;
            let on_all_nodes_result =
                Self::prepare_on_all(statement, &cluster_state, &mut connections_to_nodes).await;
            if let Ok(prepared) = on_all_nodes_result {
                // We succeeded in preparing the statement on at least one node. We're done.
                // Other nodes could have failed to prepare the statement, but this will be handled
                // as `DbError::Unprepared` upon execution, followed by a repreparation attempt.
                return Ok(prepared);
            }
        }

        // We could have been just unlucky: we could have possibly chosen random connections all of which were defunct
        // (one possibility is that we targeted overloaded shards).
        // Let's try again, this time on connections to every shard. This is a "last call" fallback.
        {
            let mut connections_to_shards = cluster_state.iter_working_connections_to_shards()?;

            Self::prepare_on_all(statement, &cluster_state, &mut connections_to_shards).await
        }
    }

    /// Prepares the statement on all given connections.
    /// These are intended to be connections to either all nodes or all shards.
    ///
    /// ASSUMPTION: the `working_connections` Iterator is nonempty.
    ///
    /// Returns:
    /// - `Ok(PreparedStatement)`, if preparation succeeded on at least one connection;
    /// - `Err(PrepareError)`, if no connection is working or preparation failed on all attempted connections.
    // TODO: There are no timeouts here. So, just one stuck node freezes the driver here, potentially indefinitely long.
    // Also, what the driver requires to get from the cluster is the prepared statement metadata.
    // It suffices that it gets only one copy of it, just from one success response. Therefore, it's a possible
    // optimisation that the function only waits for one preparation to finish successfully, and then it returns.
    // For it to be done, other preparations must continue in the background, on a separate tokio task.
    // Describing issue: #1332.
    async fn prepare_on_all(
        statement: &Statement,
        cluster_state: &ClusterState,
        working_connections: &mut (dyn Iterator<Item = Arc<Connection>> + Send),
    ) -> Result<PreparedStatement, PrepareError> {
        // Find the first result that is Ok, or Err if all failed.
        let preparations =
            working_connections.map(|c| async move { c.prepare_raw(statement).await });
        let raw_prepared_statements_results = join_all(preparations).await;

        let mut raw_prepared_statements_results_iter = raw_prepared_statements_results.into_iter();

        // Safety: We pass a nonempty iterator, so there will be at least one result.
        let first_ok_or_error = raw_prepared_statements_results_iter
            .by_ref()
            .find_or_first(|res| res.is_ok())
            .unwrap(); // Safety: there is at least one connection.

        let mut prepared: PreparedStatement = first_ok_or_error
            .map_err(|first_attempt| PrepareError::AllAttemptsFailed { first_attempt })?
            .into_prepared_statement();

        // Validate prepared ids equality.
        for another_raw_prepared in raw_prepared_statements_results_iter.flatten() {
            if prepared.get_id() != &another_raw_prepared.prepared_response.id {
                tracing::error!(
                    "Got differing ids upon statement preparation: statement \"{}\", id1: {:?}, id2: {:?}",
                    prepared.get_statement(),
                    prepared.get_id(),
                    another_raw_prepared.prepared_response.id
                );
                return Err(PrepareError::PreparedStatementIdsMismatch);
            }

            // Collect all tracing ids from prepare() queries in the final result
            prepared
                .prepare_tracing_ids
                .extend(another_raw_prepared.tracing_id);
        }

        // This is the first preparation that succeeded.
        // Let's return the PreparedStatement.
        prepared.set_partitioner_name(
            Self::extract_partitioner_name(&prepared, cluster_state)
                .and_then(PartitionerName::from_str)
                .unwrap_or_default(),
        );

        Ok(prepared)
    }

    fn extract_partitioner_name<'a>(
        prepared: &PreparedStatement,
        cluster_state: &'a ClusterState,
    ) -> Option<&'a str> {
        let table_spec = prepared.get_table_spec()?;
        cluster_state
            .keyspaces
            .get(table_spec.ks_name())?
            .tables
            .get(table_spec.table_name())?
            .partitioner
            .as_deref()
    }

    async fn do_execute_unpaged(
        &self,
        prepared: &PreparedStatement,
        values: impl SerializeRow,
    ) -> Result<QueryResult, ExecutionError> {
        let serialized_values = prepared.serialize_values(&values)?;
        let (result, paging_state) = self
            .execute(prepared, &serialized_values, None, PagingState::start())
            .await?;
        if !paging_state.finished() {
            error!(
                "Unpaged prepared query returned a non-empty paging state! This is a driver-side or server-side bug."
            );
            return Err(ExecutionError::LastAttemptError(
                RequestAttemptError::NonfinishedPagingState,
            ));
        }
        Ok(result)
    }

    async fn do_execute_single_page(
        &self,
        prepared: &PreparedStatement,
        values: impl SerializeRow,
        paging_state: PagingState,
    ) -> Result<(QueryResult, PagingStateResponse), ExecutionError> {
        let serialized_values = prepared.serialize_values(&values)?;
        let page_size = prepared.get_validated_page_size();
        self.execute(prepared, &serialized_values, Some(page_size), paging_state)
            .await
    }

    /// Sends a prepared request to the database, optionally continuing from a saved point.
    ///
    /// This is now an internal method only.
    ///
    /// Tl;dr: use [Session::execute_unpaged], [Session::execute_single_page] or [Session::execute_iter] instead.
    ///
    /// The rationale is that we believe that paging is so important concept (and it has shown to be error-prone as well)
    /// that we need to require users to make a conscious decision to use paging or not. For that, we expose
    /// the aforementioned 3 methods clearly differing in naming and API, so that no unconscious choices about paging
    /// should be made.
    async fn execute(
        &self,
        prepared: &PreparedStatement,
        serialized_values: &SerializedValues,
        page_size: Option<PageSize>,
        paging_state: PagingState,
    ) -> Result<(QueryResult, PagingStateResponse), ExecutionError> {
        let paging_state_ref = &paging_state;

        let (partition_key, token) = prepared
            .extract_partition_key_and_calculate_token(
                prepared.get_partitioner_name(),
                serialized_values,
            )
            .map_err(PartitionKeyError::into_execution_error)?
            .unzip();

        let execution_profile = prepared
            .get_execution_profile_handle()
            .unwrap_or_else(|| self.get_default_execution_profile_handle())
            .access();

        let table_spec = prepared.get_table_spec();

        let statement_info = RoutingInfo {
            consistency: prepared
                .config
                .consistency
                .unwrap_or(execution_profile.consistency),
            serial_consistency: prepared
                .config
                .serial_consistency
                .unwrap_or(execution_profile.serial_consistency),
            token,
            table: table_spec,
            is_confirmed_lwt: prepared.is_confirmed_lwt(),
        };

        let span = RequestSpan::new_prepared(
            partition_key.as_ref().map(|pk| pk.iter()),
            token,
            serialized_values.buffer_size(),
        );

        if !span.span().is_disabled() {
            if let (Some(table_spec), Some(token)) = (statement_info.table, token) {
                let cluster_state = self.get_cluster_state();
                let replicas = cluster_state.get_token_endpoints_iter(table_spec, token);
                span.record_replicas(replicas)
            }
        }

        let (run_request_result, coordinator): (
            RunRequestResult<NonErrorQueryResponse>,
            Coordinator,
        ) = self
            .run_request(
                statement_info,
                &prepared.config,
                execution_profile,
                |connection: Arc<Connection>,
                 consistency: Consistency,
                 execution_profile: &ExecutionProfileInner| {
                    let serial_consistency = prepared
                        .config
                        .serial_consistency
                        .unwrap_or(execution_profile.serial_consistency);
                    async move {
                        connection
                            .execute_raw_with_consistency(
                                prepared,
                                serialized_values,
                                consistency,
                                serial_consistency,
                                page_size,
                                paging_state_ref.clone(),
                            )
                            .await
                            .and_then(QueryResponse::into_non_error_query_response)
                    }
                },
                &span,
            )
            .instrument(span.span().clone())
            .await?;

        let response = match run_request_result {
            RunRequestResult::IgnoredWriteError => NonErrorQueryResponse {
                response: NonErrorResponse::Result(result::Result::Void),
                tracing_id: None,
                warnings: Vec::new(),
            },
            RunRequestResult::Completed(response) => response,
        };

        let (result, paging_state_response) =
            response.into_query_result_and_paging_state(coordinator)?;
        span.record_result_fields(&result);

        Ok((result, paging_state_response))
    }

    async fn do_execute_iter(
        &self,
        prepared: PreparedStatement,
        values: impl SerializeRow,
    ) -> Result<QueryPager, PagerExecutionError> {
        let serialized_values = prepared.serialize_values(&values)?;

        let execution_profile = prepared
            .get_execution_profile_handle()
            .unwrap_or_else(|| self.get_default_execution_profile_handle())
            .access();

        QueryPager::new_for_prepared_statement(PreparedPagerConfig {
            prepared,
            values: serialized_values,
            execution_profile,
            cluster_state: self.cluster.get_state(),
            #[cfg(feature = "metrics")]
            metrics: Arc::clone(&self.metrics),
        })
        .await
        .map_err(PagerExecutionError::NextPageError)
    }

    async fn do_batch(
        &self,
        batch: &Batch,
        values: impl BatchValues,
    ) -> Result<QueryResult, ExecutionError> {
        // Shard-awareness behavior for batch will be to pick shard based on first batch statement's shard
        // If users batch statements by shard, they will be rewarded with full shard awareness

        // check to ensure that we don't send a batch statement with more than u16::MAX queries
        let batch_statements_length = batch.statements.len();
        if batch_statements_length > u16::MAX as usize {
            return Err(ExecutionError::BadQuery(
                BadQuery::TooManyQueriesInBatchStatement(batch_statements_length),
            ));
        }

        let execution_profile = batch
            .get_execution_profile_handle()
            .unwrap_or_else(|| self.get_default_execution_profile_handle())
            .access();

        let consistency = batch
            .config
            .consistency
            .unwrap_or(execution_profile.consistency);

        let serial_consistency = batch
            .config
            .serial_consistency
            .unwrap_or(execution_profile.serial_consistency);

        let (first_value_token, values) =
            batch_values::peek_first_token(values, batch.statements.first())?;
        let values_ref = &values;

        let table_spec =
            if let Some(BatchStatement::PreparedStatement(ps)) = batch.statements.first() {
                ps.get_table_spec()
            } else {
                None
            };

        let statement_info = RoutingInfo {
            consistency,
            serial_consistency,
            token: first_value_token,
            table: table_spec,
            is_confirmed_lwt: false,
        };

        let span = RequestSpan::new_batch();

        let (run_request_result, coordinator): (
            RunRequestResult<NonErrorQueryResponse>,
            Coordinator,
        ) = self
            .run_request(
                statement_info,
                &batch.config,
                execution_profile,
                |connection: Arc<Connection>,
                 consistency: Consistency,
                 execution_profile: &ExecutionProfileInner| {
                    let serial_consistency = batch
                        .config
                        .serial_consistency
                        .unwrap_or(execution_profile.serial_consistency);
                    async move {
                        connection
                            .batch_with_consistency(
                                batch,
                                values_ref,
                                consistency,
                                serial_consistency,
                            )
                            .await
                            .and_then(QueryResponse::into_non_error_query_response)
                    }
                },
                &span,
            )
            .instrument(span.span().clone())
            .await?;

        let result = match run_request_result {
            RunRequestResult::IgnoredWriteError => QueryResult::mock_empty(coordinator),
            RunRequestResult::Completed(non_error_query_response) => {
                let result = non_error_query_response.into_query_result(coordinator)?;
                span.record_result_fields(&result);
                result
            }
        };

        Ok(result)
    }

    /// Prepares all statements within the batch and returns a new batch where every
    /// statement is prepared.
    /// /// # Example
    /// ```rust
    /// # extern crate scylla;
    /// # use scylla::client::session::Session;
    /// # use std::error::Error;
    /// # async fn check_only_compiles(session: &Session) -> Result<(), Box<dyn Error>> {
    /// use scylla::statement::batch::Batch;
    ///
    /// // Create a batch statement with unprepared statements
    /// let mut batch: Batch = Default::default();
    /// batch.append_statement("INSERT INTO ks.simple_unprepared1 VALUES(?, ?)");
    /// batch.append_statement("INSERT INTO ks.simple_unprepared2 VALUES(?, ?)");
    ///
    /// // Prepare all statements in the batch at once
    /// let prepared_batch: Batch = session.prepare_batch(&batch).await?;
    ///
    /// // Specify bound values to use with each statement
    /// let batch_values = ((1_i32, 2_i32),
    ///                     (3_i32, 4_i32));
    ///
    /// // Run the prepared batch
    /// session.batch(&prepared_batch, batch_values).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn prepare_batch(&self, batch: &Batch) -> Result<Batch, PrepareError> {
        let mut prepared_batch = batch.clone();

        try_join_all(
            prepared_batch
                .statements
                .iter_mut()
                .map(|statement| async move {
                    if let BatchStatement::Query(query) = statement {
                        let prepared = self.prepare_nongeneric(query).await?;
                        *statement = BatchStatement::PreparedStatement(prepared);
                    }
                    Ok::<(), PrepareError>(())
                }),
        )
        .await?;

        Ok(prepared_batch)
    }

    /// Sends `USE <keyspace_name>` request on all connections\
    /// This allows to write `SELECT * FROM table` instead of `SELECT * FROM keyspace.table`\
    ///
    /// Note that even failed `use_keyspace` can change currently used keyspace - the request is sent on all connections and
    /// can overwrite previously used keyspace.
    ///
    /// Call only one `use_keyspace` at a time.\
    /// Trying to do two `use_keyspace` requests simultaneously with different names
    /// can end with some connections using one keyspace and the rest using the other.
    ///
    /// See [the book](https://rust-driver.docs.scylladb.com/stable/statements/usekeyspace.html) for more information
    ///
    /// # Arguments
    ///
    /// * `keyspace_name` - keyspace name to use,
    ///   keyspace names can have up to 48 alphanumeric characters and contain underscores
    /// * `case_sensitive` - if set to true the generated statement will put keyspace name in quotes
    /// # Example
    /// ```rust
    /// # use scylla::client::session::Session;
    /// # use scylla::client::session_builder::SessionBuilder;
    /// # use scylla::client::Compression;
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// # let session = SessionBuilder::new().known_node("127.0.0.1:9042").build().await?;
    /// session
    ///     .query_unpaged("INSERT INTO my_keyspace.tab (a) VALUES ('test1')", &[])
    ///     .await?;
    ///
    /// session.use_keyspace("my_keyspace", false).await?;
    ///
    /// // Now we can omit keyspace name in the statement
    /// session
    ///     .query_unpaged("INSERT INTO tab (a) VALUES ('test2')", &[])
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn use_keyspace(
        &self,
        keyspace_name: impl Into<String>,
        case_sensitive: bool,
    ) -> Result<(), UseKeyspaceError> {
        let keyspace_name = keyspace_name.into();
        self.keyspace_name
            .store(Some(Arc::new(keyspace_name.clone())));

        // Trying to pass keyspace as bound value in "USE ?" doesn't work
        // So we have to create a string for query: "USE " + new_keyspace
        // To avoid any possible CQL injections it's good to verify that the name is valid
        let verified_ks_name = VerifiedKeyspaceName::new(keyspace_name, case_sensitive)?;

        self.cluster.use_keyspace(verified_ks_name).await?;

        Ok(())
    }

    /// Manually trigger a metadata refresh\
    /// The driver will fetch current nodes in the cluster and update its metadata
    ///
    /// Normally this is not needed,
    /// the driver should automatically detect all metadata changes in the cluster
    pub async fn refresh_metadata(&self) -> Result<(), MetadataError> {
        self.cluster.refresh_metadata().await
    }

    /// Access metrics collected by the driver\
    /// Driver collects various metrics like number of queries or query latencies.
    /// They can be read using this method
    #[cfg(feature = "metrics")]
    pub fn get_metrics(&self) -> Arc<Metrics> {
        Arc::clone(&self.metrics)
    }

    /// Access cluster state visible by the driver.
    ///
    /// Driver collects various information about network topology or schema.
    /// It can be read using this method.
    pub fn get_cluster_state(&self) -> Arc<ClusterState> {
        self.cluster.get_state()
    }

    /// Get [`TracingInfo`] of a traced query performed earlier
    ///
    /// See [the book](https://rust-driver.docs.scylladb.com/stable/tracing/tracing.html)
    /// for more information about query tracing
    pub async fn get_tracing_info(&self, tracing_id: &Uuid) -> Result<TracingInfo, TracingError> {
        // tracing_info_fetch_attempts is NonZeroU32 so at least one attempt will be made
        for _ in 0..self.tracing_info_fetch_attempts.get() {
            let current_try: Option<TracingInfo> = self
                .try_getting_tracing_info(tracing_id, Some(self.tracing_info_fetch_consistency))
                .await?;

            match current_try {
                Some(tracing_info) => return Ok(tracing_info),
                None => tokio::time::sleep(self.tracing_info_fetch_interval).await,
            };
        }

        Err(TracingError::EmptyResults)
    }

    /// Gets the name of the keyspace that is currently set, or `None` if no
    /// keyspace was set.
    ///
    /// It will initially return the name of the keyspace that was set
    /// in the session configuration, but calling `use_keyspace` will update
    /// it.
    ///
    /// Note: the return value might be wrong if `use_keyspace` was called
    /// concurrently or it previously failed. It is also unspecified
    /// if `get_keyspace` is called concurrently with `use_keyspace`.
    #[inline]
    pub fn get_keyspace(&self) -> Option<Arc<String>> {
        self.keyspace_name.load_full()
    }

    // Tries getting the tracing info
    // If the queries return 0 rows then returns None - the information didn't reach this node yet
    // If there is some other error returns this error
    async fn try_getting_tracing_info(
        &self,
        tracing_id: &Uuid,
        consistency: Option<Consistency>,
    ) -> Result<Option<TracingInfo>, TracingError> {
        // Query system_traces.sessions for TracingInfo
        let mut traces_session_query =
            Statement::new(crate::observability::tracing::TRACES_SESSION_QUERY_STR);
        traces_session_query.config.consistency = consistency;
        traces_session_query.set_page_size(TRACING_QUERY_PAGE_SIZE);

        // Query system_traces.events for TracingEvents
        let mut traces_events_query =
            Statement::new(crate::observability::tracing::TRACES_EVENTS_QUERY_STR);
        traces_events_query.config.consistency = consistency;
        traces_events_query.set_page_size(TRACING_QUERY_PAGE_SIZE);

        let (traces_session_res, traces_events_res) = tokio::try_join!(
            self.do_query_unpaged(&traces_session_query, (tracing_id,)),
            self.do_query_unpaged(&traces_events_query, (tracing_id,))
        )?;

        // Get tracing info
        let maybe_tracing_info: Option<TracingInfo> = traces_session_res
            .into_rows_result()
            .map_err(TracingError::TracesSessionIntoRowsResultError)?
            .maybe_first_row()
            .map_err(|err| match err {
                MaybeFirstRowError::TypeCheckFailed(e) => {
                    TracingError::TracesSessionInvalidColumnType(e)
                }
                MaybeFirstRowError::DeserializationFailed(e) => {
                    TracingError::TracesSessionDeserializationFailed(e)
                }
            })?;

        let mut tracing_info = match maybe_tracing_info {
            None => return Ok(None),
            Some(tracing_info) => tracing_info,
        };

        // Get tracing events
        let tracing_event_rows_result = traces_events_res
            .into_rows_result()
            .map_err(TracingError::TracesEventsIntoRowsResultError)?;
        let tracing_event_rows = tracing_event_rows_result.rows().map_err(|err| match err {
            RowsError::TypeCheckFailed(err) => TracingError::TracesEventsInvalidColumnType(err),
        })?;

        tracing_info.events = tracing_event_rows
            .collect::<Result<_, _>>()
            .map_err(TracingError::TracesEventsDeserializationFailed)?;

        if tracing_info.events.is_empty() {
            return Ok(None);
        }

        Ok(Some(tracing_info))
    }

    /// This method allows to easily run a request using load balancing, retry policy etc.
    /// Requires some information about the request and a closure.
    /// The closure is used to execute the request once on a chosen connection.
    /// - query will use connection.query()
    /// - execute will use connection.execute()
    ///
    /// If this closure fails with some errors, retry policy is used to perform retries.
    /// On success, this request's result is returned.
    // I tried to make this closures take a reference instead of an Arc but failed
    // maybe once async closures get stabilized this can be fixed
    async fn run_request<'a, QueryFut>(
        &'a self,
        statement_info: RoutingInfo<'a>,
        statement_config: &'a StatementConfig,
        execution_profile: Arc<ExecutionProfileInner>,
        run_request_once: impl Fn(Arc<Connection>, Consistency, &ExecutionProfileInner) -> QueryFut,
        request_span: &'a RequestSpan,
    ) -> Result<(RunRequestResult<NonErrorQueryResponse>, Coordinator), ExecutionError>
    where
        QueryFut: Future<Output = Result<NonErrorQueryResponse, RequestAttemptError>>,
    {
        let history_listener_and_id: Option<(&'a dyn HistoryListener, history::RequestId)> =
            statement_config
                .history_listener
                .as_ref()
                .map(|hl| (&**hl, hl.log_request_start()));

        let load_balancer = statement_config
            .load_balancing_policy
            .as_deref()
            .unwrap_or(execution_profile.load_balancing_policy.as_ref());

        let runner = async {
            let cluster_state = self.cluster.get_state();
            let request_plan =
                load_balancing::Plan::new(load_balancer, &statement_info, &cluster_state);

            // If a speculative execution policy is used to run request, request_plan has to be shared
            // between different async functions. This struct helps to wrap request_plan in mutex so it
            // can be shared safely.
            struct SharedPlan<'a, I>
            where
                I: Iterator<Item = (NodeRef<'a>, Shard)>,
            {
                iter: std::sync::Mutex<I>,
            }

            impl<'a, I> Iterator for &SharedPlan<'a, I>
            where
                I: Iterator<Item = (NodeRef<'a>, Shard)>,
            {
                type Item = (NodeRef<'a>, Shard);

                fn next(&mut self) -> Option<Self::Item> {
                    self.iter.lock().unwrap().next()
                }
            }

            let retry_policy = statement_config
                .retry_policy
                .as_deref()
                .unwrap_or(&*execution_profile.retry_policy);

            let speculative_policy = execution_profile.speculative_execution_policy.as_ref();

            match speculative_policy {
                Some(speculative) if statement_config.is_idempotent => {
                    let shared_request_plan = SharedPlan {
                        iter: std::sync::Mutex::new(request_plan),
                    };

                    let request_runner_generator = |is_speculative: bool| {
                        let history_data: Option<HistoryData> = history_listener_and_id
                            .as_ref()
                            .map(|(history_listener, request_id)| {
                                let speculative_id: Option<history::SpeculativeId> =
                                    if is_speculative {
                                        Some(
                                            history_listener.log_new_speculative_fiber(*request_id),
                                        )
                                    } else {
                                        None
                                    };
                                HistoryData {
                                    listener: *history_listener,
                                    request_id: *request_id,
                                    speculative_id,
                                }
                            });

                        if is_speculative {
                            request_span.inc_speculative_executions();
                        }

                        self.run_request_speculative_fiber(
                            &shared_request_plan,
                            &run_request_once,
                            &execution_profile,
                            ExecuteRequestContext {
                                is_idempotent: statement_config.is_idempotent,
                                consistency_set_on_statement: statement_config.consistency,
                                retry_session: retry_policy.new_session(),
                                history_data,
                                load_balancing_policy: load_balancer,
                                query_info: &statement_info,
                                request_span,
                            },
                        )
                    };

                    let context = speculative_execution::Context {
                        #[cfg(feature = "metrics")]
                        metrics: Arc::clone(&self.metrics),
                    };

                    speculative_execution::execute(
                        speculative.as_ref(),
                        &context,
                        request_runner_generator,
                    )
                    .await
                }
                _ => {
                    let history_data: Option<HistoryData> =
                        history_listener_and_id
                            .as_ref()
                            .map(|(history_listener, request_id)| HistoryData {
                                listener: *history_listener,
                                request_id: *request_id,
                                speculative_id: None,
                            });
                    self.run_request_speculative_fiber(
                        request_plan,
                        &run_request_once,
                        &execution_profile,
                        ExecuteRequestContext {
                            is_idempotent: statement_config.is_idempotent,
                            consistency_set_on_statement: statement_config.consistency,
                            retry_session: retry_policy.new_session(),
                            history_data,
                            load_balancing_policy: load_balancer,
                            query_info: &statement_info,
                            request_span,
                        },
                    )
                    .await
                    .unwrap_or(Err(RequestError::EmptyPlan))
                }
            }
        };

        let effective_timeout = statement_config
            .request_timeout
            .or(execution_profile.request_timeout);
        let result = match effective_timeout {
            Some(timeout) => tokio::time::timeout(timeout, runner).await.unwrap_or_else(
                |_: tokio::time::error::Elapsed| {
                    #[cfg(feature = "metrics")]
                    self.metrics.inc_request_timeouts();
                    Err(RequestError::RequestTimeout(timeout))
                },
            ),
            None => runner.await,
        };

        if let Some((history_listener, request_id)) = history_listener_and_id {
            match &result {
                Ok(_) => history_listener.log_request_success(request_id),
                Err(e) => history_listener.log_request_error(request_id, e),
            }
        }

        // Automatically handle meaningful responses.
        if let Ok((RunRequestResult::Completed(ref response), ref coordinator)) = result {
            self.handle_set_keyspace_response(response).await?;
            self.handle_auto_await_schema_agreement(response, coordinator.node().host_id)
                .await?;
        }

        result.map_err(RequestError::into_execution_error)
    }

    /// Executes the closure `run_request_once`, provided the load balancing plan and some information
    /// about the request, including retry session.
    /// If request fails, retry session is used to perform retries.
    ///
    /// Returns None, if provided plan is empty.
    async fn run_request_speculative_fiber<'a, QueryFut>(
        &'a self,
        request_plan: impl Iterator<Item = (NodeRef<'a>, Shard)>,
        run_request_once: impl Fn(Arc<Connection>, Consistency, &ExecutionProfileInner) -> QueryFut,
        execution_profile: &ExecutionProfileInner,
        mut context: ExecuteRequestContext<'a>,
    ) -> Option<Result<(RunRequestResult<NonErrorQueryResponse>, Coordinator), RequestError>>
    where
        QueryFut: Future<Output = Result<NonErrorQueryResponse, RequestAttemptError>>,
    {
        let mut last_error: Option<RequestError> = None;
        let mut current_consistency: Consistency = context
            .consistency_set_on_statement
            .unwrap_or(execution_profile.consistency);

        'nodes_in_plan: for (node, shard) in request_plan {
            let span = trace_span!("Executing request", node = %node.address, shard = %shard);
            'same_node_retries: loop {
                trace!(parent: &span, "Execution started");
                let connection = match node.connection_for_shard(shard).await {
                    Ok(connection) => connection,
                    Err(e) => {
                        trace!(
                            parent: &span,
                            error = %e,
                            "Choosing connection failed"
                        );
                        last_error = Some(e.into());
                        // Broken connection doesn't count as a failed request, don't log in metrics
                        continue 'nodes_in_plan;
                    }
                };
                context.request_span.record_shard_id(&connection);

                #[cfg(feature = "metrics")]
                self.metrics.inc_total_nonpaged_queries();
                let request_start = std::time::Instant::now();

                let connect_address = connection.get_connect_address();
                trace!(
                    parent: &span,
                    connection = %connect_address,
                    "Sending"
                );
                let coordinator =
                    Coordinator::new(node, node.sharder().is_some().then_some(shard), &connection);

                let attempt_id: Option<history::AttemptId> =
                    context.log_attempt_start(connect_address);
                let request_result: Result<NonErrorQueryResponse, RequestAttemptError> =
                    run_request_once(connection, current_consistency, execution_profile)
                        .instrument(span.clone())
                        .await;

                let elapsed = request_start.elapsed();
                let request_error: RequestAttemptError = match request_result {
                    Ok(response) => {
                        trace!(parent: &span, "Request succeeded");
                        #[cfg(feature = "metrics")]
                        let _ = self.metrics.log_query_latency(elapsed.as_millis() as u64);
                        context.log_attempt_success(&attempt_id);
                        context.load_balancing_policy.on_request_success(
                            context.query_info,
                            elapsed,
                            node,
                        );
                        return Some(Ok((RunRequestResult::Completed(response), coordinator)));
                    }
                    Err(e) => {
                        trace!(
                            parent: &span,
                            last_error = %e,
                            "Request failed"
                        );
                        #[cfg(feature = "metrics")]
                        self.metrics.inc_failed_nonpaged_queries();
                        context.load_balancing_policy.on_request_failure(
                            context.query_info,
                            elapsed,
                            node,
                            &e,
                        );
                        e
                    }
                };

                // Use retry policy to decide what to do next
                let query_info = RequestInfo {
                    error: &request_error,
                    is_idempotent: context.is_idempotent,
                    consistency: context
                        .consistency_set_on_statement
                        .unwrap_or(execution_profile.consistency),
                };

                let retry_decision = context.retry_session.decide_should_retry(query_info);
                trace!(
                    parent: &span,
                    retry_decision = ?retry_decision
                );

                context.log_attempt_error(&attempt_id, &request_error, &retry_decision);

                last_error = Some(request_error.into());

                match retry_decision {
                    RetryDecision::RetrySameTarget(new_cl) => {
                        #[cfg(feature = "metrics")]
                        self.metrics.inc_retries_num();
                        current_consistency = new_cl.unwrap_or(current_consistency);
                        continue 'same_node_retries;
                    }
                    RetryDecision::RetryNextTarget(new_cl) => {
                        #[cfg(feature = "metrics")]
                        self.metrics.inc_retries_num();
                        current_consistency = new_cl.unwrap_or(current_consistency);
                        continue 'nodes_in_plan;
                    }
                    RetryDecision::DontRetry => break 'nodes_in_plan,

                    RetryDecision::IgnoreWriteError => {
                        return Some(Ok((RunRequestResult::IgnoredWriteError, coordinator)));
                    }
                };
            }
        }

        last_error.map(Result::Err)
    }

    /// Awaits schema agreement among all reachable nodes.
    ///
    /// Issues an agreement check each `Session::schema_agreement_interval`.
    /// Loops indefinitely until the agreement is reached.
    ///
    /// If `required_node` is Some, only returns Ok if this node successfully
    /// returned its schema version during the agreement process.
    async fn await_schema_agreement_indefinitely(
        &self,
        required_node: Option<Uuid>,
    ) -> Result<Uuid, SchemaAgreementError> {
        loop {
            tokio::time::sleep(self.schema_agreement_interval).await;
            if let Some(agreed_version) = self
                .check_schema_agreement_with_required_node(required_node)
                .await?
            {
                return Ok(agreed_version);
            }
        }
    }

    /// Awaits schema agreement among all reachable nodes.
    ///
    /// Issues an agreement check each `Session::schema_agreement_interval`.
    /// If agreement is not reached in `Session::schema_agreement_timeout`,
    /// `SchemaAgreementError::Timeout` is returned.
    pub async fn await_schema_agreement(&self) -> Result<Uuid, SchemaAgreementError> {
        timeout(
            self.schema_agreement_timeout,
            self.await_schema_agreement_indefinitely(None),
        )
        .await
        .unwrap_or(Err(SchemaAgreementError::Timeout(
            self.schema_agreement_timeout,
        )))
    }

    /// Awaits schema agreement among all reachable nodes.
    ///
    /// Issues an agreement check each `Session::schema_agreement_interval`.
    /// If agreement is not reached in `Session::schema_agreement_timeout`,
    /// `SchemaAgreementError::Timeout` is returned.
    ///
    /// If `required_node` is Some, only returns Ok if this node successfully
    /// returned its schema version during the agreement process.
    async fn await_schema_agreement_with_required_node(
        &self,
        required_node: Option<Uuid>,
    ) -> Result<Uuid, SchemaAgreementError> {
        timeout(
            self.schema_agreement_timeout,
            self.await_schema_agreement_indefinitely(required_node),
        )
        .await
        .unwrap_or(Err(SchemaAgreementError::Timeout(
            self.schema_agreement_timeout,
        )))
    }

    /// Checks if all reachable nodes have the same schema version.
    ///
    /// If so, returns that agreed upon version.
    pub async fn check_schema_agreement(&self) -> Result<Option<Uuid>, SchemaAgreementError> {
        self.check_schema_agreement_with_required_node(None).await
    }

    /// Checks if all reachable nodes have the same schema version.
    /// If so, returns that agreed upon version.
    ///
    /// If `required_node` is Some, only returns Ok if this node successfully
    /// returned its schema version.
    async fn check_schema_agreement_with_required_node(
        &self,
        required_node: Option<Uuid>,
    ) -> Result<Option<Uuid>, SchemaAgreementError> {
        let cluster_state = self.get_cluster_state();
        // The iterator is guaranteed to be nonempty.
        let per_node_connections = cluster_state.iter_working_connections_per_node()?;

        // Therefore, this iterator is guaranteed to be nonempty, too.
        let handles = per_node_connections.map(|(host_id, pool)| async move {
            (host_id, Session::read_node_schema_version(pool).await)
        });
        // Hence, this is nonempty, too.
        let versions_results = join_all(handles).await;

        // Verify that required host is present, and returned success.
        if let Some(required_node) = required_node {
            match versions_results
                .iter()
                .find(|(host_id, _)| *host_id == required_node)
            {
                Some((_, Ok(SchemaNodeResult::Success(_version)))) => (),
                // For other connections we can ignore Broken error, but for required
                // host we need an actual schema version.
                Some((_, Ok(SchemaNodeResult::BrokenConnection(e)))) => {
                    return Err(SchemaAgreementError::RequestError(
                        RequestAttemptError::BrokenConnectionError(e.clone()),
                    ));
                }
                Some((_, Err(e))) => return Err(e.clone()),
                None => return Err(SchemaAgreementError::RequiredHostAbsent(required_node)),
            }
        }

        // Now we no longer need all the errors. We can return if there is
        // irrecoverable one, and collect the Ok values otherwise.
        let versions_results: Vec<_> = versions_results
            .into_iter()
            .map(|(_, result)| result)
            .try_collect()?;

        // unwrap is safe because iterator is still not empty.
        let local_version = match versions_results
            .iter()
            .find_or_first(|r| matches!(r, SchemaNodeResult::Success(_)))
            .unwrap()
        {
            SchemaNodeResult::Success(v) => *v,
            SchemaNodeResult::BrokenConnection(err) => {
                // There are only broken connection errors. Nothing better to do
                // than to return an error.
                return Err(SchemaAgreementError::RequestError(
                    RequestAttemptError::BrokenConnectionError(err.clone()),
                ));
            }
        };

        let in_agreement = versions_results
            .into_iter()
            .filter_map(|v_r| match v_r {
                SchemaNodeResult::Success(v) => Some(v),
                SchemaNodeResult::BrokenConnection(_) => None,
            })
            .all(|v| v == local_version);
        Ok(in_agreement.then_some(local_version))
    }

    /// Iterate over connections to the node.
    /// If fetching succeeds on some connection, return first fetched schema version,
    /// as Ok(SchemaNodeResult::Success(...)).
    /// Otherwise it means there are only errors:
    /// - If, and only if, all connections returned ConnectionBrokenError, first such error will be returned,
    ///   as Ok(SchemaNodeResult::BrokenConnection(...)).
    /// - Otherwise there is some other type of error on some connection. First such error will be returned as Err(...).
    ///
    /// `connections_to_node` iterator must be non-empty!
    async fn read_node_schema_version(
        connections_to_node: impl Iterator<Item = Arc<Connection>>,
    ) -> Result<SchemaNodeResult, SchemaAgreementError> {
        let mut first_broken_connection_err: Option<BrokenConnectionError> = None;
        let mut first_unignorable_err: Option<SchemaAgreementError> = None;
        for connection in connections_to_node {
            match connection.fetch_schema_version().await {
                Ok(schema_version) => return Ok(SchemaNodeResult::Success(schema_version)),
                Err(SchemaAgreementError::RequestError(
                    RequestAttemptError::BrokenConnectionError(conn_err),
                )) => {
                    if first_broken_connection_err.is_none() {
                        first_broken_connection_err = Some(conn_err);
                    }
                }
                Err(err) => {
                    if first_unignorable_err.is_none() {
                        first_unignorable_err = Some(err);
                    }
                }
            }
        }
        // The iterator was guaranteed to be nonempty, so there must have been at least one error.
        // It means at least one of `first_broken_connection_err` and `first_unrecoverable_err` is Some.
        if let Some(err) = first_unignorable_err {
            return Err(err);
        }

        Ok(SchemaNodeResult::BrokenConnection(
            first_broken_connection_err.unwrap(),
        ))
    }

    /// Retrieves the handle to execution profile that is used by this session
    /// by default, i.e. when an executed statement does not define its own handle.
    pub fn get_default_execution_profile_handle(&self) -> &ExecutionProfileHandle {
        &self.default_execution_profile_handle
    }
}

struct ExecuteRequestContext<'a> {
    is_idempotent: bool,
    consistency_set_on_statement: Option<Consistency>,
    retry_session: Box<dyn RetrySession>,
    history_data: Option<HistoryData<'a>>,
    load_balancing_policy: &'a dyn load_balancing::LoadBalancingPolicy,
    query_info: &'a load_balancing::RoutingInfo<'a>,
    request_span: &'a RequestSpan,
}

struct HistoryData<'a> {
    listener: &'a dyn HistoryListener,
    request_id: history::RequestId,
    speculative_id: Option<history::SpeculativeId>,
}

impl ExecuteRequestContext<'_> {
    fn log_attempt_start(&self, node_addr: SocketAddr) -> Option<history::AttemptId> {
        self.history_data.as_ref().map(|hd| {
            hd.listener
                .log_attempt_start(hd.request_id, hd.speculative_id, node_addr)
        })
    }

    fn log_attempt_success(&self, attempt_id_opt: &Option<history::AttemptId>) {
        let attempt_id: &history::AttemptId = match attempt_id_opt {
            Some(id) => id,
            None => return,
        };

        let history_data: &HistoryData = match &self.history_data {
            Some(data) => data,
            None => return,
        };

        history_data.listener.log_attempt_success(*attempt_id);
    }

    fn log_attempt_error(
        &self,
        attempt_id_opt: &Option<history::AttemptId>,
        error: &RequestAttemptError,
        retry_decision: &RetryDecision,
    ) {
        let attempt_id: &history::AttemptId = match attempt_id_opt {
            Some(id) => id,
            None => return,
        };

        let history_data: &HistoryData = match &self.history_data {
            Some(data) => data,
            None => return,
        };

        history_data
            .listener
            .log_attempt_error(*attempt_id, error, retry_decision);
    }
}

#[derive(Debug)]
enum SchemaNodeResult {
    Success(Uuid),
    BrokenConnection(BrokenConnectionError),
}
