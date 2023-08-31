//! `Session` is the main object used in the driver.\
//! It manages all connections to the cluster and allows to perform queries.

#[cfg(feature = "cloud")]
use crate::cloud::CloudConfig;

use crate::execution::driver_tracing::RequestSpan;
use crate::execution::history::{self, HistoryListener};
use arc_swap::ArcSwapOption;
use bytes::Bytes;
use futures::future::join_all;
use futures::future::try_join_all;
use itertools::Itertools;
use scylla_cql::frame::response::NonErrorResponse;
use std::borrow::Borrow;
use std::future::Future;
use std::net::SocketAddr;
use std::num::NonZeroU32;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::timeout;
use tracing::{debug, trace, trace_span, Instrument};
use uuid::Uuid;

use crate::cluster::host_filter::HostFilter;
#[cfg(feature = "cloud")]
use crate::cluster::CloudEndpoint;
use crate::cluster::{Cluster, ClusterData, ClusterNeatDebug};
use crate::cluster::{KnownNode, Node, NodeRef};
use crate::connection::PoolConfig;
use crate::connection::QueryResponse;
#[cfg(feature = "ssl")]
use crate::connection::SslConfig;
use crate::connection::{
    AddressTranslator, Connection, ConnectionConfig, NonErrorQueryResponse, VerifiedKeyspaceName,
};
use crate::cql_to_rust::FromRow;
use crate::execution::errors::{NewSessionError, QueryError};
use crate::execution::iterator::{PreparedIteratorConfig, RowIterator};
use crate::execution::load_balancing::{self, RoutingInfo};
use crate::execution::retries::{QueryInfo, RetryDecision, RetrySession};
use crate::execution::tracing::{
    TracingEvent, TracingInfo, TRACES_EVENTS_QUERY_STR, TRACES_SESSION_QUERY_STR,
};
use crate::execution::Metrics;
use crate::execution::{
    speculative_execution, ExecutionProfile, ExecutionProfileHandle, ExecutionProfileInner,
};
use crate::frame::response::cql_to_rust::FromRowError;
use crate::frame::response::result;
use crate::frame::value::{
    BatchValues, BatchValuesFirstSerialized, BatchValuesIterator, ValueList,
};
use crate::prepared_statement::PreparedStatement;
use crate::query::Query;
use crate::routing::partitioner::PartitionerName;
use crate::statement::Consistency;
use crate::transport::query_result::QueryResult;
use crate::transport::Compression;
use crate::{
    batch::{Batch, BatchStatement},
    statement::StatementConfig,
};

use crate::connection::PoolSize;

use crate::authentication::AuthenticatorProvider;
#[cfg(feature = "ssl")]
use openssl::ssl::SslContext;

/// `Session` manages connections to the cluster and allows to perform queries
pub struct Session {
    cluster: Cluster,
    default_execution_profile_handle: ExecutionProfileHandle,
    schema_agreement_interval: Duration,
    metrics: Arc<Metrics>,
    schema_agreement_timeout: Duration,
    schema_agreement_automatic_waiting: bool,
    refresh_metadata_on_auto_schema_agreement: bool,
    keyspace_name: ArcSwapOption<String>,
    tracing_info_fetch_attempts: NonZeroU32,
    tracing_info_fetch_interval: Duration,
    tracing_info_fetch_consistency: Consistency,
}

/// This implementation deliberately omits some details from Cluster in order
/// to avoid cluttering the print with much information of little usability.
impl std::fmt::Debug for Session {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Session")
            .field("cluster", &ClusterNeatDebug(&self.cluster))
            .field(
                "default_execution_profile_handle",
                &self.default_execution_profile_handle,
            )
            .field("schema_agreement_interval", &self.schema_agreement_interval)
            .field("metrics", &self.metrics)
            .field(
                "auto_await_schema_agreement_timeout",
                &self.schema_agreement_timeout,
            )
            .finish()
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

    /// Preferred compression algorithm to use on connections.
    /// If it's not supported by database server Session will fall back to no compression.
    pub compression: Option<Compression>,
    pub tcp_nodelay: bool,
    pub tcp_keepalive_interval: Option<Duration>,

    pub default_execution_profile_handle: ExecutionProfileHandle,

    pub used_keyspace: Option<String>,
    pub keyspace_case_sensitive: bool,

    /// Provide our Session with TLS
    #[cfg(feature = "ssl")]
    pub ssl_context: Option<SslContext>,

    pub authenticator: Option<Arc<dyn AuthenticatorProvider>>,

    pub connect_timeout: Duration,

    /// Size of the per-node connection pool, i.e. how many connections the driver should keep to each node.
    /// The default is `PerShard(1)`, which is the recommended setting for Scylla clusters.
    pub connection_pool_size: PoolSize,

    /// If true, prevents the driver from connecting to the shard-aware port, even if the node supports it.
    /// Generally, this options is best left as default (false).
    pub disallow_shard_aware_port: bool,

    /// If empty, fetch all keyspaces
    pub keyspaces_to_fetch: Vec<String>,

    /// If true, full schema is fetched with every metadata refresh.
    pub fetch_schema_metadata: bool,

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
    #[cfg(feature = "cloud")]
    pub cloud_config: Option<Arc<CloudConfig>>,

    /// If true, the driver will inject a small delay before flushing data
    /// to the socket - by rescheduling the task that writes data to the socket.
    /// This gives the task an opportunity to collect more write requests
    /// and write them in a single syscall, increasing the efficiency.
    ///
    /// However, this optimization may worsen latency if the rate of requests
    /// issued by the application is low, but otherwise the application is
    /// heavily loaded with other tasks on the same tokio executor.
    /// Please do performance measurements before committing to disabling
    /// this option.
    pub enable_write_coalescing: bool,

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
}

impl SessionConfig {
    /// Creates a [`SessionConfig`] with default configuration
    /// # Default configuration
    /// * Compression: None
    /// * Load balancing policy: Token-aware Round-robin
    ///
    /// # Example
    /// ```
    /// # use scylla::SessionConfig;
    /// let config = SessionConfig::new();
    /// ```
    pub fn new() -> Self {
        SessionConfig {
            known_nodes: Vec::new(),
            compression: None,
            tcp_nodelay: true,
            tcp_keepalive_interval: None,
            schema_agreement_interval: Duration::from_millis(200),
            default_execution_profile_handle: ExecutionProfile::new_from_inner(Default::default())
                .into_handle(),
            used_keyspace: None,
            keyspace_case_sensitive: false,
            #[cfg(feature = "ssl")]
            ssl_context: None,
            authenticator: None,
            connect_timeout: Duration::from_secs(5),
            connection_pool_size: Default::default(),
            disallow_shard_aware_port: false,
            keyspaces_to_fetch: Vec::new(),
            fetch_schema_metadata: true,
            keepalive_interval: Some(Duration::from_secs(30)),
            keepalive_timeout: Some(Duration::from_secs(30)),
            schema_agreement_timeout: Duration::from_secs(60),
            schema_agreement_automatic_waiting: true,
            address_translator: None,
            host_filter: None,
            refresh_metadata_on_auto_schema_agreement: true,
            #[cfg(feature = "cloud")]
            cloud_config: None,
            enable_write_coalescing: true,
            tracing_info_fetch_attempts: NonZeroU32::new(5).unwrap(),
            tracing_info_fetch_interval: Duration::from_millis(3),
            tracing_info_fetch_consistency: Consistency::One,
            cluster_metadata_refresh_interval: Duration::from_secs(60),
        }
    }

    /// Adds a known database server with a hostname.
    /// If the port is not explicitly specified, 9042 is used as default
    /// # Example
    /// ```
    /// # use scylla::SessionConfig;
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
    /// # use scylla::SessionConfig;
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
    /// # use scylla::SessionConfig;
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
    /// # use scylla::SessionConfig;
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

/// Trait used to implement `Vec<result::Row>::into_typed<RowT>`
// This is the only way to add custom method to Vec
pub trait IntoTypedRows {
    fn into_typed<RowT: FromRow>(self) -> TypedRowIter<RowT>;
}

// Adds method Vec<result::Row>::into_typed<RowT>(self)
// It transforms the Vec into iterator mapping to custom row type
impl IntoTypedRows for Vec<result::Row> {
    fn into_typed<RowT: FromRow>(self) -> TypedRowIter<RowT> {
        TypedRowIter {
            row_iter: self.into_iter(),
            phantom_data: Default::default(),
        }
    }
}

/// Iterator over rows parsed as the given type\
/// Returned by `rows.into_typed::<(...)>()`
pub struct TypedRowIter<RowT: FromRow> {
    row_iter: std::vec::IntoIter<result::Row>,
    phantom_data: std::marker::PhantomData<RowT>,
}

impl<RowT: FromRow> Iterator for TypedRowIter<RowT> {
    type Item = Result<RowT, FromRowError>;

    fn next(&mut self) -> Option<Self::Item> {
        self.row_iter.next().map(RowT::from_row)
    }
}

pub(crate) enum RunQueryResult<ResT> {
    IgnoredWriteError,
    Completed(ResT),
}

/// Represents a CQL session, which can be used to communicate
/// with the database
impl Session {
    /// Estabilishes a CQL session with the database
    ///
    /// Usually it's easier to use [SessionBuilder](crate::session_builder::SessionBuilder)
    /// instead of calling `Session::connect` directly, because it's more convenient.
    /// # Arguments
    /// * `config` - Connection configuration - known nodes, Compression, etc.
    /// Must contain at least one known node.
    ///
    /// # Example
    /// ```rust
    /// # use std::error::Error;
    /// # async fn check_only_compiles() -> Result<(), Box<dyn Error>> {
    /// use scylla::{Session, SessionConfig};
    /// use scylla::cluster::KnownNode;
    ///
    /// let mut config = SessionConfig::new();
    /// config.known_nodes.push(KnownNode::Hostname("127.0.0.1:9042".to_string()));
    ///
    /// let session: Session = Session::connect(config).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn connect(config: SessionConfig) -> Result<Session, NewSessionError> {
        let known_nodes = config.known_nodes;

        #[cfg(feature = "cloud")]
        let known_nodes = if let Some(cloud_servers) =
            config.cloud_config.as_ref().map(|cloud_config| {
                cloud_config
                    .get_datacenters()
                    .iter()
                    .map(|(dc_name, dc_data)| {
                        KnownNode::CloudEndpoint(CloudEndpoint {
                            hostname: dc_data.get_server().to_owned(),
                            datacenter: dc_name.clone(),
                        })
                    })
                    .collect()
            }) {
            cloud_servers
        } else {
            known_nodes
        };

        // Ensure there is at least one known node
        if known_nodes.is_empty() {
            return Err(NewSessionError::EmptyKnownNodesList);
        }

        let connection_config = ConnectionConfig {
            compression: config.compression,
            tcp_nodelay: config.tcp_nodelay,
            tcp_keepalive_interval: config.tcp_keepalive_interval,
            #[cfg(feature = "ssl")]
            ssl_config: config.ssl_context.map(SslConfig::new_with_global_context),
            authenticator: config.authenticator.clone(),
            connect_timeout: config.connect_timeout,
            event_sender: None,
            default_consistency: Default::default(),
            address_translator: config.address_translator,
            #[cfg(feature = "cloud")]
            cloud_config: config.cloud_config,
            enable_write_coalescing: config.enable_write_coalescing,
            keepalive_interval: config.keepalive_interval,
            keepalive_timeout: config.keepalive_timeout,
        };

        let pool_config = PoolConfig {
            connection_config,
            pool_size: config.connection_pool_size,
            can_use_shard_aware_port: !config.disallow_shard_aware_port,
            keepalive_interval: config.keepalive_interval,
        };

        let cluster = Cluster::new(
            known_nodes,
            pool_config,
            config.keyspaces_to_fetch,
            config.fetch_schema_metadata,
            config.host_filter,
            config.cluster_metadata_refresh_interval,
        )
        .await?;

        let default_execution_profile_handle = config.default_execution_profile_handle;

        let session = Session {
            cluster,
            default_execution_profile_handle,
            schema_agreement_interval: config.schema_agreement_interval,
            metrics: Arc::new(Metrics::new()),
            schema_agreement_timeout: config.schema_agreement_timeout,
            schema_agreement_automatic_waiting: config.schema_agreement_automatic_waiting,
            refresh_metadata_on_auto_schema_agreement: config
                .refresh_metadata_on_auto_schema_agreement,
            keyspace_name: ArcSwapOption::default(), // will be set by use_keyspace
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

    /// Sends a query to the database and receives a response.\
    /// Returns only a single page of results, to receive multiple pages use [query_iter](Session::query_iter)
    ///
    /// This is the easiest way to make a query, but performance is worse than that of prepared queries.
    ///
    /// See [the book](https://rust-driver.docs.scylladb.com/stable/queries/simple.html) for more information
    /// # Arguments
    /// * `query` - query to perform, can be just a `&str` or the [Query](crate::query::Query) struct.
    /// * `values` - values bound to the query, easiest way is to use a tuple of bound values
    ///
    /// # Examples
    /// ```rust
    /// # use scylla::Session;
    /// # use std::error::Error;
    /// # async fn check_only_compiles(session: &Session) -> Result<(), Box<dyn Error>> {
    /// // Insert an int and text into a table
    /// session
    ///     .query(
    ///         "INSERT INTO ks.tab (a, b) VALUES(?, ?)",
    ///         (2_i32, "some text")
    ///     )
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    /// ```rust
    /// # use scylla::Session;
    /// # use std::error::Error;
    /// # async fn check_only_compiles(session: &Session) -> Result<(), Box<dyn Error>> {
    /// use scylla::IntoTypedRows;
    ///
    /// // Read rows containing an int and text
    /// let rows_opt = session
    /// .query("SELECT a, b FROM ks.tab", &[])
    ///     .await?
    ///     .rows;
    ///
    /// if let Some(rows) = rows_opt {
    ///     for row in rows.into_typed::<(i32, String)>() {
    ///         // Parse row as int and text \
    ///         let (int_val, text_val): (i32, String) = row?;
    ///     }
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub async fn query(
        &self,
        query: impl Into<Query>,
        values: impl ValueList,
    ) -> Result<QueryResult, QueryError> {
        self.query_paged(query, values, None).await
    }

    /// Queries the database with a custom paging state.
    /// # Arguments
    ///
    /// * `query` - query to be performed
    /// * `values` - values bound to the query
    /// * `paging_state` - previously received paging state or None
    pub async fn query_paged(
        &self,
        query: impl Into<Query>,
        values: impl ValueList,
        paging_state: Option<Bytes>,
    ) -> Result<QueryResult, QueryError> {
        let query: Query = query.into();
        let serialized_values = values.serialized()?;

        let execution_profile = query
            .get_execution_profile_handle()
            .unwrap_or_else(|| self.get_default_execution_profile_handle())
            .access();

        let statement_info = RoutingInfo {
            consistency: query
                .config
                .consistency
                .unwrap_or(execution_profile.consistency),
            serial_consistency: query
                .config
                .serial_consistency
                .unwrap_or(execution_profile.serial_consistency),
            ..Default::default()
        };

        let span = RequestSpan::new_query(&query.contents, serialized_values.size());
        let run_query_result = self
            .run_query(
                statement_info,
                &query.config,
                execution_profile,
                |node: Arc<Node>| async move { node.random_connection().await },
                |connection: Arc<Connection>,
                 consistency: Consistency,
                 execution_profile: &ExecutionProfileInner| {
                    let serial_consistency = query
                        .config
                        .serial_consistency
                        .unwrap_or(execution_profile.serial_consistency);
                    // Needed to avoid moving query and values into async move block
                    let query_ref = &query;
                    let values_ref = &serialized_values;
                    let paging_state_ref = &paging_state;
                    async move {
                        connection
                            .query_with_consistency(
                                query_ref,
                                values_ref,
                                consistency,
                                serial_consistency,
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

        let response = match run_query_result {
            RunQueryResult::IgnoredWriteError => NonErrorQueryResponse {
                response: NonErrorResponse::Result(result::Result::Void),
                tracing_id: None,
                warnings: Vec::new(),
            },
            RunQueryResult::Completed(response) => response,
        };

        self.handle_set_keyspace_response(&response).await?;
        self.handle_auto_await_schema_agreement(&response).await?;

        let result = response.into_query_result()?;
        span.record_result_fields(&result);
        Ok(result)
    }

    async fn handle_set_keyspace_response(
        &self,
        response: &NonErrorQueryResponse,
    ) -> Result<(), QueryError> {
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
    ) -> Result<(), QueryError> {
        if self.schema_agreement_automatic_waiting {
            if response.as_schema_change().is_some() {
                self.await_schema_agreement().await?;
            }

            if self.refresh_metadata_on_auto_schema_agreement
                && response.as_schema_change().is_some()
            {
                self.refresh_metadata().await?;
            }
        }

        Ok(())
    }

    /// Run a simple query with paging\
    /// This method will query all pages of the result\
    ///
    /// Returns an async iterator (stream) over all received rows\
    /// Page size can be specified in the [Query](crate::query::Query) passed to the function
    ///
    /// See [the book](https://rust-driver.docs.scylladb.com/stable/queries/paged.html) for more information
    ///
    /// # Arguments
    /// * `query` - query to perform, can be just a `&str` or the [Query](crate::query::Query) struct.
    /// * `values` - values bound to the query, easiest way is to use a tuple of bound values
    ///
    /// # Example
    ///
    /// ```rust
    /// # use scylla::Session;
    /// # use std::error::Error;
    /// # async fn check_only_compiles(session: &Session) -> Result<(), Box<dyn Error>> {
    /// use scylla::IntoTypedRows;
    /// use futures::stream::StreamExt;
    ///
    /// let mut rows_stream = session
    ///    .query_iter("SELECT a, b FROM ks.t", &[])
    ///    .await?
    ///    .into_typed::<(i32, i32)>();
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
        query: impl Into<Query>,
        values: impl ValueList,
    ) -> Result<RowIterator, QueryError> {
        let query: Query = query.into();
        let serialized_values = values.serialized()?;

        let execution_profile = query
            .get_execution_profile_handle()
            .unwrap_or_else(|| self.get_default_execution_profile_handle())
            .access();

        RowIterator::new_for_query(
            query,
            serialized_values.into_owned(),
            execution_profile,
            self.cluster.get_data(),
            self.metrics.clone(),
        )
        .await
    }

    /// Prepares a statement on the server side and returns a prepared statement,
    /// which can later be used to perform more efficient queries
    ///
    /// Prepared queries are much faster than simple queries:
    /// * Database doesn't need to parse the query
    /// * They are properly load balanced using token aware routing
    ///
    /// > ***Warning***\
    /// > For token/shard aware load balancing to work properly, all partition key values
    /// > must be sent as bound values
    /// > (see [performance section](https://rust-driver.docs.scylladb.com/stable/queries/prepared.html#performance))
    ///
    /// See [the book](https://rust-driver.docs.scylladb.com/stable/queries/prepared.html) for more information
    ///
    /// # Arguments
    /// * `query` - query to prepare, can be just a `&str` or the [Query](crate::query::Query) struct.
    ///
    /// # Example
    /// ```rust
    /// # use scylla::Session;
    /// # use std::error::Error;
    /// # async fn check_only_compiles(session: &Session) -> Result<(), Box<dyn Error>> {
    /// use scylla::prepared_statement::PreparedStatement;
    ///
    /// // Prepare the query for later execution
    /// let prepared: PreparedStatement = session
    ///     .prepare("INSERT INTO ks.tab (a) VALUES(?)")
    ///     .await?;
    ///
    /// // Run the prepared query with some values, just like a simple query
    /// let to_insert: i32 = 12345;
    /// session.execute(&prepared, (to_insert,)).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn prepare(&self, query: impl Into<Query>) -> Result<PreparedStatement, QueryError> {
        let query = query.into();
        let query_ref = &query;

        let cluster_data = self.get_cluster_data();
        let connections_iter = cluster_data.iter_working_connections()?;

        // Prepare statements on all connections concurrently
        let handles = connections_iter.map(|c| async move { c.prepare(query_ref).await });
        let mut results = join_all(handles).await.into_iter();

        // If at least one prepare was successful, `prepare()` returns Ok.
        // Find the first result that is Ok, or Err if all failed.

        // Safety: there is at least one node in the cluster, and `Cluster::iter_working_connections()`
        // returns either an error or an iterator with at least one connection, so there will be at least one result.
        let first_ok: Result<PreparedStatement, QueryError> =
            results.by_ref().find_or_first(Result::is_ok).unwrap();
        let mut prepared: PreparedStatement = first_ok?;

        // Validate prepared ids equality
        for statement in results.flatten() {
            if prepared.get_id() != statement.get_id() {
                return Err(QueryError::ProtocolError(
                    "Prepared statement Ids differ, all should be equal",
                ));
            }

            // Collect all tracing ids from prepare() queries in the final result
            prepared
                .prepare_tracing_ids
                .extend(statement.prepare_tracing_ids);
        }

        prepared.set_partitioner_name(
            self.extract_partitioner_name(&prepared, &self.cluster.get_data())
                .and_then(PartitionerName::from_str)
                .unwrap_or_default(),
        );

        Ok(prepared)
    }

    fn extract_partitioner_name<'a>(
        &self,
        prepared: &PreparedStatement,
        cluster_data: &'a ClusterData,
    ) -> Option<&'a str> {
        cluster_data
            .keyspaces
            .get(prepared.get_keyspace_name()?)?
            .tables
            .get(prepared.get_table_name()?)?
            .partitioner
            .as_deref()
    }

    /// Execute a prepared query. Requires a [PreparedStatement](crate::prepared_statement::PreparedStatement)
    /// generated using [`Session::prepare`](Session::prepare)\
    /// Returns only a single page of results, to receive multiple pages use [execute_iter](Session::execute_iter)
    ///
    /// Prepared queries are much faster than simple queries:
    /// * Database doesn't need to parse the query
    /// * They are properly load balanced using token aware routing
    ///
    /// > ***Warning***\
    /// > For token/shard aware load balancing to work properly, all partition key values
    /// > must be sent as bound values
    /// > (see [performance section](https://rust-driver.docs.scylladb.com/stable/queries/prepared.html#performance))
    ///
    /// See [the book](https://rust-driver.docs.scylladb.com/stable/queries/prepared.html) for more information
    ///
    /// # Arguments
    /// * `prepared` - the prepared statement to execute, generated using [`Session::prepare`](Session::prepare)
    /// * `values` - values bound to the query, easiest way is to use a tuple of bound values
    ///
    /// # Example
    /// ```rust
    /// # use scylla::Session;
    /// # use std::error::Error;
    /// # async fn check_only_compiles(session: &Session) -> Result<(), Box<dyn Error>> {
    /// use scylla::prepared_statement::PreparedStatement;
    ///
    /// // Prepare the query for later execution
    /// let prepared: PreparedStatement = session
    ///     .prepare("INSERT INTO ks.tab (a) VALUES(?)")
    ///     .await?;
    ///
    /// // Run the prepared query with some values, just like a simple query
    /// let to_insert: i32 = 12345;
    /// session.execute(&prepared, (to_insert,)).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn execute(
        &self,
        prepared: &PreparedStatement,
        values: impl ValueList,
    ) -> Result<QueryResult, QueryError> {
        self.execute_paged(prepared, values, None).await
    }

    /// Executes a previously prepared statement with previously received paging state
    /// # Arguments
    ///
    /// * `prepared` - a statement prepared with [prepare](crate::session::Session::prepare)
    /// * `values` - values bound to the query
    /// * `paging_state` - paging state from the previous query or None
    pub async fn execute_paged(
        &self,
        prepared: &PreparedStatement,
        values: impl ValueList,
        paging_state: Option<Bytes>,
    ) -> Result<QueryResult, QueryError> {
        let serialized_values = values.serialized()?;
        let values_ref = &serialized_values;
        let paging_state_ref = &paging_state;

        let (partition_key, token) = prepared
            .extract_partition_key_and_calculate_token(
                prepared.get_partitioner_name(),
                &serialized_values,
            )?
            .unzip();

        let execution_profile = prepared
            .get_execution_profile_handle()
            .unwrap_or_else(|| self.get_default_execution_profile_handle())
            .access();

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
            keyspace: prepared.get_keyspace_name(),
            is_confirmed_lwt: prepared.is_confirmed_lwt(),
        };

        let span = RequestSpan::new_prepared(
            partition_key.as_ref().map(|pk| pk.iter()),
            token,
            serialized_values.size(),
        );

        if !span.span().is_disabled() {
            if let (Some(keyspace), Some(token)) = (statement_info.keyspace.as_ref(), token) {
                let cluster_data = self.get_cluster_data();
                let replicas: smallvec::SmallVec<[_; 8]> = cluster_data
                    .get_token_endpoints_iter(keyspace, token)
                    .collect();
                span.record_replicas(&replicas)
            }
        }

        let run_query_result: RunQueryResult<NonErrorQueryResponse> = self
            .run_query(
                statement_info,
                &prepared.config,
                execution_profile,
                |node: Arc<Node>| async move {
                    match token {
                        Some(token) => node.connection_for_token(token).await,
                        None => node.random_connection().await,
                    }
                },
                |connection: Arc<Connection>,
                 consistency: Consistency,
                 execution_profile: &ExecutionProfileInner| {
                    let serial_consistency = prepared
                        .config
                        .serial_consistency
                        .unwrap_or(execution_profile.serial_consistency);
                    async move {
                        connection
                            .execute_with_consistency(
                                prepared,
                                values_ref,
                                consistency,
                                serial_consistency,
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

        let response = match run_query_result {
            RunQueryResult::IgnoredWriteError => NonErrorQueryResponse {
                response: NonErrorResponse::Result(result::Result::Void),
                tracing_id: None,
                warnings: Vec::new(),
            },
            RunQueryResult::Completed(response) => response,
        };

        self.handle_set_keyspace_response(&response).await?;
        self.handle_auto_await_schema_agreement(&response).await?;

        let result = response.into_query_result()?;
        span.record_result_fields(&result);
        Ok(result)
    }

    /// Run a prepared query with paging\
    /// This method will query all pages of the result\
    ///
    /// Returns an async iterator (stream) over all received rows\
    /// Page size can be specified in the [PreparedStatement](crate::prepared_statement::PreparedStatement)
    /// passed to the function
    ///
    /// See [the book](https://rust-driver.docs.scylladb.com/stable/queries/paged.html) for more information
    ///
    /// # Arguments
    /// * `prepared` - the prepared statement to execute, generated using [`Session::prepare`](Session::prepare)
    /// * `values` - values bound to the query, easiest way is to use a tuple of bound values
    ///
    /// # Example
    ///
    /// ```rust
    /// # use scylla::Session;
    /// # use std::error::Error;
    /// # async fn check_only_compiles(session: &Session) -> Result<(), Box<dyn Error>> {
    /// use scylla::prepared_statement::PreparedStatement;
    /// use scylla::IntoTypedRows;
    /// use futures::stream::StreamExt;
    ///
    /// // Prepare the query for later execution
    /// let prepared: PreparedStatement = session
    ///     .prepare("SELECT a, b FROM ks.t")
    ///     .await?;
    ///
    /// // Execute the query and receive all pages
    /// let mut rows_stream = session
    ///    .execute_iter(prepared, &[])
    ///    .await?
    ///    .into_typed::<(i32, i32)>();
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
        values: impl ValueList,
    ) -> Result<RowIterator, QueryError> {
        let prepared = prepared.into();
        let serialized_values = values.serialized()?;

        let execution_profile = prepared
            .get_execution_profile_handle()
            .unwrap_or_else(|| self.get_default_execution_profile_handle())
            .access();

        RowIterator::new_for_prepared_statement(PreparedIteratorConfig {
            prepared,
            values: serialized_values.into_owned(),
            execution_profile,
            cluster_data: self.cluster.get_data(),
            metrics: self.metrics.clone(),
        })
        .await
    }

    /// Perform a batch query\
    /// Batch contains many `simple` or `prepared` queries which are executed at once\
    /// Batch doesn't return any rows
    ///
    /// Batch values must contain values for each of the queries
    ///
    /// See [the book](https://rust-driver.docs.scylladb.com/stable/queries/batch.html) for more information
    ///
    /// # Arguments
    /// * `batch` - [Batch](crate::batch::Batch) to be performed
    /// * `values` - List of values for each query, it's the easiest to use a tuple of tuples
    ///
    /// # Example
    /// ```rust
    /// # use scylla::Session;
    /// # use std::error::Error;
    /// # async fn check_only_compiles(session: &Session) -> Result<(), Box<dyn Error>> {
    /// use scylla::batch::Batch;
    ///
    /// let mut batch: Batch = Default::default();
    ///
    /// // A query with two bound values
    /// batch.append_statement("INSERT INTO ks.tab(a, b) VALUES(?, ?)");
    ///
    /// // A query with one bound value
    /// batch.append_statement("INSERT INTO ks.tab(a, b) VALUES(3, ?)");
    ///
    /// // A query with no bound values
    /// batch.append_statement("INSERT INTO ks.tab(a, b) VALUES(5, 6)");
    ///
    /// // Batch values is a tuple of 3 tuples containing values for each query
    /// let batch_values = ((1_i32, 2_i32), // Tuple with two values for the first query
    ///                     (4_i32,),       // Tuple with one value for the second query
    ///                     ());            // Empty tuple/unit for the third query
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
    ) -> Result<QueryResult, QueryError> {
        // Shard-awareness behavior for batch will be to pick shard based on first batch statement's shard
        // If users batch statements by shard, they will be rewarded with full shard awareness

        // Extract first serialized_value
        let first_serialized_value = values.batch_values_iter().next_serialized().transpose()?;
        let first_serialized_value = first_serialized_value.as_deref();

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

        let statement_info = match (first_serialized_value, batch.statements.first()) {
            (Some(first_serialized_value), Some(BatchStatement::PreparedStatement(ps))) => {
                RoutingInfo {
                    consistency,
                    serial_consistency,
                    token: ps.calculate_token(first_serialized_value)?,
                    keyspace: ps.get_keyspace_name(),
                    is_confirmed_lwt: false,
                }
            }
            _ => RoutingInfo {
                consistency,
                serial_consistency,
                ..Default::default()
            },
        };
        let first_value_token = statement_info.token;

        // Reuse first serialized value when serializing query, and delegate to `BatchValues::write_next_to_request`
        // directly for others (if they weren't already serialized, possibly don't even allocate the `SerializedValues`)
        let values = BatchValuesFirstSerialized::new(&values, first_serialized_value);
        let values_ref = &values;

        let span = RequestSpan::new_batch();

        let run_query_result = self
            .run_query(
                statement_info,
                &batch.config,
                execution_profile,
                |node: Arc<Node>| async move {
                    match first_value_token {
                        Some(first_value_token) => {
                            node.connection_for_token(first_value_token).await
                        }
                        None => node.random_connection().await,
                    }
                },
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
                    }
                },
                &span,
            )
            .instrument(span.span().clone())
            .await?;

        let result = match run_query_result {
            RunQueryResult::IgnoredWriteError => QueryResult::default(),
            RunQueryResult::Completed(response) => response,
        };
        span.record_result_fields(&result);
        Ok(result)
    }

    /// Prepares all statements within the batch and returns a new batch where every
    /// statement is prepared.
    /// /// # Example
    /// ```rust
    /// # extern crate scylla;
    /// # use scylla::Session;
    /// # use std::error::Error;
    /// # async fn check_only_compiles(session: &Session) -> Result<(), Box<dyn Error>> {
    /// use scylla::batch::Batch;
    ///
    /// // Create a batch statement with unprepared statements
    /// let mut batch: Batch = Default::default();
    /// batch.append_statement("INSERT INTO ks.simple_unprepared1 VALUES(?, ?)");
    /// batch.append_statement("INSERT INTO ks.simple_unprepared2 VALUES(?, ?)");
    ///
    /// // Prepare all statements in the batch at once
    /// let prepared_batch: Batch = session.prepare_batch(&batch).await?;
    ///
    /// // Specify bound values to use with each query
    /// let batch_values = ((1_i32, 2_i32),
    ///                     (3_i32, 4_i32));
    ///
    /// // Run the prepared batch
    /// session.batch(&prepared_batch, batch_values).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn prepare_batch(&self, batch: &Batch) -> Result<Batch, QueryError> {
        let mut prepared_batch = batch.clone();

        try_join_all(
            prepared_batch
                .statements
                .iter_mut()
                .map(|statement| async move {
                    if let BatchStatement::Query(query) = statement {
                        let prepared = self.prepare(query.clone()).await?;
                        *statement = BatchStatement::PreparedStatement(prepared);
                    }
                    Ok::<(), QueryError>(())
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
    /// See [the book](https://rust-driver.docs.scylladb.com/stable/queries/usekeyspace.html) for more information
    ///
    /// # Arguments
    ///
    /// * `keyspace_name` - keyspace name to use,
    /// keyspace names can have up to 48 alphanumeric characters and contain underscores
    /// * `case_sensitive` - if set to true the generated query will put keyspace name in quotes
    /// # Example
    /// ```rust
    /// # use scylla::{Session, SessionBuilder};
    /// # use scylla::transport::Compression;
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// # let session = SessionBuilder::new().known_node("127.0.0.1:9042").build().await?;
    /// session
    ///     .query("INSERT INTO my_keyspace.tab (a) VALUES ('test1')", &[])
    ///     .await?;
    ///
    /// session.use_keyspace("my_keyspace", false).await?;
    ///
    /// // Now we can omit keyspace name in the query
    /// session
    ///     .query("INSERT INTO tab (a) VALUES ('test2')", &[])
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn use_keyspace(
        &self,
        keyspace_name: impl Into<String>,
        case_sensitive: bool,
    ) -> Result<(), QueryError> {
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
    pub async fn refresh_metadata(&self) -> Result<(), QueryError> {
        self.cluster.refresh_metadata().await
    }

    /// Access metrics collected by the driver\
    /// Driver collects various metrics like number of queries or query latencies.
    /// They can be read using this method
    pub fn get_metrics(&self) -> Arc<Metrics> {
        self.metrics.clone()
    }

    /// Access cluster data collected by the driver\
    /// Driver collects various information about network topology or schema.
    /// They can be read using this method
    pub fn get_cluster_data(&self) -> Arc<ClusterData> {
        self.cluster.get_data()
    }

    /// Get [`TracingInfo`] of a traced query performed earlier
    ///
    /// See [the book](https://rust-driver.docs.scylladb.com/stable/tracing/tracing.html)
    /// for more information about query tracing
    pub async fn get_tracing_info(&self, tracing_id: &Uuid) -> Result<TracingInfo, QueryError> {
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

        Err(QueryError::ProtocolError(
            "All tracing queries returned an empty result, \
            maybe the trace information didn't propagate yet. \
            Consider configuring Session with \
            a longer fetch interval (tracing_info_fetch_interval)",
        ))
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
    ) -> Result<Option<TracingInfo>, QueryError> {
        // Query system_traces.sessions for TracingInfo
        let mut traces_session_query = Query::new(TRACES_SESSION_QUERY_STR);
        traces_session_query.config.consistency = consistency;
        traces_session_query.set_page_size(1024);

        // Query system_traces.events for TracingEvents
        let mut traces_events_query = Query::new(TRACES_EVENTS_QUERY_STR);
        traces_events_query.config.consistency = consistency;
        traces_events_query.set_page_size(1024);

        let (traces_session_res, traces_events_res) = tokio::try_join!(
            self.query(traces_session_query, (tracing_id,)),
            self.query(traces_events_query, (tracing_id,))
        )?;

        // Get tracing info
        let tracing_info_row_res: Option<Result<TracingInfo, _>> = traces_session_res
            .rows
            .ok_or(QueryError::ProtocolError(
                "Response to system_traces.sessions query was not Rows",
            ))?
            .into_typed::<TracingInfo>()
            .next();

        let mut tracing_info: TracingInfo = match tracing_info_row_res {
            Some(tracing_info_row_res) => tracing_info_row_res.map_err(|_| {
                QueryError::ProtocolError(
                    "Columns from system_traces.session have an unexpected type",
                )
            })?,
            None => return Ok(None),
        };

        // Get tracing events
        let tracing_event_rows = traces_events_res
            .rows
            .ok_or(QueryError::ProtocolError(
                "Response to system_traces.events query was not Rows",
            ))?
            .into_typed::<TracingEvent>();

        for event in tracing_event_rows {
            let tracing_event: TracingEvent = event.map_err(|_| {
                QueryError::ProtocolError(
                    "Columns from system_traces.events have an unexpected type",
                )
            })?;

            tracing_info.events.push(tracing_event);
        }

        if tracing_info.events.is_empty() {
            return Ok(None);
        }

        Ok(Some(tracing_info))
    }

    // This method allows to easily run a query using load balancing, retry policy etc.
    // Requires some information about the query and two closures
    // First closure is used to choose a connection
    // - query will use node.random_connection()
    // - execute will use node.connection_for_token()
    // The second closure is used to do the query itself on a connection
    // - query will use connection.query()
    // - execute will use connection.execute()
    // If this query closure fails with some errors retry policy is used to perform retries
    // On success this query's result is returned
    // I tried to make this closures take a reference instead of an Arc but failed
    // maybe once async closures get stabilized this can be fixed
    async fn run_query<'a, ConnFut, QueryFut, ResT>(
        &'a self,
        statement_info: RoutingInfo<'a>,
        statement_config: &'a StatementConfig,
        execution_profile: Arc<ExecutionProfileInner>,
        choose_connection: impl Fn(Arc<Node>) -> ConnFut,
        do_query: impl Fn(Arc<Connection>, Consistency, &ExecutionProfileInner) -> QueryFut,
        request_span: &'a RequestSpan,
    ) -> Result<RunQueryResult<ResT>, QueryError>
    where
        ConnFut: Future<Output = Result<Arc<Connection>, QueryError>>,
        QueryFut: Future<Output = Result<ResT, QueryError>>,
        ResT: AllowedRunQueryResTType,
    {
        let history_listener_and_id: Option<(&'a dyn HistoryListener, history::QueryId)> =
            statement_config
                .history_listener
                .as_ref()
                .map(|hl| (&**hl, hl.log_query_start()));

        let load_balancer = &execution_profile.load_balancing_policy;

        let runner = async {
            let cluster_data = self.cluster.get_data();
            let query_plan =
                load_balancing::Plan::new(load_balancer.as_ref(), &statement_info, &cluster_data);

            // If a speculative execution policy is used to run query, query_plan has to be shared
            // between different async functions. This struct helps to wrap query_plan in mutex so it
            // can be shared safely.
            struct SharedPlan<'a, I>
            where
                I: Iterator<Item = NodeRef<'a>>,
            {
                iter: std::sync::Mutex<I>,
            }

            impl<'a, I> Iterator for &SharedPlan<'a, I>
            where
                I: Iterator<Item = NodeRef<'a>>,
            {
                type Item = NodeRef<'a>;

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
                    let shared_query_plan = SharedPlan {
                        iter: std::sync::Mutex::new(query_plan),
                    };

                    let execute_query_generator = |is_speculative: bool| {
                        let history_data: Option<HistoryData> = history_listener_and_id
                            .as_ref()
                            .map(|(history_listener, query_id)| {
                                let speculative_id: Option<history::SpeculativeId> =
                                    if is_speculative {
                                        Some(history_listener.log_new_speculative_fiber(*query_id))
                                    } else {
                                        None
                                    };
                                HistoryData {
                                    listener: *history_listener,
                                    query_id: *query_id,
                                    speculative_id,
                                }
                            });

                        if is_speculative {
                            request_span.inc_speculative_executions();
                        }

                        self.execute_query(
                            &shared_query_plan,
                            &choose_connection,
                            &do_query,
                            &execution_profile,
                            ExecuteQueryContext {
                                is_idempotent: statement_config.is_idempotent,
                                consistency_set_on_statement: statement_config.consistency,
                                retry_session: retry_policy.new_session(),
                                history_data,
                                query_info: &statement_info,
                                request_span,
                            },
                        )
                    };

                    let context = speculative_execution::Context {
                        metrics: self.metrics.clone(),
                    };

                    speculative_execution::execute(
                        speculative.as_ref(),
                        &context,
                        execute_query_generator,
                    )
                    .await
                }
                _ => {
                    let history_data: Option<HistoryData> =
                        history_listener_and_id
                            .as_ref()
                            .map(|(history_listener, query_id)| HistoryData {
                                listener: *history_listener,
                                query_id: *query_id,
                                speculative_id: None,
                            });
                    self.execute_query(
                        query_plan,
                        &choose_connection,
                        &do_query,
                        &execution_profile,
                        ExecuteQueryContext {
                            is_idempotent: statement_config.is_idempotent,
                            consistency_set_on_statement: statement_config.consistency,
                            retry_session: retry_policy.new_session(),
                            history_data,
                            query_info: &statement_info,
                            request_span,
                        },
                    )
                    .await
                    .unwrap_or(Err(QueryError::ProtocolError(
                        "Empty query plan - driver bug!",
                    )))
                }
            }
        };

        let effective_timeout = statement_config
            .request_timeout
            .or(execution_profile.request_timeout);
        let result = match effective_timeout {
            Some(timeout) => tokio::time::timeout(timeout, runner)
                .await
                .unwrap_or_else(|e| {
                    Err(QueryError::RequestTimeout(format!(
                        "Request took longer than {}ms: {}",
                        timeout.as_millis(),
                        e
                    )))
                }),
            None => runner.await,
        };

        if let Some((history_listener, query_id)) = history_listener_and_id {
            match &result {
                Ok(_) => history_listener.log_query_success(query_id),
                Err(e) => history_listener.log_query_error(query_id, e),
            }
        }

        result
    }

    async fn execute_query<'a, ConnFut, QueryFut, ResT>(
        &'a self,
        query_plan: impl Iterator<Item = NodeRef<'a>>,
        choose_connection: impl Fn(Arc<Node>) -> ConnFut,
        do_query: impl Fn(Arc<Connection>, Consistency, &ExecutionProfileInner) -> QueryFut,
        execution_profile: &ExecutionProfileInner,
        mut context: ExecuteQueryContext<'a>,
    ) -> Option<Result<RunQueryResult<ResT>, QueryError>>
    where
        ConnFut: Future<Output = Result<Arc<Connection>, QueryError>>,
        QueryFut: Future<Output = Result<ResT, QueryError>>,
        ResT: AllowedRunQueryResTType,
    {
        let mut last_error: Option<QueryError> = None;
        let mut current_consistency: Consistency = context
            .consistency_set_on_statement
            .unwrap_or(execution_profile.consistency);

        'nodes_in_plan: for node in query_plan {
            let span = trace_span!("Executing query", node = %node.address);
            'same_node_retries: loop {
                trace!(parent: &span, "Execution started");
                let connection: Arc<Connection> = match choose_connection(node.clone())
                    .instrument(span.clone())
                    .await
                {
                    Ok(connection) => connection,
                    Err(e) => {
                        trace!(
                            parent: &span,
                            error = %e,
                            "Choosing connection failed"
                        );
                        last_error = Some(e);
                        // Broken connection doesn't count as a failed query, don't log in metrics
                        continue 'nodes_in_plan;
                    }
                };
                context.request_span.record_shard_id(&connection);

                self.metrics.inc_total_nonpaged_queries();
                let query_start = std::time::Instant::now();

                trace!(
                    parent: &span,
                    connection = %connection.get_connect_address(),
                    "Sending"
                );
                let attempt_id: Option<history::AttemptId> =
                    context.log_attempt_start(connection.get_connect_address());
                let query_result: Result<ResT, QueryError> =
                    do_query(connection, current_consistency, execution_profile)
                        .instrument(span.clone())
                        .await;

                let elapsed = query_start.elapsed();
                last_error = match query_result {
                    Ok(response) => {
                        trace!(parent: &span, "Query succeeded");
                        let _ = self.metrics.log_query_latency(elapsed.as_millis() as u64);
                        context.log_attempt_success(&attempt_id);
                        execution_profile.load_balancing_policy.on_query_success(
                            context.query_info,
                            elapsed,
                            node,
                        );
                        return Some(Ok(RunQueryResult::Completed(response)));
                    }
                    Err(e) => {
                        trace!(
                            parent: &span,
                            last_error = %e,
                            "Query failed"
                        );
                        self.metrics.inc_failed_nonpaged_queries();
                        execution_profile.load_balancing_policy.on_query_failure(
                            context.query_info,
                            elapsed,
                            node,
                            &e,
                        );
                        Some(e)
                    }
                };

                let the_error: &QueryError = last_error.as_ref().unwrap();
                // Use retry policy to decide what to do next
                let query_info = QueryInfo {
                    error: the_error,
                    is_idempotent: context.is_idempotent,
                    consistency: context
                        .consistency_set_on_statement
                        .unwrap_or(execution_profile.consistency),
                };

                let retry_decision = context.retry_session.decide_should_retry(query_info);
                trace!(
                    parent: &span,
                    retry_decision = format!("{:?}", retry_decision).as_str()
                );
                context.log_attempt_error(&attempt_id, the_error, &retry_decision);
                match retry_decision {
                    RetryDecision::RetrySameNode(new_cl) => {
                        self.metrics.inc_retries_num();
                        current_consistency = new_cl.unwrap_or(current_consistency);
                        continue 'same_node_retries;
                    }
                    RetryDecision::RetryNextNode(new_cl) => {
                        self.metrics.inc_retries_num();
                        current_consistency = new_cl.unwrap_or(current_consistency);
                        continue 'nodes_in_plan;
                    }
                    RetryDecision::DontRetry => break 'nodes_in_plan,

                    RetryDecision::IgnoreWriteError => {
                        return Some(Ok(RunQueryResult::IgnoredWriteError))
                    }
                };
            }
        }

        last_error.map(Result::Err)
    }

    async fn await_schema_agreement_indefinitely(&self) -> Result<Uuid, QueryError> {
        loop {
            tokio::time::sleep(self.schema_agreement_interval).await;
            if let Some(agreed_version) = self.check_schema_agreement().await? {
                return Ok(agreed_version);
            }
        }
    }

    pub async fn await_schema_agreement(&self) -> Result<Uuid, QueryError> {
        timeout(
            self.schema_agreement_timeout,
            self.await_schema_agreement_indefinitely(),
        )
        .await
        .unwrap_or(Err(QueryError::RequestTimeout(
            "schema agreement not reached in time".to_owned(),
        )))
    }

    pub async fn check_schema_agreement(&self) -> Result<Option<Uuid>, QueryError> {
        let cluster_data = self.get_cluster_data();
        let connections_iter = cluster_data.iter_working_connections()?;

        let handles = connections_iter.map(|c| async move { c.fetch_schema_version().await });
        let versions = try_join_all(handles).await?;

        let local_version: Uuid = versions[0];
        let in_agreement = versions.into_iter().all(|v| v == local_version);
        Ok(in_agreement.then_some(local_version))
    }

    /// Retrieves the handle to execution profile that is used by this session
    /// by default, i.e. when an executed statement does not define its own handle.
    pub fn get_default_execution_profile_handle(&self) -> &ExecutionProfileHandle {
        &self.default_execution_profile_handle
    }
}

// run_query, execute_query, etc have a template type called ResT.
// There was a bug where ResT was set to QueryResponse, which could
// be an error response. This was not caught by retry policy which
// assumed all errors would come from analyzing Result<ResT, QueryError>.
// This trait is a guard to make sure that this mistake doesn't
// happen again.
// When using run_query make sure that the ResT type is NOT able
// to contain any errors.
// See https://github.com/scylladb/scylla-rust-driver/issues/501
pub(crate) trait AllowedRunQueryResTType {}

impl AllowedRunQueryResTType for Uuid {}
impl AllowedRunQueryResTType for QueryResult {}
impl AllowedRunQueryResTType for NonErrorQueryResponse {}

struct ExecuteQueryContext<'a> {
    is_idempotent: bool,
    consistency_set_on_statement: Option<Consistency>,
    retry_session: Box<dyn RetrySession>,
    history_data: Option<HistoryData<'a>>,
    query_info: &'a load_balancing::RoutingInfo<'a>,
    request_span: &'a RequestSpan,
}

struct HistoryData<'a> {
    listener: &'a dyn HistoryListener,
    query_id: history::QueryId,
    speculative_id: Option<history::SpeculativeId>,
}

impl<'a> ExecuteQueryContext<'a> {
    fn log_attempt_start(&self, node_addr: SocketAddr) -> Option<history::AttemptId> {
        self.history_data.as_ref().map(|hd| {
            hd.listener
                .log_attempt_start(hd.query_id, hd.speculative_id, node_addr)
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
        error: &QueryError,
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
