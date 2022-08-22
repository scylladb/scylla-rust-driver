//! `Session` is the main object used in the driver.\
//! It manages all connections to the cluster and allows to perform queries.

use crate::frame::types::LegacyConsistency;
use bytes::Bytes;
use futures::future::join_all;
use futures::future::try_join_all;
use std::future::Future;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::net::lookup_host;
use tokio::time::timeout;
use tracing::{debug, error, trace, trace_span, Instrument};
use uuid::Uuid;

use super::connection::NonErrorQueryResponse;
use super::connection::QueryResponse;
use super::errors::{BadQuery, NewSessionError, QueryError};
use crate::cql_to_rust::FromRow;
use crate::frame::response::cql_to_rust::FromRowError;
use crate::frame::response::result;
use crate::frame::value::{BatchValues, SerializedValues, ValueList};
use crate::prepared_statement::{PartitionKeyError, PreparedStatement};
use crate::query::Query;
use crate::routing::Token;
use crate::statement::{Consistency, SerialConsistency};
use crate::tracing::{GetTracingConfig, TracingEvent, TracingInfo};
use crate::transport::cluster::{Cluster, ClusterData, ClusterNeatDebug};
use crate::transport::connection::{
    BatchResult, Connection, ConnectionConfig, VerifiedKeyspaceName,
};
use crate::transport::connection_pool::PoolConfig;
use crate::transport::iterator::{PreparedIteratorConfig, RowIterator};
use crate::transport::load_balancing::{
    LoadBalancingPolicy, RoundRobinPolicy, Statement, TokenAwarePolicy,
};
use crate::transport::metrics::Metrics;
use crate::transport::node::Node;
use crate::transport::partitioner::{
    CDCPartitioner, Murmur3Partitioner, Partitioner, PartitionerName,
};
use crate::transport::query_result::QueryResult;
use crate::transport::retry_policy::{
    DefaultRetryPolicy, QueryInfo, RetryDecision, RetryPolicy, RetrySession,
};
use crate::transport::speculative_execution;
use crate::transport::speculative_execution::SpeculativeExecutionPolicy;
use crate::transport::Compression;
use crate::{batch::Batch, statement::StatementConfig};

pub use crate::transport::connection_pool::PoolSize;

#[cfg(feature = "ssl")]
use openssl::ssl::SslContext;

/// `Session` manages connections to the cluster and allows to perform queries
pub struct Session {
    cluster: Cluster,
    load_balancer: Arc<dyn LoadBalancingPolicy>,
    schema_agreement_interval: Duration,
    retry_policy: Box<dyn RetryPolicy>,
    speculative_execution_policy: Option<Arc<dyn SpeculativeExecutionPolicy>>,
    metrics: Arc<Metrics>,
    default_consistency: Consistency,
    auto_await_schema_agreement_timeout: Option<Duration>,
}

/// This implementation deliberately omits some details from Cluster in order
/// to avoid cluttering the print with much information of little usability.
impl std::fmt::Debug for Session {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Session")
            .field("cluster", &ClusterNeatDebug(&self.cluster))
            .field("load_balancer", &self.load_balancer)
            .field("schema_agreement_interval", &self.schema_agreement_interval)
            .field("retry_policy", &self.retry_policy)
            .field(
                "speculative_execution_policy",
                &self.speculative_execution_policy,
            )
            .field("metrics", &self.metrics)
            .field("default_consistency", &self.default_consistency)
            .field(
                "auto_await_schema_agreement_timeout",
                &self.auto_await_schema_agreement_timeout,
            )
            .finish()
    }
}

/// Configuration options for [`Session`].
/// Can be created manually, but usually it's easier to use
/// [SessionBuilder](super::session_builder::SessionBuilder)
#[derive(Clone)]
pub struct SessionConfig {
    /// List of database servers known on Session startup.
    /// Session will connect to these nodes to retrieve information about other nodes in the cluster.
    /// Each node can be represented as a hostname or an IP address.
    pub known_nodes: Vec<KnownNode>,

    /// Preferred compression algorithm to use on connections.
    /// If it's not supported by database server Session will fall back to no compression.
    pub compression: Option<Compression>,
    pub tcp_nodelay: bool,

    /// Load balancing policy used by Session
    pub load_balancing: Arc<dyn LoadBalancingPolicy>,

    pub used_keyspace: Option<String>,
    pub keyspace_case_sensitive: bool,

    pub retry_policy: Box<dyn RetryPolicy>,
    pub speculative_execution_policy: Option<Arc<dyn SpeculativeExecutionPolicy>>,

    /// Provide our Session with TLS
    #[cfg(feature = "ssl")]
    pub ssl_context: Option<SslContext>,

    pub auth_username: Option<String>,
    pub auth_password: Option<String>,

    pub schema_agreement_interval: Duration,
    pub connect_timeout: std::time::Duration,

    /// Size of the per-node connection pool, i.e. how many connections the driver should keep to each node.
    /// The default is `PerShard(1)`, which is the recommended setting for Scylla clusters.
    pub connection_pool_size: PoolSize,

    /// If true, prevents the driver from connecting to the shard-aware port, even if the node supports it.
    /// Generally, this options is best left as default (false).
    pub disallow_shard_aware_port: bool,

    pub default_consistency: Consistency,

    /// If true, full schema is fetched with every metadata refresh.
    pub fetch_schema_metadata: bool,

    /// Interval of sending keepalive requests
    pub keepalive_interval: Option<Duration>,

    /// Controls the timeout for the automatic wait for schema agreement after sending a schema-altering statement.
    /// If `None`, the automatic schema agreement is disabled.
    pub auto_await_schema_agreement_timeout: Option<Duration>,
}

/// Describes database server known on Session startup.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Debug)]
pub enum KnownNode {
    Hostname(String),
    Address(SocketAddr),
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
            schema_agreement_interval: Duration::from_millis(200),
            load_balancing: Arc::new(TokenAwarePolicy::new(Box::new(RoundRobinPolicy::new()))),
            used_keyspace: None,
            keyspace_case_sensitive: false,
            retry_policy: Box::new(DefaultRetryPolicy),
            speculative_execution_policy: None,
            #[cfg(feature = "ssl")]
            ssl_context: None,
            auth_username: None,
            auth_password: None,
            connect_timeout: std::time::Duration::from_secs(5),
            connection_pool_size: Default::default(),
            disallow_shard_aware_port: false,
            default_consistency: Consistency::LocalQuorum,
            fetch_schema_metadata: true,
            keepalive_interval: None,
            auto_await_schema_agreement_timeout: Some(std::time::Duration::from_secs(60)),
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
    pub fn add_known_nodes(&mut self, hostnames: &[impl AsRef<str>]) {
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
    pub fn add_known_nodes_addr(&mut self, node_addrs: &[SocketAddr]) {
        for address in node_addrs {
            self.add_known_node_addr(*address);
        }
    }

    /// Creates a PoolConfig which can be used to create NodeConnectionPools
    fn get_pool_config(&self) -> PoolConfig {
        PoolConfig {
            connection_config: self.get_connection_config(),
            pool_size: self.connection_pool_size.clone(),
            can_use_shard_aware_port: !self.disallow_shard_aware_port,
            keepalive_interval: self.keepalive_interval,
        }
    }

    /// Makes a config that should be used in Connection
    fn get_connection_config(&self) -> ConnectionConfig {
        ConnectionConfig {
            compression: self.compression,
            tcp_nodelay: self.tcp_nodelay,
            #[cfg(feature = "ssl")]
            ssl_context: self.ssl_context.clone(),
            auth_username: self.auth_username.to_owned(),
            auth_password: self.auth_password.to_owned(),
            connect_timeout: self.connect_timeout,
            event_sender: None,
            default_consistency: self.default_consistency,
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

/// Represents a CQL session, which can be used to communicate
/// with the database
impl Session {
    // because it's more convenient
    /// Estabilishes a CQL session with the database
    ///
    /// Usually it's easier to use [SessionBuilder](crate::transport::session_builder::SessionBuilder)
    /// instead of calling `Session::connect` directly
    /// # Arguments
    /// * `config` - Connection configuration - known nodes, Compression, etc.
    /// Must contain at least one known node.
    ///
    /// # Example
    /// ```rust
    /// # use std::error::Error;
    /// # async fn check_only_compiles() -> Result<(), Box<dyn Error>> {
    /// use scylla::{Session, SessionConfig};
    /// use scylla::transport::session::KnownNode;
    ///
    /// let mut config = SessionConfig::new();
    /// config.known_nodes.push(KnownNode::Hostname("127.0.0.1:9042".to_string()));
    ///
    /// let session: Session = Session::connect(config).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn connect(config: SessionConfig) -> Result<Session, NewSessionError> {
        // Ensure there is at least one known node
        if config.known_nodes.is_empty() {
            return Err(NewSessionError::EmptyKnownNodesList);
        }

        // Find IP addresses of all known nodes passed in the config
        let mut node_addresses: Vec<SocketAddr> = Vec::with_capacity(config.known_nodes.len());

        let mut to_resolve: Vec<&str> = Vec::new();

        for node in &config.known_nodes {
            match node {
                KnownNode::Hostname(hostname) => to_resolve.push(hostname),
                KnownNode::Address(address) => node_addresses.push(*address),
            };
        }

        let resolve_futures = to_resolve.into_iter().map(resolve_hostname);
        let resolved: Vec<SocketAddr> = futures::future::try_join_all(resolve_futures).await?;

        node_addresses.extend(resolved);

        let cluster = Cluster::new(
            &node_addresses,
            config.get_pool_config(),
            config.fetch_schema_metadata,
        )
        .await?;

        let session = Session {
            cluster,
            load_balancer: config.load_balancing,
            retry_policy: config.retry_policy,
            schema_agreement_interval: config.schema_agreement_interval,
            speculative_execution_policy: config.speculative_execution_policy,
            metrics: Arc::new(Metrics::new()),
            default_consistency: config.default_consistency,
            auto_await_schema_agreement_timeout: config.auto_await_schema_agreement_timeout,
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

        let span = trace_span!("Request", query = query.contents.as_str());
        let response = self
            .run_query(
                Statement::default(),
                &query.config,
                |node: Arc<Node>| async move { node.random_connection().await },
                |connection: Arc<Connection>| {
                    // Needed to avoid moving query and values into async move block
                    let query_ref = &query;
                    let values_ref = &serialized_values;
                    let paging_state_ref = &paging_state;

                    async move {
                        connection
                            .query(query_ref, values_ref, paging_state_ref.clone())
                            .await
                            .and_then(QueryResponse::into_non_error_query_response)
                    }
                },
            )
            .instrument(span)
            .await?;
        self.handle_set_keyspace_response(&response).await?;
        self.handle_auto_await_schema_agreement(&query.contents, &response)
            .await?;

        response.into_query_result()
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
        contents: &str,
        response: &NonErrorQueryResponse,
    ) -> Result<(), QueryError> {
        if let Some(timeout) = self.auto_await_schema_agreement_timeout {
            if response.as_schema_change().is_some()
                && !self.await_timed_schema_agreement(timeout).await?
            {
                // TODO: The TimeoutError should allow to provide more context.
                // For now, print an error to the logs
                error!(
                    "Failed to reach schema agreement after a schema-altering statement: {}",
                    contents,
                );
                return Err(QueryError::TimeoutError);
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

        let retry_session = match &query.config.retry_policy {
            Some(policy) => policy.new_session(),
            None => self.retry_policy.new_session(),
        };

        let span = trace_span!("Request", query = query.contents.as_str());
        RowIterator::new_for_query(
            query,
            serialized_values.into_owned(),
            self.default_consistency,
            retry_session,
            self.load_balancer.clone(),
            self.cluster.get_data(),
            self.metrics.clone(),
        )
        .instrument(span)
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

        let connections = self.cluster.get_working_connections().await?;

        // Prepare statements on all connections concurrently
        let handles = connections.iter().map(|c| c.prepare(&query));
        let mut results = join_all(handles).await;

        // If at least one prepare was successful prepare returns Ok

        // Find first result that is Ok, or Err if all failed
        let mut first_ok: Option<Result<PreparedStatement, QueryError>> = None;

        while let Some(res) = results.pop() {
            let is_ok: bool = res.is_ok();

            first_ok = Some(res);

            if is_ok {
                break;
            }
        }

        let mut prepared: PreparedStatement = first_ok.unwrap()?;

        // Validate prepared ids equality
        for statement in results.into_iter().flatten() {
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
            self.extract_partitioner_name(&prepared, &self.cluster.get_data()),
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
    /// * `prepared` - a statement prepared with [prepare](crate::transport::session::Session::prepare)
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

        let token = self.calculate_token(prepared, &serialized_values)?;

        let statement_info = Statement {
            token,
            keyspace: prepared.get_keyspace_name(),
        };

        let span = trace_span!(
            "Request",
            prepared_id = format!("{:X}", prepared.get_id()).as_str()
        );
        let response: NonErrorQueryResponse = self
            .run_query(
                statement_info,
                &prepared.config,
                |node: Arc<Node>| async move {
                    match token {
                        Some(token) => node.connection_for_token(token).await,
                        None => node.random_connection().await,
                    }
                },
                |connection: Arc<Connection>| async move {
                    connection
                        .execute(prepared, values_ref, paging_state_ref.clone())
                        .await
                        .and_then(QueryResponse::into_non_error_query_response)
                },
            )
            .instrument(span)
            .await?;
        self.handle_set_keyspace_response(&response).await?;
        self.handle_auto_await_schema_agreement(prepared.get_statement(), &response)
            .await?;

        response.into_query_result()
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

        let token = self.calculate_token(&prepared, &serialized_values)?;

        let retry_session = match &prepared.config.retry_policy {
            Some(policy) => policy.new_session(),
            None => self.retry_policy.new_session(),
        };

        let span = trace_span!(
            "Request",
            prepared_id = format!("{:X}", prepared.get_id()).as_str()
        );
        RowIterator::new_for_prepared_statement(PreparedIteratorConfig {
            prepared,
            values: serialized_values.into_owned(),
            default_consistency: self.default_consistency,
            token,
            retry_session,
            load_balancer: self.load_balancer.clone(),
            cluster_data: self.cluster.get_data(),
            metrics: self.metrics.clone(),
        })
        .instrument(span)
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
    ) -> Result<BatchResult, QueryError> {
        let values_ref = &values;

        self.run_query(
            Statement::default(),
            &batch.config,
            |node: Arc<Node>| async move { node.random_connection().await },
            |connection: Arc<Connection>| async move { connection.batch(batch, values_ref).await },
        )
        .instrument(trace_span!("Batch"))
        .await
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
        // Trying to pass keyspace as bound value in "USE ?" doesn't work
        // So we have to create a string for query: "USE " + new_keyspace
        // To avoid any possible CQL injections it's good to verify that the name is valid
        let verified_ks_name = VerifiedKeyspaceName::new(keyspace_name.into(), case_sensitive)?;

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
        self.get_tracing_info_custom(tracing_id, &GetTracingConfig::default())
            .await
    }

    /// Queries tracing info with custom retry settings.\
    /// Tracing info might not be available immediately on queried node -
    /// that's why the driver performs a few attempts with sleeps in between.
    /// [`GetTracingConfig`] allows to specify a custom querying strategy.
    pub async fn get_tracing_info_custom(
        &self,
        tracing_id: &Uuid,
        config: &GetTracingConfig,
    ) -> Result<TracingInfo, QueryError> {
        // config.attempts is NonZeroU32 so at least one attempt will be made
        for _ in 0..config.attempts.get() {
            let current_try: Option<TracingInfo> = self
                .try_getting_tracing_info(tracing_id, Some(config.consistency))
                .await?;

            match current_try {
                Some(tracing_info) => return Ok(tracing_info),
                None => tokio::time::sleep(config.interval).await,
            };
        }

        Err(QueryError::ProtocolError(
            "All tracing queries returned an empty result, \
            maybe information didn't reach this node yet. \
            Consider using get_tracing_info_custom with \
            bigger interval in GetTracingConfig",
        ))
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
        let mut traces_session_query = Query::new(crate::tracing::TRACES_SESSION_QUERY_STR);
        traces_session_query.config.consistency = consistency;
        traces_session_query.set_page_size(1024);

        // Query system_traces.events for TracingEvents
        let mut traces_events_query = Query::new(crate::tracing::TRACES_EVENTS_QUERY_STR);
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
        statement_info: Statement<'a>,
        statement_config: &StatementConfig,
        choose_connection: impl Fn(Arc<Node>) -> ConnFut,
        do_query: impl Fn(Arc<Connection>) -> QueryFut,
    ) -> Result<ResT, QueryError>
    where
        ConnFut: Future<Output = Result<Arc<Connection>, QueryError>>,
        QueryFut: Future<Output = Result<ResT, QueryError>>,
        ResT: AllowedRunQueryResTType,
    {
        let cluster_data = self.cluster.get_data();
        let query_plan = self.load_balancer.plan(&statement_info, &cluster_data);

        // If a speculative execution policy is used to run query, query_plan has to be shared
        // between different async functions. This struct helps to wrap query_plan in mutex so it
        // can be shared safely.
        struct SharedPlan<I>
        where
            I: Iterator<Item = Arc<Node>>,
        {
            iter: std::sync::Mutex<I>,
        }

        impl<I> Iterator for &SharedPlan<I>
        where
            I: Iterator<Item = Arc<Node>>,
        {
            type Item = Arc<Node>;

            fn next(&mut self) -> Option<Self::Item> {
                self.iter.lock().unwrap().next()
            }
        }

        let retry_policy = match &statement_config.retry_policy {
            Some(policy) => policy,
            None => &self.retry_policy,
        };

        #[allow(clippy::unnecessary_lazy_evaluations)]
        let speculative_policy = statement_config
            .speculative_execution_policy
            .as_ref()
            .or_else(|| self.speculative_execution_policy.as_ref());

        match speculative_policy {
            Some(speculative) if statement_config.is_idempotent => {
                let shared_query_plan = SharedPlan {
                    iter: std::sync::Mutex::new(query_plan),
                };

                let execute_query_generator = || {
                    self.execute_query(
                        &shared_query_plan,
                        statement_config.is_idempotent,
                        statement_config.consistency,
                        retry_policy.new_session(),
                        &choose_connection,
                        &do_query,
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
            _ => self
                .execute_query(
                    query_plan,
                    statement_config.is_idempotent,
                    statement_config.consistency,
                    retry_policy.new_session(),
                    &choose_connection,
                    &do_query,
                )
                .await
                .unwrap_or(Err(QueryError::ProtocolError(
                    "Empty query plan - driver bug!",
                ))),
        }
    }

    async fn execute_query<ConnFut, QueryFut, ResT>(
        &self,
        query_plan: impl Iterator<Item = Arc<Node>>,
        is_idempotent: bool,
        consistency: Option<Consistency>,
        mut retry_session: Box<dyn RetrySession>,
        choose_connection: impl Fn(Arc<Node>) -> ConnFut,
        do_query: impl Fn(Arc<Connection>) -> QueryFut,
    ) -> Option<Result<ResT, QueryError>>
    where
        ConnFut: Future<Output = Result<Arc<Connection>, QueryError>>,
        QueryFut: Future<Output = Result<ResT, QueryError>>,
        ResT: AllowedRunQueryResTType,
    {
        let mut last_error: Option<QueryError> = None;

        'nodes_in_plan: for node in query_plan {
            let span = trace_span!("Executing query", node = node.address.to_string().as_str());
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
                            error = e.to_string().as_str(),
                            "Choosing connection failed"
                        );
                        last_error = Some(e);
                        // Broken connection doesn't count as a failed query, don't log in metrics
                        continue 'nodes_in_plan;
                    }
                };

                self.metrics.inc_total_nonpaged_queries();
                let query_start = std::time::Instant::now();

                trace!(
                    parent: &span,
                    connection = connection.get_connect_address().to_string().as_str(),
                    "Sending"
                );
                let query_result: Result<ResT, QueryError> =
                    do_query(connection).instrument(span.clone()).await;

                last_error = match query_result {
                    Ok(response) => {
                        trace!(parent: &span, "Query succeeded");
                        let _ = self
                            .metrics
                            .log_query_latency(query_start.elapsed().as_millis() as u64);
                        return Some(Ok(response));
                    }
                    Err(e) => {
                        trace!(
                            parent: &span,
                            last_error = e.to_string().as_str(),
                            "Query failed"
                        );
                        self.metrics.inc_failed_nonpaged_queries();
                        Some(e)
                    }
                };

                // Use retry policy to decide what to do next
                let query_info = QueryInfo {
                    error: last_error.as_ref().unwrap(),
                    is_idempotent,
                    consistency: LegacyConsistency::Regular(
                        consistency.unwrap_or(self.default_consistency),
                    ),
                };

                let retry_decision = retry_session.decide_should_retry(query_info);
                trace!(
                    parent: &span,
                    retry_decision = format!("{:?}", retry_decision).as_str()
                );
                match retry_decision {
                    RetryDecision::RetrySameNode => {
                        self.metrics.inc_retries_num();
                        continue 'same_node_retries;
                    }
                    RetryDecision::RetryNextNode => {
                        self.metrics.inc_retries_num();
                        continue 'nodes_in_plan;
                    }
                    RetryDecision::DontRetry => return last_error.map(Result::Err),
                };
            }
        }

        last_error.map(Result::Err)
    }

    pub async fn await_schema_agreement(&self) -> Result<(), QueryError> {
        while !self.check_schema_agreement().await? {
            tokio::time::sleep(self.schema_agreement_interval).await
        }
        Ok(())
    }

    pub async fn await_timed_schema_agreement(
        &self,
        timeout_duration: Duration,
    ) -> Result<bool, QueryError> {
        timeout(timeout_duration, self.await_schema_agreement())
            .await
            .map_or(Ok(false), |res| res.and(Ok(true)))
    }

    async fn schema_agreement_auxilary<ResT, QueryFut>(
        &self,
        do_query: impl Fn(Arc<Connection>) -> QueryFut,
    ) -> Result<ResT, QueryError>
    where
        QueryFut: Future<Output = Result<ResT, QueryError>>,
        ResT: AllowedRunQueryResTType,
    {
        let info = Statement::default();
        let config = StatementConfig {
            is_idempotent: true,
            serial_consistency: Some(SerialConsistency::LocalSerial),
            ..Default::default()
        };

        self.run_query(
            info,
            &config,
            |node: Arc<Node>| async move { node.random_connection().await },
            do_query,
        )
        .await
    }

    pub async fn check_schema_agreement(&self) -> Result<bool, QueryError> {
        let connections = self.cluster.get_working_connections().await?;

        let handles = connections.iter().map(|c| c.fetch_schema_version());
        let versions = try_join_all(handles).await?;

        let local_version: Uuid = versions[0];
        let in_agreement = versions.into_iter().all(|v| v == local_version);
        Ok(in_agreement)
    }

    pub async fn fetch_schema_version(&self) -> Result<Uuid, QueryError> {
        self.schema_agreement_auxilary(|connection: Arc<Connection>| async move {
            connection.fetch_schema_version().await
        })
        .await
    }

    fn calculate_token(
        &self,
        prepared: &PreparedStatement,
        serialized_values: &SerializedValues,
    ) -> Result<Option<Token>, QueryError> {
        if !prepared.is_token_aware() {
            return Ok(None);
        }

        let partitioner_name = prepared.get_partitioner_name();

        let partition_key = calculate_partition_key(prepared, serialized_values)?;

        Ok(Some(match partitioner_name {
            PartitionerName::Murmur3 => Murmur3Partitioner::hash(partition_key),
            PartitionerName::CDC => CDCPartitioner::hash(partition_key),
        }))
    }
}

fn calculate_partition_key(
    stmt: &PreparedStatement,
    values: &SerializedValues,
) -> Result<Bytes, QueryError> {
    match stmt.compute_partition_key(values) {
        Ok(key) => Ok(key),
        Err(PartitionKeyError::NoPkIndexValue(_, _)) => Err(QueryError::ProtocolError(
            "No pk indexes - can't calculate token",
        )),
        Err(PartitionKeyError::ValueTooLong(values_len)) => Err(QueryError::BadQuery(
            BadQuery::ValuesTooLongForKey(values_len, u16::MAX.into()),
        )),
    }
}

// Resolve the given hostname using a DNS lookup if necessary.
// The resolution may return multiple IPs and the function returns one of them.
// It prefers to return IPv4s first, and only if there are none, IPv6s.
async fn resolve_hostname(hostname: &str) -> Result<SocketAddr, NewSessionError> {
    let failed_err = NewSessionError::FailedToResolveAddress(hostname.to_string());
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

    ret.ok_or(failed_err)
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
pub trait AllowedRunQueryResTType {}

impl AllowedRunQueryResTType for Uuid {}
impl AllowedRunQueryResTType for BatchResult {}
impl AllowedRunQueryResTType for NonErrorQueryResponse {}
