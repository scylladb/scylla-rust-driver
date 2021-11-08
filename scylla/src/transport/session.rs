//! `Session` is the main object used in the driver.  
//! It manages all connections to the cluster and allows to perform queries.

use bytes::Bytes;
use futures::future::join_all;
use std::future::Future;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::net::lookup_host;
use tokio::time::timeout;
use tracing::debug;
use uuid::Uuid;

use super::connection::QueryResponse;
use super::errors::{BadQuery, NewSessionError, QueryError};
use crate::frame::response::cql_to_rust::FromRowError;
use crate::frame::response::result;
use crate::frame::value::{BatchValues, SerializedValues, ValueList};
use crate::prepared_statement::{PartitionKeyError, PreparedStatement};
use crate::query::Query;
use crate::routing::{murmur3_token, Token};
use crate::statement::{Consistency, SerialConsistency};
use crate::tracing::{GetTracingConfig, TracingEvent, TracingInfo};
use crate::transport::connection_pool::PoolConfig;
use crate::transport::{
    cluster::Cluster,
    connection::{BatchResult, Connection, ConnectionConfig, QueryResult, VerifiedKeyspaceName},
    iterator::RowIterator,
    load_balancing::{LoadBalancingPolicy, RoundRobinPolicy, Statement, TokenAwarePolicy},
    metrics::Metrics,
    node::Node,
    retry_policy::{DefaultRetryPolicy, QueryInfo, RetryDecision, RetryPolicy, RetrySession},
    speculative_execution::SpeculativeExecutionPolicy,
    Compression,
};
use crate::{batch::Batch, statement::StatementConfig};
use crate::{cql_to_rust::FromRow, transport::speculative_execution};

use crate::transport::errors::DbError;
pub use crate::transport::connection_pool::PoolSize;

#[cfg(feature = "ssl")]
use openssl::ssl::SslContext;
use itertools::Either;
use dashmap::DashMap;

/// `Session` manages connections to the cluster and allows to perform queries
pub struct Session {
    cluster: Cluster,
    load_balancer: Arc<dyn LoadBalancingPolicy>,
    schema_agreement_interval: Duration,
    retry_policy: Box<dyn RetryPolicy>,
    speculative_execution_policy: Option<Arc<dyn SpeculativeExecutionPolicy>>,
    metrics: Arc<Metrics>,
    prepared_statement_cache: PreparedStatementCache,
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

    /// The prepared statement cache size
    /// If a prepared statement is added while the limit is reached, the oldest prepared statement
    /// is removed from the cache
    pub prepared_statement_cache_size: usize,

    /// Size of the per-node connection pool, i.e. how many connections the driver should keep to each node.
    /// The default is `PerShard(1)`, which is the recommended setting for Scylla clusters.
    pub connection_pool_size: PoolSize,

    /// If true, prevents the driver from connecting to the shard-aware port, even if the node supports it.
    /// Generally, this options is best left as default (false).
    pub disallow_shard_aware_port: bool,
    /*
    These configuration options will be added in the future:


    pub tcp_keepalive: bool,

    pub default_consistency: Option<String>,
    */
}

pub struct PreparedStatementCache {
    max_capacity: usize,
    cache: DashMap<String, PreparedStatement>,
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
            prepared_statement_cache_size: 10_000,
        }
    }

    pub fn set_prepared_statement_cache_size(&mut self, prepared_statement_cache_size: usize) {
        self.prepared_statement_cache_size = prepared_statement_cache_size;
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
            ..Default::default()
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

/// Iterator over rows parsed as the given type  
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

        let cluster = Cluster::new(&node_addresses, config.get_pool_config()).await?;

        let session = Session {
            cluster,
            load_balancer: config.load_balancing,
            retry_policy: config.retry_policy,
            schema_agreement_interval: config.schema_agreement_interval,
            speculative_execution_policy: config.speculative_execution_policy,
            metrics: Arc::new(Metrics::new()),
            prepared_statement_cache: PreparedStatementCache {
                max_capacity: config.prepared_statement_cache_size,
                // Don't initialize it already with the prepared_statement_cache_size capacity
                // since that would be a waste of memory if it won't be filled
                cache: Default::default(),
            },
        };

        if let Some(keyspace_name) = config.used_keyspace {
            session
                .use_keyspace(keyspace_name, config.keyspace_case_sensitive)
                .await?;
        }

        Ok(session)
    }

    /// Sends a query to the database and receives a response.  
    /// Returns only a single page of results, to receive multiple pages use [query_iter](Session::query_iter)
    ///
    /// This is the easiest way to make a query, but performance is worse than that of prepared queries.
    ///
    /// See [the book](https://cvybhu.github.io/scyllabook/queries/simple.html) for more information
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
    ///         // Parse row as int and text   
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
        let query = query.into();
        let serialized_values = values.serialized();

        // Needed to avoid moving query and values into async move block
        let query_ref: &Query = &query;
        let values_ref = &serialized_values;
        let paging_state_ref = &paging_state;

        let response = self
            .run_query(
                Statement::default(),
                &query.config,
                |node: Arc<Node>| async move { node.random_connection().await },
                |connection: Arc<Connection>| async move {
                    connection
                        .query(query_ref, values_ref, paging_state_ref.clone())
                        .await
                },
            )
            .await?;
        self.handle_set_keyspace_response(&response).await?;

        response.into_query_result()
    }

    async fn handle_set_keyspace_response(
        &self,
        response: &QueryResponse,
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

    /// Run a simple query with paging  
    /// This method will query all pages of the result  
    ///
    /// Returns an async iterator (stream) over all received rows  
    /// Page size can be specified in the [Query](crate::query::Query) passed to the function
    ///
    /// See [the book](https://cvybhu.github.io/scyllabook/queries/paged.html) for more information
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

        Ok(RowIterator::new_for_query(
            query,
            serialized_values.into_owned(),
            retry_session,
            self.load_balancer.clone(),
            self.cluster.get_data(),
            self.metrics.clone(),
        ))
    }

    /// Prepares a statement on the server side and returns a prepared statement,
    /// which can later be used to perform more efficient queries
    ///
    /// Prepared queries are much faster than simple queries:
    /// * Database doesn't need to parse the query
    /// * They are properly load balanced using token aware routing
    ///
    /// > ***Warning***  
    /// > For token/shard aware load balancing to work properly, all partition key values
    /// > must be sent as bound values
    /// > (see [performance section](https://cvybhu.github.io/scyllabook/queries/prepared.html#performance))
    ///
    /// See [the book](https://cvybhu.github.io/scyllabook/queries/prepared.html) for more information
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

        // If at least one prepare was succesfull prepare returns Ok

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

        Ok(prepared)
    }

    /// Execute a prepared query. Requires a [PreparedStatement](crate::prepared_statement::PreparedStatement)
    /// generated using [`Session::prepare`](Session::prepare)  
    /// Returns only a single page of results, to receive multiple pages use [execute_iter](Session::execute_iter)
    ///
    /// Prepared queries are much faster than simple queries:
    /// * Database doesn't need to parse the query
    /// * They are properly load balanced using token aware routing
    ///
    /// > ***Warning***  
    /// > For token/shard aware load balancing to work properly, all partition key values
    /// > must be sent as bound values
    /// > (see [performance section](https://cvybhu.github.io/scyllabook/queries/prepared.html#performance))
    ///
    /// See [the book](https://cvybhu.github.io/scyllabook/queries/prepared.html) for more information
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

        let token = calculate_token(prepared, &serialized_values)?;

        let statement_info = Statement {
            token: Some(token),
            keyspace: prepared.get_keyspace_name(),
        };

        let response = self
            .run_query(
                statement_info,
                &prepared.config,
                |node: Arc<Node>| async move { node.connection_for_token(token).await },
                |connection: Arc<Connection>| async move {
                    connection
                        .execute(prepared, values_ref, paging_state_ref.clone())
                        .await
                },
            )
            .await?;
        self.handle_set_keyspace_response(&response).await?;

        response.into_query_result()
    }

    /// Run a prepared query with paging  
    /// This method will query all pages of the result  
    ///
    /// Returns an async iterator (stream) over all received rows  
    /// Page size can be specified in the [PreparedStatement](crate::prepared_statement::PreparedStatement)
    /// passed to the function
    ///
    /// See [the book](https://cvybhu.github.io/scyllabook/queries/paged.html) for more information
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

        let token = calculate_token(&prepared, &serialized_values)?;

        let retry_session = match &prepared.config.retry_policy {
            Some(policy) => policy.new_session(),
            None => self.retry_policy.new_session(),
        };

        Ok(RowIterator::new_for_prepared_statement(
            prepared,
            serialized_values.into_owned(),
            token,
            retry_session,
            self.load_balancer.clone(),
            self.cluster.get_data(),
            self.metrics.clone(),
        ))
    }

    /// TODO: write more docs
    pub async fn add_prepared_statement(
        &self,
        query: &Query,
    ) -> Result<PreparedStatement, QueryError> {
        if let Some(prepared) = self.prepared_statement_cache.cache.get(&query.contents) {
            // Clone, because else the value is mutably borrowed and the execute method gives a compile error
            Ok(prepared.clone())
        } else {
            let prepared = self.prepare(query.clone()).await?;

            if self.prepared_statement_cache.max_capacity == self.prepared_statement_cache.cache.len() {
                // Cache is full, remove the first entry
                // Don't delete while holding the key, this could deadlock
                // Instead, store the raw string in a variable and remove it later on when there
                // are no more references to the cache
                let mut query = "".to_string();

                // There is no easy way to just get a random/first element
                for first in self.prepared_statement_cache.cache.iter() {
                    query = first.key().clone();

                    // Only the first entry is important
                    break;
                }

                self.prepared_statement_cache.cache.remove(&query);
            }

            self.prepared_statement_cache
                .cache
                .insert(query.contents.clone(), prepared.clone());

            Ok(prepared)
        }
    }

    // TODO: doc
    // TODO: tried it with fn pointers and all, but failed because execute_paged_cached has a different fn parameter list
    // TODO: rename?
    async fn post_execute_prepared_statement<T>(
        &self,
        query: &Query,
        result: Result<T, QueryError>
    ) -> Result<Either<T, PreparedStatement>, QueryError> {
        match result {
            Ok(qr) => Ok(Either::Left(qr)),
            Err(err) => {
                // Check if the Unprepare error is thrown
                // In that case, re-prepare it and send it again
                // In all other cases, just return the error
                match err {
                    QueryError::DbError(db_error, message) => {
                        match db_error {
                            DbError::Unprepared => {
                                self.prepared_statement_cache.cache.remove(&query.contents);

                                let prepared =
                                    self.add_prepared_statement(&query).await?;

                                Ok(Either::Right(prepared))
                            }
                            _ => Err(QueryError::DbError(db_error, message)),
                        }
                    }
                    _ => Err(err),
                }
            }
        }
    }

    /// Does the same thing as [`Session::prepare`](Session::prepare) but uses the prepared statement cache
    /// TODO: more documentation
    /// TODO: maybe write the methods with macro's
    pub async fn execute_cached(
        &self,
        query: impl Into<Query>,
        values: &impl ValueList,
    ) -> Result<QueryResult, QueryError> {
        let query = query.into();
        let prepared = self.add_prepared_statement(&query).await?;
        let values = values.serialized()?;
        let result = self.execute(&prepared, values.clone()).await;

        match self.post_execute_prepared_statement(&query, result).await? {
            Either::Left(result) => Ok(result),
            Either::Right(new_prepared_statement) => {
                self.execute(&new_prepared_statement, values).await
            }
        }
    }

    pub async fn execute_iter_cached(
        &self,
        query: impl Into<Query>,
        values: impl ValueList,
    ) -> Result<RowIterator, QueryError> {
        let query = query.into();
        let prepared = self.add_prepared_statement(&query).await?;
        let values = values.serialized()?;
        let result = self.execute_iter(prepared.clone(), values.clone()).await;

        match self.post_execute_prepared_statement(&query, result).await? {
            Either::Left(result) => Ok(result),
            Either::Right(new_prepared_statement) => {
                self.execute_iter(new_prepared_statement, values).await
            }
        }
    }

    pub async fn execute_paged_cached(
        &self,
        query: impl Into<Query>,
        values: impl ValueList,
        paging_state: Option<Bytes>,
    ) -> Result<QueryResult, QueryError> {
        let query = query.into();
        let prepared = self.add_prepared_statement(&query).await?;
        let values = values.serialized()?;
        let result = self.execute_paged(&prepared, values.clone(), paging_state.clone()).await;

        match self.post_execute_prepared_statement(&query, result).await? {
            Either::Left(result) => Ok(result),
            Either::Right(new_prepared_statement) => {
                self.execute_paged(&new_prepared_statement, values, paging_state).await
            }
        }
    }

    /// Perform a batch query
    /// Batch contains many `simple` or `prepared` queries which are executed at once  
    /// Batch doesn't return any rows
    ///
    /// Batch values must contain values for each of the queries
    ///
    /// See [the book](https://cvybhu.github.io/scyllabook/queries/batch.html) for more information
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
        .await
    }

    /// Sends `USE <keyspace_name>` request on all connections  
    /// This allows to write `SELECT * FROM table` instead of `SELECT * FROM keyspace.table`  
    ///
    /// Note that even failed `use_keyspace` can change currently used keyspace - the request is sent on all connections and
    /// can overwrite previously used keyspace.
    ///
    /// Call only one `use_keyspace` at a time.  
    /// Trying to do two `use_keyspace` requests simultaneously with different names
    /// can end with some connections using one keyspace and the rest using the other.
    ///
    /// See [the book](https://cvybhu.github.io/scyllabook/queries/usekeyspace.html) for more information
    ///
    /// # Arguments
    ///
    /// * `keyspace_name` - keyspace name to use,
    /// keyspace names can have up to 48 alpha-numeric characters and contain underscores
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

    /// Manually trigger a topology refresh  
    /// The driver will fetch current nodes in the cluster and update its topology information
    ///
    /// Normally this is not needed,
    /// the driver should automatically detect all topology changes in the cluster
    pub async fn refresh_topology(&self) -> Result<(), QueryError> {
        self.cluster.refresh_topology().await
    }

    /// Access metrics collected by the driver  
    /// Driver collects various metrics like number of queries or query latencies.
    /// They can be read using this method
    pub fn get_metrics(&self) -> Arc<Metrics> {
        self.metrics.clone()
    }

    /// Get [`TracingInfo`] of a traced query performed earlier
    ///
    /// See [the book](https://cvybhu.github.io/scyllabook/tracing/tracing.html)
    /// for more information about query tracing
    pub async fn get_tracing_info(&self, tracing_id: &Uuid) -> Result<TracingInfo, QueryError> {
        self.get_tracing_info_custom(tracing_id, &GetTracingConfig::default())
            .await
    }

    /// Queries tracing info with custom retry settings.  
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
                .try_getting_tracing_info(tracing_id, config.consistency)
                .await?;

            match current_try {
                Some(tracing_info) => return Ok(tracing_info),
                None => tokio::time::sleep(config.interval).await,
            };
        }

        Err(QueryError::ProtocolError(
            "All tracing queries returned an empty result, \
            maybe information didnt reach this node yet. \
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
        consistency: Consistency,
    ) -> Result<Option<TracingInfo>, QueryError> {
        // Query system_traces.sessions for TracingInfo
        let mut traces_session_query = Query::new(crate::tracing::TRACES_SESSION_QUERY_STR);
        traces_session_query.config.consistency = consistency;
        traces_session_query.set_page_size(1024);

        // Query system_traces.events for TracingEvents
        let mut traces_events_query = Query::new(crate::tracing::TRACES_EVENTS_QUERY_STR);
        traces_events_query.config.consistency = Consistency::One;
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
        consistency: Consistency,
        mut retry_session: Box<dyn RetrySession>,
        choose_connection: impl Fn(Arc<Node>) -> ConnFut,
        do_query: impl Fn(Arc<Connection>) -> QueryFut,
    ) -> Option<Result<ResT, QueryError>>
    where
        ConnFut: Future<Output = Result<Arc<Connection>, QueryError>>,
        QueryFut: Future<Output = Result<ResT, QueryError>>,
    {
        let mut last_error: Option<QueryError> = None;

        'nodes_in_plan: for node in query_plan {
            'same_node_retries: loop {
                let connection: Arc<Connection> = match choose_connection(node.clone()).await {
                    Ok(connection) => connection,
                    Err(e) => {
                        last_error = Some(e);
                        // Broken connection doesn't count as a failed query, don't log in metrics
                        continue 'nodes_in_plan;
                    }
                };

                self.metrics.inc_total_nonpaged_queries();
                let query_start = std::time::Instant::now();

                let query_result: Result<ResT, QueryError> = do_query(connection).await;

                last_error = match query_result {
                    Ok(response) => {
                        let _ = self
                            .metrics
                            .log_query_latency(query_start.elapsed().as_millis() as u64);
                        return Some(Ok(response));
                    }
                    Err(e) => {
                        self.metrics.inc_failed_nonpaged_queries();
                        Some(e)
                    }
                };

                // Use retry policy to decide what to do next
                let query_info = QueryInfo {
                    error: last_error.as_ref().unwrap(),
                    is_idempotent,
                    consistency,
                };

                match retry_session.decide_should_retry(query_info) {
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
        let results = join_all(handles).await;
        let versions: Vec<Uuid> = results
            .iter()
            .cloned()
            .collect::<Result<Vec<_>, QueryError>>()?;
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
}

fn calculate_token(
    stmt: &PreparedStatement,
    values: &SerializedValues,
) -> Result<Token, QueryError> {
    // TODO: take the partitioner of the table that is being queried and calculate the token using
    // that partitioner. The below logic gives correct token only for murmur3partitioner
    let partition_key = match stmt.compute_partition_key(values) {
        Ok(key) => key,
        Err(PartitionKeyError::NoPkIndexValue(_, _)) => {
            return Err(QueryError::ProtocolError(
                "No pk indexes - can't calculate token",
            ))
        }
        Err(PartitionKeyError::ValueTooLong(values_len)) => {
            return Err(QueryError::BadQuery(BadQuery::ValuesTooLongForKey(
                values_len,
                u16::max_value().into(),
            )))
        }
    };

    Ok(murmur3_token(partition_key))
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

#[cfg(test)]
mod tests {
    use crate::SessionBuilder;

    #[tokio::test]
    async fn test_execute_cached() {
        let session = SessionBuilder::new_for_test().await;

        session
            .execute_cached("select * from test_table", &[])
            .await
            .unwrap();

        todo!("More assertions?")
    }

    #[tokio::test]
    async fn test_execute_cached_iter() {
        let _session = SessionBuilder::new_for_test().await;

        unimplemented!();
    }

    #[tokio::test]
    async fn test_execute_cached_paged() {
        let _session = SessionBuilder::new_for_test().await;

        unimplemented!();
    }
}
