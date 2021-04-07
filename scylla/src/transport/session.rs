use futures::future::join_all;
use std::future::Future;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::lookup_host;
use tracing::warn;
use uuid::Uuid;

use super::errors::{BadQuery, NewSessionError, QueryError};
use crate::batch::Batch;
use crate::cql_to_rust::FromRow;
use crate::frame::response::cql_to_rust::FromRowError;
use crate::frame::response::result;
use crate::frame::value::{BatchValues, SerializedValues, ValueList};
use crate::prepared_statement::{PartitionKeyError, PreparedStatement};
use crate::query::Query;
use crate::routing::{murmur3_token, Token};
use crate::statement::Consistency;
use crate::tracing::{GetTracingConfig, TracingEvent, TracingInfo};
use crate::transport::{
    cluster::Cluster,
    connection::{BatchResult, Connection, ConnectionConfig, QueryResult, VerifiedKeyspaceName},
    iterator::RowIterator,
    load_balancing::{LoadBalancingPolicy, RoundRobinPolicy, Statement, TokenAwarePolicy},
    metrics::{Metrics, MetricsView},
    node::Node,
    retry_policy::{DefaultRetryPolicy, QueryInfo, RetryDecision, RetryPolicy, RetrySession},
    Compression,
};

#[cfg(feature = "ssl")]
use openssl::ssl::SslContext;

pub struct Session {
    cluster: Cluster,
    load_balancer: Arc<dyn LoadBalancingPolicy>,
    retry_policy: Box<dyn RetryPolicy + Send + Sync>,

    metrics: Arc<Metrics>,
}

/// Configuration options for [`Session`].
/// Can be created manually, but usually it's easier to use
/// [SessionBuilder](super::session_builder::SessionBuilder)
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

    pub retry_policy: Box<dyn RetryPolicy + Send + Sync>,

    /// Provide our Session with TLS
    #[cfg(feature = "ssl")]
    pub ssl_context: Option<SslContext>,

    pub auth_username: Option<String>,
    pub auth_password: Option<String>,
    /*
    These configuration options will be added in the future:


    pub tcp_keepalive: bool,

    pub default_consistency: Option<String>,
    */
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
            tcp_nodelay: false,
            load_balancing: Arc::new(TokenAwarePolicy::new(Box::new(RoundRobinPolicy::new()))),
            used_keyspace: None,
            keyspace_case_sensitive: false,
            retry_policy: Box::new(DefaultRetryPolicy),
            #[cfg(feature = "ssl")]
            ssl_context: None,
            auth_username: None,
            auth_password: None,
        }
    }

    /// Adds a known database server with a hostname
    /// # Example
    /// ```
    /// # use scylla::SessionConfig;
    /// let mut config = SessionConfig::new();
    /// config.add_known_node("127.0.0.1:9042");
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

    /// Adds a list of known database server with hostnames
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

    /// Makes a config that should be used in Connection
    fn get_connection_config(&self) -> ConnectionConfig {
        ConnectionConfig {
            compression: self.compression,
            tcp_nodelay: self.tcp_nodelay,
            #[cfg(feature = "ssl")]
            ssl_context: self.ssl_context.clone(),
            auth_username: self.auth_username.to_owned(),
            auth_password: self.auth_password.to_owned(),
        }
    }
}

/// Creates default [`SessionConfig`], same as [`SessionConfig::new`]
impl Default for SessionConfig {
    fn default() -> Self {
        Self::new()
    }
}

// Trait used to implement Vec<result::Row>::into_typed<RowT>(self)
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

// Iterator that maps a Vec<result::Row> into custom RowType used by IntoTypedRows::into_typed
// impl Trait doesn't compile so we have to be explicit
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
    /// # Arguments
    ///
    /// * `config` - Connectiong configuration - known nodes, Compression, etc.
    pub async fn connect(config: SessionConfig) -> Result<Self, NewSessionError> {
        // Ensure there is at least one known node
        if config.known_nodes.is_empty() {
            return Err(NewSessionError::EmptyKnownNodesList);
        }

        // Find IP addresses of all known nodes passed in the config
        let mut node_addresses: Vec<SocketAddr> = Vec::with_capacity(config.known_nodes.len());

        let mut to_resolve: Vec<&str> = Vec::new();

        for node in &config.known_nodes {
            match node {
                KnownNode::Hostname(hostname) => to_resolve.push(&hostname),
                KnownNode::Address(address) => node_addresses.push(*address),
            };
        }

        let resolve_futures = to_resolve.into_iter().map(resolve_hostname);
        let resolved: Vec<SocketAddr> = futures::future::try_join_all(resolve_futures).await?;

        node_addresses.extend(resolved);

        // Start the session
        let cluster = Cluster::new(&node_addresses, config.get_connection_config()).await?;
        let metrics = Arc::new(Metrics::new());

        let session = Session {
            cluster,
            load_balancer: config.load_balancing,
            retry_policy: config.retry_policy,
            metrics,
        };

        if let Some(keyspace_name) = config.used_keyspace {
            session
                .use_keyspace(keyspace_name, config.keyspace_case_sensitive)
                .await?;
        }

        Ok(session)
    }

    // TODO: Should return an iterator over results
    // actually, if we consider "INSERT" a query, then no.
    // But maybe "INSERT" and "SELECT" should go through different methods,
    // so we expect "SELECT" to always return Vec<result::Row>?
    /// Sends a query to the database and receives a response.
    /// If `query` has paging enabled, this will return only the first page.
    /// # Arguments
    ///
    /// * `query` - query to be performed
    /// * `values` - values bound to the query
    pub async fn query(
        &self,
        query: impl Into<Query>,
        values: impl ValueList,
    ) -> Result<QueryResult, QueryError> {
        let query: Query = query.into();
        let query_text: &str = query.get_contents();
        let serialized_values = values.serialized();

        // In case the user tried doing session.query("use keyspace ks") run session::use_keyspace
        if query_is_setting_keyspace(query_text) {
            warn!("Raw USE KEYSPACE queries are experimental, please use session::use_keyspace instead");

            let keyspace_name = &query_text["use ".len()..].trim_end_matches(';').trim();
            let case_sensitive = keyspace_name.starts_with('"');
            let keyspace_name = keyspace_name.trim_matches('"');

            return self
                .use_keyspace(keyspace_name, case_sensitive)
                .await
                .map(|_| QueryResult::default());
        }

        let retry_session = match &query.retry_policy {
            Some(policy) => policy.new_session(),
            None => self.retry_policy.new_session(),
        };

        // Needed to avoid moving query and values into async move block
        let query_ref: &Query = &query;
        let values_ref = &serialized_values;

        self.run_query(
            Statement::default(),
            query.is_idempotent,
            query.consistency,
            retry_session,
            |node: Arc<Node>| async move { node.random_connection().await },
            |connection: Arc<Connection>| async move {
                connection
                    .query_single_page_by_ref(query_ref, values_ref)
                    .await
            },
        )
        .await
    }

    pub async fn query_iter(
        &self,
        query: impl Into<Query>,
        values: impl ValueList,
    ) -> Result<RowIterator, QueryError> {
        let query: Query = query.into();
        let serialized_values = values.serialized()?;

        let retry_session = match &query.retry_policy {
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
    /// # Arguments
    ///#
    pub async fn prepare(&self, query: impl Into<Query>) -> Result<PreparedStatement, QueryError> {
        let query: Query = query.into();

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
        for res in results {
            if let Ok(statement) = res {
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
        }

        Ok(prepared)
    }

    /// Executes a previously prepared statement
    /// # Arguments
    ///
    /// * `prepared` - a statement prepared with [prepare](crate::transport::session::Session::prepare)
    /// * `values` - values bound to the query
    pub async fn execute(
        &self,
        prepared: &PreparedStatement,
        values: impl ValueList,
    ) -> Result<QueryResult, QueryError> {
        let serialized_values = values.serialized()?;
        let values_ref = &serialized_values;

        let token = calculate_token(prepared, &serialized_values)?;

        let statement_info = Statement {
            token: Some(token),
            keyspace: prepared.get_keyspace_name(),
        };

        let retry_session = match &prepared.retry_policy {
            Some(policy) => policy.new_session(),
            None => self.retry_policy.new_session(),
        };

        self.run_query(
            statement_info,
            prepared.is_idempotent,
            prepared.consistency,
            retry_session,
            |node: Arc<Node>| async move { node.connection_for_token(token).await },
            |connection: Arc<Connection>| async move {
                connection.execute_single_page(prepared, values_ref).await
            },
        )
        .await
    }

    pub async fn execute_iter(
        &self,
        prepared: impl Into<PreparedStatement>,
        values: impl ValueList,
    ) -> Result<RowIterator, QueryError> {
        let prepared: PreparedStatement = prepared.into();
        let serialized_values = values.serialized()?;

        let token = calculate_token(&prepared, &serialized_values)?;

        let retry_session = match &prepared.retry_policy {
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

    /// Sends a batch to the database.
    /// # Arguments
    ///
    /// * `batch` - batch to be performed
    /// * `values` - values bound to the query
    pub async fn batch(
        &self,
        batch: &Batch,
        values: impl BatchValues,
    ) -> Result<BatchResult, QueryError> {
        let values_ref = &values;

        let retry_session = match &batch.retry_policy {
            Some(policy) => policy.new_session(),
            None => self.retry_policy.new_session(),
        };

        self.run_query(
            Statement::default(),
            batch.is_idempotent,
            batch.consistency,
            retry_session,
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

    pub async fn refresh_topology(&self) -> Result<(), QueryError> {
        self.cluster.refresh_topology().await
    }

    pub fn get_metrics(&self) -> MetricsView {
        MetricsView::new(self.metrics.clone())
    }

    /// Get [`TracingInfo`] of a traced query performed earlier
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

        return Err(QueryError::ProtocolError(
            "All tracing queries returned an empty result, \
            maybe information didnt reach this node yet. \
            Consider using get_tracing_info_custom with \
            bigger interval in GetTracingConfig",
        ));
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
        let mut traces_session_query =
            Query::new(crate::tracing::TRACES_SESSION_QUERY_STR.to_string());
        traces_session_query.consistency = consistency;

        // Query system_traces.events for TracingEvents
        let mut traces_events_query =
            Query::new(crate::tracing::TRACES_EVENTS_QUERY_STR.to_string());
        traces_events_query.consistency = Consistency::One;
        traces_events_query.consistency = consistency;

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
        query_is_idempotent: bool,
        query_consistency: Consistency,
        mut retry_session: Box<dyn RetrySession + Send + Sync>,
        choose_connection: impl Fn(Arc<Node>) -> ConnFut,
        do_query: impl Fn(Arc<Connection>) -> QueryFut,
    ) -> Result<ResT, QueryError>
    where
        ConnFut: Future<Output = Result<Arc<Connection>, QueryError>>,
        QueryFut: Future<Output = Result<ResT, QueryError>>,
    {
        let cluster_data = self.cluster.get_data();
        let query_plan = self.load_balancer.plan(&statement_info, &cluster_data);

        let mut last_error: QueryError =
            QueryError::ProtocolError("Empty query plan - driver bug!");

        'nodes_in_plan: for node in query_plan {
            'same_node_retries: loop {
                let connection: Arc<Connection> = match choose_connection(node.clone()).await {
                    Ok(connection) => connection,
                    Err(e) => {
                        last_error = e;
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
                        return Ok(response);
                    }
                    Err(e) => {
                        self.metrics.inc_failed_nonpaged_queries();
                        e
                    }
                };

                // Use retry policy to decide what to do next
                let query_info = QueryInfo {
                    error: &last_error,
                    is_idempotent: query_is_idempotent,
                    consistency: query_consistency,
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
                    RetryDecision::DontRetry => return Err(last_error),
                };
            }
        }

        return Err(last_error);
    }
}

/// Checks if a query sets a keyspace
fn query_is_setting_keyspace(query: &str) -> bool {
    let query_bytes = query.as_bytes();

    if query_bytes.len() < 4 {
        return false;
    }

    query_bytes[0..=3].eq_ignore_ascii_case("use ".as_bytes())
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
    for a in lookup_host(hostname).await? {
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
    use super::*;

    #[test]
    fn test_query_is_setting_keyspace() {
        assert!(query_is_setting_keyspace("use some_keyspace"));
        assert!(query_is_setting_keyspace("UsE anotherKeySpace;"));
        assert!(query_is_setting_keyspace("USE SCREAMINGKEYSPACE"));
        assert!(!query_is_setting_keyspace("select * from users;"));
        assert!(!query_is_setting_keyspace("us"));
        assert!(!query_is_setting_keyspace(""));
    }
}
