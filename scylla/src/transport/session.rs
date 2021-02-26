use futures::future::join_all;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Instant;
use tokio::net::lookup_host;

use super::errors::{BadQuery, NewSessionError, QueryError};
use crate::batch::Batch;
use crate::cql_to_rust::FromRow;
use crate::frame::response::cql_to_rust::FromRowError;
use crate::frame::response::result;
use crate::frame::response::Response;
use crate::frame::value::{BatchValues, SerializedValues, ValueList};
use crate::prepared_statement::{PartitionKeyError, PreparedStatement};
use crate::query::Query;
use crate::routing::{murmur3_token, Token};
use crate::transport::{
    cluster::Cluster,
    connection::{ConnectionConfig, VerifiedKeyspaceName},
    iterator::RowIterator,
    load_balancing::{LoadBalancingPolicy, RoundRobinPolicy, Statement, TokenAwarePolicy},
    metrics::{Metrics, MetricsView},
    node::Node,
    Compression,
};

pub struct Session {
    cluster: Cluster,
    load_balancer: Box<dyn LoadBalancingPolicy>,

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
    pub load_balancing: Box<dyn LoadBalancingPolicy>,

    pub used_keyspace: Option<String>,
    pub keyspace_case_sensitive: bool,
    /*
    These configuration options will be added in the future:

    pub auth_username: Option<String>,
    pub auth_password: Option<String>,

    pub use_tls: bool,
    pub tls_certificate_path: Option<String>,

    pub tcp_keepalive: bool,

    pub retry_policy: Option<String>,

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
            load_balancing: Box::new(TokenAwarePolicy::new(Box::new(RoundRobinPolicy::new()))),
            used_keyspace: None,
            keyspace_case_sensitive: false,
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
    ) -> Result<Option<Vec<result::Row>>, QueryError> {
        let now = Instant::now();
        self.metrics.inc_total_nonpaged_queries();
        let result = self.query_no_metrics(query, values).await;
        match &result {
            Ok(_) => self.log_latency(now.elapsed().as_millis() as u64),
            Err(_) => self.metrics.inc_failed_nonpaged_queries(),
        };
        result
    }

    async fn query_no_metrics(
        &self,
        query: impl Into<Query>,
        values: impl ValueList,
    ) -> Result<Option<Vec<result::Row>>, QueryError> {
        let query = query.into();
        let query_text = query.get_contents();

        // In case the user tried doing session.query("use keyspace ks") run session::use_keyspace
        if query_is_setting_keyspace(query_text) {
            // TODO: replace with log library in https://github.com/scylladb/scylla-rust-driver/issues/158
            eprintln!("Warning: Raw USE KEYSPACE queries are experimental, please use session::use_keyspace instead");

            let keyspace_name = &query_text["use ".len()..].trim_end_matches(';').trim();
            let case_sensitive = keyspace_name.starts_with('"');
            let keyspace_name = keyspace_name.trim_matches('"');

            return self
                .use_keyspace(keyspace_name, case_sensitive)
                .await
                .map(|_| None);
        }

        let statement_info = Statement {
            token: None,
            keyspace: None,
        };
        let node = self.load_balancing_plan(statement_info);

        node.random_connection()
            .await?
            .query_single_page(query, values)
            .await
    }

    pub async fn query_iter(
        &self,
        query: impl Into<Query>,
        values: impl ValueList,
    ) -> Result<RowIterator, QueryError> {
        let serialized_values = values.serialized()?;

        let statement_info = Statement {
            token: None,
            keyspace: None,
        };
        let node = self.load_balancing_plan(statement_info);

        Ok(RowIterator::new_for_query(
            node.random_connection().await?,
            query.into(),
            serialized_values.into_owned(),
            self.metrics.clone(),
        ))
    }

    /// Prepares a statement on the server side and returns a prepared statement,
    /// which can later be used to perform more efficient queries
    /// # Arguments
    ///#
    pub async fn prepare(&self, query: &str) -> Result<PreparedStatement, QueryError> {
        let connections = self.cluster.get_working_connections().await?;

        // Prepare statements on all connections concurrently
        let handles = connections.iter().map(|c| c.prepare(query));
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

        let prepared: PreparedStatement = first_ok.unwrap()?;

        // Validate prepared ids equality
        for res in results {
            if let Ok(statement) = res {
                if prepared.get_id() != statement.get_id() {
                    return Err(QueryError::ProtocolError(
                        "Prepared statement Ids differ, all should be equal",
                    ));
                }
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
    ) -> Result<Option<Vec<result::Row>>, QueryError> {
        let now = Instant::now();
        self.metrics.inc_total_nonpaged_queries();
        let result = self.execute_no_metrics(prepared, values).await;
        match &result {
            Ok(_) => self.log_latency(now.elapsed().as_millis() as u64),
            Err(_) => self.metrics.inc_failed_nonpaged_queries(),
        };
        result
    }

    async fn execute_no_metrics(
        &self,
        prepared: &PreparedStatement,
        values: impl ValueList,
    ) -> Result<Option<Vec<result::Row>>, QueryError> {
        // FIXME: Prepared statement ids are local to a node, so we must make sure
        // that prepare() sends to all nodes and keeps all ids.
        let serialized_values = values.serialized()?;

        let token = calculate_token(prepared, &serialized_values)?;

        let statement_info = Statement {
            token: Some(token),
            keyspace: None,
        };
        let node = self.load_balancing_plan(statement_info);

        let connection = node.connection_for_token(token).await?;
        let result = connection
            .execute(prepared, &serialized_values, None)
            .await?;
        match result {
            Response::Error(err) => {
                match err.code {
                    9472 => {
                        // Repreparation of a statement is needed
                        let reprepared = connection.prepare(prepared.get_statement()).await?;
                        // Reprepared statement should keep its id - it's the md5 sum
                        // of statement contents
                        if reprepared.get_id() != prepared.get_id() {
                            return Err(QueryError::ProtocolError(
                                "Prepared statement Id changed, md5 sum should stay the same",
                            ));
                        }

                        let result = connection
                            .execute(prepared, &serialized_values, None)
                            .await?;
                        match result {
                            Response::Error(err) => Err(err.into()),
                            Response::Result(result::Result::Rows(rs)) => Ok(Some(rs.rows)),
                            Response::Result(_) => Ok(None),
                            _ => Err(QueryError::ProtocolError(
                                "EXECUTE: Unexpected server response",
                            )),
                        }
                    }
                    _ => Err(err.into()),
                }
            }
            Response::Result(result::Result::Rows(rs)) => Ok(Some(rs.rows)),
            Response::Result(_) => Ok(None),
            _ => Err(QueryError::ProtocolError(
                "EXECUTE: Unexpected server response",
            )),
        }
    }

    pub async fn execute_iter(
        &self,
        prepared: impl Into<PreparedStatement>,
        values: impl ValueList,
    ) -> Result<RowIterator, QueryError> {
        let serialized_values = values.serialized()?;

        let statement_info = Statement {
            token: None,
            keyspace: None,
        };
        let node = self.load_balancing_plan(statement_info);

        Ok(RowIterator::new_for_prepared_statement(
            node.random_connection().await?,
            prepared.into(),
            serialized_values.into_owned(),
            self.metrics.clone(),
        ))
    }

    /// Sends a batch to the database.
    /// # Arguments
    ///
    /// * `batch` - batch to be performed
    /// * `values` - values bound to the query
    pub async fn batch(&self, batch: &Batch, values: impl BatchValues) -> Result<(), QueryError> {
        // FIXME: Prepared statement ids are local to a node
        // this method does not handle this
        let statement_info = Statement {
            token: None,
            keyspace: None,
        };
        let node = self.load_balancing_plan(statement_info);

        let response = node
            .random_connection()
            .await?
            .batch(&batch, values)
            .await?;
        match response {
            Response::Error(err) => Err(err.into()),
            Response::Result(_) => Ok(()),
            _ => Err(QueryError::ProtocolError(
                "BATCH: Unexpected server response",
            )),
        }
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

    fn log_latency(&self, latency: u64) {
        let _ = self // silent fail if mutex is poisoned
            .metrics
            .log_query_latency(latency);
    }

    fn load_balancing_plan(&self, statement_info: Statement) -> Arc<Node> {
        let cluster_info = self.cluster.get_data();
        let mut plan = self.load_balancer.plan(&statement_info, &cluster_info);

        plan.next().unwrap() // Plan returned by load balancing policies should never be empty
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
