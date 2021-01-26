use core::ops::Bound::{Included, Unbounded};
use futures::future::join_all;
use rand::Rng;
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
use crate::transport::cluster::{Cluster, ClusterData};
use crate::transport::connection::{Connection, ConnectionConfig};
use crate::transport::iterator::RowIterator;
use crate::transport::metrics::{Metrics, MetricsView};
use crate::transport::node::Node;
use crate::transport::Compression;

pub struct Session {
    cluster: Cluster,

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
    /*
    These configuration options will be added in the future:

    pub auth_username: Option<String>,
    pub auth_password: Option<String>,

    pub use_tls: bool,
    pub tls_certificate_path: Option<String>,

    pub tcp_nodelay: bool,
    pub tcp_keepalive: bool,

    pub load_balancing: Option<String>,
    pub retry_policy: Option<String>,

    pub default_consistency: Option<String>,
    */
}

/// Describes database server known on Session startup.
pub enum KnownNode {
    Hostname(String),
    Address(SocketAddr),
}

impl SessionConfig {
    /// Creates a [`SessionConfig`] with default configuration
    /// # Default configuration
    /// * Compression: None
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

        Ok(Session { cluster, metrics })
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
        self.any_connection()
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

        Ok(RowIterator::new_for_query(
            self.any_connection().await?,
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
    /// * `prepared` - a statement prepared with [prepare](crate::transport::session::prepare)
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
        let connection = self.pick_connection(token).await?;
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

        Ok(RowIterator::new_for_prepared_statement(
            self.any_connection().await?,
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
        let response = self.any_connection().await?.batch(&batch, values).await?;
        match response {
            Response::Error(err) => Err(err.into()),
            Response::Result(_) => Ok(()),
            _ => Err(QueryError::ProtocolError(
                "BATCH: Unexpected server response",
            )),
        }
    }

    pub async fn refresh_topology(&self) -> Result<(), QueryError> {
        self.cluster.refresh_topology().await
    }

    async fn pick_connection(&self, t: Token) -> Result<Arc<Connection>, QueryError> {
        // TODO: we try only the owner of the range (vnode) that the token lies in
        // we should calculate the *set* of replicas for this token, using the replication strategy
        // of the table being queried.
        let cluster_data: Arc<ClusterData> = self.cluster.get_data();

        let first_node: Arc<Node> = cluster_data.ring.values().next().unwrap().clone();

        let owner: Arc<Node> = cluster_data
            .ring
            .range((Included(t), Unbounded))
            .next()
            .map(|(_token, node)| node.clone())
            .unwrap_or(first_node);

        owner.connection_for_token(t).await
    }

    async fn any_connection(&self) -> Result<Arc<Connection>, QueryError> {
        let random_token: Token = Token {
            value: rand::thread_rng().gen(),
        };

        self.pick_connection(random_token).await
    }

    pub fn get_metrics(&self) -> MetricsView {
        MetricsView::new(self.metrics.clone())
    }

    fn log_latency(&self, latency: u64) {
        let _ = self // silent fail if mutex is poisoned
            .metrics
            .log_query_latency(latency);
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
