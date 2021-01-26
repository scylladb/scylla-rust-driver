use core::ops::Bound::{Included, Unbounded};
use futures::future::join_all;
use rand::Rng;
use std::fmt::Display;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Instant;
use tokio::net::{lookup_host, ToSocketAddrs};

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
use crate::transport::connect_config::{ConnectConfig, KnownNode};
use crate::transport::connection::Connection;
use crate::transport::iterator::RowIterator;
use crate::transport::metrics::{Metrics, MetricsView};
use crate::transport::node::Node;
use crate::transport::Compression;

pub struct Session {
    cluster: Cluster,

    metrics: Arc<Metrics>,
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
    /// * `addr` - address of the server
    /// * `compression` - optional compression settings
    async fn connect(
        addr: impl ToSocketAddrs + Display,
        compression: Option<Compression>,
    ) -> Result<Self, NewSessionError> {
        let addr = resolve(addr).await?;

        let cluster = Cluster::new(&[addr], compression).await?;
        let metrics = Arc::new(Metrics::new());

        Ok(Session { cluster, metrics })
    }

    pub async fn connect_with_config(config: ConnectConfig) -> Result<Self, NewSessionError> {
        let first_known: &KnownNode = config
            .known_nodes
            .first()
            .ok_or(NewSessionError::EmptyKnownNodesList)?;

        match first_known {
            KnownNode::Hostname(hostname) => Self::connect(hostname, config.compression).await,
            KnownNode::Address(address) => Self::connect(address, config.compression).await,
        }
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

// Resolve the given `ToSocketAddrs` using a DNS lookup if necessary.
// The resolution may return multiple IPs and the function returns one of them.
// It prefers to return IPv4s first, and only if there are none, IPv6s.
async fn resolve(addr: impl ToSocketAddrs + Display) -> Result<SocketAddr, NewSessionError> {
    let failed_err = NewSessionError::FailedToResolveAddress(addr.to_string());
    let mut ret = None;
    for a in lookup_host(addr).await? {
        match a {
            SocketAddr::V4(_) => return Ok(a),
            _ => {
                ret = Some(a);
            }
        }
    }

    ret.ok_or(failed_err)
}
