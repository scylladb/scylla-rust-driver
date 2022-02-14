//! Iterators over rows returned by paged queries

use std::future::Future;
use std::mem;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use bytes::Bytes;
use futures::Stream;
use std::result::Result;
use thiserror::Error;
use tokio::sync::mpsc;

use super::errors::QueryError;
use crate::cql_to_rust::{FromRow, FromRowError};

use crate::frame::types::LegacyConsistency;
use crate::frame::{
    response::{
        result,
        result::{ColumnSpec, Row, Rows},
        Response,
    },
    value::SerializedValues,
};
use crate::routing::Token;
use crate::statement::Consistency;
use crate::statement::{prepared_statement::PreparedStatement, query::Query};
use crate::transport::cluster::ClusterData;
use crate::transport::connection::{Connection, QueryResponse};
use crate::transport::load_balancing::{LoadBalancingPolicy, Statement};
use crate::transport::metrics::Metrics;
use crate::transport::node::Node;
use crate::transport::retry_policy::{QueryInfo, RetryDecision, RetrySession};
use uuid::Uuid;

/// Iterator over rows returned by paged queries\
/// Allows to easily access rows without worrying about handling multiple pages
pub struct RowIterator {
    current_row_idx: usize,
    current_page: Rows,
    page_receiver: mpsc::Receiver<Result<ReceivedPage, QueryError>>,
    tracing_ids: Vec<Uuid>,
}

struct ReceivedPage {
    pub rows: Rows,
    pub tracing_id: Option<Uuid>,
}

pub(crate) struct PreparedIteratorConfig {
    pub prepared: PreparedStatement,
    pub values: SerializedValues,
    pub default_consistency: Consistency,
    pub token: Token,
    pub retry_session: Box<dyn RetrySession>,
    pub load_balancer: Arc<dyn LoadBalancingPolicy>,
    pub cluster_data: Arc<ClusterData>,
    pub metrics: Arc<Metrics>,
}

/// Fetching pages is asynchronous so `RowIterator` does not implement the `Iterator` trait.\
/// Instead it uses the asynchronous `Stream` trait
impl Stream for RowIterator {
    type Item = Result<Row, QueryError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut s = self.as_mut();

        if s.is_current_page_exhausted() {
            match Pin::new(&mut s.page_receiver).poll_recv(cx) {
                Poll::Ready(Some(Ok(received_page))) => {
                    s.current_page = received_page.rows;
                    s.current_row_idx = 0;

                    if let Some(tracing_id) = received_page.tracing_id {
                        s.tracing_ids.push(tracing_id);
                    }
                }
                Poll::Ready(Some(Err(err))) => return Poll::Ready(Some(Err(err))),
                Poll::Ready(None) => return Poll::Ready(None),
                Poll::Pending => return Poll::Pending,
            }
        }

        let idx = s.current_row_idx;
        if idx < s.current_page.rows.len() {
            let row = mem::take(&mut s.current_page.rows[idx]);
            s.current_row_idx += 1;
            return Poll::Ready(Some(Ok(row)));
        }

        // We probably got a zero-sized page
        // Yield, but tell that we are ready
        cx.waker().wake_by_ref();
        Poll::Pending
    }
}

impl RowIterator {
    /// Converts this iterator into an iterator over rows parsed as given type
    pub fn into_typed<RowT: FromRow>(self) -> TypedRowIterator<RowT> {
        TypedRowIterator {
            row_iterator: self,
            phantom_data: Default::default(),
        }
    }

    pub(crate) async fn new_for_query(
        query: Query,
        values: SerializedValues,
        default_consistency: Consistency,
        retry_session: Box<dyn RetrySession>,
        load_balancer: Arc<dyn LoadBalancingPolicy>,
        cluster_data: Arc<ClusterData>,
        metrics: Arc<Metrics>,
    ) -> Result<RowIterator, QueryError> {
        let (sender, mut receiver) = mpsc::channel(1);
        let consistency = query.config.determine_consistency(default_consistency);

        let worker_task = async move {
            let query_ref = &query;
            let values_ref = &values;

            let choose_connection = |node: Arc<Node>| async move { node.random_connection().await };

            let page_query = |connection: Arc<Connection>, paging_state: Option<Bytes>| async move {
                connection.query(query_ref, values_ref, paging_state).await
            };

            let worker = RowIteratorWorker {
                sender,
                choose_connection,
                page_query,
                statement_info: Statement::default(),
                query_is_idempotent: query.config.is_idempotent,
                query_consistency: consistency,
                retry_session,
                load_balancer,
                metrics,
                paging_state: None,
            };

            worker.work(cluster_data).await;
        };

        tokio::task::spawn(worker_task);

        let pages_received = receiver.recv().await.unwrap()?;

        Ok(RowIterator {
            current_row_idx: 0,
            current_page: pages_received.rows,
            page_receiver: receiver,
            tracing_ids: if let Some(tracing_id) = pages_received.tracing_id {
                vec![tracing_id]
            } else {
                Vec::new()
            },
        })
    }

    pub(crate) async fn new_for_prepared_statement(
        config: PreparedIteratorConfig,
    ) -> Result<RowIterator, QueryError> {
        let (sender, mut receiver) = mpsc::channel(1);
        let consistency = config
            .prepared
            .config
            .determine_consistency(config.default_consistency);

        let statement_info = Statement {
            token: Some(config.token),
            keyspace: None,
        };

        let worker_task = async move {
            let prepared_ref = &config.prepared;
            let values_ref = &config.values;
            let token = config.token;

            let choose_connection =
                |node: Arc<Node>| async move { node.connection_for_token(token).await };

            let page_query = |connection: Arc<Connection>, paging_state: Option<Bytes>| async move {
                connection
                    .execute(prepared_ref, values_ref, paging_state)
                    .await
            };

            let worker = RowIteratorWorker {
                sender,
                choose_connection,
                page_query,
                statement_info,
                query_is_idempotent: config.prepared.config.is_idempotent,
                query_consistency: consistency,
                retry_session: config.retry_session,
                load_balancer: config.load_balancer,
                metrics: config.metrics,
                paging_state: None,
            };

            worker.work(config.cluster_data).await;
        };

        tokio::task::spawn(worker_task);

        let pages_received = receiver.recv().await.unwrap()?;

        Ok(RowIterator {
            current_row_idx: 0,
            current_page: pages_received.rows,
            page_receiver: receiver,
            tracing_ids: if let Some(tracing_id) = pages_received.tracing_id {
                vec![tracing_id]
            } else {
                Vec::new()
            },
        })
    }

    /// If tracing was enabled returns tracing ids of all finished page queries
    pub fn get_tracing_ids(&self) -> &[Uuid] {
        &self.tracing_ids
    }

    /// Returns specification of row columns
    pub fn get_column_specs(&self) -> &[ColumnSpec] {
        &self.current_page.metadata.col_specs
    }

    fn is_current_page_exhausted(&self) -> bool {
        self.current_row_idx >= self.current_page.rows.len()
    }
}

// RowIteratorWorker works in the background to fetch pages
// RowIterator receives them through a channel
struct RowIteratorWorker<'a, ConnFunc, QueryFunc> {
    sender: mpsc::Sender<Result<ReceivedPage, QueryError>>,

    // Closure used to choose a connection from a node
    // AsyncFn(Arc<Node>) -> Result<Arc<Connection>, QueryError>
    choose_connection: ConnFunc,

    // Closure used to perform a single page query
    // AsyncFn(Arc<Connection>, Option<Bytes>) -> Result<QueryResponse, QueryError>
    page_query: QueryFunc,

    statement_info: Statement<'a>,
    query_is_idempotent: bool,
    query_consistency: Consistency,

    retry_session: Box<dyn RetrySession>,
    load_balancer: Arc<dyn LoadBalancingPolicy>,
    metrics: Arc<Metrics>,

    paging_state: Option<Bytes>,
}

impl<ConnFunc, ConnFut, QueryFunc, QueryFut> RowIteratorWorker<'_, ConnFunc, QueryFunc>
where
    ConnFunc: Fn(Arc<Node>) -> ConnFut,
    ConnFut: Future<Output = Result<Arc<Connection>, QueryError>>,
    QueryFunc: Fn(Arc<Connection>, Option<Bytes>) -> QueryFut,
    QueryFut: Future<Output = Result<QueryResponse, QueryError>>,
{
    async fn work(mut self, cluster_data: Arc<ClusterData>) {
        let query_plan = self.load_balancer.plan(&self.statement_info, &cluster_data);

        let mut last_error: QueryError =
            QueryError::ProtocolError("Empty query plan - driver bug!");

        'nodes_in_plan: for node in query_plan {
            // For each node in the plan choose a connection to use
            // This connection will be reused for same node retries to preserve paging cache on the shard
            let connection: Arc<Connection> = match (self.choose_connection)(node).await {
                Ok(connection) => connection,
                Err(e) => {
                    last_error = e;
                    // Broken connection doesn't count as a failed query, don't log in metrics
                    continue 'nodes_in_plan;
                }
            };

            'same_node_retries: loop {
                // Query pages until an error occurs
                let queries_result: Result<(), QueryError> = self.query_pages(&connection).await;

                last_error = match queries_result {
                    Ok(()) => return,
                    Err(error) => error,
                };

                // Use retry policy to decide what to do next
                let query_info = QueryInfo {
                    error: &last_error,
                    is_idempotent: self.query_is_idempotent,
                    consistency: LegacyConsistency::Regular(self.query_consistency),
                };

                match self.retry_session.decide_should_retry(query_info) {
                    RetryDecision::RetrySameNode => {
                        self.metrics.inc_retries_num();
                        continue 'same_node_retries;
                    }
                    RetryDecision::RetryNextNode => {
                        self.metrics.inc_retries_num();
                        continue 'nodes_in_plan;
                    }
                    RetryDecision::DontRetry => break 'nodes_in_plan,
                };
            }
        }

        // Send last_error to RowIterator - query failed fully
        let _ = self.sender.send(Err(last_error)).await;
    }

    // Given a working connection query as many pages as possible until the first error
    async fn query_pages(&mut self, connection: &Arc<Connection>) -> Result<(), QueryError> {
        loop {
            self.metrics.inc_total_paged_queries();
            let query_start = std::time::Instant::now();

            let query_response: QueryResponse =
                (self.page_query)(connection.clone(), self.paging_state.clone()).await?;

            match query_response.response {
                Response::Result(result::Result::Rows(mut rows)) => {
                    let _ = self
                        .metrics
                        .log_query_latency(query_start.elapsed().as_millis() as u64);

                    self.paging_state = rows.metadata.paging_state.take();

                    let received_page = ReceivedPage {
                        rows,
                        tracing_id: query_response.tracing_id,
                    };

                    // Send next page to RowIterator
                    if self.sender.send(Ok(received_page)).await.is_err() {
                        // channel was closed, RowIterator was dropped - should shutdown
                        return Ok(());
                    }

                    if self.paging_state.is_none() {
                        // Reached the last query, shutdown
                        return Ok(());
                    }

                    // Query succeeded, reset retry policy for future retries
                    self.retry_session.reset();
                }
                Response::Error(err) => {
                    self.metrics.inc_failed_paged_queries();
                    return Err(err.into());
                }
                _ => {
                    self.metrics.inc_failed_paged_queries();

                    return Err(QueryError::ProtocolError(
                        "Unexpected response to next page query",
                    ));
                }
            }
        }
    }
}

/// Iterator over rows returned by paged queries
/// where each row is parsed as the given type\
/// Returned by `RowIterator::into_typed`
pub struct TypedRowIterator<RowT> {
    row_iterator: RowIterator,
    phantom_data: std::marker::PhantomData<RowT>,
}

impl<RowT> TypedRowIterator<RowT> {
    /// If tracing was enabled returns tracing ids of all finished page queries
    pub fn get_tracing_ids(&self) -> &[Uuid] {
        self.row_iterator.get_tracing_ids()
    }

    /// Returns specification of row columns
    pub fn get_column_specs(&self) -> &[ColumnSpec] {
        self.row_iterator.get_column_specs()
    }
}

/// Couldn't get next typed row from the iterator
#[derive(Error, Debug, Clone)]
pub enum NextRowError {
    /// Query to fetch next page has failed
    #[error(transparent)]
    QueryError(#[from] QueryError),

    /// Parsing values in row as given types failed
    #[error(transparent)]
    FromRowError(#[from] FromRowError),
}

/// Fetching pages is asynchronous so `TypedRowIterator` does not implement the `Iterator` trait.\
/// Instead it uses the asynchronous `Stream` trait
impl<RowT: FromRow> Stream for TypedRowIterator<RowT> {
    type Item = Result<RowT, NextRowError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut s = self.as_mut();

        let next_elem: Option<Result<Row, QueryError>> =
            match Pin::new(&mut s.row_iterator).poll_next(cx) {
                Poll::Ready(next_elem) => next_elem,
                Poll::Pending => return Poll::Pending,
            };

        let next_ready: Option<Self::Item> = match next_elem {
            Some(Ok(next_row)) => Some(RowT::from_row(next_row).map_err(|e| e.into())),
            Some(Err(e)) => Some(Err(e.into())),
            None => None,
        };

        Poll::Ready(next_ready)
    }
}

// TypedRowIterator can be moved freely for any RowT so it's Unpin
impl<RowT> Unpin for TypedRowIterator<RowT> {}
