//! Iterators over rows returned by paged queries

use std::future::Future;
use std::mem;
use std::net::SocketAddr;
use std::ops::ControlFlow;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use bytes::Bytes;
use futures::Stream;
use scylla_cql::frame::response::result::RawRows;
use scylla_cql::frame::response::NonErrorResponse;
use scylla_cql::frame::types::SerialConsistency;
use std::result::Result;
use thiserror::Error;
use tokio::sync::mpsc;
use tracing::instrument::WithSubscriber;

use super::errors::QueryError;
use super::execution_profile::ExecutionProfileInner;
use super::session::RequestSpan;
use crate::cql_to_rust::{FromRow, FromRowError};

use crate::frame::types::LegacyConsistency;
use crate::frame::{
    response::{
        result,
        result::{ColumnSpec, Row, Rows},
    },
    value::SerializedValues,
};
use crate::history::{self, HistoryListener};
use crate::routing::Token;
use crate::statement::Consistency;
use crate::statement::{prepared_statement::PreparedStatement, query::Query};
use crate::transport::cluster::ClusterData;
use crate::transport::connection::{Connection, NonErrorQueryResponse, QueryResponse};
use crate::transport::load_balancing::{self, RoutingInfo};
use crate::transport::metrics::Metrics;
use crate::transport::retry_policy::{QueryInfo, RetryDecision, RetrySession};
use crate::transport::{Node, NodeRef};
use tracing::{trace, trace_span, warn, Instrument};
use uuid::Uuid;

// #424
//
// Both `Query` and `PreparedStatement` have page size set to `None` as default,
// which means unlimited page size. This is a problem for `query_iter`
// and `execute_iter` because using them with such queries causes everything
// to be fetched in one page, despite them being meant to fetch data
// page-by-page.
//
// We can't really change the default page size for `Query`
// and `PreparedStatement` because it also affects `Session::{query,execute}`
// and this could break existing code.
//
// In order to work around the problem we just set the page size to a default
// value at the beginning of `query_iter` and `execute_iter`.
const DEFAULT_ITER_PAGE_SIZE: i32 = 5000;

/// Iterator over rows returned by paged queries\
/// Allows to easily access rows without worrying about handling multiple pages
pub struct RowIterator {
    current_row_idx: usize,
    current_page: Rows,
    page_receiver: mpsc::Receiver<Result<ReceivedPage, QueryError>>,
    tracing_ids: Vec<Uuid>,
}

struct ReceivedPage {
    pub rows: RawRows,
    pub tracing_id: Option<Uuid>,
}

pub(crate) struct PreparedIteratorConfig {
    pub prepared: PreparedStatement,
    pub values: SerializedValues,
    pub partition_key: Option<Bytes>,
    pub token: Option<Token>,
    pub execution_profile: Arc<ExecutionProfileInner>,
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
                    let rows = match received_page.rows.into_legacy_rows() {
                        Ok(rows) => rows,
                        Err(err) => return Poll::Ready(Some(Err(err.into()))),
                    };
                    s.current_page = rows;
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
        mut query: Query,
        values: SerializedValues,
        execution_profile: Arc<ExecutionProfileInner>,
        cluster_data: Arc<ClusterData>,
        metrics: Arc<Metrics>,
    ) -> Result<RowIterator, QueryError> {
        if query.get_page_size().is_none() {
            query.set_page_size(DEFAULT_ITER_PAGE_SIZE);
        }
        let (sender, receiver) = mpsc::channel(1);

        let consistency = query
            .config
            .consistency
            .unwrap_or(execution_profile.consistency);
        let serial_consistency = query
            .config
            .serial_consistency
            .unwrap_or(execution_profile.serial_consistency);

        let retry_session = query
            .get_retry_policy()
            .map(|rp| &**rp)
            .unwrap_or(&*execution_profile.retry_policy)
            .new_session();

        let parent_span = tracing::Span::current();
        let worker_task = async move {
            let query_ref = &query;
            let values_ref = &values;

            let choose_connection = |node: Arc<Node>| async move { node.random_connection().await };

            let page_query = |connection: Arc<Connection>,
                              consistency: Consistency,
                              paging_state: Option<Bytes>| async move {
                connection
                    .query_with_consistency(
                        query_ref,
                        values_ref,
                        consistency,
                        serial_consistency,
                        paging_state,
                    )
                    .await
            };

            let query_ref = &query;
            let serialized_values_size = values.size();

            let span_creator =
                move || RequestSpan::new_query(&query_ref.contents, serialized_values_size);

            let worker = RowIteratorWorker {
                sender: sender.into(),
                choose_connection,
                page_query,
                statement_info: RoutingInfo::default(),
                query_is_idempotent: query.config.is_idempotent,
                query_consistency: consistency,
                retry_session,
                execution_profile,
                metrics,
                paging_state: None,
                history_listener: query.config.history_listener.clone(),
                current_query_id: None,
                current_attempt_id: None,
                parent_span,
                span_creator,
            };

            worker.work(cluster_data).await
        };

        Self::new_from_worker_future(worker_task, receiver).await
    }

    pub(crate) async fn new_for_prepared_statement(
        mut config: PreparedIteratorConfig,
    ) -> Result<RowIterator, QueryError> {
        if config.prepared.get_page_size().is_none() {
            config.prepared.set_page_size(DEFAULT_ITER_PAGE_SIZE);
        }
        let (sender, receiver) = mpsc::channel(1);

        let consistency = config
            .prepared
            .config
            .consistency
            .unwrap_or(config.execution_profile.consistency);
        let serial_consistency = config
            .prepared
            .config
            .serial_consistency
            .unwrap_or(config.execution_profile.serial_consistency);
        let retry_session = config
            .prepared
            .get_retry_policy()
            .map(|rp| &**rp)
            .unwrap_or(&*config.execution_profile.retry_policy)
            .new_session();

        let parent_span = tracing::Span::current();
        let worker_task = async move {
            let statement_info = RoutingInfo {
                consistency,
                serial_consistency: config.prepared.get_serial_consistency(),
                token: config.token,
                keyspace: config.prepared.get_keyspace_name(),
                is_confirmed_lwt: config.prepared.is_confirmed_lwt(),
            };

            let prepared_ref = &config.prepared;
            let values_ref = &config.values;
            let partition_key = config.partition_key;
            let token = config.token;

            let choose_connection = |node: Arc<Node>| async move {
                match token {
                    Some(token) => node.connection_for_token(token).await,
                    None => node.random_connection().await,
                }
            };

            let page_query = |connection: Arc<Connection>,
                              consistency: Consistency,
                              paging_state: Option<Bytes>| async move {
                connection
                    .execute_with_consistency(
                        prepared_ref,
                        values_ref,
                        consistency,
                        serial_consistency,
                        paging_state,
                    )
                    .await
            };

            let serialized_values_size = config.values.size();

            let replicas: Option<smallvec::SmallVec<[_; 8]>> =
                if let (Some(keyspace), Some(token)) =
                    (statement_info.keyspace.as_ref(), statement_info.token)
                {
                    Some(
                        config
                            .cluster_data
                            .get_token_endpoints_iter(keyspace, token)
                            .cloned()
                            .collect(),
                    )
                } else {
                    None
                };

            let span_creator = move || {
                let span = RequestSpan::new_prepared(
                    partition_key.as_ref(),
                    token,
                    serialized_values_size,
                );
                if let Some(replicas) = replicas.as_ref() {
                    span.record_replicas(replicas);
                }
                span
            };

            let worker = RowIteratorWorker {
                sender: sender.into(),
                choose_connection,
                page_query,
                statement_info,
                query_is_idempotent: config.prepared.config.is_idempotent,
                query_consistency: consistency,
                retry_session,
                execution_profile: config.execution_profile,
                metrics: config.metrics,
                paging_state: None,
                history_listener: config.prepared.config.history_listener.clone(),
                current_query_id: None,
                current_attempt_id: None,
                parent_span,
                span_creator,
            };

            worker.work(config.cluster_data).await
        };

        Self::new_from_worker_future(worker_task, receiver).await
    }

    pub(crate) async fn new_for_connection_query_iter(
        mut query: Query,
        connection: Arc<Connection>,
        values: SerializedValues,
        consistency: Consistency,
        serial_consistency: Option<SerialConsistency>,
    ) -> Result<RowIterator, QueryError> {
        if query.get_page_size().is_none() {
            query.set_page_size(DEFAULT_ITER_PAGE_SIZE);
        }
        let (sender, receiver) = mpsc::channel::<Result<ReceivedPage, QueryError>>(1);

        let worker_task = async move {
            let worker = SingleConnectionRowIteratorWorker {
                sender: sender.into(),
                fetcher: |paging_state| {
                    connection.query_with_consistency(
                        &query,
                        &values,
                        consistency,
                        serial_consistency,
                        paging_state,
                    )
                },
            };
            worker.work().await
        };

        Self::new_from_worker_future(worker_task, receiver).await
    }

    async fn new_from_worker_future(
        worker_task: impl Future<Output = PageSendAttemptedProof> + Send + 'static,
        mut receiver: mpsc::Receiver<Result<ReceivedPage, QueryError>>,
    ) -> Result<RowIterator, QueryError> {
        tokio::task::spawn(worker_task.with_current_subscriber());

        // This unwrap is safe because:
        // - The future returned by worker.work sends at least one item
        //   to the channel (the PageSendAttemptedProof helps enforce this)
        // - That future is polled in a tokio::task which isn't going to be
        //   cancelled
        let pages_received = receiver.recv().await.unwrap()?;
        let rows = pages_received.rows.into_legacy_rows()?;

        Ok(RowIterator {
            current_row_idx: 0,
            current_page: rows,
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

// A separate module is used here so that the parent module cannot construct
// SendAttemptedProof directly.
mod checked_channel_sender {
    use scylla_cql::errors::QueryError;
    use scylla_cql::frame::response::result::RawRows;
    use std::marker::PhantomData;
    use tokio::sync::mpsc;
    use uuid::Uuid;

    use super::ReceivedPage;

    /// A value whose existence proves that there was an attempt
    /// to send an item of type T through a channel.
    /// Can only be constructed by ProvingSender::send.
    pub(crate) struct SendAttemptedProof<T>(PhantomData<T>);

    /// An mpsc::Sender which returns proofs that it attempted to send items.
    pub(crate) struct ProvingSender<T>(mpsc::Sender<T>);

    impl<T> From<mpsc::Sender<T>> for ProvingSender<T> {
        fn from(s: mpsc::Sender<T>) -> Self {
            Self(s)
        }
    }

    impl<T> ProvingSender<T> {
        pub(crate) async fn send(
            &self,
            value: T,
        ) -> (SendAttemptedProof<T>, Result<(), mpsc::error::SendError<T>>) {
            (SendAttemptedProof(PhantomData), self.0.send(value).await)
        }
    }

    type ResultPage = Result<ReceivedPage, QueryError>;

    impl ProvingSender<ResultPage> {
        pub(crate) async fn send_empty_page(
            &self,
            tracing_id: Option<Uuid>,
        ) -> (
            SendAttemptedProof<ResultPage>,
            Result<(), mpsc::error::SendError<ResultPage>>,
        ) {
            let empty_page = ReceivedPage {
                rows: RawRows::default(),
                tracing_id,
            };
            self.send(Ok(empty_page)).await
        }
    }
}

use checked_channel_sender::{ProvingSender, SendAttemptedProof};

type PageSendAttemptedProof = SendAttemptedProof<Result<ReceivedPage, QueryError>>;

// RowIteratorWorker works in the background to fetch pages
// RowIterator receives them through a channel
struct RowIteratorWorker<'a, ConnFunc, QueryFunc, SpanCreatorFunc> {
    sender: ProvingSender<Result<ReceivedPage, QueryError>>,

    // Closure used to choose a connection from a node
    // AsyncFn(Arc<Node>) -> Result<Arc<Connection>, QueryError>
    choose_connection: ConnFunc,

    // Closure used to perform a single page query
    // AsyncFn(Arc<Connection>, Option<Bytes>) -> Result<QueryResponse, QueryError>
    page_query: QueryFunc,

    statement_info: RoutingInfo<'a>,
    query_is_idempotent: bool,
    query_consistency: Consistency,
    retry_session: Box<dyn RetrySession>,
    execution_profile: Arc<ExecutionProfileInner>,
    metrics: Arc<Metrics>,

    paging_state: Option<Bytes>,

    history_listener: Option<Arc<dyn HistoryListener>>,
    current_query_id: Option<history::QueryId>,
    current_attempt_id: Option<history::AttemptId>,

    parent_span: tracing::Span,
    span_creator: SpanCreatorFunc,
}

impl<ConnFunc, ConnFut, QueryFunc, QueryFut, SpanCreator>
    RowIteratorWorker<'_, ConnFunc, QueryFunc, SpanCreator>
where
    ConnFunc: Fn(Arc<Node>) -> ConnFut,
    ConnFut: Future<Output = Result<Arc<Connection>, QueryError>>,
    QueryFunc: Fn(Arc<Connection>, Consistency, Option<Bytes>) -> QueryFut,
    QueryFut: Future<Output = Result<QueryResponse, QueryError>>,
    SpanCreator: Fn() -> RequestSpan,
{
    // Contract: this function MUST send at least one item through self.sender
    async fn work(mut self, cluster_data: Arc<ClusterData>) -> PageSendAttemptedProof {
        let load_balancer = self.execution_profile.load_balancing_policy.clone();
        let statement_info = self.statement_info.clone();
        let query_plan =
            load_balancing::Plan::new(load_balancer.as_ref(), &statement_info, &cluster_data);

        let mut last_error: QueryError =
            QueryError::ProtocolError("Empty query plan - driver bug!");
        let mut current_consistency: Consistency = self.query_consistency;

        self.log_query_start();

        'nodes_in_plan: for node in query_plan {
            let span =
                trace_span!(parent: &self.parent_span, "Executing query", node = %node.address);
            // For each node in the plan choose a connection to use
            // This connection will be reused for same node retries to preserve paging cache on the shard
            let connection: Arc<Connection> = match (self.choose_connection)(node.clone())
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
                    last_error = e;
                    // Broken connection doesn't count as a failed query, don't log in metrics
                    continue 'nodes_in_plan;
                }
            };

            'same_node_retries: loop {
                trace!(parent: &span, "Execution started");
                // Query pages until an error occurs
                let queries_result: Result<PageSendAttemptedProof, QueryError> = self
                    .query_pages(&connection, current_consistency, node)
                    .instrument(span.clone())
                    .await;

                last_error = match queries_result {
                    Ok(proof) => {
                        trace!(parent: &span, "Query succeeded");
                        // query_pages returned Ok, so we are guaranteed
                        // that it attempted to send at least one page
                        // through self.sender and we can safely return now.
                        return proof;
                    }
                    Err(error) => {
                        trace!(
                            parent: &span,
                            error = %error,
                            "Query failed"
                        );
                        error
                    }
                };

                // Use retry policy to decide what to do next
                let query_info = QueryInfo {
                    error: &last_error,
                    is_idempotent: self.query_is_idempotent,
                    consistency: LegacyConsistency::Regular(self.query_consistency),
                };

                let retry_decision = self.retry_session.decide_should_retry(query_info);
                trace!(
                    parent: &span,
                    retry_decision = format!("{:?}", retry_decision).as_str()
                );
                self.log_attempt_error(&last_error, &retry_decision);
                match retry_decision {
                    RetryDecision::RetrySameNode(cl) => {
                        self.metrics.inc_retries_num();
                        current_consistency = cl.unwrap_or(current_consistency);
                        continue 'same_node_retries;
                    }
                    RetryDecision::RetryNextNode(cl) => {
                        self.metrics.inc_retries_num();
                        current_consistency = cl.unwrap_or(current_consistency);
                        continue 'nodes_in_plan;
                    }
                    RetryDecision::DontRetry => break 'nodes_in_plan,
                    RetryDecision::IgnoreWriteError => {
                        warn!("Ignoring error during fetching pages; stopping fetching.");
                        // If we are here then, most likely, we didn't send
                        // anything through the self.sender channel.
                        // Although we are in an awkward situation (_iter
                        // interface isn't meant for sending writes),
                        // we must attempt to send something because
                        // the iterator expects it.
                        let (proof, _) = self.sender.send_empty_page(None).await;
                        return proof;
                    }
                };
            }
        }

        // Send last_error to RowIterator - query failed fully
        self.log_query_error(&last_error);
        let (proof, _) = self.sender.send(Err(last_error)).await;
        proof
    }

    // Given a working connection query as many pages as possible until the first error.
    //
    // Contract: this function must either:
    // - Return an error
    // - Return Ok but have attempted to send a page via self.sender
    async fn query_pages(
        &mut self,
        connection: &Arc<Connection>,
        consistency: Consistency,
        node: NodeRef<'_>,
    ) -> Result<PageSendAttemptedProof, QueryError> {
        loop {
            let request_span = (self.span_creator)();
            match self
                .query_one_page(connection, consistency, node, &request_span)
                .instrument(request_span.span().clone())
                .await?
            {
                ControlFlow::Break(proof) => return Ok(proof),
                ControlFlow::Continue(_) => {}
            }
        }
    }

    async fn query_one_page(
        &mut self,
        connection: &Arc<Connection>,
        consistency: Consistency,
        node: NodeRef<'_>,
        request_span: &RequestSpan,
    ) -> Result<ControlFlow<PageSendAttemptedProof, ()>, QueryError> {
        self.metrics.inc_total_paged_queries();
        let query_start = std::time::Instant::now();

        trace!(
            connection = %connection.get_connect_address(),
            "Sending"
        );
        self.log_attempt_start(connection.get_connect_address());

        let query_response =
            (self.page_query)(connection.clone(), consistency, self.paging_state.clone())
                .await
                .and_then(QueryResponse::into_non_error_query_response);

        let elapsed = query_start.elapsed();

        request_span.record_shard_id(connection);

        match query_response {
            Ok(NonErrorQueryResponse {
                response: NonErrorResponse::Result(result::Result::Rows(rows)),
                tracing_id,
                ..
            }) => {
                let _ = self.metrics.log_query_latency(elapsed.as_millis() as u64);
                self.log_attempt_success();
                self.log_query_success();
                self.execution_profile
                    .load_balancing_policy
                    .on_query_success(&self.statement_info, elapsed, node);

                self.paging_state = rows.metadata().paging_state.clone();

                request_span.record_rows_fields(&rows);

                let received_page = ReceivedPage { rows, tracing_id };

                // Send next page to RowIterator
                let (proof, res) = self.sender.send(Ok(received_page)).await;
                if res.is_err() {
                    // channel was closed, RowIterator was dropped - should shutdown
                    return Ok(ControlFlow::Break(proof));
                }

                if self.paging_state.is_none() {
                    // Reached the last query, shutdown
                    return Ok(ControlFlow::Break(proof));
                }

                // Query succeeded, reset retry policy for future retries
                self.retry_session.reset();
                self.log_query_start();

                Ok(ControlFlow::Continue(()))
            }
            Err(err) => {
                self.metrics.inc_failed_paged_queries();
                self.execution_profile
                    .load_balancing_policy
                    .on_query_failure(&self.statement_info, elapsed, node, &err);
                Err(err)
            }
            Ok(NonErrorQueryResponse {
                response: NonErrorResponse::Result(_),
                tracing_id,
                ..
            }) => {
                // We have most probably sent a modification statement (e.g. INSERT or UPDATE),
                // so let's return an empty iterator as suggested in #631.

                // We must attempt to send something because the iterator expects it.
                let (proof, _) = self.sender.send_empty_page(tracing_id).await;
                Ok(ControlFlow::Break(proof))
            }
            Ok(_) => {
                self.metrics.inc_failed_paged_queries();
                let err = QueryError::ProtocolError("Unexpected response to next page query");
                self.execution_profile
                    .load_balancing_policy
                    .on_query_failure(&self.statement_info, elapsed, node, &err);
                Err(err)
            }
        }
    }

    fn log_query_start(&mut self) {
        let history_listener: &dyn HistoryListener = match &self.history_listener {
            Some(hl) => &**hl,
            None => return,
        };

        self.current_query_id = Some(history_listener.log_query_start());
    }

    fn log_query_success(&mut self) {
        let history_listener: &dyn HistoryListener = match &self.history_listener {
            Some(hl) => &**hl,
            None => return,
        };

        let query_id: history::QueryId = match &self.current_query_id {
            Some(id) => *id,
            None => return,
        };

        history_listener.log_query_success(query_id);
    }

    fn log_query_error(&mut self, error: &QueryError) {
        let history_listener: &dyn HistoryListener = match &self.history_listener {
            Some(hl) => &**hl,
            None => return,
        };

        let query_id: history::QueryId = match &self.current_query_id {
            Some(id) => *id,
            None => return,
        };

        history_listener.log_query_error(query_id, error);
    }

    fn log_attempt_start(&mut self, node_addr: SocketAddr) {
        let history_listener: &dyn HistoryListener = match &self.history_listener {
            Some(hl) => &**hl,
            None => return,
        };

        let query_id: history::QueryId = match &self.current_query_id {
            Some(id) => *id,
            None => return,
        };

        self.current_attempt_id =
            Some(history_listener.log_attempt_start(query_id, None, node_addr));
    }

    fn log_attempt_success(&mut self) {
        let history_listener: &dyn HistoryListener = match &self.history_listener {
            Some(hl) => &**hl,
            None => return,
        };

        let attempt_id: history::AttemptId = match &self.current_attempt_id {
            Some(id) => *id,
            None => return,
        };

        history_listener.log_attempt_success(attempt_id);
    }

    fn log_attempt_error(&mut self, error: &QueryError, retry_decision: &RetryDecision) {
        let history_listener: &dyn HistoryListener = match &self.history_listener {
            Some(hl) => &**hl,
            None => return,
        };

        let attempt_id: history::AttemptId = match &self.current_attempt_id {
            Some(id) => *id,
            None => return,
        };

        history_listener.log_attempt_error(attempt_id, error, retry_decision);
    }
}

/// A massively simplified version of the RowIteratorWorker. It does not have
/// any complicated logic related to retries, it just fetches pages from
/// a single connection.
struct SingleConnectionRowIteratorWorker<Fetcher> {
    sender: ProvingSender<Result<ReceivedPage, QueryError>>,
    fetcher: Fetcher,
}

impl<Fetcher, FetchFut> SingleConnectionRowIteratorWorker<Fetcher>
where
    Fetcher: Fn(Option<Bytes>) -> FetchFut + Send + Sync,
    FetchFut: Future<Output = Result<QueryResponse, QueryError>> + Send,
{
    async fn work(mut self) -> PageSendAttemptedProof {
        match self.do_work().await {
            Ok(proof) => proof,
            Err(err) => {
                let (proof, _) = self.sender.send(Err(err)).await;
                proof
            }
        }
    }

    async fn do_work(&mut self) -> Result<PageSendAttemptedProof, QueryError> {
        let mut paging_state = None;
        loop {
            let result = (self.fetcher)(paging_state).await?;
            let response = result.into_non_error_query_response()?;
            match response.response {
                NonErrorResponse::Result(result::Result::Rows(rows)) => {
                    paging_state = rows.metadata().paging_state.clone();
                    let (proof, send_result) = self
                        .sender
                        .send(Ok(ReceivedPage {
                            rows,
                            tracing_id: response.tracing_id,
                        }))
                        .await;
                    if paging_state.is_none() || send_result.is_err() {
                        return Ok(proof);
                    }
                }
                NonErrorResponse::Result(_) => {
                    // We have most probably sent a modification statement (e.g. INSERT or UPDATE),
                    // so let's return an empty iterator as suggested in #631.

                    // We must attempt to send something because the iterator expects it.
                    let (proof, _) = self.sender.send_empty_page(response.tracing_id).await;
                    return Ok(proof);
                }
                _ => {
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
