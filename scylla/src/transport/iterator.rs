//! Iterators over rows returned by paged queries

use std::future::Future;
use std::mem;
use std::net::SocketAddr;
use std::ops::ControlFlow;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use futures::Stream;
use scylla_cql::frame::response::NonErrorResponse;
use scylla_cql::types::serialize::row::SerializedValues;
use std::result::Result;
use thiserror::Error;
use tokio::sync::mpsc;

use super::execution_profile::ExecutionProfileInner;
use super::session::RequestSpan;
use crate::cql_to_rust::{FromRow, FromRowError};

use crate::errors::{QueryError, UserRequestError};
use crate::frame::response::{
    result,
    result::{ColumnSpec, Row, Rows},
};
use crate::history::{self, HistoryListener};
use crate::statement::{prepared_statement::PreparedStatement, query::Query};
use crate::statement::{Consistency, PagingState, SerialConsistency};
use crate::transport::cluster::ClusterData;
use crate::transport::connection::{Connection, NonErrorQueryResponse, QueryResponse};
use crate::transport::load_balancing::{self, RoutingInfo};
use crate::transport::metrics::Metrics;
use crate::transport::retry_policy::{QueryInfo, RetryDecision, RetrySession};
use crate::transport::NodeRef;
use tracing::{trace, trace_span, warn, Instrument};
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
    rows: Rows,
    tracing_id: Option<Uuid>,
}

pub(crate) struct PreparedIteratorConfig {
    pub(crate) prepared: PreparedStatement,
    pub(crate) values: SerializedValues,
    pub(crate) execution_profile: Arc<ExecutionProfileInner>,
    pub(crate) cluster_data: Arc<ClusterData>,
    pub(crate) metrics: Arc<Metrics>,
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
        execution_profile: Arc<ExecutionProfileInner>,
        cluster_data: Arc<ClusterData>,
        metrics: Arc<Metrics>,
    ) -> Result<RowIterator, QueryError> {
        let (sender, receiver) = mpsc::channel(1);

        let consistency = query
            .config
            .consistency
            .unwrap_or(execution_profile.consistency);
        let serial_consistency = query
            .config
            .serial_consistency
            .unwrap_or(execution_profile.serial_consistency);

        let page_size = query.get_validated_page_size();

        let routing_info = RoutingInfo {
            consistency,
            serial_consistency,
            ..Default::default()
        };

        let retry_session = query
            .get_retry_policy()
            .map(|rp| &**rp)
            .unwrap_or(&*execution_profile.retry_policy)
            .new_session();

        let parent_span = tracing::Span::current();
        let worker_task = async move {
            let query_ref = &query;

            let page_query = |connection: Arc<Connection>,
                              consistency: Consistency,
                              paging_state: PagingState| {
                async move {
                    connection
                        .query_raw_with_consistency(
                            query_ref,
                            consistency,
                            serial_consistency,
                            Some(page_size),
                            paging_state,
                        )
                        .await
                }
            };

            let query_ref = &query;

            let span_creator = move || {
                let span = RequestSpan::new_query(&query_ref.contents);
                span.record_request_size(0);
                span
            };

            let worker = RowIteratorWorker {
                sender: sender.into(),
                page_query,
                statement_info: routing_info,
                query_is_idempotent: query.config.is_idempotent,
                query_consistency: consistency,
                retry_session,
                execution_profile,
                metrics,
                paging_state: PagingState::start(),
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
        config: PreparedIteratorConfig,
    ) -> Result<RowIterator, QueryError> {
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

        let page_size = config.prepared.get_validated_page_size();

        let retry_session = config
            .prepared
            .get_retry_policy()
            .map(|rp| &**rp)
            .unwrap_or(&*config.execution_profile.retry_policy)
            .new_session();

        let parent_span = tracing::Span::current();
        let worker_task = async move {
            let prepared_ref = &config.prepared;
            let values_ref = &config.values;

            let (partition_key, token) = match prepared_ref
                .extract_partition_key_and_calculate_token(
                    prepared_ref.get_partitioner_name(),
                    values_ref,
                ) {
                Ok(res) => res.unzip(),
                Err(err) => {
                    let (proof, _res) = ProvingSender::from(sender).send(Err(err)).await;
                    return proof;
                }
            };

            let table_spec = config.prepared.get_table_spec();
            let statement_info = RoutingInfo {
                consistency,
                serial_consistency,
                token,
                table: table_spec,
                is_confirmed_lwt: config.prepared.is_confirmed_lwt(),
            };

            let page_query = |connection: Arc<Connection>,
                              consistency: Consistency,
                              paging_state: PagingState| async move {
                connection
                    .execute_raw_with_consistency(
                        prepared_ref,
                        values_ref,
                        consistency,
                        serial_consistency,
                        Some(page_size),
                        paging_state,
                    )
                    .await
            };

            let serialized_values_size = config.values.buffer_size();

            let replicas: Option<smallvec::SmallVec<[_; 8]>> =
                if let (Some(table_spec), Some(token)) =
                    (statement_info.table, statement_info.token)
                {
                    Some(
                        config
                            .cluster_data
                            .get_token_endpoints_iter(table_spec, token)
                            .map(|(node, shard)| (node.clone(), shard))
                            .collect(),
                    )
                } else {
                    None
                };

            let span_creator = move || {
                let span = RequestSpan::new_prepared(
                    partition_key.as_ref().map(|pk| pk.iter()),
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
                page_query,
                statement_info,
                query_is_idempotent: config.prepared.config.is_idempotent,
                query_consistency: consistency,
                retry_session,
                execution_profile: config.execution_profile,
                metrics: config.metrics,
                paging_state: PagingState::start(),
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
        query: Query,
        connection: Arc<Connection>,
        consistency: Consistency,
        serial_consistency: Option<SerialConsistency>,
    ) -> Result<RowIterator, QueryError> {
        let (sender, receiver) = mpsc::channel::<Result<ReceivedPage, QueryError>>(1);

        let page_size = query.get_validated_page_size();

        let worker_task = async move {
            let worker = SingleConnectionRowIteratorWorker {
                sender: sender.into(),
                fetcher: |paging_state| {
                    connection.query_raw_with_consistency(
                        &query,
                        consistency,
                        serial_consistency,
                        Some(page_size),
                        paging_state,
                    )
                },
            };
            worker.work().await
        };

        Self::new_from_worker_future(worker_task, receiver).await
    }

    pub(crate) async fn new_for_connection_execute_iter(
        prepared: PreparedStatement,
        values: SerializedValues,
        connection: Arc<Connection>,
        consistency: Consistency,
        serial_consistency: Option<SerialConsistency>,
    ) -> Result<RowIterator, QueryError> {
        let (sender, receiver) = mpsc::channel::<Result<ReceivedPage, QueryError>>(1);

        let page_size = prepared.get_validated_page_size();

        let worker_task = async move {
            let worker = SingleConnectionRowIteratorWorker {
                sender: sender.into(),
                fetcher: |paging_state| {
                    connection.execute_raw_with_consistency(
                        &prepared,
                        &values,
                        consistency,
                        serial_consistency,
                        Some(page_size),
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
        tokio::task::spawn(worker_task);

        // This unwrap is safe because:
        // - The future returned by worker.work sends at least one item
        //   to the channel (the PageSendAttemptedProof helps enforce this)
        // - That future is polled in a tokio::task which isn't going to be
        //   cancelled
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

// A separate module is used here so that the parent module cannot construct
// SendAttemptedProof directly.
mod checked_channel_sender {
    use scylla_cql::frame::{
        request::query::PagingStateResponse,
        response::result::{ResultMetadata, Rows},
    };
    use std::{marker::PhantomData, sync::Arc};
    use tokio::sync::mpsc;
    use uuid::Uuid;

    use crate::errors::QueryError;

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
                rows: Rows {
                    metadata: Arc::new(ResultMetadata::mock_empty()),
                    paging_state_response: PagingStateResponse::NoMorePages,
                    rows_count: 0,
                    rows: Vec::new(),
                    serialized_size: 0,
                },
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
struct RowIteratorWorker<'a, QueryFunc, SpanCreatorFunc> {
    sender: ProvingSender<Result<ReceivedPage, QueryError>>,

    // Closure used to perform a single page query
    // AsyncFn(Arc<Connection>, Option<Arc<[u8]>>) -> Result<QueryResponse, UserRequestError>
    page_query: QueryFunc,

    statement_info: RoutingInfo<'a>,
    query_is_idempotent: bool,
    query_consistency: Consistency,
    retry_session: Box<dyn RetrySession>,
    execution_profile: Arc<ExecutionProfileInner>,
    metrics: Arc<Metrics>,

    paging_state: PagingState,

    history_listener: Option<Arc<dyn HistoryListener>>,
    current_query_id: Option<history::QueryId>,
    current_attempt_id: Option<history::AttemptId>,

    parent_span: tracing::Span,
    span_creator: SpanCreatorFunc,
}

impl<QueryFunc, QueryFut, SpanCreator> RowIteratorWorker<'_, QueryFunc, SpanCreator>
where
    QueryFunc: Fn(Arc<Connection>, Consistency, PagingState) -> QueryFut,
    QueryFut: Future<Output = Result<QueryResponse, UserRequestError>>,
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

        'nodes_in_plan: for (node, shard) in query_plan {
            let span =
                trace_span!(parent: &self.parent_span, "Executing query", node = %node.address);
            // For each node in the plan choose a connection to use
            // This connection will be reused for same node retries to preserve paging cache on the shard
            let connection: Arc<Connection> = match node
                .connection_for_shard(shard)
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
                    last_error = e.into();
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
                    consistency: self.query_consistency,
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
                response: NonErrorResponse::Result(result::Result::Rows(mut rows)),
                tracing_id,
                ..
            }) => {
                let _ = self.metrics.log_query_latency(elapsed.as_millis() as u64);
                self.log_attempt_success();
                self.log_query_success();
                self.execution_profile
                    .load_balancing_policy
                    .on_query_success(&self.statement_info, elapsed, node);

                let paging_state_response = rows.paging_state_response.take();

                request_span.record_rows_fields(&rows);

                let received_page = ReceivedPage { rows, tracing_id };

                // Send next page to RowIterator
                let (proof, res) = self.sender.send(Ok(received_page)).await;
                if res.is_err() {
                    // channel was closed, RowIterator was dropped - should shutdown
                    return Ok(ControlFlow::Break(proof));
                }

                match paging_state_response.into_paging_control_flow() {
                    ControlFlow::Continue(paging_state) => {
                        self.paging_state = paging_state;
                    }
                    ControlFlow::Break(()) => {
                        // Reached the last query, shutdown
                        return Ok(ControlFlow::Break(proof));
                    }
                }

                // Query succeeded, reset retry policy for future retries
                self.retry_session.reset();
                self.log_query_start();

                Ok(ControlFlow::Continue(()))
            }
            Err(err) => {
                let err = err.into();
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
    Fetcher: Fn(PagingState) -> FetchFut + Send + Sync,
    FetchFut: Future<Output = Result<QueryResponse, UserRequestError>> + Send,
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
        let mut paging_state = PagingState::start();
        loop {
            let result = (self.fetcher)(paging_state).await?;
            let response = result.into_non_error_query_response()?;
            match response.response {
                NonErrorResponse::Result(result::Result::Rows(mut rows)) => {
                    let paging_state_response = rows.paging_state_response.take();

                    let (proof, send_result) = self
                        .sender
                        .send(Ok(ReceivedPage {
                            rows,
                            tracing_id: response.tracing_id,
                        }))
                        .await;

                    if send_result.is_err() {
                        // channel was closed, RowIterator was dropped - should shutdown
                        return Ok(proof);
                    }

                    match paging_state_response.into_paging_control_flow() {
                        ControlFlow::Continue(new_paging_state) => {
                            paging_state = new_paging_state;
                        }
                        ControlFlow::Break(()) => {
                            // Reached the last query, shutdown
                            return Ok(proof);
                        }
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
