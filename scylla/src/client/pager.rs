//! Entities that provide automated transparent paging of a query.
//! They enable consuming result of a paged query as a stream over rows,
//! which abstracts over page boundaries.

use std::future::Future;
use std::net::SocketAddr;
use std::ops::ControlFlow;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use futures::Stream;
use scylla_cql::Consistency;
use scylla_cql::deserialize::result::RawRowLendingIterator;
use scylla_cql::deserialize::row::{ColumnIterator, DeserializeRow};
use scylla_cql::deserialize::{DeserializationError, TypeCheckError};
use scylla_cql::frame::frame_errors::ResultMetadataAndRowsCountParseError;
use scylla_cql::frame::request::query::PagingState;
use scylla_cql::frame::response::NonErrorResponseWithDeserializedMetadata;
use scylla_cql::frame::response::result::DeserializedMetadataAndRawRows;
use scylla_cql::frame::types::SerialConsistency;
use scylla_cql::serialize::row::SerializedValues;
use std::result::Result;
use thiserror::Error;
use tokio::sync::mpsc;

use crate::client::execution_profile::ExecutionProfileInner;
use crate::cluster::{ClusterState, NodeRef};
use crate::deserialize::DeserializeOwnedRow;
use crate::errors::{RequestAttemptError, RequestError};
use crate::frame::response::result;
use crate::network::Connection;
use crate::observability::driver_tracing::RequestSpan;
use crate::observability::history::{self, HistoryListener};
#[cfg(feature = "metrics")]
use crate::observability::metrics::Metrics;
use crate::policies::load_balancing::{self, LoadBalancingPolicy, RoutingInfo};
use crate::policies::retry::{RequestInfo, RetryDecision, RetrySession};
use crate::response::query_result::ColumnSpecs;
use crate::response::{NonErrorQueryResponse, QueryResponse};
use crate::statement::prepared::{PartitionKeyError, PreparedStatement};
use crate::statement::unprepared::Statement;
use tracing::{Instrument, trace, trace_span, warn};
use uuid::Uuid;

// Like std::task::ready!, but handles the whole stack of Poll<Option<Result<>>>.
// If it matches Poll::Ready(Some(Ok(_))), then it returns the innermost value,
// otherwise it returns from the surrounding function.
macro_rules! ready_some_ok {
    ($e:expr) => {
        match $e {
            Poll::Ready(Some(Ok(x))) => x,
            Poll::Ready(Some(Err(err))) => return Poll::Ready(Some(Err(err.into()))),
            Poll::Ready(None) => return Poll::Ready(None),
            Poll::Pending => return Poll::Pending,
        }
    };
}

struct ReceivedPage {
    rows: DeserializedMetadataAndRawRows,
    tracing_id: Option<Uuid>,
    request_coordinator: Option<Coordinator>,
}

pub(crate) struct PreparedPagerConfig {
    pub(crate) prepared: PreparedStatement,
    pub(crate) values: SerializedValues,
    pub(crate) execution_profile: Arc<ExecutionProfileInner>,
    pub(crate) cluster_state: Arc<ClusterState>,
    #[cfg(feature = "metrics")]
    pub(crate) metrics: Arc<Metrics>,
}

// A separate module is used here so that the parent module cannot construct
// SendAttemptedProof directly.
mod checked_channel_sender {
    use scylla_cql::frame::response::result::DeserializedMetadataAndRawRows;
    use std::marker::PhantomData;
    use tokio::sync::mpsc;
    use uuid::Uuid;

    use crate::response::Coordinator;

    use super::{NextPageError, ReceivedPage};

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

    type ResultPage = Result<ReceivedPage, NextPageError>;

    impl ProvingSender<ResultPage> {
        pub(crate) async fn send_empty_page(
            &self,
            tracing_id: Option<Uuid>,
            request_coordinator: Option<Coordinator>,
        ) -> (
            SendAttemptedProof<ResultPage>,
            Result<(), mpsc::error::SendError<ResultPage>>,
        ) {
            let empty_page = ReceivedPage {
                rows: DeserializedMetadataAndRawRows::mock_empty(),
                tracing_id,
                request_coordinator,
            };
            self.send(Ok(empty_page)).await
        }
    }
}

use checked_channel_sender::{ProvingSender, SendAttemptedProof};

use crate::response::Coordinator;

type PageSendAttemptedProof = SendAttemptedProof<Result<ReceivedPage, NextPageError>>;

mod timeouter {
    use std::time::Duration;

    use tokio::time::Instant;

    /// Encapsulation of a timeout for paging queries.
    pub(super) struct PageQueryTimeouter {
        timeout: Duration,
        timeout_instant: Instant,
    }

    impl PageQueryTimeouter {
        /// Creates a new PageQueryTimeouter with the given timeout duration,
        /// starting from now.
        pub(super) fn new(timeout: Duration) -> Self {
            Self {
                timeout,
                timeout_instant: Instant::now() + timeout,
            }
        }

        /// Returns the timeout duration.
        pub(super) fn timeout_duration(&self) -> Duration {
            self.timeout
        }

        /// Returns the instant at which the timeout will elapse.
        ///
        /// This can be used with `tokio::time::timeout_at`.
        pub(super) fn deadline(&self) -> Instant {
            self.timeout_instant
        }

        /// Resets the timeout countdown.
        ///
        /// This should be called right before beginning first page fetch
        /// and after each successful page fetch.
        pub(super) fn reset(&mut self) {
            self.timeout_instant = Instant::now() + self.timeout;
        }
    }
}
use timeouter::PageQueryTimeouter;

// PagerWorker works in the background to fetch pages
// QueryPager receives them through a channel
struct PagerWorker<'a, QueryFunc, SpanCreatorFunc> {
    sender: ProvingSender<Result<ReceivedPage, NextPageError>>,

    // Closure used to perform a single page query
    // AsyncFn(Arc<Connection>, Option<Arc<[u8]>>) -> Result<QueryResponse, RequestAttemptError>
    page_query: QueryFunc,

    load_balancing_policy: Arc<dyn LoadBalancingPolicy>,
    routing_info: RoutingInfo<'a>,
    query_is_idempotent: bool,
    query_consistency: Consistency,
    retry_session: Box<dyn RetrySession>,
    timeouter: Option<PageQueryTimeouter>,
    #[cfg(feature = "metrics")]
    metrics: Arc<Metrics>,

    paging_state: PagingState,

    history_listener: Option<Arc<dyn HistoryListener>>,
    current_request_id: Option<history::RequestId>,
    current_attempt_id: Option<history::AttemptId>,

    parent_span: tracing::Span,
    span_creator: SpanCreatorFunc,
}

impl<QueryFunc, QueryFut, SpanCreator> PagerWorker<'_, QueryFunc, SpanCreator>
where
    QueryFunc: Fn(Arc<Connection>, Consistency, PagingState) -> QueryFut,
    QueryFut: Future<Output = Result<QueryResponse, RequestAttemptError>>,
    SpanCreator: Fn() -> RequestSpan,
{
    // Contract: this function MUST send at least one item through self.sender
    async fn work(mut self, cluster_state: Arc<ClusterState>) -> PageSendAttemptedProof {
        let load_balancer = Arc::clone(&self.load_balancing_policy);
        let statement_info = self.routing_info.clone();
        let query_plan =
            load_balancing::Plan::new(load_balancer.as_ref(), &statement_info, &cluster_state);

        let mut last_error: RequestError = RequestError::EmptyPlan;
        let mut current_consistency: Consistency = self.query_consistency;

        self.log_request_start();
        self.timeouter.as_mut().map(PageQueryTimeouter::reset);

        'nodes_in_plan: for (node, shard) in query_plan {
            let span = trace_span!(parent: &self.parent_span, "Executing query", node = %node.address, shard = %shard);
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

                let coordinator =
                    Coordinator::new(node, node.sharder().is_some().then_some(shard), &connection);

                // Query pages until an error occurs
                let queries_result: Result<
                    Result<PageSendAttemptedProof, RequestAttemptError>,
                    RequestTimeoutError,
                > = self
                    .query_pages(&connection, current_consistency, node, coordinator.clone())
                    .instrument(span.clone())
                    .await;

                let request_error: RequestAttemptError = match queries_result {
                    Ok(Ok(proof)) => {
                        trace!(parent: &span, "Request succeeded");
                        // query_pages returned Ok, so we are guaranteed
                        // that it attempted to send at least one page
                        // through self.sender and we can safely return now.
                        return proof;
                    }
                    Ok(Err(error)) => {
                        trace!(
                            parent: &span,
                            error = %error,
                            "Request failed"
                        );
                        error
                    }
                    Err(RequestTimeoutError(timeout)) => {
                        let request_error = RequestError::RequestTimeout(timeout);
                        self.log_request_error(&request_error);
                        trace!(
                            parent: &span,
                            error = %request_error,
                            "Request timed out"
                        );
                        let (proof, _) = self
                            .sender
                            .send(Err(NextPageError::RequestFailure(request_error)))
                            .await;
                        return proof;
                    }
                };

                // Use retry policy to decide what to do next
                let query_info = RequestInfo {
                    error: &request_error,
                    is_idempotent: self.query_is_idempotent,
                    consistency: self.query_consistency,
                };

                let retry_decision = self.retry_session.decide_should_retry(query_info);
                trace!(
                    parent: &span,
                    retry_decision = ?retry_decision
                );

                self.log_attempt_error(&request_error, &retry_decision);

                last_error = request_error.into();

                match retry_decision {
                    RetryDecision::RetrySameTarget(cl) => {
                        #[cfg(feature = "metrics")]
                        self.metrics.inc_retries_num();
                        current_consistency = cl.unwrap_or(current_consistency);
                        continue 'same_node_retries;
                    }
                    RetryDecision::RetryNextTarget(cl) => {
                        #[cfg(feature = "metrics")]
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
                        // QueryPager expects it.
                        let (proof, _) = self
                            .sender
                            .send_empty_page(None, Some(coordinator.clone()))
                            .await;
                        return proof;
                    }
                };
            }
        }

        self.log_request_error(&last_error);
        let (proof, _) = self
            .sender
            .send(Err(NextPageError::RequestFailure(last_error)))
            .await;
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
        coordinator: Coordinator,
    ) -> Result<Result<PageSendAttemptedProof, RequestAttemptError>, RequestTimeoutError> {
        loop {
            let request_span = (self.span_creator)();
            match self
                .query_one_page(
                    connection,
                    consistency,
                    node,
                    coordinator.clone(),
                    &request_span,
                )
                .instrument(request_span.span().clone())
                .await
            {
                Ok(Ok(ControlFlow::Break(proof))) => {
                    // Successfully queried the last remaining page.
                    return Ok(Ok(proof));
                }

                Ok(Ok(ControlFlow::Continue(()))) => {
                    // Successfully queried one page, and there are more to fetch.
                    // Reset the timeout_instant for the next page fetch.
                    self.timeouter.as_mut().map(PageQueryTimeouter::reset);
                }
                Ok(Err(request_attempt_error)) => {
                    return Ok(Err(request_attempt_error));
                }
                Err(request_timeout_error) => {
                    return Err(request_timeout_error);
                }
            }
        }
    }

    async fn query_one_page(
        &mut self,
        connection: &Arc<Connection>,
        consistency: Consistency,
        node: NodeRef<'_>,
        coordinator: Coordinator,
        request_span: &RequestSpan,
    ) -> Result<
        Result<ControlFlow<PageSendAttemptedProof, ()>, RequestAttemptError>,
        RequestTimeoutError,
    > {
        #[cfg(feature = "metrics")]
        self.metrics.inc_total_paged_queries();
        let query_start = std::time::Instant::now();

        let connect_address = connection.get_connect_address();
        trace!(
            connection = %connect_address,
            "Sending"
        );
        self.log_attempt_start(connect_address);

        let runner = async {
            (self.page_query)(connection.clone(), consistency, self.paging_state.clone())
                .await
                .and_then(QueryResponse::into_non_error_query_response)
        };
        let query_response = match self.timeouter {
            Some(ref timeouter) => {
                match tokio::time::timeout_at(timeouter.deadline(), runner).await {
                    Ok(res) => res,
                    Err(_) /* tokio::time::error::Elapsed */ => {
                        #[cfg(feature = "metrics")]
                        self.metrics.inc_request_timeouts();
                        return Err(RequestTimeoutError(timeouter.timeout_duration()));
                    }
                }
            }

            None => runner.await,
        };

        let elapsed = query_start.elapsed();

        request_span.record_shard_id(connection);

        match query_response {
            Ok(NonErrorQueryResponse {
                response:
                    NonErrorResponseWithDeserializedMetadata::Result(
                        result::ResultWithDeserializedMetadata::Rows((rows, paging_state_response)),
                    ),
                tracing_id,
                ..
            }) => {
                #[cfg(feature = "metrics")]
                let _ = self.metrics.log_query_latency(elapsed.as_millis() as u64);
                self.log_attempt_success();
                self.log_request_success();
                self.load_balancing_policy
                    .on_request_success(&self.routing_info, elapsed, node);

                request_span.record_raw_rows_fields(&rows);

                let received_page = ReceivedPage {
                    rows,
                    tracing_id,
                    request_coordinator: Some(coordinator),
                };

                // Send next page to QueryPager
                let (proof, res) = self.sender.send(Ok(received_page)).await;
                if res.is_err() {
                    // channel was closed, QueryPager was dropped - should shutdown
                    return Ok(Ok(ControlFlow::Break(proof)));
                }

                match paging_state_response.into_paging_control_flow() {
                    ControlFlow::Continue(paging_state) => {
                        self.paging_state = paging_state;
                    }
                    ControlFlow::Break(()) => {
                        // Reached the last query, shutdown
                        return Ok(Ok(ControlFlow::Break(proof)));
                    }
                }

                // Query succeeded, reset retry policy for future retries
                self.retry_session.reset();
                self.log_request_start();

                Ok(Ok(ControlFlow::Continue(())))
            }
            Err(err) => {
                #[cfg(feature = "metrics")]
                self.metrics.inc_failed_paged_queries();
                self.load_balancing_policy.on_request_failure(
                    &self.routing_info,
                    elapsed,
                    node,
                    &err,
                );
                Ok(Err(err))
            }
            Ok(NonErrorQueryResponse {
                response: NonErrorResponseWithDeserializedMetadata::Result(_),
                tracing_id,
                ..
            }) => {
                // We have most probably sent a modification statement (e.g. INSERT or UPDATE),
                // so let's return an empty iterator as suggested in #631.

                // We must attempt to send something because the iterator expects it.
                let (proof, _) = self
                    .sender
                    .send_empty_page(tracing_id, Some(coordinator))
                    .await;
                Ok(Ok(ControlFlow::Break(proof)))
            }
            Ok(response) => {
                #[cfg(feature = "metrics")]
                self.metrics.inc_failed_paged_queries();
                let err =
                    RequestAttemptError::UnexpectedResponse(response.response.to_response_kind());
                self.load_balancing_policy.on_request_failure(
                    &self.routing_info,
                    elapsed,
                    node,
                    &err,
                );
                Ok(Err(err))
            }
        }
    }

    fn log_request_start(&mut self) {
        let history_listener: &dyn HistoryListener = match &self.history_listener {
            Some(hl) => &**hl,
            None => return,
        };

        self.current_request_id = Some(history_listener.log_request_start());
    }

    fn log_request_success(&mut self) {
        let history_listener: &dyn HistoryListener = match &self.history_listener {
            Some(hl) => &**hl,
            None => return,
        };

        let request_id: history::RequestId = match &self.current_request_id {
            Some(id) => *id,
            None => return,
        };

        history_listener.log_request_success(request_id);
    }

    fn log_request_error(&mut self, error: &RequestError) {
        let history_listener: &dyn HistoryListener = match &self.history_listener {
            Some(hl) => &**hl,
            None => return,
        };

        let request_id: history::RequestId = match &self.current_request_id {
            Some(id) => *id,
            None => return,
        };

        history_listener.log_request_error(request_id, error);
    }

    fn log_attempt_start(&mut self, node_addr: SocketAddr) {
        let history_listener: &dyn HistoryListener = match &self.history_listener {
            Some(hl) => &**hl,
            None => return,
        };

        let request_id: history::RequestId = match &self.current_request_id {
            Some(id) => *id,
            None => return,
        };

        self.current_attempt_id =
            Some(history_listener.log_attempt_start(request_id, None, node_addr));
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

    fn log_attempt_error(&mut self, error: &RequestAttemptError, retry_decision: &RetryDecision) {
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

/// A massively simplified version of the PagerWorker. It does not have
/// any complicated logic related to retries, it just fetches pages from
/// a single connection.
struct SingleConnectionPagerWorker<Fetcher> {
    sender: ProvingSender<Result<ReceivedPage, NextPageError>>,
    fetcher: Fetcher,
    timeout: Option<Duration>,
}

impl<Fetcher, FetchFut> SingleConnectionPagerWorker<Fetcher>
where
    Fetcher: Fn(PagingState) -> FetchFut + Send + Sync,
    FetchFut: Future<Output = Result<QueryResponse, RequestAttemptError>> + Send,
{
    async fn work(mut self) -> PageSendAttemptedProof {
        match self.do_work().await {
            Ok(Ok(proof)) => proof,
            Ok(Err(err)) => {
                let (proof, _) = self
                    .sender
                    .send(Err(NextPageError::RequestFailure(
                        RequestError::LastAttemptError(err),
                    )))
                    .await;
                proof
            }
            Err(RequestTimeoutError(timeout)) => {
                let (proof, _) = self
                    .sender
                    .send(Err(NextPageError::RequestFailure(
                        RequestError::RequestTimeout(timeout),
                    )))
                    .await;
                proof
            }
        }
    }

    async fn do_work(
        &mut self,
    ) -> Result<Result<PageSendAttemptedProof, RequestAttemptError>, RequestTimeoutError> {
        let mut paging_state = PagingState::start();
        loop {
            let runner = async {
                (self.fetcher)(paging_state)
                    .await
                    .and_then(QueryResponse::into_non_error_query_response)
            };
            let response_res = match self.timeout {
                Some(timeout) => {
                    match tokio::time::timeout(timeout, runner).await {
                        Ok(res) => res,
                        Err(_) /* tokio::time::error::Elapsed */ => {
                            return Err(RequestTimeoutError(timeout));
                        }
                    }
                }

                None => runner.await,
            };
            let response = match response_res {
                Ok(resp) => resp,
                Err(err) => {
                    return Ok(Err(err));
                }
            };

            match response.response {
                NonErrorResponseWithDeserializedMetadata::Result(
                    result::ResultWithDeserializedMetadata::Rows((rows, paging_state_response)),
                ) => {
                    let (proof, send_result) = self
                        .sender
                        .send(Ok(ReceivedPage {
                            rows,
                            tracing_id: response.tracing_id,
                            request_coordinator: None,
                        }))
                        .await;

                    if send_result.is_err() {
                        // channel was closed, QueryPager was dropped - should shutdown
                        return Ok(Ok(proof));
                    }

                    match paging_state_response.into_paging_control_flow() {
                        ControlFlow::Continue(new_paging_state) => {
                            paging_state = new_paging_state;
                        }
                        ControlFlow::Break(()) => {
                            // Reached the last query, shutdown
                            return Ok(Ok(proof));
                        }
                    }
                }
                NonErrorResponseWithDeserializedMetadata::Result(_) => {
                    // We have most probably sent a modification statement (e.g. INSERT or UPDATE),
                    // so let's return an empty iterator as suggested in #631.

                    // We must attempt to send something because the iterator expects it.
                    let (proof, _) = self.sender.send_empty_page(response.tracing_id, None).await;
                    return Ok(Ok(proof));
                }
                _ => {
                    return Ok(Err(RequestAttemptError::UnexpectedResponse(
                        response.response.to_response_kind(),
                    )));
                }
            }
        }
    }
}

/// An intermediate object that allows to construct a stream over a query
/// that is asynchronously paged in the background.
///
/// Before the results can be processed in a convenient way, the QueryPager
/// needs to be cast into a typed stream. This is done by use of `rows_stream()` method.
/// As the method is generic over the target type, the turbofish syntax
/// can come in handy there, e.g. `query_pager.rows_stream::<(i32, String, Uuid)>()`.
#[derive(Debug)]
pub struct QueryPager {
    current_page: RawRowLendingIterator,
    page_receiver: mpsc::Receiver<Result<ReceivedPage, NextPageError>>,
    tracing_ids: Vec<Uuid>,
    request_coordinators: Vec<Coordinator>,
}

// QueryPager is not an iterator or a stream! However, it implements
// a `next()` method that returns a [ColumnIterator], which can be used
// to manually deserialize a row.
// The `ColumnIterator` borrows from the `QueryPager`, and the [futures::Stream] trait
// does not allow for such a pattern. Lending streams are not a thing yet.
impl QueryPager {
    /// Returns the next item (`ColumnIterator`) from the stream.
    ///
    /// Because pages may have different result metadata, each one needs to be type-checked before deserialization.
    /// The bool returned in second element of the tuple indicates whether the page was fresh or not.
    /// This allows user to then perform the type check for fresh pages.
    ///
    /// This is not a part of the `Stream` interface because the returned iterator
    /// borrows from self.
    ///
    /// This is cancel-safe.
    async fn next(&mut self) -> Option<Result<(ColumnIterator<'_, '_>, bool), NextRowError>> {
        let res = std::future::poll_fn(|cx| Pin::new(&mut *self).poll_fill_page(cx)).await;
        let fresh_page = match res {
            Some(Ok(f)) => f,
            Some(Err(err)) => return Some(Err(err)),
            None => return None,
        };

        // We are guaranteed here to have a non-empty page, so unwrap
        Some(
            self.current_page
                .next()
                .unwrap()
                .map_err(NextRowError::RowDeserializationError)
                .map(|x| (x, fresh_page)),
        )
    }

    /// Tries to acquire a non-empty page, if current page is exhausted.
    /// Boolean value in `Some(Ok(r))` is true if a new page was fetched.
    fn poll_fill_page(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<bool, NextRowError>>> {
        if !self.is_current_page_exhausted() {
            return Poll::Ready(Some(Ok(false)));
        }
        ready_some_ok!(self.as_mut().poll_next_page(cx));
        if self.is_current_page_exhausted() {
            // We most likely got a zero-sized page.
            // Try again later.
            cx.waker().wake_by_ref();
            Poll::Pending
        } else {
            Poll::Ready(Some(Ok(true)))
        }
    }

    /// Makes an attempt to acquire the next page (which may be empty).
    ///
    /// On success, returns Some(Ok()).
    /// On failure, returns Some(Err()).
    /// If there are no more pages, returns None.
    fn poll_next_page(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<(), NextRowError>>> {
        let mut s = self.as_mut();

        let received_page = ready_some_ok!(Pin::new(&mut s.page_receiver).poll_recv(cx));

        s.current_page = RawRowLendingIterator::new(received_page.rows);

        if let Some(tracing_id) = received_page.tracing_id {
            s.tracing_ids.push(tracing_id);
        }

        s.request_coordinators
            .extend(received_page.request_coordinator);

        Poll::Ready(Some(Ok(())))
    }

    /// Type-checks the iterator against given type.
    ///
    /// This is automatically called upon transforming [QueryPager] into [TypedRowStream].
    // Can be used with `next()` for manual deserialization.
    #[inline]
    #[deprecated(
        since = "1.4.0",
        note = "Type check should be performed for each page, which is not possible with public API.
Also, the only thing user can do (rows_stream) will take care of type check anyway.
If you are using this API, you are probably doing something wrong."
    )]
    pub fn type_check<'frame, 'metadata, RowT: DeserializeRow<'frame, 'metadata>>(
        &self,
    ) -> Result<(), TypeCheckError> {
        RowT::type_check(self.column_specs().as_slice())
    }

    /// Casts the iterator to a given row type, enabling [Stream]'ed operations
    /// on rows, which deserialize them on-the-fly to that given type.
    /// It only allows deserializing owned types, because [Stream] is not lending.
    /// Begins with performing type check.
    #[inline]
    pub fn rows_stream<RowT: for<'frame, 'metadata> DeserializeRow<'frame, 'metadata>>(
        self,
    ) -> Result<TypedRowStream<RowT>, TypeCheckError> {
        TypedRowStream::<RowT>::new(self)
    }

    pub(crate) async fn new_for_query(
        statement: Statement,
        execution_profile: Arc<ExecutionProfileInner>,
        cluster_state: Arc<ClusterState>,
        #[cfg(feature = "metrics")] metrics: Arc<Metrics>,
    ) -> Result<Self, NextPageError> {
        let (sender, receiver) = mpsc::channel::<Result<ReceivedPage, NextPageError>>(1);

        let consistency = statement
            .config
            .consistency
            .unwrap_or(execution_profile.consistency);
        let serial_consistency = statement
            .config
            .serial_consistency
            .unwrap_or(execution_profile.serial_consistency);

        let timeouter = statement
            .get_request_timeout()
            .or(execution_profile.request_timeout)
            .map(PageQueryTimeouter::new);

        let page_size = statement.get_validated_page_size();

        let routing_info = RoutingInfo {
            consistency,
            serial_consistency,
            ..Default::default()
        };

        let load_balancing_policy = Arc::clone(
            statement
                .get_load_balancing_policy()
                .unwrap_or(&execution_profile.load_balancing_policy),
        );

        let retry_session = statement
            .get_retry_policy()
            .map(|rp| &**rp)
            .unwrap_or(&*execution_profile.retry_policy)
            .new_session();

        let parent_span = tracing::Span::current();
        let worker_task = async move {
            let statement_ref = &statement;

            let page_query = |connection: Arc<Connection>,
                              consistency: Consistency,
                              paging_state: PagingState| {
                async move {
                    connection
                        .query_raw_with_consistency(
                            statement_ref,
                            consistency,
                            serial_consistency,
                            Some(page_size),
                            paging_state,
                        )
                        .await
                }
            };

            let query_ref = &statement;

            let span_creator = move || {
                let span = RequestSpan::new_query(&query_ref.contents);
                span.record_request_size(0);
                span
            };

            let worker = PagerWorker {
                sender: sender.into(),
                page_query,
                routing_info,
                query_is_idempotent: statement.config.is_idempotent,
                query_consistency: consistency,
                load_balancing_policy,
                retry_session,
                timeouter,
                #[cfg(feature = "metrics")]
                metrics,
                paging_state: PagingState::start(),
                history_listener: statement.config.history_listener.clone(),
                current_request_id: None,
                current_attempt_id: None,
                parent_span,
                span_creator,
            };

            worker.work(cluster_state).await
        };

        Self::new_from_worker_future(worker_task, receiver).await
    }

    pub(crate) async fn new_for_prepared_statement(
        config: PreparedPagerConfig,
    ) -> Result<Self, NextPageError> {
        let (sender, receiver) = mpsc::channel::<Result<ReceivedPage, NextPageError>>(1);

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

        let timeouter = config
            .prepared
            .get_request_timeout()
            .or(config.execution_profile.request_timeout)
            .map(PageQueryTimeouter::new);

        let page_size = config.prepared.get_validated_page_size();

        let load_balancing_policy = Arc::clone(
            config
                .prepared
                .get_load_balancing_policy()
                .unwrap_or(&config.execution_profile.load_balancing_policy),
        );

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
                    let (proof, _res) = ProvingSender::from(sender)
                        .send(Err(NextPageError::PartitionKeyError(err)))
                        .await;
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
                            .cluster_state
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
                    span.record_replicas(replicas.iter().map(|(node, shard)| (node, *shard)));
                }
                span
            };

            let worker = PagerWorker {
                sender: sender.into(),
                page_query,
                routing_info: statement_info,
                query_is_idempotent: config.prepared.config.is_idempotent,
                query_consistency: consistency,
                load_balancing_policy,
                retry_session,
                timeouter,
                #[cfg(feature = "metrics")]
                metrics: config.metrics,
                paging_state: PagingState::start(),
                history_listener: config.prepared.config.history_listener.clone(),
                current_request_id: None,
                current_attempt_id: None,
                parent_span,
                span_creator,
            };

            worker.work(config.cluster_state).await
        };

        Self::new_from_worker_future(worker_task, receiver).await
    }

    #[cfg(test)]
    pub(crate) async fn new_for_connection_query_iter(
        query: Statement,
        connection: Arc<Connection>,
        consistency: Consistency,
        serial_consistency: Option<SerialConsistency>,
    ) -> Result<Self, NextPageError> {
        let (sender, receiver) = mpsc::channel::<Result<ReceivedPage, NextPageError>>(1);

        let page_size = query.get_validated_page_size();
        let timeout = query.get_request_timeout().or_else(|| {
            query
                .get_execution_profile_handle()?
                .access()
                .request_timeout
        });

        let worker_task = async move {
            let worker = SingleConnectionPagerWorker {
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
                timeout,
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
    ) -> Result<Self, NextPageError> {
        let (sender, receiver) = mpsc::channel::<Result<ReceivedPage, NextPageError>>(1);

        let page_size = prepared.get_validated_page_size();
        let timeout = prepared.get_request_timeout().or_else(|| {
            prepared
                .get_execution_profile_handle()?
                .access()
                .request_timeout
        });

        let worker_task = async move {
            let worker = SingleConnectionPagerWorker {
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
                timeout,
            };
            worker.work().await
        };

        Self::new_from_worker_future(worker_task, receiver).await
    }

    async fn new_from_worker_future(
        worker_task: impl Future<Output = PageSendAttemptedProof> + Send + 'static,
        mut receiver: mpsc::Receiver<Result<ReceivedPage, NextPageError>>,
    ) -> Result<Self, NextPageError> {
        let worker_handle = tokio::task::spawn(worker_task);

        let Some(page_received_res) = receiver.recv().await else {
            // - The future returned by worker.work sends at least one item
            //   to the channel (the PageSendAttemptedProof helps enforce this);
            // - That future is polled in a tokio::task which isn't going to be
            //   cancelled, **unless** the runtime is being shut down.
            // - Another way for the worker task to terminate without sending
            //   anything could be panic.
            // Therefore, there are two possible reasons for recv() to return None:
            // 1. The runtime is being shut down.
            // 2. The worker task panicked.
            //
            // Both cases are handled below, and in both cases we do not return
            // from this function, but rather either propagate the panic,
            // or hang indefinitely to avoid returning from here during runtime shutdown.
            let worker_result = worker_handle.await;
            match worker_result {
                Ok(_send_attempted_proof) => {
                    unreachable!(
                        "Worker task completed without sending any page, despite having returned proof of having sent some"
                    )
                }
                Err(join_error) => {
                    let is_cancelled = join_error.is_cancelled();
                    if let Ok(panic_payload) = join_error.try_into_panic() {
                        // Worker task panicked. Propagate the panic.
                        std::panic::resume_unwind(panic_payload);
                    } else {
                        // This is not a panic, so it must be runtime shutdown.
                        assert!(
                            is_cancelled,
                            "PagerWorker task join error is neither a panic nor cancellation, which should be impossible"
                        );
                        // Let's await a never-ending future to avoid returning from here.
                        // But before, let's emit a message to indicate that we're in such a situation.
                        tracing::info!(
                            "Runtime is being shut down while QueryPager is being constructed; hanging the future indefinitely"
                        );
                        return futures::future::pending().await;
                    }
                }
            }
        };
        let page_received = page_received_res?;

        Ok(Self {
            current_page: RawRowLendingIterator::new(page_received.rows),
            page_receiver: receiver,
            tracing_ids: if let Some(tracing_id) = page_received.tracing_id {
                vec![tracing_id]
            } else {
                Vec::new()
            },
            request_coordinators: Vec::from_iter(page_received.request_coordinator),
        })
    }

    /// If tracing was enabled, returns tracing ids of all finished page queries.
    #[inline]
    pub fn tracing_ids(&self) -> &[Uuid] {
        &self.tracing_ids
    }

    /// Returns the targets that served finished page queries, in query order.
    #[inline]
    pub fn request_coordinators(&self) -> impl Iterator<Item = &Coordinator> {
        self.request_coordinators.iter()
    }

    /// Returns specification of row columns
    #[inline]
    pub fn column_specs(&self) -> ColumnSpecs<'_, '_> {
        ColumnSpecs::new(self.current_page.metadata().col_specs())
    }

    fn is_current_page_exhausted(&self) -> bool {
        self.current_page.rows_remaining() == 0
    }
}

/// Returned by [QueryPager::rows_stream].
///
/// Implements [Stream], but only permits deserialization of owned types.
/// To use [Stream] API (only accessible for owned types), use [QueryPager::rows_stream].
pub struct TypedRowStream<RowT> {
    raw_row_lending_stream: QueryPager,
    current_page_typechecked: bool,
    _phantom: std::marker::PhantomData<RowT>,
}

// Manual implementation not to depend on RowT implementing Debug.
// Explanation: automatic derive of Debug would impose the RowT: Debug
// constaint for the Debug impl.
impl<T> std::fmt::Debug for TypedRowStream<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TypedRowStream")
            .field("raw_row_lending_stream", &self.raw_row_lending_stream)
            .finish()
    }
}

impl<RowT> Unpin for TypedRowStream<RowT> {}

impl<RowT> TypedRowStream<RowT>
where
    RowT: for<'frame, 'metadata> DeserializeRow<'frame, 'metadata>,
{
    fn new(raw_stream: QueryPager) -> Result<Self, TypeCheckError> {
        #[allow(deprecated)] // In TypedRowStream we take care to type check each page.
        raw_stream.type_check::<RowT>()?;

        Ok(Self {
            raw_row_lending_stream: raw_stream,
            current_page_typechecked: true,
            _phantom: Default::default(),
        })
    }
}

impl<RowT> TypedRowStream<RowT> {
    /// If tracing was enabled, returns tracing ids of all finished page queries.
    #[inline]
    pub fn tracing_ids(&self) -> &[Uuid] {
        self.raw_row_lending_stream.tracing_ids()
    }

    /// Returns the targets that served finished page queries, in query order.
    #[inline]
    pub fn request_coordinators(&self) -> impl Iterator<Item = &Coordinator> {
        self.raw_row_lending_stream.request_coordinators()
    }

    /// Returns specification of row columns
    #[inline]
    pub fn column_specs(&self) -> ColumnSpecs<'_, '_> {
        self.raw_row_lending_stream.column_specs()
    }
}

/// Stream implementation for TypedRowStream.
///
/// It only works with owned types! For example, &str is not supported.
impl<RowT> Stream for TypedRowStream<RowT>
where
    RowT: DeserializeOwnedRow,
{
    type Item = Result<RowT, NextRowError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let next_fut = async {
            let real_self: &mut Self = &mut self; // Self is Unpin, and this lets us perform partial borrows.
            real_self.raw_row_lending_stream.next().await.map(|res| {
                res.and_then(|(column_iterator, fresh_page)| {
                    if fresh_page {
                        real_self.current_page_typechecked = false;
                    }
                    if !real_self.current_page_typechecked {
                        column_iterator.type_check::<RowT>().map_err(|e| {
                            NextRowError::NextPageError(NextPageError::TypeCheckError(e))
                        })?;
                        real_self.current_page_typechecked = true;
                    }
                    <RowT as DeserializeRow>::deserialize(column_iterator)
                        .map_err(NextRowError::RowDeserializationError)
                })
            })
        };

        futures::pin_mut!(next_fut);
        let value = ready_some_ok!(next_fut.poll(cx));
        Poll::Ready(Some(Ok(value)))
    }
}

/// Failed to run a request within a provided client timeout.
#[derive(Error, Debug, Clone)]
#[error(
    "Request execution exceeded a client timeout of {}ms",
    std::time::Duration::as_millis(.0)
)]
struct RequestTimeoutError(std::time::Duration);

/// An error returned that occurred during next page fetch.
#[derive(Error, Debug, Clone)]
#[non_exhaustive]
pub enum NextPageError {
    /// PK extraction and/or token calculation error. Applies only for prepared statements.
    #[error("Failed to extract PK and compute token required for routing: {0}")]
    PartitionKeyError(#[from] PartitionKeyError),

    /// Failed to run a request responsible for fetching new page.
    #[error(transparent)]
    RequestFailure(#[from] RequestError),

    /// Failed to deserialize result metadata associated with next page response.
    #[error("Failed to deserialize result metadata associated with next page response: {0}")]
    ResultMetadataParseError(#[from] ResultMetadataAndRowsCountParseError),

    /// Failed to type check a received page.
    #[error("Failed to type check a received page: {0}")]
    TypeCheckError(#[from] TypeCheckError),
}

/// An error returned by async iterator API.
#[derive(Error, Debug, Clone)]
#[non_exhaustive]
pub enum NextRowError {
    /// Failed to fetch next page of result.
    #[error("Failed to fetch next page of result: {0}")]
    NextPageError(#[from] NextPageError),

    /// An error occurred during row deserialization.
    #[error("Row deserialization error: {0}")]
    RowDeserializationError(#[from] DeserializationError),
}
