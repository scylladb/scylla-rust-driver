//! Entities that provide automated transparent paging of a query.
//! They enable consuming result of a paged query as a stream over rows,
//! which abstracts over page boundaries.

use std::cell::OnceCell;
use std::future::Future;
use std::ops::ControlFlow;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use futures::Stream;
use slot_channel::{SlotReceiver, SlotSender, slot_channel};
use std::result::Result;
use thiserror::Error;
use tracing::{Instrument, warn};
use uuid::Uuid;

use crate::client::execution::{
    NodeAttemptTarget, Request, RequestExecutionOutcome, RequestExecutionParams, RequestPaging,
    RunRequestResult, SingleConnectionTarget, choose_tablet_block_hint,
};
use crate::client::session::Session;
use crate::cluster::{ClusterState, Node};
use crate::deserialize::DeserializeOwnedRow;
use crate::deserialize::result::RawRowLendingIterator;
use crate::deserialize::row::{ColumnIterator, DeserializeRow};
use crate::deserialize::{DeserializationError, TypeCheckError};
use crate::errors::{PagerExecutionError, RequestAttemptError, RequestError};
use crate::frame::frame_errors::ResultMetadataAndRowsCountParseError;
use crate::frame::request::query::{PagingState, PagingStateResponse};
use crate::frame::response::NonErrorResponseWithDeserializedMetadataV2 as NonErrorResponseWithDeserializedMetadata;
use crate::frame::response::result;
use crate::frame::response::result::{
    DeserializedMetadataAndRawRows, SchemaChange, SetKeyspace, TableSpec,
};
use crate::frame::types::{Consistency, SerialConsistency};
use crate::network::Connection;
use crate::observability::driver_tracing::{RequestSpan, await_in_span};
use crate::observability::history::HistoryListener;
use crate::observability::metrics::Metrics;
use crate::policies::load_balancing::{self, LoadBalancingPolicy, RoutingInfo};
use crate::policies::retry::{FallthroughRetryPolicy, RetryPolicy};
use crate::policies::speculative_execution::SpeculativeExecutionPolicy;
use crate::response::query_result::ColumnSpecs;
use crate::response::{Coordinator, NonErrorQueryResponse, QueryResponse};
use crate::routing::{NodeLocationPreference, Shard, Token};
use crate::serialize::row::SerializedValues;
use crate::statement::PageSize;
use crate::statement::StatementConfig;
use crate::statement::prepared::{PartitionKey, PartitionKeyError, PreparedStatement};
use crate::statement::unprepared::Statement;

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

struct NextReceivedPage {
    rows: DeserializedMetadataAndRawRows,
    tracing_id: Option<Uuid>,
    request_coordinator: Option<Coordinator>,
}

/*
 * The first page is special in a number of ways:
 * - It is delivered synchronously (not meaning non-`async`, but the code
 *   does not progress until the first page is there) when QueryPager is
 *   constructed. All subsequent pages are delivered asynchronously via
 *   a channel to QueryPager.
 * - The first page may be non-Rows Result and still correct. For example,
 *   `USE <keyspace>` statements return Result:SetKeyspace, and DDLs often
 *   return Result:SchemaChange response. In order to handle those special
 *   results, we need access to Session:
 *   - SetKeyspace requires issuing `USE <keyspace>` statement on
 *     connections to all nodes;
 *   - SchemaChange should be followed with awaiting schema agreement.
 *   Session is available when constructing the QueryPager (except for
 *   `Connection::execute_iter()` API, which we handle separately by
 *   treating all non-Rows results as erroneous).
 *   However, Session is not available once we have the QueryPager
 *   constructed, because it does not borrow from Session. Combining this
 *   with the fact that non-Rows result should never appear as a non-first
 *   page in the sequence of pages, this is another argument for having
 *   clear distinction between the first page case and the remaining pages.
 */

enum FirstPageContent {
    Rows {
        rows: DeserializedMetadataAndRawRows,
    },
    SetKeyspace {
        set_keyspace: SetKeyspace,
    },
    SchemaChange {
        schema_change: SchemaChange,
    },
}

struct FirstReceivedPage {
    content: FirstPageContent,
    tracing_id: Option<Uuid>,
    request_coordinator: Coordinator,
}

type ResultNextPage = Result<NextReceivedPage, NextPageError>;

enum ShouldFetchMorePages {
    NoMorePages,
    MorePages,
}

/// Drives paged execution by repeatedly invoking the unified request-execution
/// core ([`run_request_no_side_effects`]) - once per page.
///
/// Each page is one logical request: it goes through the full load balancing,
/// retry, speculative-execution, history and metrics machinery, exactly like a
/// non-paged request. The only paging-specific logic that remains here is:
/// - injecting the current [`PagingState`] into each attempt,
/// - "coordinator stability" (preferring the node/connection that served the
///   previous page for the next one),
/// - turning each successful page into a [`NextReceivedPage`] sent over the
///   channel, and stopping once the server reports no more pages.
struct PagingExecutor {
    cluster_state: Arc<ClusterState>,
    load_balancing_policy: Arc<dyn LoadBalancingPolicy>,
    retry_policy: Arc<dyn RetryPolicy>,
    speculative_policy: Option<Arc<dyn SpeculativeExecutionPolicy>>,
    request_timeout: Option<Duration>,
    is_idempotent: bool,
    consistency: Consistency,
    serial_consistency: Option<SerialConsistency>,
    metrics: Arc<Metrics>,
    history_listener: Option<Arc<dyn HistoryListener>>,
    paging_state: PagingState,
    stable_coordinator: Option<Coordinator>,
}

impl PagingExecutor {
    fn new(session: &Session, statement: &StatementConfig) -> Self {
        let execution_profile = statement
            .execution_profile_handle
            .as_ref()
            .unwrap_or_else(|| session.get_default_execution_profile_handle())
            .access();

        Self {
            cluster_state: session.get_cluster_state(),
            load_balancing_policy: Arc::clone(
                statement
                    .load_balancing_policy
                    .as_ref()
                    .unwrap_or(&execution_profile.load_balancing_policy),
            ),
            retry_policy: Arc::clone(
                statement
                    .retry_policy
                    .as_ref()
                    .unwrap_or(&execution_profile.retry_policy),
            ),
            speculative_policy: execution_profile
                .speculative_execution_policy
                .as_ref()
                .map(Arc::clone),

            request_timeout: statement
                .request_timeout
                .or(execution_profile.request_timeout),
            is_idempotent: statement.is_idempotent,
            consistency: statement
                .consistency
                .unwrap_or(execution_profile.consistency),
            serial_consistency: statement
                .serial_consistency
                .unwrap_or(execution_profile.serial_consistency),
            metrics: session.get_metrics_priv(),
            history_listener: statement.history_listener.as_ref().map(Arc::clone),
            paging_state: PagingState::start(),
            stable_coordinator: None,
        }
    }

    /// Fetches remaining pages (pages 2+) in a background task.
    /// Sends each page through the slot channel.
    ///
    /// A fresh query plan is constructed for each page, preventing plan
    /// exhaustion for long-running multi-page queries.
    ///
    /// The last successful coordinator is preferred for the next page
    /// (coordinator stability), but all other nodes remain available
    /// for retry.
    async fn query_remaining_pages<QueryFunc, QueryFut, SpanCreator>(
        mut self,
        sender: SlotSender<ResultNextPage>,
        page_query: QueryFunc,
        routing_info: RoutingInfo<'_>,
        span_creator: SpanCreator,
    ) where
        QueryFunc: Fn(Arc<Connection>, Consistency, PagingState) -> QueryFut,
        QueryFut: Future<Output = Result<QueryResponse, RequestAttemptError>>,
        SpanCreator: Fn(&ClusterState) -> RequestSpan,
    {
        // Iterates over pages until exhaustion or non-retriable error.
        loop {
            let page_span = span_creator(&self.cluster_state);

            let page_res = await_in_span!(
                self.fetch_one_page(&routing_info, &page_span, &page_query),
                page_span.span()
            );

            let page_res = match page_res {
                Ok(RequestExecutionOutcome {
                    result: RunRequestResult::IgnoredWriteError,
                    coordinator: _,
                }) => {
                    warn!("Ignoring error during fetching pages; stopping fetching.");
                    return;
                }
                Ok(RequestExecutionOutcome {
                    result: RunRequestResult::Completed(page),
                    coordinator,
                }) => self.process_next_page(coordinator, &page_span, page),
                Err(err) => Err(err),
            };
            match page_res {
                Ok((page, should_fetch_more_pages)) => {
                    if sender.send(Ok(page)).await.is_err() {
                        // Channel was closed (pager dropped) - stop fetching pages.
                        return;
                    }
                    match should_fetch_more_pages {
                        ShouldFetchMorePages::NoMorePages => {
                            // Reached the last page, shutdown.
                            return;
                        }
                        ShouldFetchMorePages::MorePages => {}
                    }
                }
                Err(err) => {
                    let _ = sender.send(Err(NextPageError::RequestFailure(err))).await;
                    return;
                }
            }
        }
    }

    /// Fetches the first page on the caller task (no spawning).
    /// Returns the first page and whether more pages should be fetched.
    async fn query_first_page<QueryFunc, QueryFut>(
        &mut self,
        request_span: &RequestSpan,
        routing_info: &RoutingInfo<'_>,
        page_query: QueryFunc,
    ) -> Result<(FirstReceivedPage, ShouldFetchMorePages), RequestError>
    where
        QueryFunc: Fn(Arc<Connection>, Consistency, PagingState) -> QueryFut,
        QueryFut: Future<Output = Result<QueryResponse, RequestAttemptError>>,
    {
        let fetch_result = await_in_span!(
            self.fetch_one_page(routing_info, request_span, &page_query),
            request_span.span()
        );

        match fetch_result {
            Err(e) => Err(e),
            Ok(RequestExecutionOutcome {
                result,
                coordinator,
            }) => match result {
                RunRequestResult::IgnoredWriteError => {
                    warn!("Ignoring error during fetching pages; stopping fetching.");
                    Ok((
                        FirstReceivedPage {
                            content: FirstPageContent::Rows {
                                rows: DeserializedMetadataAndRawRows::mock_empty(),
                            },
                            tracing_id: None,
                            request_coordinator: coordinator,
                        },
                        ShouldFetchMorePages::NoMorePages,
                    ))
                }
                RunRequestResult::Completed(response) => self
                    .process_first_page(coordinator, request_span, response)
                    .map_err(RequestError::LastAttemptError),
            },
        }
    }

    /// Fetches exactly one page by delegating to [`run_request_no_side_effects`].
    ///
    /// `self.paging_state` is injected into each attempt.
    /// If `self.stable_coordinator` is set, it is tried first (and filtered out
    /// of the fresh load balancing plan so it is not attempted twice).
    async fn fetch_one_page<PageQuery, QueryFut>(
        &self,
        routing_info: &RoutingInfo<'_>,
        page_span: &RequestSpan,
        page_query: &PageQuery,
    ) -> Result<RequestExecutionOutcome<Coordinator>, RequestError>
    where
        PageQuery: Fn(Arc<Connection>, Consistency, PagingState) -> QueryFut,
        QueryFut: Future<Output = Result<QueryResponse, RequestAttemptError>>,
    {
        let exec_params = RequestExecutionParams {
            is_idempotent: self.is_idempotent,
            consistency: self.consistency,
            serial_consistency: self.serial_consistency,
            retry_policy: self.retry_policy.as_ref(),
            load_balancing_policy: self.load_balancing_policy.as_ref(),
            metrics_and_speculative_policy: Some((
                &self.metrics,
                self.speculative_policy.as_deref(),
            )),
            request_timeout: self.request_timeout,
            history_listener: self.history_listener.as_deref(),
            request_kind: RequestPaging::Automatic,
        };

        // Adapt the paging-aware `page_query` to the two-argument closure
        // expected by the execution core, injecting the current paging state
        // and normalizing error responses.
        let run_request_once = move |connection: Arc<Connection>, consistency: Consistency| async move {
            page_query(connection, consistency, self.paging_state.clone())
                .await
                .and_then(QueryResponse::into_non_error_query_response)
        };

        let load_balancing_policy = self.load_balancing_policy.as_ref();
        let base_plan =
            load_balancing::Plan::new(load_balancing_policy, routing_info, &self.cluster_state);

        // Coordinator stability: try the previous page's coordinator first.
        let stability_target = self.stable_coordinator.as_ref().map(|coordinator| {
            // `Coordinator` gets its shard from `Connection`. If `Connection` believes it has `Shard` `None`,
            // then it means that the connection pool to that connection is not sharded.
            // In such case, shard is ignored in `connection_for_shard()`.
            // We must provide a shard to `connection_for_shard()` anyway.
            // Therefore, let's pass an arbitrarily big shard to catch regressions in this logic earlier.
            (coordinator.node(), coordinator.shard().unwrap_or(2137))
        });

        let mut plan = stability_target
            .into_iter()
            .chain(base_plan.filter(|&(node, shard)| {
                // Filter out the preselected coordinator - it is tried first.
                self.stable_coordinator.as_ref().is_none_or(|coordinator| {
                    // If the previous attempt targeted an unsharded node (such as
                    // Cassandra), filter out all targets to that node regardless
                    // of shard.
                    !(Arc::ptr_eq(node, coordinator.node())
                        && coordinator
                            .shard()
                            .is_none_or(|last_shard| last_shard == shard))
                })
            }))
            .map(|(node, shard)| NodeAttemptTarget::new(node, shard));

        let request = Request {
            params: exec_params,
            routing_info: routing_info.clone(),
            run_once: run_request_once,
            span: page_span,
        };
        request.run_request_no_side_effects(&mut plan).await
    }

    fn process_first_page(
        &mut self,
        coordinator: Coordinator,
        request_span: &RequestSpan,
        query_response: NonErrorQueryResponse,
    ) -> Result<(FirstReceivedPage, ShouldFetchMorePages), RequestAttemptError> {
        match query_response {
            NonErrorQueryResponse {
                response:
                    NonErrorResponseWithDeserializedMetadata::Result(
                        result::ResultWithDeserializedMetadata::Rows((rows, paging_state_response)),
                    ),
                tracing_id,
                ..
            } => {
                request_span.record_raw_rows_fields(&rows);

                let should_fetch_more_pages = match paging_state_response {
                    PagingStateResponse::HasMorePages { state } => {
                        self.paging_state = state;
                        self.stable_coordinator = Some(coordinator.clone());
                        ShouldFetchMorePages::MorePages
                    }
                    PagingStateResponse::NoMorePages => ShouldFetchMorePages::NoMorePages,
                };
                Ok((
                    FirstReceivedPage {
                        content: FirstPageContent::Rows { rows },
                        tracing_id,
                        request_coordinator: coordinator,
                    },
                    should_fetch_more_pages,
                ))
            }
            NonErrorQueryResponse {
                response:
                    NonErrorResponseWithDeserializedMetadata::Result(
                        result::ResultWithDeserializedMetadata::SetKeyspace(set_keyspace),
                    ),
                tracing_id,
                ..
            } => Ok((
                FirstReceivedPage {
                    content: FirstPageContent::SetKeyspace { set_keyspace },
                    tracing_id,
                    request_coordinator: coordinator,
                },
                ShouldFetchMorePages::NoMorePages,
            )),
            NonErrorQueryResponse {
                response:
                    NonErrorResponseWithDeserializedMetadata::Result(
                        result::ResultWithDeserializedMetadata::SchemaChange(schema_change),
                    ),
                tracing_id,
                ..
            } => Ok((
                FirstReceivedPage {
                    content: FirstPageContent::SchemaChange { schema_change },
                    tracing_id,
                    request_coordinator: coordinator,
                },
                ShouldFetchMorePages::NoMorePages,
            )),
            NonErrorQueryResponse {
                response: NonErrorResponseWithDeserializedMetadata::Result(_),
                tracing_id,
                ..
            } => {
                // We have most probably sent a modification statement (e.g. INSERT or UPDATE),
                // so let's return an empty stream as suggested in #631.

                Ok((
                    FirstReceivedPage {
                        content: FirstPageContent::Rows {
                            rows: DeserializedMetadataAndRawRows::mock_empty(),
                        },
                        tracing_id,
                        request_coordinator: coordinator,
                    },
                    ShouldFetchMorePages::NoMorePages,
                ))
            }
            response => Err(RequestAttemptError::UnexpectedResponse(
                response.response.to_response_kind(),
            )),
        }
    }

    fn process_next_page(
        &mut self,
        coordinator: Coordinator,
        page_span: &RequestSpan,
        response: NonErrorQueryResponse,
    ) -> Result<(NextReceivedPage, ShouldFetchMorePages), RequestError> {
        let tracing_id = response.tracing_id;
        match response.response {
            NonErrorResponseWithDeserializedMetadata::Result(
                result::ResultWithDeserializedMetadata::Rows((rows, paging_state_response)),
            ) => {
                page_span.record_raw_rows_fields(&rows);

                let received_page = NextReceivedPage {
                    rows,
                    tracing_id,
                    request_coordinator: Some(coordinator.clone()),
                };
                let should_fetch_more_pages = match paging_state_response {
                    PagingStateResponse::HasMorePages { state } => {
                        self.paging_state = state;
                        self.stable_coordinator = Some(coordinator);
                        ShouldFetchMorePages::MorePages
                    }
                    PagingStateResponse::NoMorePages => ShouldFetchMorePages::NoMorePages,
                };

                Ok((received_page, should_fetch_more_pages))
            }
            // This catches all other kinds of responses that are not rows.
            // As this is not the first page, this is certainly an error.
            other => Err(RequestError::LastAttemptError(
                RequestAttemptError::UnexpectedResponse(other.to_response_kind()),
            )),
        }
    }
}

/// No-op LBP implementation, whose sole purpose is to enable plugging [SingleConnectionPagingExecutor]
/// into the unified execution core.
#[derive(Debug)]
struct NoopLBP;
impl LoadBalancingPolicy for NoopLBP {
    fn pick<'a>(
        &'a self,
        _request: &'a RoutingInfo,
        _cluster: &'a ClusterState,
    ) -> Option<(crate::cluster::NodeRef<'a>, Option<Shard>)> {
        None
    }

    fn fallback<'a>(
        &'a self,
        _request: &'a RoutingInfo,
        _cluster: &'a ClusterState,
    ) -> load_balancing::FallbackPlan<'a> {
        Box::new(std::iter::empty())
    }

    fn name(&self) -> String {
        "NoopLBP".into()
    }
}

/// Drives paged execution over a single fixed connection, used by
/// [`Connection::execute_iter`](crate::network::Connection::execute_iter).
///
/// Unlike [`PagingExecutor`], it has no load balancing, retries, speculative
/// execution, metrics or history: it simply fetches each page on the given
/// connection through the unified execution core, applying the client-side
/// timeout.
///
/// NOTE: This executor only supports executing SELECT statements. More
/// specifically, it expects that each response is of Rows kind. Other kinds
/// of responses will result in an error.
struct SingleConnectionPagingExecutor {
    connection: Arc<Connection>,
    prepared: PreparedStatement,
    values: SerializedValues,
    token: Option<Token>,
    page_size: PageSize,
    consistency: Consistency,
    serial_consistency: Option<SerialConsistency>,
    request_timeout: Option<Duration>,
}

impl SingleConnectionPagingExecutor {
    /// Fetches exactly one page from the fixed connection by delegating to
    /// [`run_request_no_side_effects`] with a single-connection plan.
    async fn fetch_one_page(
        &self,
        paging_state: &PagingState,
        request_span: &RequestSpan,
    ) -> Result<RequestExecutionOutcome<()>, RequestError> {
        // The control-connection-style path never retries: a fallthrough retry
        // policy makes every attempt terminal.
        let retry_policy = FallthroughRetryPolicy::new();
        let exec_params = RequestExecutionParams {
            is_idempotent: self.prepared.get_is_idempotent(),
            consistency: self.consistency,
            serial_consistency: self.serial_consistency,
            retry_policy: &retry_policy,
            load_balancing_policy: &NoopLBP,
            metrics_and_speculative_policy: None,
            request_timeout: self.request_timeout,
            history_listener: None,
            request_kind: RequestPaging::Automatic,
        };

        let routing_info = RoutingInfo {
            consistency: self.consistency,
            serial_consistency: self.serial_consistency,
            token: self.token,
            table: self.prepared.get_table_spec(),
            is_confirmed_lwt: self.prepared.is_confirmed_lwt(),
            node_location_preference: &NodeLocationPreference::Any,
        };

        let run_request_once = |connection: Arc<Connection>, consistency: Consistency| {
            let paging_state = paging_state.clone();
            async move {
                connection
                    .execute_raw_with_consistency(
                        &self.prepared,
                        &self.values,
                        consistency,
                        self.serial_consistency,
                        Some(self.page_size),
                        paging_state,
                        // This API executes on one given connection and has no `ClusterState`
                        // to consult, so there is no cached tablet version to probe.
                        0,
                    )
                    .await
                    .and_then(QueryResponse::into_non_error_query_response)
            }
        };

        let mut plan = std::iter::once(SingleConnectionTarget::new(Arc::clone(&self.connection)));

        let request = Request {
            params: exec_params,
            routing_info,
            run_once: run_request_once,
            span: request_span,
        };
        request.run_request_no_side_effects(&mut plan).await
    }

    /// Interprets a page outcome. Only Rows responses are valid here.
    fn page_from_outcome(
        outcome: RequestExecutionOutcome<()>,
    ) -> Result<(NextReceivedPage, PagingStateResponse), NextPageError> {
        let response = match outcome.result {
            RunRequestResult::IgnoredWriteError => {
                unreachable!(
                    "FallthroughRetryPolicy is used, so errors are always propagated and never ignored."
                )
            }
            RunRequestResult::Completed(response) => response,
        };

        let tracing_id = response.tracing_id;
        match response.response {
            NonErrorResponseWithDeserializedMetadata::Result(
                result::ResultWithDeserializedMetadata::Rows((rows, paging_state_response)),
            ) => Ok((
                NextReceivedPage {
                    rows,
                    tracing_id,
                    request_coordinator: None,
                },
                paging_state_response,
            )),
            other => Err(NextPageError::RequestFailure(
                RequestError::LastAttemptError(RequestAttemptError::UnexpectedResponse(
                    other.to_response_kind(),
                )),
            )),
        }
    }

    fn make_span(&self) -> RequestSpan {
        RequestSpan::new_cc_query(self.prepared.get_statement(), self.values.buffer_size())
    }

    /// Fetches remaining pages (pages 2+) and sends them through the channel.
    async fn fetch_remaining_pages(
        self,
        mut paging_state: PagingState,
        sender: SlotSender<ResultNextPage>,
    ) {
        loop {
            let request_span = self.make_span();
            let outcome = await_in_span!(
                self.fetch_one_page(&paging_state, &request_span),
                request_span.span()
            );

            let outcome = match outcome {
                Ok(outcome) => outcome,
                Err(error) => {
                    let _ = sender.send(Err(NextPageError::RequestFailure(error))).await;
                    return;
                }
            };

            let (page, paging_state_response) = match Self::page_from_outcome(outcome) {
                Ok(page) => page,
                Err(error) => {
                    let _ = sender.send(Err(error)).await;
                    return;
                }
            };

            if sender.send(Ok(page)).await.is_err() {
                // Channel was closed, QueryPager was dropped - should shutdown.
                return;
            }

            match paging_state_response.into_paging_control_flow() {
                ControlFlow::Continue(new_paging_state) => {
                    paging_state = new_paging_state;
                }
                ControlFlow::Break(()) => {
                    // Reached the last query, shutdown
                    return;
                }
            }
        }
    }
}

/// A channel holding a single item, through which a pager's worker hands
/// pages over to the [`QueryPager`].
///
/// It behaves like a tokio `mpsc` channel of capacity 1, which would allocate
/// about 15 KB here: tokio allocates `mpsc` slots in blocks of 32.
mod slot_channel {
    use std::fmt;
    use std::sync::{Arc, Mutex};
    use std::task::{Context, Poll, Waker};

    pub(super) fn slot_channel<T>() -> (SlotSender<T>, SlotReceiver<T>) {
        let shared = Arc::new(Mutex::new(SlotState {
            item: None,
            sender_waker: None,
            receiver_waker: None,
            sender_dropped: false,
            receiver_dropped: false,
        }));
        (
            SlotSender {
                shared: Arc::clone(&shared),
            },
            SlotReceiver { shared },
        )
    }

    struct SlotState<T> {
        item: Option<T>,
        /// Woken when the item is taken, or the receiver is dropped.
        sender_waker: Option<Waker>,
        /// Woken when an item is put, or the sender is dropped.
        receiver_waker: Option<Waker>,
        sender_dropped: bool,
        receiver_dropped: bool,
    }

    fn register(waker: &mut Option<Waker>, cx: &Context<'_>) {
        match waker {
            Some(waker) => waker.clone_from(cx.waker()),
            None => *waker = Some(cx.waker().clone()),
        }
    }

    /// The receiver was dropped, so no more items can be sent.
    #[derive(Debug)]
    pub(super) struct ReceiverDropped;

    pub(super) struct SlotSender<T> {
        shared: Arc<Mutex<SlotState<T>>>,
    }

    impl<T> SlotSender<T> {
        /// Waits until the slot is empty, and puts `item` in it.
        pub(super) async fn send(&self, item: T) -> Result<(), ReceiverDropped> {
            // `poll_fn` calls the closure on every poll, so the closure can't move
            // `item` out. It takes it from the `Option` on the poll that succeeds.
            let mut item = Some(item);
            std::future::poll_fn(|cx| {
                let receiver_waker = {
                    let mut state = self.shared.lock().unwrap();
                    if state.receiver_dropped {
                        return Poll::Ready(Err(ReceiverDropped));
                    }
                    if state.item.is_some() {
                        register(&mut state.sender_waker, cx);
                        return Poll::Pending;
                    }
                    state.item = item.take();
                    state.receiver_waker.take()
                };
                if let Some(waker) = receiver_waker {
                    waker.wake();
                }
                Poll::Ready(Ok(()))
            })
            .await
        }
    }

    impl<T> Drop for SlotSender<T> {
        fn drop(&mut self) {
            let receiver_waker = {
                let mut state = self.shared.lock().unwrap();
                state.sender_dropped = true;
                state.receiver_waker.take()
            };
            if let Some(waker) = receiver_waker {
                waker.wake();
            }
        }
    }

    pub(super) struct SlotReceiver<T> {
        shared: Arc<Mutex<SlotState<T>>>,
    }

    impl<T> SlotReceiver<T> {
        /// Takes the item from the slot. Returns `None` once the sender is dropped
        /// and the slot is empty.
        pub(super) fn poll_recv(&mut self, cx: &Context<'_>) -> Poll<Option<T>> {
            let (item, sender_waker) = {
                let mut state = self.shared.lock().unwrap();
                let Some(item) = state.item.take() else {
                    if state.sender_dropped {
                        return Poll::Ready(None);
                    }
                    register(&mut state.receiver_waker, cx);
                    return Poll::Pending;
                };
                (item, state.sender_waker.take())
            };
            if let Some(waker) = sender_waker {
                waker.wake();
            }
            Poll::Ready(Some(item))
        }
    }

    impl<T> Drop for SlotReceiver<T> {
        fn drop(&mut self) {
            let sender_waker = {
                let mut state = self.shared.lock().unwrap();
                state.receiver_dropped = true;
                state.sender_waker.take()
            };
            if let Some(waker) = sender_waker {
                waker.wake();
            }
        }
    }

    impl<T> fmt::Debug for SlotReceiver<T> {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.debug_struct("SlotReceiver").finish_non_exhaustive()
        }
    }

    #[cfg(test)]
    mod tests {
        use std::future::poll_fn;
        use std::sync::Arc;
        use std::sync::atomic::{AtomicBool, Ordering};
        use std::task::{Context, Poll, Wake, Waker};

        use assert_matches::assert_matches;

        use super::{ReceiverDropped, slot_channel};

        /// A waker that records whether it was woken.
        #[derive(Default)]
        struct WakeFlag(AtomicBool);

        impl WakeFlag {
            fn is_woken(&self) -> bool {
                self.0.load(Ordering::Relaxed)
            }
        }

        impl Wake for WakeFlag {
            fn wake(self: Arc<Self>) {
                self.0.store(true, Ordering::Relaxed);
            }
        }

        #[tokio::test]
        async fn slot_channel_hands_over_items_one_at_a_time() {
            let (sender, mut receiver) = slot_channel();
            sender.send(1).await.unwrap();

            // The slot is full, so the next send waits until the item is taken.
            let mut second = Box::pin(sender.send(2));
            assert!(futures::poll!(&mut second).is_pending());
            assert_eq!(poll_fn(|cx| receiver.poll_recv(cx)).await, Some(1));
            second.await.unwrap();
            assert_eq!(poll_fn(|cx| receiver.poll_recv(cx)).await, Some(2));

            // The item left in the slot is still received after the sender is dropped.
            sender.send(3).await.unwrap();
            drop(sender);
            assert_eq!(poll_fn(|cx| receiver.poll_recv(cx)).await, Some(3));
            assert_eq!(poll_fn(|cx| receiver.poll_recv(cx)).await, None);
        }

        #[tokio::test]
        async fn slot_channel_send_fails_after_receiver_is_dropped() {
            let (sender, receiver) = slot_channel();
            sender.send(1).await.unwrap();
            let flag = Arc::new(WakeFlag::default());
            let waker = Waker::from(Arc::clone(&flag));
            let mut second = Box::pin(sender.send(2));
            assert!(
                second
                    .as_mut()
                    .poll(&mut Context::from_waker(&waker))
                    .is_pending()
            );

            drop(receiver);
            assert!(flag.is_woken());
            assert_matches!(second.await, Err(ReceiverDropped));
            assert_matches!(sender.send(3).await, Err(ReceiverDropped));
        }

        #[test]
        fn slot_channel_receiver_is_woken_when_sender_is_dropped() {
            let (sender, mut receiver) = slot_channel::<i32>();
            let flag = Arc::new(WakeFlag::default());
            let waker = Waker::from(Arc::clone(&flag));
            let cx = Context::from_waker(&waker);
            assert!(receiver.poll_recv(&cx).is_pending());

            drop(sender);
            assert!(flag.is_woken());
            assert_eq!(receiver.poll_recv(&cx), Poll::Ready(None));
        }
    }
}

/// Returns the list of page coordinators, holding the first page's one.
///
/// When more pages follow, room for several is reserved, to spare most of
/// the reallocations of a list growing by one coordinator per page.
fn first_page_coordinators(first: Option<Coordinator>, more_pages: bool) -> Vec<Coordinator> {
    const MULTI_PAGE_INITIAL_CAPACITY: usize = 8;

    let capacity = if more_pages {
        MULTI_PAGE_INITIAL_CAPACITY
    } else {
        usize::from(first.is_some())
    };
    let mut coordinators = Vec::with_capacity(capacity);
    coordinators.extend(first);
    coordinators
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
    // `None` if the first page was the only one.
    page_receiver: Option<SlotReceiver<ResultNextPage>>,
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

        Some(
            self.current_page
                .next()
                .unwrap()
                .map_err(NextRowError::RowDeserializationError)
                .map(|x| (x, fresh_page)),
        )
    }

    /// This is essentially the same as `next()`, but it is *public* and only available
    /// when the `unstable-csharp-rs` feature is enabled.
    /// Rationale: I don't know a way to make a function conditionally public,
    /// based on a compile-time flag, so I added an new wrapper function
    /// that is conditionally compiled and is public.
    #[cfg(all(scylla_unstable, feature = "unstable-csharp-rs"))]
    #[inline]
    pub async fn next_column_iterator(
        &mut self,
    ) -> Option<Result<(ColumnIterator<'_, '_>, bool), NextRowError>> {
        self.next().await
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

        let Some(page_receiver) = s.page_receiver.as_mut() else {
            return Poll::Ready(None);
        };
        let received_page = ready_some_ok!(page_receiver.poll_recv(cx));

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

    /// Casts the pager's stream to a given row type, enabling [Stream]'ed operations
    /// on rows, which deserialize them on-the-fly to that given type.
    /// It only allows deserializing owned types, because [Stream] is not lending.
    /// Begins with performing type check.
    #[inline]
    pub fn rows_stream<RowT: for<'frame, 'metadata> DeserializeRow<'frame, 'metadata>>(
        self,
    ) -> Result<TypedRowStream<RowT>, TypeCheckError> {
        TypedRowStream::<RowT>::new(self)
    }

    pub(crate) async fn new_for_unprepared_statement_without_values(
        session: &Session,
        statement: Statement,
    ) -> Result<Self, PagerExecutionError> {
        let mut executor = PagingExecutor::new(session, &statement.config);
        let consistency = executor.consistency;
        let serial_consistency = executor.serial_consistency;

        fn create_span(statement_contents: &str) -> RequestSpan {
            let span = RequestSpan::new_query(statement_contents);
            span.record_request_size(0);
            span
        }

        let page_size = statement.get_validated_page_size();
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

        let (first_page, should_fetch_more_pages) = {
            let routing_info = RoutingInfo {
                consistency,
                serial_consistency,
                token: None,
                table: None,
                is_confirmed_lwt: false,
                node_location_preference: session.get_node_location_preference(),
            };

            let request_span = create_span(&statement.contents);

            executor
                .query_first_page(&request_span, &routing_info, page_query)
                .await
                .map_err(NextPageError::RequestFailure)?
        };

        /* PROCESS FIRST PAGE */
        let receiver = match should_fetch_more_pages {
            // No more pages - we are done, return just the first page. The channel
            // is only created when needed, as many results fit in a single page.
            ShouldFetchMorePages::NoMorePages => None,
            ShouldFetchMorePages::MorePages => {
                /* REMAINING PAGES */
                let (sender, receiver) = slot_channel::<ResultNextPage>();
                let node_location_preference = Arc::clone(session.get_node_location_preference());
                let worker_task = async move {
                    let routing_info = RoutingInfo {
                        consistency,
                        serial_consistency,
                        token: None,
                        table: None,
                        is_confirmed_lwt: false,
                        node_location_preference: &node_location_preference,
                    };

                    let statement_ref = &statement;
                    let page_query =
                        |connection: Arc<Connection>,
                         consistency: Consistency,
                         paging_state: PagingState| async move {
                            connection
                                .query_raw_with_consistency(
                                    statement_ref,
                                    consistency,
                                    serial_consistency,
                                    Some(page_size),
                                    paging_state,
                                )
                                .await
                        };

                    let span_creator = move |_: &ClusterState| create_span(&statement_ref.contents);

                    executor
                        .query_remaining_pages(sender, page_query, routing_info, span_creator)
                        .await;
                };
                let _worker_handle = tokio::task::spawn(worker_task);
                Some(receiver)
            }
        };

        Self::new_from_first_page(first_page, receiver, session).await
    }

    /// Executes a prepared statement and fetches its first page.
    ///
    /// The values are not checked to match the statement, so this must not be
    /// exposed to end users. It isn't generic, to reduce code bloat.
    pub(crate) async fn new_for_prepared_statement(
        session: &Session,
        prepared: PreparedStatement,
        values: SerializedValues,
    ) -> Result<Self, PagerExecutionError> {
        let mut executor = PagingExecutor::new(session, &prepared.config);

        let consistency = executor.consistency;
        let serial_consistency = executor.serial_consistency;

        // Computed only if some span is enabled, so allocating doesn't matter, while
        // a `SmallVec` would make the pager's futures bigger.
        type Replicas = Vec<(Arc<Node>, Shard)>;

        fn create_span(
            partition_key: &Option<PartitionKey>,
            token: Option<Token>,
            serialized_values_size: usize,
            cluster_state: &ClusterState,
            table_spec: Option<&TableSpec<'_>>,
            replicas: &OnceCell<Option<Replicas>>,
        ) -> RequestSpan {
            let span = RequestSpan::new_prepared(
                partition_key.as_ref().map(|pk| pk.iter()),
                token,
                serialized_values_size,
            );
            if !span.span().is_disabled()
                && let (Some(table_spec), Some(token)) = (table_spec, token)
            {
                // Replicas are costly to compute, so we only do it if some page's span is
                // enabled, and then at most once for the whole paged request: `cluster_state`
                // is a fixed snapshot, so the replica set cannot change between pages.
                let replicas = replicas.get_or_init(|| {
                    cluster_state
                        .get_token_endpoints_iter(table_spec, token)
                        .ok()
                        .map(|replicas| {
                            replicas
                                .map(|(node, shard)| (node.clone(), shard))
                                .collect()
                        })
                });
                if let Some(replicas) = replicas {
                    span.record_replicas(replicas.iter().map(|(node, shard)| (node, *shard)));
                }
            }
            span
        }

        let serialized_values_size = values.buffer_size();
        let table_spec = prepared.get_table_spec();

        // Shared by the first page and every remaining page, so that the replica set is
        // computed at most once - and only if at least one of those spans is enabled.
        let replicas: OnceCell<Option<Replicas>> = OnceCell::new();

        // Scoped, so that the partition key, needed only for the span, is not kept
        // in this future's state while the first page is fetched.
        let (token, request_span) = {
            let (partition_key, token) =
                match prepared.extract_partition_key_and_calculate_token(&values) {
                    Ok(res) => res.unzip(),
                    Err(err) => {
                        return Err(PagerExecutionError::NextPageError(
                            NextPageError::PartitionKeyError(err),
                        ));
                    }
                };
            let request_span = create_span(
                &partition_key,
                token,
                serialized_values_size,
                &executor.cluster_state,
                table_spec,
                &replicas,
            );
            (token, request_span)
        };

        let routing_info = RoutingInfo {
            consistency,
            serial_consistency,
            token,
            table: table_spec,
            is_confirmed_lwt: prepared.is_confirmed_lwt(),
            node_location_preference: session.get_node_location_preference(),
        };

        let page_size = prepared.get_validated_page_size();
        let prepared_ref = &prepared;
        let values_ref = &values;
        // Chosen once for the whole paged request: computed here for the eager fetch of the
        // first page and reused as-is for every remaining page.
        let tablet_block_hint =
            choose_tablet_block_hint(&executor.cluster_state, table_spec, token);
        let page_query = move |connection: Arc<Connection>,
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
                    tablet_block_hint,
                )
                .await
        };

        let (first_page, should_fetch_more_pages) = executor
            .query_first_page(&request_span, &routing_info, page_query)
            .await
            .map_err(NextPageError::RequestFailure)?;

        /* PROCESS FIRST PAGE */
        let receiver = match should_fetch_more_pages {
            // No more pages - we are done, return just the first page. The channel
            // is only created when needed, as many results fit in a single page.
            ShouldFetchMorePages::NoMorePages => None,
            ShouldFetchMorePages::MorePages => {
                /* REMAINING PAGES */
                let (sender, receiver) = slot_channel::<ResultNextPage>();
                let node_location_preference = Arc::clone(session.get_node_location_preference());
                let worker_task = async move {
                    let partition_key = if prepared.is_token_aware() {
                        match prepared.extract_partition_key(&values) {
                            Ok(res) => Some(res),
                            Err(err) => {
                                let _ = sender
                                    .send(Err(NextPageError::PartitionKeyError(
                                        PartitionKeyError::PartitionKeyExtraction(err),
                                    )))
                                    .await;
                                return;
                            }
                        }
                    } else {
                        None
                    };

                    let table_spec = prepared.get_table_spec();
                    let routing_info = RoutingInfo {
                        consistency,
                        serial_consistency,
                        token,
                        table: table_spec,
                        is_confirmed_lwt: prepared.is_confirmed_lwt(),
                        node_location_preference: &node_location_preference,
                    };

                    let prepared = &prepared;
                    let values_ref = &values;
                    let page_query =
                        move |connection: Arc<Connection>,
                              consistency: Consistency,
                              paging_state: PagingState| async move {
                            connection
                                .execute_raw_with_consistency(
                                    prepared,
                                    values_ref,
                                    consistency,
                                    serial_consistency,
                                    Some(page_size),
                                    paging_state,
                                    tablet_block_hint,
                                )
                                .await
                        };

                    let span_creator = move |cluster_state: &ClusterState| {
                        create_span(
                            &partition_key,
                            token,
                            serialized_values_size,
                            cluster_state,
                            table_spec,
                            &replicas,
                        )
                    };

                    executor
                        .query_remaining_pages(sender, page_query, routing_info, span_creator)
                        .await;
                };
                let _worker_handle = tokio::task::spawn(worker_task);
                Some(receiver)
            }
        };

        Self::new_from_first_page(first_page, receiver, session).await
    }

    pub(crate) async fn new_for_connection_execute_iter(
        prepared: PreparedStatement,
        values: SerializedValues,
        connection: Arc<Connection>,
        consistency: Consistency,
        serial_consistency: Option<SerialConsistency>,
    ) -> Result<Self, NextPageError> {
        let page_size = prepared.get_validated_page_size();
        let request_timeout = prepared.get_request_timeout().or_else(|| {
            prepared
                .get_execution_profile_handle()?
                .access()
                .request_timeout
        });

        let (_, token) = match prepared.extract_partition_key_and_calculate_token(&values) {
            Ok(res) => res.unzip(),
            Err(err) => {
                return Err(NextPageError::PartitionKeyError(err));
            }
        };

        let executor = SingleConnectionPagingExecutor {
            connection,
            prepared,
            values,
            token,
            page_size,
            consistency,
            serial_consistency,
            request_timeout,
        };

        // Fetch the first page on the caller task (no spawning).
        let request_span = executor.make_span();
        let outcome = await_in_span!(
            executor.fetch_one_page(&PagingState::start(), &request_span),
            request_span.span()
        )
        .map_err(NextPageError::RequestFailure)?;
        let (first_page, paging_state_response) =
            SingleConnectionPagingExecutor::page_from_outcome(outcome)?;

        /* PROCESS FIRST PAGE */
        let receiver = match paging_state_response {
            // No more pages - we are done, return just the first page. The channel
            // is only created when needed, as many results fit in a single page.
            PagingStateResponse::NoMorePages => None,
            PagingStateResponse::HasMorePages { state } => {
                /* REMAINING PAGES */
                let (sender, receiver) = slot_channel::<ResultNextPage>();
                let parent_span = tracing::Span::current();
                let worker_task =
                    async move { executor.fetch_remaining_pages(state, sender).await }
                        .instrument(parent_span);
                let _worker_handle = tokio::task::spawn(worker_task);
                Some(receiver)
            }
        };

        let NextReceivedPage {
            rows,
            tracing_id,
            request_coordinator,
        } = first_page;

        Ok(Self {
            current_page: RawRowLendingIterator::new(rows),
            request_coordinators: first_page_coordinators(request_coordinator, receiver.is_some()),
            page_receiver: receiver,
            tracing_ids: Vec::from_iter(tracing_id),
        })
    }

    async fn new_from_first_page(
        first_page: FirstReceivedPage,
        receiver: Option<SlotReceiver<ResultNextPage>>,
        session: &Session,
    ) -> Result<Self, PagerExecutionError> {
        let tracing_ids = Vec::from_iter(first_page.tracing_id);
        let coordinator_id = first_page.request_coordinator.node().host_id;
        let request_coordinators =
            first_page_coordinators(Some(first_page.request_coordinator), receiver.is_some());

        let first_page = match first_page.content {
            FirstPageContent::Rows { rows } => RawRowLendingIterator::new(rows),
            FirstPageContent::SetKeyspace { set_keyspace } => {
                // If we are here, this means that we received a SET_KEYSPACE response as a first page.
                // This can happen when the user executes a "USE <keyspace>" statement.
                // Although it makes little sense to page over such a statement,
                // we must handle it gracefully. Especially that there may be users who execute
                // all statements in a paged manner (e.g., C# RS Driver).
                //
                // Let's set the keyspace on the session.
                let response = NonErrorQueryResponse {
                    response: NonErrorResponseWithDeserializedMetadata::Result(
                        result::ResultWithDeserializedMetadata::SetKeyspace(set_keyspace),
                    ),
                    tracing_id: None,
                    warnings: Box::default(),
                };
                // Rare, so boxed to keep it out of every pager's future.
                Box::pin(session.handle_set_keyspace_response(&response)).await?;
                // The stream will be empty.
                RawRowLendingIterator::new(DeserializedMetadataAndRawRows::mock_empty())
            }
            FirstPageContent::SchemaChange { schema_change } => {
                // If we are here, this means that we received a SCHEMA_CHANGE response as a first page.
                // This can happen when the user executes a DDL statement.
                // Although it makes little sense to page over such a statement,
                // we must handle it gracefully. Especially that there may be users who execute
                // all statements in a paged manner (e.g., C#-RS Driver).
                //
                // Let's await schema agreement, if Session is configured to do so.
                let response = NonErrorQueryResponse {
                    response: NonErrorResponseWithDeserializedMetadata::Result(
                        result::ResultWithDeserializedMetadata::SchemaChange(schema_change),
                    ),
                    tracing_id: None,
                    warnings: Box::default(),
                };
                // Rare, so boxed to keep it out of every pager's future.
                Box::pin(session.handle_auto_await_schema_agreement(&response, coordinator_id))
                    .await?;
                // The stream will be empty.
                RawRowLendingIterator::new(DeserializedMetadataAndRawRows::mock_empty())
            }
        };

        Ok(Self {
            current_page: first_page,
            page_receiver: receiver,
            tracing_ids,
            request_coordinators,
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

/// An error returned by async pager API.
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
