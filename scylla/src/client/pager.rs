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

use crate::deserialize::result::RawRowLendingIterator;
use crate::deserialize::row::{ColumnIterator, DeserializeRow};
use crate::deserialize::{DeserializationError, TypeCheckError};
use crate::frame::frame_errors::ResultMetadataAndRowsCountParseError;
use crate::frame::request::query::{PagingState, PagingStateResponse};
use crate::frame::response::NonErrorResponseWithDeserializedMetadataV2 as NonErrorResponseWithDeserializedMetadata;
use crate::frame::response::result::{DeserializedMetadataAndRawRows, SchemaChange, SetKeyspace};
use crate::frame::types::{Consistency, SerialConsistency};
use crate::serialize::row::SerializedValues;
use futures::Stream;
use std::result::Result;
use thiserror::Error;
use tokio::sync::mpsc;

use crate::client::execution_profile::ExecutionProfileInner;
use crate::client::session::Session;
use crate::cluster::{ClusterState, NodeRef};
use crate::deserialize::DeserializeOwnedRow;
use crate::errors::{PagerExecutionError, RequestAttemptError, RequestError};
use crate::frame::response::result;
use crate::network::Connection;
use crate::observability::driver_tracing::RequestSpan;
use crate::observability::history::{self, HistoryListener};
use crate::observability::metrics::Metrics;
use crate::policies::load_balancing::{self, LoadBalancingPolicy, RoutingInfo};
use crate::policies::retry::{RequestInfo, RetryDecision, RetrySession};
use crate::response::query_result::ColumnSpecs;
use crate::response::{Coordinator, NonErrorQueryResponse, QueryResponse};
use crate::routing::{NodeLocationPreference, Shard};
use crate::statement::prepared::{PartitionKey, PartitionKeyError, PreparedStatement};
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

enum ShouldFetchMorePages {
    NoMorePages,
    MorePages {
        first_page_coordinator: Coordinator,
        first_page_connection: Arc<Connection>,
    },
}

// PagerWorker works in the background to fetch pages
// QueryPager receives them through a channel
struct PagerWorker<SpanCreatorFunc> {
    load_balancing_policy: Arc<dyn LoadBalancingPolicy>,
    query_is_idempotent: bool,
    query_consistency: Consistency,
    retry_session: Box<dyn RetrySession>,
    timeouter: Option<PageQueryTimeouter>,
    metrics: Metrics,

    paging_state: PagingState,

    history_listener: Option<Arc<dyn HistoryListener>>,
    current_request_id: Option<history::RequestId>,
    current_attempt_id: Option<history::AttemptId>,

    parent_span: tracing::Span,
    span_creator: SpanCreatorFunc,
}

impl<SpanCreator> PagerWorker<SpanCreator>
where
    SpanCreator: Fn(&Option<PartitionKey<'_>>) -> RequestSpan,
{
    /// Fetches remaining pages (pages 2+) in a background task.
    /// Sends each page through the mpsc channel.
    ///
    /// A fresh query plan is constructed for each page, preventing plan
    /// exhaustion for long-running multi-page queries.
    ///
    /// The last successful coordinator is preferred for the next page
    /// (coordinator stability), but all other nodes remain available
    /// for retry.
    #[expect(clippy::too_many_arguments)]
    async fn work<QueryFunc, QueryFut>(
        mut self,
        cluster_state: Arc<ClusterState>,
        sender: mpsc::Sender<ResultNextPage>,
        page_query: QueryFunc,
        routing_info: RoutingInfo<'_>,
        partition_key: Option<PartitionKey<'_>>,
        mut last_successful_page_coordinator: Coordinator,
        mut last_successful_page_conn: Arc<Connection>,
    ) where
        QueryFunc: Fn(Arc<Connection>, Consistency, PagingState) -> QueryFut,
        QueryFut: Future<Output = Result<QueryResponse, RequestAttemptError>>,
    {
        let mut last_error: RequestError = RequestError::EmptyPlan;
        let mut current_consistency: Consistency = self.query_consistency;
        let load_balancer = Arc::clone(&self.load_balancing_policy);

        // Iterates over pages until exhaustion or non-retriable error.
        'paging: loop {
            let query_plan =
                load_balancing::Plan::new(load_balancer.as_ref(), &routing_info, &cluster_state);

            self.timeouter.as_mut().map(PageQueryTimeouter::reset);

            let parent_span = self.parent_span.clone();
            let create_execution_span = |node: NodeRef, shard: Option<Shard>| trace_span!(parent: &parent_span, "Executing query", node = %node.address, shard = ?shard);

            // Prefix the plan with the last successful coordinator (coordinator
            // stability), then chain the remaining nodes from the fresh plan.
            let query_plan = std::iter::once((
                last_successful_page_coordinator.node(),
                last_successful_page_coordinator.shard(),
                create_execution_span(
                    last_successful_page_coordinator.node(),
                    last_successful_page_coordinator.shard(),
                ),
                itertools::Either::Left(std::future::ready(Ok(last_successful_page_conn))),
            ))
            .chain(
                query_plan
                    // We filter out the last successful page coordinator, because it's tried out first.
                    .filter(|&(node, shard)| {
                        !(Arc::ptr_eq(node, last_successful_page_coordinator.node())
                            && last_successful_page_coordinator
                                .shard()
                                // If we targeted an unsharded node (such as Cassandra) previously,
                                // we should now repeat it as unsharded and at the same time filter out
                                // all targets to that node, with any shard.
                                .is_none_or(|last_shard| last_shard == shard))
                    })
                    .map(|(node, shard)| {
                        let span = create_execution_span(node, Some(shard));
                        // For each node in the plan choose a connection to use.
                        // This connection will be reused for same node retries to preserve paging cache on the shard.
                        let connection_fut = itertools::Either::Right(
                            node.connection_for_shard(shard).instrument(span.clone()),
                        );
                        (node, Some(shard), span, connection_fut)
                    }),
            );

            // Iterates over nodes in the query plan, trying to fetch the next page.
            'nodes_in_plan: for (node, shard, all_pages_span, connection_fut) in query_plan {
                // For each node in the plan choose a connection to use
                // This connection will be reused for same node retries to preserve paging cache on the shard
                let connection: Arc<Connection> = match connection_fut.await {
                    Ok(connection) => connection,
                    Err(e) => {
                        trace!(
                            parent: &all_pages_span,
                            error = %e,
                            "Choosing connection failed"
                        );
                        last_error = e.into();
                        // Broken connection doesn't count as a failed query, don't log in metrics.
                        continue 'nodes_in_plan;
                    }
                };

                let coordinator = Coordinator::new(node, node.sharder().and(shard), &connection);

                // Retries on the same node as long as RetrySession decides so.
                'same_node_retries: loop {
                    trace!(parent: &all_pages_span, "Execution started");

                    let request_span = (self.span_creator)(&partition_key);

                    let fetch_result = self
                        .fetch_one_page(
                            &connection,
                            current_consistency,
                            &request_span,
                            &page_query,
                        )
                        .instrument(request_span.span().clone())
                        .await;
                    let query_result = match fetch_result {
                        Err(RequestTimeoutError(timeout)) => {
                            last_error = RequestError::RequestTimeout(timeout);
                            trace!(
                                parent: &all_pages_span,
                                error = %last_error,
                                "Request timed out"
                            );
                            // This means that we failed all attempts - in this case, due to a timeout.
                            break 'nodes_in_plan;
                        }
                        Ok((elapsed, result)) => {
                            self.process_next_page(
                                &routing_info,
                                node,
                                coordinator.clone(),
                                &request_span,
                                &sender,
                                elapsed,
                                result,
                            )
                            .await
                        }
                    };

                    let request_error: RequestAttemptError = match query_result {
                        Ok(ControlFlow::Break(())) => {
                            // Successfully queried the last remaining page.
                            trace!(parent: &all_pages_span, "Request succeeded");
                            return;
                        }
                        Ok(ControlFlow::Continue(())) => {
                            // Successfully queried one page, and there are more to fetch.
                            // Reset the timeout_instant for the next page fetch.
                            self.timeouter.as_mut().map(PageQueryTimeouter::reset);

                            // Prioritize the successful coordinator for the next page fetch.
                            last_successful_page_coordinator = coordinator;
                            last_successful_page_conn = connection;

                            // Continue with a fresh plan for the next page.
                            continue 'paging;
                        }
                        Err(error) => {
                            trace!(
                                parent: &all_pages_span,
                                error = %error,
                                "Request failed"
                            );
                            error
                        }
                    };

                    let request_info = RequestInfo {
                        error: &request_error,
                        is_idempotent: self.query_is_idempotent,
                        consistency: current_consistency,
                    };

                    let retry_decision = self.retry_session.decide_should_retry(request_info);
                    trace!(
                        parent: &all_pages_span,
                        retry_decision = ?retry_decision
                    );

                    self.log_attempt_error(&request_error, &retry_decision);

                    last_error = request_error.into();

                    match retry_decision {
                        RetryDecision::RetrySameTarget(cl) => {
                            self.metrics.inc_retries_num();
                            current_consistency = cl.unwrap_or(current_consistency);
                            continue 'same_node_retries;
                        }
                        RetryDecision::RetryNextTarget(cl) => {
                            self.metrics.inc_retries_num();
                            current_consistency = cl.unwrap_or(current_consistency);
                            continue 'nodes_in_plan;
                        }
                        RetryDecision::DontRetry => break 'nodes_in_plan,
                        RetryDecision::IgnoreWriteError => {
                            self.log_request_success();
                            self.retry_session.reset();

                            warn!("Ignoring error during fetching pages; stopping fetching.");
                            return;
                        }
                    };
                }
            }

            // If we are here, it means we failed to fetch next page on any node from the plan.
            // The plan is exhausted, so we send the last error and finish.
            self.log_request_error(&last_error);
            let _ = sender
                .send(Err(NextPageError::RequestFailure(last_error)))
                .await;
            return;
        }
    }

    /// Fetches the first page on the caller task (no spawning).
    /// Returns the first page and whether more pages should be fetched.
    async fn query_first_page<QueryFunc, QueryFut>(
        &mut self,
        cluster_state: &ClusterState,
        partition_key: &Option<PartitionKey<'_>>,
        routing_info: &RoutingInfo<'_>,
        page_query: QueryFunc,
    ) -> Result<(FirstReceivedPage, ShouldFetchMorePages), NextPageError>
    where
        QueryFunc: Fn(Arc<Connection>, Consistency, PagingState) -> QueryFut,
        QueryFut: Future<Output = Result<QueryResponse, RequestAttemptError>>,
    {
        let load_balancer = Arc::clone(&self.load_balancing_policy);
        let query_plan =
            load_balancing::Plan::new(load_balancer.as_ref(), routing_info, cluster_state);

        let mut last_error: RequestError = RequestError::EmptyPlan;
        let mut current_consistency: Consistency = self.query_consistency;

        self.log_request_start();
        self.timeouter.as_mut().map(PageQueryTimeouter::reset);

        // Iterates over nodes in the query plan, trying to fetch the next page.
        'nodes_in_plan: for (node, shard) in query_plan {
            let all_pages_span = trace_span!(parent: &self.parent_span, "Executing query", node = %node.address, shard = %shard);
            // For each node in the plan choose a connection to use
            // This connection will be reused for same node retries to preserve paging cache on the shard
            let connection: Arc<Connection> = match node
                .connection_for_shard(shard)
                .instrument(all_pages_span.clone())
                .await
            {
                Ok(connection) => connection,
                Err(e) => {
                    trace!(
                        parent: &all_pages_span,
                        error = %e,
                        "Choosing connection failed"
                    );
                    last_error = e.into();
                    // Broken connection doesn't count as a failed query, don't log in metrics
                    continue 'nodes_in_plan;
                }
            };

            // Retries on the same node as long as RetrySession decides so.
            'same_node_retries: loop {
                trace!(parent: &all_pages_span, "Execution started");

                let coordinator =
                    Coordinator::new(node, node.sharder().is_some().then_some(shard), &connection);
                let request_span = (self.span_creator)(partition_key);

                let fetch_result = self
                    .fetch_one_page(&connection, current_consistency, &request_span, &page_query)
                    .instrument(request_span.span().clone())
                    .await;
                let query_result = match fetch_result {
                    Err(e) => Err(e),
                    Ok((elapsed, result)) => {
                        let result = self
                            .process_first_page(
                                routing_info,
                                node,
                                coordinator.clone(),
                                &request_span,
                                elapsed,
                                result,
                            )
                            .await;
                        Ok(result)
                    }
                };

                let request_error: RequestAttemptError = match query_result {
                    Ok(Ok((first_page, paging_state_response))) => {
                        trace!(parent: &all_pages_span, "Request succeeded");
                        let should_fetch_more_pages = match paging_state_response {
                            PagingStateResponse::HasMorePages { .. } => {
                                ShouldFetchMorePages::MorePages {
                                    first_page_coordinator: coordinator,
                                    first_page_connection: connection,
                                }
                            }
                            PagingStateResponse::NoMorePages => ShouldFetchMorePages::NoMorePages,
                        };
                        return Ok((first_page, should_fetch_more_pages));
                    }
                    Ok(Err(error)) => {
                        trace!(
                            parent: &all_pages_span,
                            error = %error,
                            "Request failed"
                        );
                        error
                    }
                    Err(RequestTimeoutError(timeout)) => {
                        let request_error = RequestError::RequestTimeout(timeout);
                        self.log_request_error(&request_error);
                        trace!(
                            parent: &all_pages_span,
                            error = %request_error,
                            "Request timed out"
                        );
                        // This means that we failed all attempts - in this case, due to a timeout.
                        return Err(NextPageError::RequestFailure(request_error));
                    }
                };

                // Use retry policy to decide what to do next
                let request_info = RequestInfo {
                    error: &request_error,
                    is_idempotent: self.query_is_idempotent,
                    consistency: current_consistency,
                };

                let retry_decision = self.retry_session.decide_should_retry(request_info);
                trace!(
                    parent: &all_pages_span,
                    retry_decision = ?retry_decision
                );

                self.log_attempt_error(&request_error, &retry_decision);

                last_error = RequestError::LastAttemptError(request_error);

                match retry_decision {
                    RetryDecision::RetrySameTarget(cl) => {
                        self.metrics.inc_retries_num();
                        current_consistency = cl.unwrap_or(current_consistency);
                        continue 'same_node_retries;
                    }
                    RetryDecision::RetryNextTarget(cl) => {
                        self.metrics.inc_retries_num();
                        current_consistency = cl.unwrap_or(current_consistency);
                        continue 'nodes_in_plan;
                    }
                    RetryDecision::DontRetry => break 'nodes_in_plan,
                    RetryDecision::IgnoreWriteError => {
                        self.log_request_success();
                        self.retry_session.reset();

                        warn!("Ignoring error during fetching pages; stopping fetching.");
                        return Ok((
                            FirstReceivedPage {
                                content: FirstPageContent::Rows {
                                    rows: DeserializedMetadataAndRawRows::mock_empty(),
                                },
                                tracing_id: None,
                                request_coordinator: coordinator,
                            },
                            ShouldFetchMorePages::NoMorePages,
                        ));
                    }
                };
            }
        }

        self.log_request_error(&last_error);
        Err(NextPageError::RequestFailure(last_error))
    }

    async fn fetch_one_page<QueryFunc, QueryFut>(
        &mut self,
        connection: &Arc<Connection>,
        consistency: Consistency,
        request_span: &RequestSpan,
        page_query: &QueryFunc,
    ) -> Result<(Duration, Result<NonErrorQueryResponse, RequestAttemptError>), RequestTimeoutError>
    where
        QueryFunc: Fn(Arc<Connection>, Consistency, PagingState) -> QueryFut,
        QueryFut: Future<Output = Result<QueryResponse, RequestAttemptError>>,
    {
        self.metrics.inc_total_paged_queries();
        let query_start = std::time::Instant::now();

        let connect_address = connection.get_connect_address();
        trace!(
            connection = %connect_address,
            "Sending"
        );
        self.log_attempt_start(connect_address);

        let runner = async {
            page_query(connection.clone(), consistency, self.paging_state.clone())
                .await
                .and_then(QueryResponse::into_non_error_query_response)
        };
        let query_response = match self.timeouter {
            Some(ref timeouter) => {
                match tokio::time::timeout_at(timeouter.deadline(), runner).await {
                    Ok(res) => res,
                    Err(_) /* tokio::time::error::Elapsed */ => {
                        self.metrics.inc_request_timeouts();
                        return Err(RequestTimeoutError(timeouter.timeout_duration()));
                    }
                }
            }

            None => runner.await,
        };

        let elapsed = query_start.elapsed();
        request_span.record_shard_id(connection);

        Ok((elapsed, query_response))
    }

    async fn process_first_page(
        &mut self,
        routing_info: &RoutingInfo<'_>,
        node: NodeRef<'_>,
        coordinator: Coordinator,
        request_span: &RequestSpan,
        elapsed: Duration,
        query_response: Result<NonErrorQueryResponse, RequestAttemptError>,
    ) -> Result<(FirstReceivedPage, PagingStateResponse), RequestAttemptError> {
        let mut log_success = || {
            let _ = self.metrics.log_query_latency(elapsed.as_millis() as u64);
            self.log_attempt_success();
            self.log_request_success();
            self.load_balancing_policy
                .on_request_success(routing_info, elapsed, node);
        };

        match query_response {
            Ok(NonErrorQueryResponse {
                response:
                    NonErrorResponseWithDeserializedMetadata::Result(
                        result::ResultWithDeserializedMetadata::Rows((rows, paging_state_response)),
                    ),
                tracing_id,
                ..
            }) => {
                log_success();
                request_span.record_raw_rows_fields(&rows);

                if let PagingStateResponse::HasMorePages { state } = &paging_state_response {
                    self.paging_state = state.clone();
                    // Log the next page fetch in advance.
                    self.log_request_start();
                }

                // Query succeeded, reset retry policy for future retries
                self.retry_session.reset();

                Ok((
                    FirstReceivedPage {
                        content: FirstPageContent::Rows { rows },
                        tracing_id,
                        request_coordinator: coordinator,
                    },
                    paging_state_response,
                ))
            }
            Err(err) => {
                self.metrics.inc_failed_paged_queries();
                self.load_balancing_policy
                    .on_request_failure(routing_info, elapsed, node, &err);
                Err(err)
            }
            Ok(NonErrorQueryResponse {
                response:
                    NonErrorResponseWithDeserializedMetadata::Result(
                        result::ResultWithDeserializedMetadata::SetKeyspace(set_keyspace),
                    ),
                tracing_id,
                ..
            }) => {
                log_success();

                Ok((
                    FirstReceivedPage {
                        content: FirstPageContent::SetKeyspace { set_keyspace },
                        tracing_id,
                        request_coordinator: coordinator,
                    },
                    PagingStateResponse::NoMorePages,
                ))
            }
            Ok(NonErrorQueryResponse {
                response:
                    NonErrorResponseWithDeserializedMetadata::Result(
                        result::ResultWithDeserializedMetadata::SchemaChange(schema_change),
                    ),
                tracing_id,
                ..
            }) => {
                log_success();

                Ok((
                    FirstReceivedPage {
                        content: FirstPageContent::SchemaChange { schema_change },
                        tracing_id,
                        request_coordinator: coordinator,
                    },
                    PagingStateResponse::NoMorePages,
                ))
            }
            Ok(NonErrorQueryResponse {
                response: NonErrorResponseWithDeserializedMetadata::Result(_),
                tracing_id,
                ..
            }) => {
                // We have most probably sent a modification statement (e.g. INSERT or UPDATE),
                // so let's return an empty stream as suggested in #631.

                log_success();

                Ok((
                    FirstReceivedPage {
                        content: FirstPageContent::Rows {
                            rows: DeserializedMetadataAndRawRows::mock_empty(),
                        },
                        tracing_id,
                        request_coordinator: coordinator,
                    },
                    PagingStateResponse::NoMorePages,
                ))
            }
            Ok(response) => {
                self.metrics.inc_failed_paged_queries();
                let err =
                    RequestAttemptError::UnexpectedResponse(response.response.to_response_kind());
                self.load_balancing_policy
                    .on_request_failure(routing_info, elapsed, node, &err);
                Err(err)
            }
        }
    }

    #[expect(clippy::too_many_arguments)]
    async fn process_next_page(
        &mut self,
        routing_info: &RoutingInfo<'_>,
        node: NodeRef<'_>,
        coordinator: Coordinator,
        request_span: &RequestSpan,
        sender: &mpsc::Sender<ResultNextPage>,
        elapsed: Duration,
        query_response: Result<NonErrorQueryResponse, RequestAttemptError>,
    ) -> Result<ControlFlow<(), ()>, RequestAttemptError> {
        match query_response {
            Ok(NonErrorQueryResponse {
                response:
                    NonErrorResponseWithDeserializedMetadata::Result(
                        result::ResultWithDeserializedMetadata::Rows((rows, paging_state_response)),
                    ),
                tracing_id,
                ..
            }) => {
                let _ = self.metrics.log_query_latency(elapsed.as_millis() as u64);
                self.log_attempt_success();
                self.log_request_success();
                self.load_balancing_policy
                    .on_request_success(routing_info, elapsed, node);

                request_span.record_raw_rows_fields(&rows);

                let received_page = NextReceivedPage {
                    rows,
                    tracing_id,
                    request_coordinator: Some(coordinator),
                };

                // Send next page to QueryPager
                let res = sender.send(Ok(received_page)).await;
                if res.is_err() {
                    // channel was closed, QueryPager was dropped - should shutdown
                    return Ok(ControlFlow::Break(()));
                }

                match paging_state_response.into_paging_control_flow() {
                    ControlFlow::Continue(paging_state) => {
                        self.paging_state = paging_state;
                    }
                    ControlFlow::Break(()) => {
                        // Reached the last query, shutdown
                        return Ok(ControlFlow::Break(()));
                    }
                }

                // Query succeeded, reset retry policy for future retries
                self.retry_session.reset();
                self.log_request_start();

                Ok(ControlFlow::Continue(()))
            }
            // This catches all other kinds of responses that are not rows.
            // As this is not the first page, this is certainly an error.
            Ok(response) => {
                self.metrics.inc_failed_paged_queries();
                let err =
                    RequestAttemptError::UnexpectedResponse(response.response.to_response_kind());
                self.load_balancing_policy
                    .on_request_failure(routing_info, elapsed, node, &err);
                Err(err)
            }
            Err(err) => {
                self.metrics.inc_failed_paged_queries();
                self.load_balancing_policy
                    .on_request_failure(routing_info, elapsed, node, &err);
                Err(err)
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
///
/// NOTE: This worker only supports executing SELECT statements.
/// More specifically, it expects that each response is of Rows kind.
/// Other kinds of responses will result in an error.
struct SingleConnectionPagerWorker<Fetcher> {
    fetcher: Fetcher,
    timeout: Option<Duration>,
}

impl<Fetcher> SingleConnectionPagerWorker<Fetcher>
where
    Fetcher: AsyncFn(PagingState) -> Result<QueryResponse, RequestAttemptError> + Send + Sync,
{
    /// Fetches a single page. Returns the page and paging state response.
    async fn query_one_page(
        &mut self,
        paging_state: PagingState,
    ) -> Result<
        Result<(NextReceivedPage, PagingStateResponse), RequestAttemptError>,
        RequestTimeoutError,
    > {
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
                let page = NextReceivedPage {
                    rows,
                    tracing_id: response.tracing_id,
                    request_coordinator: None,
                };
                Ok(Ok((page, paging_state_response)))
            }
            _ => Ok(Err(RequestAttemptError::UnexpectedResponse(
                response.response.to_response_kind(),
            ))),
        }
    }

    /// Fetches remaining pages (pages 2+) and sends them through the channel.
    async fn work(mut self, paging_state: PagingState, sender: mpsc::Sender<ResultNextPage>) {
        let mut paging_state = paging_state;
        loop {
            let result = self.query_one_page(paging_state).await;
            match result {
                Err(RequestTimeoutError(timeout)) => {
                    let _ = sender
                        .send(Err(NextPageError::RequestFailure(
                            RequestError::RequestTimeout(timeout),
                        )))
                        .await;
                    return;
                }
                Ok(Err(err)) => {
                    let _ = sender
                        .send(Err(NextPageError::RequestFailure(
                            RequestError::LastAttemptError(err),
                        )))
                        .await;
                    return;
                }
                Ok(Ok((page, paging_state_response))) => {
                    let send_result = sender.send(Ok(page)).await;
                    if send_result.is_err() {
                        // channel was closed, QueryPager was dropped - should shutdown
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
    }
}

pub(crate) struct PreparedPagerConfig {
    pub(crate) prepared: PreparedStatement,
    pub(crate) values: SerializedValues,
    pub(crate) execution_profile: Arc<ExecutionProfileInner>,
    pub(crate) cluster_state: Arc<ClusterState>,
    pub(crate) metrics: Metrics,
    pub(crate) location_preference: Arc<NodeLocationPreference>,
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
    page_receiver: mpsc::Receiver<Result<NextReceivedPage, NextPageError>>,
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

    pub(crate) async fn new_for_query(
        session: &Session,
        statement: Statement,
        execution_profile: Arc<ExecutionProfileInner>,
        cluster_state: Arc<ClusterState>,
        metrics: Metrics,
        node_location_preference: Arc<NodeLocationPreference>,
    ) -> Result<Self, PagerExecutionError> {
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

        let routing_info = RoutingInfo {
            consistency,
            serial_consistency,
            token: None,
            table: None,
            is_confirmed_lwt: false,
            node_location_preference: &node_location_preference,
        };

        let statement_contents = statement.contents.clone();
        let span_creator = move |_partition_key: &Option<PartitionKey<'_>>| {
            let span = RequestSpan::new_query(&statement_contents);
            span.record_request_size(0);
            span
        };

        let mut worker = PagerWorker {
            query_is_idempotent: statement.config.is_idempotent,
            query_consistency: consistency,
            load_balancing_policy,
            retry_session,
            timeouter,
            metrics,
            paging_state: PagingState::start(),
            history_listener: statement.config.history_listener.as_ref().map(Arc::clone),
            current_request_id: None,
            current_attempt_id: None,
            parent_span,
            span_creator,
        };

        let (first_page, should_fetch_more_pages) = worker
            .query_first_page(&cluster_state, &None, &routing_info, page_query)
            .await?;

        /* PROCESS FIRST PAGE */
        let (sender, receiver) = mpsc::channel::<ResultNextPage>(1);
        match should_fetch_more_pages {
            ShouldFetchMorePages::NoMorePages => {
                // No more pages - we are done, return the first page and an empty receiver.
                std::mem::drop(sender);
            }
            ShouldFetchMorePages::MorePages {
                first_page_coordinator,
                first_page_connection,
            } => {
                /* REMAINING PAGES */
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

                    worker
                        .work(
                            cluster_state,
                            sender,
                            page_query,
                            routing_info,
                            None,
                            first_page_coordinator,
                            first_page_connection,
                        )
                        .await;
                };
                let _worker_handle = tokio::task::spawn(worker_task);
            }
        }

        Self::new_from_first_page(first_page, receiver, session).await
    }

    pub(crate) async fn new_for_prepared_statement(
        session: &Session,
        config: PreparedPagerConfig,
    ) -> Result<Self, PagerExecutionError> {
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

        let (partition_key, token) =
            match config.prepared.extract_partition_key_and_calculate_token(
                config.prepared.get_partitioner_name(),
                &config.values,
            ) {
                Ok(res) => res.unzip(),
                Err(err) => {
                    return Err(PagerExecutionError::NextPageError(
                        NextPageError::PartitionKeyError(err),
                    ));
                }
            };

        let table_spec = config.prepared.get_table_spec();
        let routing_info = RoutingInfo {
            consistency,
            serial_consistency,
            token,
            table: table_spec,
            is_confirmed_lwt: config.prepared.is_confirmed_lwt(),
            node_location_preference: &config.location_preference,
        };

        let prepared_ref = &config.prepared;
        let values_ref = &config.values;
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
            if let (Some(table_spec), Some(token)) = (routing_info.table, routing_info.token) {
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

        let span_creator = move |partition_key: &Option<PartitionKey<'_>>| {
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

        let mut worker = PagerWorker {
            query_is_idempotent: config.prepared.config.is_idempotent,
            query_consistency: consistency,
            load_balancing_policy,
            retry_session,
            timeouter,
            metrics: config.metrics,
            paging_state: PagingState::start(),
            history_listener: config
                .prepared
                .config
                .history_listener
                .as_ref()
                .map(Arc::clone),
            current_request_id: None,
            current_attempt_id: None,
            parent_span,
            span_creator,
        };

        let (first_page, should_fetch_more_pages) = worker
            .query_first_page(
                &config.cluster_state,
                &partition_key,
                &routing_info,
                page_query,
            )
            .await?;

        // Required to end the borrow of `partition_key`, so `config` can be moved into the worker task.
        std::mem::drop(partition_key);

        /* PROCESS FIRST PAGE */
        let (sender, receiver) = mpsc::channel::<ResultNextPage>(1);
        match should_fetch_more_pages {
            ShouldFetchMorePages::NoMorePages => {
                // No more pages - we are done, return the first page and an empty receiver.
                std::mem::drop(sender);
            }
            ShouldFetchMorePages::MorePages {
                first_page_coordinator,
                first_page_connection,
            } => {
                /* REMAINING PAGES */
                let worker_task = async move {
                    let partition_key = if config.prepared.is_token_aware() {
                        match config.prepared.extract_partition_key(&config.values) {
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

                    let table_spec = config.prepared.get_table_spec();
                    let routing_info = RoutingInfo {
                        consistency,
                        serial_consistency,
                        token,
                        table: table_spec,
                        is_confirmed_lwt: config.prepared.is_confirmed_lwt(),
                        node_location_preference: &config.location_preference,
                    };

                    let prepared = &config.prepared;
                    let values_ref = &config.values;
                    let page_query =
                        |connection: Arc<Connection>,
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
                                )
                                .await
                        };

                    worker
                        .work(
                            config.cluster_state,
                            sender,
                            page_query,
                            routing_info,
                            partition_key,
                            first_page_coordinator,
                            first_page_connection,
                        )
                        .await;
                };
                let _worker_handle = tokio::task::spawn(worker_task);
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
        let timeout = prepared.get_request_timeout().or_else(|| {
            prepared
                .get_execution_profile_handle()?
                .access()
                .request_timeout
        });

        let mut worker = SingleConnectionPagerWorker {
            fetcher: async move |paging_state: PagingState| {
                connection
                    .execute_raw_with_consistency(
                        &prepared,
                        &values,
                        consistency,
                        serial_consistency,
                        Some(page_size),
                        paging_state,
                    )
                    .await
            },
            timeout,
        };

        let (first_page, paging_state_response) = worker
            .query_one_page(PagingState::start())
            .await
            .map_err(|RequestTimeoutError(timeout)| {
                NextPageError::RequestFailure(RequestError::RequestTimeout(timeout))
            })?
            .map_err(|attempt_error| {
                NextPageError::RequestFailure(RequestError::LastAttemptError(attempt_error))
            })?;

        /* PROCESS FIRST PAGE */
        let (sender, receiver) = mpsc::channel::<ResultNextPage>(1);
        match paging_state_response {
            PagingStateResponse::NoMorePages => {
                // No more pages - we are done, return the first page and an empty receiver.
                std::mem::drop(sender);
            }
            PagingStateResponse::HasMorePages { state } => {
                /* REMAINING PAGES */
                let worker_task = async move { worker.work(state, sender).await };
                let _worker_handle = tokio::task::spawn(worker_task);
            }
        }

        let NextReceivedPage {
            rows,
            tracing_id,
            request_coordinator,
        } = first_page;

        Ok(Self {
            current_page: RawRowLendingIterator::new(rows),
            page_receiver: receiver,
            tracing_ids: Vec::from_iter(tracing_id),
            request_coordinators: Vec::from_iter(request_coordinator),
        })
    }

    async fn new_from_first_page(
        first_page: FirstReceivedPage,
        receiver: mpsc::Receiver<ResultNextPage>,
        session: &Session,
    ) -> Result<Self, PagerExecutionError> {
        let tracing_ids = Vec::from_iter(first_page.tracing_id);
        let coordinator_id = first_page.request_coordinator.node().host_id;
        let request_coordinators = vec![first_page.request_coordinator];

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
                    warnings: Vec::new(),
                };
                session.handle_set_keyspace_response(&response).await?;
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
                    warnings: Vec::new(),
                };
                session
                    .handle_auto_await_schema_agreement(&response, coordinator_id)
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
