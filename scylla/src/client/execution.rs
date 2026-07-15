use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use tracing::{Instrument as _, trace, trace_span};

use crate::client::execution_profile::ExecutionProfileInner;
use crate::errors::{RequestAttemptError, RequestError};
use crate::frame::types::Consistency;

use crate::network::Connection;
use crate::observability::driver_tracing::RequestSpan;
use crate::observability::history::{self, HistoryListener};
use crate::observability::metrics::Metrics;
use crate::policies::load_balancing::{self, LoadBalancingPolicy, RoutingInfo};
use crate::policies::retry::{RequestInfo, RetryDecision, RetryPolicy};
use crate::policies::speculative_execution::{self, SpeculativeExecutionPolicy};
use crate::response::{Coordinator, NonErrorQueryResponse};
use crate::statement::StatementConfig;
use crate::{cluster::NodeRef, routing::Shard};

/// Result of running a request, before side effects are handled.
pub(crate) enum RunRequestResult<ResT> {
    IgnoredWriteError,
    Completed(ResT),
}

/// Specifies the mechanism used for query paging.
///
/// Currently, only for the purpose of bumping the right metrics counters.
#[derive(Clone, Debug)]
pub(crate) enum RequestPaging {
    Unpaged,
    Manual,
    Automatic,
}

/// Wraps a request plan in a mutex so that it can be shared between speculative
/// fibers running concurrently.
struct SharedPlan<I> {
    iter: std::sync::Mutex<I>,
}

impl<Target, I> Iterator for &SharedPlan<I>
where
    I: Iterator<Item = Target>,
{
    type Item = Target;

    fn next(&mut self) -> Option<Self::Item> {
        self.iter.lock().unwrap().next()
    }
}

/// All resolved per-request configuration needed to execute a request,
/// independent of any `Session`.
///
/// The two-level fallback between per-statement config and the execution
/// profile is resolved *before* constructing this struct.
pub(crate) struct RequestExecutionParams<'a> {
    /// Whether the request is idempotent (gates speculative execution and is
    /// passed to the retry policy).
    pub(crate) is_idempotent: bool,
    /// Consistency to use.
    pub(crate) consistency: Consistency,
    /// Retry policy used to start a fresh retry session per fiber.
    pub(crate) retry_policy: &'a dyn RetryPolicy,
    /// Load balancing policy used to order targets for the execution.
    pub(crate) load_balancing_policy: &'a dyn LoadBalancingPolicy,
    /// Metrics sink.
    pub(crate) metrics: &'a Arc<Metrics>,
    /// Speculative execution policy, if any.
    pub(crate) speculative_policy: Option<&'a dyn SpeculativeExecutionPolicy>,
    /// Client-side request timeout, if any.
    pub(crate) request_timeout: Option<Duration>,
    /// History listener, if any.
    pub(crate) history_listener: Option<&'a dyn HistoryListener>,
    /// Paged vs non-paged, for metrics.
    pub(crate) request_kind: RequestPaging,
}

/// Constructor(s) for [`RequestExecutionParams`].
impl<'a> RequestExecutionParams<'a> {
    pub(crate) fn new_for_session_apis(
        statement_config: &'a StatementConfig,
        execution_profile: &'a ExecutionProfileInner,
        metrics: &'a Arc<Metrics>,
        request_kind: RequestPaging,
    ) -> Self {
        let is_idempotent = statement_config.is_idempotent;
        let consistency = statement_config
            .consistency
            .unwrap_or(execution_profile.consistency);
        let retry_policy = statement_config
            .retry_policy
            .as_deref()
            .unwrap_or(execution_profile.retry_policy.as_ref());
        let load_balancing_policy = statement_config
            .load_balancing_policy
            .as_deref()
            .unwrap_or(execution_profile.load_balancing_policy.as_ref());
        let request_timeout = statement_config
            .request_timeout
            .or(execution_profile.request_timeout);
        let history_listener = statement_config.history_listener.as_deref();
        let speculative_policy = execution_profile.speculative_execution_policy.as_deref();

        Self {
            is_idempotent,
            consistency,
            retry_policy,
            load_balancing_policy,
            metrics,
            speculative_policy,
            request_timeout,
            history_listener,
            request_kind,
        }
    }
}

/// History data threaded through a single fiber.
struct HistoryData<'a> {
    listener: &'a dyn HistoryListener,
    request_id: history::RequestId,
    speculative_id: Option<history::SpeculativeId>,
}

/// Per-fiber execution context.
struct ExecuteRequestContext<'a> {
    history_data: Option<HistoryData<'a>>,
    routing_info: &'a load_balancing::RoutingInfo<'a>,
    request_span: &'a RequestSpan,
}

impl ExecuteRequestContext<'_> {
    fn log_attempt_start(&self, node_addr: SocketAddr) -> Option<history::AttemptId> {
        self.history_data.as_ref().map(|hd| {
            hd.listener
                .log_attempt_start(hd.request_id, hd.speculative_id, node_addr)
        })
    }

    fn log_attempt_success(&self, attempt_id_opt: &Option<history::AttemptId>) {
        let (Some(history_data), Some(attempt_id)) = (&self.history_data, attempt_id_opt) else {
            return;
        };

        history_data.listener.log_attempt_success(*attempt_id);
    }

    fn log_attempt_error(
        &self,
        attempt_id_opt: &Option<history::AttemptId>,
        error: &RequestAttemptError,
        retry_decision: &RetryDecision,
    ) {
        let (Some(history_data), Some(attempt_id)) = (&self.history_data, attempt_id_opt) else {
            return;
        };

        history_data
            .listener
            .log_attempt_error(*attempt_id, error, retry_decision);
    }
}

impl<'a> RequestExecutionParams<'a> {
    fn inc_total_queries(&self) {
        match self.request_kind {
            RequestPaging::Unpaged => self.metrics.inc_total_nonpaged_queries(),
            RequestPaging::Manual => self.metrics.inc_total_manually_paged_queries(),
            RequestPaging::Automatic => self.metrics.inc_total_automatically_paged_queries(),
        }
    }

    fn inc_failed_queries(&self) {
        match self.request_kind {
            RequestPaging::Unpaged => self.metrics.inc_failed_nonpaged_queries(),
            RequestPaging::Manual => self.metrics.inc_failed_manually_paged_queries(),
            RequestPaging::Automatic => self.metrics.inc_failed_automatically_paged_queries(),
        }
    }

    fn inc_retries_num(&self) {
        self.metrics.inc_retries_num();
    }

    fn inc_request_timeouts(&self) {
        self.metrics.inc_request_timeouts();
    }

    fn log_query_latency(&self, latency_ms: u64) {
        let _ = self.metrics.log_query_latency(latency_ms);
    }

    /// Executes a request without handling side effects and without
    /// needing a `Session`.
    ///
    /// Applies the client-side timeout and, for idempotent requests with a
    /// speculative execution policy, runs potentially multiple
    /// [`run_request_speculative_fiber`] fibers; otherwise runs a single fiber.
    ///
    /// `request_plan` is an iterator of targets. `run_request_once`
    /// performs a single attempt against a chosen connection and consistency.
    pub(crate) async fn run_request_no_side_effects<QueryFut>(
        &'a self,
        routing_info: &'a RoutingInfo<'a>,
        request_plan: impl Iterator<Item = (NodeRef<'a>, Shard)>,
        run_request_once: impl Fn(Arc<Connection>, Consistency) -> QueryFut,
        request_span: &'a RequestSpan,
    ) -> Result<(RunRequestResult<NonErrorQueryResponse>, Coordinator), RequestError>
    where
        QueryFut: Future<Output = Result<NonErrorQueryResponse, RequestAttemptError>>,
    {
        let history_listener_and_id: Option<(&dyn HistoryListener, history::RequestId)> =
            self.history_listener.map(|hl| (hl, hl.log_request_start()));

        let runner = async {
            match self.speculative_policy {
                Some(speculative) if self.is_idempotent => {
                    let shared_request_plan = SharedPlan {
                        iter: std::sync::Mutex::new(request_plan),
                    };

                    let request_runner_generator = |is_speculative: bool| {
                        let history_data: Option<HistoryData> =
                            history_listener_and_id.map(|(listener, request_id)| {
                                let speculative_id: Option<history::SpeculativeId> = is_speculative
                                    .then(|| listener.log_new_speculative_fiber(request_id));
                                HistoryData {
                                    listener,
                                    request_id,
                                    speculative_id,
                                }
                            });

                        if is_speculative {
                            request_span.inc_speculative_executions();
                        }

                        self.run_request_speculative_fiber(
                            &shared_request_plan,
                            &run_request_once,
                            ExecuteRequestContext {
                                history_data,
                                routing_info,
                                request_span,
                            },
                        )
                    };

                    let context = speculative_execution::Context {
                        #[cfg(feature = "metrics")]
                        metrics: Arc::clone(self.metrics),
                    };

                    speculative_execution::execute(speculative, &context, request_runner_generator)
                        .await
                }
                _ => {
                    let history_data: Option<HistoryData> =
                        history_listener_and_id.map(|(listener, request_id)| HistoryData {
                            listener,
                            request_id,
                            speculative_id: None,
                        });
                    self.run_request_speculative_fiber(
                        request_plan,
                        &run_request_once,
                        ExecuteRequestContext {
                            history_data,
                            routing_info,
                            request_span,
                        },
                    )
                    .await
                    .unwrap_or(Err(RequestError::EmptyPlan))
                }
            }
        };

        let result = match self.request_timeout {
            Some(timeout) => tokio::time::timeout(timeout, runner).await.unwrap_or_else(
                |_: tokio::time::error::Elapsed| {
                    self.inc_request_timeouts();

                    let timeout_error = RequestError::RequestTimeout(timeout);
                    trace!(
                        parent: request_span.span(),
                        error = %timeout_error,
                        "Request timed out"
                    );
                    Err(timeout_error)
                },
            ),
            None => runner.await,
        };

        if let Some((history_listener, request_id)) = history_listener_and_id {
            match &result {
                Ok(_) => history_listener.log_request_success(request_id),
                Err(e) => history_listener.log_request_error(request_id, e),
            }
        }

        result
    }

    /// A single execution fiber.
    ///
    /// Iterates the execution plan, picking a connection for each target and attempt,
    /// running `run_request_once` and consulting the retry policy on failure.
    ///
    /// Returns `None` only if the plan was empty.
    async fn run_request_speculative_fiber<QueryFut>(
        &self,
        request_plan: impl Iterator<Item = (NodeRef<'a>, Shard)>,
        run_request_once: impl Fn(Arc<Connection>, Consistency) -> QueryFut,
        context: ExecuteRequestContext<'a>,
    ) -> Option<Result<(RunRequestResult<NonErrorQueryResponse>, Coordinator), RequestError>>
    where
        QueryFut: Future<Output = Result<NonErrorQueryResponse, RequestAttemptError>>,
    {
        let mut retry_session = self.retry_policy.new_session();
        let mut last_error: Option<RequestError> = None;
        let mut current_consistency: Consistency = self.consistency;

        'nodes_in_plan: for (node, shard) in request_plan {
            let span = trace_span!("Executing request", node = %node.address, shard = %shard);
            'same_node_retries: loop {
                trace!(parent: &span, "Execution started");
                let connection = match node.connection_for_shard(shard).await {
                    Ok(connection) => connection,
                    Err(e) => {
                        trace!(
                            parent: &span,
                            error = %e,
                            "Choosing connection failed"
                        );
                        last_error = Some(e.into());
                        // Broken connection doesn't count as a failed request, don't log in metrics
                        continue 'nodes_in_plan;
                    }
                };
                context.request_span.record_shard_id(&connection);

                self.inc_total_queries();
                let request_start = std::time::Instant::now();

                let connect_address = connection.get_connect_address();
                trace!(
                    parent: &span,
                    connection = %connect_address,
                    "Sending"
                );
                let coordinator = Coordinator::new(node, &connection);

                let attempt_id: Option<history::AttemptId> =
                    context.log_attempt_start(connect_address);
                let request_result: Result<NonErrorQueryResponse, RequestAttemptError> =
                    run_request_once(connection, current_consistency)
                        .instrument(span.clone())
                        .await;

                let elapsed = request_start.elapsed();
                let request_error: RequestAttemptError = match request_result {
                    Ok(response) => {
                        trace!(parent: &span, "Request succeeded");
                        self.log_query_latency(elapsed.as_millis() as u64);
                        context.log_attempt_success(&attempt_id);
                        self.load_balancing_policy.on_request_success(
                            context.routing_info,
                            elapsed,
                            node,
                        );
                        return Some(Ok((RunRequestResult::Completed(response), coordinator)));
                    }
                    Err(e) => {
                        trace!(
                            parent: &span,
                            last_error = %e,
                            "Request failed"
                        );
                        self.inc_failed_queries();
                        self.load_balancing_policy.on_request_failure(
                            context.routing_info,
                            elapsed,
                            node,
                            &e,
                        );
                        e
                    }
                };

                // Use retry policy to decide what to do next.
                let request_info = RequestInfo {
                    error: &request_error,
                    is_idempotent: self.is_idempotent,
                    consistency: current_consistency,
                };

                let retry_decision = retry_session.decide_should_retry(request_info);
                trace!(
                    parent: &span,
                    retry_decision = ?retry_decision
                );

                context.log_attempt_error(&attempt_id, &request_error, &retry_decision);

                last_error = Some(request_error.into());

                match retry_decision {
                    RetryDecision::RetrySameTarget(new_cl) => {
                        self.inc_retries_num();
                        current_consistency = new_cl.unwrap_or(current_consistency);
                        continue 'same_node_retries;
                    }
                    RetryDecision::RetryNextTarget(new_cl) => {
                        self.inc_retries_num();
                        current_consistency = new_cl.unwrap_or(current_consistency);
                        continue 'nodes_in_plan;
                    }
                    RetryDecision::DontRetry => break 'nodes_in_plan,
                    RetryDecision::IgnoreWriteError => {
                        return Some(Ok((RunRequestResult::IgnoredWriteError, coordinator)));
                    }
                };
            }
        }

        last_error.map(Result::Err)
    }
}
