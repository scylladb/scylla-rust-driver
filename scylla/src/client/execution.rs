use std::future::Future;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;

use tracing::Instrument;
use tracing::trace;
use tracing::trace_span;

use crate::client::execution_profile::ExecutionProfileInner;
use crate::cluster::NodeRef;
use crate::errors::ConnectionPoolError;
use crate::errors::RequestAttemptError;
use crate::errors::RequestError;
use crate::frame::types::{Consistency, SerialConsistency};
use crate::network::Connection;
use crate::observability::driver_tracing::RequestSpan;
use crate::observability::history::{self, HistoryListener};
use crate::observability::metrics::Metrics;
use crate::policies::load_balancing;
use crate::policies::load_balancing::{LoadBalancingPolicy, RoutingInfo};
use crate::policies::retry::{RequestInfo, RetryDecision, RetryPolicy, RetrySession};
use crate::policies::speculative_execution::{self, SpeculativeExecutionPolicy};
use crate::response::Coordinator;
use crate::response::NonErrorQueryResponse;
use crate::routing::Shard;
use crate::statement::StatementConfig;

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
    /// Serial consistency to use, if any.
    pub(crate) serial_consistency: Option<SerialConsistency>,
    /// Retry policy used to start a fresh retry session per fiber.
    pub(crate) retry_policy: &'a dyn RetryPolicy,
    /// Load balancing policy used to order targets for the execution.
    pub(crate) load_balancing_policy: &'a dyn LoadBalancingPolicy,
    /// The following two fields are grouped to statically enforce that speculative execution
    /// is only used when metrics sink is provided.
    ///
    /// Metrics sink, if any. The control connection has no metrics.
    /// Speculative execution policy, if any. Only fires for idempotent requests.
    pub(crate) metrics_and_speculative_policy:
        Option<(&'a Arc<Metrics>, Option<&'a dyn SpeculativeExecutionPolicy>)>,
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
        let serial_consistency = statement_config
            .serial_consistency
            .unwrap_or(execution_profile.serial_consistency);
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
            serial_consistency,
            retry_policy,
            load_balancing_policy,
            metrics_and_speculative_policy: Some((metrics, speculative_policy)),
            request_timeout,
            history_listener,
            request_kind,
        }
    }
}

/// Abstracts a single target that a request attempt can be sent to.
///
/// A request plan is an iterator of `AttemptTarget`s. There are two
/// implementations:
/// - [`NodeAttemptTarget`] - a `(node, shard)` pair produced by a load
///   balancing plan. It produces a real [`Coordinator`] and forwards
///   success/failure feedback to the load balancing policy.
/// - [`SingleConnectionTarget`] - a single fixed connection (used by the
///   control connection, which intentionally has no [`Node`](crate::cluster::Node)).
///   It produces no coordinator (`()`) and has no load balancing feedback.
pub(crate) trait AttemptTarget {
    /// Coordinator descriptor reported back to the caller.
    type Coordinator;

    /// Acquires a connection to send the attempt on.
    ///
    /// On `Err`, the fiber skips this target and proceeds to the next one in
    /// the plan (without counting it as a failed request in metrics).
    async fn get_connection(&self) -> Result<Arc<Connection>, ConnectionPoolError>;

    /// Builds the coordinator descriptor once a connection has been chosen.
    fn coordinator(&self, connection: &Arc<Connection>) -> Self::Coordinator;

    /// Load balancing feedback after a successful attempt. No-op for targets
    /// that are not driven by load balancing.
    fn on_attempt_success(
        &self,
        load_balancing_policy: &dyn LoadBalancingPolicy,
        routing_info: &RoutingInfo<'_>,
        elapsed: Duration,
    );

    /// Load balancing feedback after a failed attempt. No-op for targets that
    /// are not driven by load balancing.
    fn on_attempt_failure(
        &self,
        load_balancing_policy: &dyn LoadBalancingPolicy,
        routing_info: &RoutingInfo<'_>,
        elapsed: Duration,
        error: &RequestAttemptError,
    );
}

/// A load-balancing-driven target: a `(node, shard)` pair.
pub(crate) struct NodeAttemptTarget<'a> {
    node: NodeRef<'a>,
    shard: Shard,
}

impl<'a> NodeAttemptTarget<'a> {
    pub(crate) fn new(node: NodeRef<'a>, shard: Shard) -> Self {
        Self { node, shard }
    }
}

impl AttemptTarget for NodeAttemptTarget<'_> {
    type Coordinator = Coordinator;

    async fn get_connection(&self) -> Result<Arc<Connection>, ConnectionPoolError> {
        self.node.connection_for_shard(self.shard).await
    }

    fn coordinator(&self, connection: &Arc<Connection>) -> Coordinator {
        Coordinator::new(self.node, connection)
    }

    fn on_attempt_success(
        &self,
        load_balancing_policy: &dyn LoadBalancingPolicy,
        routing_info: &RoutingInfo<'_>,
        elapsed: Duration,
    ) {
        load_balancing_policy.on_request_success(routing_info, elapsed, self.node);
    }

    fn on_attempt_failure(
        &self,
        load_balancing_policy: &dyn LoadBalancingPolicy,
        routing_info: &RoutingInfo<'_>,
        elapsed: Duration,
        error: &RequestAttemptError,
    ) {
        load_balancing_policy.on_request_failure(routing_info, elapsed, self.node, error);
    }
}

/// A target that always uses one specific connection. Used by the control
/// connection, which has no [`Node`](crate::cluster::Node) and no load
/// balancing.
pub(crate) struct SingleConnectionTarget {
    connection: Arc<Connection>,
}

impl SingleConnectionTarget {
    pub(crate) fn new(connection: Arc<Connection>) -> Self {
        Self { connection }
    }
}

impl AttemptTarget for SingleConnectionTarget {
    type Coordinator = ();

    async fn get_connection(&self) -> Result<Arc<Connection>, ConnectionPoolError> {
        Ok(Arc::clone(&self.connection))
    }

    fn coordinator(&self, _connection: &Arc<Connection>) {}

    fn on_attempt_success(
        &self,
        _load_balancing_policy: &dyn LoadBalancingPolicy,
        _routing_info: &RoutingInfo<'_>,
        _elapsed: Duration,
    ) {
    }

    fn on_attempt_failure(
        &self,
        _load_balancing_policy: &dyn LoadBalancingPolicy,
        _routing_info: &RoutingInfo<'_>,
        _elapsed: Duration,
        _error: &RequestAttemptError,
    ) {
    }
}

/// Outcome of [`run_request_no_side_effects`]/[`run_request_speculative_fiber`].
pub(crate) struct RequestExecutionOutcome<C> {
    /// The successful (or ignored-write) result.
    pub(crate) result: RunRequestResult<NonErrorQueryResponse>,
    /// The coordinator that served the request (target-dependent type, only available
    /// in certain execution contexts).
    pub(crate) coordinator: C,
}

/// History data threaded through a single fiber.
struct HistoryData<'a> {
    listener: &'a dyn HistoryListener,
    request_id: history::RequestId,
    speculative_id: Option<history::SpeculativeId>,
}

/// Per-fiber execution context.
struct ExecuteRequestContext<'a> {
    retry_session: Box<dyn RetrySession>,
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
        let Some((metrics, _)) = self.metrics_and_speculative_policy else {
            return;
        };
        match self.request_kind {
            RequestPaging::Unpaged => metrics.inc_total_nonpaged_queries(),
            RequestPaging::Manual => metrics.inc_total_manually_paged_queries(),
            RequestPaging::Automatic => metrics.inc_total_automatically_paged_queries(),
        }
    }

    fn inc_failed_queries(&self) {
        let Some((metrics, _)) = self.metrics_and_speculative_policy else {
            return;
        };
        match self.request_kind {
            RequestPaging::Unpaged => metrics.inc_failed_nonpaged_queries(),
            RequestPaging::Manual => metrics.inc_failed_manually_paged_queries(),
            RequestPaging::Automatic => metrics.inc_failed_automatically_paged_queries(),
        }
    }

    fn inc_retries_num(&self) {
        if let Some((metrics, _)) = self.metrics_and_speculative_policy {
            metrics.inc_retries_num();
        }
    }

    fn inc_request_timeouts(&self) {
        if let Some((metrics, _)) = self.metrics_and_speculative_policy {
            metrics.inc_request_timeouts();
        }
    }

    fn log_query_latency(&self, latency_ms: u64) {
        if let Some((metrics, _)) = self.metrics_and_speculative_policy {
            let _ = metrics.log_query_latency(latency_ms);
        }
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
    pub(crate) async fn run_request_no_side_effects<Target, QueryFut>(
        &self,
        routing_info: &'a RoutingInfo<'a>,
        request_plan: impl Iterator<Item = Target>,
        run_request_once: impl Fn(Arc<Connection>, Consistency) -> QueryFut,
        request_span: &'a RequestSpan,
    ) -> Result<RequestExecutionOutcome<Target::Coordinator>, RequestError>
    where
        Target: AttemptTarget,
        QueryFut: Future<Output = Result<NonErrorQueryResponse, RequestAttemptError>>,
    {
        let history_listener_and_id: Option<(&dyn HistoryListener, history::RequestId)> =
            self.history_listener.map(|hl| (hl, hl.log_request_start()));

        let runner = async {
            match self.metrics_and_speculative_policy {
                #[cfg_attr(not(feature = "metrics"), expect(unused_variables))]
                Some((metrics, Some(speculative))) if self.is_idempotent => {
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
                                retry_session: self.retry_policy.new_session(),
                                history_data,
                                routing_info,
                                request_span,
                            },
                        )
                    };

                    let context = speculative_execution::Context {
                        #[cfg(feature = "metrics")]
                        metrics: Arc::clone(metrics),
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
                            retry_session: self.retry_policy.new_session(),
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
    async fn run_request_speculative_fiber<Target, QueryFut>(
        &self,
        request_plan: impl Iterator<Item = Target>,
        run_request_once: impl Fn(Arc<Connection>, Consistency) -> QueryFut,
        mut context: ExecuteRequestContext<'a>,
    ) -> Option<Result<RequestExecutionOutcome<Target::Coordinator>, RequestError>>
    where
        Target: AttemptTarget,
        QueryFut: Future<Output = Result<NonErrorQueryResponse, RequestAttemptError>>,
    {
        let mut last_error: Option<RequestError> = None;
        let mut current_consistency: Consistency = self.consistency;

        'targets_in_plan: for target in request_plan {
            let span = trace_span!("Executing request on chosen target");
            'same_target_retries: loop {
                trace!(parent: &span, "Execution attempt started");
                let connection = match target.get_connection().await {
                    Ok(connection) => connection,
                    Err(e) => {
                        trace!(
                            parent: &span,
                            error = %e,
                            "Choosing connection failed"
                        );
                        last_error = Some(e.into());
                        // Broken connection doesn't count as a failed request, don't log in metrics
                        continue 'targets_in_plan;
                    }
                };
                context.request_span.record_shard_id(&connection);

                self.inc_total_queries();
                let request_start = Instant::now();

                let connect_address = connection.get_connect_address();
                trace!(
                    parent: &span,
                    connection = %connect_address,
                    "Sending"
                );

                let coordinator = target.coordinator(&connection);

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
                        target.on_attempt_success(
                            self.load_balancing_policy,
                            context.routing_info,
                            elapsed,
                        );
                        return Some(Ok(RequestExecutionOutcome {
                            result: RunRequestResult::Completed(response),
                            coordinator,
                        }));
                    }
                    Err(e) => {
                        trace!(
                            parent: &span,
                            last_error = %e,
                            "Request failed"
                        );
                        self.inc_failed_queries();
                        target.on_attempt_failure(
                            self.load_balancing_policy,
                            context.routing_info,
                            elapsed,
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

                let retry_decision = context.retry_session.decide_should_retry(request_info);
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
                        continue 'same_target_retries;
                    }
                    RetryDecision::RetryNextTarget(new_cl) => {
                        self.inc_retries_num();
                        current_consistency = new_cl.unwrap_or(current_consistency);
                        continue 'targets_in_plan;
                    }
                    RetryDecision::DontRetry => break 'targets_in_plan,
                    RetryDecision::IgnoreWriteError => {
                        return Some(Ok(RequestExecutionOutcome {
                            result: RunRequestResult::IgnoredWriteError,
                            coordinator,
                        }));
                    }
                };
            }
        }

        last_error.map(Result::Err)
    }
}
