//! Unified request-execution core shared by all request paths.
//!
//! Historically, request execution (load balancing, retries, speculative
//! execution, history, metrics) was implemented independently in several
//! places: [`Session`](crate::client::session::Session) non-paged methods, the
//! [`QueryPager`](crate::client::pager::QueryPager) worker and the control
//! connection worker. This module collapses all of that into a single set
//! of layered functions:
//!
//! - [`Request::run_request_no_side_effects`] - takes no `Session`
//!   and does not handle side effects. It applies the client-side timeout and dispatches
//!   speculative-execution fibers. It is generic over the *source* of
//!   connections (see [`AttemptTarget`]) so that it works both with a
//!   load-balancing plan of nodes and with a single fixed connection (as used
//!   by the control connection).
//! - [`Request::run_request_speculative_fiber`] - a single speculative fiber:
//!   iterates the execution plan, picks connections, runs the per-attempt
//!   closure and applies the retry policy.
//!
//! The outermost layer (`run_request`), which additionally handles
//! side effects coming from `USE <keyspace>` and schema-changing statements,
//! lives on [`Session`](crate::client::session::Session) because it needs
//! access to session state. An analogous side-effects-handling layer lives
//! [`PagingExecutor`](crate::client::pager::PagingExecutor).

use std::future::Future;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;

use tracing::trace;
use tracing::trace_span;

use crate::client::execution_profile::ExecutionProfileInner;
use crate::cluster::{ClusterState, NodeRef};
use crate::errors::{ConnectionPoolError, RequestAttemptError, RequestError};
use crate::frame::response::result::TableSpec;
use crate::frame::types::{Consistency, SerialConsistency};
use crate::network::Connection;
use crate::observability::driver_tracing::{RequestSpan, await_in_span};
use crate::observability::history::{self, HistoryListener};
use crate::observability::metrics::Metrics;
use crate::policies::load_balancing::{LoadBalancingPolicy, RoutingInfo};
use crate::policies::retry::{RequestInfo, RetryDecision, RetryPolicy, RetrySession};
use crate::policies::speculative_execution::{self, SpeculativeExecutionPolicy};
use crate::response::{Coordinator, NonErrorQueryResponse};
use crate::routing::Shard;
use crate::routing::Token;
use crate::routing::locator::tablets::TabletVersion;
use crate::statement::StatementConfig;

/// Result of running a request, before side effects are handled.
pub(crate) enum RunRequestResult<ResT> {
    IgnoredWriteError,
    Completed(ResT),
}

/// Chooses the `TABLETS_ROUTING_V2` tablet-version block to attach to an `EXECUTE`.
///
/// This decides the byte's *value* for every case, so that nothing downstream has to invent one:
/// - a cached tablet version: a nibble of it, at a random index;
/// - a tablet-cache miss: a random byte, which almost certainly mismatches and so makes the
///   server send fresh routing information;
/// - no single partition to route by (a range scan, or a statement whose partition key cannot be
///   computed): there is no tablet version to probe, and the server ignores the block for such a
///   request, so the value is irrelevant and the cheapest one is used.
///
/// Whether a byte is appended at all is a separate, per-connection decision made by
/// `Connection::execute_raw_with_consistency`.
pub(crate) fn choose_tablet_block_hint(
    cluster_state: &ClusterState,
    table_spec: Option<&TableSpec>,
    token: Option<Token>,
) -> u8 {
    match table_spec.zip(token) {
        Some((table_spec, token)) => {
            let version = cluster_state
                .replica_locator()
                .tablet_version_for_token(table_spec, token);
            TabletVersion::block_for(version)
        }
        None => 0,
    }
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

/// A request to execute: its parameters, and what each of its attempts needs.
///
/// The execution layers take it by a single reference, as an async fn keeps
/// each of its arguments in its state.
pub(crate) struct Request<'a, F> {
    pub(crate) params: RequestExecutionParams<'a>,
    pub(crate) routing_info: RoutingInfo<'a>,
    /// Performs a single attempt, on the given connection and with the given
    /// consistency.
    pub(crate) run_once: F,
    pub(crate) span: &'a RequestSpan,
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

/// History data threaded through a single fiber. Its listener is the
/// request's [`RequestExecutionParams::history_listener`].
struct HistoryData {
    request_id: history::RequestId,
    speculative_id: Option<history::SpeculativeId>,
}

/// Per-fiber execution context.
struct ExecuteRequestContext {
    /// Created lazily, upon the first attempt error, in order to avoid
    /// an allocation on the happy path (where no retry is ever needed).
    retry_session: Option<Box<dyn RetrySession>>,
    history_data: Option<HistoryData>,
}

impl ExecuteRequestContext {
    fn retry_session(&mut self, retry_policy: &dyn RetryPolicy) -> &mut dyn RetrySession {
        self.retry_session
            .get_or_insert_with(|| retry_policy.new_session())
            .as_mut()
    }
}

impl RequestExecutionParams<'_> {
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

    fn log_attempt_start(
        &self,
        history_data: &Option<HistoryData>,
        node_addr: SocketAddr,
    ) -> Option<history::AttemptId> {
        let (Some(listener), Some(history_data)) = (self.history_listener, history_data) else {
            return None;
        };
        Some(listener.log_attempt_start(
            history_data.request_id,
            history_data.speculative_id,
            node_addr,
        ))
    }

    fn log_attempt_success(&self, attempt_id: Option<history::AttemptId>) {
        let (Some(listener), Some(attempt_id)) = (self.history_listener, attempt_id) else {
            return;
        };
        listener.log_attempt_success(attempt_id);
    }

    fn log_attempt_error(
        &self,
        attempt_id: Option<history::AttemptId>,
        error: &RequestAttemptError,
        retry_decision: &RetryDecision,
    ) {
        let (Some(listener), Some(attempt_id)) = (self.history_listener, attempt_id) else {
            return;
        };
        listener.log_attempt_error(attempt_id, error, retry_decision);
    }
}

impl<'a, F> Request<'a, F> {
    /// Executes the request without handling side effects and without
    /// needing a `Session`.
    ///
    /// Applies the client-side timeout and, for idempotent requests with a
    /// speculative execution policy, runs potentially multiple
    /// [`run_request_speculative_fiber`] fibers; otherwise runs a single fiber.
    ///
    /// `request_plan` is an iterator of targets.
    pub(crate) async fn run_request_no_side_effects<Target, QueryFut>(
        &self,
        // Taken by reference: an async fn keeps arguments taken by value twice in
        // its state, and it is passed further down to the fibers.
        request_plan: &mut impl Iterator<Item = Target>,
    ) -> Result<RequestExecutionOutcome<Target::Coordinator>, RequestError>
    where
        Target: AttemptTarget,
        F: Fn(Arc<Connection>, Consistency) -> QueryFut,
        QueryFut: Future<Output = Result<NonErrorQueryResponse, RequestAttemptError>>,
    {
        // Only the id is kept, as the listener can be read from the parameters.
        let request_id: Option<history::RequestId> = self
            .params
            .history_listener
            .map(|hl| hl.log_request_start());

        let runner = async {
            match self.params.metrics_and_speculative_policy {
                #[cfg_attr(not(feature = "metrics"), expect(unused_variables))]
                Some((metrics, Some(speculative))) if self.params.is_idempotent => {
                    let shared_request_plan = SharedPlan {
                        iter: std::sync::Mutex::new(request_plan),
                    };

                    let request_runner_generator = |is_speculative: bool| {
                        let history_data: Option<HistoryData> = self
                            .params
                            .history_listener
                            .zip(request_id)
                            .map(|(listener, request_id)| {
                                let speculative_id: Option<history::SpeculativeId> = is_speculative
                                    .then(|| listener.log_new_speculative_fiber(request_id));
                                HistoryData {
                                    request_id,
                                    speculative_id,
                                }
                            });

                        if is_speculative {
                            self.span.inc_speculative_executions();
                        }

                        let mut context = ExecuteRequestContext {
                            retry_session: None,
                            history_data,
                        };
                        let request_plan = &shared_request_plan;
                        async move {
                            self.run_request_speculative_fiber(request_plan, &mut context)
                                .await
                        }
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
                        request_id.map(|request_id| HistoryData {
                            request_id,
                            speculative_id: None,
                        });
                    let mut context = ExecuteRequestContext {
                        retry_session: None,
                        history_data,
                    };
                    self.run_request_speculative_fiber(request_plan, &mut context)
                        .await
                        .unwrap_or(Err(RequestError::EmptyPlan))
                }
            }
        };

        // Matched by reference, so that this future keeps a reference rather
        // than the duration while waiting.
        let result = match &self.params.request_timeout {
            Some(timeout) => tokio::time::timeout(*timeout, runner).await.unwrap_or_else(
                |_: tokio::time::error::Elapsed| {
                    self.params.inc_request_timeouts();

                    let timeout_error = RequestError::RequestTimeout(*timeout);
                    trace!(
                        parent: self.span.span(),
                        error = %timeout_error,
                        "Request timed out"
                    );
                    Err(timeout_error)
                },
            ),
            None => runner.await,
        };

        if let Some((history_listener, request_id)) = self.params.history_listener.zip(request_id) {
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
    /// running the attempt and consulting the retry policy on failure.
    ///
    /// Returns `None` only if the plan was empty.
    async fn run_request_speculative_fiber<Target, QueryFut>(
        &self,
        request_plan: impl Iterator<Item = Target>,
        // Taken by reference: an async fn keeps arguments taken by value twice in
        // its state.
        context: &mut ExecuteRequestContext,
    ) -> Option<Result<RequestExecutionOutcome<Target::Coordinator>, RequestError>>
    where
        Target: AttemptTarget,
        F: Fn(Arc<Connection>, Consistency) -> QueryFut,
        QueryFut: Future<Output = Result<NonErrorQueryResponse, RequestAttemptError>>,
    {
        // Boxed, as it is set only after a failed attempt: inline, its size and
        // 16-byte alignment would weigh on the future of every request.
        let mut last_error: Option<Box<RequestError>> = None;
        let mut current_consistency: Consistency = self.params.consistency;

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
                        last_error = Some(Box::new(e.into()));
                        // Broken connection doesn't count as a failed request, don't log in metrics
                        continue 'targets_in_plan;
                    }
                };
                self.span.record_shard_id(&connection);

                self.params.inc_total_queries();
                let request_start = Instant::now();

                // Scoped, as logging borrows the address, which would keep it in
                // this future's state during the attempt.
                let attempt_id: Option<history::AttemptId> = {
                    let connect_address = connection.get_connect_address();
                    trace!(
                        parent: &span,
                        connection = %connect_address,
                        "Sending"
                    );
                    self.params
                        .log_attempt_start(&context.history_data, connect_address)
                };

                let coordinator = target.coordinator(&connection);

                let request_result: Result<NonErrorQueryResponse, RequestAttemptError> =
                    await_in_span!((self.run_once)(connection, current_consistency), &span);

                let elapsed = request_start.elapsed();
                let request_error: RequestAttemptError = match request_result {
                    Ok(response) => {
                        trace!(parent: &span, "Request succeeded");
                        self.params.log_query_latency(elapsed.as_millis() as u64);
                        self.params.log_attempt_success(attempt_id);
                        target.on_attempt_success(
                            self.params.load_balancing_policy,
                            &self.routing_info,
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
                        self.params.inc_failed_queries();
                        target.on_attempt_failure(
                            self.params.load_balancing_policy,
                            &self.routing_info,
                            elapsed,
                            &e,
                        );
                        e
                    }
                };

                // Use retry policy to decide what to do next.
                let request_info = RequestInfo {
                    error: &request_error,
                    is_idempotent: self.params.is_idempotent,
                    consistency: current_consistency,
                };

                let retry_decision = context
                    .retry_session(self.params.retry_policy)
                    .decide_should_retry(request_info);
                trace!(
                    parent: &span,
                    retry_decision = ?retry_decision
                );

                self.params
                    .log_attempt_error(attempt_id, &request_error, &retry_decision);

                last_error = Some(Box::new(request_error.into()));

                match retry_decision {
                    RetryDecision::RetrySameTarget(new_cl) => {
                        self.params.inc_retries_num();
                        current_consistency = new_cl.unwrap_or(current_consistency);
                        continue 'same_target_retries;
                    }
                    RetryDecision::RetryNextTarget(new_cl) => {
                        self.params.inc_retries_num();
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

        last_error.map(|error| Err(*error))
    }
}
