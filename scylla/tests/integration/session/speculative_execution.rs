//! Tests of speculative execution together with the retry policy.
//!
//! The driver can send a request more than one time, for two different
//! reasons:
//! - The retry policy asks for one more attempt after a failure.
//! - The speculative execution policy starts one more fiber when a response
//!   is slow.
//!
//! These tests check the sequence of attempts that the two policies make
//! together:
//! - the fiber of each attempt, including which speculative fiber it is,
//! - how each attempt ended,
//! - whether an attempt went to a node that an earlier attempt used,
//! - how many speculative fibers started.
//!
//! The history of the driver is the source of truth. All the other parts are
//! simulated. The three nodes are `scylla-proxy` nodes in dry mode, with no
//! ScyllaDB behind them. They answer each request from the rules below. Thus:
//! - The cases can depend on time and still run fast. `TICK` is the unit of
//!   the interval of the speculative policy.
//! - A real cluster adds variance to these time windows. Dry nodes do not.
//!
//! The cases assume that the proxy receives the request of the regular fiber
//! first. A rule of a case fires for the next request that arrives, on any
//! node. The proxy cannot tell which fiber sent a request. The regular fiber
//! sends its request at once, and the first speculative timer fires after
//! two ticks. Thus, the order holds, unless the driver is more than two ticks
//! late.
//!
//! A passing run logs one "Could not establish control connection and fetch
//! metadata" error for each case. This is expected:
//! - No cluster gives metadata to read.
//! - A session that cannot read metadata continues with a peer list built
//!   from its contact points.
//! - This peer list is the topology that these cases need.

use std::collections::HashMap;
use std::fmt;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use scylla::client::execution_profile::ExecutionProfile;
use scylla::client::session::Session;
use scylla::client::session_builder::SessionBuilder;
use scylla::errors::{DbError, ExecutionError, RequestAttemptError, RequestError};
use scylla::frame::response::result::{ColumnSpec, ColumnType, NativeType, TableSpec};
use scylla::observability::history::{
    AttemptId, AttemptResult, HistoryCollector, HistoryListener, RequestId, SpeculativeId,
    StructuredHistory, TimePoint,
};
use scylla::policies::retry::{RequestInfo, RetryDecision, RetryPolicy, RetrySession};
use scylla::policies::speculative_execution::{
    SimpleSpeculativeExecutionPolicy, SpeculativeExecutionPolicy,
};
use scylla::statement::unprepared::Statement;
use scylla_proxy::{
    Condition, Gate, Node, Proxy, ProxyError, Reaction as _, RequestFrame, RequestOpcode,
    RequestReaction, RequestRule, ResponseFrame, RunningProxy, WorkerError, example_db_errors,
    get_exclusive_local_address,
};

use crate::utils::setup_tracing;

/// The time unit of these tests. One tick must be much longer than the time
/// to answer a request. The proxy forges each response in the test process.
/// Thus, an answer is only a channel send and a loopback write.
const TICK: Duration = Duration::from_millis(100);

/// The number of dry-mode nodes in the simulated cluster.
const NODES: usize = 3;

/// The fiber of an attempt.
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
enum Fiber {
    Regular,
    /// The speculative fiber with this index. The index counts the speculative
    /// fibers in the order in which they started, from 0.
    Speculative(usize),
}

/// How an attempt ended. `NoResponse` means that the driver stopped tracking
/// the attempt, because another fiber finished the request first.
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
enum Outcome {
    NoResponse,
    Success,
    Failure,
}

/// One attempt, flattened from the [`StructuredHistory`] of the driver.
#[derive(Debug, Clone)]
struct Attempt {
    fiber: Fiber,
    outcome: Outcome,
    node: SocketAddr,
    sent: TimePoint,
    finished: Option<TimePoint>,
}

impl Attempt {
    /// The sort key of an attempt: its end time. With this key, the sequence
    /// of attempts shows the timeline of the request. An attempt that did not
    /// end has no end time. For such an attempt, the key is its send time,
    /// which is the last known fact about it.
    fn ordering_key(&self) -> TimePoint {
        self.finished.unwrap_or(self.sent)
    }
}

impl fmt::Display for Attempt {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?} {:?} on {}", self.fiber, self.outcome, self.node)
    }
}

/// The node that the test expects for an attempt, compared with the nodes of
/// the earlier attempts.
#[derive(Debug, Copy, Clone)]
enum ExpectedNode {
    /// Any node.
    Any,
    /// A node that no earlier attempt went to.
    Unique,
}

/// One attempt in the expected sequence.
#[derive(Debug, Copy, Clone)]
struct ExpectedAttempt {
    fiber: Fiber,
    outcome: Outcome,
    node: ExpectedNode,
}

fn regular(outcome: Outcome, node: ExpectedNode) -> ExpectedAttempt {
    ExpectedAttempt {
        fiber: Fiber::Regular,
        outcome,
        node,
    }
}

fn speculative(index: usize, outcome: Outcome, node: ExpectedNode) -> ExpectedAttempt {
    ExpectedAttempt {
        fiber: Fiber::Speculative(index),
        outcome,
        node,
    }
}

impl fmt::Display for ExpectedAttempt {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{:?} {:?} on {:?} node",
            self.fiber, self.outcome, self.node
        )
    }
}

/// Flattens the history of the single request of the test into one timeline.
/// The timeline contains the attempts of all the fibers of the request.
///
/// Also returns the number of speculative fibers that started. A fiber that
/// finds no node left in the plan sends nothing. It is in this count, but it
/// has no attempt in the timeline.
///
/// Returns an error, and does not panic, if the history does not hold exactly
/// one request. Thus, the failing case does not hide the cases after it.
fn attempts_of_single_request(history: StructuredHistory) -> Result<(Vec<Attempt>, usize), String> {
    let [request] = history.requests.as_slice() else {
        return Err(format!(
            "expected the history of 1 request, got {}",
            history.requests.len()
        ));
    };

    let fibers = std::iter::once((Fiber::Regular, &request.non_speculative_fiber)).chain(
        request
            .speculative_fibers
            .iter()
            .enumerate()
            .map(|(index, fiber)| (Fiber::Speculative(index), fiber)),
    );

    let mut attempts: Vec<Attempt> = fibers
        .flat_map(|(fiber, history)| {
            history.attempts.iter().map(move |attempt| {
                let (outcome, finished) = match &attempt.result {
                    Some(AttemptResult::Success(time)) => (Outcome::Success, Some(*time)),
                    Some(AttemptResult::Error(time, _, _)) => (Outcome::Failure, Some(*time)),
                    None => (Outcome::NoResponse, None),
                };
                Attempt {
                    fiber,
                    outcome,
                    node: attempt.node_addr,
                    sent: attempt.send_time,
                    finished,
                }
            })
        })
        .collect();

    attempts.sort_by_key(Attempt::ordering_key);
    Ok((attempts, request.speculative_fibers.len()))
}

/// Compares the attempts of a request with the expected sequence. On a
/// mismatch, it returns the mismatch and does not panic. Thus, one failing
/// case does not hide the cases after it.
fn check_attempts(expected: &[ExpectedAttempt], actual: &[Attempt]) -> Result<(), String> {
    if expected.len() != actual.len() {
        return Err(format!(
            "expected {} attempts, got {}",
            expected.len(),
            actual.len()
        ));
    }

    let mut nodes_seen: Vec<SocketAddr> = Vec::new();
    for (i, (expected, actual)) in expected.iter().zip(actual).enumerate() {
        if actual.fiber != expected.fiber || actual.outcome != expected.outcome {
            return Err(format!("[{i}] expected {expected}, got {actual}"));
        }
        if matches!(expected.node, ExpectedNode::Unique) && nodes_seen.contains(&actual.node) {
            return Err(format!(
                "[{i}] expected a node not tried before, got {}, already tried: {}",
                actual.node,
                nodes_seen
                    .iter()
                    .map(SocketAddr::to_string)
                    .collect::<Vec<_>>()
                    .join(", ")
            ));
        }
        nodes_seen.push(actual.node);
    }

    Ok(())
}

/// The prepared statement id in the forged `RESULT:Prepared`. The value is
/// arbitrary, because nothing finds a statement by this id. But the value must
/// be the same in all responses. Then, if the driver prepares the statement
/// again, it gets the same id as before.
const PREPARED_ID: &[u8] = &[0xDE, 0xAD, 0xBE, 0xEF];

/// The rules that each dry node needs, so that a `Session` can connect to it
/// and run a statement. A test case puts its own rules in front of these.
///
/// In dry mode, the proxy drops each request that matches no rule. The
/// requests that a session sends while it connects have no timeout. Thus, a
/// missing rule here does not make the test fail. It makes the test hang. Two
/// of these rules are easy to forget:
///
/// - The proxy must answer REGISTER. The control connection subscribes to
///   events. Then it waits for `READY`, and the session is not usable before
///   that.
/// - The proxy must answer the metadata reads of the control connection with
///   an *error*. It must not drop them. A session tolerates a failed metadata
///   read. It continues with a peer list built from the contact points. This
///   is exactly the topology that this test needs. But the read must actually
///   fail.
fn handshake_rules() -> Vec<RequestRule> {
    vec![
        RequestRule(
            Condition::RequestOpcode(RequestOpcode::Options),
            RequestReaction::forge_response(Arc::new(|frame: RequestFrame| {
                ResponseFrame::forged_supported(frame.params, &HashMap::new()).unwrap()
            })),
        ),
        RequestRule(
            Condition::RequestOpcode(RequestOpcode::Startup),
            RequestReaction::forge_response(Arc::new(|frame: RequestFrame| {
                ResponseFrame::forged_ready(frame.params)
            })),
        ),
        RequestRule(
            Condition::RequestOpcode(RequestOpcode::Register),
            RequestReaction::forge_response(Arc::new(|frame: RequestFrame| {
                ResponseFrame::forged_ready(frame.params)
            })),
        ),
        RequestRule(
            Condition::RequestOpcode(RequestOpcode::Query),
            RequestReaction::forge_response(Arc::new(|frame: RequestFrame| {
                ResponseFrame::forged_error(
                    frame.params,
                    DbError::ServerError,
                    Some("No cluster behind this proxy."),
                )
                .unwrap()
            })),
        ),
        // Only the statement under test receives a usable prepared statement.
        // Only the control connection sends other PREPARE requests. It sends
        // them for its metadata reads. It has registered for events. Thus, the
        // condition `ConnectionRegisteredAnyEvent` tells it apart. The proxy
        // sets this flag on REGISTER, and the driver registers before it uses
        // the connection.
        // The forged metadata below describes only the statement under test. If
        // the metadata reads received it, they would fail while they serialize
        // their values, not while they read the response.
        //
        // The statement binds exactly one `int`. The response must have two
        // properties:
        // - It must declare one bound column. With no bound column, the driver
        //   fails to serialize the value (`WrongColumnCount`). This occurs
        //   before the driver sends a request. Thus, the retry and speculative
        //   logic never runs.
        // - It must declare no partition key indexes. `forged_prepared` never
        //   declares them. Then the statement is not token-aware, and the load
        //   balancer puts every node in the plan. The "a node not tried before"
        //   expectations need this. With a key index, the driver would try to
        //   find replicas in a keyspace that the simulated cluster does not
        //   have.
        RequestRule(
            Condition::RequestOpcode(RequestOpcode::Prepare)
                .and(Condition::not(Condition::ConnectionRegisteredAnyEvent)),
            RequestReaction::forge_response(Arc::new(|frame: RequestFrame| {
                let bound_column = ColumnSpec::borrowed(
                    "a",
                    ColumnType::Native(NativeType::Int),
                    TableSpec::borrowed("ks", "t"),
                );
                ResponseFrame::forged_prepared(frame.params, PREPARED_ID, &[bound_column], &[])
                    .unwrap()
            })),
        ),
        RequestRule(
            Condition::RequestOpcode(RequestOpcode::Prepare),
            RequestReaction::forge_response(Arc::new(|frame: RequestFrame| {
                ResponseFrame::forged_error(
                    frame.params,
                    DbError::ServerError,
                    Some("No cluster behind this proxy."),
                )
                .unwrap()
            })),
        ),
        // The statement succeeds, unless a rule of the test case gives a
        // different result. The proxy checks the rules of the case first.
        RequestRule(
            Condition::RequestOpcode(RequestOpcode::Execute),
            RequestReaction::forge_response(Arc::new(|frame: RequestFrame| {
                ResponseFrame::forged_void(frame.params)
            })),
        ),
    ]
}

/// What the proxy does to an attempt.
enum ProxyRule {
    /// Answer the next attempt, on any node of the cluster, with an error. For
    /// this error, the retry policy retries on the next target.
    Fail,
    /// Answer every attempt with the error of `Fail`.
    FailAll,
    /// Hold the next attempt for `delay`, then answer it.
    Delay { delay: Duration },
    /// Never answer the next attempt.
    Never,
    /// Answer the next attempt with the error of `Fail`, but only after the
    /// driver has recorded the failures of all the other rules of the case. An
    /// event orders this answer, not a time. A case with this rule has only
    /// `Fail` rules besides it.
    FailLast,
    /// Answer the next attempt with success, but only after the driver has
    /// recorded the failures of all the other rules of the case. A case with
    /// this rule has only `Fail` rules besides it.
    SucceedLast,
}

impl ProxyRule {
    fn is_last(&self) -> bool {
        matches!(self, ProxyRule::FailLast | ProxyRule::SucceedLast)
    }
}

/// True only for the first evaluation in the whole cluster. The test installs
/// the rules on every node. A counter in the condition itself would let the
/// rule fire one time on each node. These rules must fire one time in total.
fn once_in_the_cluster() -> Condition {
    Condition::TrueForLimitedTimesShared(Arc::new(AtomicUsize::new(1)))
}

/// The rules of a case, and what the case needs to open the gate of its
/// rules "last".
struct CaseRules {
    rules: Vec<RequestRule>,
    /// The rules "last" wait for this gate.
    gate: Gate,
    /// The gate opens when the driver has recorded this many attempt
    /// failures. It is the number of `Fail` rules of the case.
    failures_before_gate: usize,
}

fn into_request_rules(rules: Vec<ProxyRule>) -> CaseRules {
    // An error for which the retry policy below returns `RetryNextTarget`.
    let error = example_db_errors::overloaded;
    let on_an_attempt = || Condition::RequestOpcode(RequestOpcode::Execute);

    // A rule "last" answers after the gate opens. The gate opens when the
    // driver has recorded the failures of the other rules. See
    // `GateOpeningListener`. The proxy cannot open the gate. It knows only
    // that it queued a failure for the driver. It does not know that the
    // driver has read the failure. The driver could read the held answer
    // first, on another connection.
    let gate = Gate::new();
    let failures_before_gate = rules
        .iter()
        .filter(|rule| matches!(rule, ProxyRule::Fail))
        .count();
    if rules.iter().any(ProxyRule::is_last) {
        assert!(
            failures_before_gate > 0
                && rules
                    .iter()
                    .all(|rule| rule.is_last() || matches!(rule, ProxyRule::Fail)),
            "A case with a `last` rule needs at least one `Fail` rule, and no other rule"
        );
    }

    let rules = rules
        .into_iter()
        .map(|rule| {
            let RequestRule(condition, reaction) = match rule {
                ProxyRule::Fail => RequestRule(
                    on_an_attempt().and(once_in_the_cluster()),
                    RequestReaction::forge_with_error_lazy(Box::new(error)),
                ),
                ProxyRule::FailAll => RequestRule(
                    on_an_attempt(),
                    RequestReaction::forge_with_error_lazy(Box::new(error)),
                ),
                ProxyRule::Delay { delay } => RequestRule(
                    on_an_attempt().and(once_in_the_cluster()),
                    RequestReaction::forge_response_with_delay(
                        delay,
                        Arc::new(|frame: RequestFrame| ResponseFrame::forged_void(frame.params)),
                    ),
                ),
                // A dropped frame gets no response. To the driver, this looks
                // like a stalled node.
                ProxyRule::Never => RequestRule(
                    on_an_attempt().and(once_in_the_cluster()),
                    RequestReaction::drop_frame(),
                ),
                ProxyRule::FailLast => RequestRule(
                    on_an_attempt().and(once_in_the_cluster()),
                    RequestReaction::forge_response_after_gate(
                        gate.clone(),
                        Arc::new(move |frame: RequestFrame| {
                            ResponseFrame::forged_error(frame.params.for_response(), error(), None)
                                .unwrap()
                        }),
                    ),
                ),
                ProxyRule::SucceedLast => RequestRule(
                    on_an_attempt().and(once_in_the_cluster()),
                    RequestReaction::forge_response_after_gate(
                        gate.clone(),
                        Arc::new(|frame: RequestFrame| ResponseFrame::forged_void(frame.params)),
                    ),
                ),
            };
            RequestRule(condition, reaction)
        })
        .collect();

    CaseRules {
        rules,
        gate,
        failures_before_gate,
    }
}

/// Records the history, and opens a gate after the driver recorded a given
/// number of attempt failures.
///
/// The cases with a rule "last" need the answer of that rule to come after
/// the failures of the other attempts. Else, the driver could finish the
/// request before it records these failures. The gate is closed until then.
///
/// The listener opens the gate from `log_attempt_error`. The driver calls
/// this method after it decided to retry. It calls it without an await point
/// in between. Thus, the failure is in the history before the held answer can
/// reach the driver.
struct GateOpeningListener {
    history: Arc<HistoryCollector>,
    gate: Gate,
    failures_before_gate: usize,
    failures_recorded: AtomicUsize,
}

impl GateOpeningListener {
    fn new(history: Arc<HistoryCollector>, case_rules: &CaseRules) -> Self {
        Self {
            history,
            gate: case_rules.gate.clone(),
            failures_before_gate: case_rules.failures_before_gate,
            failures_recorded: AtomicUsize::new(0),
        }
    }
}

impl fmt::Debug for GateOpeningListener {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("GateOpeningListener")
            .finish_non_exhaustive()
    }
}

impl HistoryListener for GateOpeningListener {
    fn log_request_start(&self) -> RequestId {
        self.history.log_request_start()
    }

    fn log_request_success(&self, request_id: RequestId) {
        self.history.log_request_success(request_id)
    }

    fn log_request_error(&self, request_id: RequestId, error: &RequestError) {
        self.history.log_request_error(request_id, error)
    }

    fn log_new_speculative_fiber(&self, request_id: RequestId) -> SpeculativeId {
        self.history.log_new_speculative_fiber(request_id)
    }

    fn log_attempt_start(
        &self,
        request_id: RequestId,
        speculative_id: Option<SpeculativeId>,
        node_addr: SocketAddr,
    ) -> AttemptId {
        self.history
            .log_attempt_start(request_id, speculative_id, node_addr)
    }

    fn log_attempt_success(&self, attempt_id: AttemptId) {
        self.history.log_attempt_success(attempt_id)
    }

    fn log_attempt_error(
        &self,
        attempt_id: AttemptId,
        error: &RequestAttemptError,
        retry_decision: &RetryDecision,
    ) {
        // Record first. Then open the gate.
        self.history
            .log_attempt_error(attempt_id, error, retry_decision);
        let failures_recorded = self.failures_recorded.fetch_add(1, Ordering::SeqCst) + 1;
        if failures_recorded == self.failures_before_gate {
            self.gate.open();
        }
    }
}

fn fail() -> ProxyRule {
    ProxyRule::Fail
}

fn fail_all() -> ProxyRule {
    ProxyRule::FailAll
}

fn fail_last() -> ProxyRule {
    ProxyRule::FailLast
}

fn succeed_last() -> ProxyRule {
    ProxyRule::SucceedLast
}

fn delay(delay: Duration) -> ProxyRule {
    ProxyRule::Delay { delay }
}

fn never() -> ProxyRule {
    ProxyRule::Never
}

/// Retries an `Overloaded` error on the next target, at most `max_retries`
/// times in total. Does not retry any other error. On purpose, this is not one
/// of the policies of the driver. The test must know exactly what the retry
/// policy does. Then, the only behavior left to observe is the behavior of
/// speculative execution.
#[derive(Debug)]
struct TestRetryPolicy {
    max_retries: usize,
}

impl RetryPolicy for TestRetryPolicy {
    fn new_session(&self) -> Box<dyn RetrySession> {
        Box::new(TestRetrySession {
            retries_done: 0,
            max_retries: self.max_retries,
        })
    }
}

struct TestRetrySession {
    retries_done: usize,
    max_retries: usize,
}

impl RetrySession for TestRetrySession {
    fn decide_should_retry(&mut self, request: RequestInfo) -> RetryDecision {
        if self.retries_done >= self.max_retries {
            return RetryDecision::DontRetry;
        }
        self.retries_done += 1;
        match request.error {
            RequestAttemptError::DbError(DbError::Overloaded, _) => {
                RetryDecision::RetryNextTarget(None)
            }
            _ => RetryDecision::DontRetry,
        }
    }

    fn reset(&mut self) {
        self.retries_done = 0;
    }
}

fn retry_policy(max_retries: usize) -> Arc<dyn RetryPolicy> {
    Arc::new(TestRetryPolicy { max_retries })
}

fn speculative_policy(max_retry_count: i32) -> Arc<dyn SpeculativeExecutionPolicy> {
    Arc::new(SimpleSpeculativeExecutionPolicy {
        max_retry_count: max_retry_count as usize,
        retry_interval: TICK * 2,
    })
}

struct TestCase {
    name: &'static str,
    proxy_rules: Vec<ProxyRule>,
    speculative: Arc<dyn SpeculativeExecutionPolicy>,
    retries: Arc<dyn RetryPolicy>,
    expected_attempts: Vec<ExpectedAttempt>,
    /// How many speculative fibers start. This count can be higher than the
    /// number of speculative fibers in `expected_attempts`. A fiber that finds
    /// no node left in the plan sends nothing.
    expected_speculative_fibers: usize,
    expected_success: bool,
}

#[tokio::test]
async fn test_speculative_execution_and_retries() {
    setup_tracing();
    use ExpectedNode::{Any, Unique};
    use Outcome::{Failure, NoResponse, Success};

    let test_cases = vec![
        TestCase {
            name: "no failure, no speculation",
            proxy_rules: vec![],
            speculative: speculative_policy(3),
            retries: retry_policy(3),
            expected_attempts: vec![regular(Success, Any)],
            expected_speculative_fibers: 0,
            expected_success: true,
        },
        TestCase {
            name: "one failure, retried on the next node",
            proxy_rules: vec![fail()],
            speculative: speculative_policy(3),
            retries: retry_policy(3),
            expected_attempts: vec![regular(Failure, Any), regular(Success, Unique)],
            expected_speculative_fibers: 0,
            expected_success: true,
        },
        TestCase {
            // The regular fiber gets no response. Thus, a speculative fiber
            // finishes the request. This is the purpose of the feature.
            name: "regular fiber hangs, speculative one succeeds",
            proxy_rules: vec![never()],
            speculative: speculative_policy(3),
            retries: retry_policy(3),
            expected_attempts: vec![regular(NoResponse, Any), speculative(0, Success, Unique)],
            expected_speculative_fibers: 1,
            expected_success: true,
        },
        TestCase {
            name: "regular fiber hangs, speculative one fails and then succeeds",
            proxy_rules: vec![never(), fail()],
            speculative: speculative_policy(3),
            retries: retry_policy(3),
            expected_attempts: vec![
                regular(NoResponse, Any),
                speculative(0, Failure, Unique),
                speculative(0, Success, Unique),
            ],
            expected_speculative_fibers: 1,
            expected_success: true,
        },
        TestCase {
            // The proxy holds the failure of the regular fiber. It sends the
            // failure after the two speculative attempts failed. Then the
            // regular fiber ends the request.
            name: "regular fiber fails last, after two speculative failures",
            proxy_rules: vec![fail_last(), fail(), fail()],
            speculative: speculative_policy(3),
            retries: retry_policy(3),
            expected_attempts: vec![
                speculative(0, Failure, Any),
                speculative(0, Failure, Unique),
                regular(Failure, Unique),
            ],
            expected_speculative_fibers: 2,
            expected_success: false,
        },
        TestCase {
            name: "regular fiber succeeds last, after two speculative failures",
            proxy_rules: vec![succeed_last(), fail(), fail()],
            speculative: speculative_policy(3),
            retries: retry_policy(3),
            expected_attempts: vec![
                speculative(0, Failure, Any),
                speculative(0, Failure, Unique),
                regular(Success, Unique),
            ],
            expected_speculative_fibers: 1,
            expected_success: true,
        },
        TestCase {
            // Every node fails. Thus, each retry goes to the next node of the
            // query plan. The retries stop when the plan has no more nodes.
            // The plan has only three nodes.
            name: "retries exhaust the query plan",
            proxy_rules: vec![fail_all()],
            speculative: speculative_policy(3),
            retries: retry_policy(3),
            expected_attempts: vec![
                regular(Failure, Any),
                regular(Failure, Unique),
                regular(Failure, Unique),
            ],
            expected_speculative_fibers: 1,
            expected_success: false,
        },
        TestCase {
            // The retry policy allows only one retry. Thus, the regular fiber
            // stops after two attempts. The third attempt comes from a
            // speculative fiber.
            name: "retries exhausted, speculative fiber takes the last attempt",
            proxy_rules: vec![fail_all()],
            speculative: speculative_policy(3),
            retries: retry_policy(1),
            expected_attempts: vec![
                regular(Failure, Any),
                regular(Failure, Unique),
                speculative(0, Failure, Unique),
            ],
            expected_speculative_fibers: 2,
            expected_success: false,
        },
        TestCase {
            name: "one speculative fiber allowed",
            proxy_rules: vec![never(), delay(TICK * 3)],
            speculative: speculative_policy(1),
            retries: retry_policy(3),
            expected_attempts: vec![regular(NoResponse, Any), speculative(0, Success, Unique)],
            expected_speculative_fibers: 1,
            expected_success: true,
        },
        TestCase {
            name: "two speculative fibers allowed",
            proxy_rules: vec![never(), never(), delay(TICK * 3)],
            speculative: speculative_policy(2),
            retries: retry_policy(3),
            expected_attempts: vec![
                regular(NoResponse, Any),
                speculative(0, NoResponse, Unique),
                speculative(1, Success, Unique),
            ],
            expected_speculative_fibers: 2,
            expected_success: true,
        },
        TestCase {
            // The policy allows three speculative fibers, and all three start.
            // But the plan has only three nodes, and the regular fiber and the
            // first two speculative fibers use them. Thus, the third
            // speculative fiber finds no node and sends nothing.
            name: "three speculative fibers allowed, but only three nodes exist",
            proxy_rules: vec![never(), never(), delay(TICK * 5)],
            speculative: speculative_policy(3),
            retries: retry_policy(3),
            expected_attempts: vec![
                regular(NoResponse, Any),
                speculative(0, NoResponse, Unique),
                speculative(1, Success, Unique),
            ],
            expected_speculative_fibers: 3,
            expected_success: true,
        },
    ];

    let res = with_3_dry_nodes(|contact_points, mut running_proxy| async move {
        // The test reports every case before it fails. Thus, one broken case
        // does not hide the state of the other cases.
        let mut failures: Vec<String> = Vec::new();

        for case in test_cases {
            let TestCase {
                name,
                proxy_rules,
                speculative,
                retries,
                expected_attempts,
                expected_speculative_fibers,
                expected_success,
            } = case;
            // The rules of the case come first, because the first matching
            // rule wins. The handshake rules must stay installed, because each
            // case builds its own session. This session must connect again.
            let case_rules = into_request_rules(proxy_rules);
            let rules = [case_rules.rules.clone(), handshake_rules()].concat();
            for node in running_proxy.running_nodes.iter_mut() {
                node.change_request_rules(Some(rules.clone()));
            }

            // Each case has its own session. Thus, no state goes from one case
            // to the next, for example a warm connection pool or a retry
            // session. A long metadata refresh interval prevents refresh
            // requests in the middle of a case.
            let mut builder = SessionBuilder::new()
                .cluster_metadata_refresh_interval(Duration::from_secs(600))
                .default_execution_profile_handle(
                    ExecutionProfile::builder()
                        .speculative_execution_policy(Some(speculative))
                        .retry_policy(retries)
                        // Much longer than any case waits. Thus, this timeout
                        // never fires in a healthy run. Its purpose: a case
                        // that stops making progress fails quickly. It does
                        // not hang until the default timeout. For example, a
                        // case stops if the simulated cluster starts with
                        // fewer nodes than the expectations need.
                        .request_timeout(Some(TICK * 30))
                        .build()
                        .into_handle(),
                );
            for contact_point in contact_points {
                builder = builder.known_node_addr(contact_point);
            }
            let session: Session = builder.build().await.unwrap();

            let history = Arc::new(HistoryCollector::new());
            let mut statement = Statement::from("INSERT INTO ks.t (a) VALUES (?)");
            statement.set_is_idempotent(true); // Speculative execution fires only for idempotent statements.
            statement.set_history_listener(Arc::new(GateOpeningListener::new(
                Arc::clone(&history),
                &case_rules,
            )));

            let result = session.query_unpaged(statement, (3,)).await;

            // A failing request must fail with the error that the proxy
            // sends. Another error, for example a request timeout, would mean
            // that the driver stalled.
            let unexpected_result = match (&result, expected_success) {
                (Ok(_), true) => None,
                (Ok(_), false) => Some("expected the request to fail, it succeeded".to_string()),
                (Err(err), true) => {
                    Some(format!("expected the request to succeed, it failed: {err}"))
                }
                (
                    Err(ExecutionError::LastAttemptError(RequestAttemptError::DbError(
                        DbError::Overloaded,
                        _,
                    ))),
                    false,
                ) => None,
                (Err(err), false) => Some(format!(
                    "expected the request to fail with `Overloaded`, it failed: {err}"
                )),
            };
            if let Some(unexpected_result) = unexpected_result {
                failures.push(format!("{name}: {unexpected_result}"));
            }

            let (attempts, speculative_fibers) =
                match attempts_of_single_request(history.clone_structured_history()) {
                    Ok(timeline) => timeline,
                    Err(mismatch) => {
                        failures.push(format!("{name}: {mismatch}"));
                        continue;
                    }
                };
            if speculative_fibers != expected_speculative_fibers {
                failures.push(format!(
                    "{name}: expected {expected_speculative_fibers} speculative fibers to start, \
                     {speculative_fibers} started"
                ));
            }
            if let Err(mismatch) = check_attempts(&expected_attempts, &attempts) {
                failures.push(format!(
                    "{}: {mismatch}\n  expected: {}\n  actual:   {}",
                    name,
                    expected_attempts
                        .iter()
                        .map(ExpectedAttempt::to_string)
                        .collect::<Vec<_>>()
                        .join(" | "),
                    attempts
                        .iter()
                        .map(Attempt::to_string)
                        .collect::<Vec<_>>()
                        .join(" | "),
                ));
            }
        }

        assert!(failures.is_empty(), "{}", failures.join("\n"));

        running_proxy
    })
    .await;

    match res {
        Ok(()) => (),
        Err(ProxyError::Worker(WorkerError::DriverDisconnected(_))) => (),
        Err(err) => panic!("{}", err),
    }
}

/// Runs `test` on three `scylla-proxy` nodes in dry mode. These are simulated
/// nodes with no ScyllaDB behind them. `test` gets the node addresses to use
/// as contact points.
///
/// This function is like `crate::utils::test_with_3_node_cluster`, but it
/// needs no cluster. It also needs no address translation. A session that
/// cannot read the metadata of the cluster uses a peer list built from its
/// contact points. Thus, the session knows only the proxy addresses.
async fn with_3_dry_nodes<F, Fut>(test: F) -> Result<(), ProxyError>
where
    F: FnOnce([SocketAddr; NODES], RunningProxy) -> Fut,
    Fut: Future<Output = RunningProxy>,
{
    // Each call gives a different address.
    let addresses: [SocketAddr; NODES] =
        std::array::from_fn(|_| SocketAddr::new(get_exclusive_local_address(), 9042));

    let proxy = Proxy::new(addresses.map(|address| {
        Node::builder()
            .proxy_address(address)
            .request_rules(handshake_rules())
            .build_dry_mode()
    }));

    let running_proxy = proxy.run().await.unwrap();
    let running_proxy = test(addresses, running_proxy).await;
    running_proxy.finish().await
}
