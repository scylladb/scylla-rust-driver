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
//! - the fiber of each attempt,
//! - how each attempt ended,
//! - whether an attempt went to a node that an earlier attempt used.
//!
//! The history of the driver is the source of truth. All the other parts are
//! simulated. The three nodes are `scylla-proxy` nodes in dry mode, with no
//! ScyllaDB behind them. They answer each request from the rules below. Thus:
//! - The cases can depend on time and still run fast. `TICK` is the unit of
//!   the interval of the speculative policy.
//! - A real cluster adds variance to these time windows. Dry nodes do not.
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
use std::sync::atomic::AtomicUsize;
use std::time::Duration;

use scylla::client::execution_profile::ExecutionProfile;
use scylla::client::session::Session;
use scylla::client::session_builder::SessionBuilder;
use scylla::errors::{DbError, RequestAttemptError};
use scylla::frame::response::result::{ColumnSpec, ColumnType, NativeType, TableSpec};
use scylla::observability::history::{
    AttemptResult, HistoryCollector, StructuredHistory, TimePoint,
};
use scylla::policies::retry::{RequestInfo, RetryDecision, RetryPolicy, RetrySession};
use scylla::policies::speculative_execution::{
    SimpleSpeculativeExecutionPolicy, SpeculativeExecutionPolicy,
};
use scylla::statement::unprepared::Statement;
use scylla_proxy::{
    Condition, Node, Proxy, ProxyError, Reaction as _, RequestFrame, RequestOpcode,
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
    Speculative,
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

fn speculative(outcome: Outcome, node: ExpectedNode) -> ExpectedAttempt {
    ExpectedAttempt {
        fiber: Fiber::Speculative,
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
fn attempts_of_single_request(history: StructuredHistory) -> Vec<Attempt> {
    let request = history
        .requests
        .first()
        .expect("The request left no history");

    let fibers = std::iter::once((Fiber::Regular, &request.non_speculative_fiber)).chain(
        request
            .speculative_fibers
            .iter()
            .map(|fiber| (Fiber::Speculative, fiber)),
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
    attempts
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

/// Separates the statement under test from the queries that the driver sends
/// for itself. Both kinds reach the proxy as PREPARE and EXECUTE requests.
const STATEMENT_MARKER: &str = "ks.t";

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
        // The forged metadata below describes only that statement. If the
        // queries of the driver itself received it, they would fail while they
        // serialize their values, not while they read the response.
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
            Condition::RequestOpcode(RequestOpcode::Prepare).and(
                Condition::BodyContainsCaseSensitive(STATEMENT_MARKER.as_bytes().into()),
            ),
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
    /// this error, the retry policy retries on the next target. If `delay` is
    /// set, wait for `delay` before the answer.
    Fail { delay: Option<Duration> },
    /// Answer every attempt with the error of `Fail`.
    FailAll,
    /// Hold the next attempt for `delay`, then answer it.
    Delay { delay: Duration },
    /// Never answer the next attempt.
    Never,
}

fn fail(delay: Option<Duration>) -> ProxyRule {
    ProxyRule::Fail { delay }
}

fn fail_all() -> ProxyRule {
    ProxyRule::FailAll
}

fn delay(delay: Duration) -> ProxyRule {
    ProxyRule::Delay { delay }
}

fn never() -> ProxyRule {
    ProxyRule::Never
}

/// True only for the first evaluation in the whole cluster. The test installs
/// the rules on every node. A counter in the condition itself would let the
/// rule fire one time on each node. These rules must fire one time in total.
fn once_in_the_cluster() -> Condition {
    Condition::TrueForLimitedTimesShared(Arc::new(AtomicUsize::new(1)))
}

fn into_request_rules(rules: Vec<ProxyRule>) -> Vec<RequestRule> {
    // An error for which the retry policy below returns `RetryNextTarget`.
    let error = example_db_errors::overloaded;
    let on_an_attempt = || Condition::RequestOpcode(RequestOpcode::Execute);

    rules
        .into_iter()
        .map(|rule| match rule {
            ProxyRule::Fail { delay } => RequestRule(
                on_an_attempt().and(once_in_the_cluster()),
                RequestReaction::forge_with_error_lazy_delay(Box::new(error), delay),
            ),
            ProxyRule::FailAll => RequestRule(
                on_an_attempt(),
                RequestReaction::forge_with_error_lazy_delay(Box::new(error), None),
            ),
            ProxyRule::Delay { delay } => RequestRule(
                on_an_attempt().and(once_in_the_cluster()),
                RequestReaction::forge_response_with_delay(
                    delay,
                    Arc::new(|frame: RequestFrame| ResponseFrame::forged_void(frame.params)),
                ),
            ),
            // A dropped frame gets no response. To the driver, this looks like
            // a stalled node.
            ProxyRule::Never => RequestRule(
                on_an_attempt().and(once_in_the_cluster()),
                RequestReaction::drop_frame(),
            ),
        })
        .collect()
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
            expected_success: true,
        },
        TestCase {
            name: "one failure, retried on the next node",
            proxy_rules: vec![fail(None)],
            speculative: speculative_policy(3),
            retries: retry_policy(3),
            expected_attempts: vec![regular(Failure, Any), regular(Success, Unique)],
            expected_success: true,
        },
        TestCase {
            // The regular fiber gets no response. Thus, a speculative fiber
            // finishes the request. This is the purpose of the feature.
            name: "regular fiber hangs, speculative one succeeds",
            proxy_rules: vec![never()],
            speculative: speculative_policy(3),
            retries: retry_policy(3),
            expected_attempts: vec![regular(NoResponse, Any), speculative(Success, Unique)],
            expected_success: true,
        },
        TestCase {
            name: "regular fiber hangs, speculative one fails and then succeeds",
            proxy_rules: vec![never(), fail(None)],
            speculative: speculative_policy(3),
            retries: retry_policy(3),
            expected_attempts: vec![
                regular(NoResponse, Any),
                speculative(Failure, Unique),
                speculative(Success, Unique),
            ],
            expected_success: true,
        },
        TestCase {
            // The proxy delays the failure of the regular fiber. During the
            // delay, two speculative attempts fail. Then the regular fiber ends
            // the request.
            name: "regular fiber fails last, after two speculative failures",
            proxy_rules: vec![fail(Some(TICK * 4)), fail(None), fail(None)],
            speculative: speculative_policy(3),
            retries: retry_policy(3),
            expected_attempts: vec![
                speculative(Failure, Any),
                speculative(Failure, Unique),
                regular(Failure, Unique),
            ],
            expected_success: false,
        },
        TestCase {
            name: "regular fiber succeeds last, after two speculative failures",
            proxy_rules: vec![delay(TICK * 3), fail(None), fail(None)],
            speculative: speculative_policy(3),
            retries: retry_policy(3),
            expected_attempts: vec![
                speculative(Failure, Any),
                speculative(Failure, Unique),
                regular(Success, Unique),
            ],
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
                speculative(Failure, Unique),
            ],
            expected_success: false,
        },
        TestCase {
            name: "one speculative fiber allowed",
            proxy_rules: vec![never(), delay(TICK * 3)],
            speculative: speculative_policy(1),
            retries: retry_policy(3),
            expected_attempts: vec![regular(NoResponse, Any), speculative(Success, Unique)],
            expected_success: true,
        },
        TestCase {
            name: "two speculative fibers allowed",
            proxy_rules: vec![never(), never(), delay(TICK * 3)],
            speculative: speculative_policy(2),
            retries: retry_policy(3),
            expected_attempts: vec![
                regular(NoResponse, Any),
                speculative(NoResponse, Unique),
                speculative(Success, Unique),
            ],
            expected_success: true,
        },
        TestCase {
            // The policy allows three fibers, but only three nodes exist. The
            // number of fibers that start is not more than the number of nodes.
            name: "three speculative fibers allowed, but only three nodes exist",
            proxy_rules: vec![never(), never(), delay(TICK * 5)],
            speculative: speculative_policy(3),
            retries: retry_policy(3),
            expected_attempts: vec![
                regular(NoResponse, Any),
                speculative(NoResponse, Unique),
                speculative(Success, Unique),
            ],
            expected_success: true,
        },
    ];

    let res = with_3_dry_nodes(|contact_points, mut running_proxy| async move {
        // The test reports every case before it fails. Thus, one broken case
        // does not hide the state of the other cases.
        let mut failures: Vec<String> = Vec::new();

        for case in test_cases {
            // The rules of the case come first, because the first matching
            // rule wins. The handshake rules must stay installed, because each
            // case builds its own session. This session must connect again.
            let rules = [into_request_rules(case.proxy_rules), handshake_rules()].concat();
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
                        .speculative_execution_policy(Some(Arc::clone(&case.speculative)))
                        .retry_policy(Arc::clone(&case.retries))
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
            statement.set_history_listener(history.clone());

            let result = session.query_unpaged(statement, (3,)).await;

            if result.is_ok() != case.expected_success {
                failures.push(match result {
                    Ok(_) => {
                        format!("{}: expected the request to fail, it succeeded", case.name)
                    }
                    Err(err) => format!(
                        "{}: expected the request to succeed, it failed: {err}",
                        case.name
                    ),
                });
            }

            let attempts = attempts_of_single_request(history.clone_structured_history());
            if let Err(mismatch) = check_attempts(&case.expected_attempts, &attempts) {
                failures.push(format!(
                    "{}: {mismatch}\n  expected: {}\n  actual:   {}",
                    case.name,
                    case.expected_attempts
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
