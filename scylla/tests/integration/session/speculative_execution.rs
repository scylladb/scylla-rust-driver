//! Tests of speculative execution together with the retry policy.
//!
//! The driver can send a request more than one time, for two different
//! reasons:
//! - The retry policy asks for one more attempt after a failure.
//! - The speculative execution policy starts one more fiber when a response
//!   is slow.
//!
//! The history of the driver is the source of truth. The test uses the proxy
//! only to make a chosen attempt fail or hang. The cases are about what
//! happens while a response is outstanding. Thus, they must use time:
//! - `TICK` is the unit of the interval of the speculative policy.
//! - A delay of `NEVER` stands for a response that never arrives.

use std::fmt;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::time::Duration;

use scylla::client::execution_profile::ExecutionProfile;
use scylla::client::session::Session;
use scylla::client::session_builder::SessionBuilder;
use scylla::errors::{DbError, RequestAttemptError};
use scylla::observability::history::{
    AttemptResult, HistoryCollector, StructuredHistory, TimePoint,
};
use scylla::policies::retry::{
    FallthroughRetryPolicy, RequestInfo, RetryDecision, RetryPolicy, RetrySession,
};
use scylla::policies::speculative_execution::{
    SimpleSpeculativeExecutionPolicy, SpeculativeExecutionPolicy,
};
use scylla::statement::unprepared::Statement;
use scylla_proxy::{
    Action, Condition, ProxyError, RequestOpcode, RequestReaction, RequestRule, ShardAwareness,
    WorkerError, example_db_errors,
};

use crate::utils::{
    PerformDDL as _, setup_tracing, test_with_3_node_cluster, unique_keyspace_name,
};

/// The time unit of these tests. One tick must be much longer than a request
/// to the local cluster.
const TICK: Duration = Duration::from_millis(200);

/// A delay that stands for "no response ever arrives".
const NEVER: Duration = Duration::from_secs(2000);

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

/// What the proxy does to an attempt.
enum ProxyRule {
    /// Answer the next request, on any node of the cluster, with `error`. If
    /// `delay` is set, wait for `delay` before the answer.
    Fail {
        delay: Option<Duration>,
        error: fn() -> DbError,
    },
    /// Answer every request with `error`.
    FailAll { error: fn() -> DbError },
    /// Hold the next request, on any node of the cluster, for `delay`. Then
    /// forward it to the node.
    Delay { delay: Duration },
}

/// True only for the first evaluation in the whole cluster. The test installs
/// the rules on every node. A counter in the condition itself would let the
/// rule fire one time on each node. These rules must fire one time in total.
fn once_in_the_cluster() -> Condition {
    Condition::TrueForLimitedTimesShared(Arc::new(AtomicUsize::new(1)))
}

fn into_request_rules(rules: Vec<ProxyRule>, opcode: RequestOpcode) -> Vec<RequestRule> {
    // The rules apply only to the statement under test. The traffic of the
    // control connection must pass with no change.
    let applies_to_the_statement = || {
        Condition::not(Condition::ConnectionRegisteredAnyEvent)
            .and(Condition::RequestOpcode(opcode))
    };

    rules
        .into_iter()
        .map(|rule| match rule {
            ProxyRule::Fail { delay, error } => RequestRule(
                applies_to_the_statement().and(once_in_the_cluster()),
                RequestReaction::forge_with_error_lazy_delay(Box::new(error), delay),
            ),
            ProxyRule::FailAll { error } => RequestRule(
                applies_to_the_statement(),
                RequestReaction::forge_with_error_lazy_delay(Box::new(error), None),
            ),
            ProxyRule::Delay { delay } => RequestRule(
                applies_to_the_statement().and(once_in_the_cluster()),
                RequestReaction {
                    to_addressee: Some(Action {
                        delay: Some(delay),
                        msg_processor: None,
                    }),
                    to_sender: None,
                    drop_connection: None,
                    feedback_channel: None,
                },
            ),
        })
        .collect()
}

fn fail(delay: Option<Duration>) -> ProxyRule {
    ProxyRule::Fail {
        delay,
        // An error for which the retry policy below returns `RetryNextTarget`.
        error: example_db_errors::overloaded,
    }
}

fn fail_all() -> ProxyRule {
    ProxyRule::FailAll {
        error: example_db_errors::overloaded,
    }
}

fn delay(delay: Duration) -> ProxyRule {
    ProxyRule::Delay { delay }
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
            proxy_rules: vec![delay(NEVER)],
            speculative: speculative_policy(3),
            retries: retry_policy(3),
            expected_attempts: vec![regular(NoResponse, Any), speculative(Success, Unique)],
            expected_success: true,
        },
        TestCase {
            name: "regular fiber hangs, speculative one fails and then succeeds",
            proxy_rules: vec![delay(NEVER), fail(None)],
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
            proxy_rules: vec![delay(NEVER), delay(TICK * 3)],
            speculative: speculative_policy(1),
            retries: retry_policy(3),
            expected_attempts: vec![regular(NoResponse, Any), speculative(Success, Unique)],
            expected_success: true,
        },
        TestCase {
            name: "two speculative fibers allowed",
            proxy_rules: vec![delay(NEVER), delay(NEVER), delay(TICK * 3)],
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
            proxy_rules: vec![delay(NEVER), delay(NEVER), delay(TICK * 5)],
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

    let ks = unique_keyspace_name();
    let res = test_with_3_node_cluster(
        ShardAwareness::QueryNode,
        |proxy_uris, translation_map, mut running_proxy| async move {
            let translation_map = Arc::new(translation_map);

            let setup_session: Session = SessionBuilder::new()
                .known_node(proxy_uris[0].as_str())
                .address_translator(translation_map.clone())
                .default_execution_profile_handle(
                    ExecutionProfile::builder()
                        .retry_policy(Arc::new(FallthroughRetryPolicy))
                        .build()
                        .into_handle(),
                )
                .build()
                .await
                .unwrap();

            setup_session
                .ddl(format!(
                    "CREATE KEYSPACE {ks} WITH REPLICATION = \
                 {{'class' : 'NetworkTopologyStrategy', 'replication_factor' : 3}}"
                ))
                .await
                .unwrap();
            setup_session.use_keyspace(&ks, false).await.unwrap();
            setup_session
                .ddl("CREATE TABLE t (a int primary key)")
                .await
                .unwrap();

            // The test reports every case before it fails. Thus, one broken
            // case does not hide the state of the other cases.
            let mut failures: Vec<String> = Vec::new();

            for case in test_cases {
                let rules = into_request_rules(case.proxy_rules, RequestOpcode::Execute);
                for node in running_proxy.running_nodes.iter_mut() {
                    node.change_request_rules((!rules.is_empty()).then(|| rules.clone()));
                }

                // Each case has its own session. Thus, no state goes from one
                // case to the next, for example a warm connection pool or a
                // retry session.
                let session: Session = SessionBuilder::new()
                    .known_node(proxy_uris[0].as_str())
                    .address_translator(translation_map.clone())
                    .default_execution_profile_handle(
                        ExecutionProfile::builder()
                            .speculative_execution_policy(Some(Arc::clone(&case.speculative)))
                            .retry_policy(Arc::clone(&case.retries))
                            .build()
                            .into_handle(),
                    )
                    .build()
                    .await
                    .unwrap();
                session.use_keyspace(&ks, false).await.unwrap();

                let history = Arc::new(HistoryCollector::new());
                let mut statement = Statement::from("INSERT INTO t (a) VALUES (?)");
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

            for node in running_proxy.running_nodes.iter_mut() {
                node.change_request_rules(None);
            }
            setup_session
                .ddl(format!("DROP KEYSPACE {ks}"))
                .await
                .unwrap();

            assert!(failures.is_empty(), "{}", failures.join("\n"));

            running_proxy
        },
    )
    .await;

    match res {
        Ok(()) => (),
        Err(ProxyError::Worker(WorkerError::DriverDisconnected(_))) => (),
        Err(err) => panic!("{}", err),
    }
}
