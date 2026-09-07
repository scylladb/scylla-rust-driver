//! Tests of speculative execution and how it interacts with the retry policy.
//!
//! A request can be attempted more than once for two independent reasons: the
//! retry policy asks for another attempt after a failure, and the speculative
//! execution policy starts another fiber when a response is slow in coming.
//! What is checked here is the sequence of attempts the two produce together -
//! which fiber each attempt belonged to, how it ended, and whether it went to a
//! node already tried.
//!
//! The driver's own history is the source of truth. Nothing else is real: the
//! three nodes are `scylla-proxy` nodes in dry mode, backed by no ScyllaDB at
//! all, answering every request from the rules below. That is what makes the
//! cases cheap enough to be written in terms of time - `TICK` is the unit the
//! speculative policy's interval is expressed in - and it removes the variance
//! a real cluster would add to those windows.
//!
//! A passing run logs one "Could not establish control connection and fetch
//! metadata" error per case. That is expected: there is no cluster to read
//! metadata from, and a session that cannot read it carries on with a peer list
//! built from its contact points, which is the topology these cases want.

use std::collections::HashMap;
use std::fmt;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::time::Duration;

use bytes::{BufMut as _, Bytes, BytesMut};
use scylla::client::execution_profile::ExecutionProfile;
use scylla::client::session::Session;
use scylla::client::session_builder::SessionBuilder;
use scylla::errors::{DbError, RequestAttemptError};
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
    RequestReaction, RequestRule, ResponseFrame, ResponseOpcode, RunningProxy, WorkerError,
    example_db_errors, get_exclusive_local_address,
};

use crate::utils::setup_tracing;

/// The unit the timings of these tests are expressed in. One tick has to be
/// comfortably longer than answering a request takes - which, with the proxy
/// forging every response in-process, is a channel send and a loopback write.
const TICK: Duration = Duration::from_millis(100);

/// How many dry-mode nodes the simulated cluster has.
const NODES: usize = 3;

/// Which fiber an attempt belonged to.
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
enum Fiber {
    Regular,
    Speculative,
}

/// How an attempt ended. `NoResponse` means the driver stopped tracking it,
/// because another fiber finished the request first.
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
enum Outcome {
    NoResponse,
    Success,
    Failure,
}

/// One attempt, flattened out of the driver's [`StructuredHistory`].
#[derive(Debug, Clone)]
struct Attempt {
    fiber: Fiber,
    outcome: Outcome,
    node: SocketAddr,
    sent: TimePoint,
    finished: Option<TimePoint>,
}

impl Attempt {
    /// Attempts are ordered by when they ended, so that the sequence reads as
    /// the request's timeline. One that never ended is placed by when it was
    /// sent, which is the last thing known about it.
    fn ordering_key(&self) -> TimePoint {
        self.finished.unwrap_or(self.sent)
    }
}

impl fmt::Display for Attempt {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?} {:?} on {}", self.fiber, self.outcome, self.node)
    }
}

/// Which node an attempt is expected to have gone to, relative to the nodes the
/// attempts before it went to.
#[derive(Debug, Copy, Clone)]
enum ExpectedNode {
    /// Any node at all.
    Any,
    /// A node no earlier attempt has been sent to.
    Unique,
}

/// What one attempt in the expected sequence should look like.
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

/// Flattens the history of the one request the test made into a timeline of
/// attempts across all of its fibers.
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

/// Checks the attempts a request made against the expected sequence, returning
/// what did not match rather than panicking, so that one failing case does not
/// hide the ones after it.
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

/// The prepared statement id the forged `RESULT:Prepared` hands out. Its value
/// is arbitrary - nothing looks it up - but it has to be the same everywhere,
/// so that a repreparation would agree with what was handed out before.
const PREPARED_ID: &[u8] = &[0xDE, 0xAD, 0xBE, 0xEF];

/// The CQL type id of `int`, as the protocol encodes it in a column spec.
const CQL_TYPE_INT: u16 = 0x0009;

/// Distinguishes the statement under test from the queries the driver makes on
/// its own behalf, both of which reach the proxy as PREPARE and EXECUTE.
const STATEMENT_MARKER: &str = "ks.t";

/// `RESULT:Void` - what a statement that returns no rows is answered with.
fn forged_void(frame: RequestFrame) -> ResponseFrame {
    ResponseFrame::new(
        frame.params.for_response(),
        ResponseOpcode::Result,
        Bytes::from_static(&[0, 0, 0, 1]),
    )
}

/// `RESULT:Prepared` for a statement binding exactly one `int`.
///
/// Two things about it are load-bearing. It must declare one bound column: with
/// none, serializing the value fails with `WrongColumnCount` before a request
/// is ever sent, and the retry and speculative machinery never runs. And it
/// must declare no partition key indexes, so that the statement is not
/// token-aware and the load balancer plans over every node - which is what the
/// "a node not tried before" expectations need. Declaring a key index instead
/// would send the driver looking for replicas in a keyspace that the simulated
/// cluster does not have.
///
/// The layout is CQL v4 with no protocol extensions negotiated, which is what
/// the forged `SUPPORTED` below arranges. In particular no result metadata id
/// is emitted, as the driver only reads one when `SCYLLA_USE_METADATA_ID` was
/// advertised.
fn forged_prepared(frame: RequestFrame) -> ResponseFrame {
    fn write_string(buf: &mut BytesMut, s: &str) {
        buf.put_u16(s.len() as u16);
        buf.put_slice(s.as_bytes());
    }

    let mut body = BytesMut::new();
    body.put_i32(4); // Result kind: Prepared.
    body.put_u16(PREPARED_ID.len() as u16);
    body.put_slice(PREPARED_ID);

    // Prepared metadata.
    body.put_i32(0x0001); // Flags: GLOBAL_TABLES_SPEC.
    body.put_i32(1); // One bound column.
    body.put_i32(0); // No partition key indexes.
    write_string(&mut body, "ks"); // The global table spec the flag promises.
    write_string(&mut body, "t");
    write_string(&mut body, "a"); // The one column: name, then type.
    body.put_u16(CQL_TYPE_INT);

    // Result metadata: none, as the statement returns no rows.
    body.put_i32(0); // Flags.
    body.put_i32(0); // No columns.

    ResponseFrame::new(
        frame.params.for_response(),
        ResponseOpcode::Result,
        body.freeze(),
    )
}

/// The rules every dry node needs in order for a `Session` to reach it and run
/// a statement, before any of the rules a test case adds.
///
/// Dry mode drops whatever no rule matches, and the requests a session makes
/// while connecting have no timeout of their own, so a missing rule here does
/// not fail the test - it hangs it. Two are easy to overlook:
///
/// - REGISTER must be answered. The control connection subscribes to events and
///   waits for a `READY` before the session is usable.
/// - The control connection's metadata reads must be *errored*, not dropped. A
///   session tolerates a failed metadata read - it carries on with a peer list
///   built from the contact points, which is exactly the topology wanted here -
///   but only if the read actually fails.
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
        // Only the statement under test gets a prepared statement it can use.
        // The forged metadata below describes that statement and nothing else,
        // so handing it to the driver's own queries would have them fail while
        // serializing their values rather than while reading the response.
        RequestRule(
            Condition::RequestOpcode(RequestOpcode::Prepare).and(
                Condition::BodyContainsCaseSensitive(STATEMENT_MARKER.as_bytes().into()),
            ),
            RequestReaction::forge_response(Arc::new(forged_prepared)),
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
        // The statement succeeds unless a case's own rule, which is matched
        // first, says otherwise.
        RequestRule(
            Condition::RequestOpcode(RequestOpcode::Execute),
            RequestReaction::forge_response(Arc::new(forged_void)),
        ),
    ]
}

/// What the proxy should do to an attempt.
enum ProxyRule {
    /// Answer the next attempt, wherever in the cluster it lands, with an error
    /// the retry policy retries on the next target, after `delay` if given.
    Fail { delay: Option<Duration> },
    /// Answer every attempt with that error.
    FailAll,
    /// Hold the next attempt for `delay` before answering it.
    Delay { delay: Duration },
    /// Never answer the next attempt at all.
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

/// True for the first evaluation only, cluster-wide. The rules are installed on
/// every node, so a counter held in the condition itself would let each node
/// fire once over, where these rules must fire once in total.
fn once_in_the_cluster() -> Condition {
    Condition::TrueForLimitedTimesShared(Arc::new(AtomicUsize::new(1)))
}

fn into_request_rules(rules: Vec<ProxyRule>) -> Vec<RequestRule> {
    // An error the retry policy below answers with `RetryNextTarget`.
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
                RequestReaction::forge_response_with_delay(delay, Arc::new(forged_void)),
            ),
            // Dropping the frame is a response that never comes, which is what
            // a stalled node looks like to the driver.
            ProxyRule::Never => RequestRule(
                on_an_attempt().and(once_in_the_cluster()),
                RequestReaction::drop_frame(),
            ),
        })
        .collect()
}

/// Retries an `Overloaded` on the next target, up to `max_retries` times in
/// total, and gives up on anything else. Deliberately not one of the shipped
/// policies: the point is to know exactly what the policy will do, so that what
/// is left to observe is what speculative execution does.
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
            // The regular fiber never hears back, so the request is finished by
            // a speculative fiber - which is the whole point of the feature.
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
            // The regular fiber's failure is held back long enough for two
            // speculative attempts to fail first, and it is the regular fiber
            // that ends the request.
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
            // Every node fails, so the retries walk the query plan and stop
            // when it is exhausted - there are only three nodes.
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
            // With only one retry allowed, the regular fiber gives up after two
            // attempts and the third comes from a speculative fiber.
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
            // Three fibers are allowed but only three nodes exist, so no more
            // fibers start than there are nodes to send them to.
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
        // Every case is reported before the test fails, so that one broken case
        // does not hide the state of the others.
        let mut failures: Vec<String> = Vec::new();

        for case in test_cases {
            // The case's own rules go first, as the first matching rule wins,
            // and the handshake rules must stay installed - each case builds a
            // session of its own, which has to connect all over again.
            let rules = [into_request_rules(case.proxy_rules), handshake_rules()].concat();
            for node in running_proxy.running_nodes.iter_mut() {
                node.change_request_rules(Some(rules.clone()));
            }

            // A session of its own per case, so that no state - a warmed up
            // connection pool, a retry session - carries over. The metadata
            // refresh is pushed out of the way so that its requests cannot
            // land in the middle of a case.
            let mut builder = SessionBuilder::new()
                .cluster_metadata_refresh_interval(Duration::from_secs(600))
                .default_execution_profile_handle(
                    ExecutionProfile::builder()
                        .speculative_execution_policy(Some(Arc::clone(&case.speculative)))
                        .retry_policy(Arc::clone(&case.retries))
                        // Comfortably longer than any case waits for, so it
                        // never fires in a healthy run. It is here so that a
                        // case which stops making progress - because the
                        // simulated cluster came up with fewer nodes than the
                        // expectations need, say - fails quickly instead of
                        // hanging until the default timeout.
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
            statement.set_is_idempotent(true); // Speculative execution only fires for idempotent statements.
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

/// Runs `test` against three `scylla-proxy` nodes in dry mode - simulated nodes
/// with no ScyllaDB behind them - handing it the addresses to use as contact
/// points.
///
/// This is the cluster-free counterpart of `crate::utils::test_with_3_node_cluster`.
/// There is no address translation to do: a session that cannot read the
/// cluster's metadata falls back to a peer list built from its contact points,
/// so the nodes it knows about are the proxy addresses themselves.
async fn with_3_dry_nodes<F, Fut>(test: F) -> Result<(), ProxyError>
where
    F: FnOnce([SocketAddr; NODES], RunningProxy) -> Fut,
    Fut: Future<Output = RunningProxy>,
{
    // Every call hands out an address of its own.
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
