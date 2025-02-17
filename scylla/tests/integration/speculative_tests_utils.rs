use chrono::{DateTime, Utc};
use scylla::observability::history::{AttemptResult, StructuredHistory};
use scylla_cql::frame::request::RequestOpcode;
use scylla_cql::frame::response::error::DbError;
use scylla_proxy::{Action, Condition, ConditionHandler, RequestReaction, RequestRule};
use std::fmt;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

// Attempt fiber type: regular or speculative
#[derive(Debug, Copy, Clone, Hash, PartialEq)]
pub(crate) enum FiberType {
    Regular,
    Speculative,
}

impl fmt::Display for FiberType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl Default for FiberType {
    fn default() -> Self {
        FiberType::Regular
    }
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) enum SimpleAttemptResult {
    // Attempt was registered, but no result was collected
    NoResponse,
    // Attempt succeeded
    Success,
    // Attempt failed
    Failure,
}

impl fmt::Display for SimpleAttemptResult {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}

// Attempt record, collected from `HistoryListener`
// Needed for better visibility and better API when work with simple history
#[derive(Clone, Debug)]
pub(crate) enum AttemptSimpleHistoryRecord {
    RegularNoResponse(DateTime<Utc>, SocketAddr),
    RegularSuccess(DateTime<Utc>, DateTime<Utc>, SocketAddr),
    RegularFailure(DateTime<Utc>, DateTime<Utc>, SocketAddr),
    SpeculativeNoResponse(DateTime<Utc>, SocketAddr),
    SpeculativeSuccess(DateTime<Utc>, DateTime<Utc>, SocketAddr),
    SpeculativeFailure(DateTime<Utc>, DateTime<Utc>, SocketAddr),
}

impl AttemptSimpleHistoryRecord {
    fn start_time(&self) -> DateTime<Utc> {
        match self {
            AttemptSimpleHistoryRecord::RegularNoResponse(start_time, _) => *start_time,
            AttemptSimpleHistoryRecord::RegularFailure(start_time, _, _) => *start_time,
            AttemptSimpleHistoryRecord::RegularSuccess(start_time, _, _) => *start_time,
            AttemptSimpleHistoryRecord::SpeculativeNoResponse(start_time, _) => *start_time,
            AttemptSimpleHistoryRecord::SpeculativeFailure(start_time, _, _) => *start_time,
            AttemptSimpleHistoryRecord::SpeculativeSuccess(start_time, _, _) => *start_time,
        }
    }

    fn done_time(&self) -> Option<DateTime<Utc>> {
        match self {
            AttemptSimpleHistoryRecord::RegularNoResponse(_, _) => None,
            AttemptSimpleHistoryRecord::RegularFailure(_, done_time, _) => Some(*done_time),
            AttemptSimpleHistoryRecord::RegularSuccess(_, done_time, _) => Some(*done_time),
            AttemptSimpleHistoryRecord::SpeculativeNoResponse(_, _) => None,
            AttemptSimpleHistoryRecord::SpeculativeFailure(_, start_time, _) => Some(*start_time),
            AttemptSimpleHistoryRecord::SpeculativeSuccess(_, start_time, _) => Some(*start_time),
        }
    }

    fn get_fiber(&self) -> FiberType {
        match self {
            AttemptSimpleHistoryRecord::RegularNoResponse(_, _) => FiberType::Regular,
            AttemptSimpleHistoryRecord::RegularFailure(_, _, _) => FiberType::Regular,
            AttemptSimpleHistoryRecord::RegularSuccess(_, _, _) => FiberType::Regular,
            AttemptSimpleHistoryRecord::SpeculativeNoResponse(_, _) => FiberType::Speculative,
            AttemptSimpleHistoryRecord::SpeculativeFailure(_, _, _) => FiberType::Speculative,
            AttemptSimpleHistoryRecord::SpeculativeSuccess(_, _, _) => FiberType::Speculative,
        }
    }

    fn get_result(&self) -> SimpleAttemptResult {
        match self {
            AttemptSimpleHistoryRecord::RegularNoResponse(_, _) => SimpleAttemptResult::NoResponse,
            AttemptSimpleHistoryRecord::RegularFailure(_, _, _) => SimpleAttemptResult::Failure,
            AttemptSimpleHistoryRecord::RegularSuccess(_, _, _) => SimpleAttemptResult::Success,
            AttemptSimpleHistoryRecord::SpeculativeNoResponse(_, _) => {
                SimpleAttemptResult::NoResponse
            }
            AttemptSimpleHistoryRecord::SpeculativeFailure(_, _, _) => SimpleAttemptResult::Failure,
            AttemptSimpleHistoryRecord::SpeculativeSuccess(_, _, _) => SimpleAttemptResult::Success,
        }
    }

    fn node_addr(&self) -> &SocketAddr {
        match self {
            AttemptSimpleHistoryRecord::RegularNoResponse(_, node_addr) => node_addr,
            AttemptSimpleHistoryRecord::RegularFailure(_, _, node_addr) => node_addr,
            AttemptSimpleHistoryRecord::RegularSuccess(_, _, node_addr) => node_addr,
            AttemptSimpleHistoryRecord::SpeculativeNoResponse(_, node_addr) => node_addr,
            AttemptSimpleHistoryRecord::SpeculativeFailure(_, _, node_addr) => node_addr,
            AttemptSimpleHistoryRecord::SpeculativeSuccess(_, _, node_addr) => node_addr,
        }
    }
}

impl fmt::Display for AttemptSimpleHistoryRecord {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}

// Is used to be matched against node in `AttemptSimpleHistoryRecord`
#[derive(Clone, Debug)]
pub(crate) enum NodeExpectation {
    // Any node
    // if bool is true it will forget nodes was met before
    AnyNode(bool),
    // Node different from prior attempt, if it is first attempt, it can be any node
    // if bool is true it will forget nodes was met before
    AnotherNode(bool),
    // Node is the same as last time, if it is first attempt, it can be any node
    // if bool is true it will forget nodes was met before
    SameNode(bool),
    // Node different from any prior attempt, if it is first attempt, it can be any node
    // if bool is true it will forget nodes was met before
    UniqueNode(bool),
}

impl NodeExpectation {
    fn expectation_met(&self, nodes_met: &Vec<SocketAddr>, current_node: &SocketAddr) -> bool {
        match &self {
            NodeExpectation::AnyNode(_) => true,
            NodeExpectation::AnotherNode(_) => {
                if nodes_met.is_empty() {
                    return true;
                }
                nodes_met[nodes_met.len() - 1] != *current_node
            }
            NodeExpectation::UniqueNode(_) => !nodes_met.contains(&current_node),
            NodeExpectation::SameNode(_) => {
                if nodes_met.is_empty() {
                    return true;
                }
                nodes_met[nodes_met.len() - 1] == *current_node
            }
        }
    }

    fn forget_nodes(&self) -> bool {
        match &self {
            NodeExpectation::AnyNode(forget) => *forget,
            NodeExpectation::AnotherNode(forget) => *forget,
            NodeExpectation::UniqueNode(forget) => *forget,
            NodeExpectation::SameNode(forget) => *forget,
        }
    }
}

impl fmt::Display for NodeExpectation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}

// Is used to match against whole `AttemptSimpleHistoryRecord` on the timeline
// Node expectation allows you to check on node in `AttemptSimpleHistoryRecord`
// You can check if node is the same as in prior attempt or completely unique
#[derive(Clone, Debug)]
pub(crate) enum AttemptRecordExpectation {
    RegularNoResponse(NodeExpectation),
    RegularSuccess(NodeExpectation),
    RegularFailure(NodeExpectation),
    SpeculativeNoResponse(NodeExpectation),
    SpeculativeSuccess(NodeExpectation),
    SpeculativeFailure(NodeExpectation),
}

impl AttemptRecordExpectation {
    fn node_expectation(&self) -> NodeExpectation {
        match self {
            AttemptRecordExpectation::RegularNoResponse(node_expectation) => {
                node_expectation.clone()
            }
            AttemptRecordExpectation::RegularSuccess(node_expectation) => node_expectation.clone(),
            AttemptRecordExpectation::RegularFailure(node_expectation) => node_expectation.clone(),
            AttemptRecordExpectation::SpeculativeNoResponse(node_expectation) => {
                node_expectation.clone()
            }
            AttemptRecordExpectation::SpeculativeSuccess(node_expectation) => {
                node_expectation.clone()
            }
            AttemptRecordExpectation::SpeculativeFailure(node_expectation) => {
                node_expectation.clone()
            }
        }
    }

    fn get_fiber(&self) -> FiberType {
        match self {
            AttemptRecordExpectation::RegularNoResponse(_) => FiberType::Regular,
            AttemptRecordExpectation::RegularSuccess(_) => FiberType::Regular,
            AttemptRecordExpectation::RegularFailure(_) => FiberType::Regular,
            AttemptRecordExpectation::SpeculativeNoResponse(_) => FiberType::Speculative,
            AttemptRecordExpectation::SpeculativeSuccess(_) => FiberType::Speculative,
            AttemptRecordExpectation::SpeculativeFailure(_) => FiberType::Speculative,
        }
    }

    fn get_result(&self) -> SimpleAttemptResult {
        match self {
            AttemptRecordExpectation::RegularNoResponse(_) => SimpleAttemptResult::NoResponse,
            AttemptRecordExpectation::RegularSuccess(_) => SimpleAttemptResult::Success,
            AttemptRecordExpectation::RegularFailure(_) => SimpleAttemptResult::Failure,
            AttemptRecordExpectation::SpeculativeNoResponse(_) => SimpleAttemptResult::NoResponse,
            AttemptRecordExpectation::SpeculativeSuccess(_) => SimpleAttemptResult::Success,
            AttemptRecordExpectation::SpeculativeFailure(_) => SimpleAttemptResult::Failure,
        }
    }
}

impl fmt::Display for AttemptRecordExpectation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}

// Check if recorded attempt history meet expectations
pub(crate) fn check_expectations(
    expectations: Vec<AttemptRecordExpectation>,
    result: Vec<AttemptSimpleHistoryRecord>,
) -> Result<(), String> {
    let mut nodes_met: Vec<SocketAddr> = Vec::new();
    let mut total_results = 0;

    for (attempt_id, ref attempt) in result.clone().into_iter().enumerate() {
        total_results += 1;
        let expectation = expectations.get(attempt_id);
        if expectation.is_none() {
            return Err(format!(
                "[{}]: end of expectation reached, but extra attempt {} found",
                attempt_id, attempt
            )
            .into());
        }
        let expectation = expectation.unwrap();
        let expected_fiber = expectation.get_fiber();
        let result_fiber = attempt.get_fiber();
        if result_fiber != expected_fiber {
            return Err(format!(
                    "[{}]: attempt was executed on fiber {} instead of {}, result `{}`, expectation `{}`",
                    attempt_id, result_fiber, expected_fiber, attempt, expectation
                    ).into());
        }

        let expected_status = expectation.get_result();
        let result_status = attempt.get_result();

        if expected_status != result_status {
            return Err(format!(
                "[{}] attempt resulted in {} instead of {}: result `{}`, expectation: `{}`",
                attempt_id, result_status, expected_status, attempt, expectation
            )
            .into());
        }
        let node_expectation = expectation.node_expectation();

        if !node_expectation.expectation_met(&nodes_met, attempt.node_addr()) {
            return Err(format!(
                    "[{}] attempt was scheduled to the wrong node {}, while expectation `{}`, nodes observed `{}`",
                    attempt_id, attempt.node_addr(), node_expectation, nodes_met.iter().map(|n| n.to_string()).collect::<Vec<_>>().join(", ")
                    )
                        .into());
        }

        if node_expectation.forget_nodes() {
            nodes_met.clear()
        }
        nodes_met.push(*attempt.node_addr());
    }
    if expectations.len() > total_results {
        return Err(format!(
            "end of attempts reached, but there are more expected: {:?}",
            expectations.get(total_results..).unwrap()
        )
        .into());
    }

    Ok(())
}

pub(crate) fn convert_into_simple_history(
    history: StructuredHistory,
) -> Vec<Vec<AttemptSimpleHistoryRecord>> {
    let mut timeline: Vec<(DateTime<Utc>, Vec<AttemptSimpleHistoryRecord>)> = Vec::new();
    history.requests.iter().for_each(|hist| {
        let mut attempts_timeline: Vec<AttemptSimpleHistoryRecord> = Vec::new();
        hist.non_speculative_fiber
            .attempts
            .iter()
            .for_each(|attempt| match attempt.result {
                Some(ref result) => match result {
                    AttemptResult::Success(time) => {
                        attempts_timeline.push(AttemptSimpleHistoryRecord::RegularSuccess(
                            attempt.send_time,
                            *time,
                            attempt.node_addr,
                        ))
                    }
                    AttemptResult::Error(time, _, _) => {
                        attempts_timeline.push(AttemptSimpleHistoryRecord::RegularFailure(
                            attempt.send_time,
                            *time,
                            attempt.node_addr,
                        ))
                    }
                },
                None => attempts_timeline.push(AttemptSimpleHistoryRecord::RegularNoResponse(
                    attempt.send_time,
                    attempt.node_addr,
                )),
            });

        hist.speculative_fibers.iter().for_each(|fiber| {
            fiber
                .attempts
                .iter()
                .for_each(|attempt| match attempt.result {
                    Some(ref result) => match result {
                        AttemptResult::Success(time) => {
                            attempts_timeline.push(AttemptSimpleHistoryRecord::SpeculativeSuccess(
                                attempt.send_time,
                                *time,
                                attempt.node_addr,
                            ))
                        }
                        AttemptResult::Error(time, _, _) => {
                            attempts_timeline.push(AttemptSimpleHistoryRecord::SpeculativeFailure(
                                attempt.send_time,
                                *time,
                                attempt.node_addr,
                            ))
                        }
                    },
                    None => {
                        attempts_timeline.push(AttemptSimpleHistoryRecord::SpeculativeNoResponse(
                            attempt.send_time,
                            attempt.node_addr,
                        ))
                    }
                })
        });
        attempts_timeline.sort_by(|a, b| {
            a.done_time()
                .unwrap_or(a.start_time())
                .cmp(&b.done_time().unwrap_or(b.start_time()))
        });
        timeline.push((hist.start_time, attempts_timeline));
    });

    timeline.sort_by(|a, b| a.0.cmp(&b.0));
    let timeline: Vec<Vec<AttemptSimpleHistoryRecord>> =
        timeline.iter().map(|(_, res)| res.clone()).collect();
    timeline.into()
}

pub(crate) enum SimpleProxyRules {
    Drop(Option<Duration>),
    Fail(Option<Duration>, FailError),
    Delay(Duration),
    FailAll(FailError),
}

impl SimpleProxyRules {
    pub(crate) fn fail(wait: Option<Duration>, err: fn() -> DbError) -> Self {
        SimpleProxyRules::Fail(wait, FailError(Box::new(err)))
    }

    pub(crate) fn drop(wait: Option<Duration>) -> Self {
        SimpleProxyRules::Drop(wait)
    }

    pub(crate) fn fail_all(err: fn() -> DbError) -> Self {
        SimpleProxyRules::FailAll(FailError(Box::new(err)))
    }
    
    pub(crate) fn delay(wait: Duration) -> Self {
        SimpleProxyRules::Delay(wait)
    }
}

pub(crate) struct FailError(Box<dyn Fn() -> DbError + Send + Sync>);

fn nth(expected: usize) -> Condition {
    let counter = AtomicUsize::new(0);
    Condition::CustomCondition(ConditionHandler::new(Arc::new(move |_| {
        let value = counter.fetch_add(1, Ordering::Relaxed) + 1;
        expected == value
    })))
}

pub(crate) fn into_proxy_request_rules(
    val: Vec<SimpleProxyRules>,
    target_opcode: RequestOpcode,
) -> Vec<RequestRule> {
    val.into_iter()
        .map(|action| match action {
            SimpleProxyRules::Delay(d) => RequestRule(
                Condition::not(Condition::ConnectionRegisteredAnyEvent) // Not control connection
                    .and(Condition::RequestOpcode(target_opcode)) // Query only
                    .and(nth(1)),
                RequestReaction {
                    to_addressee: Some(Action {
                        delay: Some(d),
                        msg_processor: None,
                    }),
                    to_sender: None,
                    drop_connection: None,
                    feedback_channel: None,
                }
            ),
            SimpleProxyRules::Drop(d) => RequestRule(
                Condition::not(Condition::ConnectionRegisteredAnyEvent) // Not control connection
                    .and(Condition::RequestOpcode(target_opcode)) // Query only
                    .and(nth(1)),
                RequestReaction {
                    to_addressee: None,
                    to_sender: None,
                    drop_connection: Some(d),
                    feedback_channel: None,
                },
            ),
            SimpleProxyRules::Fail(wait, err) => RequestRule(
                Condition::not(Condition::ConnectionRegisteredAnyEvent) // Not control connection
                    .and(Condition::RequestOpcode(target_opcode)) // Query only
                    .and(nth(1)),
                RequestReaction::forge_with_error_lazy_delay(err.0, wait),
            ),
            SimpleProxyRules::FailAll(err) => RequestRule(
                Condition::not(Condition::ConnectionRegisteredAnyEvent) // Not control connection
                    .and(Condition::RequestOpcode(target_opcode)), // Query only
                // no nth condition, we want to fail everything
                RequestReaction::forge_with_error_lazy_delay(err.0, None),
            ),
        })
        .collect()
}
