//! Collecting history of request executions - retries, speculative, etc.
use std::{
    collections::BTreeMap,
    fmt::{Debug, Display},
    net::SocketAddr,
    sync::Mutex,
    time::SystemTime,
};

use crate::errors::{RequestAttemptError, RequestError};
use crate::policies::retry::RetryDecision;
use chrono::{DateTime, Utc};

use tracing::warn;

/// Id of a single request, i.e. a single call to Session::{query,execute}_{unpaged,single_page}/etc.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
pub struct RequestId(pub usize);

/// Id of a single attempt within a request run - a single request sent on some connection.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
pub struct AttemptId(pub usize);

/// Id of a speculative execution fiber.
/// When speculative execution is enabled the driver will start multiple
/// speculative threads, each of them performing sequential attempts.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
pub struct SpeculativeId(pub usize);

/// Any type implementing this trait can be passed to Session
/// to collect execution history of specific requests.\
/// In order to use it call `set_history_listener` on
/// `Query`, `PreparedStatement`, etc...\
/// The listener has to generate unique IDs for new requests, attempts and speculative fibers.
/// These ids are then used by the caller to identify them.\
/// It's important to note that even after a request is finished there still might come events related to it.
/// These events come from speculative futures that didn't notice the request is done already.
pub trait HistoryListener: Debug + Send + Sync {
    /// Log that a request has started on request start - right after the call to Session::{query,execute}_*/batch.
    fn log_request_start(&self) -> RequestId;

    /// Log that request was successful - called right before returning the result from Session::query_*, execute_*, etc.
    fn log_request_success(&self, request_id: RequestId);

    /// Log that request ended with an error - called right before returning the error from Session::query_*, execute_*, etc.
    fn log_request_error(&self, request_id: RequestId, error: &RequestError);

    /// Log that a new speculative fiber has started.
    fn log_new_speculative_fiber(&self, request_id: RequestId) -> SpeculativeId;

    /// Log that an attempt has started - request has been sent on some Connection, now awaiting for an answer.
    fn log_attempt_start(
        &self,
        request_id: RequestId,
        speculative_id: Option<SpeculativeId>,
        node_addr: SocketAddr,
    ) -> AttemptId;

    /// Log that an attempt succeeded.
    fn log_attempt_success(&self, attempt_id: AttemptId);

    /// Log that an attempt ended with an error. The error and decision whether to retry the attempt are also included in the log.
    fn log_attempt_error(
        &self,
        attempt_id: AttemptId,
        error: &RequestAttemptError,
        retry_decision: &RetryDecision,
    );
}

pub type TimePoint = DateTime<Utc>;

/// HistoryCollector can be used as HistoryListener to collect all the request history events.
/// Each event is marked with an UTC timestamp.
#[derive(Debug, Default)]
pub struct HistoryCollector {
    data: Mutex<HistoryCollectorData>,
}

#[derive(Debug, Clone)]
pub struct HistoryCollectorData {
    events: Vec<(HistoryEvent, TimePoint)>,
    next_request_id: RequestId,
    next_speculative_fiber_id: SpeculativeId,
    next_attempt_id: AttemptId,
}

#[derive(Debug, Clone)]
pub enum HistoryEvent {
    NewRequest(RequestId),
    RequestSuccess(RequestId),
    RequestError(RequestId, RequestError),
    NewSpeculativeFiber(SpeculativeId, RequestId),
    NewAttempt(AttemptId, RequestId, Option<SpeculativeId>, SocketAddr),
    AttemptSuccess(AttemptId),
    AttemptError(AttemptId, RequestAttemptError, RetryDecision),
}

impl HistoryCollectorData {
    fn new() -> HistoryCollectorData {
        HistoryCollectorData {
            events: Vec::new(),
            next_request_id: RequestId(0),
            next_speculative_fiber_id: SpeculativeId(0),
            next_attempt_id: AttemptId(0),
        }
    }

    fn add_event(&mut self, event: HistoryEvent) {
        let event_time: TimePoint = SystemTime::now().into();
        self.events.push((event, event_time));
    }
}

impl Default for HistoryCollectorData {
    fn default() -> HistoryCollectorData {
        HistoryCollectorData::new()
    }
}

impl HistoryCollector {
    /// Creates a new HistoryCollector with empty data.
    pub fn new() -> HistoryCollector {
        HistoryCollector::default()
    }

    /// Clones the data collected by the collector.
    pub fn clone_collected(&self) -> HistoryCollectorData {
        self.do_with_data(|data| data.clone())
    }

    /// Takes the data out of the collector. The collected events are cleared.\
    /// It's possible that after finishing a request and taking out the events
    /// new ones will still come - from requests that haven't been cancelled yet.
    pub fn take_collected(&self) -> HistoryCollectorData {
        self.do_with_data(|data| {
            let mut data_to_swap = HistoryCollectorData {
                events: Vec::new(),
                next_request_id: data.next_request_id,
                next_speculative_fiber_id: data.next_speculative_fiber_id,
                next_attempt_id: data.next_attempt_id,
            };
            std::mem::swap(&mut data_to_swap, data);
            data_to_swap
        })
    }

    /// Clone the collected events and convert them to StructuredHistory.
    pub fn clone_structured_history(&self) -> StructuredHistory {
        StructuredHistory::from(&self.clone_collected())
    }

    /// Take the collected events out, just like in `take_collected` and convert them to StructuredHistory.
    pub fn take_structured_history(&self) -> StructuredHistory {
        StructuredHistory::from(&self.take_collected())
    }

    /// Lock the data mutex and perform an operation on it.
    fn do_with_data<OpRetType>(
        &self,
        do_fn: impl Fn(&mut HistoryCollectorData) -> OpRetType,
    ) -> OpRetType {
        match self.data.lock() {
            Ok(mut data) => do_fn(&mut data),
            Err(poison_error) => {
                // Avoid panicking on poisoned mutex - HistoryCollector isn't that important.
                // Print a warning and do the operation on dummy data so that the code compiles.
                warn!("HistoryCollector - mutex poisoned! Error: {}", poison_error);
                let mut dummy_data: HistoryCollectorData = HistoryCollectorData::default();
                do_fn(&mut dummy_data)
            }
        }
    }
}

impl HistoryListener for HistoryCollector {
    fn log_request_start(&self) -> RequestId {
        self.do_with_data(|data| {
            let new_request_id: RequestId = data.next_request_id;
            data.next_request_id.0 += 1;
            data.add_event(HistoryEvent::NewRequest(new_request_id));
            new_request_id
        })
    }

    fn log_request_success(&self, request_id: RequestId) {
        self.do_with_data(|data| {
            data.add_event(HistoryEvent::RequestSuccess(request_id));
        })
    }

    fn log_request_error(&self, request_id: RequestId, error: &RequestError) {
        self.do_with_data(|data| {
            data.add_event(HistoryEvent::RequestError(request_id, error.clone()))
        })
    }

    fn log_new_speculative_fiber(&self, request_id: RequestId) -> SpeculativeId {
        self.do_with_data(|data| {
            let new_speculative_id: SpeculativeId = data.next_speculative_fiber_id;
            data.next_speculative_fiber_id.0 += 1;
            data.add_event(HistoryEvent::NewSpeculativeFiber(
                new_speculative_id,
                request_id,
            ));
            new_speculative_id
        })
    }

    fn log_attempt_start(
        &self,
        request_id: RequestId,
        speculative_id: Option<SpeculativeId>,
        node_addr: SocketAddr,
    ) -> AttemptId {
        self.do_with_data(|data| {
            let new_attempt_id: AttemptId = data.next_attempt_id;
            data.next_attempt_id.0 += 1;
            data.add_event(HistoryEvent::NewAttempt(
                new_attempt_id,
                request_id,
                speculative_id,
                node_addr,
            ));
            new_attempt_id
        })
    }

    fn log_attempt_success(&self, attempt_id: AttemptId) {
        self.do_with_data(|data| data.add_event(HistoryEvent::AttemptSuccess(attempt_id)))
    }

    fn log_attempt_error(
        &self,
        attempt_id: AttemptId,
        error: &RequestAttemptError,
        retry_decision: &RetryDecision,
    ) {
        self.do_with_data(|data| {
            data.add_event(HistoryEvent::AttemptError(
                attempt_id,
                error.clone(),
                retry_decision.clone(),
            ))
        })
    }
}

/// Structured representation of requests history.\
/// HistoryCollector collects raw events which later can be converted
/// to this pretty representation.\
/// It has a `Display` impl which can be used for printing pretty request history.
#[derive(Debug, Clone)]
pub struct StructuredHistory {
    pub requests: Vec<RequestHistory>,
}

#[derive(Debug, Clone)]
pub struct RequestHistory {
    pub start_time: TimePoint,
    pub non_speculative_fiber: FiberHistory,
    pub speculative_fibers: Vec<FiberHistory>,
    pub result: Option<RequestHistoryResult>,
}

#[derive(Debug, Clone)]
pub enum RequestHistoryResult {
    Success(TimePoint),
    Error(TimePoint, RequestError),
}

#[derive(Debug, Clone)]
pub struct FiberHistory {
    pub start_time: TimePoint,
    pub attempts: Vec<AttemptHistory>,
}

#[derive(Debug, Clone)]
pub struct AttemptHistory {
    pub send_time: TimePoint,
    pub node_addr: SocketAddr,
    pub result: Option<AttemptResult>,
}

#[derive(Debug, Clone)]
pub enum AttemptResult {
    Success(TimePoint),
    Error(TimePoint, RequestAttemptError, RetryDecision),
}

impl From<&HistoryCollectorData> for StructuredHistory {
    fn from(data: &HistoryCollectorData) -> StructuredHistory {
        let mut attempts: BTreeMap<AttemptId, AttemptHistory> = BTreeMap::new();
        let mut requests: BTreeMap<RequestId, RequestHistory> = BTreeMap::new();
        let mut fibers: BTreeMap<SpeculativeId, FiberHistory> = BTreeMap::new();

        // Collect basic data about requests, attempts and speculative fibers
        for (event, event_time) in &data.events {
            match event {
                HistoryEvent::NewAttempt(attempt_id, _, _, node_addr) => {
                    attempts.insert(
                        *attempt_id,
                        AttemptHistory {
                            send_time: *event_time,
                            node_addr: *node_addr,
                            result: None,
                        },
                    );
                }
                HistoryEvent::AttemptSuccess(attempt_id) => {
                    if let Some(attempt) = attempts.get_mut(attempt_id) {
                        attempt.result = Some(AttemptResult::Success(*event_time));
                    }
                }
                HistoryEvent::AttemptError(attempt_id, error, retry_decision) => {
                    match attempts.get_mut(attempt_id) {
                        Some(attempt) => {
                            if attempt.result.is_some() {
                                warn!("StructuredHistory - attempt with id {:?} has multiple results", attempt_id);
                            }
                            attempt.result = Some(AttemptResult::Error(*event_time, error.clone(), retry_decision.clone()));
                        },
                        None => warn!("StructuredHistory - attempt with id {:?} finished with an error but not created", attempt_id)
                    }
                }
                HistoryEvent::NewRequest(request_id) => {
                    requests.insert(
                        *request_id,
                        RequestHistory {
                            start_time: *event_time,
                            non_speculative_fiber: FiberHistory {
                                start_time: *event_time,
                                attempts: Vec::new(),
                            },
                            speculative_fibers: Vec::new(),
                            result: None,
                        },
                    );
                }
                HistoryEvent::RequestSuccess(request_id) => {
                    if let Some(request) = requests.get_mut(request_id) {
                        request.result = Some(RequestHistoryResult::Success(*event_time));
                    }
                }
                HistoryEvent::RequestError(request_id, error) => {
                    if let Some(request) = requests.get_mut(request_id) {
                        request.result =
                            Some(RequestHistoryResult::Error(*event_time, error.clone()));
                    }
                }
                HistoryEvent::NewSpeculativeFiber(speculative_id, _) => {
                    fibers.insert(
                        *speculative_id,
                        FiberHistory {
                            start_time: *event_time,
                            attempts: Vec::new(),
                        },
                    );
                }
            }
        }

        // Move attempts to their speculative fibers
        for (event, _) in &data.events {
            if let HistoryEvent::NewAttempt(attempt_id, request_id, speculative_id, _) = event {
                if let Some(attempt) = attempts.remove(attempt_id) {
                    match speculative_id {
                        Some(spec_id) => {
                            if let Some(spec_fiber) = fibers.get_mut(spec_id) {
                                spec_fiber.attempts.push(attempt);
                            }
                        }
                        None => {
                            if let Some(request) = requests.get_mut(request_id) {
                                request.non_speculative_fiber.attempts.push(attempt);
                            }
                        }
                    }
                }
            }
        }

        // Move speculative fibers to their requests
        for (event, _) in &data.events {
            if let HistoryEvent::NewSpeculativeFiber(speculative_id, request_id) = event {
                if let Some(fiber) = fibers.remove(speculative_id) {
                    if let Some(request) = requests.get_mut(request_id) {
                        request.speculative_fibers.push(fiber);
                    }
                }
            }
        }

        StructuredHistory {
            requests: requests.into_values().collect(),
        }
    }
}

/// StructuredHistory should be used for printing request history.
impl Display for StructuredHistory {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "Requests History:")?;
        for (i, request) in self.requests.iter().enumerate() {
            writeln!(f, "=== Request #{i} ===")?;
            writeln!(f, "| start_time: {}", request.start_time)?;
            writeln!(f, "| Non-speculative attempts:")?;
            write_fiber_attempts(&request.non_speculative_fiber, f)?;
            for (spec_i, speculative_fiber) in request.speculative_fibers.iter().enumerate() {
                writeln!(f, "|")?;
                writeln!(f, "|")?;
                writeln!(f, "| > Speculative fiber #{spec_i}")?;
                writeln!(f, "| fiber start time: {}", speculative_fiber.start_time)?;
                write_fiber_attempts(speculative_fiber, f)?;
            }
            writeln!(f, "|")?;
            match &request.result {
                Some(RequestHistoryResult::Success(succ_time)) => {
                    writeln!(f, "| Request successful at {succ_time}")?;
                }
                Some(RequestHistoryResult::Error(err_time, error)) => {
                    writeln!(f, "| Request failed at {err_time}")?;
                    writeln!(f, "| Error: {error}")?;
                }
                None => writeln!(f, "| Request still running - no final result yet")?,
            };
            writeln!(f, "=================")?;
        }
        Ok(())
    }
}

fn write_fiber_attempts(fiber: &FiberHistory, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    for (i, attempt) in fiber.attempts.iter().enumerate() {
        if i != 0 {
            writeln!(f, "|")?;
        }
        writeln!(f, "| - Attempt #{} sent to {}", i, attempt.node_addr)?;
        writeln!(f, "|   request send time: {}", attempt.send_time)?;
        match &attempt.result {
            Some(AttemptResult::Success(time)) => writeln!(f, "|   Success at {time}")?,
            Some(AttemptResult::Error(time, err, retry_decision)) => {
                writeln!(f, "|   Error at {time}")?;
                writeln!(f, "|   Error: {err}")?;
                writeln!(f, "|   Retry decision: {retry_decision:?}")?;
            }
            None => writeln!(f, "|   No result yet")?,
        };
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};

    use crate::{
        errors::{DbError, RequestAttemptError, RequestError},
        policies::retry::RetryDecision,
        test_utils::setup_tracing,
    };

    use super::{
        AttemptId, AttemptResult, HistoryCollector, HistoryListener, RequestHistoryResult,
        RequestId, SpeculativeId, StructuredHistory, TimePoint,
    };
    use assert_matches::assert_matches;
    use chrono::{DateTime, NaiveDate, NaiveDateTime, NaiveTime, Utc};
    use scylla_cql::{frame::response::CqlResponseKind, Consistency};

    // Set a single time for all timestamps within StructuredHistory.
    // HistoryCollector sets the timestamp to current time which changes with each test.
    // Setting it to one makes it possible to test displaying consistently.
    fn set_one_time(mut history: StructuredHistory) -> StructuredHistory {
        let the_time: TimePoint = DateTime::<Utc>::from_naive_utc_and_offset(
            NaiveDateTime::new(
                NaiveDate::from_ymd_opt(2022, 2, 22).unwrap(),
                NaiveTime::from_hms_opt(20, 22, 22).unwrap(),
            ),
            Utc,
        );

        for request in &mut history.requests {
            request.start_time = the_time;
            match &mut request.result {
                Some(RequestHistoryResult::Success(succ_time)) => *succ_time = the_time,
                Some(RequestHistoryResult::Error(err_time, _)) => *err_time = the_time,
                None => {}
            };

            for fiber in std::iter::once(&mut request.non_speculative_fiber)
                .chain(request.speculative_fibers.iter_mut())
            {
                fiber.start_time = the_time;
                for attempt in &mut fiber.attempts {
                    attempt.send_time = the_time;
                    match &mut attempt.result {
                        Some(AttemptResult::Success(succ_time)) => *succ_time = the_time,
                        Some(AttemptResult::Error(err_time, _, _)) => *err_time = the_time,
                        None => {}
                    }
                }
            }
        }

        history
    }

    fn node1_addr() -> SocketAddr {
        SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 19042)
    }

    fn node2_addr() -> SocketAddr {
        SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 2)), 19042)
    }

    fn node3_addr() -> SocketAddr {
        SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 3)), 19042)
    }

    fn unexpected_response(kind: CqlResponseKind) -> RequestAttemptError {
        RequestAttemptError::UnexpectedResponse(kind)
    }

    fn unavailable_error() -> RequestAttemptError {
        RequestAttemptError::DbError(
            DbError::Unavailable {
                consistency: Consistency::Quorum,
                required: 2,
                alive: 1,
            },
            "Not enough nodes to satisfy consistency".to_string(),
        )
    }

    fn no_stream_id_error() -> RequestAttemptError {
        RequestAttemptError::UnableToAllocStreamId
    }

    #[test]
    fn empty_history() {
        setup_tracing();
        let history_collector = HistoryCollector::new();
        let history: StructuredHistory = history_collector.clone_structured_history();

        assert!(history.requests.is_empty());

        let displayed = "Requests History:
";
        assert_eq!(displayed, format!("{history}"));
    }

    #[test]
    fn empty_request() {
        setup_tracing();
        let history_collector = HistoryCollector::new();

        let _request_id: RequestId = history_collector.log_request_start();

        let history: StructuredHistory = history_collector.clone_structured_history();

        assert_eq!(history.requests.len(), 1);
        assert!(history.requests[0]
            .non_speculative_fiber
            .attempts
            .is_empty());
        assert!(history.requests[0].speculative_fibers.is_empty());

        let displayed = "Requests History:
=== Request #0 ===
| start_time: 2022-02-22 20:22:22 UTC
| Non-speculative attempts:
|
| Request still running - no final result yet
=================
";

        assert_eq!(displayed, format!("{}", set_one_time(history)));
    }

    #[test]
    fn one_attempt() {
        setup_tracing();
        let history_collector = HistoryCollector::new();

        let request_id: RequestId = history_collector.log_request_start();
        let attempt_id: AttemptId =
            history_collector.log_attempt_start(request_id, None, node1_addr());
        history_collector.log_attempt_success(attempt_id);
        history_collector.log_request_success(request_id);

        let history: StructuredHistory = history_collector.clone_structured_history();

        assert_eq!(history.requests.len(), 1);
        assert_eq!(history.requests[0].non_speculative_fiber.attempts.len(), 1);
        assert!(history.requests[0].speculative_fibers.is_empty());
        assert_matches!(
            history.requests[0].non_speculative_fiber.attempts[0].result,
            Some(AttemptResult::Success(_))
        );

        let displayed = "Requests History:
=== Request #0 ===
| start_time: 2022-02-22 20:22:22 UTC
| Non-speculative attempts:
| - Attempt #0 sent to 127.0.0.1:19042
|   request send time: 2022-02-22 20:22:22 UTC
|   Success at 2022-02-22 20:22:22 UTC
|
| Request successful at 2022-02-22 20:22:22 UTC
=================
";
        assert_eq!(displayed, format!("{}", set_one_time(history)));
    }

    #[test]
    fn two_error_atempts() {
        setup_tracing();
        let history_collector = HistoryCollector::new();

        let request_id: RequestId = history_collector.log_request_start();

        let attempt_id: AttemptId =
            history_collector.log_attempt_start(request_id, None, node1_addr());
        history_collector.log_attempt_error(
            attempt_id,
            &unexpected_response(CqlResponseKind::Ready),
            &RetryDecision::RetrySameTarget(Some(Consistency::Quorum)),
        );

        let second_attempt_id: AttemptId =
            history_collector.log_attempt_start(request_id, None, node1_addr());
        history_collector.log_attempt_error(
            second_attempt_id,
            &unavailable_error(),
            &RetryDecision::DontRetry,
        );

        history_collector.log_request_error(
            request_id,
            &RequestError::LastAttemptError(unavailable_error()),
        );

        let history: StructuredHistory = history_collector.clone_structured_history();

        let displayed =
"Requests History:
=== Request #0 ===
| start_time: 2022-02-22 20:22:22 UTC
| Non-speculative attempts:
| - Attempt #0 sent to 127.0.0.1:19042
|   request send time: 2022-02-22 20:22:22 UTC
|   Error at 2022-02-22 20:22:22 UTC
|   Error: Received unexpected response from the server: READY. Expected RESULT or ERROR response.
|   Retry decision: RetrySameTarget(Some(Quorum))
|
| - Attempt #1 sent to 127.0.0.1:19042
|   request send time: 2022-02-22 20:22:22 UTC
|   Error at 2022-02-22 20:22:22 UTC
|   Error: Database returned an error: Not enough nodes are alive to satisfy required consistency level (consistency: Quorum, required: 2, alive: 1), Error message: Not enough nodes to satisfy consistency
|   Retry decision: DontRetry
|
| Request failed at 2022-02-22 20:22:22 UTC
| Error: Database returned an error: Not enough nodes are alive to satisfy required consistency level (consistency: Quorum, required: 2, alive: 1), Error message: Not enough nodes to satisfy consistency
=================
";
        assert_eq!(displayed, format!("{}", set_one_time(history)));
    }

    #[test]
    fn empty_fibers() {
        setup_tracing();
        let history_collector = HistoryCollector::new();

        let request_id: RequestId = history_collector.log_request_start();
        history_collector.log_new_speculative_fiber(request_id);
        history_collector.log_new_speculative_fiber(request_id);
        history_collector.log_new_speculative_fiber(request_id);

        let history: StructuredHistory = history_collector.clone_structured_history();

        assert_eq!(history.requests.len(), 1);
        assert!(history.requests[0]
            .non_speculative_fiber
            .attempts
            .is_empty());
        assert_eq!(history.requests[0].speculative_fibers.len(), 3);
        assert!(history.requests[0].speculative_fibers[0]
            .attempts
            .is_empty());
        assert!(history.requests[0].speculative_fibers[1]
            .attempts
            .is_empty());
        assert!(history.requests[0].speculative_fibers[2]
            .attempts
            .is_empty());

        let displayed = "Requests History:
=== Request #0 ===
| start_time: 2022-02-22 20:22:22 UTC
| Non-speculative attempts:
|
|
| > Speculative fiber #0
| fiber start time: 2022-02-22 20:22:22 UTC
|
|
| > Speculative fiber #1
| fiber start time: 2022-02-22 20:22:22 UTC
|
|
| > Speculative fiber #2
| fiber start time: 2022-02-22 20:22:22 UTC
|
| Request still running - no final result yet
=================
";
        assert_eq!(displayed, format!("{}", set_one_time(history)));
    }

    #[test]
    fn complex() {
        setup_tracing();
        let history_collector = HistoryCollector::new();

        let request_id: RequestId = history_collector.log_request_start();

        let attempt1: AttemptId =
            history_collector.log_attempt_start(request_id, None, node1_addr());

        let speculative1: SpeculativeId = history_collector.log_new_speculative_fiber(request_id);

        let spec1_attempt1: AttemptId =
            history_collector.log_attempt_start(request_id, Some(speculative1), node2_addr());

        history_collector.log_attempt_error(
            attempt1,
            &unexpected_response(CqlResponseKind::Event),
            &RetryDecision::RetryNextTarget(Some(Consistency::Quorum)),
        );
        let _attempt2: AttemptId =
            history_collector.log_attempt_start(request_id, None, node3_addr());

        let speculative2: SpeculativeId = history_collector.log_new_speculative_fiber(request_id);

        let spec2_attempt1: AttemptId =
            history_collector.log_attempt_start(request_id, Some(speculative2), node1_addr());
        history_collector.log_attempt_error(
            spec2_attempt1,
            &no_stream_id_error(),
            &RetryDecision::RetrySameTarget(Some(Consistency::Quorum)),
        );

        let spec2_attempt2: AttemptId =
            history_collector.log_attempt_start(request_id, Some(speculative2), node1_addr());

        let _speculative3: SpeculativeId = history_collector.log_new_speculative_fiber(request_id);
        let speculative4: SpeculativeId = history_collector.log_new_speculative_fiber(request_id);

        history_collector.log_attempt_error(
            spec1_attempt1,
            &unavailable_error(),
            &RetryDecision::RetryNextTarget(Some(Consistency::Quorum)),
        );

        let _spec4_attempt1: AttemptId =
            history_collector.log_attempt_start(request_id, Some(speculative4), node2_addr());

        history_collector.log_attempt_success(spec2_attempt2);
        history_collector.log_request_success(request_id);

        let history: StructuredHistory = history_collector.clone_structured_history();

        let displayed = "Requests History:
=== Request #0 ===
| start_time: 2022-02-22 20:22:22 UTC
| Non-speculative attempts:
| - Attempt #0 sent to 127.0.0.1:19042
|   request send time: 2022-02-22 20:22:22 UTC
|   Error at 2022-02-22 20:22:22 UTC
|   Error: Received unexpected response from the server: EVENT. Expected RESULT or ERROR response.
|   Retry decision: RetryNextTarget(Some(Quorum))
|
| - Attempt #1 sent to 127.0.0.3:19042
|   request send time: 2022-02-22 20:22:22 UTC
|   No result yet
|
|
| > Speculative fiber #0
| fiber start time: 2022-02-22 20:22:22 UTC
| - Attempt #0 sent to 127.0.0.2:19042
|   request send time: 2022-02-22 20:22:22 UTC
|   Error at 2022-02-22 20:22:22 UTC
|   Error: Database returned an error: Not enough nodes are alive to satisfy required consistency level (consistency: Quorum, required: 2, alive: 1), Error message: Not enough nodes to satisfy consistency
|   Retry decision: RetryNextTarget(Some(Quorum))
|
|
| > Speculative fiber #1
| fiber start time: 2022-02-22 20:22:22 UTC
| - Attempt #0 sent to 127.0.0.1:19042
|   request send time: 2022-02-22 20:22:22 UTC
|   Error at 2022-02-22 20:22:22 UTC
|   Error: Unable to allocate stream id
|   Retry decision: RetrySameTarget(Some(Quorum))
|
| - Attempt #1 sent to 127.0.0.1:19042
|   request send time: 2022-02-22 20:22:22 UTC
|   Success at 2022-02-22 20:22:22 UTC
|
|
| > Speculative fiber #2
| fiber start time: 2022-02-22 20:22:22 UTC
|
|
| > Speculative fiber #3
| fiber start time: 2022-02-22 20:22:22 UTC
| - Attempt #0 sent to 127.0.0.2:19042
|   request send time: 2022-02-22 20:22:22 UTC
|   No result yet
|
| Request successful at 2022-02-22 20:22:22 UTC
=================
";
        assert_eq!(displayed, format!("{}", set_one_time(history)));
    }

    #[test]
    fn multiple_requests() {
        setup_tracing();
        let history_collector = HistoryCollector::new();

        let request1_id: RequestId = history_collector.log_request_start();
        let request1_attempt1: AttemptId =
            history_collector.log_attempt_start(request1_id, None, node1_addr());
        history_collector.log_attempt_error(
            request1_attempt1,
            &unexpected_response(CqlResponseKind::Supported),
            &RetryDecision::RetryNextTarget(Some(Consistency::Quorum)),
        );
        let request1_attempt2: AttemptId =
            history_collector.log_attempt_start(request1_id, None, node2_addr());
        history_collector.log_attempt_success(request1_attempt2);
        history_collector.log_request_success(request1_id);

        let request2_id: RequestId = history_collector.log_request_start();
        let request2_attempt1: AttemptId =
            history_collector.log_attempt_start(request2_id, None, node1_addr());
        history_collector.log_attempt_success(request2_attempt1);
        history_collector.log_request_success(request2_id);

        let history: StructuredHistory = history_collector.clone_structured_history();

        let displayed = "Requests History:
=== Request #0 ===
| start_time: 2022-02-22 20:22:22 UTC
| Non-speculative attempts:
| - Attempt #0 sent to 127.0.0.1:19042
|   request send time: 2022-02-22 20:22:22 UTC
|   Error at 2022-02-22 20:22:22 UTC
|   Error: Received unexpected response from the server: SUPPORTED. Expected RESULT or ERROR response.
|   Retry decision: RetryNextTarget(Some(Quorum))
|
| - Attempt #1 sent to 127.0.0.2:19042
|   request send time: 2022-02-22 20:22:22 UTC
|   Success at 2022-02-22 20:22:22 UTC
|
| Request successful at 2022-02-22 20:22:22 UTC
=================
=== Request #1 ===
| start_time: 2022-02-22 20:22:22 UTC
| Non-speculative attempts:
| - Attempt #0 sent to 127.0.0.1:19042
|   request send time: 2022-02-22 20:22:22 UTC
|   Success at 2022-02-22 20:22:22 UTC
|
| Request successful at 2022-02-22 20:22:22 UTC
=================
";
        assert_eq!(displayed, format!("{}", set_one_time(history)));
    }
}
