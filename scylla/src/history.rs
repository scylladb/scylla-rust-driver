//! Collecting history of query executions - retries, speculative, etc.
use std::{
    collections::BTreeMap,
    fmt::{Debug, Display},
    net::SocketAddr,
    sync::Mutex,
    time::SystemTime,
};

use crate::retry_policy::RetryDecision;
use chrono::{DateTime, Utc};

use scylla_cql::errors::QueryError;
use tracing::warn;

/// Id of a single query, i.e. a single call to Session::query/execute/etc.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
pub struct QueryId(pub usize);

/// Id of a single attempt within a query, a single request sent on some connection.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
pub struct AttemptId(pub usize);

/// Id of a speculative execution fiber.
/// When speculative execution is enabled the driver will start multiple
/// speculative threads, each of them performing sequential attempts.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
pub struct SpeculativeId(pub usize);

/// Any type implementing this trait can be passed to Session
/// to collect execution history of specific queries.\
/// In order to use it call `set_history_listener` on
/// `Query`, `PreparedStatement`, etc...\
/// The listener has to generate unique IDs for new queries, attempts and speculative fibers.
/// These ids are then used by the caller to identify them.\
/// It's important to note that even after a query is finished there still might come events related to it.
/// These events come from speculative futures that didn't notice the query is done already.
pub trait HistoryListener: Debug + Send + Sync {
    /// Log that a query has started on query start - right after the call to Session::query.
    fn log_query_start(&self) -> QueryId;

    /// Log that query was successful - called right before returning the result from Session::query, execute, etc.
    fn log_query_success(&self, query_id: QueryId);

    /// Log that query ended with an error - called right before returning the error from Session::query, execute, etc.
    fn log_query_error(&self, query_id: QueryId, error: &QueryError);

    /// Log that a new speculative fiber has started.
    fn log_new_speculative_fiber(&self, query_id: QueryId) -> SpeculativeId;

    /// Log that an attempt has started - request has been sent on some Connection, now awaiting for an answer.
    fn log_attempt_start(
        &self,
        query_id: QueryId,
        speculative_id: Option<SpeculativeId>,
        node_addr: SocketAddr,
    ) -> AttemptId;

    /// Log that an attempt succeeded.
    fn log_attempt_success(&self, attempt_id: AttemptId);

    /// Log that an attempt ended with an error. The error and decision whether to retry the attempt are also included in the log.
    fn log_attempt_error(
        &self,
        attempt_id: AttemptId,
        error: &QueryError,
        retry_decision: &RetryDecision,
    );
}

pub type TimePoint = DateTime<Utc>;

/// HistoryCollector can be used as HistoryListener to collect all the query history events.
/// Each event is marked with an UTC timestamp.
#[derive(Debug, Default)]
pub struct HistoryCollector {
    data: Mutex<HistoryCollectorData>,
}

#[derive(Debug, Clone)]
pub struct HistoryCollectorData {
    events: Vec<(HistoryEvent, TimePoint)>,
    next_query_id: QueryId,
    next_speculative_fiber_id: SpeculativeId,
    next_attempt_id: AttemptId,
}

#[derive(Debug, Clone)]
pub enum HistoryEvent {
    NewQuery(QueryId),
    QuerySuccess(QueryId),
    QueryError(QueryId, QueryError),
    NewSpeculativeFiber(SpeculativeId, QueryId),
    NewAttempt(AttemptId, QueryId, Option<SpeculativeId>, SocketAddr),
    AttemptSuccess(AttemptId),
    AttemptError(AttemptId, QueryError, RetryDecision),
}

impl HistoryCollectorData {
    fn new() -> HistoryCollectorData {
        HistoryCollectorData {
            events: Vec::new(),
            next_query_id: QueryId(0),
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
    /// It's possible that after finishing a query and taking out the events
    /// new ones will still come - from queries that haven't been cancelled yet.
    pub fn take_collected(&self) -> HistoryCollectorData {
        self.do_with_data(|data| {
            let mut data_to_swap = HistoryCollectorData {
                events: Vec::new(),
                next_query_id: data.next_query_id,
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
    fn log_query_start(&self) -> QueryId {
        self.do_with_data(|data| {
            let new_query_id: QueryId = data.next_query_id;
            data.next_query_id.0 += 1;
            data.add_event(HistoryEvent::NewQuery(new_query_id));
            new_query_id
        })
    }

    fn log_query_success(&self, query_id: QueryId) {
        self.do_with_data(|data| {
            data.add_event(HistoryEvent::QuerySuccess(query_id));
        })
    }

    fn log_query_error(&self, query_id: QueryId, error: &QueryError) {
        self.do_with_data(|data| data.add_event(HistoryEvent::QueryError(query_id, error.clone())))
    }

    fn log_new_speculative_fiber(&self, query_id: QueryId) -> SpeculativeId {
        self.do_with_data(|data| {
            let new_speculative_id: SpeculativeId = data.next_speculative_fiber_id;
            data.next_speculative_fiber_id.0 += 1;
            data.add_event(HistoryEvent::NewSpeculativeFiber(
                new_speculative_id,
                query_id,
            ));
            new_speculative_id
        })
    }

    fn log_attempt_start(
        &self,
        query_id: QueryId,
        speculative_id: Option<SpeculativeId>,
        node_addr: SocketAddr,
    ) -> AttemptId {
        self.do_with_data(|data| {
            let new_attempt_id: AttemptId = data.next_attempt_id;
            data.next_attempt_id.0 += 1;
            data.add_event(HistoryEvent::NewAttempt(
                new_attempt_id,
                query_id,
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
        error: &QueryError,
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

/// Structured representation of queries history.\
/// HistoryCollector collects raw events which later can be converted
/// to this pretty representation.\
/// It has a `Display` impl which can be used for printing pretty query history.
#[derive(Debug, Clone)]
pub struct StructuredHistory {
    pub queries: Vec<QueryHistory>,
}

#[derive(Debug, Clone)]
pub struct QueryHistory {
    pub start_time: TimePoint,
    pub non_speculative_fiber: FiberHistory,
    pub speculative_fibers: Vec<FiberHistory>,
    pub result: Option<QueryHistoryResult>,
}

#[derive(Debug, Clone)]
pub enum QueryHistoryResult {
    Success(TimePoint),
    Error(TimePoint, QueryError),
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
    Error(TimePoint, QueryError, RetryDecision),
}

impl From<&HistoryCollectorData> for StructuredHistory {
    fn from(data: &HistoryCollectorData) -> StructuredHistory {
        let mut attempts: BTreeMap<AttemptId, AttemptHistory> = BTreeMap::new();
        let mut queries: BTreeMap<QueryId, QueryHistory> = BTreeMap::new();
        let mut fibers: BTreeMap<SpeculativeId, FiberHistory> = BTreeMap::new();

        // Collect basic data about queries, attempts and speculative fibers
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
                HistoryEvent::NewQuery(query_id) => {
                    queries.insert(
                        *query_id,
                        QueryHistory {
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
                HistoryEvent::QuerySuccess(query_id) => {
                    if let Some(query) = queries.get_mut(query_id) {
                        query.result = Some(QueryHistoryResult::Success(*event_time));
                    }
                }
                HistoryEvent::QueryError(query_id, error) => {
                    if let Some(query) = queries.get_mut(query_id) {
                        query.result = Some(QueryHistoryResult::Error(*event_time, error.clone()));
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
            if let HistoryEvent::NewAttempt(attempt_id, query_id, speculative_id, _) = event {
                if let Some(attempt) = attempts.remove(attempt_id) {
                    match speculative_id {
                        Some(spec_id) => {
                            if let Some(spec_fiber) = fibers.get_mut(spec_id) {
                                spec_fiber.attempts.push(attempt);
                            }
                        }
                        None => {
                            if let Some(query) = queries.get_mut(query_id) {
                                query.non_speculative_fiber.attempts.push(attempt);
                            }
                        }
                    }
                }
            }
        }

        // Move speculative fibers to their queries
        for (event, _) in &data.events {
            if let HistoryEvent::NewSpeculativeFiber(speculative_id, query_id) = event {
                if let Some(fiber) = fibers.remove(speculative_id) {
                    if let Some(query) = queries.get_mut(query_id) {
                        query.speculative_fibers.push(fiber);
                    }
                }
            }
        }

        StructuredHistory {
            queries: queries.into_values().collect(),
        }
    }
}

/// StructuredHistory should be used for printing query history.
impl Display for StructuredHistory {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "Queries History:")?;
        for (i, query) in self.queries.iter().enumerate() {
            writeln!(f, "=== Query #{} ===", i)?;
            writeln!(f, "| start_time: {}", query.start_time)?;
            writeln!(f, "| Non-speculative attempts:")?;
            write_fiber_attempts(&query.non_speculative_fiber, f)?;
            for (spec_i, speculative_fiber) in query.speculative_fibers.iter().enumerate() {
                writeln!(f, "|")?;
                writeln!(f, "|")?;
                writeln!(f, "| > Speculative fiber #{}", spec_i)?;
                writeln!(f, "| fiber start time: {}", speculative_fiber.start_time)?;
                write_fiber_attempts(speculative_fiber, f)?;
            }
            writeln!(f, "|")?;
            match &query.result {
                Some(QueryHistoryResult::Success(succ_time)) => {
                    writeln!(f, "| Query successful at {}", succ_time)?;
                }
                Some(QueryHistoryResult::Error(err_time, error)) => {
                    writeln!(f, "| Query failed at {}", err_time)?;
                    writeln!(f, "| Error: {}", error)?;
                }
                None => writeln!(f, "| Query still running - no final result yet")?,
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
            Some(AttemptResult::Success(time)) => writeln!(f, "|   Success at {}", time)?,
            Some(AttemptResult::Error(time, err, retry_decision)) => {
                writeln!(f, "|   Error at {}", time)?;
                writeln!(f, "|   Error: {}", err)?;
                writeln!(f, "|   Retry decision: {:?}", retry_decision)?;
            }
            None => writeln!(f, "|   No result yet")?,
        };
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use std::{
        net::{IpAddr, Ipv4Addr, SocketAddr},
        sync::Arc,
    };

    use crate::{
        query::Query, retry_policy::RetryDecision, test_utils::setup_tracing,
        utils::test_utils::unique_keyspace_name,
    };

    use super::{
        AttemptId, AttemptResult, HistoryCollector, HistoryListener, QueryHistoryResult, QueryId,
        SpeculativeId, StructuredHistory, TimePoint,
    };
    use crate::test_utils::create_new_session_builder;
    use chrono::{DateTime, NaiveDate, NaiveDateTime, NaiveTime, Utc};
    use futures::StreamExt;
    use scylla_cql::{
        errors::{DbError, QueryError},
        Consistency,
    };

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

        for query in &mut history.queries {
            query.start_time = the_time;
            match &mut query.result {
                Some(QueryHistoryResult::Success(succ_time)) => *succ_time = the_time,
                Some(QueryHistoryResult::Error(err_time, _)) => *err_time = the_time,
                None => {}
            };

            for fiber in std::iter::once(&mut query.non_speculative_fiber)
                .chain(query.speculative_fibers.iter_mut())
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

    // Set a single node for all attempts within StructuredHistory.
    // When running against real life nodes this address may change,
    // setting it to one value makes it possible to run tests consistently.
    fn set_one_node(mut history: StructuredHistory) -> StructuredHistory {
        let the_node: SocketAddr = node1_addr();

        for query in &mut history.queries {
            for fiber in std::iter::once(&mut query.non_speculative_fiber)
                .chain(query.speculative_fibers.iter_mut())
            {
                for attempt in &mut fiber.attempts {
                    attempt.node_addr = the_node;
                }
            }
        }

        history
    }

    // Set a single error message for all DbErrors within StructuredHistory.
    // The error message changes between Scylla/Cassandra/their versions.
    // Setting it to one value makes it possible to run tests consistently.
    fn set_one_db_error_message(mut history: StructuredHistory) -> StructuredHistory {
        let set_msg = |err: &mut QueryError| {
            if let QueryError::DbError(_, msg) = err {
                *msg = "Error message from database".to_string();
            }
        };

        for query in &mut history.queries {
            if let Some(QueryHistoryResult::Error(_, err)) = &mut query.result {
                set_msg(err);
            }
            for fiber in std::iter::once(&mut query.non_speculative_fiber)
                .chain(query.speculative_fibers.iter_mut())
            {
                for attempt in &mut fiber.attempts {
                    if let Some(AttemptResult::Error(_, err, _)) = &mut attempt.result {
                        set_msg(err);
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

    fn timeout_error() -> QueryError {
        QueryError::TimeoutError
    }

    fn unavailable_error() -> QueryError {
        QueryError::DbError(
            DbError::Unavailable {
                consistency: Consistency::Quorum,
                required: 2,
                alive: 1,
            },
            "Not enough nodes to satisfy consistency".to_string(),
        )
    }

    fn no_stream_id_error() -> QueryError {
        QueryError::UnableToAllocStreamId
    }

    #[test]
    fn empty_history() {
        setup_tracing();
        let history_collector = HistoryCollector::new();
        let history: StructuredHistory = history_collector.clone_structured_history();

        assert!(history.queries.is_empty());

        let displayed = "Queries History:
";
        assert_eq!(displayed, format!("{}", history));
    }

    #[test]
    fn empty_query() {
        setup_tracing();
        let history_collector = HistoryCollector::new();

        let _query_id: QueryId = history_collector.log_query_start();

        let history: StructuredHistory = history_collector.clone_structured_history();

        assert_eq!(history.queries.len(), 1);
        assert!(history.queries[0].non_speculative_fiber.attempts.is_empty());
        assert!(history.queries[0].speculative_fibers.is_empty());

        let displayed = "Queries History:
=== Query #0 ===
| start_time: 2022-02-22 20:22:22 UTC
| Non-speculative attempts:
|
| Query still running - no final result yet
=================
";

        assert_eq!(displayed, format!("{}", set_one_time(history)));
    }

    #[test]
    fn one_attempt() {
        setup_tracing();
        let history_collector = HistoryCollector::new();

        let query_id: QueryId = history_collector.log_query_start();
        let attempt_id: AttemptId =
            history_collector.log_attempt_start(query_id, None, node1_addr());
        history_collector.log_attempt_success(attempt_id);
        history_collector.log_query_success(query_id);

        let history: StructuredHistory = history_collector.clone_structured_history();

        assert_eq!(history.queries.len(), 1);
        assert_eq!(history.queries[0].non_speculative_fiber.attempts.len(), 1);
        assert!(history.queries[0].speculative_fibers.is_empty());
        assert!(matches!(
            history.queries[0].non_speculative_fiber.attempts[0].result,
            Some(AttemptResult::Success(_))
        ));

        let displayed = "Queries History:
=== Query #0 ===
| start_time: 2022-02-22 20:22:22 UTC
| Non-speculative attempts:
| - Attempt #0 sent to 127.0.0.1:19042
|   request send time: 2022-02-22 20:22:22 UTC
|   Success at 2022-02-22 20:22:22 UTC
|
| Query successful at 2022-02-22 20:22:22 UTC
=================
";
        assert_eq!(displayed, format!("{}", set_one_time(history)));
    }

    #[test]
    fn two_error_atempts() {
        setup_tracing();
        let history_collector = HistoryCollector::new();

        let query_id: QueryId = history_collector.log_query_start();

        let attempt_id: AttemptId =
            history_collector.log_attempt_start(query_id, None, node1_addr());
        history_collector.log_attempt_error(
            attempt_id,
            &QueryError::TimeoutError,
            &RetryDecision::RetrySameNode(Some(Consistency::Quorum)),
        );

        let second_attempt_id: AttemptId =
            history_collector.log_attempt_start(query_id, None, node1_addr());
        history_collector.log_attempt_error(
            second_attempt_id,
            &unavailable_error(),
            &RetryDecision::DontRetry,
        );

        history_collector.log_query_error(query_id, &unavailable_error());

        let history: StructuredHistory = history_collector.clone_structured_history();

        let displayed =
"Queries History:
=== Query #0 ===
| start_time: 2022-02-22 20:22:22 UTC
| Non-speculative attempts:
| - Attempt #0 sent to 127.0.0.1:19042
|   request send time: 2022-02-22 20:22:22 UTC
|   Error at 2022-02-22 20:22:22 UTC
|   Error: Timeout Error
|   Retry decision: RetrySameNode(Some(Quorum))
|
| - Attempt #1 sent to 127.0.0.1:19042
|   request send time: 2022-02-22 20:22:22 UTC
|   Error at 2022-02-22 20:22:22 UTC
|   Error: Database returned an error: Not enough nodes are alive to satisfy required consistency level (consistency: Quorum, required: 2, alive: 1), Error message: Not enough nodes to satisfy consistency
|   Retry decision: DontRetry
|
| Query failed at 2022-02-22 20:22:22 UTC
| Error: Database returned an error: Not enough nodes are alive to satisfy required consistency level (consistency: Quorum, required: 2, alive: 1), Error message: Not enough nodes to satisfy consistency
=================
";
        assert_eq!(displayed, format!("{}", set_one_time(history)));
    }

    #[test]
    fn empty_fibers() {
        setup_tracing();
        let history_collector = HistoryCollector::new();

        let query_id: QueryId = history_collector.log_query_start();
        history_collector.log_new_speculative_fiber(query_id);
        history_collector.log_new_speculative_fiber(query_id);
        history_collector.log_new_speculative_fiber(query_id);

        let history: StructuredHistory = history_collector.clone_structured_history();

        assert_eq!(history.queries.len(), 1);
        assert!(history.queries[0].non_speculative_fiber.attempts.is_empty());
        assert_eq!(history.queries[0].speculative_fibers.len(), 3);
        assert!(history.queries[0].speculative_fibers[0].attempts.is_empty());
        assert!(history.queries[0].speculative_fibers[1].attempts.is_empty());
        assert!(history.queries[0].speculative_fibers[2].attempts.is_empty());

        let displayed = "Queries History:
=== Query #0 ===
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
| Query still running - no final result yet
=================
";
        assert_eq!(displayed, format!("{}", set_one_time(history)));
    }

    #[test]
    fn complex() {
        setup_tracing();
        let history_collector = HistoryCollector::new();

        let query_id: QueryId = history_collector.log_query_start();

        let attempt1: AttemptId = history_collector.log_attempt_start(query_id, None, node1_addr());

        let speculative1: SpeculativeId = history_collector.log_new_speculative_fiber(query_id);

        let spec1_attempt1: AttemptId =
            history_collector.log_attempt_start(query_id, Some(speculative1), node2_addr());

        history_collector.log_attempt_error(
            attempt1,
            &timeout_error(),
            &RetryDecision::RetryNextNode(Some(Consistency::Quorum)),
        );
        let _attempt2: AttemptId =
            history_collector.log_attempt_start(query_id, None, node3_addr());

        let speculative2: SpeculativeId = history_collector.log_new_speculative_fiber(query_id);

        let spec2_attempt1: AttemptId =
            history_collector.log_attempt_start(query_id, Some(speculative2), node1_addr());
        history_collector.log_attempt_error(
            spec2_attempt1,
            &no_stream_id_error(),
            &RetryDecision::RetrySameNode(Some(Consistency::Quorum)),
        );

        let spec2_attempt2: AttemptId =
            history_collector.log_attempt_start(query_id, Some(speculative2), node1_addr());

        let _speculative3: SpeculativeId = history_collector.log_new_speculative_fiber(query_id);
        let speculative4: SpeculativeId = history_collector.log_new_speculative_fiber(query_id);

        history_collector.log_attempt_error(
            spec1_attempt1,
            &unavailable_error(),
            &RetryDecision::RetryNextNode(Some(Consistency::Quorum)),
        );

        let _spec4_attempt1: AttemptId =
            history_collector.log_attempt_start(query_id, Some(speculative4), node2_addr());

        history_collector.log_attempt_success(spec2_attempt2);
        history_collector.log_query_success(query_id);

        let history: StructuredHistory = history_collector.clone_structured_history();

        let displayed = "Queries History:
=== Query #0 ===
| start_time: 2022-02-22 20:22:22 UTC
| Non-speculative attempts:
| - Attempt #0 sent to 127.0.0.1:19042
|   request send time: 2022-02-22 20:22:22 UTC
|   Error at 2022-02-22 20:22:22 UTC
|   Error: Timeout Error
|   Retry decision: RetryNextNode(Some(Quorum))
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
|   Retry decision: RetryNextNode(Some(Quorum))
|
|
| > Speculative fiber #1
| fiber start time: 2022-02-22 20:22:22 UTC
| - Attempt #0 sent to 127.0.0.1:19042
|   request send time: 2022-02-22 20:22:22 UTC
|   Error at 2022-02-22 20:22:22 UTC
|   Error: Unable to allocate stream id
|   Retry decision: RetrySameNode(Some(Quorum))
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
| Query successful at 2022-02-22 20:22:22 UTC
=================
";
        assert_eq!(displayed, format!("{}", set_one_time(history)));
    }

    #[test]
    fn multiple_queries() {
        setup_tracing();
        let history_collector = HistoryCollector::new();

        let query1_id: QueryId = history_collector.log_query_start();
        let query1_attempt1: AttemptId =
            history_collector.log_attempt_start(query1_id, None, node1_addr());
        history_collector.log_attempt_error(
            query1_attempt1,
            &timeout_error(),
            &RetryDecision::RetryNextNode(Some(Consistency::Quorum)),
        );
        let query1_attempt2: AttemptId =
            history_collector.log_attempt_start(query1_id, None, node2_addr());
        history_collector.log_attempt_success(query1_attempt2);
        history_collector.log_query_success(query1_id);

        let query2_id: QueryId = history_collector.log_query_start();
        let query2_attempt1: AttemptId =
            history_collector.log_attempt_start(query2_id, None, node1_addr());
        history_collector.log_attempt_success(query2_attempt1);
        history_collector.log_query_success(query2_id);

        let history: StructuredHistory = history_collector.clone_structured_history();

        let displayed = "Queries History:
=== Query #0 ===
| start_time: 2022-02-22 20:22:22 UTC
| Non-speculative attempts:
| - Attempt #0 sent to 127.0.0.1:19042
|   request send time: 2022-02-22 20:22:22 UTC
|   Error at 2022-02-22 20:22:22 UTC
|   Error: Timeout Error
|   Retry decision: RetryNextNode(Some(Quorum))
|
| - Attempt #1 sent to 127.0.0.2:19042
|   request send time: 2022-02-22 20:22:22 UTC
|   Success at 2022-02-22 20:22:22 UTC
|
| Query successful at 2022-02-22 20:22:22 UTC
=================
=== Query #1 ===
| start_time: 2022-02-22 20:22:22 UTC
| Non-speculative attempts:
| - Attempt #0 sent to 127.0.0.1:19042
|   request send time: 2022-02-22 20:22:22 UTC
|   Success at 2022-02-22 20:22:22 UTC
|
| Query successful at 2022-02-22 20:22:22 UTC
=================
";
        assert_eq!(displayed, format!("{}", set_one_time(history)));
    }

    #[tokio::test]
    async fn successful_query_history() {
        setup_tracing();
        let session = create_new_session_builder().build().await.unwrap();

        let mut query = Query::new("SELECT * FROM system.local");
        let history_collector = Arc::new(HistoryCollector::new());
        query.set_history_listener(history_collector.clone());

        session.query(query.clone(), ()).await.unwrap();

        let history: StructuredHistory = history_collector.clone_structured_history();

        let displayed = "Queries History:
=== Query #0 ===
| start_time: 2022-02-22 20:22:22 UTC
| Non-speculative attempts:
| - Attempt #0 sent to 127.0.0.1:19042
|   request send time: 2022-02-22 20:22:22 UTC
|   Success at 2022-02-22 20:22:22 UTC
|
| Query successful at 2022-02-22 20:22:22 UTC
=================
";
        assert_eq!(
            displayed,
            format!(
                "{}",
                set_one_db_error_message(set_one_node(set_one_time(history)))
            )
        );

        // Prepared queries retain the history listener set in Query.
        let prepared = session.prepare(query).await.unwrap();
        session.execute(&prepared, ()).await.unwrap();

        let history2: StructuredHistory = history_collector.clone_structured_history();

        let displayed2 = "Queries History:
=== Query #0 ===
| start_time: 2022-02-22 20:22:22 UTC
| Non-speculative attempts:
| - Attempt #0 sent to 127.0.0.1:19042
|   request send time: 2022-02-22 20:22:22 UTC
|   Success at 2022-02-22 20:22:22 UTC
|
| Query successful at 2022-02-22 20:22:22 UTC
=================
=== Query #1 ===
| start_time: 2022-02-22 20:22:22 UTC
| Non-speculative attempts:
| - Attempt #0 sent to 127.0.0.1:19042
|   request send time: 2022-02-22 20:22:22 UTC
|   Success at 2022-02-22 20:22:22 UTC
|
| Query successful at 2022-02-22 20:22:22 UTC
=================
";
        assert_eq!(
            displayed2,
            format!(
                "{}",
                set_one_db_error_message(set_one_node(set_one_time(history2)))
            )
        );
    }

    #[tokio::test]
    async fn failed_query_history() {
        setup_tracing();
        let session = create_new_session_builder().build().await.unwrap();

        let mut query = Query::new("This isnt even CQL");
        let history_collector = Arc::new(HistoryCollector::new());
        query.set_history_listener(history_collector.clone());

        assert!(session.query(query.clone(), ()).await.is_err());

        let history: StructuredHistory = history_collector.clone_structured_history();

        let displayed =
"Queries History:
=== Query #0 ===
| start_time: 2022-02-22 20:22:22 UTC
| Non-speculative attempts:
| - Attempt #0 sent to 127.0.0.1:19042
|   request send time: 2022-02-22 20:22:22 UTC
|   Error at 2022-02-22 20:22:22 UTC
|   Error: Database returned an error: The submitted query has a syntax error, Error message: Error message from database
|   Retry decision: DontRetry
|
| Query failed at 2022-02-22 20:22:22 UTC
| Error: Database returned an error: The submitted query has a syntax error, Error message: Error message from database
=================
";
        assert_eq!(
            displayed,
            format!(
                "{}",
                set_one_db_error_message(set_one_node(set_one_time(history)))
            )
        );
    }

    #[tokio::test]
    async fn iterator_query_history() {
        setup_tracing();
        let session = create_new_session_builder().build().await.unwrap();
        let ks = unique_keyspace_name();
        session
        .query(format!("CREATE KEYSPACE {} WITH REPLICATION = {{'class' : 'NetworkTopologyStrategy', 'replication_factor' : 1}}", ks), &[])
        .await
        .unwrap();
        session.use_keyspace(ks, true).await.unwrap();

        session
            .query("CREATE TABLE t (p int primary key)", ())
            .await
            .unwrap();
        for i in 0..32 {
            session
                .query("INSERT INTO t (p) VALUES (?)", (i,))
                .await
                .unwrap();
        }

        let mut iter_query: Query = Query::new("SELECT * FROM t");
        iter_query.set_page_size(8);
        let history_collector = Arc::new(HistoryCollector::new());
        iter_query.set_history_listener(history_collector.clone());

        let mut rows_iterator = session.query_iter(iter_query, ()).await.unwrap();
        while let Some(_row) = rows_iterator.next().await {
            // Receive rows...
        }

        let history = history_collector.clone_structured_history();

        assert!(history.queries.len() >= 4);

        let displayed_prefix = "Queries History:
=== Query #0 ===
| start_time: 2022-02-22 20:22:22 UTC
| Non-speculative attempts:
| - Attempt #0 sent to 127.0.0.1:19042
|   request send time: 2022-02-22 20:22:22 UTC
|   Success at 2022-02-22 20:22:22 UTC
|
| Query successful at 2022-02-22 20:22:22 UTC
=================
=== Query #1 ===
| start_time: 2022-02-22 20:22:22 UTC
| Non-speculative attempts:
| - Attempt #0 sent to 127.0.0.1:19042
|   request send time: 2022-02-22 20:22:22 UTC
|   Success at 2022-02-22 20:22:22 UTC
|
| Query successful at 2022-02-22 20:22:22 UTC
=================
=== Query #2 ===
| start_time: 2022-02-22 20:22:22 UTC
| Non-speculative attempts:
| - Attempt #0 sent to 127.0.0.1:19042
|   request send time: 2022-02-22 20:22:22 UTC
|   Success at 2022-02-22 20:22:22 UTC
|
| Query successful at 2022-02-22 20:22:22 UTC
=================
=== Query #3 ===
| start_time: 2022-02-22 20:22:22 UTC
| Non-speculative attempts:
| - Attempt #0 sent to 127.0.0.1:19042
|   request send time: 2022-02-22 20:22:22 UTC
|   Success at 2022-02-22 20:22:22 UTC
|
| Query successful at 2022-02-22 20:22:22 UTC
=================
";
        let displayed_str = format!(
            "{}",
            set_one_db_error_message(set_one_node(set_one_time(history)))
        );

        assert!(displayed_str.starts_with(displayed_prefix),);
    }
}
