//! Collecting history of query executions - retries, speculative, etc.
use std::{fmt::Debug, net::SocketAddr, sync::Mutex, time::SystemTime};

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

    /// Log that an attempt succeded.
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

    /// Lock the data mutex and perform an operation on it.
    fn do_with_data<OpRetType>(
        &self,
        do_fn: impl Fn(&mut HistoryCollectorData) -> OpRetType,
    ) -> OpRetType {
        match self.data.lock() {
            Ok(mut data) => do_fn(&mut *data),
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
