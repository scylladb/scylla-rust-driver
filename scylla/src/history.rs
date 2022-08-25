//! Collecting history of query executions - retries, speculative, etc.
use std::{fmt::Debug, net::SocketAddr};

use crate::retry_policy::RetryDecision;
use scylla_cql::errors::QueryError;

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
