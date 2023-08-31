//! Query retries configurations\
//! To decide when to retry a query the `Session` can use any object which implements
//! the `RetryPolicy` trait

use crate::frame::types::Consistency;
use crate::transport::errors::QueryError;

/// Information about a failed query
pub struct QueryInfo<'a> {
    /// The error with which the query failed
    pub error: &'a QueryError,
    /// A query is idempotent if it can be applied multiple times without changing the result of the initial application\
    /// If set to `true` we can be sure that it is idempotent\
    /// If set to `false` it is unknown whether it is idempotent
    pub is_idempotent: bool,
    /// Consistency with which the query failed
    pub consistency: Consistency,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RetryDecision {
    RetrySameNode(Option<Consistency>), // None means that the same consistency should be used as before
    RetryNextNode(Option<Consistency>), // ditto
    DontRetry,
    IgnoreWriteError,
}

/// Specifies a policy used to decide when to retry a query
pub trait RetryPolicy: std::fmt::Debug + Send + Sync {
    /// Called for each new query, starts a session of deciding about retries
    fn new_session(&self) -> Box<dyn RetrySession>;

    /// Used to clone this RetryPolicy
    fn clone_boxed(&self) -> Box<dyn RetryPolicy>;
}

impl Clone for Box<dyn RetryPolicy> {
    fn clone(&self) -> Box<dyn RetryPolicy> {
        self.clone_boxed()
    }
}

/// Used throughout a single query to decide when to retry it
/// After this query is finished it is destroyed or reset
pub trait RetrySession: Send + Sync {
    /// Called after the query failed - decide what to do next
    fn decide_should_retry(&mut self, query_info: QueryInfo) -> RetryDecision;

    /// Reset before using for a new query
    fn reset(&mut self);
}
