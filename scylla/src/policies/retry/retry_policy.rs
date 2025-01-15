//! Request retries configurations\
//! To decide when to retry a request the `Session` can use any object which implements
//! the `RetryPolicy` trait

use crate::errors::RequestAttemptError;
use crate::frame::types::Consistency;

/// Information about a failed request
pub struct RequestInfo<'a> {
    /// The error with which the request failed
    pub error: &'a RequestAttemptError,
    /// A request is idempotent if it can be applied multiple times without changing the result of the initial application\
    /// If set to `true` we can be sure that it is idempotent\
    /// If set to `false` it is unknown whether it is idempotent
    pub is_idempotent: bool,
    /// Consistency with which the request failed
    pub consistency: Consistency,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RetryDecision {
    RetrySameNode(Option<Consistency>), // None means that the same consistency should be used as before
    RetryNextNode(Option<Consistency>), // ditto
    DontRetry,
    IgnoreWriteError,
}

/// Specifies a policy used to decide when to retry a request
pub trait RetryPolicy: std::fmt::Debug + Send + Sync {
    /// Called for each new request, starts a session of deciding about retries
    fn new_session(&self) -> Box<dyn RetrySession>;
}

/// Used throughout a single request to decide when to retry it
/// After this request is finished it is destroyed or reset
pub trait RetrySession: Send + Sync {
    /// Called after the request failed - decide what to do next
    fn decide_should_retry(&mut self, request_info: RequestInfo) -> RetryDecision;

    /// Reset before using for a new request
    fn reset(&mut self);
}
