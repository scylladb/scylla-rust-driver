use scylla::policies::retry::{RequestInfo, RetryDecision, RetryPolicy, RetrySession};
use std::fmt::Debug;

#[derive(Debug, Clone, Default)]
#[allow(dead_code)]
pub(crate) struct NoRetryPolicy;

#[derive(Debug, Clone, Default)]
#[allow(dead_code)]
pub(crate) struct NoRetrySession;

impl RetryPolicy for NoRetryPolicy {
    fn new_session(&self) -> Box<dyn RetrySession> {
        Box::new(NoRetrySession)
    }
}

impl RetrySession for NoRetrySession {
    fn decide_should_retry(&mut self, _request_info: RequestInfo) -> RetryDecision {
        RetryDecision::DontRetry
    }

    fn reset(&mut self) {
        *self = Default::default()
    }
}
