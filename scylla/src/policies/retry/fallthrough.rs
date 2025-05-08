use super::{RequestInfo, RetryDecision, RetryPolicy, RetrySession};

/// Forwards all errors directly to the user, never retries
#[derive(Debug)]
pub struct FallthroughRetryPolicy;
pub struct FallthroughRetrySession;

impl FallthroughRetryPolicy {
    #[inline]
    pub fn new() -> FallthroughRetryPolicy {
        FallthroughRetryPolicy
    }
}

impl Default for FallthroughRetryPolicy {
    #[inline]
    fn default() -> FallthroughRetryPolicy {
        FallthroughRetryPolicy
    }
}

impl RetryPolicy for FallthroughRetryPolicy {
    #[inline]
    fn new_session(&self) -> Box<dyn RetrySession> {
        Box::new(FallthroughRetrySession)
    }
}

impl RetrySession for FallthroughRetrySession {
    #[inline]
    fn decide_should_retry(&mut self, _query_info: RequestInfo) -> RetryDecision {
        RetryDecision::DontRetry
    }

    #[inline]
    fn reset(&mut self) {}
}
