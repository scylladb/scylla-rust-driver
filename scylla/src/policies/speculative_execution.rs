//! Speculative execution mechanism allows the driver to send speculative requests to
//! multiple nodes in the cluster when the current target takes too long to respond.
//! This can help reduce latency for requests that may be slow due to network issues
//! or node load.

use futures::{
    future::FutureExt,
    stream::{FuturesUnordered, StreamExt},
};
#[cfg(feature = "metrics")]
use std::sync::Arc;
use std::{future::Future, time::Duration};
use tracing::{trace_span, Instrument};

use crate::errors::{RequestAttemptError, RequestError};
#[cfg(feature = "metrics")]
use crate::observability::metrics::Metrics;
use crate::response::Coordinator;

/// [`Context`] is passed as an argument to [`SpeculativeExecutionPolicy`] methods.
#[non_exhaustive]
pub struct Context {
    #[cfg(feature = "metrics")]
    /// Metrics instance that can be used as a context for deciding on speculative
    /// execution.
    pub metrics: Arc<Metrics>,
}

/// The policy that decides if the driver will send speculative queries to the
/// next targets when the current target takes too long to respond.
// TODO(2.0): Consider renaming the methods to get rid of "retry" naming.
pub trait SpeculativeExecutionPolicy: std::fmt::Debug + Send + Sync {
    /// The maximum number of speculative executions that will be triggered
    /// for a given request (does not include the initial request)
    fn max_retry_count(&self, context: &Context) -> usize;

    /// The delay between each speculative execution
    fn retry_interval(&self, context: &Context) -> Duration;
}

/// A [`SpeculativeExecutionPolicy`] that schedules a given number of speculative
/// executions, separated by a fixed delay.
#[derive(Debug, Clone)]
pub struct SimpleSpeculativeExecutionPolicy {
    /// The maximum number of speculative executions that will be triggered
    /// for a given request (does not include the initial request)
    pub max_retry_count: usize,

    /// The delay between each speculative execution
    pub retry_interval: Duration,
}

/// A policy that triggers speculative executions when the request to the current
/// target is above a given percentile.
#[cfg(feature = "metrics")]
#[derive(Debug, Clone)]
pub struct PercentileSpeculativeExecutionPolicy {
    /// The maximum number of speculative executions that will be triggered
    /// for a given request (does not include the initial request)
    pub max_retry_count: usize,

    /// The percentile that a request's latency must fall into to be considered
    /// slow (ex: 99.0)
    pub percentile: f64,
}

impl SpeculativeExecutionPolicy for SimpleSpeculativeExecutionPolicy {
    fn max_retry_count(&self, _: &Context) -> usize {
        self.max_retry_count
    }

    fn retry_interval(&self, _: &Context) -> Duration {
        self.retry_interval
    }
}

#[cfg(feature = "metrics")]
impl SpeculativeExecutionPolicy for PercentileSpeculativeExecutionPolicy {
    fn max_retry_count(&self, _: &Context) -> usize {
        self.max_retry_count
    }

    fn retry_interval(&self, context: &Context) -> Duration {
        let interval = context.metrics.get_latency_percentile_ms(self.percentile);
        let ms = match interval {
            Ok(d) => d,
            Err(e) => {
                tracing::warn!(
                    "Failed to get latency percentile ({}), defaulting to 100 ms",
                    e
                );
                100
            }
        };
        Duration::from_millis(ms)
    }
}

/// Checks if a result created in a speculative execution branch can be ignored.
///
/// We should ignore errors such that their presence when executing the request
/// on one node, does not imply that the same error will appear during retry on some other node.
fn can_be_ignored<ResT>(result: &Result<ResT, RequestError>) -> bool {
    match result {
        Ok(_) => false,
        // Do not remove this lint!
        // It's there for a reason - we don't want new variants
        // automatically fall under `_` pattern when they are introduced.
        #[deny(clippy::wildcard_enum_match_arm)]
        Err(e) => match e {
            // This error should not appear it. Anyway, if it possibly could
            // in the future, it should not be ignored.
            RequestError::EmptyPlan => false,

            // Request execution timed out.
            RequestError::RequestTimeout(_) => false,

            // Can try on another node.
            RequestError::ConnectionPoolError { .. } => true,

            RequestError::LastAttemptError(e) => {
                // Do not remove this lint!
                // It's there for a reason - we don't want new variants
                // automatically fall under `_` pattern when they are introduced.
                #[deny(clippy::wildcard_enum_match_arm)]
                match e {
                    // Errors that will almost certainly appear for other nodes as well
                    RequestAttemptError::SerializationError(_)
                    | RequestAttemptError::CqlRequestSerialization(_)
                    | RequestAttemptError::BodyExtensionsParseError(_)
                    | RequestAttemptError::CqlResultParseError(_)
                    | RequestAttemptError::CqlErrorParseError(_)
                    | RequestAttemptError::UnexpectedResponse(_)
                    | RequestAttemptError::RepreparedIdChanged { .. }
                    | RequestAttemptError::RepreparedIdMissingInBatch
                    | RequestAttemptError::NonfinishedPagingState => false,

                    // Errors that can be ignored
                    RequestAttemptError::BrokenConnectionError(_)
                    | RequestAttemptError::UnableToAllocStreamId => true,

                    // Handle DbErrors
                    RequestAttemptError::DbError(db_error, _) => db_error.can_speculative_retry(),
                }
            }
        },
    }
}

const EMPTY_PLAN_ERROR: RequestError = RequestError::EmptyPlan;

pub(crate) async fn execute<QueryFut, ResT>(
    policy: &dyn SpeculativeExecutionPolicy,
    context: &Context,
    mut query_runner_generator: impl FnMut(bool) -> QueryFut,
) -> Result<(ResT, Coordinator), RequestError>
where
    QueryFut: Future<Output = Option<Result<(ResT, Coordinator), RequestError>>>,
{
    let mut retries_remaining = policy.max_retry_count(context);
    let retry_interval = policy.retry_interval(context);

    let mut async_tasks = FuturesUnordered::new();
    async_tasks.push(
        query_runner_generator(false)
            .instrument(trace_span!("Speculative execution: original query")),
    );

    let sleep = tokio::time::sleep(retry_interval).fuse();
    tokio::pin!(sleep);

    let mut last_error = None;
    loop {
        futures::select! {
            _ = &mut sleep => {
                if retries_remaining > 0 {
                    async_tasks.push(query_runner_generator(true).instrument(trace_span!("Speculative execution", retries_remaining = retries_remaining)));
                    retries_remaining -= 1;

                    // reset the timeout
                    sleep.set(tokio::time::sleep(retry_interval).fuse());
                }
            }
            res = async_tasks.select_next_some() => {
                if let Some(r) = res {
                    if !can_be_ignored(&r) {
                        return r;
                    } else {
                        last_error = Some(r)
                    }
                } else {
                    // The only case where None is returned is when execution plan was exhausted.
                    // If so, there is no reason to start any more fibers.
                    // We can't always return - there may still be fibers running.
                    retries_remaining = 0;
                }
                if async_tasks.is_empty() && retries_remaining == 0 {
                    return last_error.unwrap_or({
                        Err(EMPTY_PLAN_ERROR)
                    });
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    // Important to start tests with paused clock. If starting unpaused, and calling `tokio::time::pause()`, then
    // things like `sleep` will advance the timer not fully accurately (I have no idea why), causing
    // few ms added clock advancement at the end of the test.
    // Starting paused is done with `#[tokio::test(flavor = "current_thread", start_paused = true)]`.
    // Pausing can only be done with current_thread executor.

    #[cfg(feature = "metrics")]
    use std::sync::Arc;
    use std::sync::LazyLock;
    use std::time::Duration;

    use assert_matches::assert_matches;

    use crate::errors::{RequestAttemptError, RequestError};
    #[cfg(feature = "metrics")]
    use crate::observability::metrics::Metrics;
    use crate::policies::speculative_execution::{Context, SimpleSpeculativeExecutionPolicy};
    use crate::response::Coordinator;

    static EMPTY_CONTEXT: LazyLock<Context> = LazyLock::new(|| Context {
        #[cfg(feature = "metrics")]
        metrics: Arc::new(Metrics::new()),
    });

    static IGNORABLE_ERROR: Option<Result<((), Coordinator), RequestError>> = Some(Err(
        RequestError::LastAttemptError(RequestAttemptError::UnableToAllocStreamId),
    ));

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn test_exhausted_plan_with_running_fibers() {
        let policy = SimpleSpeculativeExecutionPolicy {
            max_retry_count: 5,
            retry_interval: Duration::from_secs(1),
        };

        let generator = {
            // Index of the fiber, 0 for first execution.
            let mut counter = 0;
            move |_first: bool| {
                let future = {
                    let fiber_idx = counter;
                    async move {
                        match fiber_idx.cmp(&4) {
                            std::cmp::Ordering::Less => {
                                tokio::time::sleep(Duration::from_secs(5)).await;
                                IGNORABLE_ERROR.clone()
                            }
                            std::cmp::Ordering::Equal => None,
                            std::cmp::Ordering::Greater => {
                                panic!("Too many speculative executions - expected 4")
                            }
                        }
                    }
                };
                counter += 1;
                future
            }
        };

        let now = tokio::time::Instant::now();
        let res = super::execute(&policy, &EMPTY_CONTEXT, generator).await;
        assert_matches!(
            res,
            Err(RequestError::LastAttemptError(
                RequestAttemptError::UnableToAllocStreamId
            ))
        );
        // t - now
        // First execution is started at t
        // Speculative executions - at t+1, t+2, t+3, t+4
        // The one at t+4 will return first, with None, preventing starting new one at t+5.
        // Then execute should wait on spawned fibers. Last one will be the one spawned at t+3, finishing at t+8.
        assert_eq!(
            tokio::time::Instant::now(),
            now.checked_add(Duration::from_secs(8)).unwrap()
        )
    }

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn test_exhausted_plan_last_running_fiber() {
        let policy = SimpleSpeculativeExecutionPolicy {
            max_retry_count: 5,
            // Each attempt will finish before next starts
            retry_interval: Duration::from_secs(6),
        };

        let generator = {
            // Index of the fiber, 0 for first execution.
            let mut counter = 0;
            move |_first: bool| {
                let future = {
                    let fiber_idx = counter;
                    async move {
                        match fiber_idx.cmp(&4) {
                            std::cmp::Ordering::Less => {
                                tokio::time::sleep(Duration::from_secs(5)).await;
                                IGNORABLE_ERROR.clone()
                            }
                            std::cmp::Ordering::Equal => None,
                            std::cmp::Ordering::Greater => {
                                panic!("Too many speculative executions - expected 4")
                            }
                        }
                    }
                };
                counter += 1;
                future
            }
        };

        let now = tokio::time::Instant::now();
        let res = super::execute(&policy, &EMPTY_CONTEXT, generator).await;
        assert_matches!(
            res,
            Err(RequestError::LastAttemptError(
                RequestAttemptError::UnableToAllocStreamId
            ))
        );
        // t - now
        // First execution is started at t
        // Speculative executions - at t+6, t+12, t+18, t+24
        // Each execution finishes before next starts. The one at t+24 finishes instantly with
        // None, so the next one should not be started.
        assert_eq!(
            tokio::time::Instant::now(),
            now.checked_add(Duration::from_secs(24)).unwrap()
        )
    }

    // Regresion test for https://github.com/scylladb/scylla-rust-driver/issues/1085
    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn test_se_panic_on_ignorable_errors() {
        let policy = SimpleSpeculativeExecutionPolicy {
            max_retry_count: 5,
            // Each attempt will finish before next starts
            retry_interval: Duration::from_secs(1),
        };

        let generator = {
            move |_first: bool| async move {
                tokio::time::sleep(Duration::from_secs(5)).await;
                IGNORABLE_ERROR.clone()
            }
        };

        let now = tokio::time::Instant::now();
        let res = super::execute(&policy, &EMPTY_CONTEXT, generator).await;
        assert_matches!(
            res,
            Err(RequestError::LastAttemptError(
                RequestAttemptError::UnableToAllocStreamId
            ))
        );
        // t - now
        // First execution is started at t
        // Speculative executions - at t+1, t+2, t+3, t+4, t+5
        // Each execution sleeps 5 seconds and returns ignorable error.
        // Last execution should finish at t+10.
        assert_eq!(
            tokio::time::Instant::now(),
            now.checked_add(Duration::from_secs(10)).unwrap()
        )
    }
}
