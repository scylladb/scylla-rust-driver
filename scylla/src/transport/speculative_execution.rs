use futures::{
    future::FutureExt,
    stream::{FuturesUnordered, StreamExt},
};
use std::{future::Future, sync::Arc, time::Duration};
use tracing::{trace_span, warn, Instrument};

use super::{errors::QueryError, metrics::Metrics};

/// Context is passed as an argument to `SpeculativeExecutionPolicy` methods
pub struct Context {
    pub metrics: Arc<Metrics>,
}

/// The policy that decides if the driver will send speculative queries to the
/// next hosts when the current host takes too long to respond.
pub trait SpeculativeExecutionPolicy: std::fmt::Debug + Send + Sync {
    /// The maximum number of speculative executions that will be triggered
    /// for a given request (does not include the initial request)
    fn max_retry_count(&self, context: &Context) -> usize;

    /// The delay between each speculative execution
    fn retry_interval(&self, context: &Context) -> Duration;

    fn name(&self) -> &'static str;
}

/// A SpeculativeExecutionPolicy that schedules a given number of speculative
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
/// host is above a given percentile.
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

    fn name(&self) -> &'static str {
        "SimpleSpeculativeExecutionPolicy"
    }
}

impl SpeculativeExecutionPolicy for PercentileSpeculativeExecutionPolicy {
    fn max_retry_count(&self, _: &Context) -> usize {
        self.max_retry_count
    }

    fn retry_interval(&self, context: &Context) -> Duration {
        let interval = context.metrics.get_latency_percentile_ms(self.percentile);
        let ms = match interval {
            Ok(d) => d,
            Err(e) => {
                warn!(
                    "Failed to get latency percentile ({}), defaulting to 100 ms",
                    e
                );
                100
            }
        };
        Duration::from_millis(ms)
    }

    fn name(&self) -> &'static str {
        "PercentileSpeculativeExecutionPolicy"
    }
}

// checks if a result created in a speculative execution branch can be ignored
fn can_be_ignored<ResT>(result: &Result<ResT, QueryError>) -> bool {
    match result {
        Ok(_) => false,
        Err(QueryError::IoError(_)) => true,
        Err(QueryError::TimeoutError) => true,
        _ => false,
    }
}

const EMPTY_PLAN_ERROR: QueryError = QueryError::ProtocolError("Empty query plan - driver bug!");

pub async fn execute<QueryFut, ResT>(
    policy: &dyn SpeculativeExecutionPolicy,
    context: &Context,
    query_runner_generator: impl Fn(bool) -> QueryFut,
) -> Result<ResT, QueryError>
where
    QueryFut: Future<Output = Option<Result<ResT, QueryError>>>,
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
                match res {
                    Some(r) => {
                        if !can_be_ignored(&r) {
                            return r;
                        } else {
                            last_error = Some(r)
                        }
                    },
                    None =>  {
                        if async_tasks.is_empty() && retries_remaining == 0 {
                            return last_error.unwrap_or({
                                Err(EMPTY_PLAN_ERROR)
                            });
                        }
                    },
                }
            }
        }
    }
}
