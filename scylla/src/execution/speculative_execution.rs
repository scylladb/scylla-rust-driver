use futures::{
    future::FutureExt,
    stream::{FuturesUnordered, StreamExt},
};
use scylla_cql::frame::response::error::DbError;
use std::{future::Future, sync::Arc, time::Duration};
use tracing::{trace_span, warn, Instrument};

use crate::execution::errors::QueryError;
use crate::execution::metrics::Metrics;

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
}

/// Checks if a result created in a speculative execution branch can be ignored.
///
/// We should ignore errors such that their presence when executing the request
/// on one node, does not imply that the same error will appear during retry on some other node.
fn can_be_ignored<ResT>(result: &Result<ResT, QueryError>) -> bool {
    match result {
        Ok(_) => false,
        // Do not remove this lint!
        // It's there for a reason - we don't want new variants
        // automatically fall under `_` pattern when they are introduced.
        #[deny(clippy::wildcard_enum_match_arm)]
        Err(e) => match e {
            // Errors that will almost certainly appear for other nodes as well
            QueryError::BadQuery(_)
            | QueryError::CqlRequestSerialization(_)
            | QueryError::BodyExtensionsParseError(_)
            | QueryError::CqlResultParseError(_)
            | QueryError::CqlErrorParseError(_)
            | QueryError::ProtocolError(_) => false,

            // EmptyPlan is not returned by `Session::execute_query`.
            // It is represented by None, which is then transformed
            // to QueryError::EmptyPlan by the caller
            // (either here is speculative_execution module, or for non-speculative execution).
            // I believe this should not be ignored, since we do not expect it here.
            QueryError::EmptyPlan => false,

            // Errors that should not appear here, thus should not be ignored
            #[allow(deprecated)]
            QueryError::NextRowError(_)
            | QueryError::IntoLegacyQueryResultError(_)
            | QueryError::TimeoutError
            | QueryError::RequestTimeout(_)
            | QueryError::MetadataError(_) => false,

            // Errors that can be ignored
            QueryError::BrokenConnection(_)
            | QueryError::UnableToAllocStreamId
            | QueryError::ConnectionPoolError(_) => true,

            // Handle DbErrors
            QueryError::DbError(db_error, _) => {
                // Do not remove this lint!
                // It's there for a reason - we don't want new variants
                // automatically fall under `_` pattern when they are introduced.
                #[deny(clippy::wildcard_enum_match_arm)]
                match db_error {
                        // Errors that will almost certainly appear on other nodes as well
                        DbError::SyntaxError
                        | DbError::Invalid
                        | DbError::AlreadyExists { .. }
                        | DbError::Unauthorized
                        | DbError::ProtocolError => false,

                        // Errors that should not appear there - thus, should not be ignored.
                        DbError::AuthenticationError | DbError::Other(_) => false,

                        // For now, let's assume that UDF failure is not transient - don't ignore it
                        // TODO: investigate
                        DbError::FunctionFailure { .. } => false,

                        // Not sure when these can appear - don't ignore them
                        // TODO: Investigate these errors
                        DbError::ConfigError | DbError::TruncateError => false,

                        // Errors that we can ignore and perform a retry on some other node
                        DbError::Unavailable { .. }
                        | DbError::Overloaded
                        | DbError::IsBootstrapping
                        | DbError::ReadTimeout { .. }
                        | DbError::WriteTimeout { .. }
                        | DbError::ReadFailure { .. }
                        | DbError::WriteFailure { .. }
                        // Preparation may succeed on some other node.
                        | DbError::Unprepared { .. }
                        | DbError::ServerError
                        | DbError::RateLimitReached { .. } => true,
                    }
            }
        },
    }
}

const EMPTY_PLAN_ERROR: QueryError = QueryError::EmptyPlan;

pub(crate) async fn execute<QueryFut, ResT>(
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
                if let Some(r) = res {
                    if !can_be_ignored(&r) {
                        return r;
                    } else {
                        last_error = Some(r)
                    }
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
