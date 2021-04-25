use futures::future::FutureExt;
use futures::stream::{FuturesUnordered, StreamExt};
use std::future::Future;
use std::time::Duration;

use super::errors::QueryError;

#[derive(Debug, Clone)]
pub struct SpeculativeExecutionPolicy {
    pub attempts_count: usize,
    pub delay: Duration,
}

impl SpeculativeExecutionPolicy {
    pub async fn execute<QueryFut, ResT>(
        &self,
        query_runner_generator: impl Fn() -> QueryFut,
    ) -> Result<ResT, QueryError>
    where
        QueryFut: Future<Output = Option<Result<ResT, QueryError>>>,
    {
        let mut available_attempts = self.attempts_count;

        let mut async_tasks = FuturesUnordered::new();
        async_tasks.push(query_runner_generator());

        let sleep = tokio::time::sleep(self.delay).fuse();
        tokio::pin!(sleep);

        loop {
            futures::select! {
                _ = &mut sleep => {
                    if available_attempts > 0 {
                        async_tasks.push(query_runner_generator());
                        available_attempts -= 1;

                        // reset the timeout
                        sleep.set(tokio::time::sleep(self.delay).fuse());
                    }
                }
                res = async_tasks.select_next_some() => {
                    match res {
                        Some(r) => return r,
                        None =>  {
                            if async_tasks.is_empty() && available_attempts == 0 {
                                return Err(QueryError::ProtocolError(
                                    "Empty query plan - driver bug!",
                                ));
                            }
                            continue
                        },
                    }
                }
            }
        }
    }
}
