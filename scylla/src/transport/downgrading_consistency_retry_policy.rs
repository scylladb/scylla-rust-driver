use scylla_cql::{
    errors::{DbError, QueryError, WriteType},
    frame::types::LegacyConsistency,
    Consistency,
};
use tracing::debug;

use crate::retry_policy::{QueryInfo, RetryDecision, RetryPolicy, RetrySession};

/// Downgrading consistency retry policy - retries with lower consistency level if it knows\
/// that the initial CL is unreachable. Also, it behaves as [DefaultRetryPolicy] when it believes that the initial CL is reachable.
/// Behaviour based on [DataStax Java Driver]\
///(https://docs.datastax.com/en/drivers/java/3.11/com/datastax/driver/core/policies/DowngradingConsistencyRetryPolicy.html)
#[derive(Debug)]
pub struct DowngradingConsistencyRetryPolicy;

impl DowngradingConsistencyRetryPolicy {
    pub fn new() -> DowngradingConsistencyRetryPolicy {
        DowngradingConsistencyRetryPolicy
    }
}

impl Default for DowngradingConsistencyRetryPolicy {
    fn default() -> DowngradingConsistencyRetryPolicy {
        DowngradingConsistencyRetryPolicy::new()
    }
}

impl RetryPolicy for DowngradingConsistencyRetryPolicy {
    fn new_session(&self) -> Box<dyn RetrySession> {
        Box::new(DowngradingConsistencyRetrySession::new())
    }

    fn clone_boxed(&self) -> Box<dyn RetryPolicy> {
        Box::new(DowngradingConsistencyRetryPolicy)
    }
}

pub struct DowngradingConsistencyRetrySession {
    was_retry: bool,
}

impl DowngradingConsistencyRetrySession {
    pub fn new() -> DowngradingConsistencyRetrySession {
        DowngradingConsistencyRetrySession { was_retry: false }
    }
}

impl Default for DowngradingConsistencyRetrySession {
    fn default() -> DowngradingConsistencyRetrySession {
        DowngradingConsistencyRetrySession::new()
    }
}

impl RetrySession for DowngradingConsistencyRetrySession {
    fn decide_should_retry(&mut self, query_info: QueryInfo) -> RetryDecision {
        let cl = match query_info.consistency {
            LegacyConsistency::Serial(_) => return RetryDecision::DontRetry, // FIXME: is this proper behaviour?
            LegacyConsistency::Regular(cl) => cl,
        };

        fn max_likely_to_work_cl(known_ok: i32, previous_cl: Consistency) -> RetryDecision {
            let decision = if known_ok >= 3 {
                RetryDecision::RetrySameNode(Consistency::Three)
            } else if known_ok == 2 {
                RetryDecision::RetrySameNode(Consistency::Two)
            } else if known_ok == 1 || previous_cl == Consistency::EachQuorum {
                // JAVA-1005: EACH_QUORUM does not report a global number of alive replicas
                // so even if we get 0 alive replicas, there might be
                // a node up in some other datacenter
                RetryDecision::RetrySameNode(Consistency::One)
            } else {
                RetryDecision::DontRetry
            };
            if let RetryDecision::RetrySameNode(new_cl) = decision {
                debug!(
                    "Decided to lower required consistency from {} to {}.",
                    previous_cl, new_cl
                );
            }
            decision
        }

        match query_info.error {
            // Basic errors - there are some problems on this node
            // Retry on a different one if possible
            QueryError::IoError(_)
            | QueryError::DbError(DbError::Overloaded, _)
            | QueryError::DbError(DbError::ServerError, _)
            | QueryError::DbError(DbError::TruncateError, _) => {
                if query_info.is_idempotent {
                    RetryDecision::RetryNextNode(cl)
                } else {
                    RetryDecision::DontRetry
                }
            }
            // Unavailable - the current node believes that not enough nodes
            // are alive to satisfy specified consistency requirements.
            QueryError::DbError(DbError::Unavailable { alive, .. }, _) => {
                if !self.was_retry {
                    self.was_retry = true;
                    max_likely_to_work_cl(*alive, cl)
                } else {
                    RetryDecision::DontRetry
                }
            }
            // ReadTimeout - coordinator didn't receive enough replies in time.
            QueryError::DbError(
                DbError::ReadTimeout {
                    received,
                    required,
                    data_present,
                    ..
                },
                _,
            ) => {
                if self.was_retry {
                    RetryDecision::DontRetry
                } else if received < required {
                    self.was_retry = true;
                    max_likely_to_work_cl(*received, cl)
                } else if !*data_present {
                    self.was_retry = true;
                    RetryDecision::RetrySameNode(cl)
                } else {
                    RetryDecision::DontRetry
                }
            }
            // Write timeout - coordinator didn't receive enough replies in time.
            QueryError::DbError(
                DbError::WriteTimeout {
                    write_type,
                    received,
                    ..
                },
                _,
            ) => {
                if self.was_retry || !query_info.is_idempotent {
                    RetryDecision::DontRetry
                } else {
                    self.was_retry = true;
                    match write_type {
                        WriteType::Batch | WriteType::Simple if *received > 0 => {
                            RetryDecision::IgnoreWriteError
                        }

                        WriteType::UnloggedBatch => {
                            // Since only part of the batch could have been persisted,
                            // retry with whatever consistency should allow to persist all
                            max_likely_to_work_cl(*received, cl)
                        }
                        WriteType::BatchLog => RetryDecision::RetrySameNode(cl),

                        _ => RetryDecision::DontRetry,
                    }
                }
            }
            // The node is still bootstrapping it can't execute the query, we should try another one
            QueryError::DbError(DbError::IsBootstrapping, _) => RetryDecision::RetryNextNode(cl),
            // Connection to the contacted node is overloaded, try another one
            QueryError::UnableToAllocStreamId => RetryDecision::RetryNextNode(cl),
            // In all other cases propagate the error to the user
            _ => RetryDecision::DontRetry,
        }
    }

    fn reset(&mut self) {
        *self = DowngradingConsistencyRetrySession::new();
    }
}
