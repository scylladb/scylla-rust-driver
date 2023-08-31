use crate::execution::errors::{DbError, QueryError, WriteType};

use super::{QueryInfo, RetryDecision, RetryPolicy, RetrySession};

/// Default retry policy - retries when there is a high chance that a retry might help.\
/// Behaviour based on [DataStax Java Driver](https://docs.datastax.com/en/developer/java-driver/4.10/manual/core/retries/)
#[derive(Debug)]
pub struct DefaultRetryPolicy;

impl DefaultRetryPolicy {
    pub fn new() -> DefaultRetryPolicy {
        DefaultRetryPolicy
    }
}

impl Default for DefaultRetryPolicy {
    fn default() -> DefaultRetryPolicy {
        DefaultRetryPolicy::new()
    }
}

impl RetryPolicy for DefaultRetryPolicy {
    fn new_session(&self) -> Box<dyn RetrySession> {
        Box::new(DefaultRetrySession::new())
    }

    fn clone_boxed(&self) -> Box<dyn RetryPolicy> {
        Box::new(DefaultRetryPolicy)
    }
}

pub struct DefaultRetrySession {
    was_unavailable_retry: bool,
    was_read_timeout_retry: bool,
    was_write_timeout_retry: bool,
}

impl DefaultRetrySession {
    pub fn new() -> DefaultRetrySession {
        DefaultRetrySession {
            was_unavailable_retry: false,
            was_read_timeout_retry: false,
            was_write_timeout_retry: false,
        }
    }
}

impl Default for DefaultRetrySession {
    fn default() -> DefaultRetrySession {
        DefaultRetrySession::new()
    }
}

impl RetrySession for DefaultRetrySession {
    fn decide_should_retry(&mut self, query_info: QueryInfo) -> RetryDecision {
        if query_info.consistency.is_serial() {
            return RetryDecision::DontRetry;
        };
        match query_info.error {
            // Basic errors - there are some problems on this node
            // Retry on a different one if possible
            QueryError::IoError(_)
            | QueryError::DbError(DbError::Overloaded, _)
            | QueryError::DbError(DbError::ServerError, _)
            | QueryError::DbError(DbError::TruncateError, _) => {
                if query_info.is_idempotent {
                    RetryDecision::RetryNextNode(None)
                } else {
                    RetryDecision::DontRetry
                }
            }
            // Unavailable - the current node believes that not enough nodes
            // are alive to satisfy specified consistency requirements.
            // Maybe this node has network problems - try a different one.
            // Perform at most one retry - it's unlikely that two nodes
            // have network problems at the same time
            QueryError::DbError(DbError::Unavailable { .. }, _) => {
                if !self.was_unavailable_retry {
                    self.was_unavailable_retry = true;
                    RetryDecision::RetryNextNode(None)
                } else {
                    RetryDecision::DontRetry
                }
            }
            // ReadTimeout - coordinator didn't receive enough replies in time.
            // Retry at most once and only if there were actually enough replies
            // to satisfy consistency but they were all just checksums (data_present == false).
            // This happens when the coordinator picked replicas that were overloaded/dying.
            // Retried request should have some useful response because the node will detect
            // that these replicas are dead.
            QueryError::DbError(
                DbError::ReadTimeout {
                    received,
                    required,
                    data_present,
                    ..
                },
                _,
            ) => {
                if !self.was_read_timeout_retry && received >= required && !*data_present {
                    self.was_read_timeout_retry = true;
                    RetryDecision::RetrySameNode(None)
                } else {
                    RetryDecision::DontRetry
                }
            }
            // Write timeout - coordinator didn't receive enough replies in time.
            // Retry at most once and only for BatchLog write.
            // Coordinator probably didn't detect the nodes as dead.
            // By the time we retry they should be detected as dead.
            QueryError::DbError(DbError::WriteTimeout { write_type, .. }, _) => {
                if !self.was_write_timeout_retry
                    && query_info.is_idempotent
                    && *write_type == WriteType::BatchLog
                {
                    self.was_write_timeout_retry = true;
                    RetryDecision::RetrySameNode(None)
                } else {
                    RetryDecision::DontRetry
                }
            }
            // The node is still bootstrapping it can't execute the query, we should try another one
            QueryError::DbError(DbError::IsBootstrapping, _) => RetryDecision::RetryNextNode(None),
            // Connection to the contacted node is overloaded, try another one
            QueryError::UnableToAllocStreamId => RetryDecision::RetryNextNode(None),
            // In all other cases propagate the error to the user
            _ => RetryDecision::DontRetry,
        }
    }

    fn reset(&mut self) {
        *self = DefaultRetrySession::new();
    }
}

#[cfg(test)]
mod tests {
    use super::{DefaultRetryPolicy, QueryInfo, RetryDecision, RetryPolicy};
    use crate::execution::errors::{BadQuery, DbError, QueryError, WriteType};
    use crate::statement::Consistency;
    use bytes::Bytes;
    use std::io::ErrorKind;
    use std::sync::Arc;

    fn make_query_info(error: &QueryError, is_idempotent: bool) -> QueryInfo<'_> {
        QueryInfo {
            error,
            is_idempotent,
            consistency: Consistency::One,
        }
    }

    // Asserts that default policy never retries for this Error
    fn default_policy_assert_never_retries(error: QueryError) {
        let mut policy = DefaultRetryPolicy::new().new_session();
        assert_eq!(
            policy.decide_should_retry(make_query_info(&error, false)),
            RetryDecision::DontRetry
        );

        let mut policy = DefaultRetryPolicy::new().new_session();
        assert_eq!(
            policy.decide_should_retry(make_query_info(&error, true)),
            RetryDecision::DontRetry
        );
    }

    #[test]
    fn default_never_retries() {
        let never_retried_dberrors = vec![
            DbError::SyntaxError,
            DbError::Invalid,
            DbError::AlreadyExists {
                keyspace: String::new(),
                table: String::new(),
            },
            DbError::FunctionFailure {
                keyspace: String::new(),
                function: String::new(),
                arg_types: vec![],
            },
            DbError::AuthenticationError,
            DbError::Unauthorized,
            DbError::ConfigError,
            DbError::ReadFailure {
                consistency: Consistency::Two,
                received: 2,
                required: 1,
                numfailures: 1,
                data_present: false,
            },
            DbError::WriteFailure {
                consistency: Consistency::Two,
                received: 1,
                required: 2,
                numfailures: 1,
                write_type: WriteType::BatchLog,
            },
            DbError::Unprepared {
                statement_id: Bytes::from_static(b"deadbeef"),
            },
            DbError::ProtocolError,
            DbError::Other(0x124816),
        ];

        for dberror in never_retried_dberrors {
            default_policy_assert_never_retries(QueryError::DbError(dberror, String::new()));
        }

        default_policy_assert_never_retries(QueryError::BadQuery(BadQuery::Other(
            "Length of provided values must be equal to number of batch statements \
                        (got 1 values, 2 statements)"
                .to_owned(),
        )));
        default_policy_assert_never_retries(QueryError::ProtocolError("test"));
    }

    // Asserts that for this error policy retries on next on idempotent queries only
    fn default_policy_assert_idempotent_next(error: QueryError) {
        let mut policy = DefaultRetryPolicy::new().new_session();
        assert_eq!(
            policy.decide_should_retry(make_query_info(&error, false)),
            RetryDecision::DontRetry
        );

        let mut policy = DefaultRetryPolicy::new().new_session();
        assert_eq!(
            policy.decide_should_retry(make_query_info(&error, true)),
            RetryDecision::RetryNextNode(None)
        );
    }

    #[test]
    fn default_idempotent_next_retries() {
        let idempotent_next_errors = vec![
            QueryError::DbError(DbError::Overloaded, String::new()),
            QueryError::DbError(DbError::TruncateError, String::new()),
            QueryError::DbError(DbError::ServerError, String::new()),
            QueryError::IoError(Arc::new(std::io::Error::new(ErrorKind::Other, "test"))),
        ];

        for error in idempotent_next_errors {
            default_policy_assert_idempotent_next(error);
        }
    }

    // Always retry on next node if current one is bootstrapping
    #[test]
    fn default_bootstrapping() {
        let error = QueryError::DbError(DbError::IsBootstrapping, String::new());

        let mut policy = DefaultRetryPolicy::new().new_session();
        assert_eq!(
            policy.decide_should_retry(make_query_info(&error, false)),
            RetryDecision::RetryNextNode(None)
        );

        let mut policy = DefaultRetryPolicy::new().new_session();
        assert_eq!(
            policy.decide_should_retry(make_query_info(&error, true)),
            RetryDecision::RetryNextNode(None)
        );
    }

    // On Unavailable error we retry one time no matter the idempotence
    #[test]
    fn default_unavailable() {
        let error = QueryError::DbError(
            DbError::Unavailable {
                consistency: Consistency::Two,
                required: 2,
                alive: 1,
            },
            String::new(),
        );

        let mut policy_not_idempotent = DefaultRetryPolicy::new().new_session();
        assert_eq!(
            policy_not_idempotent.decide_should_retry(make_query_info(&error, false)),
            RetryDecision::RetryNextNode(None)
        );
        assert_eq!(
            policy_not_idempotent.decide_should_retry(make_query_info(&error, false)),
            RetryDecision::DontRetry
        );

        let mut policy_idempotent = DefaultRetryPolicy::new().new_session();
        assert_eq!(
            policy_idempotent.decide_should_retry(make_query_info(&error, true)),
            RetryDecision::RetryNextNode(None)
        );
        assert_eq!(
            policy_idempotent.decide_should_retry(make_query_info(&error, true)),
            RetryDecision::DontRetry
        );
    }

    // On ReadTimeout we retry one time if there were enough responses and the data was present no matter the idempotence
    #[test]
    fn default_read_timeout() {
        // Enough responses and data_present == false - coordinator received only checksums
        let enough_responses_no_data = QueryError::DbError(
            DbError::ReadTimeout {
                consistency: Consistency::Two,
                received: 2,
                required: 2,
                data_present: false,
            },
            String::new(),
        );

        // Not idempotent
        let mut policy = DefaultRetryPolicy::new().new_session();
        assert_eq!(
            policy.decide_should_retry(make_query_info(&enough_responses_no_data, false)),
            RetryDecision::RetrySameNode(None)
        );
        assert_eq!(
            policy.decide_should_retry(make_query_info(&enough_responses_no_data, false)),
            RetryDecision::DontRetry
        );

        // Idempotent
        let mut policy = DefaultRetryPolicy::new().new_session();
        assert_eq!(
            policy.decide_should_retry(make_query_info(&enough_responses_no_data, true)),
            RetryDecision::RetrySameNode(None)
        );
        assert_eq!(
            policy.decide_should_retry(make_query_info(&enough_responses_no_data, true)),
            RetryDecision::DontRetry
        );

        // Enough responses but data_present == true - coordinator probably timed out
        // waiting for read-repair acknowledgement.
        let enough_responses_with_data = QueryError::DbError(
            DbError::ReadTimeout {
                consistency: Consistency::Two,
                received: 2,
                required: 2,
                data_present: true,
            },
            String::new(),
        );

        // Not idempotent
        let mut policy = DefaultRetryPolicy::new().new_session();
        assert_eq!(
            policy.decide_should_retry(make_query_info(&enough_responses_with_data, false)),
            RetryDecision::DontRetry
        );

        // Idempotent
        let mut policy = DefaultRetryPolicy::new().new_session();
        assert_eq!(
            policy.decide_should_retry(make_query_info(&enough_responses_with_data, true)),
            RetryDecision::DontRetry
        );

        // Not enough responses, data_present == true
        let not_enough_responses_with_data = QueryError::DbError(
            DbError::ReadTimeout {
                consistency: Consistency::Two,
                received: 1,
                required: 2,
                data_present: true,
            },
            String::new(),
        );

        // Not idempotent
        let mut policy = DefaultRetryPolicy::new().new_session();
        assert_eq!(
            policy.decide_should_retry(make_query_info(&not_enough_responses_with_data, false)),
            RetryDecision::DontRetry
        );

        // Idempotent
        let mut policy = DefaultRetryPolicy::new().new_session();
        assert_eq!(
            policy.decide_should_retry(make_query_info(&not_enough_responses_with_data, true)),
            RetryDecision::DontRetry
        );
    }

    // WriteTimeout will retry once when the query is idempotent and write_type == BatchLog
    #[test]
    fn default_write_timeout() {
        // WriteType == BatchLog
        let good_write_type = QueryError::DbError(
            DbError::WriteTimeout {
                consistency: Consistency::Two,
                received: 1,
                required: 2,
                write_type: WriteType::BatchLog,
            },
            String::new(),
        );

        // Not idempotent
        let mut policy = DefaultRetryPolicy::new().new_session();
        assert_eq!(
            policy.decide_should_retry(make_query_info(&good_write_type, false)),
            RetryDecision::DontRetry
        );

        // Idempotent
        let mut policy = DefaultRetryPolicy::new().new_session();
        assert_eq!(
            policy.decide_should_retry(make_query_info(&good_write_type, true)),
            RetryDecision::RetrySameNode(None)
        );
        assert_eq!(
            policy.decide_should_retry(make_query_info(&good_write_type, true)),
            RetryDecision::DontRetry
        );

        // WriteType != BatchLog
        let bad_write_type = QueryError::DbError(
            DbError::WriteTimeout {
                consistency: Consistency::Two,
                received: 4,
                required: 2,
                write_type: WriteType::Simple,
            },
            String::new(),
        );

        // Not idempotent
        let mut policy = DefaultRetryPolicy::new().new_session();
        assert_eq!(
            policy.decide_should_retry(make_query_info(&bad_write_type, false)),
            RetryDecision::DontRetry
        );

        // Idempotent
        let mut policy = DefaultRetryPolicy::new().new_session();
        assert_eq!(
            policy.decide_should_retry(make_query_info(&bad_write_type, true)),
            RetryDecision::DontRetry
        );
    }
}
