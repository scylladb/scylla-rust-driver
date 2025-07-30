use scylla_cql::frame::response::error::{DbError, WriteType};

use crate::errors::RequestAttemptError;

use super::{RequestInfo, RetryDecision, RetryPolicy, RetrySession};

/// Default retry policy - retries when there is a high chance that a retry might help.\
/// Behaviour based on [DataStax Java Driver](https://docs.datastax.com/en/developer/java-driver/4.10/manual/core/retries/)
#[derive(Debug)]
pub struct DefaultRetryPolicy;

impl DefaultRetryPolicy {
    /// Creates a new instance of [DefaultRetryPolicy].
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
}

/// Implementation of [RetrySession] for [DefaultRetryPolicy].
pub struct DefaultRetrySession {
    was_unavailable_retry: bool,
    was_read_timeout_retry: bool,
    was_write_timeout_retry: bool,
}

impl DefaultRetrySession {
    /// Creates a new instance of [DefaultRetrySession].
    // TODO(2.0): unpub this.
    pub fn new() -> DefaultRetrySession {
        DefaultRetrySession {
            was_unavailable_retry: false,
            was_read_timeout_retry: false,
            was_write_timeout_retry: false,
        }
    }
}

// TODO(2.0): remove this.
impl Default for DefaultRetrySession {
    fn default() -> DefaultRetrySession {
        DefaultRetrySession::new()
    }
}

impl RetrySession for DefaultRetrySession {
    fn decide_should_retry(&mut self, request_info: RequestInfo) -> RetryDecision {
        if request_info.consistency.is_serial() {
            return RetryDecision::DontRetry;
        };
        match request_info.error {
            // Basic errors - there are some problems on this node
            // Retry on a different one if possible
            RequestAttemptError::BrokenConnectionError(_)
            | RequestAttemptError::DbError(DbError::Overloaded, _)
            | RequestAttemptError::DbError(DbError::ServerError, _)
            | RequestAttemptError::DbError(DbError::TruncateError, _) => {
                if request_info.is_idempotent {
                    RetryDecision::RetryNextTarget(None)
                } else {
                    RetryDecision::DontRetry
                }
            }
            // Unavailable - the current node believes that not enough nodes
            // are alive to satisfy specified consistency requirements.
            // Maybe this node has network problems - try a different one.
            // Perform at most one retry - it's unlikely that two nodes
            // have network problems at the same time
            RequestAttemptError::DbError(DbError::Unavailable { .. }, _) => {
                if !self.was_unavailable_retry {
                    self.was_unavailable_retry = true;
                    RetryDecision::RetryNextTarget(None)
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
            RequestAttemptError::DbError(
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
                    RetryDecision::RetrySameTarget(None)
                } else {
                    RetryDecision::DontRetry
                }
            }
            // Write timeout - coordinator didn't receive enough replies in time.
            // Retry at most once and only for BatchLog write.
            // Coordinator probably didn't detect the nodes as dead.
            // By the time we retry they should be detected as dead.
            RequestAttemptError::DbError(DbError::WriteTimeout { write_type, .. }, _) => {
                if !self.was_write_timeout_retry
                    && request_info.is_idempotent
                    && *write_type == WriteType::BatchLog
                {
                    self.was_write_timeout_retry = true;
                    RetryDecision::RetrySameTarget(None)
                } else {
                    RetryDecision::DontRetry
                }
            }
            // The node is still bootstrapping it can't execute the request, we should try another one
            RequestAttemptError::DbError(DbError::IsBootstrapping, _) => {
                RetryDecision::RetryNextTarget(None)
            }
            // Connection to the contacted node is overloaded, try another one
            RequestAttemptError::UnableToAllocStreamId => RetryDecision::RetryNextTarget(None),
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
    use super::{DefaultRetryPolicy, RequestInfo, RetryDecision, RetryPolicy};
    use crate::errors::{BrokenConnectionErrorKind, RequestAttemptError};
    use crate::errors::{DbError, WriteType};
    use crate::statement::Consistency;
    use crate::test_utils::setup_tracing;
    use bytes::Bytes;
    use scylla_cql::frame::frame_errors::{BatchSerializationError, CqlRequestSerializationError};

    fn make_request_info(error: &RequestAttemptError, is_idempotent: bool) -> RequestInfo<'_> {
        RequestInfo {
            error,
            is_idempotent,
            consistency: Consistency::One,
        }
    }

    // Asserts that default policy never retries for this Error
    fn default_policy_assert_never_retries(error: RequestAttemptError) {
        let mut policy = DefaultRetryPolicy::new().new_session();
        assert_eq!(
            policy.decide_should_retry(make_request_info(&error, false)),
            RetryDecision::DontRetry
        );

        let mut policy = DefaultRetryPolicy::new().new_session();
        assert_eq!(
            policy.decide_should_retry(make_request_info(&error, true)),
            RetryDecision::DontRetry
        );
    }

    #[test]
    fn default_never_retries() {
        setup_tracing();
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
            default_policy_assert_never_retries(RequestAttemptError::DbError(
                dberror,
                String::new(),
            ));
        }

        default_policy_assert_never_retries(RequestAttemptError::RepreparedIdMissingInBatch);
        default_policy_assert_never_retries(RequestAttemptError::RepreparedIdChanged {
            statement: String::new(),
            expected_id: vec![],
            reprepared_id: vec![],
        });
        default_policy_assert_never_retries(RequestAttemptError::CqlRequestSerialization(
            CqlRequestSerializationError::BatchSerialization(
                BatchSerializationError::TooManyStatements(u16::MAX as usize + 1),
            ),
        ));
    }

    // Asserts that for this error policy retries on next on idempotent queries only
    fn default_policy_assert_idempotent_next(error: RequestAttemptError) {
        let mut policy = DefaultRetryPolicy::new().new_session();
        assert_eq!(
            policy.decide_should_retry(make_request_info(&error, false)),
            RetryDecision::DontRetry
        );

        let mut policy = DefaultRetryPolicy::new().new_session();
        assert_eq!(
            policy.decide_should_retry(make_request_info(&error, true)),
            RetryDecision::RetryNextTarget(None)
        );
    }

    #[test]
    fn default_idempotent_next_retries() {
        setup_tracing();
        let idempotent_next_errors = vec![
            RequestAttemptError::DbError(DbError::Overloaded, String::new()),
            RequestAttemptError::DbError(DbError::TruncateError, String::new()),
            RequestAttemptError::DbError(DbError::ServerError, String::new()),
            RequestAttemptError::BrokenConnectionError(
                BrokenConnectionErrorKind::TooManyOrphanedStreamIds(5).into(),
            ),
        ];

        for error in idempotent_next_errors {
            default_policy_assert_idempotent_next(error);
        }
    }

    // Always retry on next node if current one is bootstrapping
    #[test]
    fn default_bootstrapping() {
        setup_tracing();
        let error = RequestAttemptError::DbError(DbError::IsBootstrapping, String::new());

        let mut policy = DefaultRetryPolicy::new().new_session();
        assert_eq!(
            policy.decide_should_retry(make_request_info(&error, false)),
            RetryDecision::RetryNextTarget(None)
        );

        let mut policy = DefaultRetryPolicy::new().new_session();
        assert_eq!(
            policy.decide_should_retry(make_request_info(&error, true)),
            RetryDecision::RetryNextTarget(None)
        );
    }

    // On Unavailable error we retry one time no matter the idempotence
    #[test]
    fn default_unavailable() {
        setup_tracing();
        let error = RequestAttemptError::DbError(
            DbError::Unavailable {
                consistency: Consistency::Two,
                required: 2,
                alive: 1,
            },
            String::new(),
        );

        let mut policy_not_idempotent = DefaultRetryPolicy::new().new_session();
        assert_eq!(
            policy_not_idempotent.decide_should_retry(make_request_info(&error, false)),
            RetryDecision::RetryNextTarget(None)
        );
        assert_eq!(
            policy_not_idempotent.decide_should_retry(make_request_info(&error, false)),
            RetryDecision::DontRetry
        );

        let mut policy_idempotent = DefaultRetryPolicy::new().new_session();
        assert_eq!(
            policy_idempotent.decide_should_retry(make_request_info(&error, true)),
            RetryDecision::RetryNextTarget(None)
        );
        assert_eq!(
            policy_idempotent.decide_should_retry(make_request_info(&error, true)),
            RetryDecision::DontRetry
        );
    }

    // On ReadTimeout we retry one time if there were enough responses and the data was present no matter the idempotence
    #[test]
    fn default_read_timeout() {
        setup_tracing();
        // Enough responses and data_present == false - coordinator received only checksums
        let enough_responses_no_data = RequestAttemptError::DbError(
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
            policy.decide_should_retry(make_request_info(&enough_responses_no_data, false)),
            RetryDecision::RetrySameTarget(None)
        );
        assert_eq!(
            policy.decide_should_retry(make_request_info(&enough_responses_no_data, false)),
            RetryDecision::DontRetry
        );

        // Idempotent
        let mut policy = DefaultRetryPolicy::new().new_session();
        assert_eq!(
            policy.decide_should_retry(make_request_info(&enough_responses_no_data, true)),
            RetryDecision::RetrySameTarget(None)
        );
        assert_eq!(
            policy.decide_should_retry(make_request_info(&enough_responses_no_data, true)),
            RetryDecision::DontRetry
        );

        // Enough responses but data_present == true - coordinator probably timed out
        // waiting for read-repair acknowledgement.
        let enough_responses_with_data = RequestAttemptError::DbError(
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
            policy.decide_should_retry(make_request_info(&enough_responses_with_data, false)),
            RetryDecision::DontRetry
        );

        // Idempotent
        let mut policy = DefaultRetryPolicy::new().new_session();
        assert_eq!(
            policy.decide_should_retry(make_request_info(&enough_responses_with_data, true)),
            RetryDecision::DontRetry
        );

        // Not enough responses, data_present == true
        let not_enough_responses_with_data = RequestAttemptError::DbError(
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
            policy.decide_should_retry(make_request_info(&not_enough_responses_with_data, false)),
            RetryDecision::DontRetry
        );

        // Idempotent
        let mut policy = DefaultRetryPolicy::new().new_session();
        assert_eq!(
            policy.decide_should_retry(make_request_info(&not_enough_responses_with_data, true)),
            RetryDecision::DontRetry
        );
    }

    // WriteTimeout will retry once when the request is idempotent and write_type == BatchLog
    #[test]
    fn default_write_timeout() {
        setup_tracing();
        // WriteType == BatchLog
        let good_write_type = RequestAttemptError::DbError(
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
            policy.decide_should_retry(make_request_info(&good_write_type, false)),
            RetryDecision::DontRetry
        );

        // Idempotent
        let mut policy = DefaultRetryPolicy::new().new_session();
        assert_eq!(
            policy.decide_should_retry(make_request_info(&good_write_type, true)),
            RetryDecision::RetrySameTarget(None)
        );
        assert_eq!(
            policy.decide_should_retry(make_request_info(&good_write_type, true)),
            RetryDecision::DontRetry
        );

        // WriteType != BatchLog
        let bad_write_type = RequestAttemptError::DbError(
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
            policy.decide_should_retry(make_request_info(&bad_write_type, false)),
            RetryDecision::DontRetry
        );

        // Idempotent
        let mut policy = DefaultRetryPolicy::new().new_session();
        assert_eq!(
            policy.decide_should_retry(make_request_info(&bad_write_type, true)),
            RetryDecision::DontRetry
        );
    }
}
