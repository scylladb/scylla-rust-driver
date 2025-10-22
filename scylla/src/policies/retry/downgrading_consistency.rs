use scylla_cql::Consistency;
use tracing::debug;

use super::{RequestInfo, RetryDecision, RetryPolicy, RetrySession};
use crate::errors::{DbError, RequestAttemptError, WriteType};

/// Downgrading consistency retry policy - retries with lower consistency level if it knows\
/// that the initial CL is unreachable. Also, it behaves as [DefaultRetryPolicy](crate::policies::retry::DefaultRetryPolicy)
/// when it believes that the initial CL is reachable.
/// Behaviour based on [DataStax Java Driver]\
///(<https://docs.datastax.com/en/drivers/java/3.11/com/datastax/driver/core/policies/DowngradingConsistencyRetryPolicy.html>)
#[derive(Debug)]
pub struct DowngradingConsistencyRetryPolicy;

impl DowngradingConsistencyRetryPolicy {
    /// Creates a new instance of [DowngradingConsistencyRetryPolicy].
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
}

/// Implementation of [RetrySession] for [DowngradingConsistencyRetryPolicy].
pub struct DowngradingConsistencyRetrySession {
    was_retry: bool,
}

impl DowngradingConsistencyRetrySession {
    /// Creates a new instance of [DowngradingConsistencyRetrySession].
    // TODO(2.0): unpub this.
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
    fn decide_should_retry(&mut self, request_info: RequestInfo) -> RetryDecision {
        let cl = match request_info.consistency {
            Consistency::Serial | Consistency::LocalSerial => {
                return match request_info.error {
                    RequestAttemptError::DbError(DbError::Unavailable { .. }, _) => {
                        // JAVA-764: if the requested consistency level is serial, it means that the operation failed at
                        // the paxos phase of a LWT.
                        // Retry on the next target, on the assumption that the initial coordinator could be network-isolated.
                        RetryDecision::RetryNextTarget(None)
                    }
                    _ => RetryDecision::DontRetry,
                };
            }
            cl => cl,
        };

        fn max_likely_to_work_cl(known_ok: i32, previous_cl: Consistency) -> RetryDecision {
            let decision = if known_ok >= 3 {
                RetryDecision::RetrySameTarget(Some(Consistency::Three))
            } else if known_ok == 2 {
                RetryDecision::RetrySameTarget(Some(Consistency::Two))
            } else if known_ok == 1 || previous_cl == Consistency::EachQuorum {
                // JAVA-1005: EACH_QUORUM does not report a global number of alive replicas
                // so even if we get 0 alive replicas, there might be
                // a node up in some other datacenter
                RetryDecision::RetrySameTarget(Some(Consistency::One))
            } else {
                RetryDecision::DontRetry
            };
            if let RetryDecision::RetrySameTarget(new_cl) = decision {
                debug!(
                    "Decided to lower required consistency from {} to {:?}.",
                    previous_cl, new_cl
                );
            }
            decision
        }

        // Do not remove this lint!
        // It's there for a reason - we don't want new variants
        // automatically fall under `_` pattern when they are introduced.
        #[deny(clippy::wildcard_enum_match_arm)]
        match request_info.error {
            // With connection broken, we don't know if request was executed.
            RequestAttemptError::BrokenConnectionError(_) => {
                if request_info.is_idempotent {
                    RetryDecision::RetryNextTarget(None)
                } else {
                    RetryDecision::DontRetry
                }
            }
            // DbErrors
            RequestAttemptError::DbError(db_error, _) => {
                // Do not remove this lint!
                // It's there for a reason - we don't want new variants
                // automatically fall under `_` pattern when they are introduced.
                #[deny(clippy::wildcard_enum_match_arm)]
                match db_error {
                    // Basic errors - there are some problems on this node
                    // Retry on a different one if possible
                    DbError::Overloaded | DbError::ServerError | DbError::TruncateError => {
                        if request_info.is_idempotent {
                            RetryDecision::RetryNextTarget(None)
                        } else {
                            RetryDecision::DontRetry
                        }
                    }
                    // Unavailable - the current node believes that not enough nodes
                    // are alive to satisfy specified consistency requirements.
                    DbError::Unavailable { alive, .. } => {
                        if !self.was_retry {
                            self.was_retry = true;
                            max_likely_to_work_cl(*alive, cl)
                        } else {
                            RetryDecision::DontRetry
                        }
                    }
                    // ReadTimeout - coordinator didn't receive enough replies in time.
                    DbError::ReadTimeout {
                        received,
                        required,
                        data_present,
                        ..
                    } => {
                        if self.was_retry {
                            RetryDecision::DontRetry
                        } else if received < required {
                            self.was_retry = true;
                            max_likely_to_work_cl(*received, cl)
                        } else if !*data_present {
                            self.was_retry = true;
                            RetryDecision::RetrySameTarget(None)
                        } else {
                            RetryDecision::DontRetry
                        }
                    }
                    // Write timeout - coordinator didn't receive enough replies in time.
                    DbError::WriteTimeout {
                        write_type,
                        received,
                        ..
                    } => {
                        if self.was_retry || !request_info.is_idempotent {
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
                                WriteType::BatchLog => RetryDecision::RetrySameTarget(None),

                                WriteType::Counter
                                | WriteType::Cas
                                | WriteType::View
                                | WriteType::Cdc
                                | WriteType::Simple
                                | WriteType::Batch
                                | WriteType::Other(_) => RetryDecision::DontRetry,
                            }
                        }
                    }
                    // The node is still bootstrapping it can't execute the request, we should try another one
                    DbError::IsBootstrapping => RetryDecision::RetryNextTarget(None),
                    // In all other cases propagate the error to the user
                    DbError::SyntaxError
                    | DbError::Invalid
                    | DbError::AlreadyExists { .. }
                    | DbError::FunctionFailure { .. }
                    | DbError::AuthenticationError
                    | DbError::Unauthorized
                    | DbError::ConfigError
                    | DbError::ReadFailure { .. }
                    | DbError::WriteFailure { .. }
                    | DbError::Unprepared { .. }
                    | DbError::ProtocolError
                    | DbError::RateLimitReached { .. }
                    | DbError::Other(_)
                    | _ => RetryDecision::DontRetry,
                }
            }
            // Connection to the contacted node is overloaded, try another one
            RequestAttemptError::UnableToAllocStreamId => RetryDecision::RetryNextTarget(None),
            // In all other cases propagate the error to the user
            RequestAttemptError::BodyExtensionsParseError(_)
            | RequestAttemptError::CqlErrorParseError(_)
            | RequestAttemptError::CqlRequestSerialization(_)
            | RequestAttemptError::CqlResultParseError(_)
            | RequestAttemptError::NonfinishedPagingState
            | RequestAttemptError::RepreparedIdChanged { .. }
            | RequestAttemptError::RepreparedIdMissingInBatch
            | RequestAttemptError::SerializationError(_)
            | RequestAttemptError::UnexpectedResponse(_) => RetryDecision::DontRetry,
        }
    }

    fn reset(&mut self) {
        *self = DowngradingConsistencyRetrySession::new();
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use scylla_cql::frame::frame_errors::{BatchSerializationError, CqlRequestSerializationError};

    use crate::errors::{BrokenConnectionErrorKind, RequestAttemptError};
    use crate::test_utils::setup_tracing;

    use super::*;

    const CONSISTENCY_LEVELS: &[Consistency] = &[
        Consistency::All,
        Consistency::Any,
        Consistency::EachQuorum,
        Consistency::LocalOne,
        Consistency::LocalQuorum,
        Consistency::One,
        Consistency::Quorum,
        Consistency::Three,
        Consistency::Two,
    ];

    fn make_request_info_with_cl(
        error: &RequestAttemptError,
        is_idempotent: bool,
        cl: Consistency,
    ) -> RequestInfo<'_> {
        RequestInfo {
            error,
            is_idempotent,
            consistency: cl,
        }
    }

    // Asserts that downgrading consistency policy never retries for this Error
    fn downgrading_consistency_policy_assert_never_retries(
        error: RequestAttemptError,
        cl: Consistency,
    ) {
        let mut policy = DowngradingConsistencyRetryPolicy::new().new_session();
        assert_eq!(
            policy.decide_should_retry(make_request_info_with_cl(&error, false, cl)),
            RetryDecision::DontRetry
        );

        let mut policy = DowngradingConsistencyRetryPolicy::new().new_session();
        assert_eq!(
            policy.decide_should_retry(make_request_info_with_cl(&error, true, cl)),
            RetryDecision::DontRetry
        );
    }

    #[test]
    fn downgrading_consistency_never_retries() {
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
                received: 1,
                required: 2,
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

        for &cl in CONSISTENCY_LEVELS {
            for dberror in never_retried_dberrors.clone() {
                downgrading_consistency_policy_assert_never_retries(
                    RequestAttemptError::DbError(dberror, String::new()),
                    cl,
                );
            }

            downgrading_consistency_policy_assert_never_retries(
                RequestAttemptError::RepreparedIdMissingInBatch,
                cl,
            );
            downgrading_consistency_policy_assert_never_retries(
                RequestAttemptError::RepreparedIdChanged {
                    statement: String::new(),
                    expected_id: vec![],
                    reprepared_id: vec![],
                },
                cl,
            );
            downgrading_consistency_policy_assert_never_retries(
                RequestAttemptError::CqlRequestSerialization(
                    CqlRequestSerializationError::BatchSerialization(
                        BatchSerializationError::TooManyStatements(u16::MAX as usize + 1),
                    ),
                ),
                cl,
            );
        }
    }

    // Asserts that for this error policy retries on next on idempotent queries only
    fn downgrading_consistency_policy_assert_idempotent_next(
        error: RequestAttemptError,
        cl: Consistency,
    ) {
        let mut policy = DowngradingConsistencyRetryPolicy::new().new_session();
        assert_eq!(
            policy.decide_should_retry(make_request_info_with_cl(&error, false, cl)),
            RetryDecision::DontRetry
        );

        let mut policy = DowngradingConsistencyRetryPolicy::new().new_session();
        assert_eq!(
            policy.decide_should_retry(make_request_info_with_cl(&error, true, cl)),
            RetryDecision::RetryNextTarget(None)
        );
    }

    fn max_likely_to_work_cl(known_ok: i32, current_cl: Consistency) -> RetryDecision {
        if known_ok >= 3 {
            RetryDecision::RetrySameTarget(Some(Consistency::Three))
        } else if known_ok == 2 {
            RetryDecision::RetrySameTarget(Some(Consistency::Two))
        } else if known_ok == 1 || current_cl == Consistency::EachQuorum {
            // JAVA-1005: EACH_QUORUM does not report a global number of alive replicas
            // so even if we get 0 alive replicas, there might be
            // a node up in some other datacenter
            RetryDecision::RetrySameTarget(Some(Consistency::One))
        } else {
            RetryDecision::DontRetry
        }
    }

    #[test]
    fn downgrading_consistency_idempotent_next_retries() {
        setup_tracing();
        let idempotent_next_errors = vec![
            RequestAttemptError::DbError(DbError::Overloaded, String::new()),
            RequestAttemptError::DbError(DbError::TruncateError, String::new()),
            RequestAttemptError::DbError(DbError::ServerError, String::new()),
            RequestAttemptError::BrokenConnectionError(
                BrokenConnectionErrorKind::TooManyOrphanedStreamIds(5).into(),
            ),
        ];

        for &cl in CONSISTENCY_LEVELS {
            for error in idempotent_next_errors.clone() {
                downgrading_consistency_policy_assert_idempotent_next(error, cl);
            }
        }
    }

    // Always retry on next node if current one is bootstrapping
    #[test]
    fn downgrading_consistency_bootstrapping() {
        setup_tracing();
        let error = RequestAttemptError::DbError(DbError::IsBootstrapping, String::new());

        for &cl in CONSISTENCY_LEVELS {
            let mut policy = DowngradingConsistencyRetryPolicy::new().new_session();
            assert_eq!(
                policy.decide_should_retry(make_request_info_with_cl(&error, false, cl)),
                RetryDecision::RetryNextTarget(None)
            );

            let mut policy = DowngradingConsistencyRetryPolicy::new().new_session();
            assert_eq!(
                policy.decide_should_retry(make_request_info_with_cl(&error, true, cl)),
                RetryDecision::RetryNextTarget(None)
            );
        }
    }

    // On Unavailable error we retry one time no matter the idempotence
    #[test]
    fn downgrading_consistency_unavailable() {
        setup_tracing();
        let alive = 1;
        let error = RequestAttemptError::DbError(
            DbError::Unavailable {
                consistency: Consistency::Two,
                required: 2,
                alive,
            },
            String::new(),
        );

        for &cl in CONSISTENCY_LEVELS {
            let mut policy_not_idempotent = DowngradingConsistencyRetryPolicy::new().new_session();
            assert_eq!(
                policy_not_idempotent
                    .decide_should_retry(make_request_info_with_cl(&error, false, cl)),
                max_likely_to_work_cl(alive, cl)
            );
            assert_eq!(
                policy_not_idempotent
                    .decide_should_retry(make_request_info_with_cl(&error, false, cl)),
                RetryDecision::DontRetry
            );

            let mut policy_idempotent = DowngradingConsistencyRetryPolicy::new().new_session();
            assert_eq!(
                policy_idempotent.decide_should_retry(make_request_info_with_cl(&error, true, cl)),
                max_likely_to_work_cl(alive, cl)
            );
            assert_eq!(
                policy_idempotent.decide_should_retry(make_request_info_with_cl(&error, true, cl)),
                RetryDecision::DontRetry
            );
        }
    }

    // On ReadTimeout we retry one time if there were enough responses and the data was present no matter the idempotence
    #[test]
    fn downgrading_consistency_read_timeout() {
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

        for &cl in CONSISTENCY_LEVELS {
            // Not idempotent
            let mut policy = DowngradingConsistencyRetryPolicy::new().new_session();
            assert_eq!(
                policy.decide_should_retry(make_request_info_with_cl(
                    &enough_responses_no_data,
                    false,
                    cl
                )),
                RetryDecision::RetrySameTarget(None)
            );
            assert_eq!(
                policy.decide_should_retry(make_request_info_with_cl(
                    &enough_responses_no_data,
                    false,
                    cl
                )),
                RetryDecision::DontRetry
            );

            // Idempotent
            let mut policy = DowngradingConsistencyRetryPolicy::new().new_session();
            assert_eq!(
                policy.decide_should_retry(make_request_info_with_cl(
                    &enough_responses_no_data,
                    true,
                    cl
                )),
                RetryDecision::RetrySameTarget(None)
            );
            assert_eq!(
                policy.decide_should_retry(make_request_info_with_cl(
                    &enough_responses_no_data,
                    true,
                    cl
                )),
                RetryDecision::DontRetry
            );
        }
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

        for &cl in CONSISTENCY_LEVELS {
            // Not idempotent
            let mut policy = DowngradingConsistencyRetryPolicy::new().new_session();
            assert_eq!(
                policy.decide_should_retry(make_request_info_with_cl(
                    &enough_responses_with_data,
                    false,
                    cl
                )),
                RetryDecision::DontRetry
            );

            // Idempotent
            let mut policy = DowngradingConsistencyRetryPolicy::new().new_session();
            assert_eq!(
                policy.decide_should_retry(make_request_info_with_cl(
                    &enough_responses_with_data,
                    true,
                    cl
                )),
                RetryDecision::DontRetry
            );
        }

        // Not enough responses, data_present == true
        let received = 1;
        let not_enough_responses_with_data = RequestAttemptError::DbError(
            DbError::ReadTimeout {
                consistency: Consistency::Two,
                received,
                required: 2,
                data_present: true,
            },
            String::new(),
        );
        for &cl in CONSISTENCY_LEVELS {
            let expected_decision = max_likely_to_work_cl(received, cl);

            // Not idempotent
            let mut policy = DowngradingConsistencyRetryPolicy::new().new_session();
            assert_eq!(
                policy.decide_should_retry(make_request_info_with_cl(
                    &not_enough_responses_with_data,
                    false,
                    cl
                )),
                expected_decision
            );
            if let RetryDecision::RetrySameTarget(new_cl) = expected_decision {
                assert_eq!(
                    policy.decide_should_retry(make_request_info_with_cl(
                        &not_enough_responses_with_data,
                        false,
                        new_cl.unwrap_or(cl)
                    )),
                    RetryDecision::DontRetry
                );
            }

            // Idempotent
            let mut policy = DowngradingConsistencyRetryPolicy::new().new_session();
            assert_eq!(
                policy.decide_should_retry(make_request_info_with_cl(
                    &not_enough_responses_with_data,
                    true,
                    cl
                )),
                expected_decision
            );
            if let RetryDecision::RetrySameTarget(new_cl) = expected_decision {
                assert_eq!(
                    policy.decide_should_retry(make_request_info_with_cl(
                        &not_enough_responses_with_data,
                        true,
                        new_cl.unwrap_or(cl)
                    )),
                    RetryDecision::DontRetry
                );
            }
        }
    }

    // WriteTimeout will retry once when the request is idempotent and write_type == BatchLog
    #[test]
    fn downgrading_consistency_write_timeout() {
        setup_tracing();
        for (received, required) in (1..=5).zip(2..=6) {
            // WriteType == BatchLog
            let write_type_batchlog = RequestAttemptError::DbError(
                DbError::WriteTimeout {
                    consistency: Consistency::Two,
                    received,
                    required,
                    write_type: WriteType::BatchLog,
                },
                String::new(),
            );

            for &cl in CONSISTENCY_LEVELS {
                // Not idempotent
                let mut policy = DowngradingConsistencyRetryPolicy::new().new_session();
                assert_eq!(
                    policy.decide_should_retry(make_request_info_with_cl(
                        &write_type_batchlog,
                        false,
                        cl
                    )),
                    RetryDecision::DontRetry
                );

                // Idempotent
                let mut policy = DowngradingConsistencyRetryPolicy::new().new_session();
                assert_eq!(
                    policy.decide_should_retry(make_request_info_with_cl(
                        &write_type_batchlog,
                        true,
                        cl
                    )),
                    RetryDecision::RetrySameTarget(None)
                );
                assert_eq!(
                    policy.decide_should_retry(make_request_info_with_cl(
                        &write_type_batchlog,
                        true,
                        cl
                    )),
                    RetryDecision::DontRetry
                );
            }

            // WriteType == UnloggedBatch
            let write_type_unlogged_batch = RequestAttemptError::DbError(
                DbError::WriteTimeout {
                    consistency: Consistency::Two,
                    received,
                    required,
                    write_type: WriteType::UnloggedBatch,
                },
                String::new(),
            );

            for &cl in CONSISTENCY_LEVELS {
                // Not idempotent
                let mut policy = DowngradingConsistencyRetryPolicy::new().new_session();
                assert_eq!(
                    policy.decide_should_retry(make_request_info_with_cl(
                        &write_type_unlogged_batch,
                        false,
                        cl
                    )),
                    RetryDecision::DontRetry
                );

                // Idempotent
                let mut policy = DowngradingConsistencyRetryPolicy::new().new_session();
                assert_eq!(
                    policy.decide_should_retry(make_request_info_with_cl(
                        &write_type_unlogged_batch,
                        true,
                        cl
                    )),
                    max_likely_to_work_cl(received, cl)
                );
                assert_eq!(
                    policy.decide_should_retry(make_request_info_with_cl(
                        &write_type_unlogged_batch,
                        true,
                        cl
                    )),
                    RetryDecision::DontRetry
                );
            }

            // WriteType == other
            let write_type_other = RequestAttemptError::DbError(
                DbError::WriteTimeout {
                    consistency: Consistency::Two,
                    received,
                    required,
                    write_type: WriteType::Simple,
                },
                String::new(),
            );

            for &cl in CONSISTENCY_LEVELS {
                // Not idempotent
                let mut policy = DowngradingConsistencyRetryPolicy::new().new_session();
                assert_eq!(
                    policy.decide_should_retry(make_request_info_with_cl(
                        &write_type_other,
                        false,
                        cl
                    )),
                    RetryDecision::DontRetry
                );

                // Idempotent
                let mut policy = DowngradingConsistencyRetryPolicy::new().new_session();
                assert_eq!(
                    policy.decide_should_retry(make_request_info_with_cl(
                        &write_type_other,
                        true,
                        cl
                    )),
                    RetryDecision::IgnoreWriteError
                );
                assert_eq!(
                    policy.decide_should_retry(make_request_info_with_cl(
                        &write_type_other,
                        true,
                        cl
                    )),
                    RetryDecision::DontRetry
                );
            }
        }
    }
}
