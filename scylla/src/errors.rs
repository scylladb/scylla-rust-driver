use std::sync::Arc;

use scylla_cql::{
    errors::{
        BadKeyspaceName, BadQuery, BrokenConnectionError, ConnectionPoolError, CqlResponseKind,
        DbError, RequestError,
    },
    frame::{
        frame_errors::{
            CqlErrorParseError, CqlResponseParseError, CqlResultParseError, FrameError, ParseError,
        },
        value::SerializeValuesError,
    },
    types::{
        deserialize::{DeserializationError, TypeCheckError},
        serialize::SerializationError,
    },
};

use thiserror::Error;

use crate::frame::response;

/// Error that occurred during query execution
#[derive(Error, Debug, Clone)]
#[non_exhaustive]
pub enum QueryError {
    /// Database sent a response containing some error with a message
    #[error("Database returned an error: {0}, Error message: {1}")]
    DbError(DbError, String),

    /// Caller passed an invalid query
    #[error(transparent)]
    BadQuery(#[from] BadQuery),

    /// Received a RESULT server response, but failed to deserialize it.
    #[error(transparent)]
    CqlResultParseError(#[from] CqlResultParseError),

    /// Received an ERROR server response, but failed to deserialize it.
    #[error("Failed to deserialize ERROR response: {0}")]
    CqlErrorParseError(#[from] CqlErrorParseError),

    /// Input/Output error has occurred, connection broken etc.
    #[error("IO Error: {0}")]
    IoError(Arc<std::io::Error>),

    /// Selected node's connection pool is in invalid state.
    #[error("No connections in the pool: {0}")]
    ConnectionPoolError(#[from] ConnectionPoolError),

    /// Unexpected message received
    #[error("Protocol Error: {0}")]
    ProtocolError(&'static str),

    /// Invalid message received
    #[error("Invalid message: {0}")]
    InvalidMessage(String),

    /// Timeout error has occurred, function didn't complete in time.
    #[error("Timeout Error")]
    TimeoutError,

    #[error("Too many orphaned stream ids: {0}")]
    TooManyOrphanedStreamIds(u16),

    #[error(transparent)]
    BrokenConnection(#[from] BrokenConnectionError),

    #[error("Unable to allocate stream id")]
    UnableToAllocStreamId,

    /// Client timeout occurred before any response arrived
    #[error("Request timeout: {0}")]
    RequestTimeout(String),
}

impl From<SerializeValuesError> for QueryError {
    fn from(serialized_err: SerializeValuesError) -> QueryError {
        QueryError::BadQuery(BadQuery::SerializeValuesError(serialized_err))
    }
}

impl From<SerializationError> for QueryError {
    fn from(serialized_err: SerializationError) -> QueryError {
        QueryError::BadQuery(BadQuery::SerializationError(serialized_err))
    }
}

impl From<DeserializationError> for QueryError {
    fn from(value: DeserializationError) -> Self {
        Self::InvalidMessage(value.to_string())
    }
}

impl From<TypeCheckError> for QueryError {
    fn from(value: TypeCheckError) -> Self {
        Self::InvalidMessage(value.to_string())
    }
}

impl From<ParseError> for QueryError {
    fn from(parse_error: ParseError) -> QueryError {
        QueryError::InvalidMessage(format!("Error parsing message: {}", parse_error))
    }
}

impl From<FrameError> for QueryError {
    fn from(frame_error: FrameError) -> QueryError {
        QueryError::InvalidMessage(format!("Frame error: {}", frame_error))
    }
}

impl From<tokio::time::error::Elapsed> for QueryError {
    fn from(timer_error: tokio::time::error::Elapsed) -> QueryError {
        QueryError::RequestTimeout(format!("{}", timer_error))
    }
}

impl From<UserRequestError> for QueryError {
    fn from(value: UserRequestError) -> Self {
        match value {
            UserRequestError::DbError(err, msg) => QueryError::DbError(err, msg),
            UserRequestError::CqlResultParseError(e) => e.into(),
            UserRequestError::CqlErrorParseError(e) => e.into(),
            UserRequestError::BrokenConnectionError(e) => e.into(),
            UserRequestError::UnexpectedResponse(_) => {
                // FIXME: make it typed. It needs to wait for ProtocolError refactor.
                QueryError::ProtocolError("Received unexpected response from the server. Expected RESULT or ERROR response.")
            }
            UserRequestError::FrameError(e) => e.into(),
            UserRequestError::UnableToAllocStreamId => QueryError::UnableToAllocStreamId,
            UserRequestError::RepreparedIdChanged => QueryError::ProtocolError(
                "Prepared statement Id changed, md5 sum should stay the same",
            ),
        }
    }
}

impl From<QueryError> for NewSessionError {
    fn from(query_error: QueryError) -> NewSessionError {
        match query_error {
            QueryError::DbError(e, msg) => NewSessionError::DbError(e, msg),
            QueryError::BadQuery(e) => NewSessionError::BadQuery(e),
            QueryError::CqlResultParseError(e) => NewSessionError::CqlResultParseError(e),
            QueryError::CqlErrorParseError(e) => NewSessionError::CqlErrorParseError(e),
            QueryError::IoError(e) => NewSessionError::IoError(e),
            QueryError::ConnectionPoolError(e) => NewSessionError::ConnectionPoolError(e),
            QueryError::ProtocolError(m) => NewSessionError::ProtocolError(m),
            QueryError::InvalidMessage(m) => NewSessionError::InvalidMessage(m),
            QueryError::TimeoutError => NewSessionError::TimeoutError,
            QueryError::TooManyOrphanedStreamIds(ids) => {
                NewSessionError::TooManyOrphanedStreamIds(ids)
            }
            QueryError::BrokenConnection(e) => NewSessionError::BrokenConnection(e),
            QueryError::UnableToAllocStreamId => NewSessionError::UnableToAllocStreamId,
            QueryError::RequestTimeout(msg) => NewSessionError::RequestTimeout(msg),
        }
    }
}

impl From<BadKeyspaceName> for QueryError {
    fn from(keyspace_err: BadKeyspaceName) -> QueryError {
        QueryError::BadQuery(BadQuery::BadKeyspaceName(keyspace_err))
    }
}

impl From<response::Error> for QueryError {
    fn from(error: response::Error) -> QueryError {
        QueryError::DbError(error.error, error.reason)
    }
}

/// Error that occurred during session creation
#[derive(Error, Debug, Clone)]
#[non_exhaustive]
pub enum NewSessionError {
    /// Failed to resolve hostname passed in Session creation
    #[error("Couldn't resolve any hostname: {0:?}")]
    FailedToResolveAnyHostname(Vec<String>),

    /// List of known nodes passed to Session constructor is empty
    /// There needs to be at least one node to connect to
    #[error("Empty known nodes list")]
    EmptyKnownNodesList,

    /// Database sent a response containing some error with a message
    #[error("Database returned an error: {0}, Error message: {1}")]
    DbError(DbError, String),

    /// Caller passed an invalid query
    #[error(transparent)]
    BadQuery(#[from] BadQuery),

    /// Received a RESULT server response, but failed to deserialize it.
    #[error(transparent)]
    CqlResultParseError(#[from] CqlResultParseError),

    /// Received an ERROR server response, but failed to deserialize it.
    #[error("Failed to deserialize ERROR response: {0}")]
    CqlErrorParseError(#[from] CqlErrorParseError),

    /// Input/Output error has occurred, connection broken etc.
    #[error("IO Error: {0}")]
    IoError(Arc<std::io::Error>),

    /// Selected node's connection pool is in invalid state.
    #[error("No connections in the pool: {0}")]
    ConnectionPoolError(#[from] ConnectionPoolError),

    /// Unexpected message received
    #[error("Protocol Error: {0}")]
    ProtocolError(&'static str),

    /// Invalid message received
    #[error("Invalid message: {0}")]
    InvalidMessage(String),

    /// Timeout error has occurred, couldn't connect to node in time.
    #[error("Timeout Error")]
    TimeoutError,

    #[error("Too many orphaned stream ids: {0}")]
    TooManyOrphanedStreamIds(u16),

    #[error(transparent)]
    BrokenConnection(#[from] BrokenConnectionError),

    #[error("Unable to allocate stream id")]
    UnableToAllocStreamId,

    /// Client timeout occurred before a response arrived for some query
    /// during `Session` creation.
    #[error("Client timeout: {0}")]
    RequestTimeout(String),
}

/// An error type that occurred when executing one of:
/// - QUERY
/// - PREPARE
/// - EXECUTE
/// - BATCH
///
/// requests.
#[derive(Error, Debug)]
pub enum UserRequestError {
    #[error("Database returned an error: {0}, Error message: {1}")]
    DbError(DbError, String),
    #[error(transparent)]
    CqlResultParseError(#[from] CqlResultParseError),
    #[error("Failed to deserialize ERROR response: {0}")]
    CqlErrorParseError(#[from] CqlErrorParseError),
    #[error(
        "Received unexpected response from the server: {0}. Expected RESULT or ERROR response."
    )]
    UnexpectedResponse(CqlResponseKind),
    #[error(transparent)]
    BrokenConnectionError(#[from] BrokenConnectionError),
    #[error(transparent)]
    FrameError(#[from] FrameError),
    #[error("Unable to allocate stream id")]
    UnableToAllocStreamId,
    #[error("Prepared statement Id changed, md5 sum should stay the same")]
    RepreparedIdChanged,
}

impl From<response::error::Error> for UserRequestError {
    fn from(value: response::error::Error) -> Self {
        UserRequestError::DbError(value.error, value.reason)
    }
}

impl From<RequestError> for UserRequestError {
    fn from(value: RequestError) -> Self {
        match value {
            RequestError::FrameError(e) => e.into(),
            RequestError::CqlResponseParseError(e) => match e {
                // Only possible responses are RESULT and ERROR. If we failed parsing
                // other response, treat it as unexpected response.
                CqlResponseParseError::CqlErrorParseError(e) => e.into(),
                CqlResponseParseError::CqlResultParseError(e) => e.into(),
                _ => UserRequestError::UnexpectedResponse(e.to_response_kind()),
            },
            RequestError::BrokenConnection(e) => e.into(),
            RequestError::UnableToAllocStreamId => UserRequestError::UnableToAllocStreamId,
        }
    }
}

#[cfg(test)]
mod tests {
    use scylla_cql::{
        errors::{DbError, WriteType},
        Consistency,
    };

    use crate::errors::QueryError;

    #[test]
    fn write_type_from_str() {
        let test_cases: [(&str, WriteType); 9] = [
            ("SIMPLE", WriteType::Simple),
            ("BATCH", WriteType::Batch),
            ("UNLOGGED_BATCH", WriteType::UnloggedBatch),
            ("COUNTER", WriteType::Counter),
            ("BATCH_LOG", WriteType::BatchLog),
            ("CAS", WriteType::Cas),
            ("VIEW", WriteType::View),
            ("CDC", WriteType::Cdc),
            ("SOMEOTHER", WriteType::Other("SOMEOTHER".to_string())),
        ];

        for (write_type_str, expected_write_type) in &test_cases {
            let write_type = WriteType::from(*write_type_str);
            assert_eq!(write_type, *expected_write_type);
        }
    }

    // A test to check that displaying DbError and QueryError::DbError works as expected
    // - displays error description
    // - displays error parameters
    // - displays error message
    // - indented multiline strings don't cause whitespace gaps
    #[test]
    fn dberror_full_info() {
        // Test that DbError::Unavailable is displayed correctly
        let db_error = DbError::Unavailable {
            consistency: Consistency::Three,
            required: 3,
            alive: 2,
        };

        let db_error_displayed: String = format!("{}", db_error);

        let mut expected_dberr_msg =
            "Not enough nodes are alive to satisfy required consistency level ".to_string();
        expected_dberr_msg += "(consistency: Three, required: 3, alive: 2)";

        assert_eq!(db_error_displayed, expected_dberr_msg);

        // Test that QueryError::DbError::(DbError::Unavailable) is displayed correctly
        let query_error =
            QueryError::DbError(db_error, "a message about unavailable error".to_string());
        let query_error_displayed: String = format!("{}", query_error);

        let mut expected_querr_msg = "Database returned an error: ".to_string();
        expected_querr_msg += &expected_dberr_msg;
        expected_querr_msg += ", Error message: a message about unavailable error";

        assert_eq!(query_error_displayed, expected_querr_msg);
    }
}
