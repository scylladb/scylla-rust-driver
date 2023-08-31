//! This module contains various errors which can be returned by `scylla::Session`

use std::io::ErrorKind;
use std::sync::Arc;

pub use scylla_cql::errors::{DbError, OperationType, WriteType};

use scylla_cql::frame::frame_errors::{FrameError, ParseError};
use scylla_cql::frame::response::Error as CqlError;
use scylla_cql::frame::value::SerializeValuesError;
use thiserror::Error;

/// Error that occurred during query execution
#[derive(Error, Debug, Clone)]
pub enum QueryError {
    /// Database sent a response containing some error with a message
    #[error("Database returned an error: {0}, Error message: {1}")]
    DbError(DbError, String),

    /// Caller passed an invalid query
    #[error(transparent)]
    BadQuery(#[from] BadQuery),

    /// Input/Output error has occurred, connection broken etc.
    #[error("IO Error: {0}")]
    IoError(Arc<std::io::Error>),

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

    #[error("Unable to allocate stream id")]
    UnableToAllocStreamId,

    /// Client timeout occurred before any response arrived
    #[error("Request timeout: {0}")]
    RequestTimeout(String),

    /// Address translation failed
    #[error("Address translation failed: {0}")]
    TranslationError(#[from] TranslationError),
}

/// Error that occurred during session creation
#[derive(Error, Debug, Clone)]
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

    /// Input/Output error has occurred, connection broken etc.
    #[error("IO Error: {0}")]
    IoError(Arc<std::io::Error>),

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

    #[error("Unable to allocate stream id")]
    UnableToAllocStreamId,

    /// Client timeout occurred before a response arrived for some query
    /// during `Session` creation.
    #[error("Client timeout: {0}")]
    RequestTimeout(String),

    /// Address translation failed
    #[error("Address translation failed: {0}")]
    TranslationError(#[from] TranslationError),
}

/// Error caused by failed address translation done before establishing connection
#[derive(Debug, Copy, Clone, Error)]
pub enum TranslationError {
    #[error("No rule for address")]
    NoRuleForAddress,
    #[error("Invalid address in rule")]
    InvalidAddressInRule,
}

/// Error caused by caller creating an invalid query
#[derive(Error, Debug, Clone)]
#[error("Invalid query passed to Session")]
pub enum BadQuery {
    /// Failed to serialize values passed to a query - values too big
    #[error("Serializing values failed: {0} ")]
    SerializeValuesError(#[from] SerializeValuesError),

    /// Serialized values are too long to compute partition key
    #[error("Serialized values are too long to compute partition key! Length: {0}, Max allowed length: {1}")]
    ValuesTooLongForKey(usize, usize),

    /// Passed invalid keyspace name to use
    #[error("Passed invalid keyspace name to use: {0}")]
    BadKeyspaceName(#[from] BadKeyspaceName),

    /// Other reasons of bad query
    #[error("{0}")]
    Other(String),
}

/// Invalid keyspace name given to `Session::use_keyspace()`
#[derive(Debug, Error, Clone)]
pub enum BadKeyspaceName {
    /// Keyspace name is empty
    #[error("Keyspace name is empty")]
    Empty,

    /// Keyspace name too long, must be up to 48 characters
    #[error("Keyspace name too long, must be up to 48 characters, found {1} characters. Bad keyspace name: '{0}'")]
    TooLong(String, usize),

    /// Illegal character - only alphanumeric and underscores allowed.
    #[error("Illegal character found: '{1}', only alphanumeric and underscores allowed. Bad keyspace name: '{0}'")]
    IllegalCharacter(String, char),
}

impl From<std::io::Error> for QueryError {
    fn from(io_error: std::io::Error) -> QueryError {
        QueryError::IoError(Arc::new(io_error))
    }
}

impl From<SerializeValuesError> for QueryError {
    fn from(serialized_err: SerializeValuesError) -> QueryError {
        QueryError::BadQuery(BadQuery::SerializeValuesError(serialized_err))
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

impl From<std::io::Error> for NewSessionError {
    fn from(io_error: std::io::Error) -> NewSessionError {
        NewSessionError::IoError(Arc::new(io_error))
    }
}

impl From<QueryError> for NewSessionError {
    fn from(query_error: QueryError) -> NewSessionError {
        match query_error {
            QueryError::DbError(e, msg) => NewSessionError::DbError(e, msg),
            QueryError::BadQuery(e) => NewSessionError::BadQuery(e),
            QueryError::IoError(e) => NewSessionError::IoError(e),
            QueryError::ProtocolError(m) => NewSessionError::ProtocolError(m),
            QueryError::InvalidMessage(m) => NewSessionError::InvalidMessage(m),
            QueryError::TimeoutError => NewSessionError::TimeoutError,
            QueryError::TooManyOrphanedStreamIds(ids) => {
                NewSessionError::TooManyOrphanedStreamIds(ids)
            }
            QueryError::UnableToAllocStreamId => NewSessionError::UnableToAllocStreamId,
            QueryError::RequestTimeout(msg) => NewSessionError::RequestTimeout(msg),
            QueryError::TranslationError(e) => NewSessionError::TranslationError(e),
        }
    }
}

impl From<BadKeyspaceName> for QueryError {
    fn from(keyspace_err: BadKeyspaceName) -> QueryError {
        QueryError::BadQuery(BadQuery::BadKeyspaceName(keyspace_err))
    }
}

impl QueryError {
    /// Checks if this error indicates that a chosen source port/address cannot be bound.
    /// This is caused by one of the following:
    /// - The source address is already used by another socket,
    /// - The source address is reserved and the process does not have sufficient privileges to use it.
    pub fn is_address_unavailable_for_use(&self) -> bool {
        if let QueryError::IoError(io_error) = self {
            match io_error.kind() {
                ErrorKind::AddrInUse | ErrorKind::PermissionDenied => return true,
                _ => {}
            }
        }

        false
    }
}

impl From<CqlError> for QueryError {
    fn from(error: CqlError) -> QueryError {
        QueryError::DbError(error.error, error.reason)
    }
}

#[cfg(test)]
mod tests {
    use super::DbError;
    use crate::statement::Consistency;

    use super::QueryError;

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
