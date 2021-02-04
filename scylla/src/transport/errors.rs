use crate::frame::frame_errors::{FrameError, ParseError};
use crate::frame::value::SerializeValuesError;
use std::sync::Arc;
use thiserror::Error;

/// Error that occured during query execution
#[derive(Error, Debug, Clone)]
pub enum QueryError {
    /// Database sent a response containing some error
    #[error(transparent)]
    DBError(#[from] DBError),

    /// Caller passed an invalid query
    #[error(transparent)]
    BadQuery(#[from] BadQuery),

    /// Input/Output error has occured, connection broken etc.
    #[error("IO Error: {0}")]
    IOError(Arc<std::io::Error>),

    /// Unexpected or invalid message received
    #[error("Protocol Error: {0}")]
    ProtocolError(&'static str),
}

/// An error sent from database in response to a query
#[derive(Error, Debug, Clone)]
#[error("Database response contains an error")]
pub enum DBError {
    // TODO develop this while implementing retry policies
    // to react differently to each error.
    // Make it fit what we encounter.
    #[error("Response is an error message: code: {0} message: {1}")]
    ErrorMsg(i32, String),
}

/// Error caused by caller creating an invalid query
#[derive(Error, Debug, Clone)]
#[error("Invalid query passed to Session")]
pub enum BadQuery {
    /// Failed to serialize values passed to a query - values too big
    #[error("Serializing values failed: {0} ")]
    SerializeValuesError(#[from] SerializeValuesError),

    /// Number of values provided doesn't match number of statements in a batch
    #[error("Length of provided values ({0}) must be equal to number of batch statements ({1})")]
    ValueLenMismatch(usize, usize),

    /// Serialized values are too long to compute parition key
    #[error("Serialized values are too long to compute parition key! Length: {0}, Max allowed length: {1}")]
    ValuesTooLongForKey(usize, usize),
}

/// Error that occured during session creation
#[derive(Error, Debug, Clone)]
pub enum NewSessionError {
    /// Failed to resolve hostname passed in Session creation
    #[error("Couldn't resolve address: {0}")]
    FailedToResolveAddress(String),

    /// List of known nodes passed to Session constructor is empty
    /// There needs to be at least one node to connect to
    #[error("Empty known nodes list")]
    EmptyKnownNodesList,

    /// Database sent a response containing some error
    #[error(transparent)]
    DBError(#[from] DBError),

    /// Caller passed an invalid query
    #[error(transparent)]
    BadQuery(#[from] BadQuery),

    /// Input/Output error has occured, connection broken etc.
    #[error("IO Error: {0}")]
    IOError(Arc<std::io::Error>),

    /// Unexpected or invalid message received
    #[error("Protocol Error: {0}")]
    ProtocolError(&'static str),
}

impl From<std::io::Error> for QueryError {
    fn from(io_error: std::io::Error) -> QueryError {
        QueryError::IOError(Arc::new(io_error))
    }
}

impl From<SerializeValuesError> for QueryError {
    fn from(serialized_err: SerializeValuesError) -> QueryError {
        QueryError::BadQuery(BadQuery::SerializeValuesError(serialized_err))
    }
}

impl From<ParseError> for QueryError {
    fn from(_parse_error: ParseError) -> QueryError {
        QueryError::ProtocolError("Error parsing message")
    }
}

impl From<FrameError> for QueryError {
    fn from(_frame_error: FrameError) -> QueryError {
        QueryError::ProtocolError("Error parsing message frame")
    }
}

impl From<std::io::Error> for NewSessionError {
    fn from(io_error: std::io::Error) -> NewSessionError {
        NewSessionError::IOError(Arc::new(io_error))
    }
}

impl From<QueryError> for NewSessionError {
    fn from(query_error: QueryError) -> NewSessionError {
        match query_error {
            QueryError::DBError(e) => NewSessionError::DBError(e),
            QueryError::BadQuery(e) => NewSessionError::BadQuery(e),
            QueryError::IOError(e) => NewSessionError::IOError(e),
            QueryError::ProtocolError(m) => NewSessionError::ProtocolError(m),
        }
    }
}
