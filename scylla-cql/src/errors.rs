//! This module contains various errors which can be returned by `scylla::Session`

use crate::frame::frame_errors::{
    CqlEventParseError, CqlResponseParseError, FrameError, ParseError,
};
use crate::frame::protocol_features::ProtocolFeatures;
use crate::frame::value::SerializeValuesError;
use crate::types::deserialize::{DeserializationError, TypeCheckError};
use crate::types::serialize::SerializationError;
use crate::Consistency;
use bytes::Bytes;
use std::error::Error;
use std::io::ErrorKind;
use std::net::IpAddr;
use std::sync::Arc;
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

    /// Failed to deserialize a CQL response from the server.
    #[error(transparent)]
    CqlResponseParseError(#[from] CqlResponseParseError),

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

    #[error(transparent)]
    BrokenConnection(#[from] BrokenConnectionError),

    #[error("Unable to allocate stream id")]
    UnableToAllocStreamId,

    /// Client timeout occurred before any response arrived
    #[error("Request timeout: {0}")]
    RequestTimeout(String),
}

/// An error sent from the database in response to a query
/// as described in the [specification](https://github.com/apache/cassandra/blob/5ed5e84613ef0e9664a774493db7d2604e3596e0/doc/native_protocol_v4.spec#L1029)\
#[derive(Error, Debug, Clone, PartialEq, Eq)]
pub enum DbError {
    /// The submitted query has a syntax error
    #[error("The submitted query has a syntax error")]
    SyntaxError,

    /// The query is syntactically correct but invalid
    #[error("The query is syntactically correct but invalid")]
    Invalid,

    /// Attempted to create a keyspace or a table that was already existing
    #[error(
        "Attempted to create a keyspace or a table that was already existing \
        (keyspace: {keyspace}, table: {table})"
    )]
    AlreadyExists {
        /// Created keyspace name or name of the keyspace in which table was created
        keyspace: String,
        /// Name of the table created, in case of keyspace creation it's an empty string
        table: String,
    },

    /// User defined function failed during execution
    #[error(
        "User defined function failed during execution \
        (keyspace: {keyspace}, function: {function}, arg_types: {arg_types:?})"
    )]
    FunctionFailure {
        /// Keyspace of the failed function
        keyspace: String,
        /// Name of the failed function
        function: String,
        /// Types of arguments passed to the function
        arg_types: Vec<String>,
    },

    /// Authentication failed - bad credentials
    #[error("Authentication failed - bad credentials")]
    AuthenticationError,

    /// The logged user doesn't have the right to perform the query
    #[error("The logged user doesn't have the right to perform the query")]
    Unauthorized,

    /// The query is invalid because of some configuration issue
    #[error("The query is invalid because of some configuration issue")]
    ConfigError,

    /// Not enough nodes are alive to satisfy required consistency level
    #[error(
        "Not enough nodes are alive to satisfy required consistency level \
        (consistency: {consistency}, required: {required}, alive: {alive})"
    )]
    Unavailable {
        /// Consistency level of the query
        consistency: Consistency,
        /// Number of nodes required to be alive to satisfy required consistency level
        required: i32,
        /// Found number of active nodes
        alive: i32,
    },

    /// The request cannot be processed because the coordinator node is overloaded
    #[error("The request cannot be processed because the coordinator node is overloaded")]
    Overloaded,

    /// The coordinator node is still bootstrapping
    #[error("The coordinator node is still bootstrapping")]
    IsBootstrapping,

    /// Error during truncate operation
    #[error("Error during truncate operation")]
    TruncateError,

    /// Not enough nodes responded to the read request in time to satisfy required consistency level
    #[error("Not enough nodes responded to the read request in time to satisfy required consistency level \
            (consistency: {consistency}, received: {received}, required: {required}, data_present: {data_present})")]
    ReadTimeout {
        /// Consistency level of the query
        consistency: Consistency,
        /// Number of nodes that responded to the read request
        received: i32,
        /// Number of nodes required to respond to satisfy required consistency level
        required: i32,
        /// Replica that was asked for data has responded
        data_present: bool,
    },

    /// Not enough nodes responded to the write request in time to satisfy required consistency level
    #[error("Not enough nodes responded to the write request in time to satisfy required consistency level \
            (consistency: {consistency}, received: {received}, required: {required}, write_type: {write_type})")]
    WriteTimeout {
        /// Consistency level of the query
        consistency: Consistency,
        /// Number of nodes that responded to the write request
        received: i32,
        /// Number of nodes required to respond to satisfy required consistency level
        required: i32,
        /// Type of write operation requested
        write_type: WriteType,
    },

    /// A non-timeout error during a read request
    #[error(
        "A non-timeout error during a read request \
        (consistency: {consistency}, received: {received}, required: {required}, \
        numfailures: {numfailures}, data_present: {data_present})"
    )]
    ReadFailure {
        /// Consistency level of the query
        consistency: Consistency,
        /// Number of nodes that responded to the read request
        received: i32,
        /// Number of nodes required to respond to satisfy required consistency level
        required: i32,
        /// Number of nodes that experience a failure while executing the request
        numfailures: i32,
        /// Replica that was asked for data has responded
        data_present: bool,
    },

    /// A non-timeout error during a write request
    #[error(
        "A non-timeout error during a write request \
        (consistency: {consistency}, received: {received}, required: {required}, \
        numfailures: {numfailures}, write_type: {write_type}"
    )]
    WriteFailure {
        /// Consistency level of the query
        consistency: Consistency,
        /// Number of nodes that responded to the read request
        received: i32,
        /// Number of nodes required to respond to satisfy required consistency level
        required: i32,
        /// Number of nodes that experience a failure while executing the request
        numfailures: i32,
        /// Type of write operation requested
        write_type: WriteType,
    },

    /// Tried to execute a prepared statement that is not prepared. Driver should prepare it again
    #[error(
        "Tried to execute a prepared statement that is not prepared. Driver should prepare it again"
    )]
    Unprepared {
        /// Statement id of the requested prepared query
        statement_id: Bytes,
    },

    /// Internal server error. This indicates a server-side bug
    #[error("Internal server error. This indicates a server-side bug")]
    ServerError,

    /// Invalid protocol message received from the driver
    #[error("Invalid protocol message received from the driver")]
    ProtocolError,

    /// Rate limit was exceeded for a partition affected by the request.
    /// (Scylla-specific)
    /// TODO: Should this have a "Scylla" prefix?
    #[error("Rate limit was exceeded for a partition affected by the request")]
    RateLimitReached {
        /// Type of the operation rejected by rate limiting.
        op_type: OperationType,
        /// Whether the operation was rate limited on the coordinator or not.
        /// Writes rejected on the coordinator are guaranteed not to be applied
        /// on any replica.
        rejected_by_coordinator: bool,
    },

    /// Other error code not specified in the specification
    #[error("Other error not specified in the specification. Error code: {0}")]
    Other(i32),
}

impl DbError {
    pub fn code(&self, protocol_features: &ProtocolFeatures) -> i32 {
        match self {
            DbError::ServerError => 0x0000,
            DbError::ProtocolError => 0x000A,
            DbError::AuthenticationError => 0x0100,
            DbError::Unavailable {
                consistency: _,
                required: _,
                alive: _,
            } => 0x1000,
            DbError::Overloaded => 0x1001,
            DbError::IsBootstrapping => 0x1002,
            DbError::TruncateError => 0x1003,
            DbError::WriteTimeout {
                consistency: _,
                received: _,
                required: _,
                write_type: _,
            } => 0x1100,
            DbError::ReadTimeout {
                consistency: _,
                received: _,
                required: _,
                data_present: _,
            } => 0x1200,
            DbError::ReadFailure {
                consistency: _,
                received: _,
                required: _,
                numfailures: _,
                data_present: _,
            } => 0x1300,
            DbError::FunctionFailure {
                keyspace: _,
                function: _,
                arg_types: _,
            } => 0x1400,
            DbError::WriteFailure {
                consistency: _,
                received: _,
                required: _,
                numfailures: _,
                write_type: _,
            } => 0x1500,
            DbError::SyntaxError => 0x2000,
            DbError::Unauthorized => 0x2100,
            DbError::Invalid => 0x2200,
            DbError::ConfigError => 0x2300,
            DbError::AlreadyExists {
                keyspace: _,
                table: _,
            } => 0x2400,
            DbError::Unprepared { statement_id: _ } => 0x2500,
            DbError::Other(code) => *code,
            DbError::RateLimitReached {
                op_type: _,
                rejected_by_coordinator: _,
            } => protocol_features.rate_limit_error.unwrap(),
        }
    }
}

/// Error caused by failed address translation done before establishing connection
#[derive(Debug, Copy, Clone, Error)]
pub enum TranslationError {
    #[error("No rule for address")]
    NoRuleForAddress,
    #[error("Invalid address in rule")]
    InvalidAddressInRule,
}

/// Type of the operation rejected by rate limiting
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OperationType {
    Read,
    Write,
    Other(u8),
}

/// Type of write operation requested
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WriteType {
    /// Non-batched non-counter write
    Simple,
    /// Logged batch write. If this type is received, it means the batch log has been successfully written
    /// (otherwise BatchLog type would be present)
    Batch,
    /// Unlogged batch. No batch log write has been attempted.
    UnloggedBatch,
    /// Counter write (batched or not)
    Counter,
    /// Timeout occurred during the write to the batch log when a logged batch was requested
    BatchLog,
    /// Timeout occurred during Compare And Set write/update
    Cas,
    /// Write involves VIEW update and failure to acquire local view(MV) lock for key within timeout
    View,
    /// Timeout occurred  when a cdc_total_space_in_mb is exceeded when doing a write to data tracked by cdc
    Cdc,
    /// Other type not specified in the specification
    Other(String),
}

/// Error caused by caller creating an invalid query
#[derive(Error, Debug, Clone)]
#[error("Invalid query passed to Session")]
pub enum BadQuery {
    /// Failed to serialize values passed to a query - values too big
    #[error("Serializing values failed: {0} ")]
    SerializeValuesError(#[from] SerializeValuesError),

    #[error("Serializing values failed: {0} ")]
    SerializationError(#[from] SerializationError),

    /// Serialized values are too long to compute partition key
    #[error("Serialized values are too long to compute partition key! Length: {0}, Max allowed length: {1}")]
    ValuesTooLongForKey(usize, usize),

    /// Passed invalid keyspace name to use
    #[error("Passed invalid keyspace name to use: {0}")]
    BadKeyspaceName(#[from] BadKeyspaceName),

    /// Too many queries in the batch statement
    #[error("Number of Queries in Batch Statement supplied is {0} which has exceeded the max value of 65,535")]
    TooManyQueriesInBatchStatement(usize),

    /// Other reasons of bad query
    #[error("{0}")]
    Other(String),
}

/// Possible CQL responses received from the server
#[derive(Debug, Copy, Clone)]
#[non_exhaustive]
pub enum CqlResponseKind {
    Error,
    Ready,
    Authenticate,
    Supported,
    Result,
    Event,
    AuthChallenge,
    AuthSuccess,
}

impl std::fmt::Display for CqlResponseKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let kind_str = match self {
            CqlResponseKind::Error => "ERROR",
            CqlResponseKind::Ready => "READY",
            CqlResponseKind::Authenticate => "AUTHENTICATE",
            CqlResponseKind::Supported => "SUPPORTED",
            CqlResponseKind::Result => "RESULT",
            CqlResponseKind::Event => "EVENT",
            CqlResponseKind::AuthChallenge => "AUTH_CHALLENGE",
            CqlResponseKind::AuthSuccess => "AUTH_SUCCESS",
        };

        f.write_str(kind_str)
    }
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

    /// Failed to deserialize a CQL response from the server.
    #[error(transparent)]
    CqlResponseParseError(#[from] CqlResponseParseError),

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

    #[error(transparent)]
    BrokenConnection(#[from] BrokenConnectionError),

    #[error("Unable to allocate stream id")]
    UnableToAllocStreamId,

    /// Client timeout occurred before a response arrived for some query
    /// during `Session` creation.
    #[error("Client timeout: {0}")]
    RequestTimeout(String),
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

// FIXME: this should be moved to scylla crate.
/// An error that appeared on a connection level.
/// It indicated that connection can no longer be used
/// and should be dropped.
#[derive(Error, Debug)]
#[non_exhaustive]
pub enum ConnectionError {
    #[error("Connect timeout elapsed")]
    ConnectTimeout,
    #[error(transparent)]
    IoError(#[from] std::io::Error),
    #[error("Could not find free source port for shard {0}")]
    NoSourcePortForShard(u32),
    #[error("Address translation failed: {0}")]
    TranslationError(#[from] TranslationError),
    #[error(transparent)]
    BrokenConnection(#[from] BrokenConnectionError),
    // TODO: remove it or change it later.
    #[error(transparent)]
    QueryError(#[from] QueryError),
}

impl ConnectionError {
    /// Checks if this error indicates that a chosen source port/address cannot be bound.
    /// This is caused by one of the following:
    /// - The source address is already used by another socket,
    /// - The source address is reserved and the process does not have sufficient privileges to use it.
    pub fn is_address_unavailable_for_use(&self) -> bool {
        if let ConnectionError::IoError(io_error) = self {
            match io_error.kind() {
                ErrorKind::AddrInUse | ErrorKind::PermissionDenied => return true,
                _ => {}
            }
        }

        false
    }
}

#[derive(Error, Debug, Clone)]
#[error("Connection broken, reason: {0}")]
pub struct BrokenConnectionError(Arc<dyn Error + Sync + Send>);

impl BrokenConnectionError {
    pub fn get_inner(&self) -> &Arc<dyn Error + Sync + Send> {
        &self.0
    }
}

#[derive(Error, Debug)]
#[non_exhaustive]
pub enum BrokenConnectionErrorKind {
    #[error("Timed out while waiting for response to keepalive request on connection to node {0}")]
    KeepaliveTimeout(IpAddr),
    #[error("Failed to execute keepalive query: {0}")]
    KeepaliveQueryError(RequestError),
    #[error("Failed to deserialize frame: {0}")]
    FrameError(FrameError),
    #[error("Failed to handle server event: {0}")]
    CqlEventHandlingError(#[from] CqlEventHandlingError),
    #[error("Received a server frame with unexpected stream id: {0}")]
    UnexpectedStreamId(i16),
    #[error("Failed to write data: {0}")]
    WriteError(std::io::Error),
    #[error("Too many orphaned stream ids: {0}")]
    TooManyOrphanedStreamIds(u16),
    #[error(
        "Failed to send/receive data needed to perform a request via tokio channel.
        It implies that other half of the channel has been dropped.
        The connection was already broken for some other reason."
    )]
    ChannelError,
}

/// Failed to handle a CQL event received on a stream -1.
/// Possible error kinds are:
/// - failed to deserialize response's frame header
/// - failed to deserialize CQL event response
/// - received invalid server response
/// - failed to send an event info via channel (connection is probably broken)
#[derive(Error, Debug)]
#[non_exhaustive]
pub enum CqlEventHandlingError {
    #[error("Failed to deserialize EVENT response: {0}")]
    CqlEventParseError(#[from] CqlEventParseError),
    #[error("Received unexpected server response on stream -1: {0}. Expected EVENT response")]
    UnexpectedResponse(CqlResponseKind),
    #[error("Failed to deserialize a header of frame received on stream -1: {0}")]
    FrameError(#[from] FrameError),
    #[error("Failed to send event info via channel. The channel is probably closed, which is caused by connection being broken")]
    SendError,
}

/// An error type returned from Connection::parse_response.
/// This is driver's internal type.
#[derive(Error, Debug)]
pub enum ResponseParseError {
    #[error(transparent)]
    FrameError(#[from] FrameError),
    #[error(transparent)]
    CqlResponseParseError(#[from] CqlResponseParseError),
}

/// An error that occurred when performing a request.
///
/// Possible error kinds:
/// - Connection is broken
/// - Response's frame header deserialization error
/// - CQL response (frame body) deserialization error
/// - Driver was unable to allocate a stream id for a request
///
/// This error type is only destined to narrow the return error type
/// of some functions that would previously return [`crate::errors::QueryError`].
#[derive(Error, Debug)]
pub enum RequestError {
    #[error(transparent)]
    FrameError(#[from] FrameError),
    #[error(transparent)]
    CqlResponseParseError(#[from] CqlResponseParseError),
    #[error(transparent)]
    BrokenConnection(#[from] BrokenConnectionError),
    #[error("Unable to allocate a stream id")]
    UnableToAllocStreamId,
}

impl From<BrokenConnectionErrorKind> for BrokenConnectionError {
    fn from(value: BrokenConnectionErrorKind) -> Self {
        BrokenConnectionError(Arc::new(value))
    }
}

impl From<ResponseParseError> for RequestError {
    fn from(value: ResponseParseError) -> Self {
        match value {
            ResponseParseError::FrameError(e) => e.into(),
            ResponseParseError::CqlResponseParseError(e) => e.into(),
        }
    }
}

impl std::fmt::Display for WriteType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
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

impl From<RequestError> for QueryError {
    fn from(value: RequestError) -> Self {
        match value {
            RequestError::FrameError(e) => e.into(),
            RequestError::CqlResponseParseError(e) => e.into(),
            RequestError::BrokenConnection(e) => e.into(),
            RequestError::UnableToAllocStreamId => QueryError::UnableToAllocStreamId,
        }
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
            QueryError::CqlResponseParseError(e) => NewSessionError::CqlResponseParseError(e),
            QueryError::IoError(e) => NewSessionError::IoError(e),
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

impl From<u8> for OperationType {
    fn from(operation_type: u8) -> OperationType {
        match operation_type {
            0 => OperationType::Read,
            1 => OperationType::Write,
            other => OperationType::Other(other),
        }
    }
}

impl From<&str> for WriteType {
    fn from(write_type_str: &str) -> WriteType {
        match write_type_str {
            "SIMPLE" => WriteType::Simple,
            "BATCH" => WriteType::Batch,
            "UNLOGGED_BATCH" => WriteType::UnloggedBatch,
            "COUNTER" => WriteType::Counter,
            "BATCH_LOG" => WriteType::BatchLog,
            "CAS" => WriteType::Cas,
            "VIEW" => WriteType::View,
            "CDC" => WriteType::Cdc,
            _ => WriteType::Other(write_type_str.to_string()),
        }
    }
}

impl WriteType {
    pub fn as_str(&self) -> &str {
        match self {
            WriteType::Simple => "SIMPLE",
            WriteType::Batch => "BATCH",
            WriteType::UnloggedBatch => "UNLOGGED_BATCH",
            WriteType::Counter => "COUNTER",
            WriteType::BatchLog => "BATCH_LOG",
            WriteType::Cas => "CAS",
            WriteType::View => "VIEW",
            WriteType::Cdc => "CDC",
            WriteType::Other(write_type) => write_type.as_str(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{DbError, QueryError, WriteType};
    use crate::frame::types::Consistency;

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
