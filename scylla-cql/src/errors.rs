//! This module contains various errors which can be returned by `scylla::Session`

use crate::frame::protocol_features::ProtocolFeatures;
use crate::Consistency;
use bytes::Bytes;
use thiserror::Error;

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

impl std::fmt::Display for WriteType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self)
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
