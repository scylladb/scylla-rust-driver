use crate::frame::frame_errors::{CqlErrorParseError, LowLevelDeserializationError};
use crate::frame::protocol_features::ProtocolFeatures;
use crate::frame::types;
use crate::Consistency;
use byteorder::ReadBytesExt;
use bytes::Bytes;
use thiserror::Error;

#[derive(Debug, Clone)]
pub struct Error {
    pub error: DbError,
    pub reason: String,
}

fn make_error_field_err(
    db_error: &'static str,
    field: &'static str,
    err: impl Into<LowLevelDeserializationError>,
) -> CqlErrorParseError {
    CqlErrorParseError::MalformedErrorField {
        db_error,
        field,
        err: err.into(),
    }
}

impl Error {
    pub fn deserialize(
        features: &ProtocolFeatures,
        buf: &mut &[u8],
    ) -> Result<Self, CqlErrorParseError> {
        let code = types::read_int(buf)
            .map_err(|err| CqlErrorParseError::ErrorCodeParseError(err.into()))?;
        let reason = types::read_string(buf)
            .map_err(CqlErrorParseError::ReasonParseError)?
            .to_owned();

        let error: DbError = match code {
            0x0000 => DbError::ServerError,
            0x000A => DbError::ProtocolError,
            0x0100 => DbError::AuthenticationError,
            0x1000 => DbError::Unavailable {
                consistency: types::read_consistency(buf)
                    .map_err(|err| make_error_field_err("UNAVAILABLE", "CONSISTENCY", err))?,
                required: types::read_int(buf)
                    .map_err(|err| make_error_field_err("UNAVAILABLE", "REQUIRED", err))?,
                alive: types::read_int(buf)
                    .map_err(|err| make_error_field_err("UNAVAILABLE", "ALIVE", err))?,
            },
            0x1001 => DbError::Overloaded,
            0x1002 => DbError::IsBootstrapping,
            0x1003 => DbError::TruncateError,
            0x1100 => DbError::WriteTimeout {
                consistency: types::read_consistency(buf)
                    .map_err(|err| make_error_field_err("WRITE_TIMEOUT", "CONSISTENCY", err))?,
                received: types::read_int(buf)
                    .map_err(|err| make_error_field_err("WRITE_TIMEOUT", "RECEIVED", err))?,
                required: types::read_int(buf)
                    .map_err(|err| make_error_field_err("WRITE_TIMEOUT", "REQUIRED", err))?,
                write_type: WriteType::from(
                    types::read_string(buf)
                        .map_err(|err| make_error_field_err("WRITE_TIMEOUT", "WRITE_TYPE", err))?,
                ),
            },
            0x1200 => DbError::ReadTimeout {
                consistency: types::read_consistency(buf)
                    .map_err(|err| make_error_field_err("READ_TIMEOUT", "CONSISTENCY", err))?,
                received: types::read_int(buf)
                    .map_err(|err| make_error_field_err("READ_TIMEOUT", "RECEIVED", err))?,
                required: types::read_int(buf)
                    .map_err(|err| make_error_field_err("READ_TIMEOUT", "REQUIRED", err))?,
                data_present: buf
                    .read_u8()
                    .map_err(|err| make_error_field_err("READ_TIMEOUT", "DATA_PRESENT", err))?
                    != 0,
            },
            0x1300 => DbError::ReadFailure {
                consistency: types::read_consistency(buf)
                    .map_err(|err| make_error_field_err("READ_FAILURE", "CONSISTENCY", err))?,
                received: types::read_int(buf)
                    .map_err(|err| make_error_field_err("READ_FAILURE", "RECEIVED", err))?,
                required: types::read_int(buf)
                    .map_err(|err| make_error_field_err("READ_FAILURE", "REQUIRED", err))?,
                numfailures: types::read_int(buf)
                    .map_err(|err| make_error_field_err("READ_FAILURE", "NUM_FAILURES", err))?,
                data_present: buf
                    .read_u8()
                    .map_err(|err| make_error_field_err("READ_FAILURE", "DATA_PRESENT", err))?
                    != 0,
            },
            0x1400 => DbError::FunctionFailure {
                keyspace: types::read_string(buf)
                    .map_err(|err| make_error_field_err("FUNCTION_FAILURE", "KEYSPACE", err))?
                    .to_string(),
                function: types::read_string(buf)
                    .map_err(|err| make_error_field_err("FUNCTION_FAILURE", "FUNCTION", err))?
                    .to_string(),
                arg_types: types::read_string_list(buf)
                    .map_err(|err| make_error_field_err("FUNCTION_FAILURE", "ARG_TYPES", err))?,
            },
            0x1500 => DbError::WriteFailure {
                consistency: types::read_consistency(buf)
                    .map_err(|err| make_error_field_err("WRITE_FAILURE", "CONSISTENCY", err))?,
                received: types::read_int(buf)
                    .map_err(|err| make_error_field_err("WRITE_FAILURE", "RECEIVED", err))?,
                required: types::read_int(buf)
                    .map_err(|err| make_error_field_err("WRITE_FAILURE", "REQUIRED", err))?,
                numfailures: types::read_int(buf)
                    .map_err(|err| make_error_field_err("WRITE_FAILURE", "NUM_FAILURES", err))?,
                write_type: WriteType::from(
                    types::read_string(buf)
                        .map_err(|err| make_error_field_err("WRITE_FAILURE", "WRITE_TYPE", err))?,
                ),
            },
            0x2000 => DbError::SyntaxError,
            0x2100 => DbError::Unauthorized,
            0x2200 => DbError::Invalid,
            0x2300 => DbError::ConfigError,
            0x2400 => DbError::AlreadyExists {
                keyspace: types::read_string(buf)
                    .map_err(|err| make_error_field_err("ALREADY_EXISTS", "KEYSPACE", err))?
                    .to_string(),
                table: types::read_string(buf)
                    .map_err(|err| make_error_field_err("ALREADY_EXISTS", "TABLE", err))?
                    .to_string(),
            },
            0x2500 => DbError::Unprepared {
                statement_id: Bytes::from(
                    types::read_short_bytes(buf)
                        .map_err(|err| make_error_field_err("UNPREPARED", "STATEMENT_ID", err))?
                        .to_owned(),
                ),
            },
            code if Some(code) == features.rate_limit_error => {
                DbError::RateLimitReached {
                    op_type: OperationType::from(buf.read_u8().map_err(|err| {
                        make_error_field_err("RATE_LIMIT_REACHED", "OP_TYPE", err)
                    })?),
                    rejected_by_coordinator: buf.read_u8().map_err(|err| {
                        make_error_field_err("RATE_LIMIT_REACHED", "REJECTED_BY_COORDINATOR", err)
                    })? != 0,
                }
            }
            _ => DbError::Other(code),
        };

        Ok(Error { error, reason })
    }
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

#[cfg(test)]
mod tests {
    use super::{DbError, Error, OperationType, WriteType};
    use crate::frame::protocol_features::ProtocolFeatures;
    use crate::Consistency;
    use bytes::Bytes;
    use std::convert::TryInto;

    // Serializes the beginning of an ERROR response - error code and message
    // All custom data depending on the error type is appended after these bytes
    fn make_error_request_bytes(error_code: i32, message: &str) -> Vec<u8> {
        let mut bytes: Vec<u8> = Vec::new();
        let message_len: u16 = message.len().try_into().unwrap();

        bytes.extend(error_code.to_be_bytes());
        bytes.extend(message_len.to_be_bytes());
        bytes.extend(message.as_bytes());

        bytes
    }

    // Tests deserialization of all errors without and additional data
    #[test]
    fn deserialize_simple_errors() {
        let simple_error_mappings: [(i32, DbError); 11] = [
            (0x0000, DbError::ServerError),
            (0x000A, DbError::ProtocolError),
            (0x0100, DbError::AuthenticationError),
            (0x1001, DbError::Overloaded),
            (0x1002, DbError::IsBootstrapping),
            (0x1003, DbError::TruncateError),
            (0x2000, DbError::SyntaxError),
            (0x2100, DbError::Unauthorized),
            (0x2200, DbError::Invalid),
            (0x2300, DbError::ConfigError),
            (0x1234, DbError::Other(0x1234)),
        ];

        let features = ProtocolFeatures::default();

        for (error_code, expected_error) in &simple_error_mappings {
            let bytes: Vec<u8> = make_error_request_bytes(*error_code, "simple message");
            let error: Error = Error::deserialize(&features, &mut bytes.as_slice()).unwrap();
            assert_eq!(error.error, *expected_error);
            assert_eq!(error.reason, "simple message");
        }
    }

    #[test]
    fn deserialize_unavailable() {
        let features = ProtocolFeatures::default();

        let mut bytes = make_error_request_bytes(0x1000, "message 2");
        bytes.extend(1_i16.to_be_bytes());
        bytes.extend(2_i32.to_be_bytes());
        bytes.extend(3_i32.to_be_bytes());

        let error: Error = Error::deserialize(&features, &mut bytes.as_slice()).unwrap();

        assert_eq!(
            error.error,
            DbError::Unavailable {
                consistency: Consistency::One,
                required: 2,
                alive: 3,
            }
        );
        assert_eq!(error.reason, "message 2");
    }

    #[test]
    fn deserialize_write_timeout() {
        let features = ProtocolFeatures::default();

        let mut bytes = make_error_request_bytes(0x1100, "message 2");
        bytes.extend(0x0004_i16.to_be_bytes());
        bytes.extend((-5_i32).to_be_bytes());
        bytes.extend(100_i32.to_be_bytes());

        let write_type_str = "SIMPLE";
        let write_type_str_len: u16 = write_type_str.len().try_into().unwrap();
        bytes.extend(write_type_str_len.to_be_bytes());
        bytes.extend(write_type_str.as_bytes());

        let error: Error = Error::deserialize(&features, &mut bytes.as_slice()).unwrap();

        assert_eq!(
            error.error,
            DbError::WriteTimeout {
                consistency: Consistency::Quorum,
                received: -5, // Allow negative values when they don't make sense, it's better than crashing with ProtocolError
                required: 100,
                write_type: WriteType::Simple,
            }
        );
        assert_eq!(error.reason, "message 2");
    }

    #[test]
    fn deserialize_read_timeout() {
        let features = ProtocolFeatures::default();

        let mut bytes = make_error_request_bytes(0x1200, "message 2");
        bytes.extend(0x0002_i16.to_be_bytes());
        bytes.extend(8_i32.to_be_bytes());
        bytes.extend(32_i32.to_be_bytes());
        bytes.push(0_u8);

        let error: Error = Error::deserialize(&features, &mut bytes.as_slice()).unwrap();

        assert_eq!(
            error.error,
            DbError::ReadTimeout {
                consistency: Consistency::Two,
                received: 8,
                required: 32,
                data_present: false,
            }
        );
        assert_eq!(error.reason, "message 2");
    }

    #[test]
    fn deserialize_read_failure() {
        let features = ProtocolFeatures::default();

        let mut bytes = make_error_request_bytes(0x1300, "message 2");
        bytes.extend(0x0003_i16.to_be_bytes());
        bytes.extend(4_i32.to_be_bytes());
        bytes.extend(5_i32.to_be_bytes());
        bytes.extend(6_i32.to_be_bytes());
        bytes.push(123_u8); // Any non-zero value means data_present is true

        let error: Error = Error::deserialize(&features, &mut bytes.as_slice()).unwrap();

        assert_eq!(
            error.error,
            DbError::ReadFailure {
                consistency: Consistency::Three,
                received: 4,
                required: 5,
                numfailures: 6,
                data_present: true,
            }
        );
        assert_eq!(error.reason, "message 2");
    }

    #[test]
    fn deserialize_function_failure() {
        let features = ProtocolFeatures::default();

        let mut bytes = make_error_request_bytes(0x1400, "message 2");

        let keyspace_name: &str = "keyspace_name";
        let keyspace_name_len: u16 = keyspace_name.len().try_into().unwrap();

        let function_name: &str = "function_name";
        let function_name_len: u16 = function_name.len().try_into().unwrap();

        let type1: &str = "type1";
        let type1_len: u16 = type1.len().try_into().unwrap();

        let type2: &str = "type2";
        let type2_len: u16 = type1.len().try_into().unwrap();

        bytes.extend(keyspace_name_len.to_be_bytes());
        bytes.extend(keyspace_name.as_bytes());
        bytes.extend(function_name_len.to_be_bytes());
        bytes.extend(function_name.as_bytes());
        bytes.extend(2_i16.to_be_bytes());
        bytes.extend(type1_len.to_be_bytes());
        bytes.extend(type1.as_bytes());
        bytes.extend(type2_len.to_be_bytes());
        bytes.extend(type2.as_bytes());

        let error: Error = Error::deserialize(&features, &mut bytes.as_slice()).unwrap();

        assert_eq!(
            error.error,
            DbError::FunctionFailure {
                keyspace: "keyspace_name".to_string(),
                function: "function_name".to_string(),
                arg_types: vec!["type1".to_string(), "type2".to_string()]
            }
        );
        assert_eq!(error.reason, "message 2");
    }

    #[test]
    fn deserialize_write_failure() {
        let features = ProtocolFeatures::default();

        let mut bytes = make_error_request_bytes(0x1500, "message 2");

        bytes.extend(0x0000_i16.to_be_bytes());
        bytes.extend(2_i32.to_be_bytes());
        bytes.extend(4_i32.to_be_bytes());
        bytes.extend(8_i32.to_be_bytes());

        let write_type_str = "COUNTER";
        let write_type_str_len: u16 = write_type_str.len().try_into().unwrap();
        bytes.extend(write_type_str_len.to_be_bytes());
        bytes.extend(write_type_str.as_bytes());

        let error: Error = Error::deserialize(&features, &mut bytes.as_slice()).unwrap();

        assert_eq!(
            error.error,
            DbError::WriteFailure {
                consistency: Consistency::Any,
                received: 2,
                required: 4,
                numfailures: 8,
                write_type: WriteType::Counter,
            }
        );
        assert_eq!(error.reason, "message 2");
    }

    #[test]
    fn deserialize_already_exists() {
        let features = ProtocolFeatures::default();

        let mut bytes = make_error_request_bytes(0x2400, "message 2");

        let keyspace_name: &str = "keyspace_name";
        let keyspace_name_len: u16 = keyspace_name.len().try_into().unwrap();

        let table_name: &str = "table_name";
        let table_name_len: u16 = table_name.len().try_into().unwrap();

        bytes.extend(keyspace_name_len.to_be_bytes());
        bytes.extend(keyspace_name.as_bytes());
        bytes.extend(table_name_len.to_be_bytes());
        bytes.extend(table_name.as_bytes());

        let error: Error = Error::deserialize(&features, &mut bytes.as_slice()).unwrap();

        assert_eq!(
            error.error,
            DbError::AlreadyExists {
                keyspace: "keyspace_name".to_string(),
                table: "table_name".to_string(),
            }
        );
        assert_eq!(error.reason, "message 2");
    }

    #[test]
    fn deserialize_unprepared() {
        let features = ProtocolFeatures::default();

        let mut bytes = make_error_request_bytes(0x2500, "message 3");
        let statement_id = b"deadbeef";
        bytes.extend((statement_id.len() as i16).to_be_bytes());
        bytes.extend(statement_id);

        let error: Error = Error::deserialize(&features, &mut bytes.as_slice()).unwrap();

        assert_eq!(
            error.error,
            DbError::Unprepared {
                statement_id: Bytes::from_static(b"deadbeef")
            }
        );
        assert_eq!(error.reason, "message 3");
    }

    #[test]
    fn deserialize_rate_limit_error() {
        let features = ProtocolFeatures {
            rate_limit_error: Some(0x4321),
            ..Default::default()
        };
        let mut bytes = make_error_request_bytes(0x4321, "message 1");
        bytes.extend([0u8]); // Read type
        bytes.extend([1u8]); // Rejected by coordinator
        let error = Error::deserialize(&features, &mut bytes.as_slice()).unwrap();

        assert_eq!(
            error.error,
            DbError::RateLimitReached {
                op_type: OperationType::Read,
                rejected_by_coordinator: true,
            }
        );
        assert_eq!(error.reason, "message 1");

        let features = ProtocolFeatures {
            rate_limit_error: Some(0x8765),
            ..Default::default()
        };
        let mut bytes = make_error_request_bytes(0x8765, "message 2");
        bytes.extend([1u8]); // Write type
        bytes.extend([0u8]); // Not rejected by coordinator
        let error = Error::deserialize(&features, &mut bytes.as_slice()).unwrap();

        assert_eq!(
            error.error,
            DbError::RateLimitReached {
                op_type: OperationType::Write,
                rejected_by_coordinator: false,
            }
        );
        assert_eq!(error.reason, "message 2");
    }
}
