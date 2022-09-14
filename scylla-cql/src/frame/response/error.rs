use crate::errors::{DbError, OperationType, QueryError, WriteType};
use crate::frame::frame_errors::ParseError;
use crate::frame::protocol_features::ProtocolFeatures;
use crate::frame::types;
use byteorder::ReadBytesExt;
use bytes::Bytes;

#[derive(Debug)]
pub struct Error {
    pub error: DbError,
    pub reason: String,
}

impl Error {
    pub fn deserialize(features: &ProtocolFeatures, buf: &mut &[u8]) -> Result<Self, ParseError> {
        let code = types::read_int(buf)?;
        let reason = types::read_string(buf)?.to_owned();

        let error: DbError = match code {
            0x0000 => DbError::ServerError,
            0x000A => DbError::ProtocolError,
            0x0100 => DbError::AuthenticationError,
            0x1000 => DbError::Unavailable {
                consistency: types::read_consistency(buf)?,
                required: types::read_int(buf)?,
                alive: types::read_int(buf)?,
            },
            0x1001 => DbError::Overloaded,
            0x1002 => DbError::IsBootstrapping,
            0x1003 => DbError::TruncateError,
            0x1100 => DbError::WriteTimeout {
                consistency: types::read_consistency(buf)?,
                received: types::read_int(buf)?,
                required: types::read_int(buf)?,
                write_type: WriteType::from(types::read_string(buf)?),
            },
            0x1200 => DbError::ReadTimeout {
                consistency: types::read_consistency(buf)?,
                received: types::read_int(buf)?,
                required: types::read_int(buf)?,
                data_present: buf.read_u8()? != 0,
            },
            0x1300 => DbError::ReadFailure {
                consistency: types::read_consistency(buf)?,
                received: types::read_int(buf)?,
                required: types::read_int(buf)?,
                numfailures: types::read_int(buf)?,
                data_present: buf.read_u8()? != 0,
            },
            0x1400 => DbError::FunctionFailure {
                keyspace: types::read_string(buf)?.to_string(),
                function: types::read_string(buf)?.to_string(),
                arg_types: types::read_string_list(buf)?,
            },
            0x1500 => DbError::WriteFailure {
                consistency: types::read_consistency(buf)?,
                received: types::read_int(buf)?,
                required: types::read_int(buf)?,
                numfailures: types::read_int(buf)?,
                write_type: WriteType::from(types::read_string(buf)?),
            },
            0x2000 => DbError::SyntaxError,
            0x2100 => DbError::Unauthorized,
            0x2200 => DbError::Invalid,
            0x2300 => DbError::ConfigError,
            0x2400 => DbError::AlreadyExists {
                keyspace: types::read_string(buf)?.to_string(),
                table: types::read_string(buf)?.to_string(),
            },
            0x2500 => DbError::Unprepared {
                statement_id: Bytes::from(types::read_short_bytes(buf)?.to_owned()),
            },
            code if Some(code) == features.rate_limit_error => DbError::RateLimitReached {
                op_type: OperationType::from(buf.read_u8()?),
                rejected_by_coordinator: buf.read_u8()? != 0,
            },
            _ => DbError::Other(code),
        };

        Ok(Error { error, reason })
    }
}

impl From<Error> for QueryError {
    fn from(error: Error) -> QueryError {
        QueryError::DbError(error.error, error.reason)
    }
}

#[cfg(test)]
mod tests {
    use super::Error;
    use crate::errors::{DbError, OperationType, WriteType};
    use crate::frame::protocol_features::ProtocolFeatures;
    use crate::frame::types::LegacyConsistency;
    use crate::Consistency;
    use bytes::Bytes;
    use std::convert::TryInto;

    // Serializes the beginning of an ERROR response - error code and message
    // All custom data depending on the error type is appended after these bytes
    fn make_error_request_bytes(error_code: i32, message: &str) -> Vec<u8> {
        let mut bytes: Vec<u8> = Vec::new();
        let message_len: u16 = message.len().try_into().unwrap();

        bytes.extend(&error_code.to_be_bytes());
        bytes.extend(&message_len.to_be_bytes());
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
        bytes.extend(&1_i16.to_be_bytes());
        bytes.extend(&2_i32.to_be_bytes());
        bytes.extend(&3_i32.to_be_bytes());

        let error: Error = Error::deserialize(&features, &mut bytes.as_slice()).unwrap();

        assert_eq!(
            error.error,
            DbError::Unavailable {
                consistency: LegacyConsistency::Regular(Consistency::One),
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
        bytes.extend(&0x0004_i16.to_be_bytes());
        bytes.extend(&(-5_i32).to_be_bytes());
        bytes.extend(&100_i32.to_be_bytes());

        let write_type_str = "SIMPLE";
        let write_type_str_len: u16 = write_type_str.len().try_into().unwrap();
        bytes.extend(&write_type_str_len.to_be_bytes());
        bytes.extend(write_type_str.as_bytes());

        let error: Error = Error::deserialize(&features, &mut bytes.as_slice()).unwrap();

        assert_eq!(
            error.error,
            DbError::WriteTimeout {
                consistency: LegacyConsistency::Regular(Consistency::Quorum),
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
        bytes.extend(&0x0002_i16.to_be_bytes());
        bytes.extend(&8_i32.to_be_bytes());
        bytes.extend(&32_i32.to_be_bytes());
        bytes.push(0_u8);

        let error: Error = Error::deserialize(&features, &mut bytes.as_slice()).unwrap();

        assert_eq!(
            error.error,
            DbError::ReadTimeout {
                consistency: LegacyConsistency::Regular(Consistency::Two),
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
        bytes.extend(&0x0003_i16.to_be_bytes());
        bytes.extend(&4_i32.to_be_bytes());
        bytes.extend(&5_i32.to_be_bytes());
        bytes.extend(&6_i32.to_be_bytes());
        bytes.push(123_u8); // Any non-zero value means data_present is true

        let error: Error = Error::deserialize(&features, &mut bytes.as_slice()).unwrap();

        assert_eq!(
            error.error,
            DbError::ReadFailure {
                consistency: LegacyConsistency::Regular(Consistency::Three),
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

        bytes.extend(&keyspace_name_len.to_be_bytes());
        bytes.extend(keyspace_name.as_bytes());
        bytes.extend(&function_name_len.to_be_bytes());
        bytes.extend(function_name.as_bytes());
        bytes.extend(&2_i16.to_be_bytes());
        bytes.extend(&type1_len.to_be_bytes());
        bytes.extend(type1.as_bytes());
        bytes.extend(&type2_len.to_be_bytes());
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

        bytes.extend(&0x0000_i16.to_be_bytes());
        bytes.extend(&2_i32.to_be_bytes());
        bytes.extend(&4_i32.to_be_bytes());
        bytes.extend(&8_i32.to_be_bytes());

        let write_type_str = "COUNTER";
        let write_type_str_len: u16 = write_type_str.len().try_into().unwrap();
        bytes.extend(&write_type_str_len.to_be_bytes());
        bytes.extend(write_type_str.as_bytes());

        let error: Error = Error::deserialize(&features, &mut bytes.as_slice()).unwrap();

        assert_eq!(
            error.error,
            DbError::WriteFailure {
                consistency: LegacyConsistency::Regular(Consistency::Any),
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

        bytes.extend(&keyspace_name_len.to_be_bytes());
        bytes.extend(keyspace_name.as_bytes());
        bytes.extend(&table_name_len.to_be_bytes());
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
