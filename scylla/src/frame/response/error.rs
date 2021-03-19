use crate::frame::frame_errors::ParseError;
use crate::frame::types;
use crate::transport::errors::{DBError, QueryError, WriteType};
use byteorder::ReadBytesExt;

#[derive(Debug)]
pub struct Error {
    pub error: DBError,
    pub reason: String,
}

impl Error {
    pub fn deserialize(buf: &mut &[u8]) -> Result<Self, ParseError> {
        let code = types::read_int(buf)?;
        let reason = types::read_string(buf)?.to_owned();

        let error: DBError = match code {
            0x0000 => DBError::ServerError,
            0x000A => DBError::ProtocolError,
            0x0100 => DBError::AuthenticationError,
            0x1000 => DBError::Unavailable {
                consistency: types::read_consistency(buf)?,
                required: types::read_int(buf)?,
                alive: types::read_int(buf)?,
            },
            0x1001 => DBError::Overloaded,
            0x1002 => DBError::IsBootstrapping,
            0x1003 => DBError::TruncateError,
            0x1100 => DBError::WriteTimeout {
                consistency: types::read_consistency(buf)?,
                received: types::read_int(buf)?,
                required: types::read_int(buf)?,
                write_type: WriteType::from(types::read_string(buf)?),
            },
            0x1200 => DBError::ReadTimeout {
                consistency: types::read_consistency(buf)?,
                received: types::read_int(buf)?,
                required: types::read_int(buf)?,
                data_present: buf.read_u8()? != 0,
            },
            0x1300 => DBError::ReadFailure {
                consistency: types::read_consistency(buf)?,
                received: types::read_int(buf)?,
                required: types::read_int(buf)?,
                numfailures: types::read_int(buf)?,
                data_present: buf.read_u8()? != 0,
            },
            0x1400 => DBError::FunctionFailure {
                keyspace: types::read_string(buf)?.to_string(),
                function: types::read_string(buf)?.to_string(),
                arg_types: types::read_string_list(buf)?,
            },
            0x1500 => DBError::WriteFailure {
                consistency: types::read_consistency(buf)?,
                received: types::read_int(buf)?,
                required: types::read_int(buf)?,
                numfailures: types::read_int(buf)?,
                write_type: WriteType::from(types::read_string(buf)?),
            },
            0x2000 => DBError::SyntaxError,
            0x2100 => DBError::Unauthorized,
            0x2200 => DBError::Invalid,
            0x2300 => DBError::ConfigError,
            0x2400 => DBError::AlreadyExists {
                keyspace: types::read_string(buf)?.to_string(),
                table: types::read_string(buf)?.to_string(),
            },
            0x2500 => DBError::Unprepared,
            _ => DBError::Other(code),
        };

        Ok(Error { error, reason })
    }
}

impl Into<QueryError> for Error {
    fn into(self) -> QueryError {
        QueryError::DBError(self.error, self.reason)
    }
}

#[cfg(test)]
mod tests {
    use super::Error;
    use crate::statement::Consistency;
    use crate::transport::errors::{DBError, WriteType};
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
        let simple_error_mappings: [(i32, DBError); 12] = [
            (0x0000, DBError::ServerError),
            (0x000A, DBError::ProtocolError),
            (0x0100, DBError::AuthenticationError),
            (0x1001, DBError::Overloaded),
            (0x1002, DBError::IsBootstrapping),
            (0x1003, DBError::TruncateError),
            (0x2000, DBError::SyntaxError),
            (0x2100, DBError::Unauthorized),
            (0x2200, DBError::Invalid),
            (0x2300, DBError::ConfigError),
            (0x2500, DBError::Unprepared),
            (0x1234, DBError::Other(0x1234)),
        ];

        for (error_code, expected_error) in &simple_error_mappings {
            let bytes: Vec<u8> = make_error_request_bytes(*error_code, "simple message");
            let error: Error = Error::deserialize(&mut bytes.as_slice()).unwrap();
            assert_eq!(error.error, *expected_error);
            assert_eq!(error.reason, "simple message");
        }
    }

    #[test]
    fn deserialize_unavailable() {
        let mut bytes = make_error_request_bytes(0x1000, "message 2");
        bytes.extend(&1_i16.to_be_bytes());
        bytes.extend(&2_i32.to_be_bytes());
        bytes.extend(&3_i32.to_be_bytes());

        let error: Error = Error::deserialize(&mut bytes.as_slice()).unwrap();

        assert_eq!(
            error.error,
            DBError::Unavailable {
                consistency: Consistency::One,
                required: 2,
                alive: 3,
            }
        );
        assert_eq!(error.reason, "message 2");
    }

    #[test]
    fn deserialize_write_timeout() {
        let mut bytes = make_error_request_bytes(0x1100, "message 2");
        bytes.extend(&0x0004_i16.to_be_bytes());
        bytes.extend(&(-5_i32).to_be_bytes());
        bytes.extend(&100_i32.to_be_bytes());

        let write_type_str = "SIMPLE";
        let write_type_str_len: u16 = write_type_str.len().try_into().unwrap();
        bytes.extend(&write_type_str_len.to_be_bytes());
        bytes.extend(write_type_str.as_bytes());

        let error: Error = Error::deserialize(&mut bytes.as_slice()).unwrap();

        assert_eq!(
            error.error,
            DBError::WriteTimeout {
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
        let mut bytes = make_error_request_bytes(0x1200, "message 2");
        bytes.extend(&0x0002_i16.to_be_bytes());
        bytes.extend(&8_i32.to_be_bytes());
        bytes.extend(&32_i32.to_be_bytes());
        bytes.push(0_u8);

        let error: Error = Error::deserialize(&mut bytes.as_slice()).unwrap();

        assert_eq!(
            error.error,
            DBError::ReadTimeout {
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
        let mut bytes = make_error_request_bytes(0x1300, "message 2");
        bytes.extend(&0x0003_i16.to_be_bytes());
        bytes.extend(&4_i32.to_be_bytes());
        bytes.extend(&5_i32.to_be_bytes());
        bytes.extend(&6_i32.to_be_bytes());
        bytes.push(123_u8); // Any non-zero value means data_present is true

        let error: Error = Error::deserialize(&mut bytes.as_slice()).unwrap();

        assert_eq!(
            error.error,
            DBError::ReadFailure {
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

        let error: Error = Error::deserialize(&mut bytes.as_slice()).unwrap();

        assert_eq!(
            error.error,
            DBError::FunctionFailure {
                keyspace: "keyspace_name".to_string(),
                function: "function_name".to_string(),
                arg_types: vec!["type1".to_string(), "type2".to_string()]
            }
        );
        assert_eq!(error.reason, "message 2");
    }

    #[test]
    fn deserialize_write_failure() {
        let mut bytes = make_error_request_bytes(0x1500, "message 2");

        bytes.extend(&0x0000_i16.to_be_bytes());
        bytes.extend(&2_i32.to_be_bytes());
        bytes.extend(&4_i32.to_be_bytes());
        bytes.extend(&8_i32.to_be_bytes());

        let write_type_str = "COUNTER";
        let write_type_str_len: u16 = write_type_str.len().try_into().unwrap();
        bytes.extend(&write_type_str_len.to_be_bytes());
        bytes.extend(write_type_str.as_bytes());

        let error: Error = Error::deserialize(&mut bytes.as_slice()).unwrap();

        assert_eq!(
            error.error,
            DBError::WriteFailure {
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
        let mut bytes = make_error_request_bytes(0x2400, "message 2");

        let keyspace_name: &str = "keyspace_name";
        let keyspace_name_len: u16 = keyspace_name.len().try_into().unwrap();

        let table_name: &str = "table_name";
        let table_name_len: u16 = table_name.len().try_into().unwrap();

        bytes.extend(&keyspace_name_len.to_be_bytes());
        bytes.extend(keyspace_name.as_bytes());
        bytes.extend(&table_name_len.to_be_bytes());
        bytes.extend(table_name.as_bytes());

        let error: Error = Error::deserialize(&mut bytes.as_slice()).unwrap();

        assert_eq!(
            error.error,
            DBError::AlreadyExists {
                keyspace: "keyspace_name".to_string(),
                table: "table_name".to_string(),
            }
        );
        assert_eq!(error.reason, "message 2");
    }
}
