//! CQL requests sent by the client.

pub mod auth_response;
pub mod batch;
pub mod execute;
pub mod options;
pub mod prepare;
pub mod query;
pub mod register;
pub mod startup;

use batch::BatchTypeParseError;
use thiserror::Error;

use crate::Consistency;
use crate::frame::protocol_features::ProtocolFeatures;
use crate::frame::request::execute::ExecuteV2;
use crate::serialize::row::SerializedValues;
use bytes::Bytes;

pub use auth_response::AuthResponse;
pub use batch::Batch;
#[expect(deprecated)]
pub use execute::Execute;
pub use options::Options;
pub use prepare::Prepare;
pub use query::Query;
pub use startup::Startup;

use self::batch::BatchStatement;

use super::TryFromPrimitiveError;
use super::frame_errors::{CqlRequestSerializationError, LowLevelDeserializationError};
use super::types::SerialConsistency;

/// Possible requests sent by the client.
// Why is it distinct from [RequestOpcode]?
// TODO(2.0): merge this with `RequestOpcode`.
#[derive(Debug, Copy, Clone)]
#[non_exhaustive]
pub enum CqlRequestKind {
    /// Initialize the connection. The server will respond by either a READY message
    /// (in which case the connection is ready for queries) or an AUTHENTICATE message
    /// (in which case credentials will need to be provided using AUTH_RESPONSE).
    ///
    /// This must be the first message of the connection, except for OPTIONS that can
    /// be sent before to find out the options supported by the server. Once the
    /// connection has been initialized, a client should not send any more STARTUP
    /// messages.
    Startup,

    /// Answers a server authentication challenge.
    ///
    /// Authentication in the protocol is SASL based. The server sends authentication
    /// challenges (a bytes token) to which the client answers with this message. Those
    /// exchanges continue until the server accepts the authentication by sending a
    /// AUTH_SUCCESS message after a client AUTH_RESPONSE. Note that the exchange
    /// begins with the client sending an initial AUTH_RESPONSE in response to a
    /// server AUTHENTICATE request.
    ///
    /// The response to a AUTH_RESPONSE is either a follow-up AUTH_CHALLENGE message,
    /// an AUTH_SUCCESS message or an ERROR message.
    AuthResponse,

    /// Asks the server to return which STARTUP options are supported. The server
    /// will respond with a SUPPORTED message.
    Options,

    /// Performs a CQL query, i.e., executes an unprepared statement.
    /// The server will respond to a QUERY message with a RESULT message, the content
    /// of which depends on the query.
    Query,

    /// Prepares a query for later execution (through EXECUTE).
    /// The server will respond with a RESULT::Prepared message.
    Prepare,

    /// Executes a prepared query.
    /// The response from the server will be a RESULT message.
    Execute,

    /// Allows executing a list of queries (prepared or not) as a batch (note that
    /// only DML statements are accepted in a batch).
    /// The server will respond with a RESULT message.
    Batch,

    /// Register this connection to receive some types of events.
    /// The response to a REGISTER message will be a READY message.
    Register,
}

impl std::fmt::Display for CqlRequestKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let kind_str = match self {
            CqlRequestKind::Startup => "STARTUP",
            CqlRequestKind::AuthResponse => "AUTH_RESPONSE",
            CqlRequestKind::Options => "OPTIONS",
            CqlRequestKind::Query => "QUERY",
            CqlRequestKind::Prepare => "PREPARE",
            CqlRequestKind::Execute => "EXECUTE",
            CqlRequestKind::Batch => "BATCH",
            CqlRequestKind::Register => "REGISTER",
        };

        f.write_str(kind_str)
    }
}

/// Opcode of a request, used to identify the request type in a CQL frame.
#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord)]
#[repr(u8)]
pub enum RequestOpcode {
    /// See [CqlRequestKind::Startup].
    Startup = 0x01,
    /// See [CqlRequestKind::Options].
    Options = 0x05,
    /// See [CqlRequestKind::Query].
    Query = 0x07,
    /// See [CqlRequestKind::Prepare].
    Prepare = 0x09,
    /// See [CqlRequestKind::Execute].
    Execute = 0x0A,
    /// See [CqlRequestKind::Register].
    Register = 0x0B,
    /// See [CqlRequestKind::Batch].
    Batch = 0x0D,
    /// See [CqlRequestKind::AuthResponse].
    AuthResponse = 0x0F,
}

impl TryFrom<u8> for RequestOpcode {
    type Error = TryFromPrimitiveError<u8>;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0x01 => Ok(Self::Startup),
            0x05 => Ok(Self::Options),
            0x07 => Ok(Self::Query),
            0x09 => Ok(Self::Prepare),
            0x0A => Ok(Self::Execute),
            0x0B => Ok(Self::Register),
            0x0D => Ok(Self::Batch),
            0x0F => Ok(Self::AuthResponse),
            _ => Err(TryFromPrimitiveError {
                enum_name: "RequestOpcode",
                primitive: value,
            }),
        }
    }
}

/// Requests that can be serialized into a CQL frame.
pub trait SerializableRequest {
    /// Opcode of the request, used to identify the request type in the CQL frame.
    const OPCODE: RequestOpcode;

    /// Serializes the request into the provided buffer.
    fn serialize(&self, buf: &mut Vec<u8>) -> Result<(), CqlRequestSerializationError>;

    /// Serializes the request into a heap-allocated `Bytes` object.
    fn to_bytes(&self) -> Result<Bytes, CqlRequestSerializationError> {
        let mut v = Vec::new();
        self.serialize(&mut v)?;
        Ok(v.into())
    }
}

/// Requests that can be deserialized from a CQL frame.
///
/// Not intended for driver's direct usage (as driver has no interest in deserialising CQL requests),
/// but very useful for testing (e.g. asserting that the sent requests have proper parameters set).
pub trait DeserializableRequest: SerializableRequest + Sized {
    /// Deserializes the request from the provided buffer.
    /// Use [DeserializableRequest::deserialize_with_features] instead, because some frame types
    /// require knowing protocol features for correct deserialization.
    #[deprecated(since = "1.4.0", note = "Use deserialize_with_features instead")]
    fn deserialize(buf: &mut &[u8]) -> Result<Self, RequestDeserializationError>;

    fn deserialize_with_features(
        buf: &mut &[u8],
        #[allow(unused_variables)] features: &ProtocolFeatures,
    ) -> Result<Self, RequestDeserializationError> {
        #[expect(deprecated)]
        Self::deserialize(buf)
    }
}

/// An error type returned by [`DeserializableRequest::deserialize`].
/// This is not intended for driver's direct usage. It's a testing utility,
/// mainly used by `scylla-proxy` crate.
#[doc(hidden)]
#[derive(Debug, Error)]
pub enum RequestDeserializationError {
    #[error("Low level deser error: {0}")]
    LowLevelDeserialization(#[from] LowLevelDeserializationError),
    #[error("Io error: {0}")]
    IoError(#[from] std::io::Error),
    #[error("Specified flags are not recognised: {:02x}", flags)]
    UnknownFlags { flags: u8 },
    #[error("Named values in frame are currently unsupported")]
    NamedValuesUnsupported,
    #[error("Expected SerialConsistency, got regular Consistency: {0}")]
    ExpectedSerialConsistency(Consistency),
    #[error(transparent)]
    BatchTypeParse(#[from] BatchTypeParseError),
    #[error("Unexpected batch statement kind: {0}")]
    UnexpectedBatchStatementKind(u8),
}

/// A CQL request that can be sent to the server.
#[non_exhaustive] // TODO: add remaining request types
#[deprecated(
    since = "1.4.0",
    note = "Does not support Scylla metadata id extension. Use RequestV2 instead."
)]
pub enum Request<'r> {
    /// QUERY request, used to execute a single unprepared statement.
    Query(Query<'r>),
    /// EXECUTE request, used to execute a single prepared statement.
    #[expect(deprecated)]
    Execute(Execute<'r>),
    /// BATCH request, used to execute a batch of (prepared, unprepared, or mix of both)
    /// statements.
    Batch(Batch<'r, BatchStatement<'r>, Vec<SerializedValues>>),
}

#[expect(deprecated)]
impl Request<'_> {
    /// Deserializes the request from the provided buffer.
    pub fn deserialize(
        buf: &mut &[u8],
        opcode: RequestOpcode,
    ) -> Result<Self, RequestDeserializationError> {
        match opcode {
            RequestOpcode::Query => Query::deserialize(buf).map(Self::Query),
            RequestOpcode::Execute => Execute::deserialize(buf).map(Self::Execute),
            RequestOpcode::Batch => Batch::deserialize(buf).map(Self::Batch),
            _ => unimplemented!(
                "Deserialization of opcode {:?} is not yet supported",
                opcode
            ),
        }
    }

    /// Retrieves consistency from request frame, if present.
    pub fn get_consistency(&self) -> Option<Consistency> {
        match self {
            Request::Query(q) => Some(q.parameters.consistency),
            Request::Execute(e) => Some(e.parameters.consistency),
            Request::Batch(b) => Some(b.consistency),
            #[expect(unreachable_patterns)] // until other opcodes are supported
            _ => None,
        }
    }

    /// Retrieves serial consistency from request frame.
    pub fn get_serial_consistency(&self) -> Option<Option<SerialConsistency>> {
        match self {
            Request::Query(q) => Some(q.parameters.serial_consistency),
            Request::Execute(e) => Some(e.parameters.serial_consistency),
            Request::Batch(b) => Some(b.serial_consistency),
            #[expect(unreachable_patterns)] // until other opcodes are supported
            _ => None,
        }
    }
}

/// A CQL request that can be sent to the server.
#[non_exhaustive] // TODO: add remaining request types
pub enum RequestV2<'r> {
    /// QUERY request, used to execute a single unprepared statement.
    Query(Query<'r>),
    /// EXECUTE request, used to execute a single prepared statement.
    Execute(ExecuteV2<'r>),
    /// BATCH request, used to execute a batch of (prepared, unprepared, or mix of both)
    /// statements.
    Batch(Batch<'r, BatchStatement<'r>, Vec<SerializedValues>>),
}

impl RequestV2<'_> {
    /// Deserializes the request from the provided buffer.
    pub fn deserialize(
        buf: &mut &[u8],
        opcode: RequestOpcode,
        features: &ProtocolFeatures,
    ) -> Result<Self, RequestDeserializationError> {
        match opcode {
            RequestOpcode::Query => {
                Query::deserialize_with_features(buf, features).map(Self::Query)
            }
            RequestOpcode::Execute => {
                ExecuteV2::deserialize_with_features(buf, features).map(Self::Execute)
            }
            RequestOpcode::Batch => {
                Batch::deserialize_with_features(buf, features).map(Self::Batch)
            }
            _ => unimplemented!(
                "Deserialization of opcode {:?} is not yet supported",
                opcode
            ),
        }
    }

    /// Retrieves consistency from request frame, if present.
    pub fn get_consistency(&self) -> Option<Consistency> {
        match self {
            Self::Query(q) => Some(q.parameters.consistency),
            Self::Execute(e) => Some(e.parameters.consistency),
            Self::Batch(b) => Some(b.consistency),
            #[expect(unreachable_patterns)] // until other opcodes are supported
            _ => None,
        }
    }

    /// Retrieves serial consistency from request frame.
    pub fn get_serial_consistency(&self) -> Option<Option<SerialConsistency>> {
        match self {
            Self::Query(q) => Some(q.parameters.serial_consistency),
            Self::Execute(e) => Some(e.parameters.serial_consistency),
            Self::Batch(b) => Some(b.serial_consistency),
            #[expect(unreachable_patterns)] // until other opcodes are supported
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{borrow::Cow, ops::Deref};

    use bytes::Bytes;

    use super::query::PagingState;
    use crate::Consistency;
    use crate::frame::protocol_features::ProtocolFeatures;
    use crate::frame::request::batch::{Batch, BatchStatement, BatchType};
    #[expect(deprecated)]
    use crate::frame::request::execute::Execute;
    use crate::frame::request::execute::ExecuteV2;
    use crate::frame::request::query::{Query, QueryParameters};
    use crate::frame::request::{DeserializableRequest, SerializableRequest};
    use crate::frame::response::result::{ColumnType, NativeType};
    use crate::frame::types::{self, SerialConsistency};
    use crate::serialize::row::SerializedValues;

    #[test]
    fn request_ser_de_identity() {
        // Query
        let contents = Cow::Borrowed("SELECT host_id from system.peers");
        let parameters = QueryParameters {
            consistency: Consistency::All,
            serial_consistency: Some(SerialConsistency::Serial),
            timestamp: None,
            page_size: Some(323),
            paging_state: PagingState::new_from_raw_bytes(&[2_u8, 1, 3, 7] as &[u8]),
            skip_metadata: false,
            values: {
                let mut vals = SerializedValues::new();
                vals.add_value(&2137, &ColumnType::Native(NativeType::Int))
                    .unwrap();
                Cow::Owned(vals)
            },
        };
        let query = Query {
            contents,
            parameters,
        };

        {
            let mut buf = Vec::new();
            query.serialize(&mut buf).unwrap();

            let query_deserialized =
                Query::deserialize_with_features(&mut &buf[..], &Default::default()).unwrap();
            assert_eq!(&query_deserialized, &query);
        }

        // Legacy Execute
        let id: Bytes = vec![2, 4, 5, 2, 6, 7, 3, 1].into();
        let parameters = QueryParameters {
            consistency: Consistency::Any,
            serial_consistency: None,
            timestamp: Some(3423434),
            page_size: None,
            paging_state: PagingState::start(),
            skip_metadata: false,
            values: {
                let mut vals = SerializedValues::new();
                vals.add_value(&42, &ColumnType::Native(NativeType::Int))
                    .unwrap();
                vals.add_value(&2137, &ColumnType::Native(NativeType::Int))
                    .unwrap();
                Cow::Owned(vals)
            },
        };

        #[expect(deprecated)]
        let execute = Execute {
            id,
            parameters: parameters.clone(),
        };
        {
            let mut buf = Vec::new();
            execute.serialize(&mut buf).unwrap();

            #[expect(deprecated)]
            let execute_deserialized =
                Execute::deserialize_with_features(&mut &buf[..], &Default::default()).unwrap();
            assert_eq!(&execute_deserialized, &execute);
        }

        // New Execute
        let id = [2, 4, 5, 2, 6, 7, 3, 1].as_slice().into();
        let result_metadata_id = Some([2, 4, 5, 2, 6, 7, 3, 1].as_slice().into());
        let execute_with_id = ExecuteV2 {
            id,
            result_metadata_id,
            parameters,
        };
        {
            let mut buf = Vec::new();
            execute_with_id.serialize(&mut buf).unwrap();

            let features = ProtocolFeatures {
                scylla_metadata_id_supported: true,
                ..Default::default()
            };
            let execute_deserialized =
                ExecuteV2::deserialize_with_features(&mut &buf[..], &features).unwrap();
            assert_eq!(&execute_deserialized, &execute_with_id);
        }

        // Batch
        let statements = vec![
            BatchStatement::Query {
                text: query.contents,
            },
            BatchStatement::Prepared {
                id: Cow::Borrowed(execute_with_id.id.as_ref()),
            },
        ];
        let batch = Batch {
            statements: Cow::Owned(statements),
            batch_type: BatchType::Logged,
            consistency: Consistency::EachQuorum,
            serial_consistency: Some(SerialConsistency::LocalSerial),
            timestamp: Some(32432),

            // Not execute's values, because named values are not supported in batches.
            values: vec![
                query.parameters.values.deref().clone(),
                query.parameters.values.deref().clone(),
            ],
        };
        {
            let mut buf = Vec::new();
            batch.serialize(&mut buf).unwrap();

            let batch_deserialized =
                Batch::deserialize_with_features(&mut &buf[..], &Default::default()).unwrap();
            assert_eq!(&batch_deserialized, &batch);
        }
    }

    #[test]
    fn deser_rejects_unknown_flags() {
        // Query
        let contents = Cow::Borrowed("SELECT host_id from system.peers");
        let parameters = QueryParameters {
            consistency: Default::default(),
            serial_consistency: Some(SerialConsistency::LocalSerial),
            timestamp: None,
            page_size: None,
            paging_state: PagingState::start(),
            skip_metadata: false,
            values: Cow::Borrowed(SerializedValues::EMPTY),
        };
        let query = Query {
            contents: contents.clone(),
            parameters,
        };

        {
            let mut buf = Vec::new();
            query.serialize(&mut buf).unwrap();

            // Sanity check: query deserializes to the equivalent.
            let query_deserialized =
                Query::deserialize_with_features(&mut &buf[..], &Default::default()).unwrap();
            assert_eq!(&query_deserialized.contents, &query.contents);
            assert_eq!(&query_deserialized.parameters, &query.parameters);

            // Now modify flags by adding an unknown one.
            // Find flags in buffer:
            let mut buf_ptr = buf.as_slice();
            let serialised_contents = types::read_long_string(&mut buf_ptr).unwrap();
            assert_eq!(serialised_contents, contents);

            // Now buf_ptr points at consistency.
            let consistency = types::read_consistency(&mut buf_ptr).unwrap();
            assert_eq!(consistency, Consistency::default());

            // Now buf_ptr points at flags, but it is immutable. Get mutable reference into the buffer.
            let flags_idx = buf.len() - buf_ptr.len();
            let flags_mut = &mut buf[flags_idx];

            // This assumes that the following flag is unknown, which is true at the time of writing this test.
            *flags_mut |= 0x80;

            // Unknown flag should lead to frame rejection, as unknown flags can be new protocol extensions
            // leading to different semantics.
            let _parse_error =
                Query::deserialize_with_features(&mut &buf[..], &Default::default()).unwrap_err();
        }

        // Batch
        let statements = vec![BatchStatement::Query {
            text: query.contents,
        }];
        let batch = Batch {
            statements: Cow::Owned(statements),
            batch_type: BatchType::Logged,
            consistency: Consistency::EachQuorum,
            serial_consistency: None,
            timestamp: None,

            values: vec![query.parameters.values.deref().clone()],
        };
        {
            let mut buf = Vec::new();
            batch.serialize(&mut buf).unwrap();

            // Sanity check: batch deserializes to the equivalent.
            let batch_deserialized =
                Batch::deserialize_with_features(&mut &buf[..], &Default::default()).unwrap();
            assert_eq!(batch, batch_deserialized);

            // Now modify flags by adding an unknown one.
            // There are no timestamp nor serial consistency, so flags are the last byte in the buf.
            let buf_len = buf.len();
            let flags_mut = &mut buf[buf_len - 1];
            // This assumes that the following flag is unknown, which is true at the time of writing this test.
            *flags_mut |= 0x80;

            // Unknown flag should lead to frame rejection, as unknown flags can be new protocol extensions
            // leading to different semantics.
            let _parse_error =
                Batch::deserialize_with_features(&mut &buf[..], &Default::default()).unwrap_err();
        }
    }
}
