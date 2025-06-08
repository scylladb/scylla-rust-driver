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

use crate::serialize::row::SerializedValues;
use crate::Consistency;
use bytes::Bytes;

pub use auth_response::AuthResponse;
pub use batch::Batch;
pub use batch::BatchV2;
pub use execute::Execute;
pub use options::Options;
pub use prepare::Prepare;
pub use query::Query;
pub use startup::Startup;

use self::batch::BatchStatement;

use super::frame_errors::{CqlRequestSerializationError, LowLevelDeserializationError};
use super::types::SerialConsistency;
use super::TryFromPrimitiveError;

/// Possible requests sent by the client.
#[derive(Debug, Copy, Clone)]
#[non_exhaustive]
pub enum CqlRequestKind {
    Startup,
    AuthResponse,
    Options,
    Query,
    Prepare,
    Execute,
    Batch,
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

#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord)]
#[repr(u8)]
pub enum RequestOpcode {
    Startup = 0x01,
    Options = 0x05,
    Query = 0x07,
    Prepare = 0x09,
    Execute = 0x0A,
    Register = 0x0B,
    Batch = 0x0D,
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

pub trait SerializableRequest {
    const OPCODE: RequestOpcode;

    fn serialize(&self, buf: &mut Vec<u8>) -> Result<(), CqlRequestSerializationError>;

    fn to_bytes(&self) -> Result<Bytes, CqlRequestSerializationError> {
        let mut v = Vec::new();
        self.serialize(&mut v)?;
        Ok(v.into())
    }
}

/// Not intended for driver's direct usage (as driver has no interest in deserialising CQL requests),
/// but very useful for testing (e.g. asserting that the sent requests have proper parameters set).
pub trait DeserializableRequest: SerializableRequest + Sized {
    fn deserialize(buf: &mut &[u8]) -> Result<Self, RequestDeserializationError>;
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

#[non_exhaustive] // TODO: add remaining request types
pub enum Request<'r> {
    Query(Query<'r>),
    Execute(Execute<'r>),
    Batch(Batch<'r, BatchStatement<'r>, Vec<SerializedValues>>),
    BatchV2(BatchV2<'r>),
}

impl Request<'_> {
    pub fn deserialize(
        buf: &mut &[u8],
        opcode: RequestOpcode,
    ) -> Result<Self, RequestDeserializationError> {
        match opcode {
            RequestOpcode::Query => Query::deserialize(buf).map(Self::Query),
            RequestOpcode::Execute => Execute::deserialize(buf).map(Self::Execute),
            RequestOpcode::Batch => BatchV2::deserialize(buf).map(Self::BatchV2),
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
            Request::BatchV2(b) => Some(b.consistency),
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
            Request::BatchV2(b) => Some(b.serial_consistency),
            #[expect(unreachable_patterns)] // until other opcodes are supported
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::borrow::Cow;

    use bytes::Bytes;

    use crate::serialize::row::SerializedValues;
    use crate::{
        frame::{
            request::{
                batch::{BatchStatement, BatchType, BatchV2},
                execute::Execute,
                query::{Query, QueryParameters},
                DeserializableRequest, SerializableRequest,
            },
            response::result::{ColumnType, NativeType},
            types::{self, SerialConsistency},
        },
        Consistency,
    };

    use super::query::PagingState;

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

            let query_deserialized = Query::deserialize(&mut &buf[..]).unwrap();
            assert_eq!(&query_deserialized, &query);
        }

        // Execute
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
        let execute = Execute { id, parameters };
        {
            let mut buf = Vec::new();
            execute.serialize(&mut buf).unwrap();

            let execute_deserialized = Execute::deserialize(&mut &buf[..]).unwrap();
            assert_eq!(&execute_deserialized, &execute);
        }

        // Batch
        // Not execute's values, because named values are not supported in batches.
        let mut statements_and_values = vec![];
        BatchStatement::Query {
            text: query.contents,
        }
        .serialize(&mut statements_and_values)
        .unwrap();
        statements_and_values
            .extend_from_slice(&query.parameters.values.element_count().to_be_bytes());
        statements_and_values.extend_from_slice(query.parameters.values.get_contents());

        BatchStatement::Prepared {
            id: Cow::Borrowed(&execute.id),
        }
        .serialize(&mut statements_and_values)
        .unwrap();
        statements_and_values
            .extend_from_slice(&query.parameters.values.element_count().to_be_bytes());
        statements_and_values.extend_from_slice(query.parameters.values.get_contents());

        let batch = BatchV2 {
            statements_and_values: Cow::Owned(statements_and_values),
            batch_type: BatchType::Logged,
            consistency: Consistency::EachQuorum,
            serial_consistency: Some(SerialConsistency::LocalSerial),
            timestamp: Some(32432),
            statements_len: 2,
        };
        {
            let mut buf = Vec::new();
            batch.serialize(&mut buf).unwrap();

            let batch_deserialized = BatchV2::deserialize(&mut &buf[..]).unwrap();
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
            let query_deserialized = Query::deserialize(&mut &buf[..]).unwrap();
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
            let _parse_error = Query::deserialize(&mut &buf[..]).unwrap_err();
        }

        // Batch
        let mut statements_and_values = vec![];
        BatchStatement::Query {
            text: query.contents,
        }
        .serialize(&mut statements_and_values)
        .unwrap();
        statements_and_values
            .extend_from_slice(&query.parameters.values.element_count().to_be_bytes());
        statements_and_values.extend_from_slice(query.parameters.values.get_contents());

        let batch = BatchV2 {
            batch_type: BatchType::Logged,
            consistency: Consistency::EachQuorum,
            serial_consistency: None,
            timestamp: None,
            statements_and_values: Cow::Owned(statements_and_values),
            statements_len: 1,
        };
        {
            let mut buf = Vec::new();
            batch.serialize(&mut buf).unwrap();

            // Sanity check: batch deserializes to the equivalent.
            let batch_deserialized = BatchV2::deserialize(&mut &buf[..]).unwrap();
            assert_eq!(batch, batch_deserialized);

            // Now modify flags by adding an unknown one.
            // There are no timestamp nor serial consistency, so flags are the last byte in the buf.
            let buf_len = buf.len();
            let flags_mut = &mut buf[buf_len - 1];
            // This assumes that the following flag is unknown, which is true at the time of writing this test.
            *flags_mut |= 0x80;

            // Unknown flag should lead to frame rejection, as unknown flags can be new protocol extensions
            // leading to different semantics.
            let _parse_error = BatchV2::deserialize(&mut &buf[..]).unwrap_err();
        }
    }
}
