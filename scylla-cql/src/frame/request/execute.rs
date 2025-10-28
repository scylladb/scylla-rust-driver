//! CQL protocol-level representation of a `EXECUTE` request.

use std::num::TryFromIntError;

use crate::frame::frame_errors::CqlRequestSerializationError;
use crate::frame::protocol_features::ProtocolFeatures;
use crate::frame::response::result::cow_bytes::CowBytes;
use bytes::Bytes;
use thiserror::Error;

use crate::{
    frame::request::{RequestOpcode, SerializableRequest, query},
    frame::types,
};

use super::{
    DeserializableRequest, RequestDeserializationError,
    query::{QueryParameters, QueryParametersSerializationError},
};

/// CQL protocol-level representation of an `EXECUTE` request,
/// used to execute a single prepared statement.
#[cfg_attr(test, derive(Debug, PartialEq, Eq))]
pub struct Execute<'a> {
    /// ID of the prepared statement to execute.
    pub id: Bytes,

    /// Various parameters controlling the execution of the statement.
    pub parameters: query::QueryParameters<'a>,
}

impl SerializableRequest for Execute<'_> {
    const OPCODE: RequestOpcode = RequestOpcode::Execute;

    fn serialize(&self, buf: &mut Vec<u8>) -> Result<(), CqlRequestSerializationError> {
        // Serializing statement id
        types::write_short_bytes(&self.id[..], buf)
            .map_err(ExecuteSerializationError::StatementIdSerialization)?;

        // Serializing params
        self.parameters
            .serialize(buf)
            .map_err(ExecuteSerializationError::QueryParametersSerialization)?;
        Ok(())
    }
}

impl DeserializableRequest for Execute<'_> {
    fn deserialize(buf: &mut &[u8]) -> Result<Self, RequestDeserializationError> {
        let id = types::read_short_bytes(buf)?.to_vec().into();
        let parameters = QueryParameters::deserialize(buf)?;

        Ok(Self { id, parameters })
    }
}

/// CQL protocol-level representation of an `EXECUTE` request,
/// used to execute a single prepared statement.
#[cfg_attr(test, derive(Debug, PartialEq, Eq))]
pub struct ExecuteV2<'a> {
    /// ID of the prepared statement to execute.
    pub id: CowBytes<'a>,

    /// ID of the result metadata stored locally.
    pub result_metadata_id: Option<CowBytes<'a>>,

    /// Various parameters controlling the execution of the statement.
    pub parameters: query::QueryParameters<'a>,
}

impl SerializableRequest for ExecuteV2<'_> {
    const OPCODE: RequestOpcode = RequestOpcode::Execute;

    fn serialize(&self, buf: &mut Vec<u8>) -> Result<(), CqlRequestSerializationError> {
        // Serializing statement id
        types::write_short_bytes(self.id.as_ref(), buf)
            .map_err(ExecuteSerializationError::StatementIdSerialization)?;

        // Serializing result metadata id
        if let Some(id) = self.result_metadata_id.as_ref() {
            types::write_short_bytes(id.as_ref(), buf)
                .map_err(ExecuteSerializationError::ResultMetadataIdSerialization)?;
        }

        // Serializing params
        self.parameters
            .serialize(buf)
            .map_err(ExecuteSerializationError::QueryParametersSerialization)?;
        Ok(())
    }
}

impl DeserializableRequest for ExecuteV2<'static> {
    fn deserialize(buf: &mut &[u8]) -> Result<Self, RequestDeserializationError> {
        let id = CowBytes::from(types::read_short_bytes(buf)?).into_owned();
        let result_metadata_id = CowBytes::from(types::read_short_bytes(buf)?).into_owned();
        let parameters = QueryParameters::deserialize(buf)?;

        Ok(Self {
            id,
            result_metadata_id: Some(result_metadata_id),
            parameters,
        })
    }

    fn deserialize_with_features(
        buf: &mut &[u8],
        features: &ProtocolFeatures,
    ) -> Result<Self, RequestDeserializationError> {
        let id = CowBytes::from(types::read_short_bytes(buf)?).into_owned();
        let result_metadata_id = if features.scylla_metadata_id_supported {
            Some(CowBytes::from(types::read_short_bytes(buf)?).into_owned())
        } else {
            None
        };
        let parameters = QueryParameters::deserialize(buf)?;

        Ok(Self {
            id,
            result_metadata_id,
            parameters,
        })
    }
}

/// An error type returned when serialization of EXECUTE request fails.
// TODO(2.0): Remove "Serialization" suffix from error names.
#[expect(clippy::enum_variant_names)]
#[non_exhaustive]
#[derive(Error, Debug, Clone)]
pub enum ExecuteSerializationError {
    /// Failed to serialize query parameters.
    #[error("Malformed query parameters: {0}")]
    QueryParametersSerialization(QueryParametersSerializationError),

    /// Failed to serialize prepared statement id.
    #[error("Malformed statement id: {0}")]
    StatementIdSerialization(TryFromIntError),

    #[error("Malformed result metadata id: {0}")]
    ResultMetadataIdSerialization(TryFromIntError),
}
