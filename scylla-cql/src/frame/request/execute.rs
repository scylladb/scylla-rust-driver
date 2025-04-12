use std::num::TryFromIntError;

use crate::frame::frame_errors::CqlRequestSerializationError;
use bytes::Bytes;
use thiserror::Error;

use crate::{
    frame::request::{query, RequestOpcode, SerializableRequest},
    frame::types,
};

use super::{
    query::{QueryParameters, QueryParametersSerializationError},
    DeserializableRequest, RequestDeserializationError,
};

#[cfg_attr(test, derive(Debug, PartialEq, Eq))]
pub struct Execute<'a> {
    pub id: Bytes,
    pub parameters: query::QueryParameters<'a>,
}

impl SerializableRequest for Execute<'_> {
    const OPCODE: RequestOpcode = RequestOpcode::Execute;

    #[inline]
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
    #[inline]
    fn deserialize(buf: &mut &[u8]) -> Result<Self, RequestDeserializationError> {
        let id = types::read_short_bytes(buf)?.to_vec().into();
        let parameters = QueryParameters::deserialize(buf)?;

        Ok(Self { id, parameters })
    }
}

/// An error type returned when serialization of EXECUTE request fails.
#[non_exhaustive]
#[derive(Error, Debug, Clone)]
pub enum ExecuteSerializationError {
    /// Failed to serialize query parameters.
    #[error("Malformed query parameters: {0}")]
    QueryParametersSerialization(QueryParametersSerializationError),

    /// Failed to serialize prepared statement id.
    #[error("Malformed statement id: {0}")]
    StatementIdSerialization(TryFromIntError),
}
