use std::num::TryFromIntError;

use thiserror::Error;

use crate::frame::frame_errors::CqlRequestSerializationError;

use crate::{
    frame::request::{RequestOpcode, SerializableRequest},
    frame::types,
};

pub struct Prepare<'a> {
    pub query: &'a str,
}

impl SerializableRequest for Prepare<'_> {
    const OPCODE: RequestOpcode = RequestOpcode::Prepare;

    fn serialize(&self, buf: &mut Vec<u8>) -> Result<(), CqlRequestSerializationError> {
        types::write_long_string(self.query, buf)
            .map_err(PrepareSerializationError::StatementStringSerialization)?;
        Ok(())
    }
}

/// An error type returned when serialization of PREPARE request fails.
#[non_exhaustive]
#[derive(Error, Debug, Clone)]
pub enum PrepareSerializationError {
    /// Failed to serialize the CQL statement string.
    #[error("Failed to serialize statement contents: {0}")]
    StatementStringSerialization(TryFromIntError),
}
