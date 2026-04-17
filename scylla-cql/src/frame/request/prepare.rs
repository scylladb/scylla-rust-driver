//! CQL protocol-level representation of a `PREPARE` request.

use crate::frame::frame_errors::CqlRequestSerializationError;

use crate::{
    frame::request::{RequestOpcode, SerializableRequest},
    frame::types,
};

// Re-export for backward compatibility.
pub use crate::frame::frame_errors::PrepareSerializationError;

/// CQL protocol-level representation of an `PREPARE` request,
/// used to prepare a single statement for further execution.
pub struct Prepare<'a> {
    /// CQL statement string to prepare.
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
