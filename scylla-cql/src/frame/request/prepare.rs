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

impl<'a> SerializableRequest for Prepare<'a> {
    const OPCODE: RequestOpcode = RequestOpcode::Prepare;

    fn serialize(&self, buf: &mut Vec<u8>) -> Result<(), CqlRequestSerializationError> {
        types::write_long_string(self.query, buf)
            .map_err(PrepareSerializationError::StatementStringSerialization)?;
        Ok(())
    }
}

#[non_exhaustive]
#[derive(Error, Debug, Clone)]
pub enum PrepareSerializationError {
    #[error("Failed to serialize statement contents: {0}")]
    StatementStringSerialization(TryFromIntError),
}
