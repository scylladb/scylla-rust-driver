use std::num::TryFromIntError;

use thiserror::Error;

use crate::frame::frame_errors::ParseError;

use crate::frame::request::{RequestOpcode, SerializableRequest};
use crate::frame::types::write_bytes_opt;

// Implements Authenticate Response
pub struct AuthResponse {
    pub response: Option<Vec<u8>>,
}

impl SerializableRequest for AuthResponse {
    const OPCODE: RequestOpcode = RequestOpcode::AuthResponse;

    fn serialize(&self, buf: &mut Vec<u8>) -> Result<(), ParseError> {
        Ok(write_bytes_opt(self.response.as_ref(), buf)
            .map_err(AuthResponseSerializationError::ResponseSerialization)?)
    }
}

#[non_exhaustive]
#[derive(Error, Debug, Clone)]
pub enum AuthResponseSerializationError {
    // More context is provided by wrapping error type.
    #[error(transparent)]
    ResponseSerialization(TryFromIntError),
}
