//! CQL protocol-level representation of a `AUTH_RESPONSE` request.

use std::num::TryFromIntError;

use thiserror::Error;

use crate::frame::frame_errors::CqlRequestSerializationError;

use crate::frame::request::{RequestOpcode, SerializableRequest};
use crate::frame::types::write_bytes_opt;

/// Represents AUTH_RESPONSE CQL request.
///
/// This request is sent by the client to respond to an authentication challenge.
pub struct AuthResponse {
    /// Raw response bytes.
    pub response: Option<Vec<u8>>,
}

impl SerializableRequest for AuthResponse {
    const OPCODE: RequestOpcode = RequestOpcode::AuthResponse;

    fn serialize(&self, buf: &mut Vec<u8>) -> Result<(), CqlRequestSerializationError> {
        Ok(write_bytes_opt(self.response.as_ref(), buf)
            .map_err(AuthResponseSerializationError::ResponseSerialization)?)
    }
}

/// An error type returned when serialization of AUTH_RESPONSE request fails.
#[non_exhaustive]
#[derive(Error, Debug, Clone)]
pub enum AuthResponseSerializationError {
    /// Maximum response's body length exceeded.
    #[error("AUTH_RESPONSE body bytes length too big: {0}")]
    ResponseSerialization(TryFromIntError),
}
