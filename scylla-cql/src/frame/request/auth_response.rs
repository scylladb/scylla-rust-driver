use crate::frame::frame_errors::ParseError;
use bytes::BufMut;

use crate::frame::request::{RequestOpcode, SerializableRequest};
use crate::frame::types::write_bytes_opt;

// Implements Authenticate Response
pub struct AuthResponse {
    pub response: Option<Vec<u8>>,
}

impl SerializableRequest for AuthResponse {
    const OPCODE: RequestOpcode = RequestOpcode::AuthResponse;

    fn serialize(&self, buf: &mut impl BufMut) -> Result<(), ParseError> {
        write_bytes_opt(self.response.as_ref(), buf)
    }
}
