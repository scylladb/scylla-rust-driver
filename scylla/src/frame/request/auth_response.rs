use crate::frame::frame_errors::ParseError;
use bytes::BufMut;
use std::convert::TryInto;

use crate::frame::request::{Request, RequestOpcode};
use crate::transport::Authenticator;

// Implements Authenticate Response
pub struct AuthResponse {
    pub username: Option<String>,
    pub password: Option<String>,
    pub authenticator: Authenticator,
}

impl Request for AuthResponse {
    const OPCODE: RequestOpcode = RequestOpcode::AuthResponse;

    fn serialize(&self, buf: &mut impl BufMut) -> Result<(), ParseError> {
        if self.username.is_none() || self.password.is_none() {
            return Err(ParseError::BadData(
                "Bad credentials given - username and password shouldn't be none".to_string(),
            ));
        }
        let username_as_bytes = self.username.as_ref().unwrap().as_bytes();
        let password_as_bytes = self.password.as_ref().unwrap().as_bytes();

        // The body of AuthResponse is a single [bytes] value (i32 length and then contents)
        let buf_size = 2 + username_as_bytes.len() + password_as_bytes.len();
        buf.put_i32(buf_size.try_into()?);

        buf.put_u8(0);
        buf.put_slice(username_as_bytes);
        buf.put_u8(0);
        buf.put_slice(password_as_bytes);
        Ok(())
    }
}
