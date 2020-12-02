use crate::frame::frame_errors::ParseError;
use crate::frame::types;
use crate::transport::transport_errors::{DBError, TransportError};

#[derive(Debug)]
pub struct Error {
    pub code: i32,
    pub reason: String,
}

impl Error {
    pub fn deserialize(buf: &mut &[u8]) -> Result<Self, ParseError> {
        let code = types::read_int(buf)?;
        let reason = types::read_string(buf)?.to_owned();

        Ok(Error { code, reason })
    }
}

impl Into<TransportError> for Error {
    fn into(self) -> TransportError {
        TransportError::DBError(DBError::ErrorMsg(self.code, self.reason))
    }
}
