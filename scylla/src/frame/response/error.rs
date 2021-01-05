use crate::frame::frame_errors::ParseError;
use crate::frame::types;
use crate::transport::errors::{DBError, QueryError};

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

impl Into<QueryError> for Error {
    fn into(self) -> QueryError {
        QueryError::DBError(DBError::ErrorMsg(self.code, self.reason))
    }
}
