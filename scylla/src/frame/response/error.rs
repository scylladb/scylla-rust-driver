use anyhow::Result;

use crate::frame::types;

#[derive(Debug)]
pub struct Error {
    pub code: i32,
    pub reason: String,
}

impl Error {
    pub fn deserialize(buf: &mut &[u8]) -> Result<Self> {
        let code = types::read_int(buf)?;
        let reason = types::read_string(buf)?.to_owned();

        Ok(Error { code, reason })
    }
}

impl Into<anyhow::Error> for Error {
    fn into(self) -> anyhow::Error {
        anyhow!("Error (code {}): {}", self.code, self.reason)
    }
}
