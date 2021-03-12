pub mod auth_response;
pub mod batch;
pub mod execute;
pub mod options;
pub mod prepare;
pub mod query;
pub mod startup;

use crate::frame::frame_errors::ParseError;
use bytes::{BufMut, Bytes};
use num_enum::TryFromPrimitive;

pub use auth_response::AuthResponse;
pub use batch::Batch;
pub use options::Options;
pub use prepare::Prepare;
pub use query::Query;
pub use startup::Startup;

#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord, TryFromPrimitive)]
#[repr(u8)]
pub enum RequestOpcode {
    Startup = 0x01,
    Options = 0x05,
    Query = 0x07,
    Prepare = 0x09,
    Execute = 0x0A,
    Register = 0x0B,
    Batch = 0x0D,
    AuthResponse = 0x0F,
}

pub trait Request {
    const OPCODE: RequestOpcode;

    fn serialize(&self, buf: &mut impl BufMut) -> Result<(), ParseError>;

    fn to_bytes(&self) -> Result<Bytes, ParseError> {
        let mut v = Vec::new();
        self.serialize(&mut v)?;
        Ok(v.into())
    }
}
