pub mod query;
pub mod startup;

use anyhow::Result;
use bytes::BufMut;
use num_enum::TryFromPrimitive;

pub use query::Query;
pub use startup::Startup;

#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord, TryFromPrimitive)]
#[repr(u8)]
pub enum RequestOpcode {
    Startup = 0x01,
    Query = 0x07,
}

pub trait Request {
    const OPCODE: RequestOpcode;

    fn serialize(&self, buf: &mut impl BufMut) -> Result<()>;
}
