pub mod query;
pub mod startup;

use anyhow::Result;
use bytes::BufMut;

use crate::frame::Opcode;

pub use query::Query;
pub use startup::Startup;

pub trait Request {
    const OPCODE: Opcode;

    fn serialize(&self, buf: &mut impl BufMut) -> Result<()>;
}
