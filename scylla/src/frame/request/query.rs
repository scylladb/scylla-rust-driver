use anyhow::Result;
use bytes::BufMut;

use crate::{
    frame::{request::Request, Opcode},
    types,
};

pub struct Query {
    pub contents: String,
    // TODO: All remaining parameters
}

impl Request for Query {
    const OPCODE: Opcode = Opcode::Query;

    fn serialize(&self, buf: &mut impl BufMut) -> Result<()> {
        types::write_long_string(&self.contents, buf)?;
        types::write_short(0, buf); // Dummy consistency
        buf.put_u8(0); // Flags
        Ok(())
    }
}
