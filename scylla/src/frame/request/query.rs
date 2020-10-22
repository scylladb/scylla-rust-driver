use anyhow::Result;
use bytes::BufMut;

use crate::{
    frame::request::{Request, RequestOpcode},
    frame::types,
    query,
};

pub struct Query {
    pub contents: String,
    // TODO: All remaining parameters
}

impl Request for Query {
    const OPCODE: RequestOpcode = RequestOpcode::Query;

    fn serialize(&self, buf: &mut impl BufMut) -> Result<()> {
        types::write_long_string(&self.contents, buf)?;
        types::write_short(1, buf); // consistency ONE
        buf.put_u8(0); // Flags
        Ok(())
    }
}

impl From<&query::Query> for Query {
    fn from(q: &query::Query) -> Query {
        Query { contents: q.get_contents().to_string() }
    }
}
