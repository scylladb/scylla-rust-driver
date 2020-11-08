use anyhow::Result;
use bytes::{BufMut, Bytes};

use crate::{
    frame::request::{Request, RequestOpcode},
    frame::types,
    frame::value::Value,
};

pub enum BatchedQuery {
    QueryContents(String),
    PreparedStatementID(Bytes),
}

impl BatchedQuery {
    pub fn serialize(&self, buf: &mut impl BufMut) -> Result<()> {
        match self {
            BatchedQuery::QueryContents(s) => {
                buf.put_u8(0);
                types::write_long_string(s, buf)?;
            }
            BatchedQuery::PreparedStatementID(id) => {
                buf.put_u8(1);
                types::write_short_bytes(&id[..], buf)?;
            }
        }
        Ok(())
    }
}

pub struct BatchedQueryWithValues<'a> {
    pub query: BatchedQuery,
    pub values: &'a [Value],
}

impl BatchedQueryWithValues<'_> {
    pub fn serialize(&self, buf: &mut impl BufMut) -> Result<()> {
        self.query.serialize(buf)?;
        types::write_short(self.values.len() as i16, buf);

        for value in self.values {
            match value {
                Value::Val(v) => {
                    types::write_int(v.len() as i32, buf);
                    buf.put_slice(&v[..]);
                }
                Value::Null => types::write_int(-1, buf),
                Value::NotSet => types::write_int(-2, buf),
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy)]
#[repr(u8)]
pub enum BatchType {
    Logged = 0,
    Unlogged = 1,
    Counter = 2,
}

pub struct Batch<'a> {
    pub queries: Vec<BatchedQueryWithValues<'a>>,
    pub batch_type: BatchType,
    pub consistency: i16,
}

impl Request for Batch<'_> {
    const OPCODE: RequestOpcode = RequestOpcode::Batch;

    fn serialize(&self, buf: &mut impl BufMut) -> Result<()> {
        // Serializing type of batch
        buf.put_u8(self.batch_type as u8);

        // Serializing queries
        types::write_short(self.queries.len() as i16, buf);
        for query in &self.queries {
            query.serialize(buf)?;
        }

        // Serializing consistency
        types::write_short(self.consistency, buf);

        // Serializing flags
        // FIXME: consider other flag values than 0
        buf.put_u8(0);

        Ok(())
    }
}
