use anyhow::Result;
use bytes::{BufMut, Bytes};

use crate::{
    frame::request::{Request, RequestOpcode},
    frame::types,
    frame::value::Value,
};

pub struct Batch<'a> {
    pub statements: Vec<BatchStatementWithValues<'a>>,
    pub batch_type: BatchType,
    pub consistency: i16,
}

pub struct BatchStatementWithValues<'a> {
    pub statement: BatchStatement,
    pub values: &'a [Value],
}

pub enum BatchStatement {
    QueryContents(String),
    PreparedStatementID(Bytes),
}

#[derive(Clone, Copy)]
pub enum BatchType {
    Logged = 0,
    Unlogged = 1,
    Counter = 2,
}

impl Request for Batch<'_> {
    const OPCODE: RequestOpcode = RequestOpcode::Batch;

    fn serialize(&self, buf: &mut impl BufMut) -> Result<()> {
        // Serializing type of batch
        buf.put_u8(self.batch_type as u8);

        // Serializing queries
        types::write_short(self.statements.len() as i16, buf);
        for statement in &self.statements {
            statement.serialize(buf)?;
        }

        // Serializing consistency
        types::write_short(self.consistency, buf);

        // Serializing flags
        // FIXME: consider other flag values than 0
        buf.put_u8(0);

        Ok(())
    }
}

impl Default for Batch<'_> {
    fn default() -> Self {
        Self {
            statements: Vec::new(),
            batch_type: BatchType::Logged,
            consistency: 1,
        }
    }
}

impl BatchStatement {
    fn serialize(&self, buf: &mut impl BufMut) -> Result<()> {
        match self {
            BatchStatement::QueryContents(s) => {
                buf.put_u8(0);
                types::write_long_string(s, buf)?;
            }
            BatchStatement::PreparedStatementID(id) => {
                buf.put_u8(1);
                types::write_short_bytes(&id[..], buf)?;
            }
        }

        Ok(())
    }
}

impl BatchStatementWithValues<'_> {
    fn serialize(&self, buf: &mut impl BufMut) -> Result<()> {
        self.statement.serialize(buf)?;
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
