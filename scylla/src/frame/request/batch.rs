use anyhow::Result;
use bytes::{BufMut, Bytes};

use crate::frame::{
    request::{Request, RequestOpcode},
    types,
    value::Value,
};

pub struct Batch<'a, 'b, I: Iterator<Item = BatchStatementWithValues<'a, 'b>> + Clone> {
    pub statements: I,
    pub num_of_statements: usize,
    pub batch_type: BatchType,
    pub consistency: i16,
}

pub struct BatchStatementWithValues<'a, 'b> {
    pub statement: BatchStatement<'b>,
    pub values: &'a [Value],
}

pub enum BatchStatement<'a> {
    QueryContents(&'a str),
    PreparedStatementID(&'a Bytes),
}

/// The type of a batch.
#[derive(Clone, Copy)]
pub enum BatchType {
    Logged = 0,
    Unlogged = 1,
    Counter = 2,
}

impl<'a, 'b, I: Iterator<Item = BatchStatementWithValues<'a, 'b>> + Clone> Request
    for Batch<'a, 'b, I>
{
    const OPCODE: RequestOpcode = RequestOpcode::Batch;

    fn serialize(&self, buf: &mut impl BufMut) -> Result<()> {
        // Serializing type of batch
        buf.put_u8(self.batch_type as u8);

        // Serializing queries
        types::write_short(self.num_of_statements as i16, buf);
        for statement in self.statements.clone() {
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

impl BatchStatement<'_> {
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

impl BatchStatementWithValues<'_, '_> {
    fn serialize(&self, buf: &mut impl BufMut) -> Result<()> {
        // Serializing statement
        self.statement.serialize(buf)?;

        // Serializing values bound to statement
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
