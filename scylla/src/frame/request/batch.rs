use anyhow::Result;
use bytes::{BufMut, Bytes};

use crate::frame::{
    request::{Request, RequestOpcode},
    types,
    value::Value,
};

pub struct Batch<'a, I: Iterator<Item = BatchStatementWithValues<'a>> + Clone> {
    pub statements: I,
    pub statements_count: usize,
    pub batch_type: BatchType,
    pub consistency: i16,
}

pub struct BatchStatementWithValues<'a> {
    pub statement: BatchStatement<'a>,
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

impl<'a, I: Iterator<Item = BatchStatementWithValues<'a>> + Clone> Request for Batch<'a, I> {
    const OPCODE: RequestOpcode = RequestOpcode::Batch;

    fn serialize(&self, buf: &mut impl BufMut) -> Result<()> {
        // Serializing type of batch
        buf.put_u8(self.batch_type as u8);

        // Serializing queries
        types::write_short(self.statements_count as i16, buf);
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

impl BatchStatementWithValues<'_> {
    fn serialize(&self, buf: &mut impl BufMut) -> Result<()> {
        // Serializing statement
        self.statement.serialize(buf)?;

        // Serializing values bound to statement
        types::write_values(&self.values, buf);

        Ok(())
    }
}
