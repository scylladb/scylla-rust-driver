use crate::frame::frame_errors::ParseError;
use bytes::{BufMut, Bytes};
use std::convert::TryInto;

use crate::frame::{
    request::{Request, RequestOpcode},
    types,
    value::BatchValues,
};

// Batch flags
const FLAG_WITH_SERIAL_CONSISTENCY: u8 = 0x10;

pub struct Batch<'a, StatementsIter, Values>
where
    StatementsIter: Iterator<Item = BatchStatement<'a>> + Clone,
    Values: BatchValues,
{
    pub statements: StatementsIter,
    pub statements_count: usize,
    pub batch_type: BatchType,
    pub consistency: types::Consistency,
    pub serial_consistency: Option<types::Consistency>,
    pub values: Values,
}

/// The type of a batch.
#[derive(Clone, Copy)]
pub enum BatchType {
    Logged = 0,
    Unlogged = 1,
    Counter = 2,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, PartialOrd, Ord)]
pub enum BatchStatement<'a> {
    Query { text: &'a str },
    Prepared { id: &'a Bytes },
}

impl<'a, StatementsIter, Values> Request for Batch<'a, StatementsIter, Values>
where
    StatementsIter: Iterator<Item = BatchStatement<'a>> + Clone,
    Values: BatchValues,
{
    const OPCODE: RequestOpcode = RequestOpcode::Batch;

    fn serialize(&self, buf: &mut impl BufMut) -> Result<(), ParseError> {
        // Serializing type of batch
        buf.put_u8(self.batch_type as u8);

        // Serializing queries
        types::write_short(self.statements_count.try_into()?, buf);

        for (statement_num, statement) in self.statements.clone().enumerate() {
            statement.serialize(buf)?;
            self.values.write_nth_to_request(statement_num, buf)?;
        }

        // Serializing consistency
        types::write_consistency(self.consistency, buf);

        // Serializing flags
        let mut flags = 0;
        if self.serial_consistency.is_some() {
            flags |= FLAG_WITH_SERIAL_CONSISTENCY;
        }

        buf.put_u8(flags);

        if let Some(serial_consistency) = self.serial_consistency {
            types::write_consistency(serial_consistency, buf);
        }

        Ok(())
    }
}

impl BatchStatement<'_> {
    fn serialize(&self, buf: &mut impl BufMut) -> Result<(), ParseError> {
        match self {
            BatchStatement::Query { text } => {
                buf.put_u8(0);
                types::write_long_string(text, buf)?;
            }
            BatchStatement::Prepared { id } => {
                buf.put_u8(1);
                types::write_short_bytes(&id[..], buf)?;
            }
        }

        Ok(())
    }
}
