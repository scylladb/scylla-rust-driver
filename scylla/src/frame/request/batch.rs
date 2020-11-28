use crate::frame::frame_errors::ParseError;
use bytes::{BufMut, Bytes};
use std::convert::TryInto;

use crate::frame::{
    request::{Request, RequestOpcode},
    types,
    value::BatchValues,
};

use crate::statement::batch::BatchStatement;

pub struct Batch<'a, Values: BatchValues> {
    pub statements: &'a [BatchStatement],
    pub values: Values,
    pub batch_type: BatchType,
    pub consistency: i16,
}

/// The type of a batch.
#[derive(Clone, Copy)]
pub enum BatchType {
    Logged = 0,
    Unlogged = 1,
    Counter = 2,
}

impl<'a, Values: BatchValues> Request for Batch<'a, Values> {
    const OPCODE: RequestOpcode = RequestOpcode::Batch;

    fn serialize(&self, buf: &mut impl BufMut) -> Result<(), ParseError> {
        // Serializing type of batch
        buf.put_u8(self.batch_type as u8);

        // Serializing queries
        types::write_short(self.statements.len().try_into()?, buf);

        for (statement_num, statement) in self.statements.iter().enumerate() {
            match statement {
                BatchStatement::Query(q) => {
                    let query_text: &str = q.get_contents();
                    buf.put_u8(0);
                    types::write_long_string(query_text, buf)?;
                }
                BatchStatement::PreparedStatement(s) => {
                    let id: &Bytes = s.get_id();
                    buf.put_u8(1);
                    types::write_short_bytes(&id[..], buf)?;
                }
            }

            self.values.write_nth_to_request(statement_num, buf)?;
        }

        // Serializing consistency
        types::write_short(self.consistency, buf);

        // Serializing flags
        // FIXME: consider other flag values than 0
        buf.put_u8(0);

        Ok(())
    }
}
