use bytes::BufMut;

use crate::frame::{frame_errors::ParseError, value::BatchValuesIterator};
use std::{borrow::Cow, convert::TryInto};

use crate::frame::{
    request::{Request, RequestOpcode},
    types,
    value::BatchValues,
};

// Batch flags
const FLAG_WITH_SERIAL_CONSISTENCY: u8 = 0x10;
const FLAG_WITH_DEFAULT_TIMESTAMP: u8 = 0x20;

pub struct Batch<'b, Statement, Values>
where
    BatchStatement<'b>: From<&'b Statement>,
    Statement: Clone,
    Values: BatchValues,
{
    pub statements: Cow<'b, [Statement]>,
    pub batch_type: BatchType,
    pub consistency: types::Consistency,
    pub serial_consistency: Option<types::SerialConsistency>,
    pub timestamp: Option<i64>,
    pub values: Values,
}

/// The type of a batch.
#[derive(Clone, Copy)]
pub enum BatchType {
    Logged = 0,
    Unlogged = 1,
    Counter = 2,
}

pub struct BatchTypeParseError {
    value: u8,
}

impl From<BatchTypeParseError> for ParseError {
    fn from(err: BatchTypeParseError) -> Self {
        Self::BadIncomingData(format!("Bad BatchType value: {}", err.value))
    }
}

impl TryFrom<u8> for BatchType {
    type Error = BatchTypeParseError;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(Self::Logged),
            1 => Ok(Self::Unlogged),
            2 => Ok(Self::Counter),
            _ => Err(BatchTypeParseError { value }),
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq, PartialOrd, Ord)]
pub enum BatchStatement<'a> {
    Query { text: Cow<'a, str> },
    Prepared { id: Cow<'a, [u8]> },
}

impl<Statement, Values> Request for Batch<'_, Statement, Values>
where
    for<'s> BatchStatement<'s>: From<&'s Statement>,
    Statement: Clone,
    Values: BatchValues,
{
    const OPCODE: RequestOpcode = RequestOpcode::Batch;

    fn serialize(&self, buf: &mut impl BufMut) -> Result<(), ParseError> {
        // Serializing type of batch
        buf.put_u8(self.batch_type as u8);

        // Serializing queries
        types::write_short(self.statements.len().try_into()?, buf);

        let counts_mismatch_err = |n_values: usize, n_statements: usize| {
            ParseError::BadDataToSerialize(format!(
                "Length of provided values must be equal to number of batch statements \
                    (got {n_values} values, {n_statements} statements)"
            ))
        };
        let mut n_serialized_statements = 0usize;
        let mut value_lists = self.values.batch_values_iter();
        for (idx, statement) in self.statements.iter().enumerate() {
            BatchStatement::from(statement).serialize(buf)?;
            value_lists
                .write_next_to_request(buf)
                .ok_or_else(|| counts_mismatch_err(idx, self.statements.len()))??;
            n_serialized_statements += 1;
        }
        // At this point, we have all statements serialized. If any values are still left, we have a mismatch.
        if value_lists.skip_next().is_some() {
            return Err(counts_mismatch_err(
                n_serialized_statements + 1 /*skipped above*/ + value_lists.count(),
                n_serialized_statements,
            ));
        }
        if n_serialized_statements != self.statements.len() {
            // We want to check this to avoid propagating an invalid construction of self.statements_count as a
            // hard-to-debug silent fail
            return Err(ParseError::BadDataToSerialize(format!(
                "Invalid Batch constructed: not as many statements serialized as announced \
                    (batch.statement_count: {announced_statement_count}, {n_serialized_statements}",
                announced_statement_count = self.statements.len()
            )));
        }

        // Serializing consistency
        types::write_consistency(self.consistency, buf);

        // Serializing flags
        let mut flags = 0;
        if self.serial_consistency.is_some() {
            flags |= FLAG_WITH_SERIAL_CONSISTENCY;
        }
        if self.timestamp.is_some() {
            flags |= FLAG_WITH_DEFAULT_TIMESTAMP;
        }

        buf.put_u8(flags);

        if let Some(serial_consistency) = self.serial_consistency {
            types::write_serial_consistency(serial_consistency, buf);
        }
        if let Some(timestamp) = self.timestamp {
            types::write_long(timestamp, buf);
        }

        Ok(())
    }
}

impl BatchStatement<'_> {
    fn serialize(&self, buf: &mut impl BufMut) -> Result<(), ParseError> {
        match self {
            Self::Query { text } => {
                buf.put_u8(0);
                types::write_long_string(text, buf)?;
            }
            Self::Prepared { id } => {
                buf.put_u8(1);
                types::write_short_bytes(id, buf)?;
            }
        }

        Ok(())
    }
}
