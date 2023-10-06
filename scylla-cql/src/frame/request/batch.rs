use bytes::{Buf, BufMut};
use std::{borrow::Cow, convert::TryInto};

use crate::frame::{
    frame_errors::ParseError,
    request::{RequestOpcode, SerializableRequest},
    types::{self, SerialConsistency},
    value::{BatchValues, BatchValuesIterator, SerializedValues},
};

use super::DeserializableRequest;

// Batch flags
const FLAG_WITH_SERIAL_CONSISTENCY: u8 = 0x10;
const FLAG_WITH_DEFAULT_TIMESTAMP: u8 = 0x20;
const ALL_FLAGS: u8 = FLAG_WITH_SERIAL_CONSISTENCY | FLAG_WITH_DEFAULT_TIMESTAMP;

#[cfg_attr(test, derive(Debug, PartialEq, Eq))]
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
#[cfg_attr(test, derive(Debug, PartialEq, Eq))]
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

impl<Statement, Values> SerializableRequest for Batch<'_, Statement, Values>
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
        types::write_u16(self.statements.len().try_into()?, buf);

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
    fn deserialize(buf: &mut &[u8]) -> Result<Self, ParseError> {
        let kind = buf.get_u8();
        match kind {
            0 => {
                let text = Cow::Owned(types::read_long_string(buf)?.to_owned());
                Ok(BatchStatement::Query { text })
            }
            1 => {
                let id = types::read_short_bytes(buf)?.to_vec().into();
                Ok(BatchStatement::Prepared { id })
            }
            _ => Err(ParseError::BadIncomingData(format!(
                "Unexpected batch statement kind: {}",
                kind
            ))),
        }
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

impl<'s, 'b> From<&'s BatchStatement<'b>> for BatchStatement<'s> {
    fn from(value: &'s BatchStatement) -> Self {
        match value {
            BatchStatement::Query { text } => BatchStatement::Query { text: text.clone() },
            BatchStatement::Prepared { id } => BatchStatement::Prepared { id: id.clone() },
        }
    }
}

impl<'b> DeserializableRequest for Batch<'b, BatchStatement<'b>, Vec<SerializedValues>> {
    fn deserialize(buf: &mut &[u8]) -> Result<Self, ParseError> {
        let batch_type = buf.get_u8().try_into()?;

        let statements_count: usize = types::read_u16(buf)?.try_into()?;
        let statements_with_values = (0..statements_count)
            .map(|_| {
                let batch_statement = BatchStatement::deserialize(buf)?;

                // As stated in CQL protocol v4 specification, values names in Batch are broken and should be never used.
                let values = SerializedValues::new_from_frame(buf, false)?;

                Ok((batch_statement, values))
            })
            .collect::<Result<Vec<_>, ParseError>>()?;

        let consistency = types::read_consistency(buf)?;

        let flags = buf.get_u8();
        let unknown_flags = flags & (!ALL_FLAGS);
        if unknown_flags != 0 {
            return Err(ParseError::BadIncomingData(format!(
                "Specified flags are not recognised: {:02x}",
                unknown_flags
            )));
        }
        let serial_consistency_flag = (flags & FLAG_WITH_SERIAL_CONSISTENCY) != 0;
        let default_timestamp_flag = (flags & FLAG_WITH_DEFAULT_TIMESTAMP) != 0;

        let serial_consistency = serial_consistency_flag
            .then(|| types::read_consistency(buf))
            .transpose()?
            .map(
                |consistency| match SerialConsistency::try_from(consistency) {
                    Ok(serial_consistency) => Ok(serial_consistency),
                    Err(_) => Err(ParseError::BadIncomingData(format!(
                        "Expected SerialConsistency, got regular Consistency {}",
                        consistency
                    ))),
                },
            )
            .transpose()?;

        let timestamp = default_timestamp_flag
            .then(|| types::read_long(buf))
            .transpose()?;

        let (statements, values): (Vec<BatchStatement>, Vec<SerializedValues>) =
            statements_with_values.into_iter().unzip();

        Ok(Self {
            batch_type,
            consistency,
            serial_consistency,
            timestamp,
            statements: Cow::Owned(statements),
            values,
        })
    }
}
