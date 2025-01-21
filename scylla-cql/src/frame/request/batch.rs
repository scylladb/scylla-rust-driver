use bytes::{Buf, BufMut};
use std::{borrow::Cow, convert::TryInto, num::TryFromIntError};
use thiserror::Error;

use crate::frame::{
    frame_errors::CqlRequestSerializationError,
    request::{RequestOpcode, SerializableRequest},
    types::{self, SerialConsistency},
};
use crate::serialize::{
    raw_batch::{RawBatchValues, RawBatchValuesIterator},
    row::SerializedValues,
    RowWriter, SerializationError,
};

use super::{DeserializableRequest, RequestDeserializationError};

// Batch flags
const FLAG_WITH_SERIAL_CONSISTENCY: u8 = 0x10;
const FLAG_WITH_DEFAULT_TIMESTAMP: u8 = 0x20;
const ALL_FLAGS: u8 = FLAG_WITH_SERIAL_CONSISTENCY | FLAG_WITH_DEFAULT_TIMESTAMP;

#[cfg_attr(test, derive(Debug, PartialEq, Eq))]
pub struct Batch<'b, Statement, Values>
where
    BatchStatement<'b>: From<&'b Statement>,
    Statement: Clone,
    Values: RawBatchValues,
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

#[derive(Debug, Error)]
#[error("Malformed batch type: {value}")]
pub struct BatchTypeParseError {
    value: u8,
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

impl<Statement, Values> Batch<'_, Statement, Values>
where
    for<'s> BatchStatement<'s>: From<&'s Statement>,
    Statement: Clone,
    Values: RawBatchValues,
{
    fn do_serialize(&self, buf: &mut Vec<u8>) -> Result<(), BatchSerializationError> {
        // Serializing type of batch
        buf.put_u8(self.batch_type as u8);

        // Serializing queries
        types::write_short(
            self.statements
                .len()
                .try_into()
                .map_err(|_| BatchSerializationError::TooManyStatements(self.statements.len()))?,
            buf,
        );

        let counts_mismatch_err = |n_value_lists: usize, n_statements: usize| {
            BatchSerializationError::ValuesAndStatementsLengthMismatch {
                n_value_lists,
                n_statements,
            }
        };
        let mut n_serialized_statements = 0usize;
        let mut value_lists = self.values.batch_values_iter();
        for (idx, statement) in self.statements.iter().enumerate() {
            BatchStatement::from(statement)
                .serialize(buf)
                .map_err(|err| BatchSerializationError::StatementSerialization {
                    statement_idx: idx,
                    error: err,
                })?;

            // Reserve two bytes for length
            let length_pos = buf.len();
            buf.extend_from_slice(&[0, 0]);
            let mut row_writer = RowWriter::new(buf);
            value_lists
                .serialize_next(&mut row_writer)
                .ok_or_else(|| counts_mismatch_err(idx, self.statements.len()))?
                .map_err(|err: SerializationError| {
                    BatchSerializationError::StatementSerialization {
                        statement_idx: idx,
                        error: BatchStatementSerializationError::ValuesSerialiation(err),
                    }
                })?;
            // Go back and put the length
            let count: u16 = match row_writer.value_count().try_into() {
                Ok(n) => n,
                Err(_) => {
                    return Err(BatchSerializationError::StatementSerialization {
                        statement_idx: idx,
                        error: BatchStatementSerializationError::TooManyValues(
                            row_writer.value_count(),
                        ),
                    })
                }
            };
            buf[length_pos..length_pos + 2].copy_from_slice(&count.to_be_bytes());

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
            return Err(BatchSerializationError::BadBatchConstructed {
                n_announced_statements: self.statements.len(),
                n_serialized_statements,
            });
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

impl<Statement, Values> SerializableRequest for Batch<'_, Statement, Values>
where
    for<'s> BatchStatement<'s>: From<&'s Statement>,
    Statement: Clone,
    Values: RawBatchValues,
{
    const OPCODE: RequestOpcode = RequestOpcode::Batch;

    fn serialize(&self, buf: &mut Vec<u8>) -> Result<(), CqlRequestSerializationError> {
        self.do_serialize(buf)?;
        Ok(())
    }
}

impl BatchStatement<'_> {
    fn deserialize(buf: &mut &[u8]) -> Result<Self, RequestDeserializationError> {
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
            _ => Err(RequestDeserializationError::UnexpectedBatchStatementKind(
                kind,
            )),
        }
    }
}

impl BatchStatement<'_> {
    fn serialize(&self, buf: &mut impl BufMut) -> Result<(), BatchStatementSerializationError> {
        match self {
            Self::Query { text } => {
                buf.put_u8(0);
                types::write_long_string(text, buf)
                    .map_err(BatchStatementSerializationError::StatementStringSerialization)?;
            }
            Self::Prepared { id } => {
                buf.put_u8(1);
                types::write_short_bytes(id, buf)
                    .map_err(BatchStatementSerializationError::StatementIdSerialization)?;
            }
        }

        Ok(())
    }
}

// Disable the lint, if there is more than one lifetime included.
// Can be removed once https://github.com/rust-lang/rust-clippy/issues/12495 is fixed.
#[allow(clippy::needless_lifetimes)]
impl<'s, 'b> From<&'s BatchStatement<'b>> for BatchStatement<'s> {
    fn from(value: &'s BatchStatement) -> Self {
        match value {
            BatchStatement::Query { text } => BatchStatement::Query { text: text.clone() },
            BatchStatement::Prepared { id } => BatchStatement::Prepared { id: id.clone() },
        }
    }
}

impl<'b> DeserializableRequest for Batch<'b, BatchStatement<'b>, Vec<SerializedValues>> {
    fn deserialize(buf: &mut &[u8]) -> Result<Self, RequestDeserializationError> {
        let batch_type = buf.get_u8().try_into()?;

        let statements_count: usize = types::read_short(buf)?.into();
        let statements_with_values = (0..statements_count)
            .map(|_| {
                let batch_statement = BatchStatement::deserialize(buf)?;

                // As stated in CQL protocol v4 specification, values names in Batch are broken and should be never used.
                let values = SerializedValues::new_from_frame(buf)?;

                Ok((batch_statement, values))
            })
            .collect::<Result<Vec<_>, RequestDeserializationError>>()?;

        let consistency = types::read_consistency(buf)?;

        let flags = buf.get_u8();
        let unknown_flags = flags & (!ALL_FLAGS);
        if unknown_flags != 0 {
            return Err(RequestDeserializationError::UnknownFlags {
                flags: unknown_flags,
            });
        }
        let serial_consistency_flag = (flags & FLAG_WITH_SERIAL_CONSISTENCY) != 0;
        let default_timestamp_flag = (flags & FLAG_WITH_DEFAULT_TIMESTAMP) != 0;

        let serial_consistency = serial_consistency_flag
            .then(|| types::read_consistency(buf))
            .transpose()?
            .map(
                |consistency| match SerialConsistency::try_from(consistency) {
                    Ok(serial_consistency) => Ok(serial_consistency),
                    Err(_) => Err(RequestDeserializationError::ExpectedSerialConsistency(
                        consistency,
                    )),
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

/// An error type returned when serialization of BATCH request fails.
#[non_exhaustive]
#[derive(Error, Debug, Clone)]
pub enum BatchSerializationError {
    /// Maximum number of batch statements exceeded.
    #[error("Too many statements in the batch. Received {0} statements, when u16::MAX is maximum possible value.")]
    TooManyStatements(usize),

    /// Number of batch statements differs from number of provided bound value lists.
    #[error("Number of provided value lists must be equal to number of batch statements (got {n_value_lists} value lists, {n_statements} statements)")]
    ValuesAndStatementsLengthMismatch {
        n_value_lists: usize,
        n_statements: usize,
    },

    /// Failed to serialize a statement in the batch.
    #[error("Failed to serialize batch statement. statement idx: {statement_idx}, error: {error}")]
    StatementSerialization {
        statement_idx: usize,
        error: BatchStatementSerializationError,
    },

    /// Number of announced batch statements differs from actual number of batch statements.
    #[error("Invalid Batch constructed: not as many statements serialized as announced (announced: {n_announced_statements}, serialized: {n_serialized_statements})")]
    BadBatchConstructed {
        n_announced_statements: usize,
        n_serialized_statements: usize,
    },
}

/// An error type returned when serialization of one of the
/// batch statements fails.
#[non_exhaustive]
#[derive(Error, Debug, Clone)]
pub enum BatchStatementSerializationError {
    /// Failed to serialize the CQL statement string.
    #[error("Failed to serialize unprepared statement's content: {0}")]
    StatementStringSerialization(TryFromIntError),

    /// Maximum value of statement id exceeded.
    #[error("Malformed prepared statement's id: {0}")]
    StatementIdSerialization(TryFromIntError),

    /// Failed to serialize statement's bound values.
    #[error("Failed to serialize statement's values: {0}")]
    ValuesSerialiation(SerializationError),

    /// Too many bound values provided.
    #[error("Too many values provided for the statement: {0}")]
    TooManyValues(usize),
}
