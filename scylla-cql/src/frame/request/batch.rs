//! CQL protocol-level representation of a `BATCH` request.

use bytes::{Buf, BufMut};
use std::{borrow::Cow, convert::TryInto};

use crate::frame::{
    frame_errors::CqlRequestSerializationError,
    request::{RequestOpcode, SerializableRequest},
    types::{self, SerialConsistency},
};
use crate::serialize::{
    RowWriter, SerializationError,
    raw_batch::{RawBatchValues, RawBatchValuesIterator},
    row::SerializedValues,
};

use super::{DeserializableRequest, RequestDeserializationError};

// Re-export for backward compatibility.
pub use crate::frame::frame_errors::{BatchSerializationError, BatchStatementSerializationError};

pub use scylla_cql_core::frame::request::batch::{BatchType, BatchTypeParseError};

// Batch flags
const FLAG_WITH_SERIAL_CONSISTENCY: u8 = 0x10;
const FLAG_WITH_DEFAULT_TIMESTAMP: u8 = 0x20;
const ALL_FLAGS: u8 = FLAG_WITH_SERIAL_CONSISTENCY | FLAG_WITH_DEFAULT_TIMESTAMP;

/// CQL protocol-level representation of a `BATCH` request, used to execute
/// a batch of statements (prepared, unprepared, or a mix of both).
#[cfg_attr(test, derive(Debug, PartialEq, Eq))]
pub struct Batch<'b, Statement, Values>
where
    BatchStatement<'b>: From<&'b Statement>,
    Statement: Clone,
    Values: RawBatchValues,
{
    /// The statements in the batch.
    pub statements: Cow<'b, [Statement]>,

    /// The type of the batch.
    pub batch_type: BatchType,

    /// The consistency level for the batch.
    pub consistency: types::Consistency,

    /// The serial consistency level for the batch, if any.
    pub serial_consistency: Option<types::SerialConsistency>,

    /// The client-side-assigned timestamp for the batch, if any.
    pub timestamp: Option<i64>,

    /// The bound values for the batch statements.
    pub values: Values,
}

/// A single statement in a batch, which can either be a statement string or a prepared statement ID.
#[derive(Debug, Clone, Eq, PartialEq, PartialOrd, Ord)]
pub enum BatchStatement<'a> {
    /// Unprepared CQL statement.
    Query {
        /// CQL statement string.
        text: Cow<'a, str>,
    },
    /// Prepared CQL statement.
    Prepared {
        /// Prepared CQL statement's ID.
        id: Cow<'a, [u8]>,
    },
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
            serialize_batch_statement(&BatchStatement::from(statement), buf).map_err(|err| {
                BatchSerializationError::StatementSerialization {
                    statement_idx: idx,
                    error: err,
                }
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
                    });
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

fn deserialize_batch_statement(
    buf: &mut &[u8],
) -> Result<BatchStatement<'static>, RequestDeserializationError> {
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

fn serialize_batch_statement(
    statement: &BatchStatement<'_>,
    buf: &mut impl BufMut,
) -> Result<(), BatchStatementSerializationError> {
    match statement {
        BatchStatement::Query { text } => {
            buf.put_u8(0);
            types::write_long_string(text, buf)
                .map_err(BatchStatementSerializationError::StatementStringSerialization)?;
        }
        BatchStatement::Prepared { id } => {
            buf.put_u8(1);
            types::write_short_bytes(id, buf)
                .map_err(BatchStatementSerializationError::StatementIdSerialization)?;
        }
    }

    Ok(())
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
    fn deserialize(buf: &mut &[u8]) -> Result<Self, RequestDeserializationError> {
        let batch_type = buf.get_u8().try_into()?;

        let statements_count: usize = types::read_short(buf)?.into();
        let statements_with_values = (0..statements_count)
            .map(|_| {
                let batch_statement = deserialize_batch_statement(buf)?;

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
