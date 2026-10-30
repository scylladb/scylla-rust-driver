//! CQL protocol-level representation of a `BATCH` request.

use bytes::{Buf, BufMut};
use std::{borrow::Cow, convert::TryInto};

use crate::frame::{
    frame_errors::CqlRequestSerializationError,
    protocol_features::ProtocolFeatures,
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

pub use scylla_cql_core::frame::request::batch::{BatchStatement, BatchType, BatchTypeParseError};

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

    /// Trailing routing hint defined by the `TABLETS_ROUTING_V2` protocol extension.
    /// When `Some`, it is appended after the batch parameters. The connection layer sets
    /// this to `Some(_)` on every `BATCH` sent over a connection that negotiated V2,
    /// and to `None` otherwise.
    pub routing_hint: Option<BatchRoutingHint>,
}

/// Routing hint appended to a `BATCH` request by the `TABLETS_ROUTING_V2` protocol extension.
///
/// It names the statement the driver routed the batch by and carries a tablet-version block
/// for the tablet of that statement, so that the server can return fresh routing information
/// for it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct BatchRoutingHint {
    /// The 0-based index of the statement the driver routed the batch by, or
    /// [`BatchRoutingHint::NO_ROUTING_STATEMENT`].
    ///
    /// The routing statement must be a prepared statement with partition key
    /// bind indices; otherwise the server rejects the request.
    pub routing_statement_index: u16,

    /// The tablet-version block for the tablet of the routing statement. The server ignores
    /// it when there is no routing statement.
    pub tablet_version_block: u8,
}

impl BatchRoutingHint {
    /// The routing statement index of a batch that wasn't routed by any statement.
    ///
    /// The number of statements in a batch is a `[short]`, so this never names a statement.
    pub const NO_ROUTING_STATEMENT: u16 = 0xFFFF;
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

        // Serializing the trailing TABLETS_ROUTING_V2 routing hint. On a V2 connection the
        // server requires it on every BATCH, so the connection layer always sets it there.
        if let Some(routing_hint) = self.routing_hint {
            types::write_short(routing_hint.routing_statement_index, buf);
            buf.put_u8(routing_hint.tablet_version_block);
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

impl<'b> DeserializableRequest for Batch<'b, BatchStatement<'b>, Vec<SerializedValues>> {
    fn deserialize(buf: &mut &[u8]) -> Result<Self, RequestDeserializationError> {
        // Nothing in the frame tells us whether a routing hint was appended: that depends
        // solely on whether the sending connection negotiated TABLETS_ROUTING_V2. A
        // featureless decoder therefore cannot decode it, and must not consume trailing bytes
        // that may not be there. Use `deserialize_with_features` when the negotiated features
        // are known.
        Self::deserialize_maybe_with_routing_hint(buf, false)
    }

    fn deserialize_with_features(
        buf: &mut &[u8],
        features: &ProtocolFeatures,
    ) -> Result<Self, RequestDeserializationError> {
        // On a V2 connection every BATCH carries a routing hint after the batch parameters.
        Self::deserialize_maybe_with_routing_hint(buf, features.tablets_v2_supported)
    }
}

impl<'b> Batch<'b, BatchStatement<'b>, Vec<SerializedValues>> {
    fn deserialize_maybe_with_routing_hint(
        buf: &mut &[u8],
        with_routing_hint: bool,
    ) -> Result<Self, RequestDeserializationError> {
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

        let routing_hint = with_routing_hint
            .then(|| -> Result<_, RequestDeserializationError> {
                let routing_statement_index = types::read_short(buf)?;
                let tablet_version_block = types::read_byte(buf)?;
                Ok(BatchRoutingHint {
                    routing_statement_index,
                    tablet_version_block,
                })
            })
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
            routing_hint,
        })
    }
}

#[cfg(test)]
mod tests {
    use std::borrow::Cow;

    use crate::Consistency;
    use crate::frame::protocol_features::ProtocolFeatures;
    use crate::frame::request::{DeserializableRequest, SerializableRequest};
    use crate::serialize::row::SerializedValues;

    use super::{Batch, BatchRoutingHint, BatchStatement, BatchType};

    fn sample_batch(
        routing_hint: Option<BatchRoutingHint>,
    ) -> Batch<'static, BatchStatement<'static>, Vec<SerializedValues>> {
        Batch {
            statements: Cow::Owned(vec![
                BatchStatement::Prepared {
                    id: Cow::Borrowed(&[1, 2, 3]),
                },
                BatchStatement::Query {
                    text: Cow::Borrowed("INSERT INTO ks.t (pk, v) VALUES (1, 2)"),
                },
            ]),
            batch_type: BatchType::Logged,
            consistency: Consistency::Quorum,
            serial_consistency: None,
            timestamp: Some(42),
            values: vec![SerializedValues::new(), SerializedValues::new()],
            routing_hint,
        }
    }

    fn v2_features() -> ProtocolFeatures {
        let mut features = ProtocolFeatures::default();
        features.tablets_v2_supported = true;
        features
    }

    fn non_v2_features() -> ProtocolFeatures {
        let mut features = ProtocolFeatures::default();
        features.tablets_v2_supported = false;
        features
    }

    /// On a TABLETS_ROUTING_V2 connection the frame is exactly the base BATCH frame plus
    /// the routing hint: a [short] statement index followed by a tablet-version block byte.
    #[test]
    fn batch_appends_routing_hint() {
        let with_hint = sample_batch(Some(BatchRoutingHint {
            routing_statement_index: 0x0102,
            tablet_version_block: 0xAB,
        }));
        let without_hint = sample_batch(None);

        let mut buf_with = Vec::new();
        with_hint.serialize(&mut buf_with).unwrap();
        let mut buf_without = Vec::new();
        without_hint.serialize(&mut buf_without).unwrap();

        assert_eq!(&buf_with[..buf_without.len()], buf_without.as_slice());
        assert_eq!(&buf_with[buf_without.len()..], &[0x01, 0x02, 0xAB]);
    }

    /// Serializing with a hint and deserializing with the V2 feature negotiated
    /// must recover the exact same request, including the hint.
    #[test]
    fn batch_routing_hint_roundtrip() {
        for routing_statement_index in [0, 1, BatchRoutingHint::NO_ROUTING_STATEMENT] {
            let batch = sample_batch(Some(BatchRoutingHint {
                routing_statement_index,
                tablet_version_block: 0x5C,
            }));

            let mut buf = Vec::new();
            batch.serialize(&mut buf).unwrap();

            let mut slice = &buf[..];
            let deserialized =
                Batch::deserialize_with_features(&mut slice, &v2_features()).unwrap();
            assert!(slice.is_empty());
            assert_eq!(deserialized, batch);
        }
    }

    /// Without the V2 feature negotiated, no routing hint is expected or consumed.
    #[test]
    fn batch_no_routing_hint_without_feature() {
        let batch = sample_batch(None);

        let mut buf = Vec::new();
        batch.serialize(&mut buf).unwrap();

        let mut slice = &buf[..];
        let deserialized =
            Batch::deserialize_with_features(&mut slice, &non_v2_features()).unwrap();
        assert!(slice.is_empty());
        assert_eq!(deserialized, batch);
    }
}
