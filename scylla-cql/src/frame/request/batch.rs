use bytes::{Buf, BufMut};
use std::{borrow::Cow, convert::TryInto};

use crate::{
    frame::{
        frame_errors::ParseError,
        request::{RequestOpcode, SerializableRequest},
        types::{self, SerialConsistency},
    },
    types::serialize::row::SerializedValues,
};

use super::DeserializableRequest;

// Batch flags
const FLAG_WITH_SERIAL_CONSISTENCY: u8 = 0x10;
const FLAG_WITH_DEFAULT_TIMESTAMP: u8 = 0x20;
const ALL_FLAGS: u8 = FLAG_WITH_SERIAL_CONSISTENCY | FLAG_WITH_DEFAULT_TIMESTAMP;

#[cfg_attr(test, derive(Debug, PartialEq, Eq))]
pub struct Batch<'b, Statement>
where
    BatchStatement<'b>: From<&'b Statement>,
    Statement: Clone,
{
    pub statements: Cow<'b, [Statement]>,
    pub batch_type: BatchType,
    pub consistency: types::Consistency,
    pub serial_consistency: Option<types::SerialConsistency>,
    pub timestamp: Option<i64>,
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
    Query {
        text: Cow<'a, str>,
        values: Cow<'a, SerializedValues>,
    },
    Prepared {
        id: Cow<'a, [u8]>,
        values: Cow<'a, SerializedValues>,
    },
}

impl<Statement> SerializableRequest for Batch<'_, Statement>
where
    for<'s> BatchStatement<'s>: From<&'s Statement>,
    Statement: Clone,
{
    const OPCODE: RequestOpcode = RequestOpcode::Batch;

    fn serialize(&self, buf: &mut impl BufMut) -> Result<(), ParseError> {
        // Serializing type of batch
        buf.put_u8(self.batch_type as u8);

        // Serializing queries
        types::write_short(self.statements.len().try_into()?, buf);

        for statement in self.statements.iter() {
            let stmt = BatchStatement::from(statement);
            stmt.serialize(buf)?;
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
                let values = Cow::Owned(SerializedValues::new_from_frame(buf)?);
                Ok(BatchStatement::Query { text, values })
            }
            1 => {
                let id = types::read_short_bytes(buf)?.to_vec().into();
                let values = Cow::Owned(SerializedValues::new_from_frame(buf)?);
                Ok(BatchStatement::Prepared { id, values })
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
            Self::Query { text, values } => {
                buf.put_u8(0);
                types::write_long_string(text, buf)?;
                values.write_to_request(buf);
            }
            Self::Prepared { id, values } => {
                buf.put_u8(1);
                types::write_short_bytes(id, buf)?;
                values.write_to_request(buf);
            }
        }

        Ok(())
    }
}

impl<'s, 'b> From<&'s BatchStatement<'b>> for BatchStatement<'s> {
    fn from(value: &'s BatchStatement) -> Self {
        match value {
            BatchStatement::Query { text, values } => BatchStatement::Query {
                text: text.clone(),
                values: values.clone(),
            },
            BatchStatement::Prepared { id, values } => BatchStatement::Prepared {
                id: id.clone(),
                values: values.clone(),
            },
        }
    }
}

impl<'b> DeserializableRequest for Batch<'b, BatchStatement<'b>> {
    fn deserialize(buf: &mut &[u8]) -> Result<Self, ParseError> {
        let batch_type = buf.get_u8().try_into()?;

        let statements_count: usize = types::read_short(buf)?.into();
        let statements = (0..statements_count)
            .map(|_| {
                let batch_statement = BatchStatement::deserialize(buf)?;

                Ok(batch_statement)
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

        Ok(Self {
            statements: Cow::Owned(statements),
            batch_type,
            consistency,
            serial_consistency,
            timestamp,
        })
    }
}
