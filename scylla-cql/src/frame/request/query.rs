use std::borrow::Cow;

use crate::frame::{frame_errors::ParseError, types::SerialConsistency};
use bytes::{Buf, BufMut, Bytes};

use crate::{
    frame::request::{RequestOpcode, SerializableRequest},
    frame::types,
    frame::value::LegacySerializedValues,
};

use super::DeserializableRequest;

// Query flags
const FLAG_VALUES: u8 = 0x01;
const FLAG_SKIP_METADATA: u8 = 0x02;
const FLAG_PAGE_SIZE: u8 = 0x04;
const FLAG_WITH_PAGING_STATE: u8 = 0x08;
const FLAG_WITH_SERIAL_CONSISTENCY: u8 = 0x10;
const FLAG_WITH_DEFAULT_TIMESTAMP: u8 = 0x20;
const FLAG_WITH_NAMES_FOR_VALUES: u8 = 0x40;
const ALL_FLAGS: u8 = FLAG_VALUES
    | FLAG_SKIP_METADATA
    | FLAG_PAGE_SIZE
    | FLAG_WITH_PAGING_STATE
    | FLAG_WITH_SERIAL_CONSISTENCY
    | FLAG_WITH_DEFAULT_TIMESTAMP
    | FLAG_WITH_NAMES_FOR_VALUES;

#[cfg_attr(test, derive(Debug, PartialEq, Eq))]
pub struct Query<'q> {
    pub contents: Cow<'q, str>,
    pub parameters: QueryParameters<'q>,
}

impl SerializableRequest for Query<'_> {
    const OPCODE: RequestOpcode = RequestOpcode::Query;

    fn serialize(&self, buf: &mut impl BufMut) -> Result<(), ParseError> {
        types::write_long_string(&self.contents, buf)?;
        self.parameters.serialize(buf)?;
        Ok(())
    }
}

impl<'q> DeserializableRequest for Query<'q> {
    fn deserialize(buf: &mut &[u8]) -> Result<Self, ParseError> {
        let contents = Cow::Owned(types::read_long_string(buf)?.to_owned());
        let parameters = QueryParameters::deserialize(buf)?;

        Ok(Self {
            contents,
            parameters,
        })
    }
}
#[cfg_attr(test, derive(Debug, PartialEq, Eq))]
pub struct QueryParameters<'a> {
    pub consistency: types::Consistency,
    pub serial_consistency: Option<types::SerialConsistency>,
    pub timestamp: Option<i64>,
    pub page_size: Option<i32>,
    pub paging_state: Option<Bytes>,
    pub values: Cow<'a, LegacySerializedValues>,
}

impl Default for QueryParameters<'_> {
    fn default() -> Self {
        Self {
            consistency: Default::default(),
            serial_consistency: None,
            timestamp: None,
            page_size: None,
            paging_state: None,
            values: Cow::Borrowed(LegacySerializedValues::EMPTY),
        }
    }
}

impl QueryParameters<'_> {
    pub fn serialize(&self, buf: &mut impl BufMut) -> Result<(), ParseError> {
        types::write_consistency(self.consistency, buf);

        let mut flags = 0;
        if !self.values.is_empty() {
            flags |= FLAG_VALUES;
        }

        if self.page_size.is_some() {
            flags |= FLAG_PAGE_SIZE;
        }

        if self.paging_state.is_some() {
            flags |= FLAG_WITH_PAGING_STATE;
        }

        if self.serial_consistency.is_some() {
            flags |= FLAG_WITH_SERIAL_CONSISTENCY;
        }

        if self.timestamp.is_some() {
            flags |= FLAG_WITH_DEFAULT_TIMESTAMP;
        }

        if self.values.has_names() {
            flags |= FLAG_WITH_NAMES_FOR_VALUES;
        }

        buf.put_u8(flags);

        if !self.values.is_empty() {
            self.values.write_to_request(buf);
        }

        if let Some(page_size) = self.page_size {
            types::write_int(page_size, buf);
        }

        if let Some(paging_state) = &self.paging_state {
            types::write_bytes(paging_state, buf)?;
        }

        if let Some(serial_consistency) = self.serial_consistency {
            types::write_serial_consistency(serial_consistency, buf);
        }

        if let Some(timestamp) = self.timestamp {
            types::write_long(timestamp, buf);
        }

        Ok(())
    }
}

impl<'q> QueryParameters<'q> {
    pub fn deserialize(buf: &mut &[u8]) -> Result<Self, ParseError> {
        let consistency = types::read_consistency(buf)?;

        let flags = buf.get_u8();
        let unknown_flags = flags & (!ALL_FLAGS);
        if unknown_flags != 0 {
            return Err(ParseError::BadIncomingData(format!(
                "Specified flags are not recognised: {:02x}",
                unknown_flags
            )));
        }
        let values_flag = (flags & FLAG_VALUES) != 0;
        let page_size_flag = (flags & FLAG_PAGE_SIZE) != 0;
        let paging_state_flag = (flags & FLAG_WITH_PAGING_STATE) != 0;
        let serial_consistency_flag = (flags & FLAG_WITH_SERIAL_CONSISTENCY) != 0;
        let default_timestamp_flag = (flags & FLAG_WITH_DEFAULT_TIMESTAMP) != 0;
        let values_have_names_flag = (flags & FLAG_WITH_NAMES_FOR_VALUES) != 0;

        let values = Cow::Owned(if values_flag {
            LegacySerializedValues::new_from_frame(buf, values_have_names_flag)?
        } else {
            LegacySerializedValues::new()
        });

        let page_size = page_size_flag.then(|| types::read_int(buf)).transpose()?;
        let paging_state = if paging_state_flag {
            Some(Bytes::copy_from_slice(types::read_bytes(buf)?))
        } else {
            None
        };
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
        let timestamp = if default_timestamp_flag {
            Some(types::read_long(buf)?)
        } else {
            None
        };

        Ok(Self {
            consistency,
            serial_consistency,
            timestamp,
            page_size,
            paging_state,
            values,
        })
    }
}
