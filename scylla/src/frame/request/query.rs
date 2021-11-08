use crate::frame::frame_errors::ParseError;
use bytes::{BufMut, Bytes};

use crate::{
    frame::request::{Request, RequestOpcode},
    frame::types,
    frame::value::SerializedValues,
};

// Query flags
// Unused flags are commented out so that they don't trigger warnings
const FLAG_VALUES: u8 = 0x01;
// const FLAG_SKIP_METADATA: u8 = 0x02;
const FLAG_PAGE_SIZE: u8 = 0x04;
const FLAG_WITH_PAGING_STATE: u8 = 0x08;
const FLAG_WITH_SERIAL_CONSISTENCY: u8 = 0x10;
const FLAG_WITH_DEFAULT_TIMESTAMP: u8 = 0x20;
// const FLAG_WITH_NAMES_FOR_VALUES: u8 = 0x40;

pub struct Query<'a> {
    pub contents: &'a str,
    pub parameters: QueryParameters<'a>,
}

impl Request for Query<'_> {
    const OPCODE: RequestOpcode = RequestOpcode::Query;

    fn serialize(&self, buf: &mut impl BufMut) -> Result<(), ParseError> {
        types::write_long_string(self.contents, buf)?;
        self.parameters.serialize(buf)?;
        Ok(())
    }
}

pub struct QueryParameters<'a> {
    pub consistency: types::Consistency,
    pub serial_consistency: Option<types::SerialConsistency>,
    pub timestamp: Option<i64>,
    pub page_size: Option<i32>,
    pub paging_state: Option<Bytes>,
    pub values: &'a SerializedValues,
}

impl Default for QueryParameters<'_> {
    fn default() -> Self {
        Self {
            consistency: Default::default(),
            serial_consistency: None,
            timestamp: None,
            page_size: None,
            paging_state: None,
            values: SerializedValues::EMPTY,
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
