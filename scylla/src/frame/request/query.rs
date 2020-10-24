use anyhow::Result;
use bytes::{BufMut, Bytes};

use crate::{
    frame::request::{Request, RequestOpcode},
    frame::types,
    frame::value::Value,
    statement,
};

// Query flags
// Unused flags are commented out so that they don't trigger warnings
const FLAG_VALUES: u8 = 0x01;
// const FLAG_SKIP_METADATA: u8 = 0x02;
const FLAG_PAGE_SIZE: u8 = 0x04;
const FLAG_WITH_PAGING_STATE: u8 = 0x08;
// const FLAG_WITH_SERIAL_CONSISTENCY: u8 = 0x10;
// const FLAG_WITH_DEFAULT_TIMESTAMP: u8 = 0x20;
// const FLAG_WITH_NAMES_FOR_VALUES: u8 = 0x40;

pub struct Query<'a> {
    pub contents: String,
    pub parameters: QueryParameters<'a>,
}

impl Request for Query<'_> {
    const OPCODE: RequestOpcode = RequestOpcode::Query;

    fn serialize(&self, buf: &mut impl BufMut) -> Result<()> {
        types::write_long_string(&self.contents, buf)?;
        self.parameters.serialize(buf)?;
        Ok(())
    }
}

pub struct QueryParameters<'a> {
    pub consistency: i16,
    pub page_size: Option<i32>,
    pub paging_state: Option<Bytes>,
    pub values: &'a [Value],
}

impl Default for QueryParameters<'_> {
    fn default() -> Self {
        Self {
            consistency: 1,
            page_size: None,
            paging_state: None,
            values: &[],
        }
    }
}

impl QueryParameters<'_> {
    pub(crate) fn new<'a>(
        params: &statement::QueryParameters,
        paging_state: Option<Bytes>,
        values: &'a [Value],
    ) -> QueryParameters<'a> {
        QueryParameters {
            consistency: params.consistency as i16,
            page_size: params.page_size,
            paging_state,
            values,
        }
    }

    pub fn serialize(&self, buf: &mut impl BufMut) -> Result<()> {
        types::write_short(self.consistency, buf);

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

        buf.put_u8(flags);

        if !self.values.is_empty() {
            buf.put_i16(self.values.len() as i16);

            for value in self.values {
                match value {
                    Value::Val(v) => {
                        types::write_int(v.len() as i32, buf);
                        buf.put_slice(&v[..]);
                    }
                    Value::Null => types::write_int(-1, buf),
                    Value::NotSet => types::write_int(-2, buf),
                }
            }
        }

        if let Some(page_size) = self.page_size {
            types::write_int(page_size, buf);
        }

        if let Some(paging_state) = &self.paging_state {
            types::write_bytes(&paging_state, buf)?;
        }

        Ok(())
    }
}
