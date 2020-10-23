use anyhow::Result;
use bytes::BufMut;

use crate::{
    frame::request::{Request, RequestOpcode},
    frame::types,
    frame::value::Value,
};

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
    pub values: &'a [Value],
}

impl Default for QueryParameters<'_> {
    fn default() -> Self {
        Self {
            consistency: 1,
            values: &[],
        }
    }
}

impl QueryParameters<'_> {
    pub fn serialize(&self, buf: &mut impl BufMut) -> Result<()> {
        types::write_short(self.consistency, buf);

        let mut flags = 0;
        if !self.values.is_empty() {
            flags |= 1 << 0;
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

        Ok(())
    }
}
