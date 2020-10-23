use anyhow::Result;
use bytes::{BufMut, Bytes};

use crate::{
    frame::request::{Request, RequestOpcode},
    frame::types,
    frame::value::Value,
};

pub struct Execute<'a> {
    pub id: Bytes,
    pub values: &'a [Value],
}

impl Request for Execute<'_> {
    const OPCODE: RequestOpcode = RequestOpcode::Execute;

    fn serialize(&self, buf: &mut impl BufMut) -> Result<()> {
        // Serializing statement id
        types::write_short(self.id.len() as i16, buf);
        buf.put_slice(&self.id[..]);
        // Serializing params
        types::write_short(1, buf); // consistency ONE
        buf.put_u8(if self.values.is_empty() { 0x0 } else { 0x1 }); // Flags: 0x1 means that values are present
        if !self.values.is_empty() {
            buf.put_i16(self.values.len() as i16);
        }
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
        println!("Buf {:?}", self.values);
        Ok(())
    }
}
