use crate::frame::frame_errors::ParseError;
use bytes::{BufMut, Bytes};

use crate::{
    frame::request::{query, Request, RequestOpcode},
    frame::types,
};

pub struct Execute<'a> {
    pub id: Bytes,
    pub parameters: query::QueryParameters<'a>,
}

impl Request for Execute<'_> {
    const OPCODE: RequestOpcode = RequestOpcode::Execute;

    fn serialize(&self, buf: &mut impl BufMut) -> Result<(), ParseError> {
        // Serializing statement id
        types::write_short_bytes(&self.id[..], buf)?;

        // Serializing params
        self.parameters.serialize(buf)?;
        Ok(())
    }
}
