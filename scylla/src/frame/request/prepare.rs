use crate::frame::frame_errors::ParseError;
use bytes::BufMut;

use crate::{
    frame::request::{Request, RequestOpcode},
    frame::types,
};

pub struct Prepare {
    pub query: String,
}

impl Request for Prepare {
    const OPCODE: RequestOpcode = RequestOpcode::Prepare;

    fn serialize(&self, buf: &mut impl BufMut) -> Result<(), ParseError> {
        types::write_long_string(&self.query, buf)?;
        Ok(())
    }
}
