use crate::frame::frame_errors::ParseError;
use bytes::BufMut;

use crate::{
    frame::request::{Request, RequestOpcode},
    frame::types,
};

pub struct Prepare<'a> {
    pub query: &'a str,
}

impl<'a> Request for Prepare<'a> {
    const OPCODE: RequestOpcode = RequestOpcode::Prepare;

    fn serialize(&self, buf: &mut impl BufMut) -> Result<(), ParseError> {
        types::write_long_string(self.query, buf)?;
        Ok(())
    }
}
