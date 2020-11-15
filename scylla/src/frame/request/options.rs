use crate::frame::frame_errors::ParseError;
use bytes::BufMut;

use crate::frame::request::{Request, RequestOpcode};

pub struct Options;

impl Request for Options {
    const OPCODE: RequestOpcode = RequestOpcode::Options;

    fn serialize(&self, _buf: &mut impl BufMut) -> Result<(), ParseError> {
        Ok(())
    }
}
