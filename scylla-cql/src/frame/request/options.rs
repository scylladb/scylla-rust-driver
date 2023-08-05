use crate::frame::frame_errors::ParseError;
use bytes::BufMut;

use crate::frame::request::{RequestOpcode, SerializableRequest};

pub struct Options;

impl SerializableRequest for Options {
    const OPCODE: RequestOpcode = RequestOpcode::Options;

    fn serialize(&self, _buf: &mut impl BufMut) -> Result<(), ParseError> {
        Ok(())
    }
}
