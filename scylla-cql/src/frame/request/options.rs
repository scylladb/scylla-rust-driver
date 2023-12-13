use crate::frame::frame_errors::ParseError;

use crate::frame::request::{RequestOpcode, SerializableRequest};

pub struct Options;

impl SerializableRequest for Options {
    const OPCODE: RequestOpcode = RequestOpcode::Options;

    fn serialize(&self, _buf: &mut Vec<u8>) -> Result<(), ParseError> {
        Ok(())
    }
}
