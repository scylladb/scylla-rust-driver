use crate::frame::frame_errors::ParseError;
use bytes::BufMut;

use std::collections::HashMap;

use crate::{
    frame::request::{Request, RequestOpcode},
    frame::types,
};

pub struct Startup {
    pub options: HashMap<String, String>,
}

impl Request for Startup {
    const OPCODE: RequestOpcode = RequestOpcode::Startup;

    fn serialize(&self, buf: &mut impl BufMut) -> Result<(), ParseError> {
        types::write_string_map(&self.options, buf)?;
        Ok(())
    }
}
