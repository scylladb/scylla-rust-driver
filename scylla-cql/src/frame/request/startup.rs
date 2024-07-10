use crate::frame::frame_errors::ParseError;

use std::{borrow::Cow, collections::HashMap};

use crate::{
    frame::request::{RequestOpcode, SerializableRequest},
    frame::types,
};

pub struct Startup<'a> {
    pub options: HashMap<Cow<'a, str>, Cow<'a, str>>,
}

impl SerializableRequest for Startup<'_> {
    const OPCODE: RequestOpcode = RequestOpcode::Startup;

    fn serialize(&self, buf: &mut Vec<u8>) -> Result<(), ParseError> {
        types::write_string_map(&self.options, buf)?;
        Ok(())
    }
}
