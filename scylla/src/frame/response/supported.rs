use crate::frame::frame_errors::ParseError;
use crate::frame::types;
use std::collections::HashMap;

#[derive(Debug)]
pub struct Supported {
    pub options: HashMap<String, Vec<String>>,
}

impl Supported {
    pub fn deserialize(buf: &mut &[u8]) -> Result<Self, ParseError> {
        let options = types::read_string_multimap(buf)?;

        Ok(Supported { options })
    }
}
