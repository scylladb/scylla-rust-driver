use anyhow::Result;
use std::collections::HashMap;

use crate::frame::types;

#[derive(Debug)]
pub struct Supported {
    pub options: HashMap<String, Vec<String>>,
}

impl Supported {
    pub fn deserialize(buf: &mut &[u8]) -> Result<Self> {
        let options = types::read_string_multimap(buf)?;

        Ok(Supported { options })
    }
}
