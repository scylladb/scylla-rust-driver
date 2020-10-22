use anyhow::Result;
use bytes::BufMut;

use std::collections::HashMap;

use crate::{
    frame::{request::Request, Opcode},
    types,
};

pub struct Startup {
    pub options: HashMap<String, String>,
}

impl Request for Startup {
    const OPCODE: Opcode = Opcode::Startup;

    fn serialize(&self, buf: &mut impl BufMut) -> Result<()> {
        types::write_string_map(&self.options, buf)?;
        Ok(())
    }
}
