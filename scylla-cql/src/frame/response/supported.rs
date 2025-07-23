//! CQL protocol-level representation of a `SUPPORTED` response.

use crate::frame::frame_errors::CqlSupportedParseError;
use crate::frame::types;
use std::collections::HashMap;

#[derive(Debug)]
/// The CQL protocol-level representation of an `SUPPORTED` response,
/// used to present the server's supported options.
pub struct Supported {
    /// A map of option names to their supported values.
    pub options: HashMap<String, Vec<String>>,
}

impl Supported {
    /// Deserializes a `SUPPORTED` response from the provided byte buffer.
    pub fn deserialize(buf: &mut &[u8]) -> Result<Self, CqlSupportedParseError> {
        let options = types::read_string_multimap(buf)
            .map_err(CqlSupportedParseError::OptionsMapDeserialization)?;

        Ok(Supported { options })
    }
}
