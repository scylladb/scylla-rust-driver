//! CQL protocol-level representation of a `STARTUP` request.

use crate::frame::frame_errors::CqlRequestSerializationError;

use std::{borrow::Cow, collections::HashMap};

use crate::{
    frame::request::{RequestOpcode, SerializableRequest},
    frame::types,
};

use super::DeserializableRequest;

// Re-export for backward compatibility.
pub use crate::frame::frame_errors::StartupSerializationError;

/// The CQL protocol-level representation of an `STARTUP` request,
/// used to finalise connection negotiation phase and establish the CQL connection.
pub struct Startup<'a> {
    /// The protocol options that were suggested by the server and accepted by the client.
    pub options: HashMap<Cow<'a, str>, Cow<'a, str>>,
}

impl SerializableRequest for Startup<'_> {
    const OPCODE: RequestOpcode = RequestOpcode::Startup;

    fn serialize(&self, buf: &mut Vec<u8>) -> Result<(), CqlRequestSerializationError> {
        types::write_string_map(&self.options, buf)
            .map_err(StartupSerializationError::OptionsSerialization)?;
        Ok(())
    }
}

impl DeserializableRequest for Startup<'_> {
    fn deserialize(buf: &mut &[u8]) -> Result<Self, super::RequestDeserializationError> {
        // Note: this is inefficient, but it's only used for tests and it's not common
        // to deserialize STARTUP frames anyway.
        let options = types::read_string_map(buf)?
            .into_iter()
            .map(|(k, v)| (k.into(), v.into()))
            .collect();
        Ok(Self { options })
    }
}
