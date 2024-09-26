use thiserror::Error;

use crate::frame::frame_errors::CqlRequestSerializationError;

use std::{borrow::Cow, collections::HashMap, num::TryFromIntError};

use crate::{
    frame::request::{RequestOpcode, SerializableRequest},
    frame::types,
};

pub struct Startup<'a> {
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

/// An error type returned when serialization of STARTUP request fails.
#[non_exhaustive]
#[derive(Error, Debug, Clone)]
pub enum StartupSerializationError {
    /// Failed to serialize startup options.
    #[error("Malformed startup options: {0}")]
    OptionsSerialization(TryFromIntError),
}
