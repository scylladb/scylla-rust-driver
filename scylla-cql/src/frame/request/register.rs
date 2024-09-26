use std::num::TryFromIntError;

use thiserror::Error;

use crate::frame::{
    frame_errors::CqlRequestSerializationError,
    request::{RequestOpcode, SerializableRequest},
    server_event_type::EventType,
    types,
};

pub struct Register {
    pub event_types_to_register_for: Vec<EventType>,
}

impl SerializableRequest for Register {
    const OPCODE: RequestOpcode = RequestOpcode::Register;

    fn serialize(&self, buf: &mut Vec<u8>) -> Result<(), CqlRequestSerializationError> {
        let event_types_list = self
            .event_types_to_register_for
            .iter()
            .map(|event| event.to_string())
            .collect::<Vec<_>>();

        types::write_string_list(&event_types_list, buf)
            .map_err(RegisterSerializationError::EventTypesSerialization)?;
        Ok(())
    }
}

/// An error type returned when serialization of REGISTER request fails.
#[non_exhaustive]
#[derive(Error, Debug, Clone)]
pub enum RegisterSerializationError {
    /// Failed to serialize event types list.
    #[error("Failed to serialize event types list: {0}")]
    EventTypesSerialization(TryFromIntError),
}
