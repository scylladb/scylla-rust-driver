//! CQL protocol-level representation of a `REGISTER` request.

use crate::frame::{
    frame_errors::CqlRequestSerializationError,
    request::{RequestOpcode, SerializableRequest},
    server_event_type::EventType,
    types,
};

// Re-export for backward compatibility.
pub use crate::frame::frame_errors::RegisterSerializationError;

/// The CQL protocol-level representation of an `REGISTER` request,
/// used to subscribe for server events.
pub struct Register {
    /// A list of event types to register for.
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
