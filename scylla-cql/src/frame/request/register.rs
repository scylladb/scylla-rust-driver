use crate::frame::{
    frame_errors::ParseError,
    request::{RequestOpcode, SerializableRequest},
    server_event_type::EventType,
    types,
};

pub struct Register {
    pub event_types_to_register_for: Vec<EventType>,
}

impl SerializableRequest for Register {
    const OPCODE: RequestOpcode = RequestOpcode::Register;

    fn serialize(&self, buf: &mut Vec<u8>) -> Result<(), ParseError> {
        let event_types_list = self
            .event_types_to_register_for
            .iter()
            .map(|event| event.to_string())
            .collect::<Vec<_>>();

        types::write_string_list(&event_types_list, buf)?;
        Ok(())
    }
}
