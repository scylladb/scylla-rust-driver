use bytes::BufMut;
use std::fmt;

use crate::frame::{
    frame_errors::ParseError,
    request::{Request, RequestOpcode},
    types,
};

pub enum EventType {
    TopologyChange,
    StatusChange,
    SchemaChange,
}

pub struct Register {
    pub event_types_to_register_for: Vec<EventType>,
}

impl Request for Register {
    const OPCODE: RequestOpcode = RequestOpcode::Register;

    fn serialize(&self, buf: &mut impl BufMut) -> Result<(), ParseError> {
        let event_types_list = self
            .event_types_to_register_for
            .iter()
            .map(|event| event.to_string())
            .collect::<Vec<_>>();

        types::write_string_list(&event_types_list, buf)?;
        Ok(())
    }
}

impl fmt::Display for EventType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = match &self {
            Self::TopologyChange => "TOPOLOGY_CHANGE",
            Self::StatusChange => "STATUS_CHANGE",
            Self::SchemaChange => "SCHEMA_CHANGE",
        };

        write!(f, "{}", s)
    }
}
