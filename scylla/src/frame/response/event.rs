use crate::frame::frame_errors::ParseError;
use crate::frame::server_event_type::EventType;
use crate::frame::types;
use std::net::SocketAddr;

#[derive(Debug)]
pub enum Event {
    TopologyChange(TopologyChangeEvent),
    StatusChange(StatusChangeEvent),
    SchemaChange, // TODO specify
}

#[derive(Debug)]
pub enum TopologyChangeEvent {
    NewNode(SocketAddr),
    RemovedNode(SocketAddr),
}

#[derive(Debug)]
pub enum StatusChangeEvent {
    Up(SocketAddr),
    Down(SocketAddr),
}

impl Event {
    pub fn deserialize(buf: &mut &[u8]) -> Result<Self, ParseError> {
        let event_type: EventType = types::read_string(buf)?.parse()?;
        match event_type {
            EventType::TopologyChange => {
                Ok(Self::TopologyChange(TopologyChangeEvent::deserialize(buf)?))
            }
            EventType::StatusChange => Ok(Self::StatusChange(StatusChangeEvent::deserialize(buf)?)),
            EventType::SchemaChange => {
                // TODO implement deserialization of SchemaChange
                Ok(Self::SchemaChange)
            }
        }
    }
}

impl TopologyChangeEvent {
    pub fn deserialize(buf: &mut &[u8]) -> Result<Self, ParseError> {
        let type_of_change = types::read_string(buf)?;
        let addr = types::read_inet(buf)?;

        match type_of_change {
            "NEW_NODE" => Ok(Self::NewNode(addr)),
            "REMOVED_NODE" => Ok(Self::RemovedNode(addr)),
            _ => Err(ParseError::BadData(format!(
                "Invalid type of change ({}) in TopologyChangeEvent",
                type_of_change
            ))),
        }
    }
}

impl StatusChangeEvent {
    pub fn deserialize(buf: &mut &[u8]) -> Result<Self, ParseError> {
        let type_of_change = types::read_string(buf)?;
        let addr = types::read_inet(buf)?;

        match type_of_change {
            "UP" => Ok(Self::Up(addr)),
            "DOWN" => Ok(Self::Down(addr)),
            _ => Err(ParseError::BadData(format!(
                "Invalid type of status change ({}) in StatusChangeEvent",
                type_of_change
            ))),
        }
    }
}
