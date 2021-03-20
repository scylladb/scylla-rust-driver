use crate::frame::frame_errors::ParseError;
use std::fmt;
use std::str::FromStr;

pub enum EventType {
    TopologyChange,
    StatusChange,
    SchemaChange,
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

impl FromStr for EventType {
    type Err = ParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "TOPOLOGY_CHANGE" => Ok(Self::TopologyChange),
            "STATUS_CHANGE" => Ok(Self::StatusChange),
            "SCHEMA_CHANGE" => Ok(Self::SchemaChange),
            _ => Err(ParseError::BadData(format!(
                "Invalid type event type: {}",
                s
            ))),
        }
    }
}
