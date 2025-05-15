use std::fmt;
use std::str::FromStr;

use super::frame_errors::CqlEventParseError;

// Check triggers because all variants end with "Change".
// TODO(2.0): Remove the "Change" postfix from variants.
#[expect(clippy::enum_variant_names)]
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
    type Err = CqlEventParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "TOPOLOGY_CHANGE" => Ok(Self::TopologyChange),
            "STATUS_CHANGE" => Ok(Self::StatusChange),
            "SCHEMA_CHANGE" => Ok(Self::SchemaChange),
            _ => Err(CqlEventParseError::UnknownEventType(s.to_string())),
        }
    }
}
