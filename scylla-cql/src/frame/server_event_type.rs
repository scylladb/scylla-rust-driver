//! This module defines the `EventType` enum, which represents different types of CQL events.
// TODO(2.0): Move this to a more appropriate location in the crate structure.

use std::fmt;
use std::str::FromStr;

use super::frame_errors::CqlEventParseError;

/// Represents the type of a CQL event.
// Check triggers because all variants end with "Change".
// TODO(2.0): Remove the "Change" postfix from variants.
#[expect(clippy::enum_variant_names)]
pub enum EventType {
    /// Represents a change in the cluster topology, such as node addition or removal.
    TopologyChange,
    /// Represents a change in the status of a node, such as up or down.
    StatusChange,
    /// Represents a change in the schema, such as table creation or modification.
    SchemaChange,
}

/// Represents the type of a CQL event.
// The postfix "Change" is used in all variants just because all currently existing events happen to be
// about changes in the cluster, so it makes sense to use the same postfix for all of them.
// If we add a new event type that is not about changes, then clippy will no longer complain.
#[expect(clippy::enum_variant_names)]
#[non_exhaustive]
pub enum EventTypeV2 {
    /// Represents a change in the cluster topology, such as node addition or removal.
    TopologyChange,
    /// Represents a change in the status of a node, such as up or down.
    StatusChange,
    /// Represents a change in the schema, such as table creation or modification.
    SchemaChange,
    /// Represents a change in the client routes.
    ClientRoutesChange,
}

impl fmt::Display for EventType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = match &self {
            Self::TopologyChange => "TOPOLOGY_CHANGE",
            Self::StatusChange => "STATUS_CHANGE",
            Self::SchemaChange => "SCHEMA_CHANGE",
        };

        write!(f, "{s}")
    }
}

impl fmt::Display for EventTypeV2 {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = match &self {
            Self::TopologyChange => "TOPOLOGY_CHANGE",
            Self::StatusChange => "STATUS_CHANGE",
            Self::SchemaChange => "SCHEMA_CHANGE",
            Self::ClientRoutesChange => "CLIENT_ROUTES_CHANGE",
        };

        write!(f, "{s}")
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

impl FromStr for EventTypeV2 {
    type Err = CqlEventParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "TOPOLOGY_CHANGE" => Ok(Self::TopologyChange),
            "STATUS_CHANGE" => Ok(Self::StatusChange),
            "SCHEMA_CHANGE" => Ok(Self::SchemaChange),
            "CLIENT_ROUTES_CHANGE" => Ok(Self::ClientRoutesChange),
            _ => Err(CqlEventParseError::UnknownEventType(s.to_string())),
        }
    }
}
