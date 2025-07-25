//! CQL protocol-level representation of an `EVENT` response.

use crate::frame::frame_errors::{
    ClusterChangeEventParseError, CqlEventParseError, SchemaChangeEventParseError,
};
use crate::frame::server_event_type::EventType;
use crate::frame::types;
use std::net::SocketAddr;

/// Event that the server notified the client about.
#[derive(Debug)]
// Check triggers because all variants end with "Change".
// TODO(2.0): Remove the "Change" postfix from variants.
#[expect(clippy::enum_variant_names)]
pub enum Event {
    /// Topology changed.
    TopologyChange(TopologyChangeEvent),
    /// Status of a node changed.
    StatusChange(StatusChangeEvent),
    /// Schema changed.
    SchemaChange(SchemaChangeEvent),
}

/// Event that notifies about changes in the cluster topology.
#[derive(Debug)]
pub enum TopologyChangeEvent {
    /// A new node was added to the cluster.
    NewNode(SocketAddr),
    /// A node was removed from the cluster.
    RemovedNode(SocketAddr),
}

/// Event that notifies about changes in the nodes' status.
#[derive(Debug)]
pub enum StatusChangeEvent {
    /// A node went up.
    Up(SocketAddr),
    /// A node went down.
    Down(SocketAddr),
}

/// Event that notifies about changes in the cluster topology.
#[derive(Debug)]
// Check triggers because all variants end with "Change".
// TODO(2.0): Remove the "Change" postfix from variants.
#[expect(clippy::enum_variant_names)]
pub enum SchemaChangeEvent {
    /// Keyspace was altered.
    KeyspaceChange {
        /// Type of change that was made to the keyspace.
        change_type: SchemaChangeType,
        /// Name of the keyspace that was altered.
        keyspace_name: String,
    },
    /// Table was altered.
    TableChange {
        /// Type of change that was made to the table.
        change_type: SchemaChangeType,
        /// Name of the keyspace that contains the table.
        keyspace_name: String,
        /// Name of the table that was altered.
        object_name: String,
    },
    /// Type was altered.
    TypeChange {
        /// Type of change that was made to the type.
        change_type: SchemaChangeType,
        /// Name of the keyspace that contains the type.
        keyspace_name: String,
        /// Name of the type that was altered.
        type_name: String,
    },
    /// Function was altered.
    FunctionChange {
        /// Type of change that was made to the function.
        change_type: SchemaChangeType,
        /// Name of the keyspace that contains the function.
        keyspace_name: String,
        /// Name of the function that was altered.
        function_name: String,
        /// List of argument types of the function that was altered.
        arguments: Vec<String>,
    },
    /// Aggregate was altered.
    AggregateChange {
        /// Type of change that was made to the aggregate.
        change_type: SchemaChangeType,
        /// Name of the keyspace that contains the aggregate.
        keyspace_name: String,
        /// Name of the aggregate that was altered.
        aggregate_name: String,
        /// List of argument types of the aggregate that was altered.
        arguments: Vec<String>,
    },
}

/// Type of change that was made to the schema.
#[derive(Debug)]
pub enum SchemaChangeType {
    /// The affected schema item was created.
    Created,

    /// The affected schema item was updated.
    Updated,

    /// The affected schema item was dropped.
    Dropped,

    /// A placeholder for an invalid schema change type.
    Invalid,
}

impl Event {
    /// Deserialize an event from the provided buffer.
    pub fn deserialize(buf: &mut &[u8]) -> Result<Self, CqlEventParseError> {
        let event_type: EventType = types::read_string(buf)
            .map_err(CqlEventParseError::EventTypeParseError)?
            .parse()?;
        match event_type {
            EventType::TopologyChange => Ok(Self::TopologyChange(
                TopologyChangeEvent::deserialize(buf)
                    .map_err(CqlEventParseError::TopologyChangeEventParseError)?,
            )),
            EventType::StatusChange => Ok(Self::StatusChange(
                StatusChangeEvent::deserialize(buf)
                    .map_err(CqlEventParseError::StatusChangeEventParseError)?,
            )),
            EventType::SchemaChange => Ok(Self::SchemaChange(SchemaChangeEvent::deserialize(buf)?)),
        }
    }
}

impl SchemaChangeEvent {
    /// Deserialize a schema change event from the provided buffer.
    pub fn deserialize(buf: &mut &[u8]) -> Result<Self, SchemaChangeEventParseError> {
        let type_of_change_string =
            types::read_string(buf).map_err(SchemaChangeEventParseError::TypeOfChangeParseError)?;
        let type_of_change = match type_of_change_string {
            "CREATED" => SchemaChangeType::Created,
            "UPDATED" => SchemaChangeType::Updated,
            "DROPPED" => SchemaChangeType::Dropped,
            _ => SchemaChangeType::Invalid,
        };

        let target =
            types::read_string(buf).map_err(SchemaChangeEventParseError::TargetTypeParseError)?;
        let keyspace_affected = types::read_string(buf)
            .map_err(SchemaChangeEventParseError::AffectedKeyspaceParseError)?
            .to_string();

        match target {
            "KEYSPACE" => Ok(Self::KeyspaceChange {
                change_type: type_of_change,
                keyspace_name: keyspace_affected,
            }),
            "TABLE" => {
                let table_name = types::read_string(buf)
                    .map_err(SchemaChangeEventParseError::AffectedTargetNameParseError)?
                    .to_string();
                Ok(Self::TableChange {
                    change_type: type_of_change,
                    keyspace_name: keyspace_affected,
                    object_name: table_name,
                })
            }
            "TYPE" => {
                let changed_type = types::read_string(buf)
                    .map_err(SchemaChangeEventParseError::AffectedTargetNameParseError)?
                    .to_string();
                Ok(Self::TypeChange {
                    change_type: type_of_change,
                    keyspace_name: keyspace_affected,
                    type_name: changed_type,
                })
            }
            "FUNCTION" => {
                let function = types::read_string(buf)
                    .map_err(SchemaChangeEventParseError::AffectedTargetNameParseError)?
                    .to_string();
                let number_of_arguments = types::read_short(buf).map_err(|err| {
                    SchemaChangeEventParseError::ArgumentCountParseError(err.into())
                })?;

                let mut argument_vector = Vec::with_capacity(number_of_arguments as usize);

                for _ in 0..number_of_arguments {
                    argument_vector.push(
                        types::read_string(buf)
                            .map_err(SchemaChangeEventParseError::FunctionArgumentParseError)?
                            .to_string(),
                    );
                }

                Ok(Self::FunctionChange {
                    change_type: type_of_change,
                    keyspace_name: keyspace_affected,
                    function_name: function,
                    arguments: argument_vector,
                })
            }
            "AGGREGATE" => {
                let name = types::read_string(buf)
                    .map_err(SchemaChangeEventParseError::AffectedTargetNameParseError)?
                    .to_string();
                let number_of_arguments = types::read_short(buf).map_err(|err| {
                    SchemaChangeEventParseError::ArgumentCountParseError(err.into())
                })?;

                let mut argument_vector = Vec::with_capacity(number_of_arguments as usize);

                for _ in 0..number_of_arguments {
                    argument_vector.push(
                        types::read_string(buf)
                            .map_err(SchemaChangeEventParseError::FunctionArgumentParseError)?
                            .to_string(),
                    );
                }

                Ok(Self::AggregateChange {
                    change_type: type_of_change,
                    keyspace_name: keyspace_affected,
                    aggregate_name: name,
                    arguments: argument_vector,
                })
            }

            _ => Err(SchemaChangeEventParseError::UnknownTargetOfSchemaChange(
                target.to_string(),
            )),
        }
    }
}

impl TopologyChangeEvent {
    /// Deserialize a topology change event from the provided buffer.
    pub fn deserialize(buf: &mut &[u8]) -> Result<Self, ClusterChangeEventParseError> {
        let type_of_change = types::read_string(buf)
            .map_err(ClusterChangeEventParseError::TypeOfChangeParseError)?;
        let addr =
            types::read_inet(buf).map_err(ClusterChangeEventParseError::NodeAddressParseError)?;

        match type_of_change {
            "NEW_NODE" => Ok(Self::NewNode(addr)),
            "REMOVED_NODE" => Ok(Self::RemovedNode(addr)),
            _ => Err(ClusterChangeEventParseError::UnknownTypeOfChange(
                type_of_change.to_string(),
            )),
        }
    }
}

impl StatusChangeEvent {
    /// Deserialize a status change event from the provided buffer.
    pub fn deserialize(buf: &mut &[u8]) -> Result<Self, ClusterChangeEventParseError> {
        let type_of_change = types::read_string(buf)
            .map_err(ClusterChangeEventParseError::TypeOfChangeParseError)?;
        let addr =
            types::read_inet(buf).map_err(ClusterChangeEventParseError::NodeAddressParseError)?;

        match type_of_change {
            "UP" => Ok(Self::Up(addr)),
            "DOWN" => Ok(Self::Down(addr)),
            _ => Err(ClusterChangeEventParseError::UnknownTypeOfChange(
                type_of_change.to_string(),
            )),
        }
    }
}
