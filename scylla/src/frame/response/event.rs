use crate::frame::frame_errors::ParseError;
use crate::frame::server_event_type::EventType;
use crate::frame::types;
use std::net::SocketAddr;

#[derive(Debug)]
pub enum Event {
    TopologyChange(TopologyChangeEvent),
    StatusChange(StatusChangeEvent),
    SchemaChange(SchemaChangeEvent),
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

#[derive(Debug)]
pub enum SchemaChangeEvent {
    KeyspaceChange {
        change_type: SchemaChangeType,
        keyspace_name: String,
    },
    TableChange {
        change_type: SchemaChangeType,
        keyspace_name: String,
        object_name: String,
    },
    TypeChange {
        change_type: SchemaChangeType,
        keyspace_name: String,
        type_name: String,
    },
    FunctionChange {
        change_type: SchemaChangeType,
        keyspace_name: String,
        function_name: String,
        arguments: Vec<String>,
    },
    AggregateChange {
        change_type: SchemaChangeType,
        keyspace_name: String,
        aggregate_name: String,
        arguments: Vec<String>,
    },
}

#[derive(Debug)]
pub enum SchemaChangeType {
    Created,
    Updated,
    Dropped,
    Invalid,
}

impl Event {
    pub fn deserialize(buf: &mut &[u8]) -> Result<Self, ParseError> {
        let event_type: EventType = types::read_string(buf)?.parse()?;
        match event_type {
            EventType::TopologyChange => {
                Ok(Self::TopologyChange(TopologyChangeEvent::deserialize(buf)?))
            }
            EventType::StatusChange => Ok(Self::StatusChange(StatusChangeEvent::deserialize(buf)?)),
            EventType::SchemaChange => Ok(Self::SchemaChange(SchemaChangeEvent::deserialize(buf)?)),
        }
    }
}

impl SchemaChangeEvent {
    pub fn deserialize(buf: &mut &[u8]) -> Result<Self, ParseError> {
        let type_of_change_string = types::read_string(buf)?;
        let type_of_change = match type_of_change_string {
            "CREATED" => SchemaChangeType::Created,
            "UPDATED" => SchemaChangeType::Updated,
            "DROPPED" => SchemaChangeType::Dropped,
            _ => SchemaChangeType::Invalid,
        };

        let target = types::read_string(buf)?;
        let keyspace_affected = types::read_string(buf)?.to_string();

        match target {
            "KEYSPACE" => Ok(Self::KeyspaceChange {
                change_type: type_of_change,
                keyspace_name: keyspace_affected,
            }),
            "TABLE" => {
                let table_name = types::read_string(buf)?.to_string();
                Ok(Self::TableChange {
                    change_type: type_of_change,
                    keyspace_name: keyspace_affected,
                    object_name: table_name,
                })
            }
            "TYPE" => {
                let changed_type = types::read_string(buf)?.to_string();
                Ok(Self::TypeChange {
                    change_type: type_of_change,
                    keyspace_name: keyspace_affected,
                    type_name: changed_type,
                })
            }
            "FUNCTION" => {
                let function = types::read_string(buf)?.to_string();
                let number_of_arguments = types::read_short(buf)?;

                let mut argument_vector = Vec::with_capacity(number_of_arguments as usize);

                for _ in 0..number_of_arguments {
                    argument_vector.push(types::read_string(buf)?.to_string());
                }

                Ok(Self::FunctionChange {
                    change_type: type_of_change,
                    keyspace_name: keyspace_affected,
                    function_name: function,
                    arguments: argument_vector,
                })
            }
            "AGGREGATE" => {
                let name = types::read_string(buf)?.to_string();
                let number_of_arguments = types::read_short(buf)?;

                let mut argument_vector = Vec::with_capacity(number_of_arguments as usize);

                for _ in 0..number_of_arguments {
                    argument_vector.push(types::read_string(buf)?.to_string());
                }

                Ok(Self::AggregateChange {
                    change_type: type_of_change,
                    keyspace_name: keyspace_affected,
                    aggregate_name: name,
                    arguments: argument_vector,
                })
            }

            _ => Err(ParseError::BadData(format!(
                "Invalid type of schema change ({}) in SchemaChangeEvent",
                target
            ))),
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
