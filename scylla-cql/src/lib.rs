//! Defines CQL protocol-level types and traits for interacting with ScyllaDB/Cassandra.
//!
//! Mainly intended to be used by the ScyllaDB driver, but can also be useful for other
//! applications that need to interact with CQL.

pub mod frame;

pub use scylla_cql_core::DeserializeRow;
pub use scylla_cql_core::DeserializeValue;
pub use scylla_cql_core::SerializeRow;
pub use scylla_cql_core::SerializeValue;

pub mod deserialize;
pub mod serialize;

pub mod value;

pub mod utils;

pub use crate::frame::types::Consistency;

#[doc(hidden)]
pub use scylla_cql_core::_macro_internal;
