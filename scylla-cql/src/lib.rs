//! Defines CQL protocol-level types and traits for interacting with ScyllaDB/Cassandra.
//!
//! Mainly intended to be used by the ScyllaDB driver, but can also be useful for other
//! applications that need to interact with CQL.

pub mod frame;

pub use scylla_macros::DeserializeRow;
pub use scylla_macros::DeserializeValue;
pub use scylla_macros::SerializeRow;
pub use scylla_macros::SerializeValue;

pub mod deserialize;
pub mod serialize;

pub mod value;

pub mod utils;

pub use crate::frame::types::Consistency;

#[doc(hidden)]
pub mod _macro_internal;

#[cfg(test)]
mod macros_tests;

mod macros_doctests;
