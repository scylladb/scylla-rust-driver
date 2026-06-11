//! Stable public API types for the ScyllaDB Rust driver.
//!
//! This crate contains CQL data types and traits that form part of the stable
//! public API of the `scylla` driver crate. It is re-exported by both `scylla`
//! and `scylla-cql`, ensuring type identity is preserved across versions.

pub use scylla_macros::DeserializeRow;
pub use scylla_macros::DeserializeValue;
pub use scylla_macros::SerializeRow;
pub use scylla_macros::SerializeValue;

pub mod deserialize;
pub mod frame;
pub(crate) mod pretty;
pub mod utils;

pub mod serialize;
pub mod value;

#[doc(hidden)]
pub mod _macro_internal;

#[cfg(test)]
mod macros_tests;

#[cfg(doctest)]
mod macros_doctests;
