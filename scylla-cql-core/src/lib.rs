//! Stable public API types for the ScyllaDB Rust driver.
//!
//! This crate contains CQL data types and traits that form part of the stable
//! public API of the `scylla` driver crate. It is re-exported by both `scylla`
//! and `scylla-cql`, ensuring type identity is preserved across versions.

pub mod frame;
pub(crate) mod pretty;
pub mod utils;

pub mod value;
