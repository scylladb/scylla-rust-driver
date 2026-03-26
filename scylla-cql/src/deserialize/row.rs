//! Provides types for dealing with row deserialization.
//!
//! This module re-exports all types from `scylla_cql_core::deserialize::row`.

// Re-export everything from scylla-cql-core's row module.
pub use scylla_cql_core::deserialize::row::{
    BuiltinDeserializationError, BuiltinDeserializationErrorKind, BuiltinTypeCheckError,
    BuiltinTypeCheckErrorKind, ColumnIterator, DeserializeRow, RawColumn,
    deser_error_replace_rust_name, mk_deser_err, mk_typck_err,
};

#[cfg(test)]
#[path = "row_tests.rs"]
pub(crate) mod tests;
