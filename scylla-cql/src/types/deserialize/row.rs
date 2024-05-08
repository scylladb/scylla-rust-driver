//! Provides types for dealing with row deserialization.

use std::fmt::Display;

use thiserror::Error;

use super::{DeserializationError, TypeCheckError};

use crate::frame::response::result::ColumnType;

// Error facilities

/// Failed to type check incoming result column types again given Rust type,
/// one of the types having support built into the driver.
#[derive(Debug, Error, Clone)]
#[error("Failed to type check the Rust type {rust_name} against CQL column types {cql_types:?} : {kind}")]
pub struct BuiltinTypeCheckError {
    /// Name of the Rust type used to represent the values.
    pub rust_name: &'static str,

    /// The CQL types of the values that the Rust type was being deserialized from.
    pub cql_types: Vec<ColumnType>,

    /// Detailed information about the failure.
    pub kind: BuiltinTypeCheckErrorKind,
}

fn mk_typck_err<T>(
    cql_types: impl IntoIterator<Item = ColumnType>,
    kind: impl Into<BuiltinTypeCheckErrorKind>,
) -> TypeCheckError {
    mk_typck_err_named(std::any::type_name::<T>(), cql_types, kind)
}

fn mk_typck_err_named(
    name: &'static str,
    cql_types: impl IntoIterator<Item = ColumnType>,
    kind: impl Into<BuiltinTypeCheckErrorKind>,
) -> TypeCheckError {
    TypeCheckError::new(BuiltinTypeCheckError {
        rust_name: name,
        cql_types: Vec::from_iter(cql_types),
        kind: kind.into(),
    })
}

/// Describes why type checking incoming result column types again given Rust type failed.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum BuiltinTypeCheckErrorKind {}

impl Display for BuiltinTypeCheckErrorKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        Ok(())
    }
}

/// Failed to deserialize a row from the DB response, represented by one of the types
/// built into the driver.
#[derive(Debug, Error, Clone)]
#[error("Failed to deserialize query result row {rust_name}: {kind}")]
pub struct BuiltinDeserializationError {
    /// Name of the Rust type used to represent the row.
    pub rust_name: &'static str,

    /// Detailed information about the failure.
    pub kind: BuiltinDeserializationErrorKind,
}

pub(super) fn mk_deser_err<T>(
    kind: impl Into<BuiltinDeserializationErrorKind>,
) -> DeserializationError {
    mk_deser_err_named(std::any::type_name::<T>(), kind)
}

pub(super) fn mk_deser_err_named(
    name: &'static str,
    kind: impl Into<BuiltinDeserializationErrorKind>,
) -> DeserializationError {
    DeserializationError::new(BuiltinDeserializationError {
        rust_name: name,
        kind: kind.into(),
    })
}

/// Describes why deserializing a result row failed.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum BuiltinDeserializationErrorKind {}

impl Display for BuiltinDeserializationErrorKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        Ok(())
    }
}
