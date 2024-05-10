//! Provides types for dealing with row deserialization.

use std::fmt::Display;

use thiserror::Error;

use super::{DeserializationError, FrameSlice, TypeCheckError};
use crate::frame::response::result::{ColumnSpec, ColumnType};

/// Represents a raw, unparsed column value.
#[non_exhaustive]
pub struct RawColumn<'frame> {
    pub index: usize,
    pub spec: &'frame ColumnSpec,
    pub slice: Option<FrameSlice<'frame>>,
}

/// Iterates over columns of a single row.
#[derive(Clone, Debug)]
pub struct ColumnIterator<'frame> {
    specs: std::iter::Enumerate<std::slice::Iter<'frame, ColumnSpec>>,
    slice: FrameSlice<'frame>,
}

impl<'frame> ColumnIterator<'frame> {
    /// Creates a new iterator over a single row.
    ///
    /// - `specs` - information about columns of the serialized response,
    /// - `slice` - a [FrameSlice] which points to the serialized row.
    #[inline]
    pub(crate) fn new(specs: &'frame [ColumnSpec], slice: FrameSlice<'frame>) -> Self {
        Self {
            specs: specs.iter().enumerate(),
            slice,
        }
    }

    /// Returns the remaining number of columns that this iterator is expected
    /// to return.
    #[inline]
    pub fn columns_remaining(&self) -> usize {
        self.specs.len()
    }
}

impl<'frame> Iterator for ColumnIterator<'frame> {
    type Item = Result<RawColumn<'frame>, DeserializationError>;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        let (column_index, spec) = self.specs.next()?;
        Some(
            self.slice
                .read_cql_bytes()
                .map(|slice| RawColumn {
                    index: column_index,
                    spec,
                    slice,
                })
                .map_err(|err| {
                    mk_deser_err::<Self>(
                        BuiltinDeserializationErrorKind::RawColumnDeserializationFailed {
                            column_index,
                            column_name: spec.name.clone(),
                            err: DeserializationError::new(err),
                        },
                    )
                }),
        )
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        self.specs.size_hint()
    }
}

/// A type that can be deserialized from a row that was returned from a query.
///
/// For tips on how to write a custom implementation of this trait, see the
/// documentation of the parent module.
///
/// The crate also provides a derive macro which allows to automatically
/// implement the trait for a custom type. For more details on what the macro
/// is capable of, see its documentation.
pub trait DeserializeRow<'frame>
where
    Self: Sized,
{
    /// Checks that the schema of the result matches what this type expects.
    ///
    /// This function can check whether column types and names match the
    /// expectations.
    fn type_check(specs: &[ColumnSpec]) -> Result<(), TypeCheckError>;

    /// Deserializes a row from given column iterator.
    ///
    /// This function can assume that the driver called `type_check` to verify
    /// the row's type. Note that `deserialize` is not an unsafe function,
    /// so it should not use the assumption about `type_check` being called
    /// as an excuse to run `unsafe` code.
    fn deserialize(row: ColumnIterator<'frame>) -> Result<Self, DeserializationError>;
}

// raw deserialization as ColumnIterator

// What is the purpose of implementing DeserializeRow for ColumnIterator?
//
// Sometimes users might be interested in operating on ColumnIterator directly.
// Implementing DeserializeRow for it allows us to simplify our interface. For example,
// we have `QueryResult::rows<T: DeserializeRow>()` - you can put T = ColumnIterator
// instead of having a separate rows_raw function or something like this.
impl<'frame> DeserializeRow<'frame> for ColumnIterator<'frame> {
    #[inline]
    fn type_check(_specs: &[ColumnSpec]) -> Result<(), TypeCheckError> {
        Ok(())
    }

    #[inline]
    fn deserialize(row: ColumnIterator<'frame>) -> Result<Self, DeserializationError> {
        Ok(row)
    }
}

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
pub enum BuiltinDeserializationErrorKind {
    /// One of the raw columns failed to deserialize, most probably
    /// due to the invalid column structure inside a row in the frame.
    RawColumnDeserializationFailed {
        /// Index of the raw column that failed to deserialize.
        column_index: usize,

        /// Name of the raw column that failed to deserialize.
        column_name: String,

        /// The error that caused the raw column deserialization to fail.
        err: DeserializationError,
    },
}

impl Display for BuiltinDeserializationErrorKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            BuiltinDeserializationErrorKind::RawColumnDeserializationFailed {
                column_index,
                column_name,
                err,
            } => {
                write!(
                    f,
                    "failed to deserialize raw column {column_name} at index {column_index} (most probably due to invalid column structure inside a row): {err}"
                )
            }
        }
    }
}
