//! Provides types for dealing with row deserialization.

use std::fmt::Display;

use thiserror::Error;

use super::value::DeserializeValue;
use super::{make_error_replace_rust_name, DeserializationError, FrameSlice, TypeCheckError};
use crate::frame::response::result::{ColumnSpec, ColumnType};
use crate::value::{CqlValue, Row};

/// Represents a raw, unparsed column value.
#[non_exhaustive]
pub struct RawColumn<'frame, 'metadata> {
    pub index: usize,
    pub spec: &'metadata ColumnSpec<'metadata>,
    pub slice: Option<FrameSlice<'frame>>,
}

/// Iterates over columns of a single row.
#[derive(Clone, Debug)]
pub struct ColumnIterator<'frame, 'metadata> {
    specs: std::iter::Enumerate<std::slice::Iter<'metadata, ColumnSpec<'metadata>>>,
    slice: FrameSlice<'frame>,
}

impl<'frame, 'metadata> ColumnIterator<'frame, 'metadata> {
    /// Creates a new iterator over a single row.
    ///
    /// - `specs` - information about columns of the serialized response,
    /// - `slice` - a [FrameSlice] which points to the serialized row.
    #[inline]
    pub fn new(specs: &'metadata [ColumnSpec<'metadata>], slice: FrameSlice<'frame>) -> Self {
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

impl<'frame, 'metadata> Iterator for ColumnIterator<'frame, 'metadata> {
    type Item = Result<RawColumn<'frame, 'metadata>, DeserializationError>;

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
                            column_name: spec.name().to_owned(),
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
pub trait DeserializeRow<'frame, 'metadata>
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
    fn deserialize(row: ColumnIterator<'frame, 'metadata>) -> Result<Self, DeserializationError>;
}

// raw deserialization as ColumnIterator

// What is the purpose of implementing DeserializeRow for ColumnIterator?
//
// Sometimes users might be interested in operating on ColumnIterator directly.
// Implementing DeserializeRow for it allows us to simplify our interface. For example,
// we have `QueryResult::rows<T: DeserializeRow>()` - you can put T = ColumnIterator
// instead of having a separate rows_raw function or something like this.
impl<'frame, 'metadata> DeserializeRow<'frame, 'metadata> for ColumnIterator<'frame, 'metadata> {
    #[inline]
    fn type_check(_specs: &[ColumnSpec]) -> Result<(), TypeCheckError> {
        Ok(())
    }

    #[inline]
    fn deserialize(row: ColumnIterator<'frame, 'metadata>) -> Result<Self, DeserializationError> {
        Ok(row)
    }
}

make_error_replace_rust_name!(
    _typck_error_replace_rust_name,
    TypeCheckError,
    BuiltinTypeCheckError
);

make_error_replace_rust_name!(
    deser_error_replace_rust_name,
    DeserializationError,
    BuiltinDeserializationError
);

// legacy/dynamic deserialization as Row
//
/// While no longer encouraged (because the new framework encourages deserializing
/// directly into desired types, entirely bypassing [CqlValue]), this can be indispensable
/// for some use cases, i.e. those involving dynamic parsing (ORMs?).
impl<'frame, 'metadata> DeserializeRow<'frame, 'metadata> for Row {
    #[inline]
    fn type_check(_specs: &[ColumnSpec]) -> Result<(), TypeCheckError> {
        // CqlValues accept all types, no type checking needed.
        Ok(())
    }

    #[inline]
    fn deserialize(
        mut row: ColumnIterator<'frame, 'metadata>,
    ) -> Result<Self, DeserializationError> {
        let mut columns = Vec::with_capacity(row.size_hint().0);
        while let Some(column) = row
            .next()
            .transpose()
            .map_err(deser_error_replace_rust_name::<Self>)?
        {
            columns.push(
                <Option<CqlValue>>::deserialize(column.spec.typ(), column.slice).map_err(
                    |err| {
                        mk_deser_err::<Self>(
                            BuiltinDeserializationErrorKind::ColumnDeserializationFailed {
                                column_index: column.index,
                                column_name: column.spec.name().to_owned(),
                                err,
                            },
                        )
                    },
                )?,
            );
        }
        Ok(Self { columns })
    }
}

// tuples
//
/// This is the new encouraged way for deserializing a row.
/// If only you know the exact column types in advance, you had better deserialize the row
/// to a tuple. The new deserialization framework will take care of all type checking
/// and needed conversions, issuing meaningful errors in case something goes wrong.
macro_rules! impl_tuple {
    ($($Ti:ident),*; $($idx:literal),*; $($idf:ident),*) => {
        impl<'frame, 'metadata, $($Ti),*> DeserializeRow<'frame, 'metadata> for ($($Ti,)*)
        where
            $($Ti: DeserializeValue<'frame, 'metadata>),*
        {
            fn type_check(specs: &[ColumnSpec]) -> Result<(), TypeCheckError> {
                const TUPLE_LEN: usize = (&[$($idx),*] as &[i32]).len();

                let column_types_iter = || specs.iter().map(|spec| spec.typ().clone().into_owned());
                if let [$($idf),*] = &specs {
                    $(
                        <$Ti as DeserializeValue<'frame, 'metadata>>::type_check($idf.typ())
                            .map_err(|err| mk_typck_err::<Self>(column_types_iter(), BuiltinTypeCheckErrorKind::ColumnTypeCheckFailed {
                                column_index: $idx,
                                column_name: specs[$idx].name().to_owned(),
                                err
                            }))?;
                    )*
                    Ok(())
                } else {
                    Err(mk_typck_err::<Self>(column_types_iter(), BuiltinTypeCheckErrorKind::WrongColumnCount {
                        rust_cols: TUPLE_LEN, cql_cols: specs.len()
                    }))
                }
            }

            fn deserialize(mut row: ColumnIterator<'frame, 'metadata>) -> Result<Self, DeserializationError> {
                const TUPLE_LEN: usize = (&[$($idx),*] as &[i32]).len();

                let ret = (
                    $({
                        let column = row.next().unwrap_or_else(|| unreachable!(
                            "Typecheck should have prevented this scenario! Column count mismatch: rust type {}, cql row {}",
                            TUPLE_LEN,
                            $idx
                        )).map_err(deser_error_replace_rust_name::<Self>)?;

                        <$Ti as DeserializeValue<'frame, 'metadata>>::deserialize(column.spec.typ(), column.slice)
                            .map_err(|err| mk_deser_err::<Self>(BuiltinDeserializationErrorKind::ColumnDeserializationFailed {
                                column_index: column.index,
                                column_name: column.spec.name().to_owned(),
                                err,
                            }))?
                    },)*
                );
                assert!(
                    row.next().is_none(),
                    "Typecheck should have prevented this scenario! Column count mismatch: rust type {}, cql row is bigger",
                    TUPLE_LEN,
                );
                Ok(ret)
            }
        }
    }
}

use super::value::impl_tuple_multiple;

// Implements row-to-tuple deserialization for all tuple sizes up to 16.
impl_tuple_multiple!(
    T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15;
    0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15;
    t0, t1, t2, t3, t4, t5, t6, t7, t8, t9, t10, t11, t12, t13, t14, t15
);

// Error facilities

/// Failed to type check incoming result column types again given Rust type,
/// one of the types having support built into the driver.
#[derive(Debug, Error, Clone)]
#[error("Failed to type check the Rust type {rust_name} against CQL column types {cql_types:?} : {kind}")]
pub struct BuiltinTypeCheckError {
    /// Name of the Rust type used to represent the values.
    pub rust_name: &'static str,

    /// The CQL types of the values that the Rust type was being deserialized from.
    pub cql_types: Vec<ColumnType<'static>>,

    /// Detailed information about the failure.
    pub kind: BuiltinTypeCheckErrorKind,
}

// Not part of the public API; used in derive macros.
#[doc(hidden)]
pub fn mk_typck_err<T>(
    cql_types: impl IntoIterator<Item = ColumnType<'static>>,
    kind: impl Into<BuiltinTypeCheckErrorKind>,
) -> TypeCheckError {
    mk_typck_err_named(std::any::type_name::<T>(), cql_types, kind)
}

fn mk_typck_err_named(
    name: &'static str,
    cql_types: impl IntoIterator<Item = ColumnType<'static>>,
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
pub enum BuiltinTypeCheckErrorKind {
    /// The Rust type expects `rust_cols` columns, but the statement operates on `cql_cols`.
    WrongColumnCount {
        /// The number of values that the Rust type provides.
        rust_cols: usize,

        /// The number of columns that the statement operates on.
        cql_cols: usize,
    },

    /// The CQL row contains a column for which a corresponding field is not found
    /// in the Rust type.
    ColumnWithUnknownName {
        /// Index of the excess column.
        column_index: usize,

        /// Name of the column that is present in CQL row but not in the Rust type.
        column_name: String,
    },

    /// Several values required by the Rust type are not provided by the DB.
    ValuesMissingForColumns {
        /// Names of the columns in the Rust type for which the DB doesn't
        /// provide value.
        column_names: Vec<&'static str>,
    },

    /// A different column name was expected at given position.
    ColumnNameMismatch {
        /// Index of the field determining the expected name.
        field_index: usize,

        /// Index of the column having mismatched name.
        column_index: usize,

        /// Name of the column, as expected by the Rust type.
        rust_column_name: &'static str,

        /// Name of the column for which the DB requested a value.
        db_column_name: String,
    },

    /// Column type check failed between Rust type and DB type at given position (=in given column).
    ColumnTypeCheckFailed {
        /// Index of the column.
        column_index: usize,

        /// Name of the column, as provided by the DB.
        column_name: String,

        /// Inner type check error due to the type mismatch.
        err: TypeCheckError,
    },

    /// Duplicated column in DB metadata.
    DuplicatedColumn {
        /// Column index of the second occurrence of the column with the same name.
        column_index: usize,

        /// The name of the duplicated column.
        column_name: &'static str,
    },
}

impl Display for BuiltinTypeCheckErrorKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            BuiltinTypeCheckErrorKind::WrongColumnCount {
                rust_cols,
                cql_cols,
            } => {
                write!(f, "wrong column count: the statement operates on {cql_cols} columns, but the given rust types contains {rust_cols}")
            }
            BuiltinTypeCheckErrorKind::ColumnWithUnknownName { column_name, column_index } => {
                write!(
                    f,
                    "the CQL row contains a column {} at column index {}, but the corresponding field is not found in the Rust type",
                    column_name,
                    column_index,
                )
            }
            BuiltinTypeCheckErrorKind::ValuesMissingForColumns { column_names } => {
                write!(
                    f,
                    "values for columns {:?} are missing from the DB data but are required by the Rust type",
                    column_names
                )
            },
            BuiltinTypeCheckErrorKind::ColumnNameMismatch {
                field_index,
                column_index,rust_column_name,
                db_column_name
            } => write!(
                f,
                "expected column with name {} at column index {}, but the Rust field name at corresponding field index {} is {}",
                db_column_name,
                column_index,
                field_index,
                rust_column_name,
            ),
            BuiltinTypeCheckErrorKind::ColumnTypeCheckFailed {
                column_index,
                column_name,
                err,
            } => write!(
                f,
                "mismatched types in column {column_name} at index {column_index}: {err}"
            ),
            BuiltinTypeCheckErrorKind::DuplicatedColumn { column_name, column_index } => write!(
                f,
                "column {} occurs more than once in DB metadata; second occurrence is at column index {}",
                column_name,
                column_index,
            ),
        }
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

// Not part of the public API; used in derive macros.
#[doc(hidden)]
pub fn mk_deser_err<T>(kind: impl Into<BuiltinDeserializationErrorKind>) -> DeserializationError {
    mk_deser_err_named(std::any::type_name::<T>(), kind)
}

fn mk_deser_err_named(
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
    /// One of the columns failed to deserialize.
    ColumnDeserializationFailed {
        /// Index of the column that failed to deserialize.
        column_index: usize,

        /// Name of the column that failed to deserialize.
        column_name: String,

        /// The error that caused the column deserialization to fail.
        err: DeserializationError,
    },

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
            BuiltinDeserializationErrorKind::ColumnDeserializationFailed {
                column_index,
                column_name,
                err,
            } => {
                write!(
                    f,
                    "failed to deserialize column {column_name} at index {column_index}: {err}"
                )
            }
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

#[cfg(test)]
#[path = "row_tests.rs"]
pub(crate) mod tests;

/// ```compile_fail
///
/// #[derive(scylla_macros::DeserializeRow)]
/// #[scylla(crate = scylla_cql, skip_name_checks)]
/// struct TestRow {}
/// ```
fn _test_struct_deserialization_name_check_skip_requires_enforce_order() {}

/// ```compile_fail
///
/// #[derive(scylla_macros::DeserializeRow)]
/// #[scylla(crate = scylla_cql, skip_name_checks)]
/// struct TestRow {
///     #[scylla(rename = "b")]
///     a: i32,
/// }
/// ```
fn _test_struct_deserialization_skip_name_check_conflicts_with_rename() {}

/// ```compile_fail
///
/// #[derive(scylla_macros::DeserializeRow)]
/// #[scylla(crate = scylla_cql)]
/// struct TestRow {
///     #[scylla(rename = "b")]
///     a: i32,
///     b: String,
/// }
/// ```
fn _test_struct_deserialization_skip_rename_collision_with_field() {}

/// ```compile_fail
///
/// #[derive(scylla_macros::DeserializeRow)]
/// #[scylla(crate = scylla_cql)]
/// struct TestRow {
///     #[scylla(rename = "c")]
///     a: i32,
///     #[scylla(rename = "c")]
///     b: String,
/// }
/// ```
fn _test_struct_deserialization_rename_collision_with_another_rename() {}
