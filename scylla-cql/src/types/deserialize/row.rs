//! Provides types for dealing with row deserialization.

use std::fmt::Display;

use thiserror::Error;

use super::value::DeserializeValue;
use super::{make_error_replace_rust_name, DeserializationError, FrameSlice, TypeCheckError};
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

// tuples
//
/// This is the new encouraged way for deserializing a row.
/// If only you know the exact column types in advance, you had better deserialize the row
/// to a tuple. The new deserialization framework will take care of all type checking
/// and needed conversions, issuing meaningful errors in case something goes wrong.
macro_rules! impl_tuple {
    ($($Ti:ident),*; $($idx:literal),*; $($idf:ident),*) => {
        impl<'frame, $($Ti),*> DeserializeRow<'frame> for ($($Ti,)*)
        where
            $($Ti: DeserializeValue<'frame>),*
        {
            fn type_check(specs: &[ColumnSpec]) -> Result<(), TypeCheckError> {
                const TUPLE_LEN: usize = (&[$($idx),*] as &[i32]).len();

                let column_types_iter = || specs.iter().map(|spec| spec.typ.clone());
                if let [$($idf),*] = &specs {
                    $(
                        <$Ti as DeserializeValue<'frame>>::type_check(&$idf.typ)
                            .map_err(|err| mk_typck_err::<Self>(column_types_iter(), BuiltinTypeCheckErrorKind::ColumnTypeCheckFailed {
                                column_index: $idx,
                                column_name: specs[$idx].name.clone(),
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

            fn deserialize(mut row: ColumnIterator<'frame>) -> Result<Self, DeserializationError> {
                const TUPLE_LEN: usize = (&[$($idx),*] as &[i32]).len();

                let ret = (
                    $({
                        let column = row.next().unwrap_or_else(|| unreachable!(
                            "Typecheck should have prevented this scenario! Column count mismatch: rust type {}, cql row {}",
                            TUPLE_LEN,
                            $idx
                        )).map_err(deser_error_replace_rust_name::<Self>)?;

                        <$Ti as DeserializeValue<'frame>>::deserialize(&column.spec.typ, column.slice)
                            .map_err(|err| mk_deser_err::<Self>(BuiltinDeserializationErrorKind::ColumnDeserializationFailed {
                                column_index: column.index,
                                column_name: column.spec.name.clone(),
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
pub enum BuiltinTypeCheckErrorKind {
    /// The Rust type expects `rust_cols` columns, but the statement operates on `cql_cols`.
    WrongColumnCount {
        /// The number of values that the Rust type provides.
        rust_cols: usize,

        /// The number of columns that the statement operates on.
        cql_cols: usize,
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
            BuiltinTypeCheckErrorKind::ColumnTypeCheckFailed {
                column_index,
                column_name,
                err,
            } => write!(
                f,
                "mismatched types in column {column_name} at index {column_index}: {err}"
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
mod tests {
    use bytes::Bytes;

    use crate::frame::response::result::{ColumnSpec, ColumnType};
    use crate::types::deserialize::{DeserializationError, FrameSlice};

    use super::super::tests::{serialize_cells, spec};
    use super::{ColumnIterator, DeserializeRow};

    #[test]
    fn test_tuple_deserialization() {
        // Empty tuple
        deserialize::<()>(&[], &Bytes::new()).unwrap();

        // 1-elem tuple
        let (a,) = deserialize::<(i32,)>(
            &[spec("i", ColumnType::Int)],
            &serialize_cells([val_int(123)]),
        )
        .unwrap();
        assert_eq!(a, 123);

        // 3-elem tuple
        let (a, b, c) = deserialize::<(i32, i32, i32)>(
            &[
                spec("i1", ColumnType::Int),
                spec("i2", ColumnType::Int),
                spec("i3", ColumnType::Int),
            ],
            &serialize_cells([val_int(123), val_int(456), val_int(789)]),
        )
        .unwrap();
        assert_eq!((a, b, c), (123, 456, 789));

        // Make sure that column type mismatch is detected
        deserialize::<(i32, String, i32)>(
            &[
                spec("i1", ColumnType::Int),
                spec("i2", ColumnType::Int),
                spec("i3", ColumnType::Int),
            ],
            &serialize_cells([val_int(123), val_int(456), val_int(789)]),
        )
        .unwrap_err();

        // Make sure that borrowing types compile and work correctly
        let specs = &[spec("s", ColumnType::Text)];
        let byts = serialize_cells([val_str("abc")]);
        let (s,) = deserialize::<(&str,)>(specs, &byts).unwrap();
        assert_eq!(s, "abc");
    }

    #[test]
    fn test_deserialization_as_column_iterator() {
        let col_specs = [
            spec("i1", ColumnType::Int),
            spec("i2", ColumnType::Text),
            spec("i3", ColumnType::Counter),
        ];
        let serialized_values = serialize_cells([val_int(123), val_str("ScyllaDB"), None]);
        let mut iter = deserialize::<ColumnIterator>(&col_specs, &serialized_values).unwrap();

        let col1 = iter.next().unwrap().unwrap();
        assert_eq!(col1.spec.name, "i1");
        assert_eq!(col1.spec.typ, ColumnType::Int);
        assert_eq!(col1.slice.unwrap().as_slice(), &123i32.to_be_bytes());

        let col2 = iter.next().unwrap().unwrap();
        assert_eq!(col2.spec.name, "i2");
        assert_eq!(col2.spec.typ, ColumnType::Text);
        assert_eq!(col2.slice.unwrap().as_slice(), "ScyllaDB".as_bytes());

        let col3 = iter.next().unwrap().unwrap();
        assert_eq!(col3.spec.name, "i3");
        assert_eq!(col3.spec.typ, ColumnType::Counter);
        assert!(col3.slice.is_none());

        assert!(iter.next().is_none());
    }

    fn val_int(i: i32) -> Option<Vec<u8>> {
        Some(i.to_be_bytes().to_vec())
    }

    fn val_str(s: &str) -> Option<Vec<u8>> {
        Some(s.as_bytes().to_vec())
    }

    fn deserialize<'frame, R>(
        specs: &'frame [ColumnSpec],
        byts: &'frame Bytes,
    ) -> Result<R, DeserializationError>
    where
        R: DeserializeRow<'frame>,
    {
        <R as DeserializeRow<'frame>>::type_check(specs)
            .map_err(|typecheck_err| DeserializationError(typecheck_err.0))?;
        let slice = FrameSlice::new(byts);
        let iter = ColumnIterator::new(specs, slice);
        <R as DeserializeRow<'frame>>::deserialize(iter)
    }
}
