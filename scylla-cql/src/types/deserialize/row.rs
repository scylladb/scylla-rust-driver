//! Provides types for dealing with row deserialization.

use std::fmt::Display;

use thiserror::Error;

use super::value::DeserializeValue;
use super::{make_error_replace_rust_name, DeserializationError, FrameSlice, TypeCheckError};
use crate::frame::response::result::{ColumnSpec, ColumnType, CqlValue, Row};

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

// legacy/dynamic deserialization as Row
//
/// While no longer encouraged (because the new framework encourages deserializing
/// directly into desired types, entirely bypassing [CqlValue]), this can be indispensable
/// for some use cases, i.e. those involving dynamic parsing (ORMs?).
impl<'frame> DeserializeRow<'frame> for Row {
    #[inline]
    fn type_check(_specs: &[ColumnSpec]) -> Result<(), TypeCheckError> {
        // CqlValues accept all types, no type checking needed.
        Ok(())
    }

    #[inline]
    fn deserialize(mut row: ColumnIterator<'frame>) -> Result<Self, DeserializationError> {
        let mut columns = Vec::with_capacity(row.size_hint().0);
        while let Some(column) = row
            .next()
            .transpose()
            .map_err(deser_error_replace_rust_name::<Self>)?
        {
            columns.push(
                <Option<CqlValue>>::deserialize(&column.spec.typ, column.slice).map_err(|err| {
                    mk_deser_err::<Self>(
                        BuiltinDeserializationErrorKind::ColumnDeserializationFailed {
                            column_index: column.index,
                            column_name: column.spec.name.clone(),
                            err,
                        },
                    )
                })?,
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

// Not part of the public API; used in derive macros.
#[doc(hidden)]
pub fn mk_typck_err<T>(
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
        /// Column index of the second occurence of the column with the same name.
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
                "column {} occurs more than once in DB metadata; second occurence is at column index {}",
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
mod tests {
    use assert_matches::assert_matches;
    use bytes::Bytes;
    use scylla_macros::DeserializeRow;

    use crate::frame::response::result::{ColumnSpec, ColumnType};
    use crate::types::deserialize::row::BuiltinDeserializationErrorKind;
    use crate::types::deserialize::{value, DeserializationError, FrameSlice};

    use super::super::tests::{serialize_cells, spec};
    use super::{BuiltinDeserializationError, ColumnIterator, CqlValue, DeserializeRow, Row};
    use super::{BuiltinTypeCheckError, BuiltinTypeCheckErrorKind};

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

    // Do not remove. It's not used in tests but we keep it here to check that
    // we properly ignore warnings about unused variables, unnecessary `mut`s
    // etc. that usually pop up when generating code for empty structs.
    #[allow(unused)]
    #[derive(DeserializeRow)]
    #[scylla(crate = crate)]
    struct TestUdtWithNoFieldsUnordered {}

    #[allow(unused)]
    #[derive(DeserializeRow)]
    #[scylla(crate = crate, enforce_order)]
    struct TestUdtWithNoFieldsOrdered {}

    #[test]
    fn test_struct_deserialization_loose_ordering() {
        #[derive(DeserializeRow, PartialEq, Eq, Debug)]
        #[scylla(crate = "crate")]
        struct MyRow<'a> {
            a: &'a str,
            b: Option<i32>,
            #[scylla(skip)]
            c: String,
        }

        // Original order of columns
        let specs = &[spec("a", ColumnType::Text), spec("b", ColumnType::Int)];
        let byts = serialize_cells([val_str("abc"), val_int(123)]);
        let row = deserialize::<MyRow<'_>>(specs, &byts).unwrap();
        assert_eq!(
            row,
            MyRow {
                a: "abc",
                b: Some(123),
                c: String::new(),
            }
        );

        // Different order of columns - should still work
        let specs = &[spec("b", ColumnType::Int), spec("a", ColumnType::Text)];
        let byts = serialize_cells([val_int(123), val_str("abc")]);
        let row = deserialize::<MyRow<'_>>(specs, &byts).unwrap();
        assert_eq!(
            row,
            MyRow {
                a: "abc",
                b: Some(123),
                c: String::new(),
            }
        );

        // Missing column
        let specs = &[spec("a", ColumnType::Text)];
        MyRow::type_check(specs).unwrap_err();

        // Wrong column type
        let specs = &[spec("a", ColumnType::Int), spec("b", ColumnType::Int)];
        MyRow::type_check(specs).unwrap_err();
    }

    #[test]
    fn test_struct_deserialization_strict_ordering() {
        #[derive(DeserializeRow, PartialEq, Eq, Debug)]
        #[scylla(crate = "crate", enforce_order)]
        struct MyRow<'a> {
            a: &'a str,
            b: Option<i32>,
            #[scylla(skip)]
            c: String,
        }

        // Correct order of columns
        let specs = &[spec("a", ColumnType::Text), spec("b", ColumnType::Int)];
        let byts = serialize_cells([val_str("abc"), val_int(123)]);
        let row = deserialize::<MyRow<'_>>(specs, &byts).unwrap();
        assert_eq!(
            row,
            MyRow {
                a: "abc",
                b: Some(123),
                c: String::new(),
            }
        );

        // Wrong order of columns
        let specs = &[spec("b", ColumnType::Int), spec("a", ColumnType::Text)];
        MyRow::type_check(specs).unwrap_err();

        // Missing column
        let specs = &[spec("a", ColumnType::Text)];
        MyRow::type_check(specs).unwrap_err();

        // Wrong column type
        let specs = &[spec("a", ColumnType::Int), spec("b", ColumnType::Int)];
        MyRow::type_check(specs).unwrap_err();
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

    #[track_caller]
    pub(crate) fn get_typck_err_inner<'a>(
        err: &'a (dyn std::error::Error + 'static),
    ) -> &'a BuiltinTypeCheckError {
        match err.downcast_ref() {
            Some(err) => err,
            None => panic!("not a BuiltinTypeCheckError: {:?}", err),
        }
    }

    #[track_caller]
    fn get_typck_err(err: &DeserializationError) -> &BuiltinTypeCheckError {
        get_typck_err_inner(err.0.as_ref())
    }

    #[track_caller]
    fn get_deser_err(err: &DeserializationError) -> &BuiltinDeserializationError {
        match err.0.downcast_ref() {
            Some(err) => err,
            None => panic!("not a BuiltinDeserializationError: {:?}", err),
        }
    }

    #[test]
    fn test_tuple_errors() {
        // Column type check failure
        {
            let col_name: &str = "i";
            let specs = &[spec(col_name, ColumnType::Int)];
            let err = deserialize::<(i64,)>(specs, &serialize_cells([val_int(123)])).unwrap_err();
            let err = get_typck_err(&err);
            assert_eq!(err.rust_name, std::any::type_name::<(i64,)>());
            assert_eq!(
                err.cql_types,
                specs
                    .iter()
                    .map(|spec| spec.typ.clone())
                    .collect::<Vec<_>>()
            );
            let BuiltinTypeCheckErrorKind::ColumnTypeCheckFailed {
                column_index,
                column_name,
                err,
            } = &err.kind
            else {
                panic!("unexpected error kind: {}", err.kind)
            };
            assert_eq!(*column_index, 0);
            assert_eq!(column_name, col_name);
            let err = super::super::value::tests::get_typeck_err_inner(err.0.as_ref());
            assert_eq!(err.rust_name, std::any::type_name::<i64>());
            assert_eq!(err.cql_type, ColumnType::Int);
            assert_matches!(
                &err.kind,
                super::super::value::BuiltinTypeCheckErrorKind::MismatchedType {
                    expected: &[ColumnType::BigInt, ColumnType::Counter]
                }
            );
        }

        // Column deserialization failure
        {
            let col_name: &str = "i";
            let err = deserialize::<(i64,)>(
                &[spec(col_name, ColumnType::BigInt)],
                &serialize_cells([val_int(123)]),
            )
            .unwrap_err();
            let err = get_deser_err(&err);
            assert_eq!(err.rust_name, std::any::type_name::<(i64,)>());
            let BuiltinDeserializationErrorKind::ColumnDeserializationFailed {
                column_name,
                err,
                ..
            } = &err.kind
            else {
                panic!("unexpected error kind: {}", err.kind)
            };
            assert_eq!(column_name, col_name);
            let err = super::super::value::tests::get_deser_err(err);
            assert_eq!(err.rust_name, std::any::type_name::<i64>());
            assert_eq!(err.cql_type, ColumnType::BigInt);
            assert_matches!(
                err.kind,
                super::super::value::BuiltinDeserializationErrorKind::ByteLengthMismatch {
                    expected: 8,
                    got: 4
                }
            );
        }

        // Raw column deserialization failure
        {
            let col_name: &str = "i";
            let err = deserialize::<(i64,)>(
                &[spec(col_name, ColumnType::BigInt)],
                &Bytes::from_static(b"alamakota"),
            )
            .unwrap_err();
            let err = get_deser_err(&err);
            assert_eq!(err.rust_name, std::any::type_name::<(i64,)>());
            let BuiltinDeserializationErrorKind::RawColumnDeserializationFailed {
                column_index: _column_index,
                column_name,
                err: _err,
            } = &err.kind
            else {
                panic!("unexpected error kind: {}", err.kind)
            };
            assert_eq!(column_name, col_name);
        }
    }

    #[test]
    fn test_row_errors() {
        // Column type check failure - happens never, because Row consists of CqlValues,
        // which accept all CQL types.

        // Column deserialization failure
        {
            let col_name: &str = "i";
            let err = deserialize::<Row>(
                &[spec(col_name, ColumnType::BigInt)],
                &serialize_cells([val_int(123)]),
            )
            .unwrap_err();
            let err = get_deser_err(&err);
            assert_eq!(err.rust_name, std::any::type_name::<Row>());
            let BuiltinDeserializationErrorKind::ColumnDeserializationFailed {
                column_index: _column_index,
                column_name,
                err,
            } = &err.kind
            else {
                panic!("unexpected error kind: {}", err.kind)
            };
            assert_eq!(column_name, col_name);
            let err = super::super::value::tests::get_deser_err(err);
            assert_eq!(err.rust_name, std::any::type_name::<CqlValue>());
            assert_eq!(err.cql_type, ColumnType::BigInt);
            let super::super::value::BuiltinDeserializationErrorKind::ByteLengthMismatch {
                expected: 8,
                got: 4,
            } = &err.kind
            else {
                panic!("unexpected error kind: {}", err.kind)
            };
        }

        // Raw column deserialization failure
        {
            let col_name: &str = "i";
            let err = deserialize::<Row>(
                &[spec(col_name, ColumnType::BigInt)],
                &Bytes::from_static(b"alamakota"),
            )
            .unwrap_err();
            let err = get_deser_err(&err);
            assert_eq!(err.rust_name, std::any::type_name::<Row>());
            let BuiltinDeserializationErrorKind::RawColumnDeserializationFailed {
                column_index: _column_index,
                column_name,
                err: _err,
            } = &err.kind
            else {
                panic!("unexpected error kind: {}", err.kind)
            };
            assert_eq!(column_name, col_name);
        }
    }

    fn specs_to_types(specs: &[ColumnSpec]) -> Vec<ColumnType> {
        specs.iter().map(|spec| spec.typ.clone()).collect()
    }

    #[test]
    fn test_struct_deserialization_errors() {
        // Loose ordering
        {
            #[derive(scylla_macros::DeserializeRow, PartialEq, Eq, Debug)]
            #[scylla(crate = "crate")]
            struct MyRow<'a> {
                a: &'a str,
                #[scylla(skip)]
                x: String,
                b: Option<i32>,
                #[scylla(rename = "c")]
                d: bool,
            }

            // Type check errors
            {
                // Missing column
                {
                    let specs = [spec("a", ColumnType::Ascii), spec("b", ColumnType::Int)];
                    let err = MyRow::type_check(&specs).unwrap_err();
                    let err = get_typck_err_inner(err.0.as_ref());
                    assert_eq!(err.rust_name, std::any::type_name::<MyRow>());
                    assert_eq!(err.cql_types, specs_to_types(&specs));
                    let BuiltinTypeCheckErrorKind::ValuesMissingForColumns {
                        column_names: ref missing_fields,
                    } = err.kind
                    else {
                        panic!("unexpected error kind: {:?}", err.kind)
                    };
                    assert_eq!(missing_fields.as_slice(), &["c"]);
                }

                // Duplicated column
                {
                    let specs = [
                        spec("a", ColumnType::Ascii),
                        spec("b", ColumnType::Int),
                        spec("a", ColumnType::Ascii),
                    ];

                    let err = MyRow::type_check(&specs).unwrap_err();
                    let err = get_typck_err_inner(err.0.as_ref());
                    assert_eq!(err.rust_name, std::any::type_name::<MyRow>());
                    assert_eq!(err.cql_types, specs_to_types(&specs));
                    let BuiltinTypeCheckErrorKind::DuplicatedColumn {
                        column_index,
                        column_name,
                    } = err.kind
                    else {
                        panic!("unexpected error kind: {:?}", err.kind)
                    };
                    assert_eq!(column_index, 2);
                    assert_eq!(column_name, "a");
                }

                // Unknown column
                {
                    let specs = [
                        spec("d", ColumnType::Counter),
                        spec("a", ColumnType::Ascii),
                        spec("b", ColumnType::Int),
                    ];

                    let err = MyRow::type_check(&specs).unwrap_err();
                    let err = get_typck_err_inner(err.0.as_ref());
                    assert_eq!(err.rust_name, std::any::type_name::<MyRow>());
                    assert_eq!(err.cql_types, specs_to_types(&specs));
                    let BuiltinTypeCheckErrorKind::ColumnWithUnknownName {
                        column_index,
                        ref column_name,
                    } = err.kind
                    else {
                        panic!("unexpected error kind: {:?}", err.kind)
                    };
                    assert_eq!(column_index, 0);
                    assert_eq!(column_name.as_str(), "d");
                }

                // Column incompatible types - column type check failed
                {
                    let specs = [spec("b", ColumnType::Int), spec("a", ColumnType::Blob)];
                    let err = MyRow::type_check(&specs).unwrap_err();
                    let err = get_typck_err_inner(err.0.as_ref());
                    assert_eq!(err.rust_name, std::any::type_name::<MyRow>());
                    assert_eq!(err.cql_types, specs_to_types(&specs));
                    let BuiltinTypeCheckErrorKind::ColumnTypeCheckFailed {
                        column_index,
                        ref column_name,
                        ref err,
                    } = err.kind
                    else {
                        panic!("unexpected error kind: {:?}", err.kind)
                    };
                    assert_eq!(column_index, 1);
                    assert_eq!(column_name.as_str(), "a");
                    let err = value::tests::get_typeck_err_inner(err.0.as_ref());
                    assert_eq!(err.rust_name, std::any::type_name::<&str>());
                    assert_eq!(err.cql_type, ColumnType::Blob);
                    assert_matches!(
                        err.kind,
                        value::BuiltinTypeCheckErrorKind::MismatchedType {
                            expected: &[ColumnType::Ascii, ColumnType::Text]
                        }
                    );
                }
            }

            // Deserialization errors
            {
                // Got null
                {
                    let specs = [
                        spec("c", ColumnType::Boolean),
                        spec("a", ColumnType::Blob),
                        spec("b", ColumnType::Int),
                    ];

                    let err = MyRow::deserialize(ColumnIterator::new(
                        &specs,
                        FrameSlice::new(&serialize_cells([Some([true as u8])])),
                    ))
                    .unwrap_err();
                    let err = get_deser_err(&err);
                    assert_eq!(err.rust_name, std::any::type_name::<MyRow>());
                    let BuiltinDeserializationErrorKind::RawColumnDeserializationFailed {
                        column_index,
                        ref column_name,
                        ..
                    } = err.kind
                    else {
                        panic!("unexpected error kind: {:?}", err.kind)
                    };
                    assert_eq!(column_index, 1);
                    assert_eq!(column_name, "a");
                }

                // Column deserialization failed
                {
                    let specs = [
                        spec("b", ColumnType::Int),
                        spec("a", ColumnType::Ascii),
                        spec("c", ColumnType::Boolean),
                    ];

                    let row_bytes = serialize_cells(
                        [
                            &0_i32.to_be_bytes(),
                            "alamakota".as_bytes(),
                            &42_i16.to_be_bytes(),
                        ]
                        .map(Some),
                    );

                    let err = deserialize::<MyRow>(&specs, &row_bytes).unwrap_err();

                    let err = get_deser_err(&err);
                    assert_eq!(err.rust_name, std::any::type_name::<MyRow>());
                    let BuiltinDeserializationErrorKind::ColumnDeserializationFailed {
                        column_index,
                        ref column_name,
                        ref err,
                    } = err.kind
                    else {
                        panic!("unexpected error kind: {:?}", err.kind)
                    };
                    assert_eq!(column_index, 2);
                    assert_eq!(column_name.as_str(), "c");
                    let err = value::tests::get_deser_err(err);
                    assert_eq!(err.rust_name, std::any::type_name::<bool>());
                    assert_eq!(err.cql_type, ColumnType::Boolean);
                    assert_matches!(
                        err.kind,
                        value::BuiltinDeserializationErrorKind::ByteLengthMismatch {
                            expected: 1,
                            got: 2,
                        }
                    );
                }
            }
        }
    }
}
