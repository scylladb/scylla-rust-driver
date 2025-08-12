//! Contains the [`SerializeRow`] trait and its implementations.

// Note: When editing above doc-comment edit the corresponding comment on
// re-export module in scylla crate too.

use std::collections::{BTreeMap, HashSet};
use std::fmt::Display;
use std::hash::BuildHasher;
use std::{collections::HashMap, sync::Arc};

use bytes::BufMut;
use thiserror::Error;

use crate::frame::request::RequestDeserializationError;
use crate::frame::response::result::ColumnType;
use crate::frame::response::result::PreparedMetadata;
use crate::frame::types;
use crate::frame::{response::result::ColumnSpec, types::RawValue};

use super::value::SerializeValue;
use super::{CellWriter, RowWriter, SerializationError};

/// Contains information needed to serialize a row.
pub struct RowSerializationContext<'a> {
    pub(crate) columns: &'a [ColumnSpec<'a>],
}

impl<'a> RowSerializationContext<'a> {
    /// Creates the serialization context from prepared statement metadata.
    #[inline]
    pub fn from_prepared(prepared: &'a PreparedMetadata) -> Self {
        Self {
            columns: prepared.col_specs.as_slice(),
        }
    }

    /// Creates the serialization context directly from column specs.
    #[inline]
    pub fn from_specs(specs: &'a [ColumnSpec<'a>]) -> Self {
        Self { columns: specs }
    }

    /// Constructs an empty `RowSerializationContext`, as if for a statement
    /// with no bind markers.
    #[inline]
    pub const fn empty() -> Self {
        Self { columns: &[] }
    }

    /// Returns column/bind marker specifications for given query.
    #[inline]
    pub fn columns(&self) -> &'a [ColumnSpec<'_>] {
        self.columns
    }
}

/// Represents a set of values that can be sent along a CQL statement.
///
/// This is a low-level trait that is exposed to the specifics to the CQL
/// protocol and usually does not have to be implemented directly. See the
/// chapter on "Query Values" in the driver docs for information about how
/// this trait is supposed to be used.
pub trait SerializeRow {
    /// Serializes the row according to the information in the given context.
    ///
    /// It's the trait's responsibility to produce values in the order as
    /// specified in given serialization context.
    fn serialize(
        &self,
        ctx: &RowSerializationContext<'_>,
        writer: &mut RowWriter,
    ) -> Result<(), SerializationError>;

    /// Returns whether this row contains any values or not.
    ///
    /// This method is used before executing a simple statement in order to check
    /// whether there are any values provided to it. If there are some, then
    /// the query will be prepared first in order to obtain information about
    /// the bind marker types and names so that the values can be properly
    /// type checked and serialized.
    fn is_empty(&self) -> bool;
}

macro_rules! impl_serialize_row_for_unit {
    () => {
        fn serialize(
            &self,
            ctx: &RowSerializationContext<'_>,
            _writer: &mut RowWriter,
        ) -> Result<(), SerializationError> {
            if !ctx.columns().is_empty() {
                return Err(mk_typck_err::<Self>(
                    BuiltinTypeCheckErrorKind::WrongColumnCount {
                        rust_cols: 0,
                        cql_cols: ctx.columns().len(),
                    },
                ));
            }
            // Row is empty - do nothing
            Ok(())
        }

        #[inline]
        fn is_empty(&self) -> bool {
            true
        }
    };
}

impl SerializeRow for () {
    impl_serialize_row_for_unit!();
}

impl SerializeRow for [u8; 0] {
    impl_serialize_row_for_unit!();
}

macro_rules! impl_serialize_row_for_slice {
    () => {
        fn serialize(
            &self,
            ctx: &RowSerializationContext<'_>,
            writer: &mut RowWriter,
        ) -> Result<(), SerializationError> {
            if ctx.columns().len() != self.len() {
                return Err(mk_typck_err::<Self>(
                    BuiltinTypeCheckErrorKind::WrongColumnCount {
                        rust_cols: self.len(),
                        cql_cols: ctx.columns().len(),
                    },
                ));
            }
            for (col, val) in ctx.columns().iter().zip(self.iter()) {
                $crate::_macro_internal::ser::row::serialize_column::<Self>(val, col, writer)?;
            }
            Ok(())
        }

        #[inline]
        fn is_empty(&self) -> bool {
            <[T]>::is_empty(self.as_ref())
        }
    };
}

impl<'a, T: SerializeValue + 'a> SerializeRow for &'a [T] {
    impl_serialize_row_for_slice!();
}

impl<T: SerializeValue> SerializeRow for Vec<T> {
    impl_serialize_row_for_slice!();
}

impl<T: SerializeRow + ?Sized> SerializeRow for Box<T> {
    #[inline]
    fn serialize(
        &self,
        ctx: &RowSerializationContext<'_>,
        writer: &mut RowWriter,
    ) -> Result<(), SerializationError> {
        self.as_ref().serialize(ctx, writer)
    }

    #[inline]
    fn is_empty(&self) -> bool {
        self.as_ref().is_empty()
    }
}

macro_rules! impl_serialize_row_for_map {
    () => {
        fn serialize(
            &self,
            ctx: &RowSerializationContext<'_>,
            writer: &mut RowWriter,
        ) -> Result<(), SerializationError> {
            // Unfortunately, column names aren't guaranteed to be unique.
            // We need to track not-yet-used columns in order to see
            // whether some values were not used at the end, and report an error.
            let mut unused_columns: HashSet<&str> = self.keys().map(|k| k.as_ref()).collect();

            for col in ctx.columns.iter() {
                match self.get(col.name()) {
                    None => {
                        return Err(mk_typck_err::<Self>(
                            BuiltinTypeCheckErrorKind::ValueMissingForColumn {
                                name: col.name().to_owned(),
                            },
                        ))
                    }
                    Some(v) => {
                        $crate::_macro_internal::ser::row::serialize_column::<Self>(
                            v, col, writer,
                        )?;
                        let _ = unused_columns.remove(col.name());
                    }
                }
            }

            if !unused_columns.is_empty() {
                // Report the lexicographically first value for deterministic error messages
                let name = unused_columns.iter().min().unwrap();
                return Err(mk_typck_err::<Self>(
                    BuiltinTypeCheckErrorKind::NoColumnWithName {
                        name: name.to_string(),
                    },
                ));
            }

            Ok(())
        }

        #[inline]
        fn is_empty(&self) -> bool {
            Self::is_empty(self)
        }
    };
}

impl<T: SerializeValue> SerializeRow for BTreeMap<String, T> {
    impl_serialize_row_for_map!();
}

impl<T: SerializeValue> SerializeRow for BTreeMap<&str, T> {
    impl_serialize_row_for_map!();
}

impl<T: SerializeValue, S: BuildHasher> SerializeRow for HashMap<String, T, S> {
    impl_serialize_row_for_map!();
}

impl<T: SerializeValue, S: BuildHasher> SerializeRow for HashMap<&str, T, S> {
    impl_serialize_row_for_map!();
}

impl<T: SerializeRow + ?Sized> SerializeRow for &T {
    fn serialize(
        &self,
        ctx: &RowSerializationContext<'_>,
        writer: &mut RowWriter,
    ) -> Result<(), SerializationError> {
        <T as SerializeRow>::serialize(self, ctx, writer)
    }

    #[inline]
    fn is_empty(&self) -> bool {
        <T as SerializeRow>::is_empty(self)
    }
}

macro_rules! impl_tuple {
    (
        $($typs:ident),*;
        $($fidents:ident),*;
        $($tidents:ident),*;
        $length:expr
    ) => {
        impl<$($typs: SerializeValue),*> SerializeRow for ($($typs,)*) {
            fn serialize(
                &self,
                ctx: &RowSerializationContext<'_>,
                writer: &mut RowWriter,
            ) -> Result<(), SerializationError> {
                let ($($tidents,)*) = match ctx.columns() {
                    [$($tidents),*] => ($($tidents,)*),
                    _ => return Err(mk_typck_err::<Self>(
                        BuiltinTypeCheckErrorKind::WrongColumnCount {
                            rust_cols: $length,
                            cql_cols: ctx.columns().len(),
                        },
                    )),
                };
                let ($($fidents,)*) = self;
                $(
                    $crate::_macro_internal::ser::row::serialize_column::<Self>($fidents, $tidents, writer)?;
                )*
                Ok(())
            }

            #[inline]
            fn is_empty(&self) -> bool {
                $length == 0
            }
        }
    };
}

macro_rules! impl_tuples {
    (;;;$length:expr) => {};
    (
        $typ:ident$(, $($typs:ident),*)?;
        $fident:ident$(, $($fidents:ident),*)?;
        $tident:ident$(, $($tidents:ident),*)?;
        $length:expr
    ) => {
        impl_tuples!(
            $($($typs),*)?;
            $($($fidents),*)?;
            $($($tidents),*)?;
            $length - 1
        );
        impl_tuple!(
            $typ$(, $($typs),*)?;
            $fident$(, $($fidents),*)?;
            $tident$(, $($tidents),*)?;
            $length
        );
    };
}

impl_tuples!(
    T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15;
    f0, f1, f2, f3, f4, f5, f6, f7, f8, f9, f10, f11, f12, f13, f14, f15;
    t0, t1, t2, t3, t4, t5, t6, t7, t8, t9, t10, t11, t12, t13, t14, t15;
    16
);

/// Failed to type check values for a statement, represented by one of the types
/// built into the driver.
#[derive(Debug, Error, Clone)]
#[error("Failed to type check query arguments {rust_name}: {kind}")]
pub struct BuiltinTypeCheckError {
    /// Name of the Rust type used to represent the values.
    pub rust_name: &'static str,

    /// Detailed information about the failure.
    pub kind: BuiltinTypeCheckErrorKind,
}

#[doc(hidden)]
pub fn mk_typck_err<T>(kind: impl Into<BuiltinTypeCheckErrorKind>) -> SerializationError {
    mk_typck_err_named(std::any::type_name::<T>(), kind)
}

fn mk_typck_err_named(
    name: &'static str,
    kind: impl Into<BuiltinTypeCheckErrorKind>,
) -> SerializationError {
    SerializationError::new(BuiltinTypeCheckError {
        rust_name: name,
        kind: kind.into(),
    })
}

/// Failed to serialize values for a statement, represented by one of the types
/// built into the driver.
#[derive(Debug, Error, Clone)]
#[error("Failed to serialize query arguments {rust_name}: {kind}")]
pub struct BuiltinSerializationError {
    /// Name of the Rust type used to represent the values.
    pub rust_name: &'static str,

    /// Detailed information about the failure.
    pub kind: BuiltinSerializationErrorKind,
}

pub(crate) fn mk_ser_err<T>(kind: impl Into<BuiltinSerializationErrorKind>) -> SerializationError {
    mk_ser_err_named(std::any::type_name::<T>(), kind)
}

fn mk_ser_err_named(
    name: &'static str,
    kind: impl Into<BuiltinSerializationErrorKind>,
) -> SerializationError {
    SerializationError::new(BuiltinSerializationError {
        rust_name: name,
        kind: kind.into(),
    })
}

/// Describes why type checking values for a statement failed.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum BuiltinTypeCheckErrorKind {
    /// The Rust type provides `rust_cols` columns, but the statement operates on `cql_cols`.
    WrongColumnCount {
        /// The number of values that the Rust type provides.
        rust_cols: usize,

        /// The number of columns that the statement operates on.
        cql_cols: usize,
    },

    /// The Rust type provides a value for some column, but that column is not
    /// present in the statement.
    NoColumnWithName {
        /// Name of the column that is missing in the statement.
        name: String,
    },

    /// A value required by the statement is not provided by the Rust type.
    ValueMissingForColumn {
        /// Name of the column for which the Rust type doesn't
        /// provide a value.
        name: String,
    },

    /// A different column name was expected at given position.
    ColumnNameMismatch {
        /// Name of the column, as expected by the Rust type.
        rust_column_name: String,

        /// Name of the column for which the DB requested a value.
        db_column_name: String,
    },
}

impl Display for BuiltinTypeCheckErrorKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            BuiltinTypeCheckErrorKind::WrongColumnCount { rust_cols, cql_cols } => {
                write!(f, "wrong column count: the statement operates on {cql_cols} columns, but the given rust type provides {rust_cols}")
            }
            BuiltinTypeCheckErrorKind::NoColumnWithName { name } => {
                write!(
                    f,
                    "value for column {name} was provided, but there is no bind marker for this column in the query"
                )
            }
            BuiltinTypeCheckErrorKind::ValueMissingForColumn { name } => {
                write!(
                    f,
                    "value for column {name} was not provided, but the query requires it"
                )
            }
            BuiltinTypeCheckErrorKind::ColumnNameMismatch { rust_column_name, db_column_name } => write!(
                f,
                "expected column with name {db_column_name} at given position, but the Rust field name is {rust_column_name}"
            ),
        }
    }
}

/// Describes why serializing values for a statement failed.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum BuiltinSerializationErrorKind {
    /// One of the columns failed to serialize.
    ColumnSerializationFailed {
        /// Name of the column that failed to serialize.
        name: String,

        /// The error that caused the column serialization to fail.
        err: SerializationError,
    },
    /// Too many values to add, max 65,535 values can be sent in a request.
    TooManyValues,
}

impl Display for BuiltinSerializationErrorKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            BuiltinSerializationErrorKind::ColumnSerializationFailed { name, err } => {
                write!(f, "failed to serialize column {name}: {err}")
            }
            BuiltinSerializationErrorKind::TooManyValues => {
                write!(
                    f,
                    "Too many values to add, max 65,535 values can be sent in a request"
                )
            }
        }
    }
}

/// A buffer containing already serialized values.
///
/// It is not aware of the types of contained values,
/// it is basically a byte buffer in the format expected by the CQL protocol.
/// Usually there is no need for a user of a driver to use this struct, it is mostly internal.
/// The exception are APIs like `ClusterState::compute_token` / `ClusterState::get_endpoints`.
/// Allows adding new values to the buffer and iterating over the content.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct SerializedValues {
    serialized_values: Vec<u8>,
    element_count: u16,
}

impl SerializedValues {
    /// Constructs a new, empty `SerializedValues`.
    pub const fn new() -> Self {
        SerializedValues {
            serialized_values: Vec::new(),
            element_count: 0,
        }
    }

    /// A const empty instance, useful for taking references
    pub const EMPTY: &'static SerializedValues = &SerializedValues::new();

    /// Constructs `SerializedValues` from given [`SerializeRow`] object.
    pub fn from_serializable<T: SerializeRow + ?Sized>(
        ctx: &RowSerializationContext,
        row: &T,
    ) -> Result<Self, SerializationError> {
        Self::from_closure(|writer| row.serialize(ctx, writer)).map(|(sr, _)| sr)
    }

    /// Constructs `SerializedValues` via given closure.
    pub fn from_closure<F, R>(f: F) -> Result<(Self, R), SerializationError>
    where
        F: FnOnce(&mut RowWriter) -> Result<R, SerializationError>,
    {
        let mut data = Vec::new();
        let mut writer = RowWriter::new(&mut data);
        let ret = f(&mut writer)?;
        let element_count = match writer.value_count().try_into() {
            Ok(n) => n,
            Err(_) => {
                return Err(SerializationError(Arc::new(mk_ser_err::<Self>(
                    BuiltinSerializationErrorKind::TooManyValues,
                ))));
            }
        };

        Ok((
            SerializedValues {
                serialized_values: data,
                element_count,
            },
            ret,
        ))
    }

    /// Returns `true` if the row contains no elements.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.element_count() == 0
    }

    /// Returns an iterator over the values serialized into the object so far.
    #[inline]
    pub fn iter(&self) -> impl Iterator<Item = RawValue<'_>> {
        SerializedValuesIterator {
            serialized_values: &self.serialized_values,
        }
    }

    /// Returns the number of values written so far.
    #[inline]
    pub fn element_count(&self) -> u16 {
        self.element_count
    }

    /// Returns the total serialized size of the values written so far.
    #[inline]
    pub fn buffer_size(&self) -> usize {
        self.serialized_values.len()
    }

    pub(crate) fn write_to_request(&self, buf: &mut impl BufMut) {
        buf.put_u16(self.element_count);
        buf.put(self.serialized_values.as_slice())
    }

    // Gets the serialized values as raw bytes, without the preceding u16 length.
    pub(crate) fn get_contents(&self) -> &[u8] {
        &self.serialized_values
    }

    /// Serializes value and appends it to the list
    pub fn add_value<T: SerializeValue>(
        &mut self,
        val: &T,
        typ: &ColumnType,
    ) -> Result<(), SerializationError> {
        if self.element_count() == u16::MAX {
            return Err(SerializationError(Arc::new(mk_ser_err::<Self>(
                BuiltinSerializationErrorKind::TooManyValues,
            ))));
        }

        let len_before_serialize: usize = self.serialized_values.len();

        let writer = CellWriter::new(&mut self.serialized_values);
        if let Err(e) = val.serialize(typ, writer) {
            self.serialized_values.resize(len_before_serialize, 0);
            Err(e)
        } else {
            self.element_count += 1;
            Ok(())
        }
    }

    /// Creates value list from the request frame
    /// This is used only for testing - request deserialization.
    pub(crate) fn new_from_frame(buf: &mut &[u8]) -> Result<Self, RequestDeserializationError> {
        let values_num = types::read_short(buf)?;
        let values_beg = *buf;
        for _ in 0..values_num {
            let _serialized = types::read_value(buf)?;
        }

        let values_len_in_buf = values_beg.len() - buf.len();
        let values_in_frame = &values_beg[0..values_len_in_buf];
        Ok(SerializedValues {
            serialized_values: values_in_frame.to_vec(),
            element_count: values_num,
        })
    }
}

impl Default for SerializedValues {
    fn default() -> Self {
        Self::new()
    }
}

/// An iterator over raw values in some [`SerializedValues`].
#[derive(Clone, Copy)]
pub struct SerializedValuesIterator<'a> {
    serialized_values: &'a [u8],
}

impl<'a> Iterator for SerializedValuesIterator<'a> {
    type Item = RawValue<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.serialized_values.is_empty() {
            return None;
        }

        Some(types::read_value(&mut self.serialized_values).expect("badly encoded value"))
    }
}

mod doctests {

    /// ```compile_fail
    ///
    /// #[derive(scylla_macros::SerializeRow)]
    /// #[scylla(crate = scylla_cql, skip_name_checks)]
    /// struct TestRow {}
    /// ```
    fn _test_struct_deserialization_name_check_skip_requires_enforce_order() {}

    /// ```compile_fail
    ///
    /// #[derive(scylla_macros::SerializeRow)]
    /// #[scylla(crate = scylla_cql, skip_name_checks)]
    /// struct TestRow {
    ///     #[scylla(rename = "b")]
    ///     a: i32,
    /// }
    /// ```
    fn _test_struct_deserialization_skip_name_check_conflicts_with_rename() {}

    /// ```compile_fail
    ///
    /// #[derive(scylla_macros::SerializeRow)]
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
    /// #[derive(scylla_macros::SerializeRow)]
    /// #[scylla(crate = scylla_cql)]
    /// struct TestRow {
    ///     #[scylla(rename = "c")]
    ///     a: i32,
    ///     #[scylla(rename = "c")]
    ///     b: String,
    /// }
    /// ```
    fn _test_struct_deserialization_rename_collision_with_another_rename() {}
}

#[cfg(test)]
#[path = "row_tests.rs"]
pub(crate) mod tests;
