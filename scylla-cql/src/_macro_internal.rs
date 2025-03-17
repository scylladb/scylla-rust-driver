pub use crate::frame::response::result::{ColumnSpec, ColumnType};
pub use crate::{DeserializeRow, DeserializeValue, SerializeRow, SerializeValue};

pub use crate::deserialize::row::{
    deser_error_replace_rust_name as row_deser_error_replace_rust_name,
    mk_deser_err as mk_row_deser_err, mk_typck_err as mk_row_typck_err,
    BuiltinDeserializationError as BuiltinRowDeserializationError,
    BuiltinDeserializationErrorKind as BuiltinRowDeserializationErrorKind,
    BuiltinTypeCheckErrorKind as DeserBuiltinRowTypeCheckErrorKind, ColumnIterator, DeserializeRow,
};
pub use crate::deserialize::value::{
    deser_error_replace_rust_name as value_deser_error_replace_rust_name,
    mk_deser_err as mk_value_deser_err, mk_typck_err as mk_value_typck_err,
    BuiltinDeserializationError as BuiltinTypeDeserializationError,
    BuiltinDeserializationErrorKind as BuiltinTypeDeserializationErrorKind,
    BuiltinTypeCheckErrorKind as DeserBuiltinTypeTypeCheckErrorKind, DeserializeValue,
    UdtDeserializationErrorKind, UdtIterator, UdtTypeCheckErrorKind as DeserUdtTypeCheckErrorKind,
};
pub use crate::deserialize::{DeserializationError, FrameSlice, TypeCheckError};
pub use crate::serialize::row::{
    BuiltinSerializationError as BuiltinRowSerializationError,
    BuiltinSerializationErrorKind as BuiltinRowSerializationErrorKind,
    BuiltinTypeCheckError as BuiltinRowTypeCheckError,
    BuiltinTypeCheckErrorKind as BuiltinRowTypeCheckErrorKind, RowSerializationContext,
    SerializeRow,
};
pub use crate::serialize::value::{
    BuiltinSerializationError as BuiltinTypeSerializationError,
    BuiltinSerializationErrorKind as BuiltinTypeSerializationErrorKind,
    BuiltinTypeCheckError as BuiltinTypeTypeCheckError,
    BuiltinTypeCheckErrorKind as BuiltinTypeTypeCheckErrorKind, SerializeValue,
    UdtSerializationErrorKind, UdtTypeCheckErrorKind,
};
pub use crate::serialize::writers::WrittenCellProof;
pub use crate::serialize::{CellValueBuilder, CellWriter, RowWriter, SerializationError};

/// Represents a set of values that can be sent along a CQL statement when serializing by name
///
/// For now this trait is an implementation detail of `#[derive(SerializeRow)]` when
/// serializing by name
pub trait SerializeRowByName {
    /// A type that can handle serialization of this struct column-by-column
    type Partial<'d>: PartialSerializeRowByName
    where
        Self: 'd;

    /// Returns a type that can serialize this row "column-by-column"
    fn partial(&self) -> Self::Partial<'_>;
}

/// How to serialize a row column-by-column
///
/// For now this trait is an implementation detail of `#[derive(SerializeRow)]` when
/// serializing by name
pub trait PartialSerializeRowByName {
    /// Serializes a single column in the row according to the information in the
    /// given context
    ///
    /// It returns whether the column finished the serialization of the struct, did
    /// it partially, none of at all, or errored
    fn serialize_field(
        &mut self,
        spec: &ColumnSpec,
        writer: &mut RowWriter<'_>,
    ) -> Result<self::ser::row::FieldStatus, SerializationError>;

    /// Checks if there are any missing columns to finish the serialization
    fn check_missing(self) -> Result<(), SerializationError>;
}

pub mod ser {
    pub mod row {
        use super::super::{PartialSerializeRowByName, SerializeRowByName};
        use crate::{
            frame::response::result::ColumnSpec,
            serialize::{
                row::{
                    mk_ser_err, BuiltinSerializationErrorKind, BuiltinTypeCheckErrorKind,
                    RowSerializationContext,
                },
                value::SerializeValue,
                writers::WrittenCellProof,
                RowWriter, SerializationError,
            },
        };

        pub use crate::serialize::row::mk_typck_err;

        /// Serializes a single value coming from type T into the writer
        ///
        /// `T` is not used for any sanity nor logical checks; it is only used when creating an
        /// error message.
        #[inline]
        pub fn serialize_column<'b, T>(
            value: &impl SerializeValue,
            spec: &ColumnSpec,
            writer: &'b mut RowWriter<'_>,
        ) -> Result<WrittenCellProof<'b>, SerializationError> {
            let sub_writer = writer.make_cell_writer();
            value.serialize(spec.typ(), sub_writer).map_err(|err| {
                mk_ser_err::<T>(BuiltinSerializationErrorKind::ColumnSerializationFailed {
                    name: spec.name().to_owned(),
                    err,
                })
            })
        }

        /// Whether a field used a column to finish its serialization or not
        ///
        /// Used when serializing by name as a single column may not have finished a rust
        /// field in the case of a flattened struct
        ///
        /// For now this enum is an implementation detail of `#[derive(SerializeRow)]` when
        /// serializing by name
        #[derive(Debug)]
        pub enum FieldStatus {
            /// The column finished the serialization for this field
            Done,
            /// The column was used but there are other fields not yet serialized
            NotDone,
            /// The column did not belong to this field
            NotUsed,
        }

        pub struct ByName<'t, T: SerializeRowByName>(pub &'t T);

        impl<T: SerializeRowByName> ByName<'_, T> {
            /// Serializes all the fields/columns by name
            pub fn serialize(
                self,
                ctx: &RowSerializationContext,
                writer: &mut RowWriter<'_>,
            ) -> Result<(), SerializationError> {
                let mut partial = self.0.partial();

                for spec in ctx.columns() {
                    let serialized = partial.serialize_field(spec, writer)?;

                    if matches!(serialized, FieldStatus::NotUsed) {
                        return Err(mk_typck_err::<Self>(
                            BuiltinTypeCheckErrorKind::NoColumnWithName {
                                name: spec.name().to_owned(),
                            },
                        ));
                    }
                }

                partial.check_missing()
            }
        }
    }
}
