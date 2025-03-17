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

pub mod ser {
    pub mod row {
        use crate::{
            frame::response::result::ColumnSpec,
            serialize::{
                row::BuiltinSerializationErrorKind, value::SerializeValue,
                writers::WrittenCellProof, RowWriter, SerializationError,
            },
        };

        use crate::serialize::row::mk_ser_err;

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
    }
}
