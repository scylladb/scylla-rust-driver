pub mod frame;
#[macro_use]
pub mod macros {
    pub use scylla_macros::DeserializeRow;
    pub use scylla_macros::DeserializeValue;
    pub use scylla_macros::FromRow;
    pub use scylla_macros::FromUserType;
    pub use scylla_macros::IntoUserType;
    pub use scylla_macros::SerializeRow;
    pub use scylla_macros::SerializeValue;
    pub use scylla_macros::ValueList;

    // Reexports for derive(IntoUserType)
    pub use bytes::{BufMut, Bytes, BytesMut};

    #[allow(deprecated)]
    pub use crate::impl_from_cql_value_from_method;

    #[allow(deprecated)]
    pub use crate::impl_serialize_row_via_value_list;
    #[allow(deprecated)]
    pub use crate::impl_serialize_value_via_value;
}

mod types;

pub use types::deserialize;
pub use types::serialize;

pub use crate::frame::response::cql_to_rust;
#[allow(deprecated)]
pub use crate::frame::response::cql_to_rust::FromRow;

pub use crate::frame::types::Consistency;

#[doc(hidden)]
pub mod _macro_internal {
    #[allow(deprecated)]
    pub use crate::frame::response::cql_to_rust::{
        FromCqlVal, FromCqlValError, FromRow, FromRowError,
    };
    pub use crate::frame::response::result::{ColumnSpec, ColumnType, CqlValue, Row};
    #[allow(deprecated)]
    pub use crate::frame::value::{
        LegacySerializedValues, SerializedResult, Value, ValueList, ValueTooBig,
    };
    pub use crate::macros::*;

    pub use crate::deserialize::row::{
        deser_error_replace_rust_name as row_deser_error_replace_rust_name,
        mk_deser_err as mk_row_deser_err, mk_typck_err as mk_row_typck_err,
        BuiltinDeserializationError as BuiltinRowDeserializationError,
        BuiltinDeserializationErrorKind as BuiltinRowDeserializationErrorKind,
        BuiltinTypeCheckErrorKind as DeserBuiltinRowTypeCheckErrorKind, ColumnIterator,
        DeserializeRow,
    };
    pub use crate::deserialize::value::{
        deser_error_replace_rust_name as value_deser_error_replace_rust_name,
        mk_deser_err as mk_value_deser_err, mk_typck_err as mk_value_typck_err,
        BuiltinDeserializationError as BuiltinTypeDeserializationError,
        BuiltinDeserializationErrorKind as BuiltinTypeDeserializationErrorKind,
        BuiltinTypeCheckErrorKind as DeserBuiltinTypeTypeCheckErrorKind, DeserializeValue,
        UdtDeserializationErrorKind, UdtIterator,
        UdtTypeCheckErrorKind as DeserUdtTypeCheckErrorKind,
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
}

#[cfg(test)]
mod types_tests;
