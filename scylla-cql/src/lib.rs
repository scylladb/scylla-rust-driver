pub mod errors;
pub mod frame;
#[macro_use]
pub mod macros {
    pub use scylla_macros::FromRow;
    pub use scylla_macros::FromUserType;
    pub use scylla_macros::IntoUserType;
    pub use scylla_macros::SerializeRow;
    pub use scylla_macros::SerializeValue;
    pub use scylla_macros::ValueList;

    // Reexports for derive(IntoUserType)
    pub use bytes::{BufMut, Bytes, BytesMut};

    pub use crate::impl_from_cql_value_from_method;
}

pub mod types;

pub use crate::frame::response::cql_to_rust;
pub use crate::frame::response::cql_to_rust::FromRow;

pub use crate::frame::types::Consistency;

#[doc(hidden)]
pub mod _macro_internal {
    pub use crate::frame::response::cql_to_rust::{
        FromCqlVal, FromCqlValError, FromRow, FromRowError,
    };
    pub use crate::frame::response::result::{CqlValue, Row};
    pub use crate::frame::value::{
        LegacySerializedValues, SerializedResult, Value, ValueList, ValueTooBig,
    };
    pub use crate::macros::*;

    pub use crate::types::serialize::row::{
        BuiltinSerializationError as BuiltinRowSerializationError,
        BuiltinSerializationErrorKind as BuiltinRowSerializationErrorKind,
        BuiltinTypeCheckError as BuiltinRowTypeCheckError,
        BuiltinTypeCheckErrorKind as BuiltinRowTypeCheckErrorKind, RowSerializationContext,
        SerializeRow,
    };
    pub use crate::types::serialize::value::{
        BuiltinSerializationError as BuiltinTypeSerializationError,
        BuiltinSerializationErrorKind as BuiltinTypeSerializationErrorKind,
        BuiltinTypeCheckError as BuiltinTypeTypeCheckError,
        BuiltinTypeCheckErrorKind as BuiltinTypeTypeCheckErrorKind, SerializeValue,
        UdtSerializationErrorKind, UdtTypeCheckErrorKind,
    };
    pub use crate::types::serialize::writers::WrittenCellProof;
    pub use crate::types::serialize::{
        CellValueBuilder, CellWriter, RowWriter, SerializationError,
    };

    pub use crate::frame::response::result::ColumnType;
}
