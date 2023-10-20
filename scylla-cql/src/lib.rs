pub mod errors;
pub mod frame;
#[macro_use]
pub mod macros;

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
        SerializedResult, SerializedValues, Value, ValueList, ValueTooBig,
    };
    pub use crate::macros::*;

    pub use crate::types::serialize::value::{
        BuiltinSerializationError as BuiltinTypeSerializationError,
        BuiltinSerializationErrorKind as BuiltinTypeSerializationErrorKind,
        BuiltinTypeCheckError as BuiltinTypeTypeCheckError,
        BuiltinTypeCheckErrorKind as BuiltinTypeTypeCheckErrorKind, SerializeCql,
        UdtSerializationErrorKind, UdtTypeCheckErrorKind,
    };
    pub use crate::types::serialize::writers::WrittenCellProof;
    pub use crate::types::serialize::{CellValueBuilder, CellWriter, SerializationError};

    pub use crate::frame::response::result::ColumnType;
}
