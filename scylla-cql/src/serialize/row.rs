//! Contains the [`SerializeRow`] trait and its implementations.

// Note: When editing above doc-comment edit the corresponding comment on
// re-export module in scylla crate too.

pub use scylla_cql_core::serialize::row::{
    BuiltinSerializationError, BuiltinSerializationErrorKind, BuiltinTypeCheckError,
    BuiltinTypeCheckErrorKind, RowSerializationContext, SerializeRow, SerializedValues,
    SerializedValuesIterator, mk_ser_err, mk_typck_err,
};
