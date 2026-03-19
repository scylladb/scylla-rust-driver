//! Contains the [`SerializeValue`] trait and its implementations.

// Note: When editing above doc-comment edit the corresponding comment on
// re-export module in scylla crate too.

pub use scylla_cql_core::serialize::value::{
    BuiltinSerializationError, BuiltinSerializationErrorKind, BuiltinTypeCheckError,
    BuiltinTypeCheckErrorKind, MapSerializationErrorKind, MapTypeCheckErrorKind, SerializeValue,
    SetOrListSerializationErrorKind, SetOrListTypeCheckErrorKind, TupleSerializationErrorKind,
    TupleTypeCheckErrorKind, UdtSerializationErrorKind, UdtTypeCheckErrorKind,
    VectorSerializationErrorKind,
};

#[cfg(test)]
#[path = "value_tests.rs"]
pub(crate) mod tests;
