//! Provides types for dealing with CQL value deserialization.
//!
//! This module re-exports all types from `scylla_cql_core::deserialize::value`.

// Re-export everything from scylla-cql-core's value module.
pub use scylla_cql_core::deserialize::value::{
    BuiltinDeserializationError, BuiltinDeserializationErrorKind, BuiltinTypeCheckError,
    BuiltinTypeCheckErrorKind, BytesSequenceIterator, DeserializeValue, Emptiable,
    FixedLengthBytesSequenceIterator, ListlikeIterator, MapDeserializationErrorKind, MapIterator,
    MapTypeCheckErrorKind, MaybeEmpty, SetOrListDeserializationErrorKind,
    SetOrListTypeCheckErrorKind, TupleDeserializationErrorKind, TupleTypeCheckErrorKind,
    UdtDeserializationErrorKind, UdtIterator, UdtTypeCheckErrorKind,
    VectorDeserializationErrorKind, VectorIterator, VectorTypeCheckErrorKind,
    deser_error_replace_rust_name, mk_deser_err, mk_typck_err,
};

#[cfg(test)]
#[path = "value_tests.rs"]
pub(crate) mod tests;
