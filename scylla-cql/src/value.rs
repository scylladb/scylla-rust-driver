//! Defines CQL values of various types and their representations,
//! as well as conversion between them and other types.

use crate::deserialize::value::DeserializeValue;
use crate::deserialize::{DeserializationError, FrameSlice};
use crate::frame::response::result::ColumnType;

// Re-export all public types from scylla-cql-core for backward compatibility.
pub use scylla_cql_core::value::{
    Counter, CqlDate, CqlDecimal, CqlDecimalBorrowed, CqlDuration, CqlTime, CqlTimestamp,
    CqlTimeuuid, CqlValue, CqlVarint, CqlVarintBorrowed, Emptiable, MaybeEmpty, MaybeUnset, Row,
    Unset, ValueOverflow,
};

/// Deserializes any CQL value from a byte slice according to the provided CQL type.
///
/// This delegates to the [`DeserializeValue`] implementation for [`CqlValue`].
///
/// The `new_borrowed` version of `FrameSlice` is deficient in that it does not hold
/// a `Bytes` reference to the frame, only a slice.
/// This is not a problem here, fortunately, because none of `CqlValue` variants contain
/// any `Bytes` - only exclusively owned types - so we never call `FrameSlice::to_bytes()`.
pub fn deser_cql_value(
    typ: &ColumnType,
    buf: &mut &[u8],
) -> std::result::Result<CqlValue, DeserializationError> {
    CqlValue::deserialize(typ, Some(FrameSlice::new_borrowed(buf)))
}
