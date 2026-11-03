//! Building blocks for the CQL value deserialization benchmarks.
//!
//! Unlike the request scenarios, these do not talk to a cluster: a value is
//! serialized into its wire form once during setup, and the measured loop only
//! deserializes it. The request path allocates hundreds of times per request,
//! which would drown out the deserialization path's own cost.

use std::hint::black_box;

use scylla::deserialize::FrameSlice;
use scylla::deserialize::value::DeserializeValue;
use scylla::frame::response::result::ColumnType;
use scylla::serialize::value::SerializeValue;
use scylla::serialize::writers::CellWriter;

use crate::value::TypedValue;

/// A CQL value in its serialized wire form together with the CQL type it was
/// serialized against - exactly the pair that
/// [`DeserializeValue::deserialize`] consumes.
pub struct SerializedValue {
    typ: ColumnType<'static>,
    bytes: Vec<u8>,
}

impl SerializedValue {
    /// Serializes `value` into its wire form.
    pub fn new<V: SerializeValue>(value: TypedValue<V>) -> Self {
        // `new_without_size` because `deserialize` is given the cell contents,
        // without the length prefix that precedes them in a frame.
        let mut bytes = Vec::new();
        value
            .value
            .serialize(&value.typ, CellWriter::new_without_size(&mut bytes))
            .unwrap();
        Self {
            typ: value.typ,
            bytes,
        }
    }

    /// Deserializes the value into `T` `n` times.
    pub fn deserialize_n<T>(&self, n: usize)
    where
        T: for<'a> DeserializeValue<'a, 'a>,
    {
        for _ in 0..n {
            let value = T::deserialize(&self.typ, Some(FrameSlice::new_borrowed(&self.bytes)))
                .expect("deserialization failed");
            black_box(value);
        }
    }
}
