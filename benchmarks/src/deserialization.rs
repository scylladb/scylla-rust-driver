//! Building blocks for the CQL value deserialization benchmarks.
//!
//! Unlike the request scenarios, these do not talk to a cluster: a value is
//! serialized into its wire form once during setup, and the measured loop only
//! deserializes it. The request path allocates hundreds of times per request,
//! which would drown out the deserialization path's own cost.

use std::borrow::Cow;
use std::collections::BTreeMap;
use std::hint::black_box;
use std::sync::Arc;

use scylla::deserialize::FrameSlice;
use scylla::deserialize::value::DeserializeValue;
use scylla::frame::response::result::{CollectionType, ColumnType, NativeType, UserDefinedType};
use scylla::serialize::value::SerializeValue;
use scylla::serialize::writers::CellWriter;
use scylla::value::CqlValue;

/// Number of elements in the benchmarked collections. Large enough that the
/// geometric growth of a collection built without a reserved capacity takes
/// several reallocations.
pub const COLLECTION_LEN: usize = 1024;

/// Number of dimensions of the benchmarked vector, matching a typical text
/// embedding size.
pub const VECTOR_DIMENSIONS: u16 = 1536;

/// Number of fields of the benchmarked user defined type.
pub const UDT_FIELDS: usize = 64;

/// A CQL value in its serialized wire form together with the CQL type it was
/// serialized against - exactly the pair that
/// [`DeserializeValue::deserialize`] consumes.
pub struct SerializedValue {
    typ: ColumnType<'static>,
    bytes: Vec<u8>,
}

impl SerializedValue {
    fn new<V: SerializeValue>(typ: ColumnType<'static>, value: &V) -> Self {
        // `new_without_size` because `deserialize` is given the cell contents,
        // without the length prefix that precedes them in a frame.
        let mut bytes = Vec::new();
        value
            .serialize(&typ, CellWriter::new_without_size(&mut bytes))
            .unwrap();
        Self { typ, bytes }
    }

    fn ints(len: usize) -> Vec<i32> {
        (0..len as i32).collect()
    }

    /// A `list<int>` of `len` elements.
    pub fn list_int(len: usize) -> Self {
        let typ = ColumnType::Collection {
            frozen: false,
            typ: CollectionType::List(Box::new(ColumnType::Native(NativeType::Int))),
        };
        Self::new(typ, &Self::ints(len))
    }

    /// A `set<int>` of `len` elements.
    pub fn set_int(len: usize) -> Self {
        let typ = ColumnType::Collection {
            frozen: false,
            typ: CollectionType::Set(Box::new(ColumnType::Native(NativeType::Int))),
        };
        Self::new(typ, &Self::ints(len))
    }

    /// A `map<int, bigint>` of `len` entries.
    ///
    /// The values are `bigint` rather than `text` so that the measurement is not
    /// dominated by one `String` allocation per entry.
    pub fn map_int_bigint(len: usize) -> Self {
        let typ = ColumnType::Collection {
            frozen: false,
            typ: CollectionType::Map(
                Box::new(ColumnType::Native(NativeType::Int)),
                Box::new(ColumnType::Native(NativeType::BigInt)),
            ),
        };
        // A `BTreeMap` rather than a `HashMap` so that the serialized bytes -
        // and hence the measurement - do not depend on hash order.
        let map: BTreeMap<i32, i64> = (0..len as i32).map(|i| (i, i64::from(i))).collect();
        Self::new(typ, &map)
    }

    /// A `vector<float, dimensions>`.
    pub fn vector_float(dimensions: u16) -> Self {
        let typ = ColumnType::Vector {
            typ: Box::new(ColumnType::Native(NativeType::Float)),
            dimensions,
        };
        let values: Vec<f32> = (0..dimensions).map(f32::from).collect();
        Self::new(typ, &values)
    }

    /// A user defined type of `field_count` `int` fields.
    pub fn udt_of_ints(field_count: usize) -> Self {
        let field_types: Vec<(Cow<'static, str>, ColumnType<'static>)> = (0..field_count)
            .map(|i| {
                (
                    Cow::Owned(format!("f{i}")),
                    ColumnType::Native(NativeType::Int),
                )
            })
            .collect();
        let typ = ColumnType::UserDefinedType {
            frozen: false,
            definition: Arc::new(UserDefinedType {
                name: Cow::Borrowed("udt"),
                keyspace: Cow::Borrowed("ks"),
                field_types: field_types.clone(),
            }),
        };
        let value = CqlValue::UserDefinedType {
            keyspace: "ks".to_string(),
            name: "udt".to_string(),
            fields: field_types
                .into_iter()
                .enumerate()
                .map(|(i, (name, _))| (name.into_owned(), Some(CqlValue::Int(i as i32))))
                .collect(),
        };
        Self::new(typ, &value)
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
