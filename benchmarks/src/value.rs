//! CQL values used by the serialization and deserialization benchmarks.

use std::borrow::Cow;
use std::collections::BTreeMap;
use std::hint::black_box;
use std::sync::Arc;

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

/// A Rust value together with the CQL type it is (de)serialized as.
pub struct TypedValue<V> {
    pub typ: ColumnType<'static>,
    pub value: V,
}

impl TypedValue<Vec<i32>> {
    fn ints(len: usize) -> Vec<i32> {
        (0..len as i32).collect()
    }

    /// A `list<int>` of `len` elements.
    pub fn list_int(len: usize) -> Self {
        Self {
            typ: ColumnType::Collection {
                frozen: false,
                typ: CollectionType::List(Box::new(ColumnType::Native(NativeType::Int))),
            },
            value: Self::ints(len),
        }
    }

    /// A `set<int>` of `len` elements.
    pub fn set_int(len: usize) -> Self {
        Self {
            typ: ColumnType::Collection {
                frozen: false,
                typ: CollectionType::Set(Box::new(ColumnType::Native(NativeType::Int))),
            },
            value: Self::ints(len),
        }
    }
}

impl TypedValue<BTreeMap<i32, i64>> {
    /// A `map<int, bigint>` of `len` entries.
    ///
    /// The values are `bigint` rather than `text` so that the measurement is not
    /// dominated by one `String` allocation per entry.
    pub fn map_int_bigint(len: usize) -> Self {
        Self {
            typ: ColumnType::Collection {
                frozen: false,
                typ: CollectionType::Map(
                    Box::new(ColumnType::Native(NativeType::Int)),
                    Box::new(ColumnType::Native(NativeType::BigInt)),
                ),
            },
            // A `BTreeMap` rather than a `HashMap` so that the serialized bytes -
            // and hence the measurement - do not depend on hash order.
            value: (0..len as i32).map(|i| (i, i64::from(i))).collect(),
        }
    }
}

impl TypedValue<Vec<f32>> {
    /// A `vector<float, dimensions>`.
    pub fn vector_float(dimensions: u16) -> Self {
        Self {
            typ: ColumnType::Vector {
                typ: Box::new(ColumnType::Native(NativeType::Float)),
                dimensions,
            },
            value: (0..dimensions).map(f32::from).collect(),
        }
    }
}

impl TypedValue<Vec<String>> {
    /// A `vector<text, dimensions>`. Unlike `float`, `text` elements are
    /// variable-length, which takes a different serialization path.
    pub fn vector_text(dimensions: u16) -> Self {
        Self {
            typ: ColumnType::Vector {
                typ: Box::new(ColumnType::Native(NativeType::Text)),
                dimensions,
            },
            value: (0..dimensions).map(|i| i.to_string()).collect(),
        }
    }
}

impl TypedValue<CqlValue> {
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
        Self { typ, value }
    }
}

impl<V: SerializeValue> TypedValue<V> {
    /// Serializes the value `n` times.
    pub fn serialize_n(&self, n: usize) {
        for _ in 0..n {
            // A new buffer each time, as each request serializes its values
            // into a new one.
            let mut buf = Vec::new();
            self.value
                .serialize(&self.typ, CellWriter::new(&mut buf))
                .expect("serialization failed");
            black_box(buf);
        }
    }
}
