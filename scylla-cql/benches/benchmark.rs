#![allow(missing_docs)]

use std::borrow::Cow;

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};

use scylla_cql::frame::request::query::PagingState;
use scylla_cql::frame::request::SerializableRequest;
use scylla_cql::frame::response::result::{ColumnType, NativeType};
use scylla_cql::frame::{request::query, Compression, SerializedRequest};
use scylla_cql::serialize::row::SerializedValues;

fn make_query(contents: &str, values: SerializedValues) -> query::Query<'_> {
    query::Query {
        contents: Cow::Borrowed(contents),
        parameters: query::QueryParameters {
            consistency: scylla_cql::Consistency::LocalQuorum,
            serial_consistency: None,
            values: Cow::Owned(values),
            skip_metadata: false,
            page_size: None,
            paging_state: PagingState::start(),
            timestamp: None,
        },
    }
}

fn serialized_request_make_bench(c: &mut Criterion) {
    let mut values = SerializedValues::new();
    let mut group = c.benchmark_group("LZ4Compression.SerializedRequest");
    let query_args = [
        ("INSERT foo INTO ks.table_name (?)", {
            values.add_value(&1234, &ColumnType::Native(NativeType::Int)).unwrap();
            values.clone()
        }),
        ("INSERT foo, bar, baz INTO ks.table_name (?, ?, ?)", {
            values.add_value(&"a value", &ColumnType::Native(NativeType::Text)).unwrap();
            values.add_value(&"i am storing a string", &ColumnType::Native(NativeType::Text)).unwrap();
            values.clone()
        }),
        (
            "INSERT foo, bar, baz, boop, blah INTO longer_keyspace.a_big_table_name (?, ?, ?, ?, 1000)",
            {
                values.add_value(&"dc0c8cd7-d954-47c1-8722-a857941c43fb", &ColumnType::Native(NativeType::Text)).unwrap();
                values.clone()
            }
        ),
    ];
    let queries = query_args.map(|(q, v)| make_query(q, v));

    for query in queries {
        let query_size = query.to_bytes().unwrap().len();
        group.bench_with_input(
            BenchmarkId::new("SerializedRequest::make", query_size),
            &query,
            |b, query| {
                b.iter(|| {
                    let _ = std::hint::black_box(SerializedRequest::make(
                        query,
                        Some(Compression::Lz4),
                        false,
                    ));
                })
            },
        );
    }
}

criterion_group!(benches, serialized_request_make_bench);
criterion_main!(benches);
