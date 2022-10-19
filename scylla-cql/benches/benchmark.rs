use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};

use scylla_cql::frame::request::Request;
use scylla_cql::frame::value::SerializedValues;
use scylla_cql::frame::value::ValueList;
use scylla_cql::frame::{request::query, Compression, SerializedRequest};

// Macro to avoid lifetime issues
fn make_query<'a>(contents: &'a str, values: &'a SerializedValues) -> query::Query<'a> {
    query::Query {
        contents,
        parameters: query::QueryParameters {
            consistency: scylla_cql::Consistency::LocalQuorum,
            serial_consistency: None,
            values,
            page_size: None,
            paging_state: None,
            timestamp: None,
        },
    }
}

fn serialized_request_make_bench(c: &mut Criterion) {
    let mut group = c.benchmark_group("LZ4Compression.SerializedRequest");
    let query_args = [
        ("INSERT foo INTO ks.table_name (?)", &(1234,).serialized().unwrap()),
        ("INSERT foo, bar, baz INTO ks.table_name (?, ?, ?)", &(1234, "a value", "i am storing a string").serialized().unwrap()),
        (
            "INSERT foo, bar, baz, boop, blah INTO longer_keyspace.a_big_table_name (?, ?, ?, ?, 1000)", 
            &(1234, "a value", "i am storing a string", "dc0c8cd7-d954-47c1-8722-a857941c43fb").serialized().unwrap()
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
                    let _ = criterion::black_box(SerializedRequest::make(
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
