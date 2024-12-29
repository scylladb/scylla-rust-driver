use criterion::{criterion_group, criterion_main, Criterion};

use bytes::BytesMut;
use scylla::routing::partitioner::{calculate_token_for_partition_key, Murmur3Partitioner};
use scylla_cql::{
    frame::{response::result::ColumnType, types},
    types::serialize::row::SerializedValues,
};

fn types_benchmark(c: &mut Criterion) {
    let mut buf = BytesMut::with_capacity(64);
    c.bench_function("short", |b| {
        b.iter(|| {
            buf.clear();
            types::write_short(u16::MAX, &mut buf);
            types::read_short(&mut &buf[..]).unwrap();
        })
    });
    c.bench_function("int", |b| {
        b.iter(|| {
            buf.clear();
            types::write_int(-1, &mut buf);
            types::read_int(&mut &buf[..]).unwrap();
        })
    });
    c.bench_function("long", |b| {
        b.iter(|| {
            buf.clear();
            types::write_long(-1, &mut buf);
            types::read_long(&mut &buf[..]).unwrap();
        })
    });
    c.bench_function("string", |b| {
        b.iter(|| {
            buf.clear();
            types::write_string("hello, world", &mut buf).unwrap();
            types::read_string(&mut &buf[..]).unwrap();
        })
    });
}

fn calculate_token_bench(c: &mut Criterion) {
    let mut serialized_simple_pk = SerializedValues::new();
    serialized_simple_pk
        .add_value(&"I'm prepared!!!", &ColumnType::Text)
        .unwrap();

    let mut serialized_simple_pk_long_column = SerializedValues::new();
    serialized_simple_pk_long_column
        .add_value(&17_i32, &ColumnType::Int)
        .unwrap();
    serialized_simple_pk_long_column
        .add_value(&16_i32, &ColumnType::Int)
        .unwrap();
    serialized_simple_pk_long_column
        .add_value(
            &String::from_iter(std::iter::repeat('.').take(2000)),
            &ColumnType::Text,
        )
        .unwrap();

    let mut serialized_complex_pk = SerializedValues::new();
    serialized_complex_pk
        .add_value(&17_i32, &ColumnType::Int)
        .unwrap();
    serialized_complex_pk
        .add_value(&16_i32, &ColumnType::Int)
        .unwrap();
    serialized_complex_pk
        .add_value(&"I'm prepared!!!", &ColumnType::Text)
        .unwrap();

    let mut serialized_values_long_column = SerializedValues::new();
    serialized_values_long_column
        .add_value(&17_i32, &ColumnType::Int)
        .unwrap();
    serialized_values_long_column
        .add_value(&16_i32, &ColumnType::Int)
        .unwrap();
    serialized_values_long_column
        .add_value(
            &String::from_iter(std::iter::repeat('.').take(2000)),
            &ColumnType::Text,
        )
        .unwrap();

    c.bench_function("calculate_token_from_partition_key simple pk", |b| {
        b.iter(|| calculate_token_for_partition_key(&serialized_simple_pk, &Murmur3Partitioner))
    });

    c.bench_function(
        "calculate_token_from_partition_key simple pk long column",
        |b| {
            b.iter(|| {
                calculate_token_for_partition_key(
                    &serialized_simple_pk_long_column,
                    &Murmur3Partitioner,
                )
            })
        },
    );

    c.bench_function("calculate_token_from_partition_key complex pk", |b| {
        b.iter(|| calculate_token_for_partition_key(&serialized_complex_pk, &Murmur3Partitioner))
    });

    c.bench_function(
        "calculate_token_from_partition_key complex pk long column",
        |b| {
            b.iter(|| {
                calculate_token_for_partition_key(
                    &serialized_values_long_column,
                    &Murmur3Partitioner,
                )
            })
        },
    );
}

criterion_group!(benches, types_benchmark, calculate_token_bench);
criterion_main!(benches);
