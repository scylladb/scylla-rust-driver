use criterion::{criterion_group, criterion_main, Criterion};

use bytes::BytesMut;
use scylla::internal_testing::calculate_token_for_partition_key;
use scylla::routing::partitioner::PartitionerName;
use scylla_cql::frame::response::result::{ColumnType, NativeType};
use scylla_cql::frame::types;
use scylla_cql::serialize::row::SerializedValues;

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
        .add_value(&"I'm prepared!!!", &ColumnType::Native(NativeType::Text))
        .unwrap();

    let mut serialized_simple_pk_long_column = SerializedValues::new();
    serialized_simple_pk_long_column
        .add_value(&17_i32, &ColumnType::Native(NativeType::Int))
        .unwrap();
    serialized_simple_pk_long_column
        .add_value(&16_i32, &ColumnType::Native(NativeType::Int))
        .unwrap();
    serialized_simple_pk_long_column
        .add_value(
            &String::from_iter(std::iter::repeat('.').take(2000)),
            &ColumnType::Native(NativeType::Text),
        )
        .unwrap();

    let mut serialized_complex_pk = SerializedValues::new();
    serialized_complex_pk
        .add_value(&17_i32, &ColumnType::Native(NativeType::Int))
        .unwrap();
    serialized_complex_pk
        .add_value(&16_i32, &ColumnType::Native(NativeType::Int))
        .unwrap();
    serialized_complex_pk
        .add_value(&"I'm prepared!!!", &ColumnType::Native(NativeType::Text))
        .unwrap();

    let mut serialized_values_long_column = SerializedValues::new();
    serialized_values_long_column
        .add_value(&17_i32, &ColumnType::Native(NativeType::Int))
        .unwrap();
    serialized_values_long_column
        .add_value(&16_i32, &ColumnType::Native(NativeType::Int))
        .unwrap();
    serialized_values_long_column
        .add_value(
            &String::from_iter(std::iter::repeat('.').take(2000)),
            &ColumnType::Native(NativeType::Text),
        )
        .unwrap();

    c.bench_function("calculate_token_from_partition_key simple pk", |b| {
        b.iter(|| {
            calculate_token_for_partition_key(&serialized_simple_pk, &PartitionerName::Murmur3)
        })
    });

    c.bench_function(
        "calculate_token_from_partition_key simple pk long column",
        |b| {
            b.iter(|| {
                calculate_token_for_partition_key(
                    &serialized_simple_pk_long_column,
                    &PartitionerName::Murmur3,
                )
            })
        },
    );

    c.bench_function("calculate_token_from_partition_key complex pk", |b| {
        b.iter(|| {
            calculate_token_for_partition_key(&serialized_complex_pk, &PartitionerName::Murmur3)
        })
    });

    c.bench_function(
        "calculate_token_from_partition_key complex pk long column",
        |b| {
            b.iter(|| {
                calculate_token_for_partition_key(
                    &serialized_values_long_column,
                    &PartitionerName::Murmur3,
                )
            })
        },
    );
}

criterion_group!(benches, types_benchmark, calculate_token_bench);
criterion_main!(benches);
