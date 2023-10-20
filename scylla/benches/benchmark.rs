use criterion::{criterion_group, criterion_main, Criterion};

use bytes::BytesMut;
use scylla::{
    frame::types,
    frame::value::ValueList,
    transport::partitioner::{calculate_token_for_partition_key, Murmur3Partitioner},
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
    let simple_pk = ("I'm prepared!!!",);
    let serialized_simple_pk = simple_pk.serialized().unwrap().into_owned();
    let simple_pk_long_column = (
        17_i32,
        16_i32,
        String::from_iter(std::iter::repeat('.').take(2000)),
    );
    let serialized_simple_pk_long_column = simple_pk_long_column.serialized().unwrap().into_owned();

    let complex_pk = (17_i32, 16_i32, "I'm prepared!!!");
    let serialized_complex_pk = complex_pk.serialized().unwrap().into_owned();
    let complex_pk_long_column = (
        17_i32,
        16_i32,
        String::from_iter(std::iter::repeat('.').take(2000)),
    );
    let serialized_values_long_column = complex_pk_long_column.serialized().unwrap().into_owned();

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
