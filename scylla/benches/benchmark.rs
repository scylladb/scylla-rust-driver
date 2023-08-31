use criterion::{criterion_group, criterion_main, Criterion};

use scylla::{
    frame::value::ValueList,
    routing::partitioner::{calculate_token_for_partition_key, Murmur3Partitioner},
};

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

criterion_group!(benches, calculate_token_bench);
criterion_main!(benches);
