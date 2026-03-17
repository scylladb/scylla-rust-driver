#![allow(missing_docs)]

use bytes::Bytes;
use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};

use scylla_cql::deserialize::FrameSlice;
use scylla_cql::deserialize::value::DeserializeValue;
use scylla_cql::frame::response::result::{ColumnType, NativeType};
use scylla_cql::serialize::value::SerializeValue;
use scylla_cql::serialize::writers::CellWriter;

/// Creates a `Vec<f32>` of the given length with deterministic values.
fn make_f32_vector(len: usize) -> Vec<f32> {
    (0..len).map(|i| (i as f32) * 0.001).collect()
}

/// Creates the CQL `ColumnType` for `vector<float, N>`.
fn make_vector_type(dimensions: u16) -> ColumnType<'static> {
    ColumnType::Vector {
        typ: Box::new(ColumnType::Native(NativeType::Float)),
        dimensions,
    }
}

/// Serializes a value using the `SerializeValue` trait, returning raw bytes
/// (including the 4-byte CQL cell length prefix).
fn serialize_value(typ: &ColumnType, value: &dyn SerializeValue) -> Vec<u8> {
    let mut buf = Vec::new();
    let writer = CellWriter::new(&mut buf);
    value.serialize(typ, writer).unwrap();
    buf
}

fn vector_serialize_bench(c: &mut Criterion) {
    let mut group = c.benchmark_group("VectorSerialize_f32");

    for &dim in &[3u16, 128, 768, 1536] {
        let vec = make_f32_vector(dim as usize);
        let typ = make_vector_type(dim);

        group.bench_with_input(
            BenchmarkId::new("serialize", dim),
            &(&vec, &typ),
            |b, (vec, typ)| {
                b.iter(|| {
                    let mut buf = Vec::with_capacity(4 + dim as usize * 4);
                    let writer = CellWriter::new(&mut buf);
                    let _ =
                        std::hint::black_box((*vec as &dyn SerializeValue).serialize(typ, writer));
                    std::hint::black_box(buf);
                })
            },
        );
    }

    group.finish();
}

fn vector_deserialize_bench(c: &mut Criterion) {
    let mut group = c.benchmark_group("VectorDeserialize_f32");

    for &dim in &[3u16, 128, 768, 1536] {
        let vec = make_f32_vector(dim as usize);
        let typ = make_vector_type(dim);

        // Serialize to get the wire-format bytes
        let raw = serialize_value(&typ, &vec);
        let bytes: Bytes = raw.into();

        // Pre-validate type check outside the benchmark loop
        <Vec<f32> as DeserializeValue>::type_check(&typ).unwrap();

        group.bench_with_input(
            BenchmarkId::new("deserialize", dim),
            &(&typ, &bytes),
            |b, (typ, bytes)| {
                b.iter(|| {
                    let mut frame_slice = FrameSlice::new(bytes);
                    let value = frame_slice.read_cql_bytes().unwrap();
                    let result: Vec<f32> =
                        <Vec<f32> as DeserializeValue>::deserialize(typ, value).unwrap();
                    std::hint::black_box(result);
                })
            },
        );
    }

    group.finish();
}

fn vector_roundtrip_bench(c: &mut Criterion) {
    let mut group = c.benchmark_group("VectorRoundtrip_f32");

    for &dim in &[768u16, 1536] {
        let vec = make_f32_vector(dim as usize);
        let typ = make_vector_type(dim);

        group.bench_with_input(
            BenchmarkId::new("roundtrip", dim),
            &(&vec, &typ),
            |b, (vec, typ)| {
                b.iter(|| {
                    // Serialize
                    let mut buf = Vec::with_capacity(4 + dim as usize * 4);
                    let writer = CellWriter::new(&mut buf);
                    (*vec as &dyn SerializeValue)
                        .serialize(typ, writer)
                        .unwrap();

                    // Deserialize
                    let bytes: Bytes = buf.into();
                    let mut frame_slice = FrameSlice::new(&bytes);
                    let value = frame_slice.read_cql_bytes().unwrap();
                    let result: Vec<f32> =
                        <Vec<f32> as DeserializeValue>::deserialize(typ, value).unwrap();
                    std::hint::black_box(result);
                })
            },
        );
    }

    group.finish();
}

criterion_group!(
    benches,
    vector_serialize_bench,
    vector_deserialize_bench,
    vector_roundtrip_bench
);
criterion_main!(benches);
