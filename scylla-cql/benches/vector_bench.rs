#![allow(missing_docs)]

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use bytes::Bytes;
use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};

use scylla_cql::deserialize::FrameSlice;
use scylla_cql::deserialize::value::DeserializeValue;
use scylla_cql::frame::response::result::{ColumnType, NativeType};
use scylla_cql::serialize::value::SerializeValue;
use scylla_cql::serialize::writers::CellWriter;

// ---------------------------------------------------------------------------
// Counting allocator — tracks allocation count and total bytes while enabled
// ---------------------------------------------------------------------------

static ALLOC_COUNT: AtomicUsize = AtomicUsize::new(0);
static ALLOC_BYTES: AtomicUsize = AtomicUsize::new(0);
static COUNTING_ENABLED: AtomicBool = AtomicBool::new(false);

struct CountingAllocator;

unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        if COUNTING_ENABLED.load(Ordering::Relaxed) {
            ALLOC_COUNT.fetch_add(1, Ordering::Relaxed);
            ALLOC_BYTES.fetch_add(layout.size(), Ordering::Relaxed);
        }
        unsafe { System.alloc(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) }
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        if COUNTING_ENABLED.load(Ordering::Relaxed) {
            ALLOC_COUNT.fetch_add(1, Ordering::Relaxed);
            ALLOC_BYTES.fetch_add(new_size, Ordering::Relaxed);
        }
        unsafe { System.realloc(ptr, layout, new_size) }
    }
}

#[global_allocator]
static GLOBAL: CountingAllocator = CountingAllocator;

struct AllocStats {
    count: usize,
    bytes: usize,
}

fn reset_alloc_counters() {
    ALLOC_COUNT.store(0, Ordering::Relaxed);
    ALLOC_BYTES.store(0, Ordering::Relaxed);
}

fn start_counting() {
    reset_alloc_counters();
    COUNTING_ENABLED.store(true, Ordering::Relaxed);
}

fn stop_counting() -> AllocStats {
    COUNTING_ENABLED.store(false, Ordering::Relaxed);
    AllocStats {
        count: ALLOC_COUNT.load(Ordering::Relaxed),
        bytes: ALLOC_BYTES.load(Ordering::Relaxed),
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

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

// ---------------------------------------------------------------------------
// Allocation profiling — runs once per dimension, prints stats to stdout
// ---------------------------------------------------------------------------

fn vector_alloc_profile(c: &mut Criterion) {
    // Use a tiny benchmark just to get criterion to call us; the real output
    // is the printed allocation table.
    let _ = c;

    eprintln!();
    eprintln!("=== Vector f32 Allocation Profile (single operation) ===");
    eprintln!(
        "{:<14} {:>6} {:>12} {:>12}   {:>6} {:>12} {:>12}",
        "Dimensions", "S-Cnt", "S-Bytes", "S-Ideal", "D-Cnt", "D-Bytes", "D-Ideal"
    );

    for &dim in &[3u16, 128, 768, 1536] {
        let vec = make_f32_vector(dim as usize);
        let typ = make_vector_type(dim);
        let ideal_payload = dim as usize * 4; // wire size without CQL cell header

        // --- Serialization allocation profile ---
        // Pre-allocate the output buffer to isolate serialization-internal allocs
        let mut buf = Vec::with_capacity(4 + ideal_payload);
        start_counting();
        {
            let writer = CellWriter::new(&mut buf);
            let _ = std::hint::black_box(vec.serialize(&typ, writer));
        }
        let ser_stats = stop_counting();

        // --- Deserialization allocation profile ---
        let raw = serialize_value(&typ, &vec);
        let bytes: Bytes = raw.into();
        <Vec<f32> as DeserializeValue>::type_check(&typ).unwrap();

        start_counting();
        {
            let mut frame_slice = FrameSlice::new(&bytes);
            let value = frame_slice.read_cql_bytes().unwrap();
            let result: Vec<f32> =
                <Vec<f32> as DeserializeValue>::deserialize(&typ, value).unwrap();
            std::hint::black_box(result);
        }
        let deser_stats = stop_counting();

        eprintln!(
            "{:<14} {:>6} {:>12} {:>12}   {:>6} {:>12} {:>12}",
            dim,
            ser_stats.count,
            ser_stats.bytes,
            ideal_payload,
            deser_stats.count,
            deser_stats.bytes,
            ideal_payload,
        );
    }
    eprintln!();
}

// ---------------------------------------------------------------------------
// Timing benchmarks
// ---------------------------------------------------------------------------

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
    vector_alloc_profile,
    vector_serialize_bench,
    vector_deserialize_bench,
    vector_roundtrip_bench
);
criterion_main!(benches);
