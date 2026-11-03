//! CQL value serialization benchmarks measured under Valgrind via gungraun.
//!
//! These scenarios do not need a cluster: the value is built in `setup` (which
//! the harness excludes from the measurement) and only its serialization is
//! measured. See `benches/requests.rs` for the end-to-end request scenarios.

#![allow(missing_docs)]

use std::collections::BTreeMap;
use std::hint::black_box;

use benchmarks::value::{COLLECTION_LEN, TypedValue, UDT_FIELDS, VECTOR_DIMENSIONS};
use gungraun::Dhat;
use gungraun::prelude::*;
use scylla::value::CqlValue;

// See `benches/deserialization.rs` for why the iteration counts are literals.

/// A value together with the number of times its measured loop should
/// serialize it. The iteration count is threaded through `setup` (which is
/// where gungraun passes the benchmark arguments).
type State<V> = (TypedValue<V>, usize);

fn setup_list_int(n: usize) -> State<Vec<i32>> {
    (TypedValue::list_int(COLLECTION_LEN), n)
}

fn setup_map_int_bigint(n: usize) -> State<BTreeMap<i32, i64>> {
    (TypedValue::map_int_bigint(COLLECTION_LEN), n)
}

fn setup_vector_float(n: usize) -> State<Vec<f32>> {
    (TypedValue::vector_float(VECTOR_DIMENSIONS), n)
}

fn setup_vector_text(n: usize) -> State<Vec<String>> {
    (TypedValue::vector_text(VECTOR_DIMENSIONS), n)
}

fn setup_udt(n: usize) -> State<CqlValue> {
    (TypedValue::udt_of_ints(UDT_FIELDS), n)
}

// Dropping the state is done in teardown so that it is not attributed to the
// benchmark.
fn teardown<T>(state: T) {
    drop(state);
}

#[library_benchmark]
#[benches::counts(args = [100], setup = setup_list_int, teardown = teardown)]
fn vec_to_list(state: State<Vec<i32>>) -> State<Vec<i32>> {
    state.0.serialize_n(black_box(state.1));
    state
}

#[library_benchmark]
#[benches::counts(args = [100], setup = setup_map_int_bigint, teardown = teardown)]
fn btree_map_to_map(state: State<BTreeMap<i32, i64>>) -> State<BTreeMap<i32, i64>> {
    state.0.serialize_n(black_box(state.1));
    state
}

#[library_benchmark]
#[benches::counts(args = [100], setup = setup_vector_float, teardown = teardown)]
fn vec_to_float_vector(state: State<Vec<f32>>) -> State<Vec<f32>> {
    state.0.serialize_n(black_box(state.1));
    state
}

#[library_benchmark]
#[benches::counts(args = [100], setup = setup_vector_text, teardown = teardown)]
fn vec_to_text_vector(state: State<Vec<String>>) -> State<Vec<String>> {
    state.0.serialize_n(black_box(state.1));
    state
}

#[library_benchmark]
#[benches::counts(args = [100], setup = setup_udt, teardown = teardown)]
fn cql_value_to_udt(state: State<CqlValue>) -> State<CqlValue> {
    state.0.serialize_n(black_box(state.1));
    state
}

library_benchmark_group!(
    name = serialization;
    benchmarks = vec_to_list, btree_map_to_map, vec_to_float_vector, vec_to_text_vector,
        cql_value_to_udt
);

main!(
    config = LibraryBenchmarkConfig::default()
        // Measure allocations and peak memory with DHAT in addition to the
        // default Callgrind instruction counting.
        .tool(Dhat::default())
        // See `benches/requests.rs` for why these two arguments are set.
        .valgrind_args(["--trace-children=no", "--num-callers=500"]);
    library_benchmark_groups = serialization
);
