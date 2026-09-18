//! CQL value deserialization benchmarks measured under Valgrind via gungraun.
//!
//! These scenarios do not need a cluster: the value is serialized into its wire
//! form in `setup` (which the harness excludes from the measurement) and only
//! its deserialization is measured. See `benches/requests.rs` for the
//! end-to-end request scenarios.

#![allow(missing_docs)]

use std::collections::{HashMap, HashSet};
use std::hint::black_box;

use benchmarks::deserialization::{COLLECTION_LEN, SerializedValue, UDT_FIELDS, VECTOR_DIMENSIONS};
use gungraun::Dhat;
use gungraun::prelude::*;
use scylla::value::CqlValue;

// The iteration count `N` each scenario runs is written as an integer literal
// (rather than a named constant) on purpose: gungraun records the *textual*
// argument in each result's `details` field, so a literal keeps the actual
// number visible there (a named constant would be recorded as its name).

/// A serialized value together with the number of times its measured loop
/// should deserialize it. The iteration count is threaded through `setup`
/// (which is where gungraun passes the benchmark arguments).
type State = (SerializedValue, usize);

fn setup_list_int(n: usize) -> State {
    (SerializedValue::list_int(COLLECTION_LEN), n)
}

fn setup_set_int(n: usize) -> State {
    (SerializedValue::set_int(COLLECTION_LEN), n)
}

fn setup_map_int_bigint(n: usize) -> State {
    (SerializedValue::map_int_bigint(COLLECTION_LEN), n)
}

fn setup_vector_float(n: usize) -> State {
    (SerializedValue::vector_float(VECTOR_DIMENSIONS), n)
}

fn setup_udt(n: usize) -> State {
    (SerializedValue::udt_of_ints(UDT_FIELDS), n)
}

// Dropping the state is done in teardown so that it is not attributed to the
// benchmark.
fn teardown<T>(state: T) {
    drop(state);
}

#[library_benchmark]
#[benches::counts(args = [100], setup = setup_list_int, teardown = teardown)]
fn list_to_vec(state: State) -> State {
    state.0.deserialize_n::<Vec<i32>>(black_box(state.1));
    state
}

#[library_benchmark]
#[benches::counts(args = [100], setup = setup_set_int, teardown = teardown)]
fn set_to_hash_set(state: State) -> State {
    state.0.deserialize_n::<HashSet<i32>>(black_box(state.1));
    state
}

#[library_benchmark]
#[benches::counts(args = [100], setup = setup_map_int_bigint, teardown = teardown)]
fn map_to_hash_map(state: State) -> State {
    state
        .0
        .deserialize_n::<HashMap<i32, i64>>(black_box(state.1));
    state
}

#[library_benchmark]
#[benches::counts(args = [100], setup = setup_vector_float, teardown = teardown)]
fn vector_to_vec(state: State) -> State {
    state.0.deserialize_n::<Vec<f32>>(black_box(state.1));
    state
}

#[library_benchmark]
#[benches::counts(args = [100], setup = setup_list_int, teardown = teardown)]
fn list_to_cql_value(state: State) -> State {
    state.0.deserialize_n::<CqlValue>(black_box(state.1));
    state
}

#[library_benchmark]
#[benches::counts(args = [100], setup = setup_udt, teardown = teardown)]
fn udt_to_cql_value(state: State) -> State {
    state.0.deserialize_n::<CqlValue>(black_box(state.1));
    state
}

library_benchmark_group!(
    name = deserialization;
    benchmarks = list_to_vec, set_to_hash_set, map_to_hash_map, vector_to_vec, list_to_cql_value,
        udt_to_cql_value
);

main!(
    config = LibraryBenchmarkConfig::default()
        // Measure allocations and peak memory with DHAT in addition to the
        // default Callgrind instruction counting.
        .tool(Dhat::default())
        // See `benches/requests.rs` for why these two arguments are set.
        .valgrind_args(["--trace-children=no", "--num-callers=500"]);
    library_benchmark_groups = deserialization
);
