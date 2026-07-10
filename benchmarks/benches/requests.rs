//! Driver request benchmarks measured under Valgrind via gungraun.
//!
//! Each scenario is run under:
//! - **Callgrind**, which yields the instruction count, and
//! - **DHAT**, which yields the number of allocations (heap blocks), the bytes
//!   allocated and the peak heap usage (max memory). The DHAT output file
//!   written next to the results in `target/iai/` can be opened in `dh_view.html`
//!   to list the exact allocation sites, which is useful when deciding what to
//!   optimize.
//!
//! The contact point is taken from the `SCYLLA_URI` environment variable (see
//! [`benchmarks::node_address`]); it is passed through to the benchmark process
//! even though gungraun otherwise clears the environment.

#![allow(missing_docs)]

use std::hint::black_box;

use benchmarks::{BenchContext, ScenarioConfig, node_address};
use gungraun::Dhat;
use gungraun::prelude::*;

// The request count `N` each scenario runs is written as an integer literal
// (rather than a named constant) on purpose: gungraun records the *textual*
// argument in each result's `details` field, so a literal keeps the actual
// number visible there (a named constant would be recorded as its name, e.g.
// `setup_default(REQUESTS)`).

/// A connected context together with the number of requests its measured loop
/// should run. The request count is threaded through `setup` (which is where
/// gungraun passes the benchmark arguments) so that each scenario runs its
/// configured number of requests.
type State = (BenchContext, usize);

fn setup_default(n: usize) -> State {
    (
        BenchContext::new(&node_address(), ScenarioConfig::default()).unwrap(),
        n,
    )
}

// Dropping the state (closing the session, shutting down the runtime) is done in
// teardown so that it is not attributed to the benchmark.
fn teardown<T>(state: T) {
    drop(state);
}

#[library_benchmark]
#[benches::counts(args = [100], setup = setup_default, teardown = teardown)]
fn insert(state: State) -> State {
    state.0.run_inserts(black_box(state.1));
    state
}

#[library_benchmark]
#[benches::counts(args = [100], setup = setup_default, teardown = teardown)]
fn unpaged_select(state: State) -> State {
    state.0.run_unpaged_selects(black_box(state.1));
    state
}

library_benchmark_group!(
    name = requests;
    benchmarks = insert, unpaged_select
);

main!(
    config = LibraryBenchmarkConfig::default()
        // Measure allocations and peak memory with DHAT in addition to the
        // default Callgrind instruction counting.
        .tool(Dhat::default())
        // trace-children: Driver doesn't spawn processes, so this won't
        // influence the benchmarks, but the benchmark may want to spawn
        // a process (e.g. a proxy with error injection) which we wouldn't
        // want to trace.
        // num-callers: Allocations are attributed using call stack. If this number
        // is slow, then the benchmarking function won't appear there, and the allocation
        // won't be attributed to the benchmark.
        .valgrind_args(["--trace-children=no", "--num-callers=500"])
        // The environment is cleared for library benchmarks; the setup
        // functions need these to find the cluster.
        .pass_through_envs(["SCYLLA_URI", "SCYLLA_URI2", "SCYLLA_URI3"]);
    library_benchmark_groups = requests
);
