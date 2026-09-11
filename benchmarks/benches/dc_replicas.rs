//! Micro-benchmark comparing two ways of answering "which replicas of this
//! tablet are in datacenter X?", measured under Valgrind via gungraun.
//!
//! The driver keeps, for every tablet, its replica list as `Vec<(Arc<Node>, Shard)>`
//! and answers per-datacenter queries from the load balancing policy. Two
//! representations are compared:
//!
//! - **`PerDcMap`**: additionally keep a `HashMap<String, Vec<(Arc<Node>, Shard)>>`
//!   grouping the replicas by datacenter, and answer a query with one map lookup.
//!   This is what the driver does today.
//! - **`Filter`**: keep only the full list and answer a query by filtering it on
//!   `node.datacenter == dc`, which for a replica list of a few entries is a few
//!   short string comparisons.
//!
//! Every representation is exercised in three ways, each mirroring a real
//! operation on tablet metadata:
//!
//! - `build`: constructing it from a resolved replica list (learning a tablet),
//! - `clone`: cloning it (what cloning the cluster state does per tablet),
//! - `lookup`: the hot path - for a stream of (tablet, datacenter) queries,
//!   iterate the datacenter's replicas, and separately `choose` one of them by
//!   index the way `ReplicaSet::choose` does (a `len()` followed by an `nth()`).
//!   For `Filter` a single-pass `choose` is measured as well.
//!
//! The data is realistic rather than minimal: datacenter names are of the
//! cloud-region kind (`us-east-1`), the replica list holds `RF` replicas per
//! datacenter in payload order (not grouped by datacenter), and the queried
//! datacenter name is a separate allocation from the one stored in the nodes,
//! as it is in the driver (it comes from the policy's configuration).
//!
//! This benchmark needs no cluster. Run it with
//! `cargo bench -p benchmarks --bench dc_replicas`.

#![allow(missing_docs)]

use std::collections::HashMap;
use std::hint::black_box;
use std::sync::Arc;

use gungraun::Dhat;
use gungraun::prelude::*;

type Shard = u32;

/// Stand-in for the driver's `Node`: only the fields the comparison touches.
/// Like in the driver, the datacenter is an owned `String` inside an `Arc`ed
/// node, so filtering has to dereference the `Arc` and compare strings.
struct Node {
    datacenter: Option<String>,
    #[expect(dead_code)] // Realistic size of the node; never read.
    rack: Option<String>,
}

const DATACENTERS: [&str; 3] = ["us-east-1", "eu-west-1", "ap-southeast-2"];

/// Number of tablets in the fixture: enough that the lookups do not all hit the
/// same tablet.
const TABLETS: usize = 64;

/// Number of lookups per measured `lookup` run.
const LOOKUPS: usize = 4096;

/// Datacenter-grouped replicas, as the driver keeps them today.
#[derive(Clone)]
struct PerDcMap {
    // Only datacenter queries are benchmarked, but the full list is part of the
    // representation and of what `build` and `clone` cost, so it is kept.
    #[expect(dead_code)]
    all: Vec<(Arc<Node>, Shard)>,
    per_dc: HashMap<String, Vec<(Arc<Node>, Shard)>>,
}

impl PerDcMap {
    fn build(all: Vec<(Arc<Node>, Shard)>) -> Self {
        let mut per_dc: HashMap<String, Vec<(Arc<Node>, Shard)>> = HashMap::new();
        for (node, shard) in all.iter() {
            if let Some(dc) = node.datacenter.as_ref() {
                if let Some(replicas) = per_dc.get_mut(dc) {
                    replicas.push((Arc::clone(node), *shard));
                } else {
                    per_dc.insert(dc.to_string(), vec![(Arc::clone(node), *shard)]);
                }
            }
        }
        Self { all, per_dc }
    }

    fn dc_replicas(&self, dc: &str) -> &[(Arc<Node>, Shard)] {
        self.per_dc.get(dc).map(Vec::as_slice).unwrap_or(&[])
    }

    fn iter_dc(&self, dc: &str) -> impl Iterator<Item = (&Arc<Node>, Shard)> {
        self.dc_replicas(dc).iter().map(|(n, s)| (n, *s))
    }

    fn choose_dc(&self, dc: &str, index: usize) -> Option<(&Arc<Node>, Shard)> {
        let replicas = self.dc_replicas(dc);
        let len = replicas.len();
        if len == 0 {
            return None;
        }
        replicas.get(index % len).map(|(n, s)| (n, *s))
    }
}

/// Replica list only; datacenter queries filter it.
#[derive(Clone)]
struct Filter {
    all: Vec<(Arc<Node>, Shard)>,
}

impl Filter {
    fn build(all: Vec<(Arc<Node>, Shard)>) -> Self {
        Self { all }
    }

    fn iter_dc<'a, 'd>(
        &'a self,
        dc: &'d str,
    ) -> impl Iterator<Item = (&'a Arc<Node>, Shard)> + use<'a, 'd> {
        self.all
            .iter()
            .filter(move |(n, _)| n.datacenter.as_deref() == Some(dc))
            .map(|(n, s)| (n, *s))
    }

    /// Two passes, like `ReplicaSet::choose` does for datacenter-filtered
    /// vnode replicas today: count the matches, then take the n-th.
    fn choose_dc(&self, dc: &str, index: usize) -> Option<(&Arc<Node>, Shard)> {
        let len = self.iter_dc(dc).count();
        if len == 0 {
            return None;
        }
        self.iter_dc(dc).nth(index % len)
    }

    /// Single pass: collects the indices of the datacenter's replicas into a
    /// stack buffer, then picks one. Falls back to the two-pass version for
    /// lists too long for the buffer.
    fn choose_dc_single_pass(&self, dc: &str, index: usize) -> Option<(&Arc<Node>, Shard)> {
        let mut matching = [0u8; 16];
        let mut count = 0usize;
        if self.all.len() > usize::from(u8::MAX) {
            return self.choose_dc(dc, index);
        }
        for (i, (n, _)) in self.all.iter().enumerate() {
            if n.datacenter.as_deref() == Some(dc) {
                if count == matching.len() {
                    return self.choose_dc(dc, index);
                }
                matching[count] = i as u8;
                count += 1;
            }
        }
        if count == 0 {
            return None;
        }
        let (n, s) = &self.all[usize::from(matching[index % count])];
        Some((n, *s))
    }
}

/// Shape of the replica lists: `dcs` datacenters with `rf` replicas in each.
#[derive(Clone, Copy)]
struct Shape {
    dcs: usize,
    rf: usize,
}

/// The nodes of a cluster with `shape.dcs` datacenters, `2 * shape.rf` nodes
/// each (so a tablet's replicas are a proper subset of the datacenter).
fn make_nodes(shape: Shape) -> Vec<Arc<Node>> {
    (0..shape.dcs)
        .flat_map(|dc| {
            (0..2 * shape.rf).map(move |i| {
                Arc::new(Node {
                    datacenter: Some(DATACENTERS[dc].to_string()),
                    rack: Some(format!("rack{}", i % 3)),
                })
            })
        })
        .collect()
}

/// Resolved replica lists of `TABLETS` tablets, `shape.rf` replicas in each
/// datacenter, interleaved across datacenters the way a real payload lists them.
/// The node choice is a fixed pseudo-random sequence, so it is the same in every
/// run.
fn make_replica_lists(shape: Shape, nodes: &[Arc<Node>]) -> Vec<Vec<(Arc<Node>, Shard)>> {
    let mut rng = Lcg(0x9E37_79B9_7F4A_7C15);
    let per_dc = 2 * shape.rf;
    (0..TABLETS)
        .map(|_| {
            let mut replicas = Vec::with_capacity(shape.dcs * shape.rf);
            for r in 0..shape.rf {
                for dc in 0..shape.dcs {
                    // Distinct nodes within a datacenter: offset by `r`, stride
                    // through the datacenter's nodes.
                    let idx = dc * per_dc + (rng.next() as usize + r) % per_dc;
                    replicas.push((Arc::clone(&nodes[idx]), rng.next() as Shard % 8));
                }
            }
            replicas
        })
        .collect()
}

/// The (tablet index, datacenter, chosen index) of every lookup. Owned `String`s
/// for the datacenter, distinct from the ones inside the nodes, as in the driver.
fn make_queries(shape: Shape) -> Vec<(usize, String, usize)> {
    let mut rng = Lcg(0x2545_F491_4F6C_DD1D);
    (0..LOOKUPS)
        .map(|_| {
            (
                rng.next() as usize % TABLETS,
                DATACENTERS[rng.next() as usize % shape.dcs].to_string(),
                rng.next() as usize,
            )
        })
        .collect()
}

/// Minimal deterministic generator (no `rand` dependency, no seeding subtleties).
struct Lcg(u64);

impl Lcg {
    fn next(&mut self) -> u64 {
        self.0 = self
            .0
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        self.0 >> 33
    }
}

// ---- build ----------------------------------------------------------------

type BuildState = Vec<Vec<(Arc<Node>, Shard)>>;

fn setup_build(dcs: usize, rf: usize) -> BuildState {
    let shape = Shape { dcs, rf };
    make_replica_lists(shape, &make_nodes(shape))
}

#[library_benchmark]
#[benches::shapes(args = [(1, 3), (2, 3), (3, 3)], setup = setup_build)]
fn build_per_dc_map(lists: BuildState) -> Vec<PerDcMap> {
    lists.into_iter().map(PerDcMap::build).collect()
}

#[library_benchmark]
#[benches::shapes(args = [(1, 3), (2, 3), (3, 3)], setup = setup_build)]
fn build_filter(lists: BuildState) -> Vec<Filter> {
    lists.into_iter().map(Filter::build).collect()
}

// ---- clone ----------------------------------------------------------------

fn setup_clone_per_dc_map(dcs: usize, rf: usize) -> Vec<PerDcMap> {
    setup_build(dcs, rf)
        .into_iter()
        .map(PerDcMap::build)
        .collect()
}

fn setup_clone_filter(dcs: usize, rf: usize) -> Vec<Filter> {
    setup_build(dcs, rf)
        .into_iter()
        .map(Filter::build)
        .collect()
}

#[library_benchmark]
#[benches::shapes(args = [(1, 3), (2, 3), (3, 3)], setup = setup_clone_per_dc_map)]
fn clone_per_dc_map(tablets: Vec<PerDcMap>) -> (Vec<PerDcMap>, Vec<PerDcMap>) {
    let cloned = tablets.clone();
    (tablets, cloned)
}

#[library_benchmark]
#[benches::shapes(args = [(1, 3), (2, 3), (3, 3)], setup = setup_clone_filter)]
fn clone_filter(tablets: Vec<Filter>) -> (Vec<Filter>, Vec<Filter>) {
    let cloned = tablets.clone();
    (tablets, cloned)
}

// ---- lookup ---------------------------------------------------------------

type LookupState<T> = (Vec<T>, Vec<(usize, String, usize)>);

fn setup_lookup_per_dc_map(dcs: usize, rf: usize) -> LookupState<PerDcMap> {
    (
        setup_clone_per_dc_map(dcs, rf),
        make_queries(Shape { dcs, rf }),
    )
}

fn setup_lookup_filter(dcs: usize, rf: usize) -> LookupState<Filter> {
    (setup_clone_filter(dcs, rf), make_queries(Shape { dcs, rf }))
}

// Iterates every replica of the queried datacenter, as the policy does when
// building a query plan.
#[library_benchmark]
#[benches::shapes(args = [(1, 3), (2, 3), (3, 3)], setup = setup_lookup_per_dc_map)]
fn iter_per_dc_map(state: LookupState<PerDcMap>) -> LookupState<PerDcMap> {
    let (tablets, queries) = &state;
    let mut visited = 0usize;
    for (tablet, dc, _) in queries {
        for (node, shard) in tablets[*tablet].iter_dc(dc) {
            black_box((node, shard));
            visited += 1;
        }
    }
    black_box(visited);
    state
}

#[library_benchmark]
#[benches::shapes(args = [(1, 3), (2, 3), (3, 3)], setup = setup_lookup_filter)]
fn iter_filter(state: LookupState<Filter>) -> LookupState<Filter> {
    let (tablets, queries) = &state;
    let mut visited = 0usize;
    for (tablet, dc, _) in queries {
        for (node, shard) in tablets[*tablet].iter_dc(dc) {
            black_box((node, shard));
            visited += 1;
        }
    }
    black_box(visited);
    state
}

// Picks one replica of the queried datacenter by index (`len()` then `nth()`),
// as the policy does when picking the first replica to try.
#[library_benchmark]
#[benches::shapes(args = [(1, 3), (2, 3), (3, 3)], setup = setup_lookup_per_dc_map)]
fn choose_per_dc_map(state: LookupState<PerDcMap>) -> LookupState<PerDcMap> {
    let (tablets, queries) = &state;
    for (tablet, dc, index) in queries {
        black_box(tablets[*tablet].choose_dc(dc, *index));
    }
    state
}

#[library_benchmark]
#[benches::shapes(args = [(1, 3), (2, 3), (3, 3)], setup = setup_lookup_filter)]
fn choose_filter(state: LookupState<Filter>) -> LookupState<Filter> {
    let (tablets, queries) = &state;
    for (tablet, dc, index) in queries {
        black_box(tablets[*tablet].choose_dc(dc, *index));
    }
    state
}

#[library_benchmark]
#[benches::shapes(args = [(1, 3), (2, 3), (3, 3)], setup = setup_lookup_filter)]
fn choose_filter_single_pass(state: LookupState<Filter>) -> LookupState<Filter> {
    let (tablets, queries) = &state;
    for (tablet, dc, index) in queries {
        black_box(tablets[*tablet].choose_dc_single_pass(dc, *index));
    }
    state
}

library_benchmark_group!(
    name = dc_replicas;
    benchmarks =
        build_per_dc_map,
        build_filter,
        clone_per_dc_map,
        clone_filter,
        iter_per_dc_map,
        iter_filter,
        choose_per_dc_map,
        choose_filter,
        choose_filter_single_pass
);

main!(
    config = LibraryBenchmarkConfig::default()
        .tool(Dhat::default())
        .valgrind_args(["--trace-children=no", "--num-callers=500"]);
    library_benchmark_groups = dc_replicas
);
