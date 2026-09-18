# Driver benchmarks

Micro-benchmarks that measure, for each scenario:

- **instructions** executed (via Callgrind),
- **allocations** — the number of heap blocks allocated (via DHAT), and
- **peak memory** — the whole-process maximum live heap, "at t-gmax" (via DHAT).

Because the metrics are collected under Valgrind rather than by wall-clock
timing, they are deterministic enough to compare a pull request against its base
and reliably surface instruction-count and allocation changes.

## Scenarios

The `requests` benchmark (`benches/requests.rs`) covers:

| Scenario         | What it does                                                        |
| ---------------- | ------------------------------------------------------------------- |
| `insert`         | `INSERT`s via `Session::execute_unpaged` on a prepared statement.   |
| `unpaged_select` | Unpaged `SELECT`s via `Session::execute_unpaged`.                   |
| `batch`          | Unlogged `BATCH`es of 64 prepared statements.               |
| `paged_select`   | Auto-paged `SELECT`s via `Session::execute_iter`, draining pages.   |

Connecting, schema creation, statement preparation and data population happen in
each scenario's `setup`, which the harness excludes from the measurements; only
the request loop is measured.

The `deserialization` benchmark (`benches/deserialization.rs`) measures
deserialization of a single CQL value, without a cluster:

| Scenario            | What it deserializes                       |
| ------------------- | ------------------------------------------ |
| `list_to_vec`       | `list<int>` of 1024 elements to `Vec<i32>`. |
| `set_to_hash_set`   | `set<int>` of 1024 elements to `HashSet<i32>`. |
| `map_to_hash_map`   | `map<int, bigint>` of 1024 entries to `HashMap<i32, i64>`. |
| `vector_to_vec`     | `vector<float, 1536>` to `Vec<f32>`.       |
| `list_to_cql_value` | `list<int>` of 1024 elements to `CqlValue`. |
| `udt_to_cql_value`  | A user defined type of 64 `int` fields to `CqlValue`. |

The value is serialized into its wire form in `setup`; only its deserialization
is measured. The request path allocates hundreds of times per request, which
would drown out the deserialization path's own cost in the `requests` scenarios.

## Requirements

- [Valgrind](https://valgrind.org/) (provides Callgrind and DHAT).
- The gungraun runner, matching the `gungraun` dev-dependency:

  ```bash
  cargo install gungraun-runner --version 0.19.4
  ```

- A running ScyllaDB cluster, for the `requests` benchmark. It defaults to the
  repository's three-node docker-compose cluster (`make up`); override the
  contact points with `SCYLLA_URI`, `SCYLLA_URI2` and `SCYLLA_URI3` if needed.
  The `deserialization` benchmark does not need one.

## Running

From the repository root:

```bash
# Save the current results as the "base" baseline (e.g. on the base branch):
make bench-baseline

# ... make your changes, then compare against the saved baseline:
make bench
```

Both targets ensure the cluster is up first. Under the hood they run:

```bash
cargo bench -p benchmarks --benches -- --save-baseline=base
cargo bench -p benchmarks --benches -- --baseline=base
```

## Finding what to optimize

Each run writes a DHAT output file per scenario (e.g.
`target/iai/benchmarks/requests/requests/insert/dhat.insert.out`). Open it in
DHAT's [`dh_view.html`](https://valgrind.org/docs/manual/dh-manual.html) viewer
to browse the allocations by call stack and see exactly where they come from.
Since the totals are whole-process, this lists every allocation site, including
those deep in the driver and in background tasks.
