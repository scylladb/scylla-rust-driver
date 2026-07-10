# Driver benchmarks

Micro-benchmarks that measure, for each request scenario:

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

## Requirements

- [Valgrind](https://valgrind.org/) (provides Callgrind and DHAT).
- The gungraun runner, matching the `gungraun` dev-dependency:

  ```bash
  cargo install gungraun-runner --version 0.19.4
  ```

- A running ScyllaDB cluster. The benchmarks default to the repository's
  three-node docker-compose cluster (`make up`); override the contact points
  with `SCYLLA_URI`, `SCYLLA_URI2` and `SCYLLA_URI3` if needed.

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
cargo bench -p benchmarks --bench requests -- --save-baseline=base
cargo bench -p benchmarks --bench requests -- --baseline=base
```

## Finding what to optimize

Each run writes a DHAT output file per scenario (e.g.
`target/iai/benchmarks/requests/requests/insert/dhat.insert.out`). Open it in
DHAT's [`dh_view.html`](https://valgrind.org/docs/manual/dh-manual.html) viewer
to browse the allocations by call stack and see exactly where they come from.
Since the totals are whole-process, this lists every allocation site, including
those deep in the driver and in background tasks.
