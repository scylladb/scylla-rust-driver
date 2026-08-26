# Examples

Each example is a single, self-contained program that teaches one thing. All of
them are meant to be *run*, not only read - CI runs every one of them on every
change.

## Running them

Running the examples needs both a cluster to talk to and
[`ccm`](https://github.com/scylladb/scylla-ccm) installed: most examples use an
ordinary ScyllaDB cluster, which `make up` starts with Docker Compose, while
five bring up a cluster of their own through `ccm`. Then:

```bash
make run-examples
```

brings the Docker Compose cluster up (if it is not up already) and runs every
example in turn. It is a thin wrapper around `./scripts/run-examples.sh`, which
you can also call directly; the script takes no arguments.

### Running a single example

```bash
cargo run -p examples --example basic
```

- `SCYLLA_URI` selects the cluster to connect to; it defaults to
  `172.42.0.2:9042`, which is what `make up` provides. The five `ccm`-based
  examples ignore it, since they create their own cluster.

## What each example teaches

| Example | Teaches |
| --- | --- |
| `auth` | Connecting to a cluster that requires password authentication - and what a wrong password looks like. |
| `basic` | The end-to-end basics: connect, create a keyspace and a table, insert, and read rows as tuples, as derived structs, or untyped. |
| `batch` | Grouping `INSERT`/`UPDATE`/`DELETE` statements into one batch, what atomicity it does and does not buy, and why a batch is not a throughput tool. |
| `client_routes` | Reaching nodes through the private endpoints they publish in `system.client_routes` instead of the addresses from `system.local` / `system.peers`. |
| `compare_tokens` | Computing a partition key's token in the driver, checking it against the token the cluster reports, and listing the replicas that own it. |
| `cql_time_types` | Reading and writing `date`, `time` and `timestamp` using `chrono`, `time`, and the raw `CqlDate`/`CqlTime`/`CqlTimestamp` types. |
| `cqlsh_rs` | A miniature `cqlsh`: a REPL with CQL keyword completion that executes whatever you type. |
| `custom_deserialization` | Implementing `DeserializeValue` by hand for a column value and deriving `DeserializeRow` for a whole row. |
| `custom_load_balancing_policy` | Writing a load balancing policy of your own - here one restricted to a single datacenter - and checking which node really coordinated each request. |
| `custom_serialization` | Implementing `SerializeValue` by hand for a column value and deriving `SerializeRow` for structs of statement values. |
| `enforce_coordinator` | Forcing a request onto a chosen node, and inspecting the coordinator that executed it. |
| `execution_profile` | Bundling consistency, timeout, retry, load balancing and speculative execution settings into profiles, and attaching them to sessions and statements. |
| `get_by_name` | Locating columns in a result by name rather than by position. |
| `logging_log` | Routing the driver's `tracing` messages into the `log` ecosystem (`env_logger`), controlled by `RUST_LOG`. |
| `logging_tracing` | Viewing the driver's log messages with a `tracing_subscriber`, controlled by `RUST_LOG`. |
| `metrics` | Reading the metrics the driver keeps about its own work: request and error counters, latency percentiles, throughput rates and connection statistics. |
| `parallel` | Saturating the driver with bounded concurrent requests, using a prepared statement so every request is routed straight to a replica. |
| `query_history` | Collecting the history of a request's execution: its attempts, retries and speculative executions. |
| `schema_agreement` | Taking control of waiting for schema agreement instead of paying for the driver's automatic wait after every schema change. |
| `select_paging` | Iterating over a large result with the automatic row stream, and driving paging by hand with `PagingState`. |
| `speculative_execution` | Configuring a speculative execution policy so that one slow replica does not hold up an idempotent request. |
| `tls_openssl` | Connecting to a cluster over TLS with the `openssl` backend. |
| `tls_rustls` | Connecting to a cluster over TLS with the `rustls` backend. |
| `token_ring` | Walking the cluster's token ring and listing the replicas that own each token range. |
| `tower` | Wrapping a `Session` in a `tower::Service`. |
| `tracing` | Enabling CQL tracing for queries, prepares, executions, paged reads and batches, and fetching the resulting tracing information. |
| `user_defined_type` | Mapping a CQL user-defined type onto a Rust struct with `SerializeValue`/`DeserializeValue`. |

## The examples that start their own cluster

`auth`, `client_routes`, `custom_load_balancing_policy`, `tls_openssl` and
`tls_rustls` bring up a throwaway cluster with `scylla-ccm-bridge` and tear it
down again when they finish. They do this because each of them needs a cluster
configured in a way a plain one cannot offer:

| Example | Needs a cluster with |
| --- | --- |
| `auth` | `PasswordAuthenticator` enabled - a default cluster accepts any credentials and silently ignores them, so nothing would be demonstrated. |
| `client_routes` | Private endpoints published in `system.client_routes`, differing from the addresses the nodes advertise. |
| `custom_load_balancing_policy` | Two datacenters, so that a datacenter-aware policy has something to choose between. |
| `tls_openssl`, `tls_rustls` | TLS enabled, with a certificate authority and node certificates generated up front. |

In those files the cluster setup is fenced between `--- CI setup ---` and
`--- end of CI setup ---` banner comments. It is scaffolding, not the lesson:
skip it when reading, and read what follows the closing banner.

Where that scaffolding is too big to sit inside the example - the TLS pair share
it, and the `client_routes` one is larger than the example itself - it lives in
`examples/ci/` instead. Nothing in that directory is an example; every `.rs`
file in `examples/` itself is one.

## Adding an example

CI runs every example automatically - `scripts/run-examples.sh` asks `cargo` for
the list of example targets rather than keeping one of its own, so a new
`[[example]]` entry in `Cargo.toml` is picked up without any further wiring.
That puts three obligations on a new example:

- **It must be non-interactive.** It gets `/dev/null` on stdin and must run to
  completion and exit with a zero status on its own.
- **It must clean up after itself.** If it starts anything (a cluster, a
  process, a temporary directory), it has to tear it down before returning.
- **It must get a row in the table above.** That table is the one thing here
  still maintained by hand, so it is the one place a new example can silently
  go missing.
