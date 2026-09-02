# Scylla Rust Driver

This book contains documentation for [scylla-rust-driver](https://github.com/scylladb/scylla-rust-driver) - a driver
for the [Scylla](https://scylladb.com) database written in Rust.
Although optimized for Scylla, the driver is also compatible with [Apache Cassandra®](https://cassandra.apache.org/).

## Other documentation

* [Examples](https://github.com/scylladb/scylla-rust-driver/tree/main/examples)
* [Rust and Scylla lesson](https://university.scylladb.com/courses/using-scylla-drivers/lessons/rust-and-scylla-2/) on Scylla University
* [API documentation](https://docs.rs/scylla)
* [Scylla documentation](https://docs.scylladb.com)
* [Cassandra® documentation](https://cassandra.apache.org/doc/latest/)

## Contents

* [Quick start](https://rust-driver.docs.scylladb.com/v1.3.0/quickstart/quickstart.md) - Setting up a Rust project using `scylla-rust-driver` and executing a few CQL statements
* [Migration guides](https://rust-driver.docs.scylladb.com/v1.3.0/migration-guides/migration-guides.md) - How to update the code that used an older version of this driver
* [Connecting to the cluster](https://rust-driver.docs.scylladb.com/v1.3.0/connecting/connecting.md) - Configuring a connection to scylla cluster
* [CQL statement execution](https://rust-driver.docs.scylladb.com/v1.3.0/statements/statements.md) - Executing different types of CQL statement (simple, prepared, batch, paged)
* [Execution profiles](https://rust-driver.docs.scylladb.com/v1.3.0/execution-profiles/execution-profiles.md) - Grouping statement execution configuration options together and switching them all at once
* [Data Types](https://rust-driver.docs.scylladb.com/v1.3.0/data-types/data-types.md) - How to use various column data types
* [Load balancing](https://rust-driver.docs.scylladb.com/v1.3.0/load-balancing/load-balancing.md) - Load balancing configuration
* [Retry policy configuration](https://rust-driver.docs.scylladb.com/v1.3.0/retry-policy/retry-policy.md) - What to do when execution attempt fails, statement idempotence
* [Driver metrics](https://rust-driver.docs.scylladb.com/v1.3.0/metrics/metrics.md) - Statistics about the driver - number of executed statements, latency etc.
* [Logging](https://rust-driver.docs.scylladb.com/v1.3.0/logging/logging.md) - Viewing and integrating logs produced by the driver
* [Request tracing](https://rust-driver.docs.scylladb.com/v1.3.0/tracing/tracing.md) - Tracing request execution
* [Database schema](https://rust-driver.docs.scylladb.com/v1.3.0/schema/schema.md) - Fetching and inspecting database schema
