# Scylla Rust Driver
This book contains documentation for [scylla-rust-driver](https://github.com/scylladb/scylla-rust-driver) - a driver
for the [Scylla](https://scylladb.com) database written in Rust. 
Although optimized for Scylla, the driver is also compatible with [Apache Cassandra®](https://cassandra.apache.org/).

This project is not yet available as GA, but you're welcome to [download and try it](https://github.com/scylladb/scylla-rust-driver)!

```eval_rst
.. image:: monster+rust.png
   :scale: 30%
```


## Contents
* [Quick start](quickstart/quickstart.md) - Setting up a Rust project using `scylla-rust-driver` and running a few queries
* [Connecting to the cluster](connecting/connecting.md) - Configuring a connection to scylla cluster
* [Making queries](queries/queries.md) - Making different types of queries (simple, prepared, batch, paged)
* [Data Types](data-types/data-types.md) - How to use various column data types
* [Load balancing](load-balancing/load-balancing.md) - Load balancing configuration, local datacenters etc.
* [Retry policy configuration](retry-policy/retry-policy.md) - What to do when a query fails, query idempotence
* [Driver metrics](metrics/metrics.md) - Statistics about the driver - number of queries, latency etc.
* [Logging](logging/logging.md) - Viewing and integrating logs produced by the driver
* [Query tracing](let me reverse the or/tracing.md) - Tracing query execution


### Other documentation
* [Examples](https://github.com/scylladb/scylla-rust-driver/tree/main/examples)
* [API documentation](https://docs.rs/scylla)
* [Scylla documentation](https://docs.scylladb.com)
* [Cassandra® documentation](https://cassandra.apache.org/doc/latest/)
