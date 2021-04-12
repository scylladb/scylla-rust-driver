==================
Scylla Rust Driver
==================
.. toctree::
   :maxdepth: 2
   :hidden:

   Quick start <quickstart/quickstart>
   Connecting to the cluster <connecting/connecting>
   Making queries <queries/queries>
   Data Types <data-types/data-type>
   Load balancing <load-balancing/load-balancing>
   Retry policy configuration <retry-policy/retry-policy>
   Driver metrics <metrics/metrics>
   Logging <logging/logging>
   Query tracing <tracing/tracing>

This book contains documentation for the Scylla Rust Driver, a driver
for the [Scylla](https://scylladb.com) database written in pure Rust with a fully async API using `Tokio <https://crates.io/crates/tokio>`_.
The Scylla Rust Driver is a `GitHub Project <https://github.com/scylladb/scylla-rust-driver>`_.
Although optimized for Scylla, the driver is also compatible with [Apache Cassandra®](https://cassandra.apache.org/).

**Download the driver `here <https://crates.io/crates/scylla>`_**

Or see the documentation, to get started:

* `Quick start <quickstart/quickstart>`_ - Setting up a Rust project using `scylla-rust-driver` and running a few queries
* `Connecting to the cluster <connecting/connecting>`_ - Configuring a connection to scylla cluster
* `Making queries <queries/queries>`_ - Making different types of queries (simple, prepared, batch, paged)
* `Data Types <data-types/data-types>`_ - How to use various column data types
* `Load balancing <load-balancing/load-balancing>`_ - Load balancing configuration, local datacenters etc.
* `Retry policy configuration <retry-policy/retry-policy>`_ - What to do when a query fails, query idempotence
* `Driver metrics <metrics/metrics>`_ - Statistics about the driver - number of queries, latency etc.
* `Logging <logging/logging>`_ - Viewing and integrating logs produced by the driver
* `Query tracing <tracing/tracing>`_ - Tracing query execution


* `Examples <https://github.com/scylladb/scylla-rust-driver/tree/main/examples>`_
* `API documentation <https://docs.rs/scylla>`_
* `Scylla documentation <https://docs.scylladb.com>`_
* `Cassandra® documentation <https://cassandra.apache.org/doc/latest/>`_
