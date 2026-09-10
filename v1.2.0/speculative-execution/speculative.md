# Speculative execution

Speculative query execution is an optimization technique where a driver
pre-emptively starts a second execution of a query against another node,
before the first node has replied.

There are multiple speculative execution strategies that the driver can use.
Speculative execution can be configured for the whole whole `Session` during
its creation.

Available speculative execution strategies:

* [Simple](https://rust-driver.docs.scylladb.com/v1.2.0/speculative-execution/simple.md)
* [Latency Percentile](https://rust-driver.docs.scylladb.com/v1.2.0/speculative-execution/percentile.md)

Speculative execution is not enabled by default, and currently only
non-iter session methods use it.
