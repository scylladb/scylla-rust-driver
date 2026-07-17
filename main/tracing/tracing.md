# Query tracing

The driver has utilities for monitoring the execution of queries.
There are two separate ways to get information about what happened with a query: `Tracing` and `Query Execution History`.

## Tracing

Tracing is a feature provided by Scylla. When sending a query we can set a flag that signifies that we would like it to be traced.
After completing the query ScyllaDB provides a `tracing_id` which can be used to fetch information about it - which nodes it was sent to, what operations were performed etc.

Queries that support tracing:

* [`Session::query_unpaged()`](https://rust-driver.docs.scylladb.com/main/tracing/basic.md)
* [`Session::query_iter()`](https://rust-driver.docs.scylladb.com/main/tracing/paged.md)
* [`Session::execute_unpaged()`](https://rust-driver.docs.scylladb.com/main/tracing/basic.md)
* [`Session::execute_iter()`](https://rust-driver.docs.scylladb.com/main/tracing/paged.md)
* [`Session::batch()`](https://rust-driver.docs.scylladb.com/main/tracing/basic.md)
* [`Session::prepare()`](https://rust-driver.docs.scylladb.com/main/tracing/prepare.md)

After obtaining the tracing id you can use `Session::get_tracing_info()` to query tracing information.<br />
\\\\
`TracingInfo` contains values that are the same in ScyllaDB and Cassandra®, skipping any database-specific ones.<br />
\\\\
If `TracingInfo` does not contain some needed value it’s possible to query it manually from the tables
`system_traces.sessions` and `system_traces.events`

## Query Execution History

Tracing provides information about how the query execution went on database nodes, but it doesn’t say anything about what was going on inside the driver.<br />
\\\\
This is what query execution history was made for.

It allows to follow what the driver was thinking - all query attempts, retry decisions, speculative executions.
More information is available in the [Query Execution History](https://rust-driver.docs.scylladb.com/main/tracing/query-history.md) chapter.
