# Query tracing

The driver has utilites for monitoring the execution of queries.
There are two separate ways to get information about what happened with a query: `Tracing` and `Query Execution History`.

### Tracing

Tracing is a feature provided by Scylla. When sending a query we can set a flag that signifies that we would like it to be traced.
After completing the query Scylla provides a `tracing_id` which can be used to fetch information about it - which nodes it was sent to, what operations were performed etc.

Queries that support tracing:
* [`Session::query()`](basic.md)
* [`Session::query_iter()`](paged.md)
* [`Session::execute()`](basic.md)
* [`Session::execute_iter()`](paged.md)
* [`Session::batch()`](basic.md)
* [`Session::prepare()`](prepare.md)

After obtaining the tracing id you can use `Session::get_tracing_info()` to query tracing information.\
`TracingInfo` contains values that are the same in Scylla and Cassandra®, skipping any database-specific ones.\
If `TracingInfo` does not contain some needed value it's possible to query it manually from the tables
`system_traces.sessions` and `system_traces.events`

### Query Execution History

Tracing provides information about how the query execution went on database nodes, but it doesn't say anything about what was going on inside the driver.\
This is what query execution history was made for.

It allows to follow what the driver was thinking - all query attempts, retry decisions, speculative executions.
More information is available in the [Query Execution History](query-history.md) chapter.

```eval_rst
.. toctree::
   :hidden:
   :glob:

   basic
   paged
   prepare
   query-history
```