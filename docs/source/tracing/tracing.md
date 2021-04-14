# Query tracing

Each query's execution can be traced to see to which nodes it was sent, what operations were performed etc.

First tracing has to be enabled for a query and then its result will contain a tracing id which can be used to query tracing information.

Queries that support tracing:
* [`Session::query()`](simple-prepared.md)
* [`Session::query_iter()`](paged.md)
* [`Session::execute()`](simple-prepared.md)
* [`Session::execute_iter()`](paged.md)
* [`Session::batch()`](batch.md)
* [`Session::prepare()`](prepare.md)

After obtaining the tracing id you can use `Session::get_tracing_info()` to query tracing information.  
`TracingInfo` contains values that are the same in Scylla and Cassandra®.  
If `TracingInfo` does not contain some needed value it's possible to query it manually from the tables
`system_traces.sessions` and `system_traces.events`

```eval_rst
.. toctree::
   :hidden:
   :glob:

   simple-prepared
   batch
   paged
   prepare

```