# Executing CQL statements - best practices

Driver supports all kinds of statements supported by ScyllaDB. The following tables aim to bridge between DB concepts and driver's API.
They include recommendations on which API to use in what cases.

## Kinds of CQL statements (from the CQL protocol point of view)

| Kind of CQL statement | Single              | Batch                                    |
|-----------------------|---------------------|------------------------------------------|
| Prepared              | `PreparedStatement` | `Batch` filled with `PreparedStatement`s |
| Unprepared            | `Statement`         | `Batch` filled with `Statement`s        |

This is **NOT** strictly related to content of the CQL statement string.

> ***Interesting note***\
> In fact, any kind of CQL statement could contain any CQL statement string.
> Yet, some of such combinations don't make sense and will be rejected by the DB.
> For example, SELECTs in a Batch are nonsense.

### [Unprepared](unprepared.md) vs [Prepared](prepared.md)

> ***GOOD TO KNOW***\
> Each time a statement is executed by sending a statement string to the DB, it needs to be parsed. Driver does not parse CQL, therefore it sees statement strings as opaque.\
> There is an option to *prepare* a statement, i.e. parse it once by the DB and associate it with an ID. After preparation, it's enough that driver sends the ID
> and the DB already knows what operation to perform - no more expensive parsing necessary! Moreover, upon preparation driver receives valuable data for load balancing,
> enabling advanced load balancing (so better performance!) of all further executions of that prepared statement.\
> ***Key take-over:*** always prepare statements that you are going to execute multiple times.

| Statement comparison | Unprepared                                | Prepared                                                                                                        |
|----------------------|-------------------------------------------|-----------------------------------------------------------------------------------------------------------------|
| Exposed Session API  | `query_*`                                 | `execute_*`                                                                                                     |
| Usability            | execute CQL statement string directly     | need to be separately prepared before use, in-background repreparations if statement falls off the server cache |
| Performance          | poor (statement parsed each time)         | good (statement parsed only upon preparation)                                                                   |
| Load balancing       | primitive (random choice of a node/shard) | advanced (proper node/shard, optimisations for LWT statements)                                                  |
| Suitable operations  | one-shot operations                       | repeated operations                                                                                             |

> ***Warning***\
> If a statement contains bind markers (`?`), then it needs some values to be passed along the statement string.
> If a statement is prepared, the metadata received from the DB can be used to verify validity of passed bind values.
> In case of unprepared statements, this metadata is missing and thus verification is not feasible.
> This used to allow some silent bugs sneaking in in user applications.
>
> To prevent that, the driver will silently prepare every unprepared statement prior to its execution.
> This has an overhead, which further lessens advantages of unprepared statements over prepared statements.
>
> That behaviour is especially important in batches:
> For each simple statement with a non-empty list of values in the batch,
> the driver will send a prepare request, and it will be done **sequentially**!
> Results of preparation are not cached between `Session::batch` calls.
> Therefore, consider preparing the statements before putting them into the batch.

### Single vs [Batch](batch.md)

| Statement comparison | Single                                                | Batch                                                                                                                                                                                |
|----------------------|-------------------------------------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| Exposed Session API  | `query_*`, `execute_*`                                | `batch`                                                                                                                                                                              |
| Usability            | simple setup                                          | need to aggregate statements and binding values to each is more cumbersome                                                                                                           |
| Performance          | good (DB is optimised for handling single statements) | good for small batches, may be worse for larger (also: higher risk of request timeout due to big portion of work)                                                                    |
| Load balancing       | advanced if prepared, else primitive                  | advanced if prepared **and ALL** statements in the batch target the same partition, else primitive                                                                                   |
| Suitable operations  | most of operations                                    | - a list of operations that needs to be executed atomically (batch LightWeight Transaction)</br> - a batch of operations targetting the same partition (as an advanced optimisation) |

## CQL statements - operations (based on what the CQL string contains)

| CQL data manipulation statement                | Recommended statement kind                                                                                                                   | Recommended Session operation                                                                               |
|------------------------------------------------|----------------------------------------------------------------------------------------------------------------------------------------------|-------------------------------------------------------------------------------------------------------------|
| SELECT                                         | `PreparedStatement` if repeated, `Statement` if once                                                                                         | `{query,execute}_iter` (or `{query,execute}_single_page` in a manual loop for performance / more control)   |
| INSERT, UPDATE                                 | `PreparedStatement` if repeated, `Statement` if once, `Batch` if multiple statements are to be executed atomically (LightWeight Transaction) | `{query,execute}_unpaged` (paging is irrelevant, because the result set of such operation is empty)         |
| CREATE/DROP {KEYSPACE, TABLE, TYPE, INDEX,...} | `Statement`, `Batch` if multiple statements are to be executed atomically (LightWeight Transaction)                                          | `query_unpaged` (paging is irrelevant, because the result set of such operation is empty)                   |

### [Paged](paged.md) vs Unpaged query

> ***GOOD TO KNOW***\
> SELECT statements return a [result set](result.md), possibly a large one. Therefore, paging is available to fetch it in chunks, relieving load on cluster and lowering latency.\
> ***Key take-overs:***\
> For SELECTs you had better **avoid unpaged queries**.\
> For non-SELECTs, unpaged API is preferred.

| Query result fetching | Unpaged                                                                                                                 | Paged                                                                                                                                                                |
|-----------------------|-------------------------------------------------------------------------------------------------------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| Exposed Session API   | `{query,execute}_unpaged`                                                                                               | `{query,execute}_single_page`, `{query,execute}_iter`                                                                                                                |
| Usability             | get all results in a single CQL frame, so into a [single Rust struct](result.md)                                        | need to fetch multiple CQL frames and iterate over them - using driver's abstractions (`{query,execute}_iter`) or manually (`{query,execute}_single_page` in a loop) |
| Performance           | - for large results, puts **high load on the cluster**</br> - for small results, the same as paged                      | - for large results, relieves the cluster</br> - for small results, the same as unpaged                                                                              |
| Memory footprint      | potentially big - all results have to be stored at once                                                                 | small - at most constant number of pages are stored by the driver at the same time                                                                                   |
| Latency               | potentially big - all results have to be generated at once                                                              | small - at most one chunk of data must be generated at once, so latency of each chunk is small                                                                       |
| Suitable operations   | - in general: operations with empty result set (non-SELECTs)</br> - as possible optimisation: SELECTs with LIMIT clause | - in general: all SELECTs                                                                                                                                            |

For more detailed comparison and more best practices, see [doc page about paging](paged.md).

### Queries are fully asynchronous - you can run as many of them in parallel as you wish

## `USE KEYSPACE`

There is a special functionality to enable [USE keyspace](usekeyspace.md).

```{eval-rst}
.. toctree::
   :hidden:
   :glob:

   unprepared
   values
   result
   prepared
   batch
   paged
   usekeyspace
   schema-agreement
   lwt
   timeouts
   timestamp-generators
```
