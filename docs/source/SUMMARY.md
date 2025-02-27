# Summary

[Scylla Rust Driver](index.md)

- [Quick start](quickstart/quickstart.md)
    - [Creating a project](quickstart/create-project.md)
    - [Running Scylla using Docker](quickstart/scylla-docker.md)
    - [Connecting and executing a simple CQL statement](quickstart/example.md)

- [Migration guides](migration-guides/migration-guides.md)
    - [Adjusting code to changes in serialization API introduced in 0.11](migration-guides/0.11-serialization.md)
    - [Adjusting code to changes in deserialization API introduced in 0.15](migration-guides/0.15-deserialization.md)

- [Connecting to the cluster](connecting/connecting.md)
    - [Compression](connecting/compression.md)
    - [Authentication](connecting/authentication.md)
    - [TLS](connecting/tls.md)

- [Executing CQL statements](statements/statements.md)
    - [Unprepared statement](statements/unprepared.md)
    - [Statement values](statements/values.md)
    - [Query result](statements/result.md)
    - [Prepared statement](statements/prepared.md)
    - [Batch statement](statements/batch.md)
    - [Paged query](statements/paged.md)
    - [Lightweight transaction statement (LWT)](statements/lwt.md)
    - [USE keyspace](statements/usekeyspace.md)
    - [Schema agreement](statements/schema-agreement.md)
    - [Request timeouts](statements/timeouts.md)
    - [Timestamp generators](statements/timestamp-generators.md)

- [Execution profiles](execution-profiles/execution-profiles.md)
    - [Creating a profile and setting it](execution-profiles/create-and-use.md)
    - [All options supported by a profile](execution-profiles/maximal-example.md)
    - [Options priority](execution-profiles/priority.md)
    - [Remapping a profile handle](execution-profiles/remap.md)

- [Data Types](data-types/data-types.md)
    - [Bool, Tinyint, Smallint, Int, Bigint, Float, Double](data-types/primitive.md)
    - [Ascii, Text, Varchar](data-types/text.md)
    - [Counter](data-types/counter.md)
    - [Blob](data-types/blob.md)
    - [Inet](data-types/inet.md)
    - [Uuid](data-types/uuid.md)
    - [Timeuuid](data-types/timeuuid.md)
    - [Date](data-types/date.md)
    - [Time](data-types/time.md)
    - [Timestamp](data-types/timestamp.md)
    - [Duration](data-types/duration.md)
    - [Decimal](data-types/decimal.md)
    - [Varint](data-types/varint.md)
    - [List, Set, Map](data-types/collections.md)
    - [Tuple](data-types/tuple.md)
    - [UDT (User defined type)](data-types/udt.md)

- [Load balancing](load-balancing/load-balancing.md)
    - [Default policy](load-balancing/default-policy.md)

- [Retry policy configuration](retry-policy/retry-policy.md)
    - [Fallthrough retry policy](retry-policy/fallthrough.md)
    - [Default retry policy](retry-policy/default.md)
    - [Downgrading consistency policy](retry-policy/downgrading-consistency.md)

- [Speculative execution](speculative-execution/speculative.md)
    - [Simple](speculative-execution/simple.md)
    - [Latency Percentile](speculative-execution/percentile.md)

- [Driver metrics](metrics/metrics.md)

- [Logging](logging/logging.md)

- [Query tracing](tracing/tracing.md)
    - [Tracing a simple/prepared query](tracing/basic.md)
    - [Tracing a paged query](tracing/paged.md)
    - [Tracing `Session::prepare`](tracing/prepare.md)
    - [Query Execution History](tracing/query-history.md)

- [Database schema](schema/schema.md)
