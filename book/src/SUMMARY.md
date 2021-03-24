# Summary

[Scylla Rust Driver](title-page.md)

- [Quick start](quickstart/quickstart.md)
    - [Creating a project](quickstart/create-project.md)
    - [Running Scylla using Docker](quickstart/scylla-docker.md)
    - [Connecting and running a simple query](quickstart/example.md)

- [Connecting to the cluster](connecting/connecting.md)
    - [Compression](connecting/compression.md)
    - [Authentication](connecting/authentication.md)
    - [TLS](connecting/tls.md)

- [Making queries](queries/queries.md)
    - [Simple query](queries/simple.md)
    - [Query values](queries/values.md)
    - [Query result](queries/result.md)
    - [Prepared query](queries/prepared.md)
    - [Batch statement](queries/batch.md)
    - [Paged query](queries/paged.md)
    - [USE keyspace](queries/usekeyspace.md)

- [Data Types](data-types/data-types.md)
    - [Bool, Tinyint, Smallint, Int, Bigint, Float, Double](data-types/primitive.md)
    - [Ascii, Text, Varchar](data-types/text.md)
    - [Counter](data-types/counter.md)
    - [Blob](data-types/blob.md)
    - [Inet](data-types/inet.md)
    - [Uuid, Timeuuid](data-types/uuid.md)
    - [Date](data-types/date.md)
    - [Time](data-types/time.md)
    - [Timestamp](data-types/timestamp.md)
    - [Decimal](data-types/decimal.md)
    - [Varint](data-types/varint.md)
    - [List, Set, Map](data-types/collections.md)
    - [Tuple](data-types/tuple.md)
    - [UDT (User defined type)](data-types/udt.md)

- [Load balancing](load-balancing/load-balancing.md)

- [Retry policy configuration](retry-policy/retry-policy.md)

- [Driver metrics](metrics/metrics.md)

- [Query tracing](tracing/tracing.md)