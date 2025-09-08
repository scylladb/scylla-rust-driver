<img src="https://github.com/scylladb/scylla-rust-driver/raw/main/assets/monster+rust.png" height="150" align="right">

# ScyllaDB Rust Driver

[![Crates.io](https://img.shields.io/crates/v/scylla.svg)](https://crates.io/crates/scylla) [![docs.rs](https://docs.rs/scylla/badge.svg)](https://docs.rs/scylla)
[![minimum rustc version](https://img.shields.io/badge/rustc-1.70-orange.svg)](https://crates.io/crates/scylla)

This is a client-side driver for [ScyllaDB] written in pure Rust with a fully async API using [Tokio].
Although optimized for ScyllaDB, the driver is also compatible with [Apache Cassandra®].

## Getting Started
The [documentation book](https://rust-driver.docs.scylladb.com/stable/index.html) is a good place to get started. Another useful resource is the Rust and ScyllaDB [lesson](https://university.scylladb.com/courses/using-scylla-drivers/lessons/rust-and-scylla-2/) on Scylla University.

## Examples
```rust
use futures::TryStreamExt;

let uri = "127.0.0.1:9042";

let session: Session = SessionBuilder::new().known_node(uri).build().await?;

let query_pager = session.query_iter("SELECT a, b, c FROM ks.t", &[]).await?;
let mut stream = query_pager.rows_stream::<(i32, i32, String)>()?;
while let Some((a, b, c)) = stream.try_next().await? {
    println!("a, b, c: {}, {}, {}", a, b, c);
}
```

Please see the full [example](examples/basic.rs) program for more information.
You can also run the example as follows if you have a ScyllaDB server running:

```sh
SCYLLA_URI="127.0.0.1:9042" cargo run --example basic
```

All examples are available in the [examples](examples) directory

## Features and Roadmap

The driver supports the following:

* Asynchronous API
* Type-safe serialization and deserialization
* Zero-copy deserialization
* Derive macros for user struct serialization and deserialization
* Token-aware routing
* Shard-aware and Tablet-aware routing (specific to ScyllaDB)
* Prepared, unprepared and batch statements
* Query paging - both transparent and manual
* CachingSession that transparently prepares statements
* CQL binary protocol version 4
* Configurable policies:
    * Load balancing
    * Retry
    * Speculative execution
    * and other (timestamp generation, address translation, host filtering)
* Execution profiles
* Driver-side metrics, and query execution history
* TLS. Both OpenSSL and Rustls are supported
* Compression (LZ4 and Snappy algorithms)
* Authentication
* Cluster metadata access
* CQL tracing

For planned future improvements, see our [Milestones].

## Getting Help

We invite you to discuss any issues and ask questions on the [ScyllaDB Forum] and the `#rust-driver` channel on [ScyllaDB Slack].

## Version support

The driver is considered production ready, hence its version is not 0.x.
We do however acknowledge that the API will surely need some breaking changes in
the future, which means it is not our goal to stay on 1.x forever - we will
release new major version in the future.

The API stability guarantee we provide is that we won't release new major
versions very often.
In case of 2.0, it won't be released earlier than 9 months after release 1.0.
For further major versions this duration may be increased.

After new major version is released, the previous major version will still
receive bugfixes for some time (exact time is yet to be determined), but new
features will only be developed for the latest major version.

## Supported Rust Versions

Our driver's minimum supported Rust version (MSRV) is 1.85.0.
Changes to MSRV can only happen in major and minor relases, but not in patch releases.
We will not bump MSRV to a Rust version that was released less than 6 months ago.

## Reference Documentation

* [CQL binary protocol] specification version 4

## Other Drivers

* [cdrs-tokio]: Apache Cassandra driver written in pure Rust.
* [cassandra-rs]: Rust wrappers for the [DataStax C++ driver] for Apache Cassandra.

## License

This project is licensed under either of

- Apache License, Version 2.0, ([LICENSE-APACHE](LICENSE-APACHE) or [http://www.apache.org/licenses/LICENSE-2.0](http://www.apache.org/licenses/LICENSE-2.0))
- MIT license ([LICENSE-MIT](LICENSE-MIT) or [http://opensource.org/licenses/MIT](http://opensource.org/licenses/MIT))

at your option.

[ScyllaDB Slack]: http://slack.scylladb.com/
[ScyllaDB Forum]: https://forum.scylladb.com/
[Milestones]: https://github.com/scylladb/scylla-rust-driver/milestones
[Apache Cassandra®]: https://cassandra.apache.org/
[cdrs-tokio]: https://github.com/krojew/cdrs-tokio
[CQL binary protocol]: https://github.com/apache/cassandra/blob/trunk/doc/native_protocol_v4.spec
[DataStax C++ driver]: https://github.com/datastax/cpp-driver/
[ScyllaDB]: https://www.scylladb.com/
[Tokio]: https://crates.io/crates/tokio
[cassandra-rs]: https://github.com/Metaswitch/cassandra-rs
