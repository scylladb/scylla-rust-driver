<img src="https://github.com/scylladb/scylla-rust-driver/raw/main/assets/monster+rust.png" height="150" align="right">

# ScyllaDB Rust Driver

[![Crates.io](https://img.shields.io/crates/v/scylla.svg)](https://crates.io/crates/scylla) [![docs.rs](https://docs.rs/scylla/badge.svg)](https://docs.rs/scylla)
[![minimum rustc version](https://img.shields.io/badge/rustc-1.70-orange.svg)](https://crates.io/crates/scylla)

This is a client-side driver for [ScyllaDB] written in pure Rust with a fully async API using [Tokio].
Although optimized for ScyllaDB, the driver is also compatible with [Apache Cassandra®].

**Note: this driver is officially supported but currently available in beta. Bug reports and pull requests are welcome!**

## Getting Started
The [documentation book](https://rust-driver.docs.scylladb.com/stable/index.html) is a good place to get started. Another useful resource is the Rust and Scylla [lesson](https://university.scylladb.com/courses/using-scylla-drivers/lessons/rust-and-scylla-2/) on Scylla University.

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
You can also run the example as follows if you have a Scylla server running:

```sh
SCYLLA_URI="127.0.0.1:9042" cargo run --example basic
```

All examples are available in the [examples](examples) directory

## Features and Roadmap

The driver supports the following:

* Asynchronous API
* Token-aware routing
* Shard-aware routing (specific to ScyllaDB)
* Prepared statements
* Query paging
* Compression (LZ4 and Snappy algorithms)
* CQL binary protocol version 4
* Batch statements
* Configurable load balancing policies
* Driver-side metrics
* TLS support. Supports either [OpenSSL](https://docs.rs/openssl/0.10.70/openssl/#automatic) or [rustls](https://docs.rs/rustls/latest/rustls/)
* Configurable retry policies
* Authentication support
* CQL tracing

Ongoing efforts:
* CQL Events
* More tests
* More benchmarks

## Getting Help

Please join the `#rust-driver` channel on [ScyllaDB Slack] to discuss any issues or questions you might have.

## Supported Rust Versions
Our driver's minimum supported Rust version (MSRV) is 1.70.0. Any changes:
- Will be announced in release notes.
- Before 1.0 will only happen in major releases.
- After 1.0 will also happen in minor, but not patch releases.

Exact MSRV policy after 1.0 is not yet decided.

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
[Apache Cassandra®]: https://cassandra.apache.org/
[cdrs-tokio]: https://github.com/krojew/cdrs-tokio
[CQL binary protocol]: https://github.com/apache/cassandra/blob/trunk/doc/native_protocol_v4.spec
[DataStax C++ driver]: https://github.com/datastax/cpp-driver/
[ScyllaDB]: https://www.scylladb.com/
[Tokio]: https://crates.io/crates/tokio
[cassandra-rs]: https://github.com/Metaswitch/cassandra-rs
