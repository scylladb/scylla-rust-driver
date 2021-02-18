<img src="assets/monster+rust.png" height="150" align="right">

# ScyllaDB Rust Driver

This is a client-side driver for [ScyllaDB] written in pure Rust with a fully async API using [Tokio].
Although optimized for ScyllaDB, the driver is also compatible with [Apache Cassandra®].

**Note: this driver is still in early development and is not for production nor officially supported.**

## Getting Started

```rust
let uri = "127.0.0.1:9042";

let session: Session = SessionBuilder::new().known_node(uri).build().await?;

if let Some(rows) = session.query("SELECT a, b, c FROM ks.t", &[]).await? {
    for row in rows.into_typed::<(i32, i32, String)>() {
        let (a, b, c) = row?;
        println!("a, b, c: {}, {}, {}", a, b, c);
    }
}
```

Please see the full [example](examples/basic.rs) program for more information.
You can also run the example as follows if you have a Scylla server running:

```sh
SCYLLA_URI="127.0.0.1:9042" cargo run --example basic
```

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

We are planning to implement the following:

* Restarting nodes :)
* Configurable retry policies
* Authentication support
* TLS support
* CQL tracing

## Getting Help

Please join the `#rust-driver` channel on [ScyllaDB Slack] to discuss any issues or questions you might have.

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
