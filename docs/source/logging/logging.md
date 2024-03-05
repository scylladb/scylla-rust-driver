# Logging

The driver uses the [tracing](https://github.com/tokio-rs/tracing) crate for all logs.\
To view the logs you have to create a `tracing` subscriber to which all logs will be written.

To just print the logs you can use the default subscriber:
```rust
# extern crate scylla;
# extern crate tokio;
# extern crate tracing;
# extern crate tracing_subscriber;
# use std::error::Error;
# use scylla::{Session, SessionBuilder};
use tracing::info;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    // Install global collector configured based on RUST_LOG env var
    // This collector will receive logs from the driver
    tracing_subscriber::fmt::init();

    let uri = std::env::var("SCYLLA_URI")
        .unwrap_or_else(|_| "127.0.0.1:9042".to_string());

    info!("Connecting to {}", uri);

    let session: Session = SessionBuilder::new().known_node(uri).build().await?;
    session
        .query(
            "CREATE KEYSPACE IF NOT EXISTS ks WITH REPLICATION = \
            {'class' : 'NetworkTopologyStrategy', 'replication_factor' : 1}",
            &[],
        )
        .await?;

    // This query should generate a warning message
    session.query("USE ks", &[]).await?;

    Ok(())
}
```

To start this example execute:
```shell
RUST_LOG=info cargo run
```

The full [example](https://github.com/scylladb/scylla-rust-driver/tree/main/examples/logging.rs) is available in the `examples` folder

## 'log' compatibility

It may be surprising for some that viewing drivers logs using `log` ecosystem doesn't work out of the box despite `tracing` having a compatiblity layer
behind `log` / `log-always` feature flags. 

The problem is that this compatibility using `log` feature (which is recommended for libraries) seems to not work well with `.with_current_subscriber()` / Tokio tasks.
For example, for the following program:
```rust
# extern crate env_logger;
# extern crate log;
# extern crate tokio;
# extern crate tracing;
// main.rs

use tracing::instrument::WithSubscriber;

#[tokio::main]
async fn main() {
    env_logger::init_from_env(env_logger::Env::new().default_filter_or("info"));

    log::info!("info log");
    tracing::info!("info tracing");

    tokio::spawn(
        async move {
            log::info!("spawned info log");
            tracing::info!("spawned info tracing")
        }
        .with_current_subscriber(),
    )
    .await
    .unwrap();

    log::info!("another info log");
    tracing::info!("another info tracing");
}

```

```toml
# Cargo.toml

[package]
name = "reproducer"
version = "1.0.0"
edition = "2021"

[dependencies]
env_logger = "0.10"
log = "0.4"
tracing = { version = "0.1.36", features = [ "log" ] }
tokio = { version = "1.27", features = [ "full" ] }
```

the output is:
```bash
[2024-01-26T00:51:06Z INFO  reproducer] info log
[2024-01-26T00:51:06Z INFO  reproducer] info tracing
[2024-01-26T00:51:06Z INFO  reproducer] spawned info log
[2024-01-26T00:51:06Z INFO  reproducer] another info log
```

The other feature, `log-always`, works with the driver - but is not something we want to enable in this library.
We recommend using tracing ecosystem, but if for some reason you need to stick with `log`, you can try
enabling `log-always` feature in `tracing` by adding it to your direct dependencies (`tracing = { version = "0.1", features = [ "log-always" ] }`).
This should enable driver log collection via `log` loggers.
