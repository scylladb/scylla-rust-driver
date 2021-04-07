# Logging

The driver uses the [tracing](https://github.com/tokio-rs/tracing) crate for all logs.  
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
            {'class' : 'SimpleStrategy', 'replication_factor' : 1}",
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