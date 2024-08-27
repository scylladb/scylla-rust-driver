use anyhow::Result;
use scylla::transport::session::Session;
use scylla::SessionBuilder;
use std::env;
use tracing::info;

// To run this example, and view logged messages, RUST_LOG env var needs to be set
// This can be done using shell command presented below
// RUST_LOG=info cargo run --example logging_log
#[tokio::main]
async fn main() -> Result<()> {
    // Driver uses `tracing` for logging purposes, but it's possible to use `log`
    // ecosystem to view the messages. This requires adding `tracing` crate to
    // dependencies and enabling its "log" feature. Then you will be able to use
    // loggers like `env_logger` to see driver's messages.
    env_logger::init();

    let uri = env::var("SCYLLA_URI").unwrap_or_else(|_| "127.0.0.1:9042".to_string());
    info!("Connecting to {}", uri);

    let session: Session = SessionBuilder::new().known_node(uri).build().await?;
    session.query_unpaged("CREATE KEYSPACE IF NOT EXISTS examples_ks WITH REPLICATION = {'class' : 'NetworkTopologyStrategy', 'replication_factor' : 1}", &[]).await?;

    session.query_unpaged("USE examples_ks", &[]).await?;

    Ok(())
}
