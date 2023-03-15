use anyhow::Result;
use scylla::transport::session::Legacy08Session;
use scylla::SessionBuilder;
use std::env;
use tracing::info;

// To run this example, and view logged messages, RUST_LOG env var needs to be set
// This can be done using shell command presented below
// RUST_LOG=info cargo run --example logging
#[tokio::main]
async fn main() -> Result<()> {
    // Install global collector configured based on RUST_LOG env var
    // This collector will receive logs from the driver
    tracing_subscriber::fmt::init();

    let uri = env::var("SCYLLA_URI").unwrap_or_else(|_| "127.0.0.1:9042".to_string());
    info!("Connecting to {}", uri);

    let session: Legacy08Session = SessionBuilder::new().known_node(uri).build().await?;
    session.query("CREATE KEYSPACE IF NOT EXISTS ks WITH REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor' : 1}", &[]).await?;

    session.query("USE ks", &[]).await?;

    Ok(())
}
