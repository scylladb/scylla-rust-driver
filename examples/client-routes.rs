use anyhow::Result;
use scylla::client::{
    client_routes::{ClientRoutesConfig, ClientRoutesProxy},
    session_builder::ClientRoutesSessionBuilder,
};

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();

    let connection_id = "d1d64c5d-cc27-5607-a636-d96975e89340";
    let uri = "127.0.0.1:12137";
    println!("Connecting to {uri} via ClientRoutes ...");

    let proxy = ClientRoutesProxy::new_with_connection_id(connection_id.into());

    let config = ClientRoutesConfig::new(vec![proxy])?;

    let session = ClientRoutesSessionBuilder::new(config)
        .known_node(uri)
        .build()
        .await?;

    session.query_unpaged(
        "CREATE KEYSPACE IF NOT EXISTS examples_ks WITH REPLICATION = {'class' : 'NetworkTopologyStrategy', 'replication_factor' : 1}",
        &[]
    )
    .await?;

    println!("Ok.");

    Ok(())
}
