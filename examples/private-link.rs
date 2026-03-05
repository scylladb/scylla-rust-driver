use anyhow::Result;
use scylla::client::{
    private_link::{PrivateLinkConfig, PrivateLinkEndpoint},
    session_builder::PrivateLinkSessionBuilder,
};

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();

    let connection_id = "RUST RULEZ";
    let uri = "127.0.0.1:12137";
    println!("Connecting to {uri} via PrivateLink ...");

    let endpoint = PrivateLinkEndpoint::new_with_connection_id(connection_id.into()); // .with_overriden_hostname("127.0.0.2".into());

    let config = PrivateLinkConfig::new([endpoint], vec![uri.to_owned()])?;

    let session = PrivateLinkSessionBuilder::new(config)
        .build()
        .await
        .unwrap();

    session.query_unpaged("CREATE KEYSPACE IF NOT EXISTS examples_ks WITH REPLICATION = {'class' : 'NetworkTopologyStrategy', 'replication_factor' : 1}", &[]).await.unwrap();

    println!("Ok.");

    std::future::pending::<()>().await;

    Ok(())
}
