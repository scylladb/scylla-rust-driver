use std::env;
use std::path::Path;

use anyhow::Result;
use scylla::client::session_builder::CloudSessionBuilder;
use scylla::cloud::CloudTlsProvider;

#[tokio::main]
async fn main() -> Result<()> {
    println!("Connecting to SNI proxy as described in cloud config yaml ...");
    let config_path = env::args()
        .nth(1)
        .unwrap_or("examples/config_data.yaml".to_owned());
    let session = CloudSessionBuilder::new(Path::new(&config_path), CloudTlsProvider::OpenSsl010)
        .unwrap()
        .build()
        .await
        .unwrap();

    session.query_unpaged("CREATE KEYSPACE IF NOT EXISTS examples_ks WITH REPLICATION = {'class' : 'NetworkTopologyStrategy', 'replication_factor' : 1}",
                    &[]).await.unwrap();
    session
        .query_unpaged("DROP TABLE IF EXISTS examples_ks.cloud;", &[])
        .await
        .unwrap();

    println!("Ok.");

    Ok(())
}
