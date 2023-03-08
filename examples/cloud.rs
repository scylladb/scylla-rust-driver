use std::path::Path;

use anyhow::Result;
use scylla::CloudSessionBuilder;

#[tokio::main]
async fn main() -> Result<()> {
    println!("Connecting to SNI proxy as described in cloud config yaml ...");

    let session = CloudSessionBuilder::new(Path::new("examples/config_data.yaml"))
        .unwrap()
        .build()
        .await
        .unwrap();

    session.query("CREATE KEYSPACE IF NOT EXISTS ks WITH REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor' : 1}",
                    &[]).await.unwrap();
    session
        .query("DROP TABLE IF EXISTS ks.t;", &[])
        .await
        .unwrap();

    println!("Ok.");

    Ok(())
}
