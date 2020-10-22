use anyhow::Result;
use scylla::transport::session::Session;
use std::env;

#[tokio::main]
async fn main() -> Result<()> {
    let uri = env::var("SCYLLA_URI").unwrap_or("localhost:9042".to_string());

    println!("Connecting to {} ...", uri);

    let session = Session::connect(uri).await?;

    session.query("CREATE KEYSPACE IF NOT EXISTS ks WITH REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor' : 1}").await?;

    session.query("CREATE TABLE IF NOT EXISTS ks.t (a int, b int, c text, primary key (a, b))").await?;

    session.query("INSERT INTO ks.t (a, b, c) VALUES (1, 2, 'abc')").await?;

    println!("Ok.");

    Ok(())
}
