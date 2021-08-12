use anyhow::{anyhow, Result};
use scylla::transport::session::Session;
use scylla::SessionBuilder;
use std::env;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();
    let uri = env::var("SCYLLA_URI").unwrap_or_else(|_| "127.0.0.1".to_string());

    println!("Connecting to {} ...", uri);

    let session: Session = SessionBuilder::new().known_node(uri).build().await?;

    session.query("CREATE KEYSPACE IF NOT EXISTS ks WITH REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor' : 1}", &[]).await?;

    session
        .query(
            "CREATE TABLE IF NOT EXISTS ks.hello (pk int, ck int, value text, primary key (pk, ck))",
            &[],
        )
        .await?;

    session
        .query(
            "INSERT INTO ks.hello (pk, ck, value) VALUES (?, ?, ?)",
            (3, 4, "def"),
        )
        .await?;

    session
        .query(
            "INSERT INTO ks.hello (pk, ck, value) VALUES (1, 2, 'abc')",
            &[],
        )
        .await?;

    let query_result = session
        .query("SELECT pk, ck, value FROM ks.hello", &[])
        .await?;
    let (ck_idx, _) = query_result
        .get_column_spec("ck")
        .ok_or_else(|| anyhow!("No ck column found"))?;
    let (value_idx, _) = query_result
        .get_column_spec("value")
        .ok_or_else(|| anyhow!("No value column found"))?;
    println!("ck           |  value");
    println!("---------------------");
    for row in query_result.rows.ok_or_else(|| anyhow!("no rows found"))? {
        println!("{:?} | {:?}", row.columns[ck_idx], row.columns[value_idx]);
    }

    Ok(())
}
