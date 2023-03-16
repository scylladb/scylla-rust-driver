use anyhow::{anyhow, Result};
use scylla::transport::session::LegacySession;
use scylla::SessionBuilder;
use std::env;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();
    let uri = env::var("SCYLLA_URI").unwrap_or_else(|_| "127.0.0.1".to_string());

    println!("Connecting to {} ...", uri);

    let session: LegacySession = SessionBuilder::new().known_node(uri).build_legacy().await?;

    session.query_unpaged("CREATE KEYSPACE IF NOT EXISTS examples_ks WITH REPLICATION = {'class' : 'NetworkTopologyStrategy', 'replication_factor' : 1}", &[]).await?;

    session
        .query_unpaged(
            "CREATE TABLE IF NOT EXISTS examples_ks.get_by_name (pk int, ck int, value text, primary key (pk, ck))",
            &[],
        )
        .await?;

    session
        .query_unpaged(
            "INSERT INTO examples_ks.get_by_name (pk, ck, value) VALUES (?, ?, ?)",
            (3, 4, "def"),
        )
        .await?;

    session
        .query_unpaged(
            "INSERT INTO examples_ks.get_by_name (pk, ck, value) VALUES (1, 2, 'abc')",
            &[],
        )
        .await?;

    let query_result = session
        .query_unpaged("SELECT pk, ck, value FROM examples_ks.get_by_name", &[])
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
