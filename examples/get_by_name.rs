use anyhow::{anyhow, Result};
use scylla::client::session::Session;
use scylla::client::session_builder::SessionBuilder;
use scylla::value::Row;
use std::env;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();
    let uri = env::var("SCYLLA_URI").unwrap_or_else(|_| "127.0.0.1".to_string());

    println!("Connecting to {} ...", uri);

    let session: Session = SessionBuilder::new().known_node(uri).build().await?;

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

    let rows_result = session
        .query_unpaged("SELECT pk, ck, value FROM examples_ks.get_by_name", &[])
        .await?
        .into_rows_result()?;
    let col_specs = rows_result.column_specs();
    let (ck_idx, _) = col_specs
        .get_by_name("ck")
        .ok_or_else(|| anyhow!("No ck column found"))?;
    let (value_idx, _) = col_specs
        .get_by_name("value")
        .ok_or_else(|| anyhow!("No value column found"))?;
    let rows = rows_result
        .rows::<Row>()
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    println!("ck           |  value");
    println!("---------------------");
    for row in rows {
        println!("{:?} | {:?}", row.columns[ck_idx], row.columns[value_idx]);
    }

    Ok(())
}
