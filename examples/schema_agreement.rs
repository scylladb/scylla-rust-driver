use anyhow::{bail, Result};
use scylla::session::{IntoTypedRows, Session};
use scylla::transport::errors::QueryError;
use scylla::SessionBuilder;
use std::env;
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<()> {
    // Create connection
    let uri = env::var("SCYLLA_URI").unwrap_or_else(|_| "127.0.0.1:9042".to_string());

    println!("Connecting to {} ...", uri);

    let session: Session = SessionBuilder::new()
        .known_node(uri)
        .schema_agreement_interval(Duration::from_secs(1)) // check every second for schema agreement if not agreed first check
        .build()
        .await?;

    let schema_version = session.await_schema_agreement().await?;

    println!("Schema version: {}", schema_version);

    session.query("CREATE KEYSPACE IF NOT EXISTS ks WITH REPLICATION = {'class' : 'NetworkTopologyStrategy', 'replication_factor' : 1}", &[]).await?;

    match session.await_schema_agreement().await {
        Ok(_schema_version) => println!("Schema is in agreement in time"),
        Err(QueryError::RequestTimeout(_)) => println!("Schema is NOT in agreement in time"),
        Err(err) => bail!(err),
    };
    session
        .query(
            "CREATE TABLE IF NOT EXISTS ks.t (a int, b int, c text, primary key (a, b))",
            &[],
        )
        .await?;

    session.await_schema_agreement().await?;
    session
        .query("INSERT INTO ks.t (a, b, c) VALUES (?, ?, ?)", (3, 4, "def"))
        .await?;

    session.await_schema_agreement().await?;
    session
        .query("INSERT INTO ks.t (a, b, c) VALUES (1, 2, 'abc')", &[])
        .await?;

    let prepared = session
        .prepare("INSERT INTO ks.t (a, b, c) VALUES (?, 7, ?)")
        .await?;
    session
        .execute(&prepared, (42_i32, "I'm prepared!"))
        .await?;
    session
        .execute(&prepared, (43_i32, "I'm prepared 2!"))
        .await?;
    session
        .execute(&prepared, (44_i32, "I'm prepared 3!"))
        .await?;

    // Rows can be parsed as tuples
    if let Some(rows) = session.query("SELECT a, b, c FROM ks.t", &[]).await?.rows {
        for row in rows.into_typed::<(i32, i32, String)>() {
            let (a, b, c) = row?;
            println!("a, b, c: {}, {}, {}", a, b, c);
        }
    }
    println!("Ok.");

    let schema_version = session.await_schema_agreement().await?;
    println!("Schema version: {}", schema_version);

    Ok(())
}
