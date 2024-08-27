use anyhow::{bail, Result};
use scylla::transport::errors::QueryError;
use scylla::transport::session::Session;
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

    session.query_unpaged("CREATE KEYSPACE IF NOT EXISTS examples_ks WITH REPLICATION = {'class' : 'NetworkTopologyStrategy', 'replication_factor' : 1}", &[]).await?;

    match session.await_schema_agreement().await {
        Ok(_schema_version) => println!("Schema is in agreement in time"),
        Err(QueryError::RequestTimeout(_)) => println!("Schema is NOT in agreement in time"),
        Err(err) => bail!(err),
    };
    session
        .query_unpaged(
            "CREATE TABLE IF NOT EXISTS examples_ks.schema_agreement (a int, b int, c text, primary key (a, b))",
            &[],
        )
        .await?;

    session.await_schema_agreement().await?;
    session
        .query_unpaged(
            "INSERT INTO examples_ks.schema_agreement (a, b, c) VALUES (?, ?, ?)",
            (3, 4, "def"),
        )
        .await?;

    session.await_schema_agreement().await?;
    session
        .query_unpaged(
            "INSERT INTO examples_ks.schema_agreement (a, b, c) VALUES (1, 2, 'abc')",
            &[],
        )
        .await?;

    let prepared = session
        .prepare("INSERT INTO examples_ks.schema_agreement (a, b, c) VALUES (?, 7, ?)")
        .await?;
    session
        .execute_unpaged(&prepared, (42_i32, "I'm prepared!"))
        .await?;
    session
        .execute_unpaged(&prepared, (43_i32, "I'm prepared 2!"))
        .await?;
    session
        .execute_unpaged(&prepared, (44_i32, "I'm prepared 3!"))
        .await?;

    // Rows can be parsed as tuples
    let result = session
        .query_unpaged("SELECT a, b, c FROM examples_ks.schema_agreement", &[])
        .await?;
    let mut iter = result.rows_typed::<(i32, i32, String)>()?;
    while let Some((a, b, c)) = iter.next().transpose()? {
        println!("a, b, c: {}, {}, {}", a, b, c);
    }

    println!("Ok.");

    let schema_version = session.await_schema_agreement().await?;
    println!("Schema version: {}", schema_version);

    Ok(())
}
