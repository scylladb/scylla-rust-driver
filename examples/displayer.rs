use anyhow::Result;
use futures::StreamExt as _;
use futures::TryStreamExt as _;
use scylla::frame::response::result::Row;
use scylla::transport::session::Session;
use scylla::DeserializeRow;
use scylla::QueryRowsResult;
use scylla::SessionBuilder;
use tracing_subscriber::field::display;
use std::env;

#[tokio::main]
async fn main() -> Result<()> {
    let uri = env::var("SCYLLA_URI").unwrap_or_else(|_| "127.0.0.1:9042".to_string());

    println!("Connecting to {} ...", uri);

    let session: Session = SessionBuilder::new().known_node(uri).build().await?;

    session.query_unpaged("CREATE KEYSPACE IF NOT EXISTS examples_ks WITH REPLICATION = {'class' : 'NetworkTopologyStrategy', 'replication_factor' : 1}", &[]).await?;

    session
        .query_unpaged(
            "CREATE TABLE IF NOT EXISTS examples_ks.basic (a int, b int, c text, primary key (a, b))",
            &[],
        )
        .await?;

    session
        .query_unpaged(
            "INSERT INTO examples_ks.basic (a, b, c) VALUES (?, ?, ?)",
            (3, 4, "def"),
        )
        .await?;

    session
        .query_unpaged(
            "INSERT INTO examples_ks.basic (a, b, c) VALUES (1, 2, 'abc')",
            &[],
        )
        .await?;

    let prepared = session
        .prepare("INSERT INTO examples_ks.basic (a, b, c) VALUES (?, 7, ?)")
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
    let mut iter = session
        .query_iter("SELECT a, b, c FROM examples_ks.basic", &[])
        .await?
        .rows_stream::<(i32, i32, String)>()?;
    while let Some((a, b, c)) = iter.try_next().await? {
        println!("a, b, c: {}, {}, {}", a, b, c);
    }

    // Or as custom structs that derive DeserializeRow
    #[allow(unused)]
    #[derive(Debug, DeserializeRow)]
    struct RowData {
        a: i32,
        b: Option<i32>,
        c: String,
    }

    let result : QueryRowsResult= session
        .query_unpaged("SELECT a, b, c FROM examples_ks.basic", &[])
        .await?
        .into_rows_result()?;
   
    let displayer = result.rows_displayer();
    println!("DISPLAYER:");
    println!("{}", displayer);

    Ok(())
}
