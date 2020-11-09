use anyhow::Result;
use scylla::cql_to_rust::FromRow;
use scylla::macros::FromRow;
use scylla::transport::session::{IntoTypedRows, Session};
use std::env;

#[tokio::main]
async fn main() -> Result<()> {
    let uri = env::var("SCYLLA_URI").unwrap_or("127.0.0.1:9042".to_string());

    println!("Connecting to {} ...", uri);

    let session = Session::connect(uri, None).await?;
    session.refresh_topology().await?;

    session.query("CREATE KEYSPACE IF NOT EXISTS ks WITH REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor' : 1}", &[]).await?;

    session
        .query(
            "CREATE TABLE IF NOT EXISTS ks.t (a int, b int, c text, primary key (a, b))",
            &[],
        )
        .await?;

    session
        .query(
            "INSERT INTO ks.t (a, b, c) VALUES (?, ?, ?)",
            &scylla::values!(3, 4, "def"),
        )
        .await?;

    session
        .query("INSERT INTO ks.t (a, b, c) VALUES (1, 2, 'abc')", &[])
        .await?;

    let prepared = session
        .prepare("INSERT INTO ks.t (a, b, c) VALUES (?, 7, ?)")
        .await?;
    session
        .execute(&prepared, &scylla::values!(42_i32, "I'm prepared!"))
        .await?;
    session
        .execute(&prepared, &scylla::values!(43_i32, "I'm prepared 2!"))
        .await?;
    session
        .execute(&prepared, &scylla::values!(44_i32, "I'm prepared 3!"))
        .await?;

    // Rows can be parsed as tuples
    if let Some(rows) = session.query("SELECT a, b, c FROM ks.t", &[]).await? {
        for row in rows.into_typed::<(i32, i32, String)>() {
            let (a, b, c) = row?;
            println!("a, b, c: {}, {}, {}", a, b, c);
        }
    }

    // Or as custom structs that derive FromRow
    #[derive(Debug, FromRow)]
    struct RowData {
        a: i32,
        b: Option<i32>,
        c: String,
    }

    if let Some(rows) = session.query("SELECT a, b, c FROM ks.t", &[]).await? {
        for row_data in rows.into_typed::<RowData>() {
            let row_data = row_data?;
            println!("row_data: {:?}", row_data);
        }
    }

    // Or simply as untyped rows
    if let Some(rows) = session.query("SELECT a, b, c FROM ks.t", &[]).await? {
        for row in rows {
            let a = row.columns[0].as_ref().unwrap().as_int().unwrap();
            let b = row.columns[1].as_ref().unwrap().as_int().unwrap();
            let c = row.columns[2].as_ref().unwrap().as_text().unwrap();
            println!("a, b, c: {}, {}, {}", a, b, c);

            // Alternatively each row can be parsed individually
            // let (a2, b2, c2) = row.into_typed::<(i32, i32, String)>() ?;
        }
    }

    println!("Ok.");

    Ok(())
}
