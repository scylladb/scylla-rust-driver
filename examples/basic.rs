use anyhow::Result;
use scylla::macros::FromRow;
use scylla::transport::session::Session;
use scylla::SessionBuilder;
use std::env;

#[tokio::main]
async fn main() -> Result<()> {
    let uri = env::var("SCYLLA_URI").unwrap_or_else(|_| "127.0.0.1:9042".to_string());

    println!("Connecting to {} ...", uri);

    let session: Session = SessionBuilder::new().known_node(uri).build().await?;

    session.query("CREATE KEYSPACE IF NOT EXISTS ks WITH REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor' : 1}", &[]).await?;

    session
        .query(
            "CREATE TABLE IF NOT EXISTS ks.t (a int, b int, c text, primary key (a, b))",
            &[],
        )
        .await?;

    session
        .query("INSERT INTO ks.t (a, b, c) VALUES (?, ?, ?)", (3, 4, "def"))
        .await?;

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
    let result = session.query("SELECT a, b, c FROM ks.t", &[]).await?;
    let mut iter = result.rows::<(i32, i32, String)>()?;
    while let Some((a, b, c)) = iter.next().transpose()? {
        println!("a, b, c: {}, {}, {}", a, b, c);
    }

    // Or as custom structs that derive FromRow
    #[derive(Debug, FromRow)]
    struct RowData {
        _a: i32,
        _b: Option<i32>,
        _c: String,
    }

    let result = session.query("SELECT a, b, c FROM ks.t", &[]).await?;
    let mut iter = result.rows::<(i32, i32, String)>()?;
    while let Some(row_data) = iter.next().transpose()? {
        println!("row_data: {:?}", row_data);
    }

    let metrics = session.get_metrics();
    println!("Queries requested: {}", metrics.get_queries_num());
    println!("Iter queries requested: {}", metrics.get_queries_iter_num());
    println!("Errors occurred: {}", metrics.get_errors_num());
    println!("Iter errors occurred: {}", metrics.get_errors_iter_num());
    println!("Average latency: {}", metrics.get_latency_avg_ms().unwrap());
    println!(
        "99.9 latency percentile: {}",
        metrics.get_latency_percentile_ms(99.9).unwrap()
    );

    println!("Ok.");

    Ok(())
}
