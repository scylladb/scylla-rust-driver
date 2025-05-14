use anyhow::Result;
use futures::StreamExt as _;
use futures::TryStreamExt as _;
use scylla::client::session::Session;
use scylla::client::session_builder::SessionBuilder;
use scylla::value::Row;
use scylla::DeserializeRow;
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
    #[expect(unused)]
    #[derive(Debug, DeserializeRow)]
    struct RowData {
        a: i32,
        b: Option<i32>,
        c: String,
    }

    let mut iter = session
        .query_iter("SELECT a, b, c FROM examples_ks.basic", &[])
        .await?
        .rows_stream::<RowData>()?;
    while let Some(row_data) = iter.try_next().await? {
        println!("row_data: {:?}", row_data);
    }

    // Or simply as untyped rows
    let mut iter = session
        .query_iter("SELECT a, b, c FROM examples_ks.basic", &[])
        .await?
        .rows_stream::<Row>()?;
    while let Some(row) = iter.next().await.transpose()? {
        let a = row.columns[0].as_ref().unwrap().as_int().unwrap();
        let b = row.columns[1].as_ref().unwrap().as_int().unwrap();
        let c = row.columns[2].as_ref().unwrap().as_text().unwrap();
        println!("a, b, c: {}, {}, {}", a, b, c);
    }

    let metrics = session.get_metrics();
    println!("Queries requested: {}", metrics.get_queries_num());
    println!("Iter queries requested: {}", metrics.get_queries_iter_num());
    println!("Errors occurred: {}", metrics.get_errors_num());
    println!("Iter errors occurred: {}", metrics.get_errors_iter_num());
    println!("Average latency: {}", metrics.get_latency_avg_ms()?);
    println!(
        "99.9 latency percentile: {}",
        metrics.get_latency_percentile_ms(99.9)?
    );

    let snapshot = metrics.get_snapshot()?;
    println!("Min: {}", snapshot.min);
    println!("Max: {}", snapshot.max);
    println!("Mean: {}", snapshot.mean);
    println!("Standard deviation: {}", snapshot.stddev);
    println!("Median: {}", snapshot.median);
    println!("75th percentile: {}", snapshot.percentile_75);
    println!("95th percentile: {}", snapshot.percentile_95);
    println!("98th percentile: {}", snapshot.percentile_98);
    println!("99th percentile: {}", snapshot.percentile_99);
    println!("99.9th percentile: {}", snapshot.percentile_99_9);

    println!("Mean rate: {}", metrics.get_mean_rate());
    println!("One minute rate: {}", metrics.get_one_minute_rate());
    println!("Five minute rate: {}", metrics.get_five_minute_rate());
    println!("Fifteen minute rate: {}", metrics.get_fifteen_minute_rate());

    println!("Total connections: {}", metrics.get_total_connections());
    println!("Connection timeouts: {}", metrics.get_connection_timeouts());
    println!("Requests timeouts: {}", metrics.get_request_timeouts());

    println!("Ok.");

    Ok(())
}
