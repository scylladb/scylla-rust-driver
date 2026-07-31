//! Shows the metrics that the driver collects about its own request handling:
//! request and error counters, latency percentiles, throughput rates and
//! connection statistics.
//!
//! Requires the `metrics` feature of the `scylla` crate to be enabled.

use anyhow::Result;
use futures::TryStreamExt as _;
use scylla::client::session::Session;
use scylla::client::session_builder::SessionBuilder;
use scylla::statement::unprepared::Statement;
use std::env;

#[tokio::main]
async fn main() -> Result<()> {
    let uri = env::var("SCYLLA_URI").unwrap_or_else(|_| "172.42.0.2:9042".to_string());

    println!("Connecting to {uri} ...");

    let session: Session = SessionBuilder::new().known_node(uri).build().await?;

    session.query_unpaged("CREATE KEYSPACE IF NOT EXISTS examples_ks WITH REPLICATION = {'class' : 'NetworkTopologyStrategy', 'replication_factor' : 1}", &[]).await?;

    session
        .query_unpaged(
            "CREATE TABLE IF NOT EXISTS examples_ks.metrics (a int primary key)",
            &[],
        )
        .await?;

    // Execute a couple of requests, so that the metrics below have something to report.
    let insert = session
        .prepare("INSERT INTO examples_ks.metrics (a) VALUES (?)")
        .await?;
    for a in 0..100_i32 {
        session.execute_unpaged(&insert, (a,)).await?;
    }

    let select_stmt = {
        let mut stmt = Statement::new("SELECT a FROM examples_ks.metrics");
        // To force multiple pages on our small example.
        stmt.set_page_size(10);
        stmt
    };

    // Paged requests are accounted for separately from the unpaged ones above.
    let mut rows = session
        .query_iter(select_stmt.clone(), &[])
        .await?
        .rows_stream::<(i32,)>()?;
    while rows.try_next().await?.is_some() {}

    let metrics = session.get_metrics();
    println!(
        "Unpaged queries requested: {}",
        metrics.get_requests_unpaged_num()
    );
    println!(
        "Automatically paged queries requested: {}",
        metrics.get_requests_automatically_paged_num()
    );
    println!(
        "Errors occurred in unpaged requests: {}",
        metrics.get_errors_unpaged_num()
    );
    println!(
        "Errors occurred in automatically paged requests: {}",
        metrics.get_errors_automatically_paged_num()
    );
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
