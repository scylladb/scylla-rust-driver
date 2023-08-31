//! This example shows how to collect history of query execution.

use anyhow::Result;
use futures::StreamExt;
use scylla::history::{HistoryCollector, StructuredHistory};
use scylla::query::Query;
use scylla::session::Session;
use scylla::SessionBuilder;
use std::env;
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<()> {
    let uri = env::var("SCYLLA_URI").unwrap_or_else(|_| "127.0.0.1:9042".to_string());

    println!("Connecting to {} ...", uri);

    let session: Session = SessionBuilder::new().known_node(uri).build().await?;

    session.query("CREATE KEYSPACE IF NOT EXISTS ks WITH REPLICATION = {'class' : 'NetworkTopologyStrategy', 'replication_factor' : 1}", &[]).await?;

    session
        .query(
            "CREATE TABLE IF NOT EXISTS ks.t (a int, b int, c text, primary key (a, b))",
            &[],
        )
        .await?;

    // Create a query for which we would like to trace the history of its execution
    let mut query: Query = Query::new("SELECT * FROM ks.t");
    let history_listener = Arc::new(HistoryCollector::new());
    query.set_history_listener(history_listener.clone());

    // Run the query, doesn't matter if it failed, the history will still be saved
    let _ignore_error = session.query(query.clone(), ()).await;

    // Access the collected history and print it
    let structured_history: StructuredHistory = history_listener.clone_structured_history();
    println!("Query history: {}", structured_history);

    // A single history collector can contain histories of multiple queries.
    // To clear a collector create a new one and set it again.
    let _second_execution = session.query(query, ()).await;
    let structured_history: StructuredHistory = history_listener.clone_structured_history();
    println!("Two queries history: {}", structured_history);

    // The same works for other types of queries, e.g iterators
    for i in 0..32 {
        session
            .query("INSERT INTO ks.t (a, b, c) VALUES (?, ?, 't')", (i, i))
            .await?;
    }

    let mut iter_query: Query = Query::new("SELECT * FROM ks.t");
    iter_query.set_page_size(8);
    let iter_history_listener = Arc::new(HistoryCollector::new());
    iter_query.set_history_listener(iter_history_listener.clone());

    let mut rows_iterator = session.query_iter(iter_query, ()).await?;
    while let Some(_row) = rows_iterator.next().await {
        // Receive rows...
    }

    println!(
        "Iterator history: {}",
        iter_history_listener.clone_structured_history()
    );
    Ok(())
}
