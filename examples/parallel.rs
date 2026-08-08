//! Shows how to saturate the driver with concurrent requests.
//!
//! Three things matter for throughput:
//! - the statement is prepared once and reused, so the driver knows its
//!   partition key and can route every request straight to a replica;
//! - requests are issued concurrently rather than one after another, since a
//!   single request spends almost all of its time waiting for the database;
//! - the number of in-flight requests is bounded, so that a burst of work
//!   cannot exhaust memory or overload the cluster.

use anyhow::Result;
use scylla::client::session::Session;
use scylla::client::session_builder::SessionBuilder;
use std::env;
use std::sync::Arc;
use tokio::task::JoinSet;

/// How many rows to insert.
const ROWS: i32 = 100_000;

/// How many requests may be in flight at any given moment.
const CONCURRENCY: usize = 256;

#[tokio::main]
async fn main() -> Result<()> {
    let uri = env::var("SCYLLA_URI").unwrap_or_else(|_| "172.42.0.2:9042".to_string());

    println!("Connecting to {uri} ...");

    let session: Session = SessionBuilder::new().known_node(uri).build().await?;
    let session = Arc::new(session);

    session.query_unpaged("CREATE KEYSPACE IF NOT EXISTS examples_ks WITH REPLICATION = {'class' : 'NetworkTopologyStrategy', 'replication_factor' : 1}", &[]).await?;

    session
        .query_unpaged(
            "CREATE TABLE IF NOT EXISTS examples_ks.parallel (a int, b int, c text, primary key (a, b))",
            &[],
        )
        .await?;

    // Prepare once, outside of the loop. Preparing per request would double the
    // number of round trips and defeat token-aware routing.
    let insert = Arc::new(
        session
            .prepare("INSERT INTO examples_ks.parallel (a, b, c) VALUES (?, ?, 'abc')")
            .await?,
    );

    // A `JoinSet` owns the spawned tasks and reports how many it still holds,
    // which is all that is needed to bound the concurrency: reaping a task that
    // has already finished returns immediately, so the bound never costs a wait.
    let mut requests = JoinSet::new();

    for a in 0..ROWS {
        if requests.len() >= CONCURRENCY {
            // Wait for one of the in-flight requests to finish before adding
            // another one, and propagate its error if it failed.
            requests.join_next().await.expect("set is not empty")??;
        }

        let session = Arc::clone(&session);
        let insert = Arc::clone(&insert);
        requests.spawn(async move { session.execute_unpaged(&insert, (a, 2 * a)).await });
    }

    // Wait for the remaining requests and make sure none of them failed.
    while let Some(result) = requests.join_next().await {
        result??;
    }

    println!("Ok.");

    Ok(())
}
