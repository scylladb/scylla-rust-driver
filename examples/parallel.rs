use anyhow::Result;
use scylla::{LegacySession, SessionBuilder};
use std::env;
use std::sync::Arc;

use tokio::sync::Semaphore;

#[tokio::main]
async fn main() -> Result<()> {
    let uri = env::var("SCYLLA_URI").unwrap_or_else(|_| "127.0.0.1:9042".to_string());

    println!("Connecting to {} ...", uri);

    let session: LegacySession = SessionBuilder::new().known_node(uri).build().await?;
    let session = Arc::new(session);

    session.query_unpaged("CREATE KEYSPACE IF NOT EXISTS examples_ks WITH REPLICATION = {'class' : 'NetworkTopologyStrategy', 'replication_factor' : 1}", &[]).await?;

    session
        .query_unpaged(
            "CREATE TABLE IF NOT EXISTS examples_ks.parallel (a int, b int, c text, primary key (a, b))",
            &[],
        )
        .await?;

    let parallelism = 256;
    let sem = Arc::new(Semaphore::new(parallelism));

    for i in 0..100_000usize {
        if i % 1000 == 0 {
            println!("{}", i);
        }
        let session = session.clone();
        let permit = sem.clone().acquire_owned().await;
        tokio::task::spawn(async move {
            session
                .query_unpaged(
                    format!(
                        "INSERT INTO examples_ks.parallel (a, b, c) VALUES ({}, {}, 'abc')",
                        i,
                        2 * i
                    ),
                    &[],
                )
                .await
                .unwrap();

            let _permit = permit;
        });
    }

    // Wait for all in-flight requests to finish
    for _ in 0..parallelism {
        sem.acquire().await.unwrap().forget();
    }

    println!("Ok.");

    Ok(())
}
