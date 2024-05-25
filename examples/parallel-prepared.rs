use anyhow::Result;
use scylla::{Session, SessionBuilder};
use std::env;
use std::sync::Arc;

use tokio::sync::Semaphore;

#[tokio::main]
async fn main() -> Result<()> {
    let uri = env::var("SCYLLA_URI").unwrap_or_else(|_| "127.0.0.1:9042".to_string());

    println!("Connecting to {} ...", uri);

    let session: Session = SessionBuilder::new().known_node(uri).build().await?;
    let session = Arc::new(session);

    session.query("CREATE KEYSPACE IF NOT EXISTS examples_ks WITH REPLICATION = {'class' : 'NetworkTopologyStrategy', 'replication_factor' : 1}", &[]).await?;

    session
        .query(
            "CREATE TABLE IF NOT EXISTS examples_ks.parallel_prepared (a int, b int, c text, primary key (a, b))",
            &[],
        )
        .await?;

    let prepared = Arc::new(
        session
            .prepare("INSERT INTO examples_ks.parallel_prepared (a, b, c) VALUES (?, ?, 'abc')")
            .await?,
    );
    println!("Prepared statement: {:#?}", prepared);

    let parallelism = 256;
    let sem = Arc::new(Semaphore::new(parallelism));

    for i in 0..100_000usize {
        if i % 1000 == 0 {
            println!("{}", i);
        }
        let session = session.clone();
        let prepared = prepared.clone();
        let permit = sem.clone().acquire_owned().await;
        tokio::task::spawn(async move {
            session
                .execute(&prepared, (i as i32, 2 * i as i32))
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
