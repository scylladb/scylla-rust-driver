use anyhow::Result;
use scylla::transport::session::Session;
use std::env;
use std::sync::Arc;

use tokio::sync::Semaphore;

#[tokio::main]
async fn main() -> Result<()> {
    let uri = env::var("SCYLLA_URI").unwrap_or("127.0.0.1:9042".to_string());

    println!("Connecting to {} ...", uri);

    let session = Arc::new(Session::connect(uri.parse()?, None).await?);

    session.query("CREATE KEYSPACE IF NOT EXISTS ks WITH REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor' : 1}", &[]).await?;

    session
        .query(
            "CREATE TABLE IF NOT EXISTS ks.t2 (a int, b int, c text, primary key (a, b))",
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
            let i = i;
            session
                .query(
                    format!(
                        "INSERT INTO ks.t2 (a, b, c) VALUES ({}, {}, 'abc')",
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
        sem.acquire().await.forget();
    }

    println!("Ok.");

    Ok(())
}
