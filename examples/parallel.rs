use anyhow::Result;
use futures::future::join_all;
use scylla::transport::session::Session;
use std::env;
use std::sync::Arc;

use tokio::sync::Semaphore;

#[tokio::main]
async fn main() -> Result<()> {
    let uri = env::var("SCYLLA_URI").unwrap_or("localhost:9042".to_string());

    println!("Connecting to {} ...", uri);

    let session = Arc::new(Session::connect(uri, None).await?);

    session.query("CREATE KEYSPACE IF NOT EXISTS ks WITH REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor' : 1}", &[]).await?;

    session
        .query(
            "CREATE TABLE IF NOT EXISTS ks.t2 (a int, b int, c text, primary key (a, b))",
            &[],
        )
        .await?;

    let sem = Arc::new(Semaphore::new(256));

    let no_tasks = 100_000usize;
    let mut futs = Vec::with_capacity(no_tasks);
    for i in 0..no_tasks {
        if i % 1000 == 0 {
            println!("{}", i);
        }
        let session = session.clone();
        let permit = sem.clone().acquire_owned().await;
        futs.push(tokio::task::spawn(async move {
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
        }));
    }

    join_all(futs).await;

    println!("Ok.");

    Ok(())
}
