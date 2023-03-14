use anyhow::Result;
use futures::future::join_all;
use scylla::transport::session::Session;
use scylla::SessionBuilder;
use std::env;

#[tokio::main]
async fn main() -> Result<()> {
    let uri = env::var("SCYLLA_URI").unwrap_or_else(|_| "127.0.0.1:9042".to_string());

    println!("Connecting to {} ...", uri);

    let session_builder = SessionBuilder::new().known_node(uri);

    let sessions: Vec<Session> = join_all(
        (0..100)
            .map(|_: usize| async { session_builder.build().await.unwrap() })
            .collect::<Vec<_>>(),
    )
    .await;

    sessions[0].query("CREATE KEYSPACE IF NOT EXISTS ks WITH REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor' : 1}", &[]).await?;

    sessions[0]
        .query(
            "CREATE TABLE IF NOT EXISTS ks.t (a int, b int, c text, primary key (a, b))",
            &[],
        )
        .await?;

    for (i, session) in sessions.iter().enumerate().take(100) {
        session.await_schema_agreement().await.unwrap();
        session
            .query(
                "INSERT INTO ks.t (a,b,c) VALUES (?,?,?)",
                (i as i32, i as i32, "hello"),
            )
            .await
            .unwrap();
    }

    let num_rows = sessions[42]
        .query("SELECT a, b, c FROM ks.t", &[])
        .await?
        .rows_num()
        .unwrap();
    println!("Read {} rows", num_rows);

    Ok(())
}
