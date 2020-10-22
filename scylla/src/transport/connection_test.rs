use std::time::Duration;

use crate::transport::session::Session;

// TODO: Requires a running local Scylla instance
#[tokio::test]
#[ignore]
async fn test_connecting() {
    let session = Session::connect("localhost:9042").await.unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;

    session.query("CREATE KEYSPACE IF NOT EXISTS ks WITH REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor' : 1}").await.unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;

    session
        .query("CREATE TABLE IF NOT EXISTS ks.t (a int, b int, c text, primary key (a, b))")
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;

    session
        .query("INSERT INTO ks.t (a, b, c) VALUES (1, 2, 'abc')")
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;
}
