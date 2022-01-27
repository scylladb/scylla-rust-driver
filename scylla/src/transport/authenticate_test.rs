#[tokio::test]
#[ignore]
async fn authenticate_superuser() {
    let uri = std::env::var("SCYLLA_URI").unwrap_or_else(|_| "127.0.0.1:9042".to_string());

    println!("Connecting to {} with cassandra superuser ...", uri);

    let session = crate::SessionBuilder::new()
        .known_node(uri)
        .user("cassandra", "cassandra")
        .build()
        .await
        .unwrap();
    let ks = crate::transport::session_test::unique_name();

    session.query(format!("CREATE KEYSPACE IF NOT EXISTS {} WITH REPLICATION = {{'class' : 'SimpleStrategy', 'replication_factor' : 1}}", ks), &[]).await.unwrap();
    session.use_keyspace(ks, false).await.unwrap();
    session.query("DROP TABLE IF EXISTS t;", &[]).await.unwrap();

    println!("Ok.");
}
