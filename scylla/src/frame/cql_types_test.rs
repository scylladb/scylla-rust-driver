// TODO: Requires a running local Scylla instance
#[tokio::test]
#[ignore]
async fn test_cql_collections() {
    // Create connection
    let uri = std::env::var("SCYLLA_URI").unwrap_or_else(|_| "127.0.0.1:9042".to_string());

    println!("Connecting to {} ...", uri);

    let session = crate::transport::session::Session::connect(uri, None)
        .await
        .unwrap();
    session.refresh_topology().await.unwrap();

    session.query("CREATE KEYSPACE IF NOT EXISTS ks WITH REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor' : 1}", &[]).await.unwrap();

    session
        .query(
            "CREATE TABLE IF NOT EXISTS ks.days (day date, id int PRIMARY KEY)",
            &[],
        )
        .await
        .unwrap();

    let date_ts: u32 = 10;
    session
        .query("INSERT INTO ks.days (day, id) VALUES (?, 1)", (date_ts,))
        .await
        .unwrap();

    session
        .query(
            "CREATE TABLE IF NOT EXISTS ks.truefalse (boolvalue boolean, id int PRIMARY KEY)",
            &[],
        )
        .await
        .unwrap();

    let val: bool = true;
    session
        .query(
            "INSERT INTO ks.truefalse (boolvalue, id) VALUES (?, 1)",
            (val,),
        )
        .await
        .unwrap();
}
