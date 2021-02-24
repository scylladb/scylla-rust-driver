use crate::transport::session::IntoTypedRows;
use crate::SessionBuilder;
use std::env;

// TODO: Requires a running local Scylla instance
#[tokio::test]
#[ignore]
async fn test_cql_types() {
    // Create connection
    let uri = env::var("SCYLLA_URI").unwrap_or_else(|_| "127.0.0.1:9042".to_string());

    println!("Connecting to {} ...", uri);

    let session = SessionBuilder::new().known_node(uri).build().await.unwrap();

    session.query("CREATE KEYSPACE IF NOT EXISTS ks WITH REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor' : 1}", &[]).await.unwrap();
    // Date type test
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

    if let Some(rows) = session
        .query("SELECT day, id FROM ks.days", &[])
        .await
        .unwrap()
    {
        for row in rows.into_typed::<(u32, i32)>() {
            let (a, b) = row.unwrap();
            println!("day, id: {}, {}", a, b);
        }
    }

    // Bool test
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

    if let Some(rows) = session
        .query("SELECT boolvalue, id FROM ks.truefalse", &[])
        .await
        .unwrap()
    {
        for row in rows.into_typed::<(bool, i32)>() {
            let (a, b) = row.unwrap();
            println!("bool, id: {}, {}", a, b);
        }
    }

    // Float test

    session
        .query(
            "CREATE TABLE IF NOT EXISTS ks.floatingpoint (boolvalue float, id int PRIMARY KEY)",
            &[],
        )
        .await
        .unwrap();

    let val: f32 = 1.42;
    session
        .query(
            "INSERT INTO ks.floatingpoint (float, id) VALUES (?, 1)",
            (val,),
        )
        .await
        .unwrap();

    if let Some(rows) = session
        .query("SELECT float, id FROM ks.floatingpoint", &[])
        .await
        .unwrap()
    {
        for row in rows.into_typed::<(f32, i32)>() {
            let (a, b) = row.unwrap();
            println!("float, id: {}, {}", a, b);
        }
    }
}
