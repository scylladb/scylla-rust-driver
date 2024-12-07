use anyhow::Result;
use scylla::transport::session::Session;
use scylla::DeserializeRow;
use scylla::QueryRowsResult;
use scylla::SessionBuilder;
use std::env;

#[tokio::main]
async fn main() -> Result<()> {
    let uri = env::var("SCYLLA_URI").unwrap_or_else(|_| "127.0.0.1:9042".to_string());

    println!("Connecting to {} ...", uri);

    let session: Session = SessionBuilder::new().known_node(uri).build().await?;

    session.query_unpaged("CREATE KEYSPACE IF NOT EXISTS examples_ks WITH REPLICATION = {'class' : 'NetworkTopologyStrategy', 'replication_factor' : 1}", &[]).await?;

    session
        .query_unpaged(
            "CREATE TABLE IF NOT EXISTS examples_ks.basic (a int, b int, c text, primary key (a, b))",
            &[],
        )
        .await?;

    session
        .query_unpaged(
            "INSERT INTO examples_ks.basic (a, b, c) VALUES (?, ?, ?)",
            (3, 4, "lorem Ipsum jest tekstem stosowanym jako przykładowy wypełniacz w przemyśle poligraficznym. Został po raz pierwszy użyty w XV w. przez nieznanego drukarza do wypełnienia tekstem próbnej książki. Pięć wieków później zaczął być używany przemyśle elektronicznym,"),
        )
        .await?;

    session
        .query_unpaged(
            "INSERT INTO examples_ks.basic (a, b, c) VALUES (1, 2, 'abc')",
            &[],
        )
        .await?;

    let prepared = session
        .prepare("INSERT INTO examples_ks.basic (a, b, c) VALUES (?, 7, ?)")
        .await?;
    session
        .execute_unpaged(&prepared, (42_i32, "I'm prepared!"))
        .await?;
    session
        .execute_unpaged(&prepared, (43_i32, "I'm prepared 2!"))
        .await?;
    session
        .execute_unpaged(&prepared, (44_i32, "I'm prepared 3!"))
        .await?;


    // Or as custom structs that derive DeserializeRow
    #[allow(unused)]
    #[derive(Debug, DeserializeRow)]
    struct RowData {
        a: i32,
        b: Option<i32>,
        c: String,
    }

    let result : QueryRowsResult= session
        .query_unpaged("SELECT a, b, c FROM examples_ks.basic", &[])
        .await?
        .into_rows_result()?;
   
    let displayer = result.rows_displayer();
    println!("DISPLAYER:");
    println!("{}", displayer);


    // example 2
    session
    .query_unpaged(
        "CREATE TABLE IF NOT EXISTS examples_ks.basic4 (a int, b int, c text, d int, timest timestamp, bytes blob, fl float, db double, time1 time, primary key (a, c))",
        &[],
    )
    .await?;

    println!("Created table examples_ks.basic2");
    session
    .query_unpaged(
        "INSERT INTO examples_ks.basic4 
        (a, b, c, d, timest, bytes, fl, db, time1) 
        VALUES 
        (1, 10, 'example text', 3, toTimestamp(now()), textAsBlob('sample bytes'), 3.14, 2.718281, '14:30:00');",
        &[],
    )
    .await?;
    println!("insert1 done");

    println!("SELECT * FROM examples_ks.basic2");

    let result2 : QueryRowsResult= session
        .query_unpaged("SELECT * FROM examples_ks.basic4", &[])
        .await?
        .into_rows_result()?;
   
    let displayer = result2.rows_displayer();
    println!("DISPLAYER:");
    println!("{}", displayer);

    // example 3

    session
    .query_unpaged(
        "CREATE TABLE IF NOT EXISTS examples_ks.basic6 (a int, timud timeuuid, date1 date, ipaddr inet, dur duration, primary key (a))",
        &[],
    )
    .await?;

    session
    .query_unpaged(
        "INSERT INTO examples_ks.basic6 
        (a, timud, date1, ipaddr, dur) 
        VALUES 
        (1, now(), '2021-01-01', '3.14.15.9', 1h);",
        &[],
    )
    .await?;

    session
    .query_unpaged(
        "INSERT INTO examples_ks.basic6 
        (a, timud, date1, ipaddr, dur) 
        VALUES
        (3, NOW(), '2024-01-15', '128.0.0.1', 89h4m48s137ms);", // cqlsh prints this as 89.08003805555556h4.8022833333333335m48.137s137.0ms
        &[],
    )
    .await?;

    session
    .query_unpaged(
        "INSERT INTO examples_ks.basic6 
        (a, timud, date1, ipaddr, dur) 
        VALUES
        (4, NOW(), '2024-01-15', '192.168.0.14', 13y2w89h4m48s137ms);", // cqlsh prints this as 89.08003805555556h4.8022833333333335m48.137s137.0ms
        &[],
    )
    .await?;

    session
    .query_unpaged(
        "INSERT INTO examples_ks.basic6 (a, timud, date1, ipaddr, dur) 
        VALUES (2, NOW(), '2024-02-20', '2001:0db8:0:0::1428:57ab', 5d2h);",
        &[],
    )
    .await?;

    session
    .query_unpaged(
        "INSERT INTO examples_ks.basic6 (a, timud, date1, ipaddr, dur) 
        VALUES (5, NOW(), '-250000-02-20', '2001:db8::1428:57ab', 1y1mo1w1d1h1m1s700ms);",
        &[],
    )
    .await?;

    let result2 : QueryRowsResult= session
        .query_unpaged("SELECT * FROM examples_ks.basic6", &[])
        .await?
        .into_rows_result()?;
   
    let displayer = result2.rows_displayer();
    println!("DISPLAYER:");
    println!("{}", displayer);
    Ok(())
}
