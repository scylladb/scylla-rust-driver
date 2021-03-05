use crate::transport::session::IntoTypedRows;
use crate::transport::session::Session;
use crate::SessionBuilder;
use bigdecimal::BigDecimal;
use num_bigint::{BigInt, ToBigInt};
use std::env;

// TODO: Requires a running local Scylla instance
#[tokio::test]
async fn test_cql_types() {
    // Create connection
    let uri = env::var("SCYLLA_URI").unwrap_or_else(|_| "127.0.0.1:9042".to_string());

    println!("Connecting to {} ...", uri);

    let session: Session = SessionBuilder::new().known_node(uri).build().await.unwrap();

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

    if let Some(rows) = session.query("SELECT day FROM ks.days", &[]).await.unwrap() {
        for row in rows.into_typed::<(u32,)>() {
            let day: u32 = row.unwrap().0;
            println!("day: {}", day);
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
        .query("SELECT boolvalue FROM ks.truefalse", &[])
        .await
        .unwrap()
    {
        for row in rows.into_typed::<(bool,)>() {
            let bool_val: bool = row.unwrap().0;
            println!("bool value: {}", bool_val);
            assert_eq!(bool_val, true);
        }
    }

    // Float test

    session
        .query(
            "CREATE TABLE IF NOT EXISTS ks.floatingpoint (floatval float, id int PRIMARY KEY)",
            &[],
        )
        .await
        .unwrap();

    let val: f32 = 1.42;
    session
        .query(
            "INSERT INTO ks.floatingpoint (floatval, id) VALUES (?, 1)",
            (val,),
        )
        .await
        .unwrap();

    session
        .query(
            "INSERT INTO ks.floatingpoint (floatval, id) VALUES (?, 2)",
            (f32::MAX,),
        )
        .await
        .unwrap();

    session
        .query(
            "INSERT INTO ks.floatingpoint (floatval, id) VALUES (?, 2)",
            (f32::MIN,),
        )
        .await
        .unwrap();

    if let Some(rows) = session
        .query("SELECT floatval FROM ks.floatingpoint", &[])
        .await
        .unwrap()
    {
        for row in rows.into_typed::<(f32,)>() {
            let row_val: f32 = row.unwrap().0;
            println!("float value: {}", row_val);
        }
    }

    // Varint

    session
        .query(
            "CREATE TABLE IF NOT EXISTS ks.varint_table (value varint, id int PRIMARY KEY)",
            &[],
        )
        .await
        .unwrap();

    let val_positive = 10000.to_bigint().unwrap();
    let val_negative = (-1111111111111111111_i64).to_bigint().unwrap();

    session
        .query(
            "INSERT INTO ks.varint_table (value, id) VALUES (?, 1)",
            (val_positive,),
        )
        .await
        .unwrap();

    session
        .query(
            "INSERT INTO ks.varint_table (value, id) VALUES (?, 2)",
            (val_negative,),
        )
        .await
        .unwrap();

    if let Some(rows) = session
        .query("SELECT value FROM ks.varint_table where id = 1", &[])
        .await
        .unwrap()
    {
        for row in rows.into_typed::<(BigInt,)>() {
            let bigint_value_row: BigInt = row.unwrap().0;
            println!("varint value: {}", bigint_value_row);
            assert_eq!(bigint_value_row, 10000.to_bigint().unwrap());
        }
    }

    if let Some(rows) = session
        .query("SELECT value FROM ks.varint_table where id = 2", &[])
        .await
        .unwrap()
    {
        for row in rows.into_typed::<(BigInt,)>() {
            let bigint_value_row: BigInt = row.unwrap().0;
            println!("varint value: {}", bigint_value_row);
            assert_eq!(
                bigint_value_row,
                (-1111111111111111111_i64).to_bigint().unwrap()
            );
        }
    }

    // Decimal

    session
        .query(
            "CREATE TABLE IF NOT EXISTS ks.decimal_table (value decimal, id int PRIMARY KEY)",
            &[],
        )
        .await
        .unwrap();

    let scale: i64 = 2;
    let val_positive = 10000.to_bigint().unwrap();
    let val_negative = (-1111111111111111111_i64).to_bigint().unwrap();

    let decimal_positive = BigDecimal::from((val_positive, scale));
    let decimal_negative = BigDecimal::from((val_negative, scale));

    let decimal_other = BigDecimal::from(-234);

    session
        .query(
            "INSERT INTO ks.decimal_table (value, id) VALUES (?, 1)",
            (decimal_positive,),
        )
        .await
        .unwrap();

    session
        .query(
            "INSERT INTO ks.decimal_table (value, id) VALUES (1000000000000000000000000000000.111, 5)",
            (),
        )
        .await
        .unwrap();

    if let Some(rows) = session
        .query("SELECT value FROM ks.decimal_table WHERE id = 1", &[])
        .await
        .unwrap()
    {
        for row in rows.into_typed::<(BigDecimal,)>() {
            let decimal_new: BigDecimal = row.unwrap().0;
            println!("decimal value: {}", decimal_new);
            assert_eq!(
                decimal_new,
                BigDecimal::from((10000.to_bigint().unwrap(), scale))
            );
        }
    }

    session
        .query(
            "INSERT INTO ks.decimal_table (value, id) VALUES (?, 2)",
            (decimal_negative,),
        )
        .await
        .unwrap();

    if let Some(rows) = session
        .query("SELECT value FROM ks.decimal_table WHERE id = 2", &[])
        .await
        .unwrap()
    {
        for row in rows.into_typed::<(BigDecimal,)>() {
            let decimal_new: BigDecimal = row.unwrap().0;
            println!("decimal value: {}", decimal_new);
            assert_eq!(
                decimal_new,
                BigDecimal::from(((-1111111111111111111_i64).to_bigint().unwrap(), scale))
            );
        }
    }

    session
        .query(
            "INSERT INTO ks.decimal_table (value, id) VALUES (?, 3)",
            (decimal_other,),
        )
        .await
        .unwrap();

    if let Some(rows) = session
        .query("SELECT value FROM ks.decimal_table WHERE id = 3", &[])
        .await
        .unwrap()
    {
        for row in rows.into_typed::<(BigDecimal,)>() {
            let decimal_new: BigDecimal = row.unwrap().0;
            println!("decimal value: {}", decimal_new);
            assert_eq!(decimal_new, BigDecimal::from(-234));
        }
    }
}
