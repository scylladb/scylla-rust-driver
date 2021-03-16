use crate::cql_to_rust::FromCqlVal;
use crate::frame::response::result::CqlValue;
use crate::frame::value::Counter;
use crate::frame::value::Value;
use crate::frame::value::{Date, Time, Timestamp};
use crate::transport::session::IntoTypedRows;
use crate::transport::session::Session;
use crate::SessionBuilder;
use bigdecimal::BigDecimal;
use chrono::{Duration, NaiveDate};
use num_bigint::BigInt;
use std::cmp::PartialEq;
use std::env;
use std::fmt::Debug;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};
use std::str::FromStr;
use uuid::Uuid;

// Used to prepare a table for test
// Creates keyspace ks
// Drops and creates table ks.{table_name} (id int PRIMARY KEY, val {type_name})
async fn init_test(table_name: &str, type_name: &str) -> Session {
    let uri = env::var("SCYLLA_URI").unwrap_or_else(|_| "127.0.0.1:9042".to_string());

    println!("Connecting to {} ...", uri);
    let session: Session = SessionBuilder::new().known_node(uri).build().await.unwrap();

    session
        .query(
            "CREATE KEYSPACE IF NOT EXISTS ks WITH REPLICATION = \
            {'class' : 'SimpleStrategy', 'replication_factor' : 1}",
            &[],
        )
        .await
        .unwrap();

    session
        .query(format!("DROP TABLE IF EXISTS ks.{}", table_name), &[])
        .await
        .unwrap();

    session
        .query(
            format!(
                "CREATE TABLE IF NOT EXISTS ks.{} (id int PRIMARY KEY, val {})",
                table_name, type_name
            ),
            &[],
        )
        .await
        .unwrap();

    session
}

// This function tests serialization and deserialization mechanisms by sending insert and select
// queries to running Scylla instance.
// To do so, it:
// Prepares a table for tests (by creating keyspace ks and table ks.{table_name} using init_test)
// Runs a test that, for every element of `tests`:
// - inserts 2 values (one encoded as string and one as bound values) into table ks.{type_name}
// - selects this 2 values and compares them with expected value
// Expected values and bound values are computed using T::from_str
async fn run_tests<T>(tests: &[&str], type_name: &str)
where
    T: Value + FromCqlVal<CqlValue> + FromStr + Debug + Clone + PartialEq,
{
    let session: Session = init_test(type_name, type_name).await;

    for test in tests.iter() {
        let insert_string_encoded_value = format!(
            "INSERT INTO ks.{} (id, val) VALUES (0, {})",
            type_name, test
        );
        session
            .query(insert_string_encoded_value, &[])
            .await
            .unwrap();

        let insert_bound_value = format!("INSERT INTO ks.{} (id, val) VALUES (1, ?)", type_name);
        let value_to_bound = T::from_str(test).ok().unwrap();
        session
            .query(insert_bound_value, (value_to_bound,))
            .await
            .unwrap();

        let select_values = format!("SELECT val from ks.{}", type_name);
        let read_values: Vec<T> = session
            .query(select_values, &[])
            .await
            .unwrap()
            .rows
            .unwrap()
            .into_typed::<(T,)>()
            .map(Result::unwrap)
            .map(|row| row.0)
            .collect::<Vec<_>>();

        let expected_value = T::from_str(test).ok().unwrap();
        assert_eq!(read_values, vec![expected_value.clone(), expected_value]);
    }
}

#[tokio::test]
async fn test_varint() {
    let tests = [
        "0",
        "1",
        "127",
        "128",
        "129",
        "-1",
        "-128",
        "-129",
        "123456789012345678901234567890",
        "-123456789012345678901234567890",
    ];

    run_tests::<BigInt>(&tests, "varint").await;
}

#[tokio::test]
async fn test_decimal() {
    let tests = [
        "4.2",
        "0",
        "1.999999999999999999999999999999999999999",
        "997",
        "123456789012345678901234567890.1234567890",
        "-123456789012345678901234567890.1234567890",
    ];

    run_tests::<BigDecimal>(&tests, "decimal").await;
}

#[tokio::test]
async fn test_bool() {
    let tests = ["true", "false"];

    run_tests::<bool>(&tests, "boolean").await;
}

#[tokio::test]
async fn test_float() {
    let max = f32::MAX.to_string();
    let min = f32::MIN.to_string();
    let tests = [
        "3.14",
        "997",
        "0.1",
        "128",
        "-128",
        max.as_str(),
        min.as_str(),
    ];

    run_tests::<f32>(&tests, "float").await;
}

#[tokio::test]
async fn test_counter() {
    let big_increment = i64::MAX.to_string();
    let tests = ["1", "997", big_increment.as_str()];

    // Can't use run_tests, because counters are special and can't be inserted
    let type_name = "counter";
    let session: Session = init_test(type_name, type_name).await;

    for (i, test) in tests.iter().enumerate() {
        let update_bound_value = format!("UPDATE ks.{} SET val = val + ? WHERE id = ?", type_name);
        let value_to_bound = Counter(i64::from_str(test).unwrap());
        session
            .query(update_bound_value, (value_to_bound, i as i32))
            .await
            .unwrap();

        let select_values = format!("SELECT val FROM ks.{} WHERE id = ?", type_name);
        let read_values: Vec<Counter> = session
            .query(select_values, (i as i32,))
            .await
            .unwrap()
            .rows
            .unwrap()
            .into_typed::<(Counter,)>()
            .map(Result::unwrap)
            .map(|row| row.0)
            .collect::<Vec<_>>();

        let expected_value = Counter(i64::from_str(test).unwrap());
        assert_eq!(read_values, vec![expected_value]);
    }
}

#[tokio::test]
async fn test_naive_date() {
    let session: Session = init_test("naive_date", "date").await;

    let min_naive_date: NaiveDate = chrono::naive::MIN_DATE;
    assert_eq!(min_naive_date, NaiveDate::from_ymd(-262144, 1, 1));

    let max_naive_date: NaiveDate = chrono::naive::MAX_DATE;
    assert_eq!(max_naive_date, NaiveDate::from_ymd(262143, 12, 31));

    let tests = [
        // Basic test values
        ("0000-1-1", Some(NaiveDate::from_ymd(0000, 1, 1))),
        ("1970-01-01", Some(NaiveDate::from_ymd(1970, 1, 1))),
        ("2020-03-07", Some(NaiveDate::from_ymd(2020, 3, 7))),
        ("1337-4-5", Some(NaiveDate::from_ymd(1337, 4, 5))),
        ("-1-12-31", Some(NaiveDate::from_ymd(-1, 12, 31))),
        // min/max values allowed by NaiveDate
        ("-262144-1-1", Some(min_naive_date)),
        ("262143-12-31", Some(max_naive_date)),
        // 1 less/more than min/max values allowed by NaiveDate
        ("-262145-12-31", None),
        ("262144-1-1", None),
        // min/max values allowed by the database
        ("-5877641-06-23", None),
        ("5881580-07-11", None),
    ];

    for (date_text, date) in tests.iter() {
        session
            .query(
                format!(
                    "INSERT INTO ks.naive_date (id, val) VALUES (0, '{}')",
                    date_text
                ),
                &[],
            )
            .await
            .unwrap();

        let read_date: Option<NaiveDate> = session
            .query("SELECT val from ks.naive_date", &[])
            .await
            .unwrap()
            .rows
            .unwrap()
            .into_typed::<(NaiveDate,)>()
            .next()
            .unwrap()
            .ok()
            .map(|row| row.0);

        assert_eq!(read_date, *date);

        // If date is representable by NaiveDate try inserting it and reading again
        if let Some(naive_date) = date {
            session
                .query(
                    "INSERT INTO ks.naive_date (id, val) VALUES (0, ?)",
                    (naive_date,),
                )
                .await
                .unwrap();

            let (read_date,): (NaiveDate,) = session
                .query("SELECT val from ks.naive_date", &[])
                .await
                .unwrap()
                .rows
                .unwrap()
                .into_typed::<(NaiveDate,)>()
                .next()
                .unwrap()
                .unwrap();
            assert_eq!(read_date, *naive_date);
        }
    }

    // 1 less/more than min/max values allowed by the database should cause error
    session
        .query(
            "INSERT INTO ks.naive_date (id, val) VALUES (0, '-5877641-06-22')",
            &[],
        )
        .await
        .unwrap_err();

    session
        .query(
            "INSERT INTO ks.naive_date (id, val) VALUES (0, '5881580-07-12')",
            &[],
        )
        .await
        .unwrap_err();
}

#[tokio::test]
async fn test_date() {
    // Tests value::Date which allows to insert dates outside NaiveDate range

    let session: Session = init_test("date_tests", "date").await;

    let tests = [
        ("1970-01-01", Date(2_u32.pow(31))),
        ("1969-12-02", Date(2_u32.pow(31) - 30)),
        ("1970-01-31", Date(2_u32.pow(31) + 30)),
        ("-5877641-06-23", Date(0)),
        ("5881580-07-11", Date(u32::max_value())),
    ];

    for (date_text, date) in &tests {
        session
            .query(
                format!(
                    "INSERT INTO ks.date_tests (id, val) VALUES (0, '{}')",
                    date_text
                ),
                &[],
            )
            .await
            .unwrap();

        let read_date: Date = session
            .query("SELECT val from ks.date_tests", &[])
            .await
            .unwrap()
            .rows
            .unwrap()[0]
            .columns[0]
            .as_ref()
            .map(|cql_val| match cql_val {
                CqlValue::Date(days) => Date(*days),
                _ => panic!(),
            })
            .unwrap();

        assert_eq!(read_date, *date);
    }
}

#[tokio::test]
async fn test_time() {
    // Time is an i64 - nanoseconds since midnight
    // in range 0..=86399999999999

    let session: Session = init_test("time_tests", "time").await;

    let max_time: i64 = 24 * 60 * 60 * 1_000_000_000 - 1;
    assert_eq!(max_time, 86399999999999);

    let tests = [
        ("00:00:00", Duration::nanoseconds(0)),
        ("01:01:01", Duration::seconds(60 * 60 + 60 + 1)),
        ("00:00:00.000000000", Duration::nanoseconds(0)),
        ("00:00:00.000000001", Duration::nanoseconds(1)),
        ("23:59:59.999999999", Duration::nanoseconds(max_time)),
    ];

    for (time_str, time_duration) in &tests {
        // Insert time as a string and verify that it matches
        session
            .query(
                format!(
                    "INSERT INTO ks.time_tests (id, val) VALUES (0, '{}')",
                    time_str
                ),
                &[],
            )
            .await
            .unwrap();

        let (read_time,): (Duration,) = session
            .query("SELECT val from ks.time_tests", &[])
            .await
            .unwrap()
            .rows
            .unwrap()
            .into_typed::<(Duration,)>()
            .next()
            .unwrap()
            .unwrap();

        assert_eq!(read_time, *time_duration);

        // Insert time as a bound Time value and verify that it matches
        session
            .query(
                "INSERT INTO ks.time_tests (id, val) VALUES (0, ?)",
                (Time(*time_duration),),
            )
            .await
            .unwrap();

        let (read_time,): (Duration,) = session
            .query("SELECT val from ks.time_tests", &[])
            .await
            .unwrap()
            .rows
            .unwrap()
            .into_typed::<(Duration,)>()
            .next()
            .unwrap()
            .unwrap();

        assert_eq!(read_time, *time_duration);
    }

    // Tests with invalid time values
    // Make sure that database rejects them
    let invalid_tests = [
        "-01:00:00",
        // "-00:00:01", - actually this gets parsed as 0h 0m 1s, looks like a harmless bug
        //"0", - this is invalid in scylla but valid in cassandra
        //"86399999999999",
        "24:00:00.000000000",
        "00:00:00.0000000001",
        "23:59:59.9999999999",
    ];

    for time_str in &invalid_tests {
        session
            .query(
                format!(
                    "INSERT INTO ks.time_tests (id, val) VALUES (0, '{}')",
                    time_str
                ),
                &[],
            )
            .await
            .unwrap_err();
    }
}

#[tokio::test]
async fn test_timestamp() {
    let session: Session = init_test("timestamp_tests", "timestamp").await;

    let epoch_date = NaiveDate::from_ymd(1970, 1, 1);

    //let before_epoch = NaiveDate::from_ymd(1333, 4, 30);
    //let before_epoch_offset = before_epoch.signed_duration_since(epoch_date);

    let after_epoch = NaiveDate::from_ymd(2020, 3, 8);
    let after_epoch_offset = after_epoch.signed_duration_since(epoch_date);

    let tests = [
        ("0", Duration::milliseconds(0)),
        (
            "9223372036854775807",
            Duration::milliseconds(i64::max_value()),
        ),
        (
            "-9223372036854775808",
            Duration::milliseconds(i64::min_value()),
        ),
        ("1970-01-01", Duration::milliseconds(0)),
        ("2020-03-08", after_epoch_offset),
        // Scylla rejects timestamps before 1970-01-01, but the specification says it shouldn't
        // https://github.com/apache/cassandra/blob/78b13cd0e7a33d45c2081bb135e860bbaca7cbe5/doc/native_protocol_v4.spec#L929
        // Scylla bug?
        // ("1333-04-30", before_epoch_offset),
        // Example taken from https://cassandra.apache.org/doc/latest/cql/types.html
        // Doesn't work 0_o - Scylla's fault?
        //("2011-02-03T04:05:00.000+0000", Duration::milliseconds(1299038700000)),
    ];

    for (timestamp_str, timestamp_duration) in &tests {
        // Insert timestamp as a string and verify that it matches
        session
            .query(
                format!(
                    "INSERT INTO ks.timestamp_tests (id, val) VALUES (0, '{}')",
                    timestamp_str
                ),
                &[],
            )
            .await
            .unwrap();

        let (read_timestamp,): (Duration,) = session
            .query("SELECT val from ks.timestamp_tests", &[])
            .await
            .unwrap()
            .rows
            .unwrap()
            .into_typed::<(Duration,)>()
            .next()
            .unwrap()
            .unwrap();

        assert_eq!(read_timestamp, *timestamp_duration);

        // Insert timestamp as a bound Timestamp value and verify that it matches
        session
            .query(
                "INSERT INTO ks.timestamp_tests (id, val) VALUES (0, ?)",
                (Timestamp(*timestamp_duration),),
            )
            .await
            .unwrap();

        let (read_timestamp,): (Duration,) = session
            .query("SELECT val from ks.timestamp_tests", &[])
            .await
            .unwrap()
            .rows
            .unwrap()
            .into_typed::<(Duration,)>()
            .next()
            .unwrap()
            .unwrap();

        assert_eq!(read_timestamp, *timestamp_duration);
    }
}

#[tokio::test]
async fn test_timeuuid() {
    let session: Session = init_test("timeuuid_tests", "timeuuid").await;

    // A few random timeuuids generated manually
    let tests = [
        (
            "8e14e760-7fa8-11eb-bc66-000000000001",
            [
                0x8e, 0x14, 0xe7, 0x60, 0x7f, 0xa8, 0x11, 0xeb, 0xbc, 0x66, 0, 0, 0, 0, 0, 0x01,
            ],
        ),
        (
            "9b349580-7fa8-11eb-bc66-000000000001",
            [
                0x9b, 0x34, 0x95, 0x80, 0x7f, 0xa8, 0x11, 0xeb, 0xbc, 0x66, 0, 0, 0, 0, 0, 0x01,
            ],
        ),
        (
            "5d74bae0-7fa3-11eb-bc66-000000000001",
            [
                0x5d, 0x74, 0xba, 0xe0, 0x7f, 0xa3, 0x11, 0xeb, 0xbc, 0x66, 0, 0, 0, 0, 0, 0x01,
            ],
        ),
    ];

    for (timeuuid_str, timeuuid_bytes) in &tests {
        // Insert timeuuid as a string and verify that it matches
        session
            .query(
                format!(
                    "INSERT INTO ks.timeuuid_tests (id, val) VALUES (0, {})",
                    timeuuid_str
                ),
                &[],
            )
            .await
            .unwrap();

        let (read_timeuuid,): (Uuid,) = session
            .query("SELECT val from ks.timeuuid_tests", &[])
            .await
            .unwrap()
            .rows
            .unwrap()
            .into_typed::<(Uuid,)>()
            .next()
            .unwrap()
            .unwrap();

        assert_eq!(read_timeuuid.as_bytes(), timeuuid_bytes);

        // Insert timeuuid as a bound value and verify that it matches
        let test_uuid: Uuid = Uuid::from_slice(timeuuid_bytes.as_ref()).unwrap();
        session
            .query(
                "INSERT INTO ks.timeuuid_tests (id, val) VALUES (0, ?)",
                (test_uuid,),
            )
            .await
            .unwrap();

        let (read_timeuuid,): (Uuid,) = session
            .query("SELECT val from ks.timeuuid_tests", &[])
            .await
            .unwrap()
            .rows
            .unwrap()
            .into_typed::<(Uuid,)>()
            .next()
            .unwrap()
            .unwrap();

        assert_eq!(read_timeuuid.as_bytes(), timeuuid_bytes);
    }
}

#[tokio::test]
async fn test_inet() {
    let session: Session = init_test("inet_tests", "inet").await;

    let tests = [
        ("0.0.0.0", IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0))),
        ("127.0.0.1", IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1))),
        ("10.0.0.1", IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1))),
        (
            "255.255.255.255",
            IpAddr::V4(Ipv4Addr::new(255, 255, 255, 255)),
        ),
        ("::0", IpAddr::V6(Ipv6Addr::new(0, 0, 0, 0, 0, 0, 0, 0))),
        ("::1", IpAddr::V6(Ipv6Addr::new(0, 0, 0, 0, 0, 0, 0, 1))),
        (
            "2001:db8::8a2e:370:7334",
            IpAddr::V6(Ipv6Addr::new(
                0x2001, 0x0db8, 0, 0, 0, 0x8a2e, 0x0370, 0x7334,
            )),
        ),
        (
            "2001:0db8:0000:0000:0000:8a2e:0370:7334",
            IpAddr::V6(Ipv6Addr::new(
                0x2001, 0x0db8, 0, 0, 0, 0x8a2e, 0x0370, 0x7334,
            )),
        ),
        (
            "ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff",
            IpAddr::V6(Ipv6Addr::new(
                u16::MAX,
                u16::MAX,
                u16::MAX,
                u16::MAX,
                u16::MAX,
                u16::MAX,
                u16::MAX,
                u16::MAX,
            )),
        ),
    ];

    for (inet_str, inet) in &tests {
        // Insert inet as a string and verify that it matches
        session
            .query(
                format!(
                    "INSERT INTO ks.inet_tests (id, val) VALUES (0, '{}')",
                    inet_str
                ),
                &[],
            )
            .await
            .unwrap();

        let (read_inet,): (IpAddr,) = session
            .query("SELECT val from ks.inet_tests WHERE id = 0", &[])
            .await
            .unwrap()
            .rows
            .unwrap()
            .into_typed::<(IpAddr,)>()
            .next()
            .unwrap()
            .unwrap();

        assert_eq!(read_inet, *inet);

        // Insert inet as a bound value and verify that it matches
        session
            .query("INSERT INTO ks.inet_tests (id, val) VALUES (0, ?)", (inet,))
            .await
            .unwrap();

        let (read_inet,): (IpAddr,) = session
            .query("SELECT val from ks.inet_tests WHERE id = 0", &[])
            .await
            .unwrap()
            .rows
            .unwrap()
            .into_typed::<(IpAddr,)>()
            .next()
            .unwrap()
            .unwrap();

        assert_eq!(read_inet, *inet);
    }
}

#[tokio::test]
async fn test_blob() {
    let session: Session = init_test("blob_tests", "blob").await;

    let long_blob: Vec<u8> = vec![0x11; 1234];
    let mut long_blob_str: String = "0x".to_string();
    long_blob_str.extend(std::iter::repeat('1').take(2 * 1234));

    let tests = [
        ("0x", vec![]),
        ("0x00", vec![0x00]),
        ("0x01", vec![0x01]),
        ("0xff", vec![0xff]),
        ("0x1122", vec![0x11, 0x22]),
        ("0x112233", vec![0x11, 0x22, 0x33]),
        ("0x11223344", vec![0x11, 0x22, 0x33, 0x44]),
        ("0x1122334455", vec![0x11, 0x22, 0x33, 0x44, 0x55]),
        ("0x112233445566", vec![0x11, 0x22, 0x33, 0x44, 0x55, 0x66]),
        (
            "0x11223344556677",
            vec![0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77],
        ),
        (
            "0x1122334455667788",
            vec![0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88],
        ),
        (&long_blob_str, long_blob),
    ];

    for (blob_str, blob) in &tests {
        // Insert blob as a string and verify that it matches
        session
            .query(
                format!(
                    "INSERT INTO ks.blob_tests (id, val) VALUES (0, {})",
                    blob_str
                ),
                &[],
            )
            .await
            .unwrap();

        let (read_blob,): (Vec<u8>,) = session
            .query("SELECT val from ks.blob_tests WHERE id = 0", &[])
            .await
            .unwrap()
            .rows
            .unwrap()
            .into_typed::<(Vec<u8>,)>()
            .next()
            .unwrap()
            .unwrap();

        assert_eq!(read_blob, *blob);

        // Insert blob as a bound value and verify that it matches
        session
            .query("INSERT INTO ks.blob_tests (id, val) VALUES (0, ?)", (blob,))
            .await
            .unwrap();

        let (read_blob,): (Vec<u8>,) = session
            .query("SELECT val from ks.blob_tests WHERE id = 0", &[])
            .await
            .unwrap()
            .rows
            .unwrap()
            .into_typed::<(Vec<u8>,)>()
            .next()
            .unwrap()
            .unwrap();

        assert_eq!(read_blob, *blob);
    }
}
