use crate as scylla;
use crate::cql_to_rust::FromCqlVal;
use crate::frame::response::result::CqlValue;
use crate::frame::value::{Counter, CqlDate, CqlTime, CqlTimestamp};
use crate::macros::FromUserType;
use crate::test_utils::create_new_session_builder;
use crate::transport::session::Session;
use crate::utils::test_utils::unique_keyspace_name;
use itertools::Itertools;
use scylla_cql::frame::value::{CqlTimeuuid, CqlVarint};
use scylla_cql::types::serialize::value::SerializeCql;
use scylla_macros::SerializeCql;
use std::cmp::PartialEq;
use std::fmt::Debug;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};
use std::str::FromStr;

// Used to prepare a table for test
// Creates a new keyspace
// Drops and creates table {table_name} (id int PRIMARY KEY, val {type_name})
async fn init_test(table_name: &str, type_name: &str) -> Session {
    let session: Session = create_new_session_builder().build().await.unwrap();
    let ks = unique_keyspace_name();

    session
        .query(
            format!(
                "CREATE KEYSPACE IF NOT EXISTS {} WITH REPLICATION = \
            {{'class' : 'NetworkTopologyStrategy', 'replication_factor' : 1}}",
                ks
            ),
            &[],
        )
        .await
        .unwrap();
    session.use_keyspace(ks, false).await.unwrap();

    session
        .query(format!("DROP TABLE IF EXISTS {}", table_name), &[])
        .await
        .unwrap();

    session
        .query(
            format!(
                "CREATE TABLE IF NOT EXISTS {} (id int PRIMARY KEY, val {})",
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
// Prepares a table for tests (by creating test keyspace and table {table_name} using init_test)
// Runs a test that, for every element of `tests`:
// - inserts 2 values (one encoded as string and one as bound values) into table {type_name}
// - selects this 2 values and compares them with expected value
// Expected values and bound values are computed using T::from_str
async fn run_tests<T>(tests: &[&str], type_name: &str)
where
    T: SerializeCql + FromCqlVal<CqlValue> + FromStr + Debug + Clone + PartialEq,
{
    let session: Session = init_test(type_name, type_name).await;
    session.await_schema_agreement().await.unwrap();

    for test in tests.iter() {
        let insert_string_encoded_value =
            format!("INSERT INTO {} (id, val) VALUES (0, {})", type_name, test);
        session
            .query(insert_string_encoded_value, &[])
            .await
            .unwrap();

        let insert_bound_value = format!("INSERT INTO {} (id, val) VALUES (1, ?)", type_name);
        let value_to_bound = T::from_str(test).ok().unwrap();
        session
            .query(insert_bound_value, (value_to_bound,))
            .await
            .unwrap();

        let select_values = format!("SELECT val from {}", type_name);
        let read_values: Vec<T> = session
            .query(select_values, &[])
            .await
            .unwrap()
            .rows_typed::<(T,)>()
            .unwrap()
            .map(Result::unwrap)
            .map(|row| row.0)
            .collect::<Vec<_>>();

        let expected_value = T::from_str(test).ok().unwrap();
        assert_eq!(read_values, vec![expected_value.clone(), expected_value]);
    }
}

#[cfg(any(feature = "num-bigint-03", feature = "num-bigint-04"))]
fn varint_test_cases() -> Vec<&'static str> {
    vec![
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
        // Test cases for numbers that can't be contained in u/i128.
        "1234567890123456789012345678901234567890",
        "-1234567890123456789012345678901234567890",
    ]
}

#[cfg(feature = "num-bigint-03")]
#[tokio::test]
async fn test_varint03() {
    let tests = varint_test_cases();
    run_tests::<num_bigint_03::BigInt>(&tests, "varint").await;
}

#[cfg(feature = "num-bigint-04")]
#[tokio::test]
async fn test_varint04() {
    let tests = varint_test_cases();
    run_tests::<num_bigint_04::BigInt>(&tests, "varint").await;
}

#[tokio::test]
async fn test_cql_varint() {
    let tests = [
        vec![0x00],       // 0
        vec![0x01],       // 1
        vec![0x00, 0x01], // 1 (with leading zeros)
        vec![0x7F],       // 127
        vec![0x00, 0x80], // 128
        vec![0x00, 0x81], // 129
        vec![0xFF],       // -1
        vec![0x80],       // -128
        vec![0xFF, 0x7F], // -129
        vec![
            0x01, 0x8E, 0xE9, 0x0F, 0xF6, 0xC3, 0x73, 0xE0, 0xEE, 0x4E, 0x3F, 0x0A, 0xD2,
        ], // 123456789012345678901234567890
        vec![
            0xFE, 0x71, 0x16, 0xF0, 0x09, 0x3C, 0x8C, 0x1F, 0x11, 0xB1, 0xC0, 0xF5, 0x2E,
        ], // -123456789012345678901234567890
    ];

    let table_name = "cql_varint_tests";
    let session: Session = create_new_session_builder().build().await.unwrap();
    let ks = unique_keyspace_name();

    session
        .query(
            format!(
                "CREATE KEYSPACE IF NOT EXISTS {} WITH REPLICATION = \
            {{'class' : 'NetworkTopologyStrategy', 'replication_factor' : 1}}",
                ks
            ),
            &[],
        )
        .await
        .unwrap();
    session.use_keyspace(ks, false).await.unwrap();

    session
        .query(
            format!(
                "CREATE TABLE IF NOT EXISTS {} (id int PRIMARY KEY, val varint)",
                table_name
            ),
            &[],
        )
        .await
        .unwrap();

    let prepared_insert = session
        .prepare(format!(
            "INSERT INTO {} (id, val) VALUES (0, ?)",
            table_name
        ))
        .await
        .unwrap();
    let prepared_select = session
        .prepare(format!("SELECT val FROM {} WHERE id = 0", table_name))
        .await
        .unwrap();

    for test in tests {
        let cql_varint = CqlVarint::from_signed_bytes_be_slice(&test);
        session
            .execute(&prepared_insert, (&cql_varint,))
            .await
            .unwrap();

        let read_values: Vec<CqlVarint> = session
            .execute(&prepared_select, &[])
            .await
            .unwrap()
            .rows_typed::<(CqlVarint,)>()
            .unwrap()
            .map(Result::unwrap)
            .map(|row| row.0)
            .collect::<Vec<_>>();

        assert_eq!(read_values, vec![cql_varint])
    }
}

#[cfg(feature = "bigdecimal-04")]
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

    run_tests::<bigdecimal_04::BigDecimal>(&tests, "decimal").await;
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
        let update_bound_value = format!("UPDATE {} SET val = val + ? WHERE id = ?", type_name);
        let value_to_bound = Counter(i64::from_str(test).unwrap());
        session
            .query(update_bound_value, (value_to_bound, i as i32))
            .await
            .unwrap();

        let select_values = format!("SELECT val FROM {} WHERE id = ?", type_name);
        let read_values: Vec<Counter> = session
            .query(select_values, (i as i32,))
            .await
            .unwrap()
            .rows_typed::<(Counter,)>()
            .unwrap()
            .map(Result::unwrap)
            .map(|row| row.0)
            .collect::<Vec<_>>();

        let expected_value = Counter(i64::from_str(test).unwrap());
        assert_eq!(read_values, vec![expected_value]);
    }
}

#[cfg(feature = "chrono")]
#[tokio::test]
async fn test_naive_date() {
    use chrono::Datelike;
    use chrono::NaiveDate;

    let session: Session = init_test("chrono_naive_date_tests", "date").await;

    let min_naive_date: NaiveDate = NaiveDate::MIN;
    let min_naive_date_string = min_naive_date.format("%Y-%m-%d").to_string();
    let min_naive_date_out_of_range_string = (min_naive_date.year() - 1).to_string() + "-12-31";

    let tests = [
        // Basic test values
        (
            "0000-01-01",
            Some(NaiveDate::from_ymd_opt(0, 1, 1).unwrap()),
        ),
        (
            "1970-01-01",
            Some(NaiveDate::from_ymd_opt(1970, 1, 1).unwrap()),
        ),
        (
            "2020-03-07",
            Some(NaiveDate::from_ymd_opt(2020, 3, 7).unwrap()),
        ),
        (
            "1337-04-05",
            Some(NaiveDate::from_ymd_opt(1337, 4, 5).unwrap()),
        ),
        (
            "-0001-12-31",
            Some(NaiveDate::from_ymd_opt(-1, 12, 31).unwrap()),
        ),
        // min/max values allowed by NaiveDate
        (min_naive_date_string.as_str(), Some(min_naive_date)),
        // NOTICE: dropped for Cassandra 4 compatibility
        //("262143-12-31", Some(max_naive_date)),

        // Slightly less/more than min/max values allowed by NaiveDate
        (min_naive_date_out_of_range_string.as_str(), None),
        // NOTICE: dropped for Cassandra 4 compatibility
        //("262144-01-01", None),
        // min/max values allowed by the database
        ("-5877641-06-23", None),
        //("5881580-07-11", None),
    ];

    for (date_text, date) in tests.iter() {
        session
            .query(
                format!(
                    "INSERT INTO chrono_naive_date_tests (id, val) VALUES (0, '{}')",
                    date_text
                ),
                &[],
            )
            .await
            .unwrap();

        let read_date: Option<NaiveDate> = session
            .query("SELECT val from chrono_naive_date_tests", &[])
            .await
            .unwrap()
            .rows_typed::<(NaiveDate,)>()
            .unwrap()
            .next()
            .unwrap()
            .ok()
            .map(|row| row.0);

        assert_eq!(read_date, *date);

        // If date is representable by NaiveDate try inserting it and reading again
        if let Some(naive_date) = date {
            session
                .query(
                    "INSERT INTO chrono_naive_date_tests (id, val) VALUES (0, ?)",
                    (naive_date,),
                )
                .await
                .unwrap();

            let (read_date,): (NaiveDate,) = session
                .query("SELECT val from chrono_naive_date_tests", &[])
                .await
                .unwrap()
                .single_row_typed::<(NaiveDate,)>()
                .unwrap();
            assert_eq!(read_date, *naive_date);
        }
    }
}

#[tokio::test]
async fn test_cql_date() {
    // Tests value::Date which allows to insert dates outside NaiveDate range

    let session: Session = init_test("cql_date_tests", "date").await;

    let tests = [
        ("1970-01-01", CqlDate(2_u32.pow(31))),
        ("1969-12-02", CqlDate(2_u32.pow(31) - 30)),
        ("1970-01-31", CqlDate(2_u32.pow(31) + 30)),
        ("-5877641-06-23", CqlDate(0)),
        // NOTICE: dropped for Cassandra 4 compatibility
        //("5881580-07-11", Date(u32::MAX)),
    ];

    for (date_text, date) in &tests {
        session
            .query(
                format!(
                    "INSERT INTO cql_date_tests (id, val) VALUES (0, '{}')",
                    date_text
                ),
                &[],
            )
            .await
            .unwrap();

        let read_date: CqlDate = session
            .query("SELECT val from cql_date_tests", &[])
            .await
            .unwrap()
            .rows
            .unwrap()[0]
            .columns[0]
            .as_ref()
            .map(|cql_val| cql_val.as_cql_date().unwrap())
            .unwrap();

        assert_eq!(read_date, *date);
    }

    // 1 less/more than min/max values allowed by the database should cause error
    session
        .query(
            "INSERT INTO cql_date_tests (id, val) VALUES (0, '-5877641-06-22')",
            &[],
        )
        .await
        .unwrap_err();

    session
        .query(
            "INSERT INTO cql_date_tests (id, val) VALUES (0, '5881580-07-12')",
            &[],
        )
        .await
        .unwrap_err();
}

#[cfg(feature = "time")]
#[tokio::test]
async fn test_date() {
    use time::{Date, Month::*};

    let session: Session = init_test("time_date_tests", "date").await;

    let tests = [
        // Basic test values
        (
            "0000-01-01",
            Some(Date::from_calendar_date(0, January, 1).unwrap()),
        ),
        (
            "1970-01-01",
            Some(Date::from_calendar_date(1970, January, 1).unwrap()),
        ),
        (
            "2020-03-07",
            Some(Date::from_calendar_date(2020, March, 7).unwrap()),
        ),
        (
            "1337-04-05",
            Some(Date::from_calendar_date(1337, April, 5).unwrap()),
        ),
        (
            "-0001-12-31",
            Some(Date::from_calendar_date(-1, December, 31).unwrap()),
        ),
        // min/max values allowed by time::Date depend on feature flags, but following values must always be allowed
        (
            "9999-12-31",
            Some(Date::from_calendar_date(9999, December, 31).unwrap()),
        ),
        (
            "-9999-01-01",
            Some(Date::from_calendar_date(-9999, January, 1).unwrap()),
        ),
        // min value allowed by the database
        ("-5877641-06-23", None),
    ];

    for (date_text, date) in tests.iter() {
        session
            .query(
                format!(
                    "INSERT INTO time_date_tests (id, val) VALUES (0, '{}')",
                    date_text
                ),
                &[],
            )
            .await
            .unwrap();

        let read_date = session
            .query("SELECT val from time_date_tests", &[])
            .await
            .unwrap()
            .first_row_typed::<(Date,)>()
            .ok()
            .map(|val| val.0);

        assert_eq!(read_date, *date);

        // If date is representable by time::Date try inserting it and reading again
        if let Some(date) = date {
            session
                .query(
                    "INSERT INTO time_date_tests (id, val) VALUES (0, ?)",
                    (date,),
                )
                .await
                .unwrap();

            let (read_date,) = session
                .query("SELECT val from time_date_tests", &[])
                .await
                .unwrap()
                .first_row_typed::<(Date,)>()
                .unwrap();
            assert_eq!(read_date, *date);
        }
    }
}

#[tokio::test]
async fn test_cql_time() {
    // CqlTime is an i64 - nanoseconds since midnight
    // in range 0..=86399999999999

    let session: Session = init_test("cql_time_tests", "time").await;

    let max_time: i64 = 24 * 60 * 60 * 1_000_000_000 - 1;
    assert_eq!(max_time, 86399999999999);

    let tests = [
        ("00:00:00", CqlTime(0)),
        ("01:01:01", CqlTime((60 * 60 + 60 + 1) * 1_000_000_000)),
        ("00:00:00.000000000", CqlTime(0)),
        ("00:00:00.000000001", CqlTime(1)),
        ("23:59:59.999999999", CqlTime(max_time)),
    ];

    for (time_str, time_duration) in &tests {
        // Insert time as a string and verify that it matches
        session
            .query(
                format!(
                    "INSERT INTO cql_time_tests (id, val) VALUES (0, '{}')",
                    time_str
                ),
                &[],
            )
            .await
            .unwrap();

        let (read_time,) = session
            .query("SELECT val from cql_time_tests", &[])
            .await
            .unwrap()
            .single_row_typed::<(CqlTime,)>()
            .unwrap();

        assert_eq!(read_time, *time_duration);

        // Insert time as a bound CqlTime value and verify that it matches
        session
            .query(
                "INSERT INTO cql_time_tests (id, val) VALUES (0, ?)",
                (*time_duration,),
            )
            .await
            .unwrap();

        let (read_time,) = session
            .query("SELECT val from cql_time_tests", &[])
            .await
            .unwrap()
            .single_row_typed::<(CqlTime,)>()
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
                    "INSERT INTO cql_time_tests (id, val) VALUES (0, '{}')",
                    time_str
                ),
                &[],
            )
            .await
            .unwrap_err();
    }
}

#[cfg(feature = "chrono")]
#[tokio::test]
async fn test_naive_time() {
    use chrono::NaiveTime;

    let session = init_test("chrono_time_tests", "time").await;

    let tests = [
        ("00:00:00", NaiveTime::MIN),
        ("01:01:01", NaiveTime::from_hms_opt(1, 1, 1).unwrap()),
        (
            "00:00:00.000000000",
            NaiveTime::from_hms_nano_opt(0, 0, 0, 0).unwrap(),
        ),
        (
            "00:00:00.000000001",
            NaiveTime::from_hms_nano_opt(0, 0, 0, 1).unwrap(),
        ),
        (
            "12:34:56.789012345",
            NaiveTime::from_hms_nano_opt(12, 34, 56, 789_012_345).unwrap(),
        ),
        (
            "23:59:59.999999999",
            NaiveTime::from_hms_nano_opt(23, 59, 59, 999_999_999).unwrap(),
        ),
    ];

    for (time_text, time) in tests.iter() {
        // Insert as string and read it again
        session
            .query(
                format!(
                    "INSERT INTO chrono_time_tests (id, val) VALUES (0, '{}')",
                    time_text
                ),
                &[],
            )
            .await
            .unwrap();

        let (read_time,) = session
            .query("SELECT val from chrono_time_tests", &[])
            .await
            .unwrap()
            .first_row_typed::<(NaiveTime,)>()
            .unwrap();

        assert_eq!(read_time, *time);

        // Insert as type and read it again
        session
            .query(
                "INSERT INTO chrono_time_tests (id, val) VALUES (0, ?)",
                (time,),
            )
            .await
            .unwrap();

        let (read_time,) = session
            .query("SELECT val from chrono_time_tests", &[])
            .await
            .unwrap()
            .first_row_typed::<(NaiveTime,)>()
            .unwrap();
        assert_eq!(read_time, *time);
    }

    // chrono can represent leap seconds, this should not panic
    let leap_second = NaiveTime::from_hms_nano_opt(23, 59, 59, 1_500_000_000);
    session
        .query(
            "INSERT INTO cql_time_tests (id, val) VALUES (0, ?)",
            (leap_second,),
        )
        .await
        .unwrap_err();
}

#[cfg(feature = "time")]
#[tokio::test]
async fn test_time() {
    use time::Time;

    let session = init_test("time_time_tests", "time").await;

    let tests = [
        ("00:00:00", Time::MIDNIGHT),
        ("01:01:01", Time::from_hms(1, 1, 1).unwrap()),
        (
            "00:00:00.000000000",
            Time::from_hms_nano(0, 0, 0, 0).unwrap(),
        ),
        (
            "00:00:00.000000001",
            Time::from_hms_nano(0, 0, 0, 1).unwrap(),
        ),
        (
            "12:34:56.789012345",
            Time::from_hms_nano(12, 34, 56, 789_012_345).unwrap(),
        ),
        (
            "23:59:59.999999999",
            Time::from_hms_nano(23, 59, 59, 999_999_999).unwrap(),
        ),
    ];

    for (time_text, time) in tests.iter() {
        // Insert as string and read it again
        session
            .query(
                format!(
                    "INSERT INTO time_time_tests (id, val) VALUES (0, '{}')",
                    time_text
                ),
                &[],
            )
            .await
            .unwrap();

        let (read_time,) = session
            .query("SELECT val from time_time_tests", &[])
            .await
            .unwrap()
            .first_row_typed::<(Time,)>()
            .unwrap();

        assert_eq!(read_time, *time);

        // Insert as type and read it again
        session
            .query(
                "INSERT INTO time_time_tests (id, val) VALUES (0, ?)",
                (time,),
            )
            .await
            .unwrap();

        let (read_time,) = session
            .query("SELECT val from time_time_tests", &[])
            .await
            .unwrap()
            .first_row_typed::<(Time,)>()
            .unwrap();
        assert_eq!(read_time, *time);
    }
}

#[tokio::test]
async fn test_cql_timestamp() {
    let session: Session = init_test("cql_timestamp_tests", "timestamp").await;

    //let epoch_date = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();

    //let before_epoch = NaiveDate::from_ymd_opt(1333, 4, 30).unwrap();
    //let before_epoch_offset = before_epoch.signed_duration_since(epoch_date);

    //let after_epoch = NaiveDate::from_ymd_opt(2020, 3, 8).unwrap();
    //let after_epoch_offset = after_epoch.signed_duration_since(epoch_date);

    let tests = [
        ("0", CqlTimestamp(0)),
        ("9223372036854775807", CqlTimestamp(i64::MAX)),
        ("-9223372036854775808", CqlTimestamp(i64::MIN)),
        // NOTICE: dropped for Cassandra 4 compatibility
        //("1970-01-01", Duration::milliseconds(0)),
        //("2020-03-08", after_epoch_offset),

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
                    "INSERT INTO cql_timestamp_tests (id, val) VALUES (0, '{}')",
                    timestamp_str
                ),
                &[],
            )
            .await
            .unwrap();

        let (read_timestamp,) = session
            .query("SELECT val from cql_timestamp_tests", &[])
            .await
            .unwrap()
            .single_row_typed::<(CqlTimestamp,)>()
            .unwrap();

        assert_eq!(read_timestamp, *timestamp_duration);

        // Insert timestamp as a bound CqlTimestamp value and verify that it matches
        session
            .query(
                "INSERT INTO cql_timestamp_tests (id, val) VALUES (0, ?)",
                (*timestamp_duration,),
            )
            .await
            .unwrap();

        let (read_timestamp,) = session
            .query("SELECT val from cql_timestamp_tests", &[])
            .await
            .unwrap()
            .single_row_typed::<(CqlTimestamp,)>()
            .unwrap();

        assert_eq!(read_timestamp, *timestamp_duration);
    }
}

#[cfg(feature = "chrono")]
#[tokio::test]
async fn test_date_time() {
    use chrono::{DateTime, NaiveDate, NaiveDateTime, NaiveTime, Utc};

    let session = init_test("chrono_datetime_tests", "timestamp").await;

    let tests = [
        ("0", DateTime::from_timestamp(0, 0).unwrap()),
        (
            "2001-02-03T04:05:06.789+0000",
            NaiveDateTime::new(
                NaiveDate::from_ymd_opt(2001, 2, 3).unwrap(),
                NaiveTime::from_hms_milli_opt(4, 5, 6, 789).unwrap(),
            )
            .and_utc(),
        ),
        (
            "2011-02-03T04:05:00.000+0000",
            NaiveDateTime::new(
                NaiveDate::from_ymd_opt(2011, 2, 3).unwrap(),
                NaiveTime::from_hms_milli_opt(4, 5, 0, 0).unwrap(),
            )
            .and_utc(),
        ),
        // New Zealand timezone, converted to GMT
        (
            "2011-02-03T04:05:06.987+1245",
            NaiveDateTime::new(
                NaiveDate::from_ymd_opt(2011, 2, 2).unwrap(),
                NaiveTime::from_hms_milli_opt(15, 20, 6, 987).unwrap(),
            )
            .and_utc(),
        ),
        (
            "9999-12-31T23:59:59.999+0000",
            NaiveDateTime::new(
                NaiveDate::from_ymd_opt(9999, 12, 31).unwrap(),
                NaiveTime::from_hms_milli_opt(23, 59, 59, 999).unwrap(),
            )
            .and_utc(),
        ),
        (
            "-377705116800000",
            NaiveDateTime::new(
                NaiveDate::from_ymd_opt(-9999, 1, 1).unwrap(),
                NaiveTime::from_hms_milli_opt(0, 0, 0, 0).unwrap(),
            )
            .and_utc(),
        ),
    ];

    for (datetime_text, datetime) in tests.iter() {
        // Insert as string and read it again
        session
            .query(
                format!(
                    "INSERT INTO chrono_datetime_tests (id, val) VALUES (0, '{}')",
                    datetime_text
                ),
                &[],
            )
            .await
            .unwrap();

        let (read_datetime,) = session
            .query("SELECT val from chrono_datetime_tests", &[])
            .await
            .unwrap()
            .first_row_typed::<(DateTime<Utc>,)>()
            .unwrap();

        assert_eq!(read_datetime, *datetime);

        // Insert as type and read it again
        session
            .query(
                "INSERT INTO chrono_datetime_tests (id, val) VALUES (0, ?)",
                (datetime,),
            )
            .await
            .unwrap();

        let (read_datetime,) = session
            .query("SELECT val from chrono_datetime_tests", &[])
            .await
            .unwrap()
            .first_row_typed::<(DateTime<Utc>,)>()
            .unwrap();
        assert_eq!(read_datetime, *datetime);
    }

    // chrono datetime has higher precision, round excessive submillisecond time down
    let nanosecond_precision_1st_half = NaiveDateTime::new(
        NaiveDate::from_ymd_opt(2015, 6, 30).unwrap(),
        NaiveTime::from_hms_nano_opt(23, 59, 59, 123_123_456).unwrap(),
    )
    .and_utc();
    let nanosecond_precision_1st_half_rounded = NaiveDateTime::new(
        NaiveDate::from_ymd_opt(2015, 6, 30).unwrap(),
        NaiveTime::from_hms_milli_opt(23, 59, 59, 123).unwrap(),
    )
    .and_utc();
    session
        .query(
            "INSERT INTO chrono_datetime_tests (id, val) VALUES (0, ?)",
            (nanosecond_precision_1st_half,),
        )
        .await
        .unwrap();

    let (read_datetime,) = session
        .query("SELECT val from chrono_datetime_tests", &[])
        .await
        .unwrap()
        .first_row_typed::<(DateTime<Utc>,)>()
        .unwrap();
    assert_eq!(read_datetime, nanosecond_precision_1st_half_rounded);

    let nanosecond_precision_2nd_half = NaiveDateTime::new(
        NaiveDate::from_ymd_opt(2015, 6, 30).unwrap(),
        NaiveTime::from_hms_nano_opt(23, 59, 59, 123_987_654).unwrap(),
    )
    .and_utc();
    let nanosecond_precision_2nd_half_rounded = NaiveDateTime::new(
        NaiveDate::from_ymd_opt(2015, 6, 30).unwrap(),
        NaiveTime::from_hms_milli_opt(23, 59, 59, 123).unwrap(),
    )
    .and_utc();
    session
        .query(
            "INSERT INTO chrono_datetime_tests (id, val) VALUES (0, ?)",
            (nanosecond_precision_2nd_half,),
        )
        .await
        .unwrap();

    let (read_datetime,) = session
        .query("SELECT val from chrono_datetime_tests", &[])
        .await
        .unwrap()
        .first_row_typed::<(DateTime<Utc>,)>()
        .unwrap();
    assert_eq!(read_datetime, nanosecond_precision_2nd_half_rounded);

    // chrono can represent leap seconds, this should not panic
    let leap_second = NaiveDateTime::new(
        NaiveDate::from_ymd_opt(2015, 6, 30).unwrap(),
        NaiveTime::from_hms_milli_opt(23, 59, 59, 1500).unwrap(),
    )
    .and_utc();
    session
        .query(
            "INSERT INTO cql_datetime_tests (id, val) VALUES (0, ?)",
            (leap_second,),
        )
        .await
        .unwrap_err();
}

#[cfg(feature = "time")]
#[tokio::test]
async fn test_offset_date_time() {
    use time::{Date, Month::*, OffsetDateTime, PrimitiveDateTime, Time, UtcOffset};

    let session = init_test("time_datetime_tests", "timestamp").await;

    let tests = [
        ("0", OffsetDateTime::UNIX_EPOCH),
        (
            "2001-02-03T04:05:06.789+0000",
            PrimitiveDateTime::new(
                Date::from_calendar_date(2001, February, 3).unwrap(),
                Time::from_hms_milli(4, 5, 6, 789).unwrap(),
            )
            .assume_utc(),
        ),
        (
            "2011-02-03T04:05:00.000+0000",
            PrimitiveDateTime::new(
                Date::from_calendar_date(2011, February, 3).unwrap(),
                Time::from_hms_milli(4, 5, 0, 0).unwrap(),
            )
            .assume_utc(),
        ),
        // New Zealand timezone, converted to GMT
        (
            "2011-02-03T04:05:06.987+1245",
            PrimitiveDateTime::new(
                Date::from_calendar_date(2011, February, 3).unwrap(),
                Time::from_hms_milli(4, 5, 6, 987).unwrap(),
            )
            .assume_offset(UtcOffset::from_hms(12, 45, 0).unwrap()),
        ),
        (
            "9999-12-31T23:59:59.999+0000",
            PrimitiveDateTime::new(
                Date::from_calendar_date(9999, December, 31).unwrap(),
                Time::from_hms_milli(23, 59, 59, 999).unwrap(),
            )
            .assume_utc(),
        ),
        (
            "-377705116800000",
            PrimitiveDateTime::new(
                Date::from_calendar_date(-9999, January, 1).unwrap(),
                Time::from_hms_milli(0, 0, 0, 0).unwrap(),
            )
            .assume_utc(),
        ),
    ];

    for (datetime_text, datetime) in tests.iter() {
        // Insert as string and read it again
        session
            .query(
                format!(
                    "INSERT INTO time_datetime_tests (id, val) VALUES (0, '{}')",
                    datetime_text
                ),
                &[],
            )
            .await
            .unwrap();

        let (read_datetime,) = session
            .query("SELECT val from time_datetime_tests", &[])
            .await
            .unwrap()
            .first_row_typed::<(OffsetDateTime,)>()
            .unwrap();

        assert_eq!(read_datetime, *datetime);

        // Insert as type and read it again
        session
            .query(
                "INSERT INTO time_datetime_tests (id, val) VALUES (0, ?)",
                (datetime,),
            )
            .await
            .unwrap();

        let (read_datetime,) = session
            .query("SELECT val from time_datetime_tests", &[])
            .await
            .unwrap()
            .first_row_typed::<(OffsetDateTime,)>()
            .unwrap();
        assert_eq!(read_datetime, *datetime);
    }

    // time datetime has higher precision, round excessive submillisecond time down
    let nanosecond_precision_1st_half = PrimitiveDateTime::new(
        Date::from_calendar_date(2015, June, 30).unwrap(),
        Time::from_hms_nano(23, 59, 59, 123_123_456).unwrap(),
    )
    .assume_utc();
    let nanosecond_precision_1st_half_rounded = PrimitiveDateTime::new(
        Date::from_calendar_date(2015, June, 30).unwrap(),
        Time::from_hms_milli(23, 59, 59, 123).unwrap(),
    )
    .assume_utc();
    session
        .query(
            "INSERT INTO time_datetime_tests (id, val) VALUES (0, ?)",
            (nanosecond_precision_1st_half,),
        )
        .await
        .unwrap();

    let (read_datetime,) = session
        .query("SELECT val from time_datetime_tests", &[])
        .await
        .unwrap()
        .first_row_typed::<(OffsetDateTime,)>()
        .unwrap();
    assert_eq!(read_datetime, nanosecond_precision_1st_half_rounded);

    let nanosecond_precision_2nd_half = PrimitiveDateTime::new(
        Date::from_calendar_date(2015, June, 30).unwrap(),
        Time::from_hms_nano(23, 59, 59, 123_987_654).unwrap(),
    )
    .assume_utc();
    let nanosecond_precision_2nd_half_rounded = PrimitiveDateTime::new(
        Date::from_calendar_date(2015, June, 30).unwrap(),
        Time::from_hms_milli(23, 59, 59, 123).unwrap(),
    )
    .assume_utc();
    session
        .query(
            "INSERT INTO time_datetime_tests (id, val) VALUES (0, ?)",
            (nanosecond_precision_2nd_half,),
        )
        .await
        .unwrap();

    let (read_datetime,) = session
        .query("SELECT val from time_datetime_tests", &[])
        .await
        .unwrap()
        .first_row_typed::<(OffsetDateTime,)>()
        .unwrap();
    assert_eq!(read_datetime, nanosecond_precision_2nd_half_rounded);
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
                    "INSERT INTO timeuuid_tests (id, val) VALUES (0, {})",
                    timeuuid_str
                ),
                &[],
            )
            .await
            .unwrap();

        let (read_timeuuid,): (CqlTimeuuid,) = session
            .query("SELECT val from timeuuid_tests", &[])
            .await
            .unwrap()
            .single_row_typed::<(CqlTimeuuid,)>()
            .unwrap();

        assert_eq!(read_timeuuid.as_bytes(), timeuuid_bytes);

        // Insert timeuuid as a bound value and verify that it matches
        let test_uuid: CqlTimeuuid = CqlTimeuuid::from_slice(timeuuid_bytes.as_ref()).unwrap();
        session
            .query(
                "INSERT INTO timeuuid_tests (id, val) VALUES (0, ?)",
                (test_uuid,),
            )
            .await
            .unwrap();

        let (read_timeuuid,): (CqlTimeuuid,) = session
            .query("SELECT val from timeuuid_tests", &[])
            .await
            .unwrap()
            .single_row_typed::<(CqlTimeuuid,)>()
            .unwrap();

        assert_eq!(read_timeuuid.as_bytes(), timeuuid_bytes);
    }
}

#[tokio::test]
async fn test_timeuuid_ordering() {
    let session: Session = create_new_session_builder().build().await.unwrap();
    let ks = unique_keyspace_name();

    session
        .query(
            format!(
                "CREATE KEYSPACE IF NOT EXISTS {} WITH REPLICATION = \
            {{'class' : 'NetworkTopologyStrategy', 'replication_factor' : 1}}",
                ks
            ),
            &[],
        )
        .await
        .unwrap();
    session.use_keyspace(ks, false).await.unwrap();

    session
        .query(
            "CREATE TABLE tab (p int, t timeuuid, PRIMARY KEY (p, t))",
            (),
        )
        .await
        .unwrap();

    // Timeuuid values, sorted in the same order as Scylla/Cassandra sorts them.
    let sorted_timeuuid_vals: Vec<CqlTimeuuid> = vec![
        CqlTimeuuid::from_str("00000000-0000-1000-8080-808080808080").unwrap(),
        CqlTimeuuid::from_str("00000000-0000-1000-ffff-ffffffffffff").unwrap(),
        CqlTimeuuid::from_str("00000000-0000-1000-0000-000000000000").unwrap(),
        CqlTimeuuid::from_str("fed35080-0efb-11ee-a1ca-00006490e9a4").unwrap(),
        CqlTimeuuid::from_str("00000257-0efc-11ee-9547-00006490e9a6").unwrap(),
        CqlTimeuuid::from_str("ffffffff-ffff-1fff-ffff-ffffffffffef").unwrap(),
        CqlTimeuuid::from_str("ffffffff-ffff-1fff-ffff-ffffffffffff").unwrap(),
        CqlTimeuuid::from_str("ffffffff-ffff-1fff-0000-000000000000").unwrap(),
        CqlTimeuuid::from_str("ffffffff-ffff-1fff-7f7f-7f7f7f7f7f7f").unwrap(),
    ];

    // Generate all permutations.
    let perms = Itertools::permutations(sorted_timeuuid_vals.iter(), sorted_timeuuid_vals.len())
        .collect::<Vec<_>>();
    // Ensure that all of the permutations were generated.
    assert_eq!(362880, perms.len());

    // Verify that Scylla really sorts timeuuids as defined in sorted_timeuuid_vals
    let prepared = session
        .prepare("INSERT INTO tab (p, t) VALUES (0, ?)")
        .await
        .unwrap();
    for timeuuid_val in &perms[0] {
        session.execute(&prepared, (timeuuid_val,)).await.unwrap();
    }

    let scylla_order_timeuuids: Vec<CqlTimeuuid> = session
        .query("SELECT t FROM tab WHERE p = 0", ())
        .await
        .unwrap()
        .rows_typed::<(CqlTimeuuid,)>()
        .unwrap()
        .map(|r| r.unwrap().0)
        .collect();

    assert_eq!(sorted_timeuuid_vals, scylla_order_timeuuids);

    for perm in perms {
        // Test if rust timeuuid values are sorted in the same way as in Scylla
        let mut rust_sorted_timeuuids: Vec<CqlTimeuuid> = perm
            .clone()
            .into_iter()
            .map(|x| x.to_owned())
            .collect::<Vec<_>>();
        rust_sorted_timeuuids.sort();

        assert_eq!(sorted_timeuuid_vals, rust_sorted_timeuuids);
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
                    "INSERT INTO inet_tests (id, val) VALUES (0, '{}')",
                    inet_str
                ),
                &[],
            )
            .await
            .unwrap();

        let (read_inet,): (IpAddr,) = session
            .query("SELECT val from inet_tests WHERE id = 0", &[])
            .await
            .unwrap()
            .single_row_typed::<(IpAddr,)>()
            .unwrap();

        assert_eq!(read_inet, *inet);

        // Insert inet as a bound value and verify that it matches
        session
            .query("INSERT INTO inet_tests (id, val) VALUES (0, ?)", (inet,))
            .await
            .unwrap();

        let (read_inet,): (IpAddr,) = session
            .query("SELECT val from inet_tests WHERE id = 0", &[])
            .await
            .unwrap()
            .single_row_typed::<(IpAddr,)>()
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
                format!("INSERT INTO blob_tests (id, val) VALUES (0, {})", blob_str),
                &[],
            )
            .await
            .unwrap();

        let (read_blob,): (Vec<u8>,) = session
            .query("SELECT val from blob_tests WHERE id = 0", &[])
            .await
            .unwrap()
            .single_row_typed::<(Vec<u8>,)>()
            .unwrap();

        assert_eq!(read_blob, *blob);

        // Insert blob as a bound value and verify that it matches
        session
            .query("INSERT INTO blob_tests (id, val) VALUES (0, ?)", (blob,))
            .await
            .unwrap();

        let (read_blob,): (Vec<u8>,) = session
            .query("SELECT val from blob_tests WHERE id = 0", &[])
            .await
            .unwrap()
            .single_row_typed::<(Vec<u8>,)>()
            .unwrap();

        assert_eq!(read_blob, *blob);
    }
}

#[tokio::test]
async fn test_udt_after_schema_update() {
    let table_name = "udt_tests";
    let type_name = "usertype1";

    let session: Session = create_new_session_builder().build().await.unwrap();
    let ks = unique_keyspace_name();

    session
        .query(
            format!(
                "CREATE KEYSPACE IF NOT EXISTS {} WITH REPLICATION = \
            {{'class' : 'NetworkTopologyStrategy', 'replication_factor' : 1}}",
                ks
            ),
            &[],
        )
        .await
        .unwrap();
    session.use_keyspace(ks, false).await.unwrap();

    session
        .query(format!("DROP TABLE IF EXISTS {}", table_name), &[])
        .await
        .unwrap();

    session
        .query(format!("DROP TYPE IF EXISTS {}", type_name), &[])
        .await
        .unwrap();

    session
        .query(
            format!(
                "CREATE TYPE IF NOT EXISTS {} (first int, second boolean)",
                type_name
            ),
            &[],
        )
        .await
        .unwrap();

    session
        .query(
            format!(
                "CREATE TABLE IF NOT EXISTS {} (id int PRIMARY KEY, val {})",
                table_name, type_name
            ),
            &[],
        )
        .await
        .unwrap();

    #[derive(SerializeCql, FromUserType, Debug, PartialEq)]
    #[scylla(crate = crate)]
    struct UdtV1 {
        first: i32,
        second: bool,
    }

    let v1 = UdtV1 {
        first: 123,
        second: true,
    };

    session
        .query(
            format!(
                "INSERT INTO {}(id,val) VALUES (0, {})",
                table_name, "{first: 123, second: true}"
            ),
            &[],
        )
        .await
        .unwrap();

    let (read_udt,): (UdtV1,) = session
        .query(format!("SELECT val from {} WHERE id = 0", table_name), &[])
        .await
        .unwrap()
        .single_row_typed::<(UdtV1,)>()
        .unwrap();

    assert_eq!(read_udt, v1);

    session
        .query(
            format!("INSERT INTO {}(id,val) VALUES (0, ?)", table_name),
            &(&v1,),
        )
        .await
        .unwrap();

    let (read_udt,): (UdtV1,) = session
        .query(format!("SELECT val from {} WHERE id = 0", table_name), &[])
        .await
        .unwrap()
        .single_row_typed::<(UdtV1,)>()
        .unwrap();

    assert_eq!(read_udt, v1);

    session
        .query(format!("ALTER TYPE {} ADD third text;", type_name), &[])
        .await
        .unwrap();

    #[derive(FromUserType, Debug, PartialEq)]
    struct UdtV2 {
        first: i32,
        second: bool,
        third: Option<String>,
    }

    let (read_udt,): (UdtV2,) = session
        .query(format!("SELECT val from {} WHERE id = 0", table_name), &[])
        .await
        .unwrap()
        .single_row_typed::<(UdtV2,)>()
        .unwrap();

    assert_eq!(
        read_udt,
        UdtV2 {
            first: 123,
            second: true,
            third: None,
        }
    );
}

#[tokio::test]
async fn test_empty() {
    let session: Session = init_test("empty_tests", "int").await;

    session
        .query(
            "INSERT INTO empty_tests (id, val) VALUES (0, blobasint(0x))",
            (),
        )
        .await
        .unwrap();

    let (empty,) = session
        .query("SELECT val FROM empty_tests WHERE id = 0", ())
        .await
        .unwrap()
        .first_row_typed::<(CqlValue,)>()
        .unwrap();

    assert_eq!(empty, CqlValue::Empty);

    session
        .query(
            "INSERT INTO empty_tests (id, val) VALUES (1, ?)",
            (CqlValue::Empty,),
        )
        .await
        .unwrap();

    let (empty,) = session
        .query("SELECT val FROM empty_tests WHERE id = 1", ())
        .await
        .unwrap()
        .first_row_typed::<(CqlValue,)>()
        .unwrap();

    assert_eq!(empty, CqlValue::Empty);
}

#[tokio::test]
async fn test_udt_with_missing_field() {
    let table_name = "udt_tests";
    let type_name = "usertype1";

    let session: Session = create_new_session_builder().build().await.unwrap();
    let ks = unique_keyspace_name();

    session
        .query(
            format!(
                "CREATE KEYSPACE IF NOT EXISTS {} WITH REPLICATION = \
            {{'class' : 'NetworkTopologyStrategy', 'replication_factor' : 1}}",
                ks
            ),
            &[],
        )
        .await
        .unwrap();
    session.use_keyspace(ks, false).await.unwrap();

    session
        .query(format!("DROP TABLE IF EXISTS {}", table_name), &[])
        .await
        .unwrap();

    session
        .query(format!("DROP TYPE IF EXISTS {}", type_name), &[])
        .await
        .unwrap();

    session
        .query(
            format!(
                "CREATE TYPE IF NOT EXISTS {} (first int, second boolean, third float, fourth blob)",
                type_name
            ),
            &[],
        )
        .await
        .unwrap();

    session
        .query(
            format!(
                "CREATE TABLE IF NOT EXISTS {} (id int PRIMARY KEY, val {})",
                table_name, type_name
            ),
            &[],
        )
        .await
        .unwrap();

    let mut id = 0;

    async fn verify_insert_select_identity<TQ, TR>(
        session: &Session,
        table_name: &str,
        id: i32,
        element: TQ,
        expected: TR,
    ) where
        TQ: SerializeCql,
        TR: FromCqlVal<CqlValue> + PartialEq + Debug,
    {
        session
            .query(
                format!("INSERT INTO {}(id,val) VALUES (?,?)", table_name),
                &(id, &element),
            )
            .await
            .unwrap();
        let result = session
            .query(
                format!("SELECT val from {} WHERE id = ?", table_name),
                &(id,),
            )
            .await
            .unwrap()
            .single_row_typed::<(TR,)>()
            .unwrap()
            .0;
        assert_eq!(expected, result);
    }

    #[derive(FromUserType, Debug, PartialEq)]
    struct UdtFull {
        first: i32,
        second: bool,
        third: Option<f32>,
        fourth: Option<Vec<u8>>,
    }

    #[derive(SerializeCql)]
    #[scylla(crate = crate)]
    struct UdtV1 {
        first: i32,
        second: bool,
    }

    verify_insert_select_identity(
        &session,
        table_name,
        id,
        UdtV1 {
            first: 3,
            second: true,
        },
        UdtFull {
            first: 3,
            second: true,
            third: None,
            fourth: None,
        },
    )
    .await;

    id += 1;

    #[derive(SerializeCql)]
    #[scylla(crate = crate)]
    struct UdtV2 {
        first: i32,
        second: bool,
        third: Option<f32>,
    }

    verify_insert_select_identity(
        &session,
        table_name,
        id,
        UdtV2 {
            first: 3,
            second: true,
            third: Some(123.45),
        },
        UdtFull {
            first: 3,
            second: true,
            third: Some(123.45),
            fourth: None,
        },
    )
    .await;

    id += 1;

    #[derive(SerializeCql)]
    #[scylla(crate = crate)]
    struct UdtV3 {
        first: i32,
        second: bool,
        fourth: Option<Vec<u8>>,
    }

    verify_insert_select_identity(
        &session,
        table_name,
        id,
        UdtV3 {
            first: 3,
            second: true,
            fourth: Some(vec![3, 6, 9]),
        },
        UdtFull {
            first: 3,
            second: true,
            third: None,
            fourth: Some(vec![3, 6, 9]),
        },
    )
    .await;

    id += 1;

    #[derive(SerializeCql)]
    #[scylla(crate = crate, flavor="enforce_order")]
    struct UdtV4 {
        first: i32,
        second: bool,
    }

    verify_insert_select_identity(
        &session,
        table_name,
        id,
        UdtV4 {
            first: 3,
            second: true,
        },
        UdtFull {
            first: 3,
            second: true,
            third: None,
            fourth: None,
        },
    )
    .await;
}
