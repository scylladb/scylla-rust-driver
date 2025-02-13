use core::str;
use std::fmt::{Debug, Display};
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};
use std::str::FromStr;
use std::vec;

use crate::utils::{
    create_new_session_builder, scylla_supports_tablets, setup_tracing, unique_keyspace_name,
    DeserializeOwnedValue, PerformDDL,
};
use assert_matches::assert_matches;
use itertools::Itertools;
use scylla::client::session::Session;
use scylla::deserialize::row::BuiltinDeserializationError;
use scylla::deserialize::value::{Emptiable, MaybeEmpty};
use scylla::errors::DeserializationError;
use scylla::serialize::value::SerializeValue;
use scylla::value::{
    Counter, CqlDate, CqlDuration, CqlTime, CqlTimestamp, CqlTimeuuid, CqlValue, CqlVarint,
};
use scylla::{DeserializeValue, SerializeValue};

async fn prepare_test_table(table_name: &str, type_name: &str, supports_tablets: bool) -> Session {
    let session: Session = create_new_session_builder().build().await.unwrap();
    let ks = unique_keyspace_name();

    let mut create_ks = format!(
        "CREATE KEYSPACE IF NOT EXISTS {} WITH REPLICATION = \
    {{'class' : 'NetworkTopologyStrategy', 'replication_factor' : 1}}",
        ks
    );

    if !supports_tablets && scylla_supports_tablets(&session).await {
        create_ks += " AND TABLETS = {'enabled': false}"
    }

    session.ddl(create_ks).await.unwrap();
    session.use_keyspace(ks, false).await.unwrap();

    session
        .ddl(format!("DROP TABLE IF EXISTS {}", table_name))
        .await
        .unwrap();

    session
        .ddl(format!(
            "CREATE TABLE IF NOT EXISTS {} (id int PRIMARY KEY, val {})",
            table_name, type_name
        ))
        .await
        .unwrap();

    session
}

fn assert_error_is_not_expected_non_null(error: &DeserializationError) {
    match &error.downcast_ref::<BuiltinDeserializationError>().unwrap().kind{
        scylla::deserialize::row::BuiltinDeserializationErrorKind::ColumnDeserializationFailed { err, .. } => match err.downcast_ref::<scylla::deserialize::value::BuiltinDeserializationError>().unwrap().kind {
            scylla::deserialize::value::BuiltinDeserializationErrorKind::ExpectedNonNull => {
            },
            _ => panic!("Unexpected error: {:?}", error),
        }
        _ => panic!("Unexpected error: {:?}", error),
    }
}

#[cfg(any(feature = "chrono-04", feature = "time-03",))]
fn assert_error_is_not_overflow_or_expected_non_null(error: &DeserializationError) {
    match &error.downcast_ref::<BuiltinDeserializationError>().unwrap().kind {
            scylla::deserialize::row::BuiltinDeserializationErrorKind::ColumnDeserializationFailed { err, .. } => match err.downcast_ref::<scylla::deserialize::value::BuiltinDeserializationError>().unwrap().kind {
                scylla::deserialize::value::BuiltinDeserializationErrorKind::ValueOverflow => {
                },
                scylla::deserialize::value::BuiltinDeserializationErrorKind::ExpectedNonNull => {
                },
                _ => panic!("Unexpected error: {:?}", error),
            }
            _ => panic!("Unexpected error: {:?}", error),
    }
}

// Native type test, which expects value to have the display trait and be trivially copyable
async fn run_native_serialize_test<T>(table_name: &str, type_name: &str, values: &[Option<T>])
where
    T: SerializeValue
        + DeserializeOwnedValue
        + FromStr
        + Debug
        + Clone
        + PartialEq
        + Default
        + Display
        + Copy,
{
    let session = prepare_test_table(table_name, type_name, true).await;
    tracing::debug!("Active keyspace: {}", session.get_keyspace().unwrap());

    for original_value in values {
        session
            .query_unpaged(
                format!("INSERT INTO {} (id, val) VALUES (1, ?)", table_name),
                (&original_value,),
            )
            .await
            .unwrap();

        let selected_value: Vec<T> = session
            .query_unpaged(format!("SELECT val FROM {}", table_name), &[])
            .await
            .unwrap()
            .into_rows_result()
            .unwrap()
            .rows::<(T,)>()
            .unwrap()
            .map(|row| match row {
                Ok((val,)) => val,
                Err(e) => {
                    assert_error_is_not_expected_non_null(&e);
                    T::default()
                }
            })
            .collect::<Vec<_>>();

        for value in selected_value {
            tracing::debug!(
                "Received: {} | Expected: {}",
                value,
                if original_value.is_none() {
                    T::default()
                } else {
                    original_value.unwrap()
                }
            );
            assert_eq!(value, original_value.unwrap_or(T::default()));
        }
    }
}

// Test for types that either don't have the Display trait or foreign types from other libraries
async fn run_foreign_serialize_test<T>(table_name: &str, type_name: &str, values: &[Option<T>])
where
    T: SerializeValue + DeserializeOwnedValue + Debug + Clone + PartialEq,
{
    let session = prepare_test_table(table_name, type_name, true).await;

    for original_value in values {
        session
            .query_unpaged(
                format!("INSERT INTO {} (id, val) VALUES (1, ?)", table_name),
                (&original_value,),
            )
            .await
            .unwrap();

        let selected_value: Vec<T> = session
            .query_unpaged(format!("SELECT val FROM {}", table_name), &[])
            .await
            .unwrap()
            .into_rows_result()
            .unwrap()
            .rows::<(T,)>()
            .unwrap()
            .filter_map(|row| match row {
                Ok((_,)) => row.ok(),
                Err(e) => {
                    assert_error_is_not_expected_non_null(&e);
                    None
                }
            })
            .map(|row| row.0)
            .collect::<Vec<_>>();

        for value in selected_value {
            assert_eq!(Some(value), *original_value);
        }
    }
}

async fn run_literal_and_bound_value_test<LitVal, BoundVal>(
    table_name: &str,
    type_name: &str,
    values: &[(LitVal, BoundVal)],
    escape_literal: bool,
) where
    LitVal: Display + Clone + PartialEq,
    BoundVal: SerializeValue + DeserializeOwnedValue + Debug + Clone + PartialEq,
{
    fn error_handler(error: &DeserializationError) {
        assert_error_is_not_expected_non_null(error);
    }

    run_literal_and_bound_value_test_with_callback(
        table_name,
        type_name,
        values,
        escape_literal,
        &error_handler,
    )
    .await;
}

// Test that inserts a raw literal value inside the query and compares it to a boundable type
async fn run_literal_and_bound_value_test_with_callback<LitVal, BoundVal, ErrHandler>(
    table_name: &str,
    type_name: &str,
    values: &[(LitVal, BoundVal)],
    escape_literal: bool,
    err_handler: &ErrHandler,
) where
    LitVal: Display + Clone + PartialEq,
    BoundVal: SerializeValue + DeserializeOwnedValue + Debug + Clone + PartialEq,
    ErrHandler: Fn(&DeserializationError),
{
    let session = prepare_test_table(table_name, type_name, true).await;

    for (literal_val, bound_val) in values {
        session
            .query_unpaged(
                format!(
                    "INSERT INTO {} (id, val) VALUES (1, {})",
                    table_name,
                    if escape_literal {
                        format!("'{}'", literal_val)
                    } else {
                        literal_val.to_string()
                    }
                ),
                &[],
            )
            .await
            .unwrap();

        let read_literal_val: Vec<BoundVal> = session
            .query_unpaged(format!("SELECT val FROM {}", table_name), &[])
            .await
            .unwrap()
            .into_rows_result()
            .unwrap()
            .rows::<(BoundVal,)>()
            .unwrap()
            .filter_map(|row| match row {
                Ok((_,)) => row.ok(),
                Err(e) => {
                    err_handler(&e);
                    None
                }
            })
            .map(|row| row.0)
            .collect::<Vec<_>>();

        session
            .query_unpaged(
                format!("INSERT INTO {} (id, val) VALUES (1, ?)", table_name),
                (&bound_val,),
            )
            .await
            .unwrap();

        let read_bound_val: Vec<BoundVal> = session
            .query_unpaged(format!("SELECT val FROM {}", table_name), &[])
            .await
            .unwrap()
            .into_rows_result()
            .unwrap()
            .rows::<(BoundVal,)>()
            .unwrap()
            .filter_map(|row| match row {
                Ok((_,)) => row.ok(),
                Err(e) => {
                    err_handler(&e);
                    None
                }
            })
            .map(|row| row.0)
            .collect::<Vec<_>>();

        for (read_literal, read_bound) in read_literal_val.iter().zip(read_bound_val.iter()) {
            assert_eq!(*read_literal, *bound_val);
            assert_eq!(*read_bound, *bound_val);
        }
    }
}

// Special test for types that can return MaybeEmpty containers for certain results
async fn run_literal_input_maybe_empty_output_test<X>(
    table_name: &str,
    type_name: &str,
    values: &[(Option<&str>, Option<MaybeEmpty<X>>)],
) where
    X: SerializeValue + DeserializeOwnedValue + Debug + Clone + PartialEq + Emptiable,
{
    let session = prepare_test_table(table_name, type_name, true).await;

    for (input_val, expected_val) in values {
        let query = format!(
            "INSERT INTO {} (id, val) VALUES (1, '{}')",
            table_name,
            &input_val.unwrap()
        );
        tracing::debug!("Executing query: {}", query);
        session.query_unpaged(query, &[]).await.unwrap();

        let selected_values: Vec<MaybeEmpty<X>> = session
            .query_unpaged(format!("SELECT val FROM {}", table_name), &[])
            .await
            .unwrap()
            .into_rows_result()
            .unwrap()
            .rows::<(MaybeEmpty<X>,)>()
            .unwrap()
            .filter_map(|row| match row {
                Ok((_,)) => row.ok(),
                Err(e) => {
                    assert_error_is_not_expected_non_null(&e);
                    None
                }
            })
            .map(|row| row.0)
            .collect::<Vec<_>>();

        for read_value in selected_values {
            assert_eq!(Some(read_value), *expected_val);
        }
    }
}

// Scalar types

#[tokio::test]
async fn test_serialize_deserialize_bigint() {
    setup_tracing();
    const TABLE_NAME: &str = "bigint_serialization_test";

    let test_cases = [
        Some(i64::MIN),
        Some(i64::MAX),
        Some(1),
        Some(-1),
        Some(0),
        None,
    ];

    run_native_serialize_test(TABLE_NAME, "bigint", &test_cases).await;
}

#[tokio::test]
async fn test_serialize_deserialize_int() {
    setup_tracing();
    const TABLE_NAME: &str = "int_serialization_test";

    let test_cases = [
        Some(i32::MIN),
        Some(i32::MAX),
        Some(1),
        Some(-1),
        Some(0),
        None,
    ];

    run_native_serialize_test(TABLE_NAME, "int", &test_cases).await;
}

#[tokio::test]
async fn test_serialize_deserialize_smallint() {
    setup_tracing();
    const TABLE_NAME: &str = "smallint_serialization_test";

    let test_cases = [
        Some(i16::MIN),
        Some(i16::MAX),
        Some(1),
        Some(-1),
        Some(0),
        None,
    ];

    run_native_serialize_test(TABLE_NAME, "smallint", &test_cases).await;
}

#[tokio::test]
async fn test_serialize_deserialize_tinyint() {
    setup_tracing();
    const TABLE_NAME: &str = "tinyint_serialization_test";

    let test_cases = [
        Some(i8::MIN),
        Some(i8::MAX),
        Some(1),
        Some(-1),
        Some(0),
        None,
    ];

    run_native_serialize_test(TABLE_NAME, "tinyint", &test_cases).await;
}

#[tokio::test]
async fn test_serialize_deserialize_float() {
    setup_tracing();
    const TABLE_NAME: &str = "float_serialization_test";

    let test_cases = [
        Some(f32::MIN),
        Some(f32::MAX),
        Some(1.0),
        Some(-1.0),
        Some(0.0),
        None,
    ];

    run_native_serialize_test(TABLE_NAME, "float", &test_cases).await;
}

#[tokio::test]
async fn test_serialize_deserialize_bool() {
    setup_tracing();
    const TABLE_NAME: &str = "bool_serialization_test";

    let test_cases = [Some(true), Some(false), None];

    run_native_serialize_test(TABLE_NAME, "boolean", &test_cases).await;
}

#[tokio::test]
async fn test_serialize_deserialize_double() {
    setup_tracing();
    const TABLE_NAME: &str = "double_serialization_test";

    let test_cases = [
        Some(f64::MIN),
        Some(f64::MAX),
        Some(1.0),
        Some(-1.0),
        Some(0.0),
        None,
    ];

    run_native_serialize_test(TABLE_NAME, "double", &test_cases).await;
}

#[tokio::test]
async fn test_serialize_deserialize_double_literal() {
    setup_tracing();
    const TABLE_NAME: &str = "double_serialization_test";

    let test_cases: Vec<(String, f64)> = [f64::MIN, f64::MAX, 1.0, -1.0, 0.0, 0.1]
        .iter()
        .map(|val| (val.to_string(), *val))
        .collect();

    run_literal_and_bound_value_test(TABLE_NAME, "double", test_cases.as_slice(), false).await;
}

#[tokio::test]
async fn test_serialize_deserialize_float_literal() {
    setup_tracing();
    const TABLE_NAME: &str = "float_serialization_test";

    let test_cases: Vec<(String, f32)> = [f32::MIN, f32::MAX, 1.0, -1.0, 0.0, 0.1]
        .iter()
        .map(|val| (val.to_string(), *val))
        .collect();

    run_literal_and_bound_value_test(TABLE_NAME, "float", test_cases.as_slice(), false).await;
}

// Arbitrary precision types

#[tokio::test]
async fn test_serialize_deserialize_cql_varint() {
    setup_tracing();
    const TABLE_NAME: &str = "varint_serialization_test";

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

    let mut test_cases: Vec<Option<CqlVarint>> = tests
        .iter()
        .map(|val| Some(CqlVarint::from_signed_bytes_be_slice(val)))
        .collect();
    test_cases.push(None);

    run_foreign_serialize_test(TABLE_NAME, "varint", test_cases.as_slice()).await;
}

#[cfg(any(
    feature = "num-bigint-03",
    feature = "num-bigint-04",
    feature = "bigdecimal-04"
))]
fn test_num_set() -> Vec<&'static str> {
    vec![
        "0",
        "-1",
        "1",
        "127",
        "128",
        "-127",
        "-128",
        "255",
        "123456789012345678901234567890",
        "-123456789012345678901234567890",
        // Test cases for numbers that can't be contained in u/i128.
        "1234567890123456789012345678901234567890",
        "-1234567890123456789012345678901234567890",
    ]
}

#[cfg(feature = "num-bigint-03")]
#[tokio::test]
async fn test_serialize_deserialize_num_bigint_03_varint() {
    use num_bigint_03::BigInt;

    setup_tracing();
    const TABLE_NAME: &str = "bn03_varint_serialization_test";

    let mut test_cases: Vec<Option<BigInt>> = test_num_set()
        .iter()
        .map(|val| Some(BigInt::from_str(val).expect("Failed to parse BigInt")))
        .collect();
    test_cases.push(None);

    run_foreign_serialize_test(TABLE_NAME, "varint", test_cases.as_slice()).await;
}

#[cfg(feature = "num-bigint-04")]
#[tokio::test]
async fn test_serialize_deserialize_num_bigint_04_varint() {
    use num_bigint_04::BigInt;

    setup_tracing();
    const TABLE_NAME: &str = "bn04_varint_serialization_test";

    let mut test_cases: Vec<Option<BigInt>> = test_num_set()
        .iter()
        .map(|val| Some(BigInt::from_str(val).expect("Failed to parse BigInt")))
        .collect();
    test_cases.push(None);

    run_foreign_serialize_test(TABLE_NAME, "varint", test_cases.as_slice()).await;
}

#[cfg(feature = "bigdecimal-04")]
#[tokio::test]
async fn test_serialize_deserialize_num_bigdecimal_04_varint() {
    use bigdecimal_04::BigDecimal;

    setup_tracing();
    const TABLE_NAME: &str = "bd04_decimal_serialization_test";

    let mut test_cases: Vec<Option<BigDecimal>> = test_num_set()
        .iter()
        .map(|val| Some(BigDecimal::from_str(val).expect("Failed to parse BigInt")))
        .collect();
    test_cases.push(None);

    run_foreign_serialize_test(TABLE_NAME, "decimal", test_cases.as_slice()).await;
}

// Special types

#[tokio::test]
async fn test_serialize_deserialize_counter() {
    setup_tracing();
    const TABLE_NAME: &str = "counter_serialization_test";
    let test_cases = [-1, 0, 1, 127, 1000, i64::MAX, i64::MIN];
    let session: Session = prepare_test_table(TABLE_NAME, "counter", false).await;

    for (i, test_value) in test_cases.iter().enumerate() {
        let prepared_statement = format!("UPDATE {} SET val = val + ? WHERE id = ?", TABLE_NAME);
        let value_to_bind = Counter(*test_value);
        session
            .query_unpaged(prepared_statement, (value_to_bind, i as i32))
            .await
            .unwrap();

        let select_values = format!("SELECT val FROM {} WHERE id = ?", TABLE_NAME);
        let read_values: Vec<Counter> = session
            .query_unpaged(select_values, (i as i32,))
            .await
            .unwrap()
            .into_rows_result()
            .unwrap()
            .rows::<(Counter,)>()
            .unwrap()
            .map(Result::unwrap)
            .map(|row| row.0)
            .collect::<Vec<_>>();

        let expected_value = Counter(*test_value);
        assert_eq!(read_values, vec![expected_value]);
    }
}

#[tokio::test]
async fn test_serialize_deserialize_inet() {
    setup_tracing();
    const TABLE_NAME: &str = "inet_serialization_test";

    let mut test_cases: Vec<(Option<&str>, Option<MaybeEmpty<IpAddr>>)> = vec![
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
    ]
    .iter()
    .map(|(str_val, inet_val)| (Some(*str_val), Some(MaybeEmpty::Value(*inet_val))))
    .collect();

    test_cases.push((Some(""), Some(MaybeEmpty::Empty)));

    run_literal_input_maybe_empty_output_test(TABLE_NAME, "inet", test_cases.as_slice()).await;
}

// Blob type

#[tokio::test]
async fn test_serialize_deserialize_blob() {
    setup_tracing();
    const TABLE_NAME: &str = "blob_serialization_test";

    let long_blob: Vec<u8> = vec![0x11; 1234];
    let mut long_blob_str: String = "0x".to_string();
    long_blob_str.extend(std::iter::repeat('1').take(2 * 1234));

    let test_cases: Vec<(Option<&str>, Option<Vec<u8>>)> = vec![
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
    ]
    .iter()
    .map(|(str_val, blob_val)| (Some(*str_val), Some(blob_val.clone())))
    .collect();

    let session = prepare_test_table(TABLE_NAME, "blob", true).await;

    for (input_val, expected_val) in test_cases {
        let query = format!(
            "INSERT INTO {} (id, val) VALUES (1, {})",
            TABLE_NAME,
            &input_val.unwrap()
        );
        tracing::debug!("Executing query: {}", query);
        session.query_unpaged(query, &[]).await.unwrap();

        let selected_values: Vec<Vec<u8>> = session
            .query_unpaged(format!("SELECT val FROM {}", TABLE_NAME), &[])
            .await
            .unwrap()
            .into_rows_result()
            .unwrap()
            .rows::<(Vec<u8>,)>()
            .unwrap()
            .filter_map(|row| match row {
                Ok((_,)) => row.ok(),
                Err(e) => {
                    assert_error_is_not_expected_non_null(&e);
                    None
                }
            })
            .map(|row| row.0)
            .collect::<Vec<_>>();

        for read_value in selected_values {
            assert_eq!(Some(read_value), expected_val);
        }

        //as bound value
        let query = format!("INSERT INTO {} (id, val) VALUES (1, ?)", TABLE_NAME,);
        tracing::debug!("Executing query: {}", query);
        session
            .query_unpaged(query, (expected_val.clone().unwrap(),))
            .await
            .unwrap();

        let selected_values: Vec<Vec<u8>> = session
            .query_unpaged(format!("SELECT val FROM {}", TABLE_NAME), &[])
            .await
            .unwrap()
            .into_rows_result()
            .unwrap()
            .rows::<(Vec<u8>,)>()
            .unwrap()
            .filter_map(|row: Result<(Vec<u8>,), DeserializationError>| match row {
                Ok((_,)) => row.ok(),
                Err(e) => {
                    assert_error_is_not_expected_non_null(&e);
                    None
                }
            })
            .map(|row| row.0)
            .collect::<Vec<_>>();

        for read_value in selected_values {
            assert_eq!(Some(read_value), expected_val);
        }
    }
}

// Date, Time, Duration Types

#[cfg(feature = "chrono-04")]
#[tokio::test]
async fn test_serialize_deserialize_naive_date_04() {
    setup_tracing();
    const TABLE_NAME: &str = "chrono_04_serialization_test";
    use chrono::Datelike;
    use chrono::NaiveDate;

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

    run_literal_and_bound_value_test_with_callback(
        TABLE_NAME,
        "date",
        &tests,
        true,
        &assert_error_is_not_overflow_or_expected_non_null,
    )
    .await;
}

#[tokio::test]
async fn test_serialize_deserialize_cql_date() {
    setup_tracing();
    const TABLE_NAME: &str = "cql_date_serialization_test";
    // Tests value::Date which allows to insert dates outside NaiveDate range

    let tests = [
        ("1970-01-01", CqlDate(2_u32.pow(31))),
        ("1969-12-02", CqlDate(2_u32.pow(31) - 30)),
        ("1970-01-31", CqlDate(2_u32.pow(31) + 30)),
        ("-5877641-06-23", CqlDate(0)),
        // NOTICE: dropped for Cassandra 4 compatibility
        //("5881580-07-11", Date(u32::MAX)),
    ];

    run_literal_and_bound_value_test(TABLE_NAME, "date", &tests, true).await;
}

#[tokio::test]
async fn test_serialize_deserialize_cql_date_invalid_dates() {
    setup_tracing();
    const TABLE_NAME: &str = "cql_date_serialization_test";
    let session = prepare_test_table(TABLE_NAME, "date", true).await;

    // 1 less/more than min/max values allowed by the database should cause error
    session
        .query_unpaged(
            "INSERT INTO cql_date_tests (id, val) VALUES (0, '-5877641-06-22')",
            &[],
        )
        .await
        .unwrap_err();

    session
        .query_unpaged(
            "INSERT INTO cql_date_tests (id, val) VALUES (0, '5881580-07-12')",
            &[],
        )
        .await
        .unwrap_err();
}

#[cfg(feature = "time-03")]
#[tokio::test]
async fn test_serialize_deserialize_date_03() {
    setup_tracing();
    use time::{Date, Month::*};
    const TABLE_NAME: &str = "date_03_serialization_test";

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

    run_literal_and_bound_value_test_with_callback(
        TABLE_NAME,
        "date",
        &tests,
        true,
        &assert_error_is_not_overflow_or_expected_non_null,
    )
    .await;
}

#[tokio::test]
async fn test_serialize_deserialize_cql_time() {
    setup_tracing();
    const TABLE_NAME: &str = "time_serialization_test";
    // CqlTime is an i64 - nanoseconds since midnight
    // in range 0..=86399999999999

    let max_time: i64 = 24 * 60 * 60 * 1_000_000_000 - 1;
    assert_eq!(max_time, 86399999999999);

    let tests = [
        ("00:00:00", CqlTime(0)),
        ("01:01:01", CqlTime((60 * 60 + 60 + 1) * 1_000_000_000)),
        ("00:00:00.000000000", CqlTime(0)),
        ("00:00:00.000000001", CqlTime(1)),
        ("23:59:59.999999999", CqlTime(max_time)),
    ];

    run_literal_and_bound_value_test(TABLE_NAME, "time", &tests, true).await;
}

#[tokio::test]
async fn test_serialize_deserialize_cql_time_invalid_values() {
    setup_tracing();
    const TABLE_NAME: &str = "time_serialization_test";
    let session = prepare_test_table(TABLE_NAME, "time", true).await;

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
            .query_unpaged(
                format!(
                    "INSERT INTO {} (id, val) VALUES (0, '{}')",
                    TABLE_NAME, time_str
                ),
                &[],
            )
            .await
            .unwrap_err();
    }
}

#[cfg(feature = "chrono-04")]
#[tokio::test]
async fn test_serialize_deserialize_naive_time_04() {
    setup_tracing();
    use chrono::NaiveTime;
    const TABLE_NAME: &str = "chrono_04_time_serialization_test";

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

    run_literal_and_bound_value_test(TABLE_NAME, "time", &tests, true).await;
}

#[cfg(feature = "chrono-04")]
#[tokio::test]
async fn test_serialize_deserialize_naive_time_04_leap_seconds() {
    setup_tracing();
    use chrono::NaiveTime;
    const TABLE_NAME: &str = "chrono_04_time_serialization_test";
    let session = prepare_test_table(TABLE_NAME, "time", true).await;

    // chrono can represent leap seconds, this should not panic
    let leap_second = NaiveTime::from_hms_nano_opt(23, 59, 59, 1_500_000_000);
    session
        .query_unpaged(
            format!("INSERT INTO {} (id, val) VALUES (0, ?)", TABLE_NAME),
            (leap_second,),
        )
        .await
        .unwrap_err();
}

#[cfg(feature = "time-03")]
#[tokio::test]
async fn test_serialize_deserialize_time_03() {
    setup_tracing();
    use time::Time;
    const TABLE_NAME: &str = "chrono_03_time_serialization_test";

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

    run_literal_and_bound_value_test(TABLE_NAME, "time", &tests, true).await;
}

#[tokio::test]
async fn test_serialize_deserialize_cql_timestamp() {
    setup_tracing();
    const TABLE_NAME: &str = "timestamp_serialization_test";

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

    run_literal_and_bound_value_test(TABLE_NAME, "timestamp", &tests, true).await;
}

#[tokio::test]
async fn test_serialize_deserialize_cqlvalue_duration() {
    setup_tracing();
    let session: Session = create_new_session_builder().build().await.unwrap();

    let ks = unique_keyspace_name();
    session
        .ddl(format!(
            "CREATE KEYSPACE IF NOT EXISTS {} WITH REPLICATION = \
                {{'class' : 'NetworkTopologyStrategy', 'replication_factor' : 1}}",
            ks
        ))
        .await
        .unwrap();
    session.use_keyspace(&ks, false).await.unwrap();

    let duration_cql_value = CqlValue::Duration(CqlDuration {
        months: 6,
        days: 9,
        nanoseconds: 21372137,
    });

    session.ddl("CREATE TABLE IF NOT EXISTS cqlvalue_duration_test (pk int, ck int, v duration, primary key (pk, ck))").await.unwrap();
    let fixture_queries = vec![
        (
            "INSERT INTO cqlvalue_duration_test (pk, ck, v) VALUES (0, 0, ?)",
            vec![&duration_cql_value],
        ),
        (
            "INSERT INTO cqlvalue_duration_test (pk, ck, v) VALUES (0, 1, 89h4m48s)",
            vec![],
        ),
        (
            "INSERT INTO cqlvalue_duration_test (pk, ck, v) VALUES (0, 2, PT89H8M53S)",
            vec![],
        ),
        (
            "INSERT INTO cqlvalue_duration_test (pk, ck, v) VALUES (0, 3, P0000-00-00T89:09:09)",
            vec![],
        ),
    ];

    for query in fixture_queries {
        session.query_unpaged(query.0, query.1).await.unwrap();
    }

    let rows_result = session
        .query_unpaged(
            "SELECT v FROM cqlvalue_duration_test WHERE pk = ?",
            (CqlValue::Int(0),),
        )
        .await
        .unwrap()
        .into_rows_result()
        .unwrap();

    let mut rows_iter = rows_result.rows::<(CqlValue,)>().unwrap();

    let (first_value,) = rows_iter.next().unwrap().unwrap();
    assert_eq!(first_value, duration_cql_value);

    let (second_value,) = rows_iter.next().unwrap().unwrap();
    assert_eq!(
        second_value,
        CqlValue::Duration(CqlDuration {
            months: 0,
            days: 0,
            nanoseconds: 320_688_000_000_000,
        })
    );

    let (third_value,) = rows_iter.next().unwrap().unwrap();
    assert_eq!(
        third_value,
        CqlValue::Duration(CqlDuration {
            months: 0,
            days: 0,
            nanoseconds: 320_933_000_000_000,
        })
    );

    let (fourth_value,) = rows_iter.next().unwrap().unwrap();
    assert_eq!(
        fourth_value,
        CqlValue::Duration(CqlDuration {
            months: 0,
            days: 0,
            nanoseconds: 320_949_000_000_000,
        })
    );

    assert_matches!(rows_iter.next(), None);
}

#[cfg(feature = "chrono-04")]
#[tokio::test]
async fn test_serialize_deserialize_date_time_04() {
    setup_tracing();
    use chrono::{DateTime, NaiveDate, NaiveDateTime, NaiveTime};
    const TABLE_NAME: &str = "timestamp_serialization_test";

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

    run_literal_and_bound_value_test(TABLE_NAME, "timestamp", &tests, true).await;
}

#[cfg(feature = "chrono-04")]
#[tokio::test]
async fn test_serialize_deserialize_date_time_04_high_precision_round_down() {
    setup_tracing();
    use chrono::{DateTime, NaiveDate, NaiveDateTime, NaiveTime, Utc};
    const TABLE_NAME: &str = "timestamp_serialization_test";
    let session = prepare_test_table(TABLE_NAME, "timestamp", true).await;

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
        .query_unpaged(
            format!("INSERT INTO {} (id, val) VALUES (0, ?)", TABLE_NAME),
            (nanosecond_precision_1st_half,),
        )
        .await
        .unwrap();

    let (read_datetime,) = session
        .query_unpaged(format!("SELECT val from {}", TABLE_NAME), &[])
        .await
        .unwrap()
        .into_rows_result()
        .unwrap()
        .first_row::<(DateTime<Utc>,)>()
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
        .query_unpaged(
            format!("INSERT INTO {} (id, val) VALUES (0, ?)", TABLE_NAME),
            (nanosecond_precision_2nd_half,),
        )
        .await
        .unwrap();

    let (read_datetime,) = session
        .query_unpaged(format!("SELECT val from {}", TABLE_NAME), &[])
        .await
        .unwrap()
        .into_rows_result()
        .unwrap()
        .first_row::<(DateTime<Utc>,)>()
        .unwrap();
    assert_eq!(read_datetime, nanosecond_precision_2nd_half_rounded);

    // chrono can represent leap seconds, this should not panic
    let leap_second = NaiveDateTime::new(
        NaiveDate::from_ymd_opt(2015, 6, 30).unwrap(),
        NaiveTime::from_hms_milli_opt(23, 59, 59, 1500).unwrap(),
    )
    .and_utc();
    session
        .query_unpaged(
            format!("INSERT INTO {} (id, val) VALUES (0, ?)", TABLE_NAME),
            (leap_second,),
        )
        .await
        .unwrap();
}

#[cfg(feature = "time-03")]
#[tokio::test]
async fn test_serialize_deserialize_offset_date_time_03() {
    setup_tracing();
    use time::{Date, Month::*, OffsetDateTime, PrimitiveDateTime, Time, UtcOffset};
    const TABLE_NAME: &str = "timestamp_serialization_test";

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

    run_literal_and_bound_value_test(TABLE_NAME, "timestamp", &tests, true).await;
}

#[cfg(feature = "time-03")]
#[tokio::test]
async fn test_serialize_deserialize_offset_date_time_03_high_precision_rounding_down() {
    setup_tracing();
    use time::{Date, Month::*, OffsetDateTime, PrimitiveDateTime, Time};
    const TABLE_NAME: &str = "timestamp_serialization_test";
    let session = prepare_test_table(TABLE_NAME, "timestamp", true).await;

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
        .query_unpaged(
            format!("INSERT INTO {} (id, val) VALUES (0, ?)", TABLE_NAME),
            (nanosecond_precision_1st_half,),
        )
        .await
        .unwrap();

    let (read_datetime,) = session
        .query_unpaged(format!("SELECT val from {}", TABLE_NAME), &[])
        .await
        .unwrap()
        .into_rows_result()
        .unwrap()
        .first_row::<(OffsetDateTime,)>()
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
        .query_unpaged(
            format!("INSERT INTO {} (id, val) VALUES (0, ?)", TABLE_NAME),
            (nanosecond_precision_2nd_half,),
        )
        .await
        .unwrap();

    let (read_datetime,) = session
        .query_unpaged(format!("SELECT val from {}", TABLE_NAME), &[])
        .await
        .unwrap()
        .into_rows_result()
        .unwrap()
        .first_row::<(OffsetDateTime,)>()
        .unwrap();
    assert_eq!(read_datetime, nanosecond_precision_2nd_half_rounded);
}

//UUID Types

#[tokio::test]
async fn test_serialize_deserialize_uuid() {
    setup_tracing();
    const TABLE_NAME: &str = "uuid_serialization_test";

    let tests: Vec<(String, uuid::Uuid)> = (0..100)
        .map(|_| uuid::Uuid::new_v4())
        .map(|new_uuid| (new_uuid.to_string(), new_uuid))
        .collect();

    run_literal_and_bound_value_test(TABLE_NAME, "uuid", tests.as_slice(), false).await;
}

#[tokio::test]
async fn test_serialize_deserialize_timeuuid() {
    setup_tracing();
    const TABLE_NAME: &str = "timeuuid_serialization_test";

    // A few random timeuuids generated manually
    let tests = [
        (
            "8e14e760-7fa8-11eb-bc66-000000000001",
            CqlTimeuuid::from_bytes([
                0x8e, 0x14, 0xe7, 0x60, 0x7f, 0xa8, 0x11, 0xeb, 0xbc, 0x66, 0, 0, 0, 0, 0, 0x01,
            ]),
        ),
        (
            "9b349580-7fa8-11eb-bc66-000000000001",
            CqlTimeuuid::from_bytes([
                0x9b, 0x34, 0x95, 0x80, 0x7f, 0xa8, 0x11, 0xeb, 0xbc, 0x66, 0, 0, 0, 0, 0, 0x01,
            ]),
        ),
        (
            "5d74bae0-7fa3-11eb-bc66-000000000001",
            CqlTimeuuid::from_bytes([
                0x5d, 0x74, 0xba, 0xe0, 0x7f, 0xa3, 0x11, 0xeb, 0xbc, 0x66, 0, 0, 0, 0, 0, 0x01,
            ]),
        ),
    ];

    run_literal_and_bound_value_test(TABLE_NAME, "timeuuid", &tests, false).await;
}

#[tokio::test]
async fn test_serialize_deserialize_timeuuid_ordering() {
    setup_tracing();
    let session: Session = create_new_session_builder().build().await.unwrap();
    let ks = unique_keyspace_name();

    session
        .ddl(format!(
            "CREATE KEYSPACE IF NOT EXISTS {} WITH REPLICATION = \
            {{'class' : 'NetworkTopologyStrategy', 'replication_factor' : 1}}",
            ks
        ))
        .await
        .unwrap();
    session.use_keyspace(ks, false).await.unwrap();

    session
        .ddl("CREATE TABLE tab (p int, t timeuuid, PRIMARY KEY (p, t))")
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
        session
            .execute_unpaged(&prepared, (timeuuid_val,))
            .await
            .unwrap();
    }

    let scylla_order_timeuuids: Vec<CqlTimeuuid> = session
        .query_unpaged("SELECT t FROM tab WHERE p = 0", ())
        .await
        .unwrap()
        .into_rows_result()
        .unwrap()
        .rows::<(CqlTimeuuid,)>()
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
async fn test_serialize_deserialize_strings_varchar() {
    let table_name = "varchar_serialization_test";

    let test_cases: Vec<(&str, String)> = vec![
        ("Hello, World!", String::from("Hello, World!")),
        ("Hello, Мир", String::from("Hello, Мир")), // multi-byte string
        ("🦀A🦀B🦀C", String::from("🦀A🦀B🦀C")),
        ("おはようございます", String::from("おはようございます")),
    ];

    run_literal_and_bound_value_test(table_name, "varchar", test_cases.as_slice(), true).await;
}

#[tokio::test]
async fn test_serialize_deserialize_strings_ascii() {
    let table_name = "ascii_serialization_test";

    let test_cases: Vec<(&str, String)> = vec![
        ("Hello, World!", String::from("Hello, World!")),
        (
            str::from_utf8(&[0x0, 0x7f]).unwrap(),
            String::from_utf8(vec![0x0, 0x7f]).unwrap(),
        ), // min/max ASCII values
    ];

    run_literal_and_bound_value_test(table_name, "varchar", test_cases.as_slice(), true).await;
}

// UDT Types

#[tokio::test]
async fn test_serialize_deserialize_udt_after_schema_update() {
    setup_tracing();
    let table_name = "udt_tests";
    let type_name = "usertype1";

    let session: Session = create_new_session_builder().build().await.unwrap();
    let ks = unique_keyspace_name();

    session
        .ddl(format!(
            "CREATE KEYSPACE IF NOT EXISTS {} WITH REPLICATION = \
            {{'class' : 'NetworkTopologyStrategy', 'replication_factor' : 1}}",
            ks
        ))
        .await
        .unwrap();
    session.use_keyspace(ks, false).await.unwrap();

    session
        .ddl(format!("DROP TABLE IF EXISTS {}", table_name))
        .await
        .unwrap();

    session
        .ddl(format!("DROP TYPE IF EXISTS {}", type_name))
        .await
        .unwrap();

    session
        .ddl(format!(
            "CREATE TYPE IF NOT EXISTS {} (first int, second boolean)",
            type_name
        ))
        .await
        .unwrap();

    session
        .ddl(format!(
            "CREATE TABLE IF NOT EXISTS {} (id int PRIMARY KEY, val {})",
            table_name, type_name
        ))
        .await
        .unwrap();

    #[derive(SerializeValue, DeserializeValue, Debug, PartialEq)]
    struct UdtV1 {
        first: i32,
        second: bool,
    }

    let v1 = UdtV1 {
        first: 123,
        second: true,
    };

    session
        .query_unpaged(
            format!(
                "INSERT INTO {}(id,val) VALUES (0, {})",
                table_name, "{first: 123, second: true}"
            ),
            &[],
        )
        .await
        .unwrap();

    let (read_udt,): (UdtV1,) = session
        .query_unpaged(format!("SELECT val from {} WHERE id = 0", table_name), &[])
        .await
        .unwrap()
        .into_rows_result()
        .unwrap()
        .single_row::<(UdtV1,)>()
        .unwrap();

    assert_eq!(read_udt, v1);

    session
        .query_unpaged(
            format!("INSERT INTO {}(id,val) VALUES (0, ?)", table_name),
            &(&v1,),
        )
        .await
        .unwrap();

    let (read_udt,): (UdtV1,) = session
        .query_unpaged(format!("SELECT val from {} WHERE id = 0", table_name), &[])
        .await
        .unwrap()
        .into_rows_result()
        .unwrap()
        .single_row::<(UdtV1,)>()
        .unwrap();

    assert_eq!(read_udt, v1);

    session
        .ddl(format!("ALTER TYPE {} ADD third text;", type_name))
        .await
        .unwrap();

    #[derive(DeserializeValue, Debug, PartialEq)]
    struct UdtV2 {
        first: i32,
        second: bool,
        third: Option<String>,
    }

    let (read_udt,): (UdtV2,) = session
        .query_unpaged(format!("SELECT val from {} WHERE id = 0", table_name), &[])
        .await
        .unwrap()
        .into_rows_result()
        .unwrap()
        .single_row::<(UdtV2,)>()
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
async fn test_serialize_deserialize_empty() {
    setup_tracing();
    let session: Session = prepare_test_table("empty_tests", "int", true).await;

    session
        .query_unpaged(
            "INSERT INTO empty_tests (id, val) VALUES (0, blobasint(0x))",
            (),
        )
        .await
        .unwrap();

    let (empty,) = session
        .query_unpaged("SELECT val FROM empty_tests WHERE id = 0", ())
        .await
        .unwrap()
        .into_rows_result()
        .unwrap()
        .first_row::<(CqlValue,)>()
        .unwrap();

    assert_eq!(empty, CqlValue::Empty);

    session
        .query_unpaged(
            "INSERT INTO empty_tests (id, val) VALUES (1, ?)",
            (CqlValue::Empty,),
        )
        .await
        .unwrap();

    let (empty,) = session
        .query_unpaged("SELECT val FROM empty_tests WHERE id = 1", ())
        .await
        .unwrap()
        .into_rows_result()
        .unwrap()
        .first_row::<(CqlValue,)>()
        .unwrap();

    assert_eq!(empty, CqlValue::Empty);
}

#[tokio::test]
async fn test_udt_with_missing_field() {
    setup_tracing();
    let table_name = "udt_tests";
    let type_name = "usertype1";

    let session: Session = create_new_session_builder().build().await.unwrap();
    let ks = unique_keyspace_name();

    session
        .ddl(format!(
            "CREATE KEYSPACE IF NOT EXISTS {} WITH REPLICATION = \
            {{'class' : 'NetworkTopologyStrategy', 'replication_factor' : 1}}",
            ks
        ))
        .await
        .unwrap();
    session.use_keyspace(ks, false).await.unwrap();

    session
        .ddl(format!("DROP TABLE IF EXISTS {}", table_name))
        .await
        .unwrap();

    session
        .ddl(format!("DROP TYPE IF EXISTS {}", type_name))
        .await
        .unwrap();

    session
        .ddl(format!(
            "CREATE TYPE IF NOT EXISTS {} (first int, second boolean, third float, fourth blob)",
            type_name
        ))
        .await
        .unwrap();

    session
        .ddl(format!(
            "CREATE TABLE IF NOT EXISTS {} (id int PRIMARY KEY, val {})",
            table_name, type_name
        ))
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
        TQ: SerializeValue,
        TR: DeserializeOwnedValue + PartialEq + Debug,
    {
        session
            .query_unpaged(
                format!("INSERT INTO {}(id,val) VALUES (?,?)", table_name),
                &(id, &element),
            )
            .await
            .unwrap();
        let result = session
            .query_unpaged(
                format!("SELECT val from {} WHERE id = ?", table_name),
                &(id,),
            )
            .await
            .unwrap()
            .into_rows_result()
            .unwrap()
            .single_row::<(TR,)>()
            .unwrap()
            .0;
        assert_eq!(expected, result);
    }

    #[derive(DeserializeValue, Debug, PartialEq)]
    struct UdtFull {
        first: i32,
        second: bool,
        third: Option<f32>,
        fourth: Option<Vec<u8>>,
    }

    #[derive(SerializeValue)]
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

    #[derive(SerializeValue)]
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

    #[derive(SerializeValue)]
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

    #[derive(SerializeValue)]
    #[scylla(flavor = "enforce_order")]
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

#[tokio::test]
async fn test_serialize_deserialize_cqlvalue_udt() {
    setup_tracing();
    let session: Session = create_new_session_builder().build().await.unwrap();
    let ks = unique_keyspace_name();
    session
        .ddl(format!(
            "CREATE KEYSPACE IF NOT EXISTS {} WITH REPLICATION = \
            {{'class' : 'NetworkTopologyStrategy', 'replication_factor' : 1}}",
            ks
        ))
        .await
        .unwrap();
    session.use_keyspace(&ks, false).await.unwrap();

    session
        .ddl("CREATE TYPE IF NOT EXISTS cqlvalue_udt_type (int_val int, text_val text)")
        .await
        .unwrap();
    session.ddl("CREATE TABLE IF NOT EXISTS cqlvalue_udt_test (k int, my cqlvalue_udt_type, primary key (k))").await.unwrap();

    let udt_cql_value = CqlValue::UserDefinedType {
        keyspace: ks,
        name: "cqlvalue_udt_type".to_string(),
        fields: vec![
            ("int_val".to_string(), Some(CqlValue::Int(42))),
            ("text_val".to_string(), Some(CqlValue::Text("hi".into()))),
        ],
    };

    session
        .query_unpaged(
            "INSERT INTO cqlvalue_udt_test (k, my) VALUES (5, ?)",
            (&udt_cql_value,),
        )
        .await
        .unwrap();

    let rows_result = session
        .query_unpaged("SELECT my FROM cqlvalue_udt_test", &[])
        .await
        .unwrap()
        .into_rows_result()
        .unwrap();

    let (received_udt_cql_value,) = rows_result.single_row::<(CqlValue,)>().unwrap();

    assert_eq!(received_udt_cql_value, udt_cql_value);
}
