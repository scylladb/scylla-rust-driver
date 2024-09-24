use crate::frame::value::{CqlTimeuuid, CqlVarint};
use crate::frame::{response::result::CqlValue, types::RawValue, value::LegacyBatchValuesIterator};
use crate::types::serialize::batch::{BatchValues, BatchValuesIterator, LegacyBatchValuesAdapter};
use crate::types::serialize::row::{RowSerializationContext, SerializeRow};
use crate::types::serialize::value::SerializeValue;
use crate::types::serialize::{CellWriter, RowWriter};

use super::response::result::{ColumnSpec, ColumnType, TableSpec};
use super::value::{
    CqlDate, CqlDuration, CqlTime, CqlTimestamp, LegacyBatchValues, LegacySerializedValues,
    MaybeUnset, SerializeValuesError, Unset, Value, ValueList, ValueTooBig,
};
#[cfg(test)]
use assert_matches::assert_matches;
use bytes::BufMut;
use std::collections::hash_map::DefaultHasher;
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::hash::{BuildHasherDefault, Hash, Hasher};
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};
use std::str::FromStr;
use std::{borrow::Cow, convert::TryInto};
use uuid::Uuid;

fn serialized<T>(val: T, typ: ColumnType) -> Vec<u8>
where
    T: Value + SerializeValue,
{
    let mut result: Vec<u8> = Vec::new();
    Value::serialize(&val, &mut result).unwrap();

    let mut new_result: Vec<u8> = Vec::new();
    let writer = CellWriter::new(&mut new_result);
    SerializeValue::serialize(&val, &typ, writer).unwrap();

    assert_eq!(result, new_result);

    result
}

fn serialized_only_new<T: SerializeValue>(val: T, typ: ColumnType) -> Vec<u8> {
    let mut result: Vec<u8> = Vec::new();
    let writer = CellWriter::new(&mut result);
    SerializeValue::serialize(&val, &typ, writer).unwrap();
    result
}

fn compute_hash<T: Hash>(x: &T) -> u64 {
    let mut hasher = DefaultHasher::new();
    x.hash(&mut hasher);
    hasher.finish()
}

#[test]
fn boolean_serialization() {
    assert_eq!(serialized(true, ColumnType::Boolean), vec![0, 0, 0, 1, 1]);
    assert_eq!(serialized(false, ColumnType::Boolean), vec![0, 0, 0, 1, 0]);
}

#[test]
fn fixed_integral_serialization() {
    assert_eq!(serialized(8_i8, ColumnType::TinyInt), vec![0, 0, 0, 1, 8]);
    assert_eq!(
        serialized(16_i16, ColumnType::SmallInt),
        vec![0, 0, 0, 2, 0, 16]
    );
    assert_eq!(
        serialized(32_i32, ColumnType::Int),
        vec![0, 0, 0, 4, 0, 0, 0, 32]
    );
    assert_eq!(
        serialized(64_i64, ColumnType::BigInt),
        vec![0, 0, 0, 8, 0, 0, 0, 0, 0, 0, 0, 64]
    );
}

#[test]
fn counter_serialization() {
    assert_eq!(
        serialized(0x0123456789abcdef_i64, ColumnType::BigInt),
        vec![0, 0, 0, 8, 0x01, 0x23, 0x45, 0x67, 0x89, 0xab, 0xcd, 0xef]
    );
}

fn cql_varint_normalization_test_cases() -> [(Vec<u8>, Vec<u8>); 11] {
    [
        (vec![], vec![0x00]),                 // 0
        (vec![0x00], vec![0x00]),             // 0
        (vec![0x00, 0x00], vec![0x00]),       // 0
        (vec![0x01], vec![0x01]),             // 1
        (vec![0x00, 0x01], vec![0x01]),       // 1
        (vec![0x7f], vec![0x7f]),             // 127
        (vec![0x00, 0x7f], vec![0x7f]),       // 127
        (vec![0x80], vec![0x80]),             // -128
        (vec![0x00, 0x80], vec![0x00, 0x80]), // 128
        (vec![0xff], vec![0xff]),             // -1
        (vec![0x00, 0xff], vec![0x00, 0xff]), // 255
    ]
}

#[test]
fn cql_varint_normalization() {
    let test_cases = cql_varint_normalization_test_cases();

    for test in test_cases {
        let non_normalized = CqlVarint::from_signed_bytes_be(test.0);
        let normalized = CqlVarint::from_signed_bytes_be(test.1);

        assert_eq!(non_normalized, normalized);
        assert_eq!(compute_hash(&non_normalized), compute_hash(&normalized));
    }
}

#[cfg(feature = "num-bigint-03")]
#[test]
fn cql_varint_normalization_with_bigint03() {
    let test_cases = cql_varint_normalization_test_cases();

    for test in test_cases {
        let non_normalized: num_bigint_03::BigInt = CqlVarint::from_signed_bytes_be(test.0).into();
        let normalized: num_bigint_03::BigInt = CqlVarint::from_signed_bytes_be(test.1).into();

        assert_eq!(non_normalized, normalized);
    }
}

#[test]
fn cql_varint_serialization() {
    let cases_from_the_spec: &[Vec<u8>] = &[
        vec![0x00],
        vec![0x01],
        vec![0x7F],
        vec![0x00, 0x80],
        vec![0x00, 0x81],
        vec![0xFF],
        vec![0x80],
        vec![0xFF, 0x7F],
    ];

    for b in cases_from_the_spec {
        let x = CqlVarint::from_signed_bytes_be_slice(b);
        let b_with_len = (b.len() as i32)
            .to_be_bytes()
            .iter()
            .chain(b)
            .cloned()
            .collect::<Vec<_>>();
        assert_eq!(serialized(x, ColumnType::Varint), b_with_len);
    }
}

#[cfg(any(
    feature = "num-bigint-03",
    feature = "num-bigint-04",
    feature = "bigdecimal-04"
))]
fn varint_test_cases_from_spec() -> Vec<(i64, Vec<u8>)> {
    vec![
        (0, vec![0x00]),
        (1, vec![0x01]),
        (127, vec![0x7F]),
        (128, vec![0x00, 0x80]),
        (129, vec![0x00, 0x81]),
        (-1, vec![0xFF]),
        (-128, vec![0x80]),
        (-129, vec![0xFF, 0x7F]),
    ]
}

#[cfg(any(feature = "num-bigint-03", feature = "num-bigint-04"))]
fn generic_num_bigint_serialization<B>()
where
    B: From<i64> + Value + SerializeValue,
{
    let cases_from_the_spec: &[(i64, Vec<u8>)] = &varint_test_cases_from_spec();

    for (i, b) in cases_from_the_spec {
        let x = B::from(*i);
        let b_with_len = (b.len() as i32)
            .to_be_bytes()
            .iter()
            .chain(b)
            .cloned()
            .collect::<Vec<_>>();
        assert_eq!(serialized(x, ColumnType::Varint), b_with_len);
    }
}

#[cfg(feature = "num-bigint-03")]
#[test]
fn bigint03_serialization() {
    generic_num_bigint_serialization::<num_bigint_03::BigInt>()
}

#[cfg(feature = "num-bigint-04")]
#[test]
fn bigint04_serialization() {
    generic_num_bigint_serialization::<num_bigint_04::BigInt>()
}

#[cfg(feature = "bigdecimal-04")]
#[test]
fn bigdecimal04_serialization() {
    // Bigint cases
    let cases_from_the_spec: &[(i64, Vec<u8>)] = &varint_test_cases_from_spec();

    for exponent in -10_i32..10_i32 {
        for (digits, serialized_digits) in cases_from_the_spec {
            let repr = ((serialized_digits.len() + 4) as i32)
                .to_be_bytes()
                .iter()
                .chain(&exponent.to_be_bytes())
                .chain(serialized_digits)
                .cloned()
                .collect::<Vec<_>>();
            let digits = bigdecimal_04::num_bigint::BigInt::from(*digits);
            let x = bigdecimal_04::BigDecimal::new(digits, exponent as i64);
            assert_eq!(serialized(x, ColumnType::Decimal), repr);
        }
    }
}

#[test]
fn floating_point_serialization() {
    assert_eq!(
        serialized(123.456f32, ColumnType::Float),
        [0, 0, 0, 4]
            .into_iter()
            .chain((123.456f32).to_be_bytes())
            .collect::<Vec<_>>()
    );
    assert_eq!(
        serialized(123.456f64, ColumnType::Double),
        [0, 0, 0, 8]
            .into_iter()
            .chain((123.456f64).to_be_bytes())
            .collect::<Vec<_>>()
    );
}

#[test]
fn text_serialization() {
    assert_eq!(
        serialized("abc", ColumnType::Text),
        vec![0, 0, 0, 3, 97, 98, 99]
    );
    assert_eq!(
        serialized("abc".to_string(), ColumnType::Ascii),
        vec![0, 0, 0, 3, 97, 98, 99]
    );
}

#[test]
fn u8_array_serialization() {
    let val = [1u8; 4];
    assert_eq!(
        serialized(val, ColumnType::Blob),
        vec![0, 0, 0, 4, 1, 1, 1, 1]
    );
}

#[test]
fn u8_slice_serialization() {
    let val = vec![1u8, 1, 1, 1];
    assert_eq!(
        serialized(val.as_slice(), ColumnType::Blob),
        vec![0, 0, 0, 4, 1, 1, 1, 1]
    );
}

#[test]
fn cql_date_serialization() {
    assert_eq!(
        serialized(CqlDate(0), ColumnType::Date),
        vec![0, 0, 0, 4, 0, 0, 0, 0]
    );
    assert_eq!(
        serialized(CqlDate(u32::MAX), ColumnType::Date),
        vec![0, 0, 0, 4, 255, 255, 255, 255]
    );
}

#[test]
fn vec_u8_slice_serialization() {
    let val = vec![1u8, 1, 1, 1];
    assert_eq!(
        serialized(val, ColumnType::Blob),
        vec![0, 0, 0, 4, 1, 1, 1, 1]
    );
}

#[test]
fn ipaddr_serialization() {
    let ipv4 = IpAddr::V4(Ipv4Addr::new(1, 2, 3, 4));
    assert_eq!(
        serialized(ipv4, ColumnType::Inet),
        vec![0, 0, 0, 4, 1, 2, 3, 4]
    );

    let ipv6 = IpAddr::V6(Ipv6Addr::new(1, 2, 3, 4, 5, 6, 7, 8));
    assert_eq!(
        serialized(ipv6, ColumnType::Inet),
        vec![
            0, 0, 0, 16, // serialized size
            0, 1, 0, 2, 0, 3, 0, 4, 0, 5, 0, 6, 0, 7, 0, 8, // contents
        ]
    );
}

#[cfg(feature = "chrono-04")]
#[test]
fn naive_date_04_serialization() {
    use chrono_04::NaiveDate;
    // 1970-01-31 is 2^31
    let unix_epoch: NaiveDate = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
    assert_eq!(
        serialized(unix_epoch, ColumnType::Date),
        vec![0, 0, 0, 4, 128, 0, 0, 0]
    );
    assert_eq!(2_u32.pow(31).to_be_bytes(), [128, 0, 0, 0]);

    // 1969-12-02 is 2^31 - 30
    let before_epoch: NaiveDate = NaiveDate::from_ymd_opt(1969, 12, 2).unwrap();
    assert_eq!(
        serialized(before_epoch, ColumnType::Date),
        vec![0, 0, 0, 4, 127, 255, 255, 226]
    );
    assert_eq!((2_u32.pow(31) - 30).to_be_bytes(), [127, 255, 255, 226]);

    // 1970-01-31 is 2^31 + 30
    let after_epoch: NaiveDate = NaiveDate::from_ymd_opt(1970, 1, 31).unwrap();
    assert_eq!(
        serialized(after_epoch, ColumnType::Date),
        vec![0, 0, 0, 4, 128, 0, 0, 30]
    );
    assert_eq!((2_u32.pow(31) + 30).to_be_bytes(), [128, 0, 0, 30]);
}

#[cfg(feature = "time-03")]
#[test]
fn date_03_serialization() {
    // 1970-01-31 is 2^31
    let unix_epoch = time_03::Date::from_ordinal_date(1970, 1).unwrap();
    assert_eq!(
        serialized(unix_epoch, ColumnType::Date),
        vec![0, 0, 0, 4, 128, 0, 0, 0]
    );
    assert_eq!(2_u32.pow(31).to_be_bytes(), [128, 0, 0, 0]);

    // 1969-12-02 is 2^31 - 30
    let before_epoch =
        time_03::Date::from_calendar_date(1969, time_03::Month::December, 2).unwrap();
    assert_eq!(
        serialized(before_epoch, ColumnType::Date),
        vec![0, 0, 0, 4, 127, 255, 255, 226]
    );
    assert_eq!((2_u32.pow(31) - 30).to_be_bytes(), [127, 255, 255, 226]);

    // 1970-01-31 is 2^31 + 30
    let after_epoch = time_03::Date::from_calendar_date(1970, time_03::Month::January, 31).unwrap();
    assert_eq!(
        serialized(after_epoch, ColumnType::Date),
        vec![0, 0, 0, 4, 128, 0, 0, 30]
    );
    assert_eq!((2_u32.pow(31) + 30).to_be_bytes(), [128, 0, 0, 30]);

    // Min date represented by time_03::Date (without large-dates feature)
    let long_before_epoch =
        time_03::Date::from_calendar_date(-9999, time_03::Month::January, 1).unwrap();
    let days_till_epoch = (unix_epoch - long_before_epoch).whole_days();
    assert_eq!(
        (2_u32.pow(31) - days_till_epoch as u32).to_be_bytes(),
        [127, 189, 75, 125]
    );
    assert_eq!(
        serialized(long_before_epoch, ColumnType::Date),
        vec![0, 0, 0, 4, 127, 189, 75, 125]
    );

    // Max date represented by time_03::Date (without large-dates feature)
    let long_after_epoch =
        time_03::Date::from_calendar_date(9999, time_03::Month::December, 31).unwrap();
    let days_since_epoch = (long_after_epoch - unix_epoch).whole_days();
    assert_eq!(
        (2_u32.pow(31) + days_since_epoch as u32).to_be_bytes(),
        [128, 44, 192, 160]
    );
    assert_eq!(
        serialized(long_after_epoch, ColumnType::Date),
        vec![0, 0, 0, 4, 128, 44, 192, 160]
    );
}

#[test]
fn cql_time_serialization() {
    // CqlTime is an i64 - nanoseconds since midnight
    // in range 0..=86399999999999

    let max_time: i64 = 24 * 60 * 60 * 1_000_000_000 - 1;
    assert_eq!(max_time, 86399999999999);

    // Check that basic values are serialized correctly
    // Invalid values are also serialized correctly - database will respond with an error
    for test_val in [0, 1, 15, 18463, max_time, -1, -324234, max_time + 16].into_iter() {
        let test_time: CqlTime = CqlTime(test_val);
        let bytes: Vec<u8> = serialized(test_time, ColumnType::Time);

        let mut expected_bytes: Vec<u8> = vec![0, 0, 0, 8];
        expected_bytes.extend_from_slice(&test_val.to_be_bytes());

        assert_eq!(bytes, expected_bytes);
        assert_eq!(expected_bytes.len(), 12);
    }
}

#[cfg(feature = "chrono-04")]
#[test]
fn naive_time_04_serialization() {
    use chrono_04::NaiveTime;

    let midnight_time: i64 = 0;
    let max_time: i64 = 24 * 60 * 60 * 1_000_000_000 - 1;
    let any_time: i64 = (3600 + 2 * 60 + 3) * 1_000_000_000 + 4;
    let test_cases = [
        (NaiveTime::MIN, midnight_time.to_be_bytes()),
        (
            NaiveTime::from_hms_nano_opt(23, 59, 59, 999_999_999).unwrap(),
            max_time.to_be_bytes(),
        ),
        (
            NaiveTime::from_hms_nano_opt(1, 2, 3, 4).unwrap(),
            any_time.to_be_bytes(),
        ),
    ];
    for (time, expected) in test_cases {
        let bytes = serialized(time, ColumnType::Time);

        let mut expected_bytes: Vec<u8> = vec![0, 0, 0, 8];
        expected_bytes.extend_from_slice(&expected);

        assert_eq!(bytes, expected_bytes)
    }

    // Leap second must return error on serialize
    let leap_second = NaiveTime::from_hms_nano_opt(23, 59, 59, 1_500_000_000).unwrap();
    let mut buffer = Vec::new();
    assert_eq!(
        <_ as Value>::serialize(&leap_second, &mut buffer),
        Err(ValueTooBig)
    )
}

#[cfg(feature = "time-03")]
#[test]
fn time_03_serialization() {
    let midnight_time: i64 = 0;
    let max_time: i64 = 24 * 60 * 60 * 1_000_000_000 - 1;
    let any_time: i64 = (3600 + 2 * 60 + 3) * 1_000_000_000 + 4;
    let test_cases = [
        (time_03::Time::MIDNIGHT, midnight_time.to_be_bytes()),
        (
            time_03::Time::from_hms_nano(23, 59, 59, 999_999_999).unwrap(),
            max_time.to_be_bytes(),
        ),
        (
            time_03::Time::from_hms_nano(1, 2, 3, 4).unwrap(),
            any_time.to_be_bytes(),
        ),
    ];
    for (time, expected) in test_cases {
        let bytes = serialized(time, ColumnType::Time);

        let mut expected_bytes: Vec<u8> = vec![0, 0, 0, 8];
        expected_bytes.extend_from_slice(&expected);

        assert_eq!(bytes, expected_bytes)
    }
}

#[test]
fn cql_timestamp_serialization() {
    // CqlTimestamp is milliseconds since unix epoch represented as i64

    for test_val in &[0, -1, 1, -45345346, 453451, i64::MIN, i64::MAX] {
        let test_timestamp: CqlTimestamp = CqlTimestamp(*test_val);
        let bytes: Vec<u8> = serialized(test_timestamp, ColumnType::Timestamp);

        let mut expected_bytes: Vec<u8> = vec![0, 0, 0, 8];
        expected_bytes.extend_from_slice(&test_val.to_be_bytes());

        assert_eq!(bytes, expected_bytes);
        assert_eq!(expected_bytes.len(), 12);
    }
}

#[cfg(feature = "chrono-04")]
#[test]
fn date_time_04_serialization() {
    use chrono_04::{DateTime, Utc};
    let test_cases: [(DateTime<Utc>, [u8; 8]); 7] = [
        (
            // Max time serialized without error
            DateTime::<Utc>::MAX_UTC,
            DateTime::<Utc>::MAX_UTC.timestamp_millis().to_be_bytes(),
        ),
        (
            // Min time serialized without error
            DateTime::<Utc>::MIN_UTC,
            DateTime::<Utc>::MIN_UTC.timestamp_millis().to_be_bytes(),
        ),
        (
            // UNIX epoch baseline
            DateTime::from_timestamp(0, 0).unwrap(),
            0i64.to_be_bytes(),
        ),
        (
            // One second since UNIX epoch
            DateTime::from_timestamp(1, 0).unwrap(),
            1000i64.to_be_bytes(),
        ),
        (
            // 1 nanosecond since UNIX epoch, lost during serialization
            DateTime::from_timestamp(0, 1).unwrap(),
            0i64.to_be_bytes(),
        ),
        (
            // 1 millisecond since UNIX epoch
            DateTime::from_timestamp(0, 1_000_000).unwrap(),
            1i64.to_be_bytes(),
        ),
        (
            // 2 days before UNIX epoch
            DateTime::from_timestamp(-2 * 24 * 60 * 60, 0).unwrap(),
            (-2 * 24i64 * 60 * 60 * 1000).to_be_bytes(),
        ),
    ];
    for (test_datetime, expected) in test_cases {
        let bytes: Vec<u8> = serialized(test_datetime, ColumnType::Timestamp);

        let mut expected_bytes: Vec<u8> = vec![0, 0, 0, 8];
        expected_bytes.extend_from_slice(&expected);

        assert_eq!(bytes, expected_bytes);
        assert_eq!(expected_bytes.len(), 12);
    }
}

#[cfg(feature = "time-03")]
#[test]
fn offset_date_time_03_serialization() {
    use time_03::{Date, Month, OffsetDateTime, PrimitiveDateTime, Time};
    let offset_max =
        PrimitiveDateTime::MAX.assume_offset(time_03::UtcOffset::from_hms(-23, -59, -59).unwrap());
    let offset_min =
        PrimitiveDateTime::MIN.assume_offset(time_03::UtcOffset::from_hms(23, 59, 59).unwrap());
    let test_cases = [
        (
            // Max time serialized without error
            offset_max,
            (offset_max.unix_timestamp() * 1000 + offset_max.nanosecond() as i64 / 1_000_000)
                .to_be_bytes(),
        ),
        (
            // Min time serialized without error
            offset_min,
            (offset_min.unix_timestamp() * 1000 + offset_min.nanosecond() as i64 / 1_000_000)
                .to_be_bytes(),
        ),
        (
            // UNIX epoch baseline
            OffsetDateTime::from_unix_timestamp(0).unwrap(),
            0i64.to_be_bytes(),
        ),
        (
            // One second since UNIX epoch
            OffsetDateTime::from_unix_timestamp(1).unwrap(),
            1000i64.to_be_bytes(),
        ),
        (
            // 1 nanosecond since UNIX epoch, lost during serialization
            OffsetDateTime::from_unix_timestamp_nanos(1).unwrap(),
            0i64.to_be_bytes(),
        ),
        (
            // 1 millisecond since UNIX epoch
            OffsetDateTime::from_unix_timestamp_nanos(1_000_000).unwrap(),
            1i64.to_be_bytes(),
        ),
        (
            // 2 days before UNIX epoch
            PrimitiveDateTime::new(
                Date::from_calendar_date(1969, Month::December, 30).unwrap(),
                Time::MIDNIGHT,
            )
            .assume_utc(),
            (-2 * 24i64 * 60 * 60 * 1000).to_be_bytes(),
        ),
    ];
    for (datetime, expected) in test_cases {
        let bytes: Vec<u8> = serialized(datetime, ColumnType::Timestamp);

        let mut expected_bytes: Vec<u8> = vec![0, 0, 0, 8];
        expected_bytes.extend_from_slice(&expected);

        assert_eq!(bytes, expected_bytes);
        assert_eq!(expected_bytes.len(), 12);
    }
}

#[test]
fn timeuuid_serialization() {
    // A few random timeuuids generated manually
    let tests = [
        [
            0x8e, 0x14, 0xe7, 0x60, 0x7f, 0xa8, 0x11, 0xeb, 0xbc, 0x66, 0, 0, 0, 0, 0, 0x01,
        ],
        [
            0x9b, 0x34, 0x95, 0x80, 0x7f, 0xa8, 0x11, 0xeb, 0xbc, 0x66, 0, 0, 0, 0, 0, 0x01,
        ],
        [
            0x5d, 0x74, 0xba, 0xe0, 0x7f, 0xa3, 0x11, 0xeb, 0xbc, 0x66, 0, 0, 0, 0, 0, 0x01,
        ],
    ];

    for uuid_bytes in &tests {
        let uuid = Uuid::from_slice(uuid_bytes.as_ref()).unwrap();
        let uuid_serialized: Vec<u8> = serialized(uuid, ColumnType::Uuid);

        let mut expected_serialized: Vec<u8> = vec![0, 0, 0, 16];
        expected_serialized.extend_from_slice(uuid_bytes.as_ref());

        assert_eq!(uuid_serialized, expected_serialized);
    }
}

#[test]
fn timeuuid_ordering_properties() {
    let x = CqlTimeuuid::from_str("00000000-0000-1000-8080-808080808080").unwrap();
    let y = CqlTimeuuid::from_str("00000000-0000-2000-8080-808080808080").unwrap();

    let cmp_res = x.cmp(&y);
    assert_eq!(std::cmp::Ordering::Equal, cmp_res);

    assert_eq!(x, y);
    assert_eq!(compute_hash(&x), compute_hash(&y));
}

#[test]
fn cqlduration_serialization() {
    let duration = CqlDuration {
        months: 1,
        days: 2,
        nanoseconds: 3,
    };
    assert_eq!(
        serialized(duration, ColumnType::Duration),
        vec![0, 0, 0, 3, 2, 4, 6]
    );
}

#[test]
fn box_serialization() {
    let x: Box<i32> = Box::new(123);
    assert_eq!(
        serialized(x, ColumnType::Int),
        vec![0, 0, 0, 4, 0, 0, 0, 123]
    );
}

#[test]
fn vec_set_serialization() {
    let m = vec!["ala", "ma", "kota"];
    assert_eq!(
        serialized(m, ColumnType::Set(Box::new(ColumnType::Text))),
        vec![
            0, 0, 0, 25, // 25 bytes
            0, 0, 0, 3, // 3 items
            0, 0, 0, 3, 97, 108, 97, // ala
            0, 0, 0, 2, 109, 97, // ma
            0, 0, 0, 4, 107, 111, 116, 97, // kota
        ]
    )
}

#[test]
fn slice_set_serialization() {
    let m = ["ala", "ma", "kota"];
    assert_eq!(
        serialized(m.as_ref(), ColumnType::Set(Box::new(ColumnType::Text))),
        vec![
            0, 0, 0, 25, // 25 bytes
            0, 0, 0, 3, // 3 items
            0, 0, 0, 3, 97, 108, 97, // ala
            0, 0, 0, 2, 109, 97, // ma
            0, 0, 0, 4, 107, 111, 116, 97, // kota
        ]
    )
}

// A deterministic hasher just for the tests.
#[derive(Default)]
struct DumbHasher {
    state: u8,
}

impl Hasher for DumbHasher {
    fn finish(&self) -> u64 {
        self.state as u64
    }

    fn write(&mut self, bytes: &[u8]) {
        for b in bytes {
            self.state ^= b;
        }
    }
}

type DumbBuildHasher = BuildHasherDefault<DumbHasher>;

#[test]
fn hashset_serialization() {
    let m: HashSet<&'static str, DumbBuildHasher> = ["ala", "ma", "kota"].into_iter().collect();
    assert_eq!(
        serialized(m, ColumnType::Set(Box::new(ColumnType::Text))),
        vec![
            0, 0, 0, 25, // 25 bytes
            0, 0, 0, 3, // 3 items
            0, 0, 0, 2, 109, 97, // ma
            0, 0, 0, 4, 107, 111, 116, 97, // kota
            0, 0, 0, 3, 97, 108, 97, // ala
        ]
    )
}

#[test]
fn hashmap_serialization() {
    let m: HashMap<&'static str, i32, DumbBuildHasher> =
        [("ala", 1), ("ma", 2), ("kota", 3)].into_iter().collect();
    assert_eq!(
        serialized(
            m,
            ColumnType::Map(Box::new(ColumnType::Text), Box::new(ColumnType::Int))
        ),
        vec![
            0, 0, 0, 49, // 49 bytes
            0, 0, 0, 3, // 3 items
            0, 0, 0, 2, 109, 97, // ma
            0, 0, 0, 4, 0, 0, 0, 2, // 2
            0, 0, 0, 4, 107, 111, 116, 97, // kota
            0, 0, 0, 4, 0, 0, 0, 3, // 3
            0, 0, 0, 3, 97, 108, 97, // ala
            0, 0, 0, 4, 0, 0, 0, 1, // 1
        ]
    )
}

#[test]
fn btreeset_serialization() {
    let m: BTreeSet<&'static str> = ["ala", "ma", "kota"].into_iter().collect();
    assert_eq!(
        serialized(m, ColumnType::Set(Box::new(ColumnType::Text))),
        vec![
            0, 0, 0, 25, // 25 bytes
            0, 0, 0, 3, // 3 items
            0, 0, 0, 3, 97, 108, 97, // ala
            0, 0, 0, 4, 107, 111, 116, 97, // kota
            0, 0, 0, 2, 109, 97, // ma
        ]
    )
}

#[test]
fn btreemap_serialization() {
    let m: BTreeMap<&'static str, i32> = [("ala", 1), ("ma", 2), ("kota", 3)].into_iter().collect();
    assert_eq!(
        serialized(
            m,
            ColumnType::Map(Box::new(ColumnType::Text), Box::new(ColumnType::Int))
        ),
        vec![
            0, 0, 0, 49, // 49 bytes
            0, 0, 0, 3, // 3 items
            0, 0, 0, 3, 97, 108, 97, // ala
            0, 0, 0, 4, 0, 0, 0, 1, // 1
            0, 0, 0, 4, 107, 111, 116, 97, // kota
            0, 0, 0, 4, 0, 0, 0, 3, // 3
            0, 0, 0, 2, 109, 97, // ma
            0, 0, 0, 4, 0, 0, 0, 2, // 2
        ]
    )
}

#[test]
fn cqlvalue_serialization() {
    // We only check those variants here which have some custom logic,
    // e.g. UDTs or tuples.

    // Empty
    assert_eq!(
        serialized(CqlValue::Empty, ColumnType::Int),
        vec![0, 0, 0, 0],
    );

    // UDTs
    let udt = CqlValue::UserDefinedType {
        keyspace: "ks".to_string(),
        type_name: "t".to_string(),
        fields: vec![
            ("foo".to_string(), Some(CqlValue::Int(123))),
            ("bar".to_string(), None),
        ],
    };
    let typ = ColumnType::UserDefinedType {
        type_name: "t".into(),
        keyspace: "ks".into(),
        field_types: vec![
            ("foo".into(), ColumnType::Int),
            ("bar".into(), ColumnType::Text),
        ],
    };

    assert_eq!(
        serialized(udt, typ.clone()),
        vec![
            0, 0, 0, 12, // size of the whole thing
            0, 0, 0, 4, 0, 0, 0, 123, // foo: 123_i32
            255, 255, 255, 255, // bar: null
        ]
    );

    // Unlike the legacy Value trait, SerializeValue takes case of reordering
    // the fields
    let udt = CqlValue::UserDefinedType {
        keyspace: "ks".to_string(),
        type_name: "t".to_string(),
        fields: vec![
            ("bar".to_string(), None),
            ("foo".to_string(), Some(CqlValue::Int(123))),
        ],
    };

    assert_eq!(
        serialized_only_new(udt, typ.clone()),
        vec![
            0, 0, 0, 12, // size of the whole thing
            0, 0, 0, 4, 0, 0, 0, 123, // foo: 123_i32
            255, 255, 255, 255, // bar: null
        ]
    );

    // Tuples
    let tup = CqlValue::Tuple(vec![Some(CqlValue::Int(123)), None]);
    let typ = ColumnType::Tuple(vec![ColumnType::Int, ColumnType::Text]);
    assert_eq!(
        serialized(tup, typ),
        vec![
            0, 0, 0, 12, // size of the whole thing
            0, 0, 0, 4, 0, 0, 0, 123, // 123_i32
            255, 255, 255, 255, // null
        ]
    );

    // It's not required to specify all the values for the tuple,
    // only some prefix is sufficient. The rest will be treated by the DB
    // as nulls.
    // TODO: Need a database test for that
    let tup = CqlValue::Tuple(vec![Some(CqlValue::Int(123)), None]);
    let typ = ColumnType::Tuple(vec![ColumnType::Int, ColumnType::Text, ColumnType::Counter]);
    assert_eq!(
        serialized(tup, typ),
        vec![
            0, 0, 0, 12, // size of the whole thing
            0, 0, 0, 4, 0, 0, 0, 123, // 123_i32
            255, 255, 255, 255, // null
        ]
    );
}

#[cfg(feature = "secrecy-08")]
#[test]
fn secret_serialization() {
    let secret = secrecy_08::Secret::new(987654i32);
    assert_eq!(
        serialized(secret, ColumnType::Int),
        vec![0, 0, 0, 4, 0x00, 0x0f, 0x12, 0x06]
    );
}

#[test]
fn option_value() {
    assert_eq!(
        serialized(Some(32_i32), ColumnType::Int),
        vec![0, 0, 0, 4, 0, 0, 0, 32]
    );
    let null_i32: Option<i32> = None;
    assert_eq!(
        serialized(null_i32, ColumnType::Int),
        &(-1_i32).to_be_bytes()[..]
    );
}

#[test]
fn unset_value() {
    assert_eq!(
        serialized(Unset, ColumnType::Int),
        &(-2_i32).to_be_bytes()[..]
    );

    let unset_i32: MaybeUnset<i32> = MaybeUnset::Unset;
    assert_eq!(
        serialized(unset_i32, ColumnType::Int),
        &(-2_i32).to_be_bytes()[..]
    );

    let set_i32: MaybeUnset<i32> = MaybeUnset::Set(32);
    assert_eq!(
        serialized(set_i32, ColumnType::Int),
        vec![0, 0, 0, 4, 0, 0, 0, 32]
    );
}

#[test]
fn ref_value() {
    fn serialized_generic<T: Value>(val: T) -> Vec<u8> {
        let mut result: Vec<u8> = Vec::new();
        val.serialize(&mut result).unwrap();
        result
    }

    // This trickery is needed to prevent the compiler from performing deref coercions on refs
    // and effectively defeating the purpose of this test. With specialisations provided
    // in such an explicit way, the compiler is not allowed to coerce.
    fn check<T: Value>(x: &T, y: T) {
        assert_eq!(serialized_generic::<&T>(x), serialized_generic::<T>(y));
    }

    check(&1_i32, 1_i32);
}

#[test]
fn empty_serialized_values() {
    const EMPTY: LegacySerializedValues = LegacySerializedValues::new();
    assert_eq!(EMPTY.len(), 0);
    assert!(EMPTY.is_empty());
    assert_eq!(EMPTY.iter().next(), None);

    let mut empty_request = Vec::<u8>::new();
    EMPTY.write_to_request(&mut empty_request);
    assert_eq!(empty_request, vec![0, 0]);
}

#[test]
fn serialized_values() {
    let mut values = LegacySerializedValues::new();
    assert!(values.is_empty());

    // Add first value
    values.add_value(&8_i8).unwrap();
    {
        assert_eq!(values.len(), 1);
        assert!(!values.is_empty());

        let mut request = Vec::<u8>::new();
        values.write_to_request(&mut request);
        assert_eq!(request, vec![0, 1, 0, 0, 0, 1, 8]);

        assert_eq!(
            values.iter().collect::<Vec<_>>(),
            vec![RawValue::Value([8].as_ref())]
        );
    }

    // Add second value
    values.add_value(&16_i16).unwrap();
    {
        assert_eq!(values.len(), 2);
        assert!(!values.is_empty());

        let mut request = Vec::<u8>::new();
        values.write_to_request(&mut request);
        assert_eq!(request, vec![0, 2, 0, 0, 0, 1, 8, 0, 0, 0, 2, 0, 16]);

        assert_eq!(
            values.iter().collect::<Vec<_>>(),
            vec![
                RawValue::Value([8].as_ref()),
                RawValue::Value([0, 16].as_ref())
            ]
        );
    }

    // Add a value that's too big, recover gracefully
    struct TooBigValue;
    impl Value for TooBigValue {
        fn serialize(&self, buf: &mut Vec<u8>) -> Result<(), ValueTooBig> {
            // serialize some
            buf.put_i32(1);

            // then throw an error
            Err(ValueTooBig)
        }
    }

    assert_eq!(
        values.add_value(&TooBigValue),
        Err(SerializeValuesError::ValueTooBig(ValueTooBig))
    );

    // All checks for two values should still pass
    {
        assert_eq!(values.len(), 2);
        assert!(!values.is_empty());

        let mut request = Vec::<u8>::new();
        values.write_to_request(&mut request);
        assert_eq!(request, vec![0, 2, 0, 0, 0, 1, 8, 0, 0, 0, 2, 0, 16]);

        assert_eq!(
            values.iter().collect::<Vec<_>>(),
            vec![
                RawValue::Value([8].as_ref()),
                RawValue::Value([0, 16].as_ref())
            ]
        );
    }
}

#[test]
fn unit_value_list() {
    let serialized_unit: LegacySerializedValues =
        <() as ValueList>::serialized(&()).unwrap().into_owned();
    assert!(serialized_unit.is_empty());
}

#[test]
fn empty_array_value_list() {
    let serialized_arr: LegacySerializedValues = <[u8; 0] as ValueList>::serialized(&[])
        .unwrap()
        .into_owned();
    assert!(serialized_arr.is_empty());
}

#[test]
fn slice_value_list() {
    let values: &[i32] = &[1, 2, 3];
    let cols = &[
        col_spec("ala", ColumnType::Int),
        col_spec("ma", ColumnType::Int),
        col_spec("kota", ColumnType::Int),
    ];
    let serialized = serialize_values(values, cols);

    assert_eq!(
        serialized.iter().collect::<Vec<_>>(),
        vec![
            RawValue::Value([0, 0, 0, 1].as_ref()),
            RawValue::Value([0, 0, 0, 2].as_ref()),
            RawValue::Value([0, 0, 0, 3].as_ref())
        ]
    );
}

#[test]
fn vec_value_list() {
    let values: Vec<i32> = vec![1, 2, 3];
    let cols = &[
        col_spec("ala", ColumnType::Int),
        col_spec("ma", ColumnType::Int),
        col_spec("kota", ColumnType::Int),
    ];
    let serialized = serialize_values(values, cols);

    assert_eq!(
        serialized.iter().collect::<Vec<_>>(),
        vec![
            RawValue::Value([0, 0, 0, 1].as_ref()),
            RawValue::Value([0, 0, 0, 2].as_ref()),
            RawValue::Value([0, 0, 0, 3].as_ref())
        ]
    );
}

fn col_spec<'a>(name: impl Into<Cow<'a, str>>, typ: ColumnType<'a>) -> ColumnSpec<'a> {
    ColumnSpec {
        name: name.into(),
        typ,
        table_spec: TableSpec::borrowed("ks", "tbl"),
    }
}

fn serialize_values<T: ValueList + SerializeRow>(
    vl: T,
    columns: &[ColumnSpec],
) -> LegacySerializedValues {
    let serialized = <T as ValueList>::serialized(&vl).unwrap().into_owned();
    let mut old_serialized = Vec::new();
    serialized.write_to_request(&mut old_serialized);

    let ctx = RowSerializationContext { columns };
    let mut new_serialized = vec![0, 0];
    let mut writer = RowWriter::new(&mut new_serialized);
    <T as SerializeRow>::serialize(&vl, &ctx, &mut writer).unwrap();
    let value_count: u16 = writer.value_count().try_into().unwrap();
    let is_empty = writer.value_count() == 0;

    // Prepend with value count, like `ValueList` does
    new_serialized[0..2].copy_from_slice(&value_count.to_be_bytes());

    assert_eq!(old_serialized, new_serialized);
    assert_eq!(<T as SerializeRow>::is_empty(&vl), is_empty);
    assert_eq!(serialized.is_empty(), is_empty);

    serialized
}

fn serialize_values_only_new<T: SerializeRow>(vl: T, columns: &[ColumnSpec]) -> Vec<u8> {
    let ctx = RowSerializationContext { columns };
    let mut serialized = vec![0, 0];
    let mut writer = RowWriter::new(&mut serialized);
    <T as SerializeRow>::serialize(&vl, &ctx, &mut writer).unwrap();
    let value_count: u16 = writer.value_count().try_into().unwrap();
    let is_empty = writer.value_count() == 0;

    // Prepend with value count, like `ValueList` does
    serialized[0..2].copy_from_slice(&value_count.to_be_bytes());

    assert_eq!(<T as SerializeRow>::is_empty(&vl), is_empty);

    serialized
}

#[test]
fn tuple_value_list() {
    fn check_i8_tuple(tuple: impl ValueList + SerializeRow, expected: core::ops::Range<u8>) {
        let typs = expected
            .clone()
            .enumerate()
            .map(|(i, _)| col_spec(format!("col_{i}"), ColumnType::TinyInt))
            .collect::<Vec<_>>();
        let serialized = serialize_values(tuple, &typs);
        assert_eq!(serialized.len() as usize, expected.len());

        let serialized_vals: Vec<u8> = serialized
            .iter()
            .map(|o: RawValue| o.as_value().unwrap()[0])
            .collect();

        let expected: Vec<u8> = expected.collect();

        assert_eq!(serialized_vals, expected);
    }

    check_i8_tuple((), 1..1);
    check_i8_tuple((1_i8,), 1..2);
    check_i8_tuple((1_i8, 2_i8), 1..3);
    check_i8_tuple((1_i8, 2_i8, 3_i8), 1..4);
    check_i8_tuple((1_i8, 2_i8, 3_i8, 4_i8), 1..5);
    check_i8_tuple((1_i8, 2_i8, 3_i8, 4_i8, 5_i8), 1..6);
    check_i8_tuple((1_i8, 2_i8, 3_i8, 4_i8, 5_i8, 6_i8), 1..7);
    check_i8_tuple((1_i8, 2_i8, 3_i8, 4_i8, 5_i8, 6_i8, 7_i8), 1..8);
    check_i8_tuple((1_i8, 2_i8, 3_i8, 4_i8, 5_i8, 6_i8, 7_i8, 8_i8), 1..9);
    check_i8_tuple(
        (1_i8, 2_i8, 3_i8, 4_i8, 5_i8, 6_i8, 7_i8, 8_i8, 9_i8),
        1..10,
    );
    check_i8_tuple(
        (1_i8, 2_i8, 3_i8, 4_i8, 5_i8, 6_i8, 7_i8, 8_i8, 9_i8, 10_i8),
        1..11,
    );
    check_i8_tuple(
        (
            1_i8, 2_i8, 3_i8, 4_i8, 5_i8, 6_i8, 7_i8, 8_i8, 9_i8, 10_i8, 11_i8,
        ),
        1..12,
    );
    check_i8_tuple(
        (
            1_i8, 2_i8, 3_i8, 4_i8, 5_i8, 6_i8, 7_i8, 8_i8, 9_i8, 10_i8, 11_i8, 12_i8,
        ),
        1..13,
    );
    check_i8_tuple(
        (
            1_i8, 2_i8, 3_i8, 4_i8, 5_i8, 6_i8, 7_i8, 8_i8, 9_i8, 10_i8, 11_i8, 12_i8, 13_i8,
        ),
        1..14,
    );
    check_i8_tuple(
        (
            1_i8, 2_i8, 3_i8, 4_i8, 5_i8, 6_i8, 7_i8, 8_i8, 9_i8, 10_i8, 11_i8, 12_i8, 13_i8, 14_i8,
        ),
        1..15,
    );
    check_i8_tuple(
        (
            1_i8, 2_i8, 3_i8, 4_i8, 5_i8, 6_i8, 7_i8, 8_i8, 9_i8, 10_i8, 11_i8, 12_i8, 13_i8,
            14_i8, 15_i8,
        ),
        1..16,
    );
    check_i8_tuple(
        (
            1_i8, 2_i8, 3_i8, 4_i8, 5_i8, 6_i8, 7_i8, 8_i8, 9_i8, 10_i8, 11_i8, 12_i8, 13_i8,
            14_i8, 15_i8, 16_i8,
        ),
        1..17,
    );
}

#[test]
fn map_value_list() {
    // The legacy ValueList would serialize this as a list of named values,
    // whereas the new SerializeRow will order the values by their names.

    // Note that the alphabetical order of the keys is "ala", "kota", "ma",
    // but the impl sorts properly.
    let row = BTreeMap::from_iter([("ala", 1), ("ma", 2), ("kota", 3)]);
    let cols = &[
        col_spec("ala", ColumnType::Int),
        col_spec("ma", ColumnType::Int),
        col_spec("kota", ColumnType::Int),
    ];
    let new_values = serialize_values_only_new(row.clone(), cols);
    assert_eq!(
        new_values,
        vec![
            0, 3, // value count: 3
            0, 0, 0, 4, 0, 0, 0, 1, // ala: 1
            0, 0, 0, 4, 0, 0, 0, 2, // ma: 2
            0, 0, 0, 4, 0, 0, 0, 3, // kota: 3
        ]
    );

    // While ValueList will serialize differently, the fallback SerializeRow impl
    // should convert it to how serialized BTreeMap would look like if serialized
    // directly through SerializeRow.
    let ser = <_ as ValueList>::serialized(&row).unwrap();
    let fallbacked = serialize_values_only_new(ser, cols);

    assert_eq!(new_values, fallbacked);
}

#[test]
fn ref_value_list() {
    let values: &[i32] = &[1, 2, 3];
    let typs = &[
        col_spec("col_1", ColumnType::Int),
        col_spec("col_2", ColumnType::Int),
        col_spec("col_3", ColumnType::Int),
    ];
    let serialized = serialize_values::<&&[i32]>(&values, typs);

    assert_eq!(
        serialized.iter().collect::<Vec<_>>(),
        vec![
            RawValue::Value([0, 0, 0, 1].as_ref()),
            RawValue::Value([0, 0, 0, 2].as_ref()),
            RawValue::Value([0, 0, 0, 3].as_ref())
        ]
    );
}

#[test]
fn serialized_values_value_list() {
    let mut ser_values = LegacySerializedValues::new();
    ser_values.add_value(&1_i32).unwrap();
    ser_values.add_value(&"qwertyuiop").unwrap();

    let ser_ser_values: Cow<LegacySerializedValues> = ser_values.serialized().unwrap();
    assert_matches!(ser_ser_values, Cow::Borrowed(_));

    assert_eq!(&ser_values, ser_ser_values.as_ref());
}

#[test]
fn cow_serialized_values_value_list() {
    let cow_ser_values: Cow<LegacySerializedValues> = Cow::Owned(LegacySerializedValues::new());

    let serialized: Cow<LegacySerializedValues> = cow_ser_values.serialized().unwrap();
    assert_matches!(serialized, Cow::Borrowed(_));

    assert_eq!(cow_ser_values.as_ref(), serialized.as_ref());
}

fn make_batch_value_iters<'bv, BV: BatchValues + LegacyBatchValues>(
    bv: &'bv BV,
    adapter_bv: &'bv LegacyBatchValuesAdapter<&'bv BV>,
) -> (
    BV::LegacyBatchValuesIter<'bv>,
    BV::BatchValuesIter<'bv>,
    <LegacyBatchValuesAdapter<&'bv BV> as BatchValues>::BatchValuesIter<'bv>,
) {
    (
        <BV as LegacyBatchValues>::batch_values_iter(bv),
        <BV as BatchValues>::batch_values_iter(bv),
        <_ as BatchValues>::batch_values_iter(adapter_bv),
    )
}

fn serialize_batch_value_iterators<'a>(
    (legacy_bvi, bvi, bvi_adapted): &mut (
        impl LegacyBatchValuesIterator<'a>,
        impl BatchValuesIterator<'a>,
        impl BatchValuesIterator<'a>,
    ),
    columns: &[ColumnSpec],
) -> Vec<u8> {
    let mut legacy_data = Vec::new();
    legacy_bvi
        .write_next_to_request(&mut legacy_data)
        .unwrap()
        .unwrap();

    fn serialize_bvi<'bv>(
        bvi: &mut impl BatchValuesIterator<'bv>,
        ctx: &RowSerializationContext,
    ) -> Vec<u8> {
        let mut data = vec![0, 0];
        let mut writer = RowWriter::new(&mut data);
        bvi.serialize_next(ctx, &mut writer).unwrap().unwrap();
        let value_count: u16 = writer.value_count().try_into().unwrap();
        data[0..2].copy_from_slice(&value_count.to_be_bytes());
        data
    }

    let ctx = RowSerializationContext { columns };
    let data = serialize_bvi(bvi, &ctx);
    let adapted_data = serialize_bvi(bvi_adapted, &ctx);

    assert_eq!(legacy_data, data);
    assert_eq!(adapted_data, data);
    data
}

#[test]
fn slice_batch_values() {
    let batch_values: &[&[i8]] = &[&[1, 2], &[2, 3, 4, 5], &[6]];
    let legacy_batch_values = LegacyBatchValuesAdapter(&batch_values);

    let mut iters = make_batch_value_iters(&batch_values, &legacy_batch_values);
    {
        let cols = &[
            col_spec("a", ColumnType::TinyInt),
            col_spec("b", ColumnType::TinyInt),
        ];
        let request = serialize_batch_value_iterators(&mut iters, cols);
        assert_eq!(request, vec![0, 2, 0, 0, 0, 1, 1, 0, 0, 0, 1, 2]);
    }

    {
        let cols = &[
            col_spec("a", ColumnType::TinyInt),
            col_spec("b", ColumnType::TinyInt),
            col_spec("c", ColumnType::TinyInt),
            col_spec("d", ColumnType::TinyInt),
        ];
        let request = serialize_batch_value_iterators(&mut iters, cols);
        assert_eq!(
            request,
            vec![0, 4, 0, 0, 0, 1, 2, 0, 0, 0, 1, 3, 0, 0, 0, 1, 4, 0, 0, 0, 1, 5]
        );
    }

    {
        let cols = &[col_spec("a", ColumnType::TinyInt)];
        let request = serialize_batch_value_iterators(&mut iters, cols);
        assert_eq!(request, vec![0, 1, 0, 0, 0, 1, 6]);
    }

    assert_eq!(iters.0.write_next_to_request(&mut Vec::new()), None);

    let ctx = RowSerializationContext { columns: &[] };
    let mut data = Vec::new();
    let mut writer = RowWriter::new(&mut data);
    assert!(iters.1.serialize_next(&ctx, &mut writer).is_none());
}

#[test]
fn vec_batch_values() {
    let batch_values: Vec<Vec<i8>> = vec![vec![1, 2], vec![2, 3, 4, 5], vec![6]];
    let legacy_batch_values = LegacyBatchValuesAdapter(&batch_values);

    let mut iters = make_batch_value_iters(&batch_values, &legacy_batch_values);
    {
        let cols = &[
            col_spec("a", ColumnType::TinyInt),
            col_spec("b", ColumnType::TinyInt),
        ];
        let request = serialize_batch_value_iterators(&mut iters, cols);
        assert_eq!(request, vec![0, 2, 0, 0, 0, 1, 1, 0, 0, 0, 1, 2]);
    }

    {
        let cols = &[
            col_spec("a", ColumnType::TinyInt),
            col_spec("b", ColumnType::TinyInt),
            col_spec("c", ColumnType::TinyInt),
            col_spec("d", ColumnType::TinyInt),
        ];
        let request = serialize_batch_value_iterators(&mut iters, cols);
        assert_eq!(
            request,
            vec![0, 4, 0, 0, 0, 1, 2, 0, 0, 0, 1, 3, 0, 0, 0, 1, 4, 0, 0, 0, 1, 5]
        );
    }

    {
        let cols = &[col_spec("a", ColumnType::TinyInt)];
        let request = serialize_batch_value_iterators(&mut iters, cols);
        assert_eq!(request, vec![0, 1, 0, 0, 0, 1, 6]);
    }
}

#[test]
fn tuple_batch_values() {
    fn check_twoi32_tuple(tuple: impl BatchValues + LegacyBatchValues, size: usize) {
        let legacy_tuple = LegacyBatchValuesAdapter(&tuple);
        let mut iters = make_batch_value_iters(&tuple, &legacy_tuple);
        for i in 0..size {
            let cols = &[
                col_spec("a", ColumnType::Int),
                col_spec("b", ColumnType::Int),
            ];

            let request = serialize_batch_value_iterators(&mut iters, cols);

            let mut expected: Vec<u8> = Vec::new();
            let i: i32 = i.try_into().unwrap();
            expected.put_i16(2);
            expected.put_i32(4);
            expected.put_i32(i + 1);
            expected.put_i32(4);
            expected.put_i32(2 * (i + 1));

            assert_eq!(request, expected);
        }
    }

    // rustfmt wants to have each tuple inside a tuple in a separate line
    // so we end up with 170 lines of tuples
    // FIXME: Is there some cargo fmt flag to fix this?

    check_twoi32_tuple(((1, 2),), 1);
    check_twoi32_tuple(((1, 2), (2, 4)), 2);
    check_twoi32_tuple(((1, 2), (2, 4), (3, 6)), 3);
    check_twoi32_tuple(((1, 2), (2, 4), (3, 6), (4, 8)), 4);
    check_twoi32_tuple(((1, 2), (2, 4), (3, 6), (4, 8), (5, 10)), 5);
    check_twoi32_tuple(((1, 2), (2, 4), (3, 6), (4, 8), (5, 10), (6, 12)), 6);
    check_twoi32_tuple(
        ((1, 2), (2, 4), (3, 6), (4, 8), (5, 10), (6, 12), (7, 14)),
        7,
    );
    check_twoi32_tuple(
        (
            (1, 2),
            (2, 4),
            (3, 6),
            (4, 8),
            (5, 10),
            (6, 12),
            (7, 14),
            (8, 16),
        ),
        8,
    );
    check_twoi32_tuple(
        (
            (1, 2),
            (2, 4),
            (3, 6),
            (4, 8),
            (5, 10),
            (6, 12),
            (7, 14),
            (8, 16),
            (9, 18),
        ),
        9,
    );
    check_twoi32_tuple(
        (
            (1, 2),
            (2, 4),
            (3, 6),
            (4, 8),
            (5, 10),
            (6, 12),
            (7, 14),
            (8, 16),
            (9, 18),
            (10, 20),
        ),
        10,
    );
    check_twoi32_tuple(
        (
            (1, 2),
            (2, 4),
            (3, 6),
            (4, 8),
            (5, 10),
            (6, 12),
            (7, 14),
            (8, 16),
            (9, 18),
            (10, 20),
            (11, 22),
        ),
        11,
    );
    check_twoi32_tuple(
        (
            (1, 2),
            (2, 4),
            (3, 6),
            (4, 8),
            (5, 10),
            (6, 12),
            (7, 14),
            (8, 16),
            (9, 18),
            (10, 20),
            (11, 22),
            (12, 24),
        ),
        12,
    );
    check_twoi32_tuple(
        (
            (1, 2),
            (2, 4),
            (3, 6),
            (4, 8),
            (5, 10),
            (6, 12),
            (7, 14),
            (8, 16),
            (9, 18),
            (10, 20),
            (11, 22),
            (12, 24),
            (13, 26),
        ),
        13,
    );
    check_twoi32_tuple(
        (
            (1, 2),
            (2, 4),
            (3, 6),
            (4, 8),
            (5, 10),
            (6, 12),
            (7, 14),
            (8, 16),
            (9, 18),
            (10, 20),
            (11, 22),
            (12, 24),
            (13, 26),
            (14, 28),
        ),
        14,
    );
    check_twoi32_tuple(
        (
            (1, 2),
            (2, 4),
            (3, 6),
            (4, 8),
            (5, 10),
            (6, 12),
            (7, 14),
            (8, 16),
            (9, 18),
            (10, 20),
            (11, 22),
            (12, 24),
            (13, 26),
            (14, 28),
            (15, 30),
        ),
        15,
    );
    check_twoi32_tuple(
        (
            (1, 2),
            (2, 4),
            (3, 6),
            (4, 8),
            (5, 10),
            (6, 12),
            (7, 14),
            (8, 16),
            (9, 18),
            (10, 20),
            (11, 22),
            (12, 24),
            (13, 26),
            (14, 28),
            (15, 30),
            (16, 32),
        ),
        16,
    );
}

#[test]
#[allow(clippy::needless_borrow)]
fn ref_batch_values() {
    let batch_values: &[&[i8]] = &[&[1, 2], &[2, 3, 4, 5], &[6]];
    let cols = &[
        col_spec("a", ColumnType::TinyInt),
        col_spec("b", ColumnType::TinyInt),
    ];

    return check_ref_bv::<&&&&&[&[i8]]>(&&&&batch_values, cols);
    fn check_ref_bv<B: BatchValues + LegacyBatchValues>(batch_values: B, cols: &[ColumnSpec]) {
        let legacy_batch_values = LegacyBatchValuesAdapter(&batch_values);
        let mut iters = make_batch_value_iters(&batch_values, &legacy_batch_values);

        let request = serialize_batch_value_iterators(&mut iters, cols);
        assert_eq!(request, vec![0, 2, 0, 0, 0, 1, 1, 0, 0, 0, 1, 2]);
    }
}

#[test]
#[allow(clippy::needless_borrow)]
fn check_ref_tuple() {
    fn assert_has_batch_values<BV: BatchValues + LegacyBatchValues>(
        bv: BV,
        cols: &[&[ColumnSpec]],
    ) {
        let legacy_bv = LegacyBatchValuesAdapter(&bv);
        let mut iters = make_batch_value_iters(&bv, &legacy_bv);
        for cols in cols {
            serialize_batch_value_iterators(&mut iters, cols);
        }
    }
    let s = String::from("hello");
    let tuple: ((&str,),) = ((&s,),);
    let cols: &[&[ColumnSpec]] = &[&[col_spec("a", ColumnType::Text)]];
    assert_has_batch_values::<&_>(&tuple, cols);
    let tuple2: ((&str, &str), (&str, &str)) = ((&s, &s), (&s, &s));
    let cols: &[&[ColumnSpec]] = &[
        &[
            col_spec("a", ColumnType::Text),
            col_spec("b", ColumnType::Text),
        ],
        &[
            col_spec("a", ColumnType::Text),
            col_spec("b", ColumnType::Text),
        ],
    ];
    assert_has_batch_values::<&_>(&tuple2, cols);
}

#[test]
fn check_batch_values_iterator_is_not_lending() {
    // This is an interesting property if we want to improve the batch shard selection heuristic
    fn f(bv: impl LegacyBatchValues) {
        let mut it = bv.batch_values_iter();
        let mut it2 = bv.batch_values_iter();
        // Make sure we can hold all these at the same time
        let v = vec![
            it.next_serialized().unwrap().unwrap(),
            it2.next_serialized().unwrap().unwrap(),
            it.next_serialized().unwrap().unwrap(),
            it2.next_serialized().unwrap().unwrap(),
        ];
        let _ = v;
    }
    fn g(bv: impl BatchValues) {
        let mut it = bv.batch_values_iter();
        let mut it2 = bv.batch_values_iter();

        let columns = &[col_spec("a", ColumnType::Int)];
        let ctx = RowSerializationContext { columns };
        let mut data = Vec::new();
        let mut writer = RowWriter::new(&mut data);

        // Make sure we can hold all these at the same time
        let v = vec![
            it.serialize_next(&ctx, &mut writer).unwrap().unwrap(),
            it2.serialize_next(&ctx, &mut writer).unwrap().unwrap(),
            it.serialize_next(&ctx, &mut writer).unwrap().unwrap(),
            it2.serialize_next(&ctx, &mut writer).unwrap().unwrap(),
        ];
        let _ = v;
    }
    f(((10,), (11,)));
    g(((10,), (11,)));
}
