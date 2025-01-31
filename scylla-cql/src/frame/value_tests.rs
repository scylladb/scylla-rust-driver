use crate::frame::response::result::{CollectionType, NativeType, UserDefinedType};
use crate::frame::value::{CqlTimeuuid, CqlVarint};
use crate::frame::{response::result::CqlValue, types::RawValue};
use crate::serialize::batch::{BatchValues, BatchValuesIterator};
use crate::serialize::row::{RowSerializationContext, SerializeRow, SerializedValues};
use crate::serialize::value::{mk_ser_err, SerializeValue};
use crate::serialize::value::{
    BuiltinSerializationError as BuiltinTypeSerializationError,
    BuiltinSerializationErrorKind as BuiltinTypeSerializationErrorKind,
};
use crate::serialize::writers::WrittenCellProof;
use crate::serialize::{CellWriter, RowWriter, SerializationError};

use super::response::result::{ColumnSpec, ColumnType, TableSpec};
use super::value::{CqlDate, CqlDuration, CqlTime, CqlTimestamp, MaybeUnset, Unset};
use crate::serialize::value::tests::do_serialize;
#[cfg(test)]
use assert_matches::assert_matches;
use bytes::BufMut;
use std::collections::hash_map::DefaultHasher;
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::hash::{BuildHasherDefault, Hash, Hasher};
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};
use std::str::FromStr;
use std::sync::Arc;
use std::{borrow::Cow, convert::TryInto};
use uuid::Uuid;

// For now it is only used in a test that is also behind this feature flag,
// so without it it throws a warning about being unused.
#[cfg(feature = "chrono-04")]
fn try_serialized<T: SerializeValue>(
    val: T,
    typ: ColumnType,
) -> Result<Vec<u8>, SerializationError> {
    let mut result: Vec<u8> = Vec::new();
    let writer = CellWriter::new(&mut result);
    SerializeValue::serialize(&val, &typ, writer)?;
    Ok(result)
}

fn compute_hash<T: Hash>(x: &T) -> u64 {
    let mut hasher = DefaultHasher::new();
    x.hash(&mut hasher);
    hasher.finish()
}

#[test]
fn boolean_serialization() {
    assert_eq!(
        do_serialize(true, &ColumnType::Native(NativeType::Boolean)),
        vec![0, 0, 0, 1, 1]
    );
    assert_eq!(
        do_serialize(false, &ColumnType::Native(NativeType::Boolean)),
        vec![0, 0, 0, 1, 0]
    );
}

#[test]
fn fixed_integral_serialization() {
    assert_eq!(
        do_serialize(8_i8, &ColumnType::Native(NativeType::TinyInt)),
        vec![0, 0, 0, 1, 8]
    );
    assert_eq!(
        do_serialize(16_i16, &ColumnType::Native(NativeType::SmallInt)),
        vec![0, 0, 0, 2, 0, 16]
    );
    assert_eq!(
        do_serialize(32_i32, &ColumnType::Native(NativeType::Int)),
        vec![0, 0, 0, 4, 0, 0, 0, 32]
    );
    assert_eq!(
        do_serialize(64_i64, &ColumnType::Native(NativeType::BigInt)),
        vec![0, 0, 0, 8, 0, 0, 0, 0, 0, 0, 0, 64]
    );
}

#[test]
fn counter_serialization() {
    assert_eq!(
        do_serialize(
            0x0123456789abcdef_i64,
            &ColumnType::Native(NativeType::BigInt)
        ),
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
        assert_eq!(
            do_serialize(x, &ColumnType::Native(NativeType::Varint)),
            b_with_len
        );
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
    B: From<i64> + SerializeValue,
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
        assert_eq!(
            do_serialize(x, &ColumnType::Native(NativeType::Varint)),
            b_with_len
        );
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
            assert_eq!(do_serialize(x, &ColumnType::Native(NativeType::Decimal)), repr);
        }
    }
}

#[test]
fn floating_point_serialization() {
    assert_eq!(
        do_serialize(123.456f32, &ColumnType::Native(NativeType::Float)),
        [0, 0, 0, 4]
            .into_iter()
            .chain((123.456f32).to_be_bytes())
            .collect::<Vec<_>>()
    );
    assert_eq!(
        do_serialize(123.456f64, &ColumnType::Native(NativeType::Double)),
        [0, 0, 0, 8]
            .into_iter()
            .chain((123.456f64).to_be_bytes())
            .collect::<Vec<_>>()
    );
}

#[test]
fn text_serialization() {
    assert_eq!(
        do_serialize("abc", &ColumnType::Native(NativeType::Text)),
        vec![0, 0, 0, 3, 97, 98, 99]
    );
    assert_eq!(
        do_serialize("abc".to_string(), &ColumnType::Native(NativeType::Ascii)),
        vec![0, 0, 0, 3, 97, 98, 99]
    );
}

#[test]
fn u8_array_serialization() {
    let val = [1u8; 4];
    assert_eq!(
        do_serialize(val, &ColumnType::Native(NativeType::Blob)),
        vec![0, 0, 0, 4, 1, 1, 1, 1]
    );
}

#[test]
fn u8_slice_serialization() {
    let val = vec![1u8, 1, 1, 1];
    assert_eq!(
        do_serialize(val.as_slice(), &ColumnType::Native(NativeType::Blob)),
        vec![0, 0, 0, 4, 1, 1, 1, 1]
    );
}

#[test]
fn cql_date_serialization() {
    assert_eq!(
        do_serialize(CqlDate(0), &ColumnType::Native(NativeType::Date)),
        vec![0, 0, 0, 4, 0, 0, 0, 0]
    );
    assert_eq!(
        do_serialize(CqlDate(u32::MAX), &ColumnType::Native(NativeType::Date)),
        vec![0, 0, 0, 4, 255, 255, 255, 255]
    );
}

#[test]
fn vec_u8_slice_serialization() {
    let val = vec![1u8, 1, 1, 1];
    assert_eq!(
        do_serialize(val, &ColumnType::Native(NativeType::Blob)),
        vec![0, 0, 0, 4, 1, 1, 1, 1]
    );
}

#[test]
fn ipaddr_serialization() {
    let ipv4 = IpAddr::V4(Ipv4Addr::new(1, 2, 3, 4));
    assert_eq!(
        do_serialize(ipv4, &ColumnType::Native(NativeType::Inet)),
        vec![0, 0, 0, 4, 1, 2, 3, 4]
    );

    let ipv6 = IpAddr::V6(Ipv6Addr::new(1, 2, 3, 4, 5, 6, 7, 8));
    assert_eq!(
        do_serialize(ipv6, &ColumnType::Native(NativeType::Inet)),
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
        do_serialize(unix_epoch, &ColumnType::Native(NativeType::Date)),
        vec![0, 0, 0, 4, 128, 0, 0, 0]
    );
    assert_eq!(2_u32.pow(31).to_be_bytes(), [128, 0, 0, 0]);

    // 1969-12-02 is 2^31 - 30
    let before_epoch: NaiveDate = NaiveDate::from_ymd_opt(1969, 12, 2).unwrap();
    assert_eq!(
        do_serialize(before_epoch, &ColumnType::Native(NativeType::Date)),
        vec![0, 0, 0, 4, 127, 255, 255, 226]
    );
    assert_eq!((2_u32.pow(31) - 30).to_be_bytes(), [127, 255, 255, 226]);

    // 1970-01-31 is 2^31 + 30
    let after_epoch: NaiveDate = NaiveDate::from_ymd_opt(1970, 1, 31).unwrap();
    assert_eq!(
        do_serialize(after_epoch, &ColumnType::Native(NativeType::Date)),
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
        do_serialize(unix_epoch, &ColumnType::Native(NativeType::Date)),
        vec![0, 0, 0, 4, 128, 0, 0, 0]
    );
    assert_eq!(2_u32.pow(31).to_be_bytes(), [128, 0, 0, 0]);

    // 1969-12-02 is 2^31 - 30
    let before_epoch =
        time_03::Date::from_calendar_date(1969, time_03::Month::December, 2).unwrap();
    assert_eq!(
        do_serialize(before_epoch, &ColumnType::Native(NativeType::Date)),
        vec![0, 0, 0, 4, 127, 255, 255, 226]
    );
    assert_eq!((2_u32.pow(31) - 30).to_be_bytes(), [127, 255, 255, 226]);

    // 1970-01-31 is 2^31 + 30
    let after_epoch = time_03::Date::from_calendar_date(1970, time_03::Month::January, 31).unwrap();
    assert_eq!(
        do_serialize(after_epoch, &ColumnType::Native(NativeType::Date)),
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
        do_serialize(long_before_epoch, &ColumnType::Native(NativeType::Date)),
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
        do_serialize(long_after_epoch, &ColumnType::Native(NativeType::Date)),
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
        let bytes: Vec<u8> = do_serialize(test_time, &ColumnType::Native(NativeType::Time));

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
        let bytes = do_serialize(time, &ColumnType::Native(NativeType::Time));

        let mut expected_bytes: Vec<u8> = vec![0, 0, 0, 8];
        expected_bytes.extend_from_slice(&expected);

        assert_eq!(bytes, expected_bytes)
    }

    // Leap second must return error on serialize
    let leap_second = NaiveTime::from_hms_nano_opt(23, 59, 59, 1_500_000_000).unwrap();
    let err = try_serialized(leap_second, ColumnType::Native(NativeType::Time)).unwrap_err();
    assert_matches!(
        err.downcast_ref::<BuiltinTypeSerializationError>()
            .unwrap()
            .kind,
        BuiltinTypeSerializationErrorKind::ValueOverflow
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
        let bytes = do_serialize(time, &ColumnType::Native(NativeType::Time));

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
        let bytes: Vec<u8> = do_serialize(test_timestamp, &ColumnType::Native(NativeType::Timestamp));

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
        let bytes: Vec<u8> = do_serialize(test_datetime, &ColumnType::Native(NativeType::Timestamp));

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
        let bytes: Vec<u8> = do_serialize(datetime, &ColumnType::Native(NativeType::Timestamp));

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
        let uuid_serialized: Vec<u8> = do_serialize(uuid, &ColumnType::Native(NativeType::Uuid));

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
        do_serialize(duration, &ColumnType::Native(NativeType::Duration)),
        vec![0, 0, 0, 3, 2, 4, 6]
    );
}

#[test]
fn box_serialization() {
    let x: Box<i32> = Box::new(123);
    assert_eq!(
        do_serialize(x, &ColumnType::Native(NativeType::Int)),
        vec![0, 0, 0, 4, 0, 0, 0, 123]
    );
}

#[test]
fn vec_set_serialization() {
    let m = vec!["ala", "ma", "kota"];
    assert_eq!(
        do_serialize(
            m,
            &ColumnType::Collection {
                frozen: false,
                typ: CollectionType::Set(Box::new(ColumnType::Native(NativeType::Text)))
            }
        ),
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
        do_serialize(
            m.as_ref(),
            &ColumnType::Collection {
                frozen: false,
                typ: CollectionType::Set(Box::new(ColumnType::Native(NativeType::Text)))
            }
        ),
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
        do_serialize(
            m,
            &ColumnType::Collection {
                frozen: false,
                typ: CollectionType::Set(Box::new(ColumnType::Native(NativeType::Text)))
            }
        ),
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
        do_serialize(
            m,
            &ColumnType::Collection {
                frozen: false,
                typ: CollectionType::Map(
                    Box::new(ColumnType::Native(NativeType::Text)),
                    Box::new(ColumnType::Native(NativeType::Int))
                )
            }
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
        do_serialize(
            m,
            &ColumnType::Collection {
                frozen: false,
                typ: CollectionType::Set(Box::new(ColumnType::Native(NativeType::Text)))
            }
        ),
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
        do_serialize(
            m,
            &ColumnType::Collection {
                frozen: false,
                typ: CollectionType::Map(
                    Box::new(ColumnType::Native(NativeType::Text)),
                    Box::new(ColumnType::Native(NativeType::Int))
                )
            }
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
        do_serialize(CqlValue::Empty, &ColumnType::Native(NativeType::Int)),
        vec![0, 0, 0, 0],
    );

    // UDTs
    let udt = CqlValue::UserDefinedType {
        keyspace: "ks".to_string(),
        name: "t".to_string(),
        fields: vec![
            ("foo".to_string(), Some(CqlValue::Int(123))),
            ("bar".to_string(), None),
        ],
    };
    let typ = ColumnType::UserDefinedType {
        frozen: false,
        definition: Arc::new(UserDefinedType {
            name: "t".into(),
            keyspace: "ks".into(),
            field_types: vec![
                ("foo".into(), ColumnType::Native(NativeType::Int)),
                ("bar".into(), ColumnType::Native(NativeType::Text)),
            ],
        }),
    };

    assert_eq!(
        do_serialize(udt, &typ),
        vec![
            0, 0, 0, 12, // size of the whole thing
            0, 0, 0, 4, 0, 0, 0, 123, // foo: 123_i32
            255, 255, 255, 255, // bar: null
        ]
    );

    // SerializeValue takes case of reordering the fields
    let udt = CqlValue::UserDefinedType {
        keyspace: "ks".to_string(),
        name: "t".to_string(),
        fields: vec![
            ("bar".to_string(), None),
            ("foo".to_string(), Some(CqlValue::Int(123))),
        ],
    };

    assert_eq!(
        do_serialize(udt, &typ),
        vec![
            0, 0, 0, 12, // size of the whole thing
            0, 0, 0, 4, 0, 0, 0, 123, // foo: 123_i32
            255, 255, 255, 255, // bar: null
        ]
    );

    // Tuples
    let tup = CqlValue::Tuple(vec![Some(CqlValue::Int(123)), None]);
    let typ = ColumnType::Tuple(vec![
        ColumnType::Native(NativeType::Int),
        ColumnType::Native(NativeType::Text),
    ]);
    assert_eq!(
        do_serialize(tup, &typ),
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
    let typ = ColumnType::Tuple(vec![
        ColumnType::Native(NativeType::Int),
        ColumnType::Native(NativeType::Text),
        ColumnType::Native(NativeType::Counter),
    ]);
    assert_eq!(
        do_serialize(tup, &typ),
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
        do_serialize(secret, &ColumnType::Native(NativeType::Int)),
        vec![0, 0, 0, 4, 0x00, 0x0f, 0x12, 0x06]
    );
}

#[test]
fn option_value() {
    assert_eq!(
        do_serialize(Some(32_i32), &ColumnType::Native(NativeType::Int)),
        vec![0, 0, 0, 4, 0, 0, 0, 32]
    );
    let null_i32: Option<i32> = None;
    assert_eq!(
        do_serialize(null_i32, &ColumnType::Native(NativeType::Int)),
        &(-1_i32).to_be_bytes()[..]
    );
}

#[test]
fn unset_value() {
    assert_eq!(
        do_serialize(Unset, &ColumnType::Native(NativeType::Int)),
        &(-2_i32).to_be_bytes()[..]
    );

    let unset_i32: MaybeUnset<i32> = MaybeUnset::Unset;
    assert_eq!(
        do_serialize(unset_i32, &ColumnType::Native(NativeType::Int)),
        &(-2_i32).to_be_bytes()[..]
    );

    let set_i32: MaybeUnset<i32> = MaybeUnset::Set(32);
    assert_eq!(
        do_serialize(set_i32, &ColumnType::Native(NativeType::Int)),
        vec![0, 0, 0, 4, 0, 0, 0, 32]
    );
}

#[test]
fn ref_value() {
    // This trickery is needed to prevent the compiler from performing deref coercions on refs
    // and effectively defeating the purpose of this test. With specialisations provided
    // in such an explicit way, the compiler is not allowed to coerce.
    fn check<T: SerializeValue>(x: &T, y: T, typ: &ColumnType) {
        assert_eq!(do_serialize::<&T>(x, typ), do_serialize::<T>(y, typ));
    }

    check(&1_i32, 1_i32, &ColumnType::Native(NativeType::Int));
}

#[test]
fn empty_serialized_values() {
    const EMPTY: SerializedValues = SerializedValues::new();
    assert_eq!(EMPTY.element_count(), 0);
    assert!(EMPTY.is_empty());
    assert_eq!(EMPTY.iter().next(), None);

    let mut empty_request = Vec::<u8>::new();
    EMPTY.write_to_request(&mut empty_request);
    assert_eq!(empty_request, vec![0, 0]);
}

#[test]
fn serialized_values() {
    let mut values = SerializedValues::new();
    assert!(values.is_empty());

    // Add first value
    values
        .add_value(&8_i8, &ColumnType::Native(NativeType::TinyInt))
        .unwrap();
    {
        assert_eq!(values.element_count(), 1);
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
    values
        .add_value(&16_i16, &ColumnType::Native(NativeType::SmallInt))
        .unwrap();
    {
        assert_eq!(values.element_count(), 2);
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
    impl SerializeValue for TooBigValue {
        fn serialize<'b>(
            &self,
            typ: &ColumnType,
            writer: CellWriter<'b>,
        ) -> Result<WrittenCellProof<'b>, SerializationError> {
            // serialize some
            writer.into_value_builder().append_bytes(&[1u8]);

            // then throw an error
            Err(mk_ser_err::<Self>(
                typ,
                BuiltinTypeSerializationErrorKind::SizeOverflow,
            ))
        }
    }

    let err = values
        .add_value(&TooBigValue, &ColumnType::Native(NativeType::Ascii))
        .unwrap_err();
    let err_inner = err.downcast_ref::<BuiltinTypeSerializationError>().unwrap();

    assert_matches!(
        err_inner.kind,
        BuiltinTypeSerializationErrorKind::SizeOverflow
    );

    // All checks for two values should still pass
    {
        assert_eq!(values.element_count(), 2);
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
    let serialized_unit: SerializedValues =
        SerializedValues::from_serializable(&RowSerializationContext::empty(), &()).unwrap();
    assert!(serialized_unit.is_empty());
}

#[test]
fn empty_array_value_list() {
    let serialized_arr: SerializedValues =
        SerializedValues::from_serializable(&RowSerializationContext::empty(), &[] as &[u8; 0])
            .unwrap();
    assert!(serialized_arr.is_empty());
}

#[test]
fn slice_value_list() {
    let values: &[i32] = &[1, 2, 3];
    let cols = &[
        col_spec("ala", ColumnType::Native(NativeType::Int)),
        col_spec("ma", ColumnType::Native(NativeType::Int)),
        col_spec("kota", ColumnType::Native(NativeType::Int)),
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
        col_spec("ala", ColumnType::Native(NativeType::Int)),
        col_spec("ma", ColumnType::Native(NativeType::Int)),
        col_spec("kota", ColumnType::Native(NativeType::Int)),
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

fn serialize_values<T: SerializeRow>(vl: T, columns: &[ColumnSpec]) -> SerializedValues {
    let ctx = RowSerializationContext { columns };
    let serialized: SerializedValues = SerializedValues::from_serializable(&ctx, &vl).unwrap();

    assert_eq!(<T as SerializeRow>::is_empty(&vl), serialized.is_empty());

    serialized
}

#[test]
fn tuple_value_list() {
    fn check_i8_tuple(tuple: impl SerializeRow, expected: core::ops::Range<u8>) {
        let typs = expected
            .clone()
            .enumerate()
            .map(|(i, _)| col_spec(format!("col_{i}"), ColumnType::Native(NativeType::TinyInt)))
            .collect::<Vec<_>>();
        let serialized = serialize_values(tuple, &typs);
        assert_eq!(serialized.element_count() as usize, expected.len());

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
    // SerializeRow will order the values by their names.
    // Note that the alphabetical order of the keys is "ala", "kota", "ma",
    // but the impl sorts properly.
    let row = BTreeMap::from_iter([("ala", 1), ("ma", 2), ("kota", 3)]);
    let cols = &[
        col_spec("ala", ColumnType::Native(NativeType::Int)),
        col_spec("ma", ColumnType::Native(NativeType::Int)),
        col_spec("kota", ColumnType::Native(NativeType::Int)),
    ];
    let values = serialize_values(row.clone(), cols);
    let mut values_bytes = Vec::new();
    values.write_to_request(&mut values_bytes);
    assert_eq!(
        values_bytes,
        vec![
            0, 3, // value count: 3
            0, 0, 0, 4, 0, 0, 0, 1, // ala: 1
            0, 0, 0, 4, 0, 0, 0, 2, // ma: 2
            0, 0, 0, 4, 0, 0, 0, 3, // kota: 3
        ]
    );
}

#[test]
fn ref_value_list() {
    let values: &[i32] = &[1, 2, 3];
    let typs = &[
        col_spec("col_1", ColumnType::Native(NativeType::Int)),
        col_spec("col_2", ColumnType::Native(NativeType::Int)),
        col_spec("col_3", ColumnType::Native(NativeType::Int)),
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

fn make_batch_value_iter<BV: BatchValues>(bv: &BV) -> BV::BatchValuesIter<'_> {
    <BV as BatchValues>::batch_values_iter(bv)
}

fn serialize_batch_value_iterator<'a>(
    bvi: &mut impl BatchValuesIterator<'a>,

    columns: &[ColumnSpec],
) -> Vec<u8> {
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
    serialize_bvi(bvi, &ctx)
}

#[test]
fn slice_batch_values() {
    let batch_values: &[&[i8]] = &[&[1, 2], &[2, 3, 4, 5], &[6]];

    let mut iter = make_batch_value_iter(&batch_values);
    {
        let cols = &[
            col_spec("a", ColumnType::Native(NativeType::TinyInt)),
            col_spec("b", ColumnType::Native(NativeType::TinyInt)),
        ];
        let request = serialize_batch_value_iterator(&mut iter, cols);
        assert_eq!(request, vec![0, 2, 0, 0, 0, 1, 1, 0, 0, 0, 1, 2]);
    }

    {
        let cols = &[
            col_spec("a", ColumnType::Native(NativeType::TinyInt)),
            col_spec("b", ColumnType::Native(NativeType::TinyInt)),
            col_spec("c", ColumnType::Native(NativeType::TinyInt)),
            col_spec("d", ColumnType::Native(NativeType::TinyInt)),
        ];
        let request = serialize_batch_value_iterator(&mut iter, cols);
        assert_eq!(
            request,
            vec![0, 4, 0, 0, 0, 1, 2, 0, 0, 0, 1, 3, 0, 0, 0, 1, 4, 0, 0, 0, 1, 5]
        );
    }

    {
        let cols = &[col_spec("a", ColumnType::Native(NativeType::TinyInt))];
        let request = serialize_batch_value_iterator(&mut iter, cols);
        assert_eq!(request, vec![0, 1, 0, 0, 0, 1, 6]);
    }

    assert_matches!(
        iter.serialize_next(
            &RowSerializationContext::empty(),
            &mut RowWriter::new(&mut Vec::new())
        ),
        None
    );

    let ctx = RowSerializationContext { columns: &[] };
    let mut data = Vec::new();
    let mut writer = RowWriter::new(&mut data);
    assert!(iter.serialize_next(&ctx, &mut writer).is_none());
}

#[test]
fn vec_batch_values() {
    let batch_values: Vec<Vec<i8>> = vec![vec![1, 2], vec![2, 3, 4, 5], vec![6]];

    let mut iter = make_batch_value_iter(&batch_values);
    {
        let cols = &[
            col_spec("a", ColumnType::Native(NativeType::TinyInt)),
            col_spec("b", ColumnType::Native(NativeType::TinyInt)),
        ];
        let request = serialize_batch_value_iterator(&mut iter, cols);
        assert_eq!(request, vec![0, 2, 0, 0, 0, 1, 1, 0, 0, 0, 1, 2]);
    }

    {
        let cols = &[
            col_spec("a", ColumnType::Native(NativeType::TinyInt)),
            col_spec("b", ColumnType::Native(NativeType::TinyInt)),
            col_spec("c", ColumnType::Native(NativeType::TinyInt)),
            col_spec("d", ColumnType::Native(NativeType::TinyInt)),
        ];
        let request = serialize_batch_value_iterator(&mut iter, cols);
        assert_eq!(
            request,
            vec![0, 4, 0, 0, 0, 1, 2, 0, 0, 0, 1, 3, 0, 0, 0, 1, 4, 0, 0, 0, 1, 5]
        );
    }

    {
        let cols = &[col_spec("a", ColumnType::Native(NativeType::TinyInt))];
        let request = serialize_batch_value_iterator(&mut iter, cols);
        assert_eq!(request, vec![0, 1, 0, 0, 0, 1, 6]);
    }
}

#[test]
fn tuple_batch_values() {
    fn check_twoi32_tuple(tuple: impl BatchValues, size: usize) {
        let mut iter = make_batch_value_iter(&tuple);

        for i in 0..size {
            let cols = &[
                col_spec("a", ColumnType::Native(NativeType::Int)),
                col_spec("b", ColumnType::Native(NativeType::Int)),
            ];

            let request = serialize_batch_value_iterator(&mut iter, cols);

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
        col_spec("a", ColumnType::Native(NativeType::TinyInt)),
        col_spec("b", ColumnType::Native(NativeType::TinyInt)),
    ];

    return check_ref_bv::<&&&&&[&[i8]]>(&&&&batch_values, cols);
    fn check_ref_bv<B: BatchValues>(batch_values: B, cols: &[ColumnSpec]) {
        let mut iter = make_batch_value_iter(&batch_values);

        let request = serialize_batch_value_iterator(&mut iter, cols);
        assert_eq!(request, vec![0, 2, 0, 0, 0, 1, 1, 0, 0, 0, 1, 2]);
    }
}

#[test]
#[allow(clippy::needless_borrow)]
fn check_ref_tuple() {
    fn assert_has_batch_values<BV: BatchValues>(bv: BV, cols: &[&[ColumnSpec]]) {
        let mut iter = make_batch_value_iter(&bv);

        for cols in cols {
            serialize_batch_value_iterator(&mut iter, cols);
        }
    }
    let s = String::from("hello");
    let tuple: ((&str,),) = ((&s,),);
    let cols: &[&[ColumnSpec]] = &[&[col_spec("a", ColumnType::Native(NativeType::Text))]];
    assert_has_batch_values::<&_>(&tuple, cols);
    let tuple2: ((&str, &str), (&str, &str)) = ((&s, &s), (&s, &s));
    let cols: &[&[ColumnSpec]] = &[
        &[
            col_spec("a", ColumnType::Native(NativeType::Text)),
            col_spec("b", ColumnType::Native(NativeType::Text)),
        ],
        &[
            col_spec("a", ColumnType::Native(NativeType::Text)),
            col_spec("b", ColumnType::Native(NativeType::Text)),
        ],
    ];
    assert_has_batch_values::<&_>(&tuple2, cols);
}

#[test]
fn check_batch_values_iterator_is_not_lending() {
    // This is an interesting property if we want to improve the batch shard selection heuristic
    fn g(bv: impl BatchValues) {
        let mut it = bv.batch_values_iter();
        let mut it2 = bv.batch_values_iter();

        let columns = &[col_spec("a", ColumnType::Native(NativeType::Int))];
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
    g(((10,), (11,)));
}
