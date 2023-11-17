use crate::frame::value::BatchValuesIterator;

use super::value::{
    BatchValues, CqlDate, CqlTime, CqlTimestamp, MaybeUnset, SerializeValuesError,
    SerializedValues, Unset, Value, ValueList, ValueTooBig,
};
use bytes::BufMut;
use std::{borrow::Cow, convert::TryInto};
use uuid::Uuid;

fn serialized(val: impl Value) -> Vec<u8> {
    let mut result: Vec<u8> = Vec::new();
    val.serialize(&mut result).unwrap();
    result
}

#[test]
fn basic_serialization() {
    assert_eq!(serialized(8_i8), vec![0, 0, 0, 1, 8]);
    assert_eq!(serialized(16_i16), vec![0, 0, 0, 2, 0, 16]);
    assert_eq!(serialized(32_i32), vec![0, 0, 0, 4, 0, 0, 0, 32]);
    assert_eq!(
        serialized(64_i64),
        vec![0, 0, 0, 8, 0, 0, 0, 0, 0, 0, 0, 64]
    );

    assert_eq!(serialized("abc"), vec![0, 0, 0, 3, 97, 98, 99]);
    assert_eq!(serialized("abc".to_string()), vec![0, 0, 0, 3, 97, 98, 99]);
}

#[test]
fn u8_array_serialization() {
    let val = [1u8; 4];
    assert_eq!(serialized(val), vec![0, 0, 0, 4, 1, 1, 1, 1]);
}

#[test]
fn u8_slice_serialization() {
    let val = vec![1u8, 1, 1, 1];
    assert_eq!(serialized(val.as_slice()), vec![0, 0, 0, 4, 1, 1, 1, 1]);
}

#[test]
fn cql_date_serialization() {
    assert_eq!(serialized(CqlDate(0)), vec![0, 0, 0, 4, 0, 0, 0, 0]);
    assert_eq!(
        serialized(CqlDate(u32::MAX)),
        vec![0, 0, 0, 4, 255, 255, 255, 255]
    );
}

#[cfg(feature = "chrono")]
#[test]
fn naive_date_serialization() {
    use chrono::NaiveDate;
    // 1970-01-31 is 2^31
    let unix_epoch: NaiveDate = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
    assert_eq!(serialized(unix_epoch), vec![0, 0, 0, 4, 128, 0, 0, 0]);
    assert_eq!(2_u32.pow(31).to_be_bytes(), [128, 0, 0, 0]);

    // 1969-12-02 is 2^31 - 30
    let before_epoch: NaiveDate = NaiveDate::from_ymd_opt(1969, 12, 2).unwrap();
    assert_eq!(
        serialized(before_epoch),
        vec![0, 0, 0, 4, 127, 255, 255, 226]
    );
    assert_eq!((2_u32.pow(31) - 30).to_be_bytes(), [127, 255, 255, 226]);

    // 1970-01-31 is 2^31 + 30
    let after_epoch: NaiveDate = NaiveDate::from_ymd_opt(1970, 1, 31).unwrap();
    assert_eq!(serialized(after_epoch), vec![0, 0, 0, 4, 128, 0, 0, 30]);
    assert_eq!((2_u32.pow(31) + 30).to_be_bytes(), [128, 0, 0, 30]);
}

#[cfg(feature = "time")]
#[test]
fn date_serialization() {
    // 1970-01-31 is 2^31
    let unix_epoch = time::Date::from_ordinal_date(1970, 1).unwrap();
    assert_eq!(serialized(unix_epoch), vec![0, 0, 0, 4, 128, 0, 0, 0]);
    assert_eq!(2_u32.pow(31).to_be_bytes(), [128, 0, 0, 0]);

    // 1969-12-02 is 2^31 - 30
    let before_epoch = time::Date::from_calendar_date(1969, time::Month::December, 2).unwrap();
    assert_eq!(
        serialized(before_epoch),
        vec![0, 0, 0, 4, 127, 255, 255, 226]
    );
    assert_eq!((2_u32.pow(31) - 30).to_be_bytes(), [127, 255, 255, 226]);

    // 1970-01-31 is 2^31 + 30
    let after_epoch = time::Date::from_calendar_date(1970, time::Month::January, 31).unwrap();
    assert_eq!(serialized(after_epoch), vec![0, 0, 0, 4, 128, 0, 0, 30]);
    assert_eq!((2_u32.pow(31) + 30).to_be_bytes(), [128, 0, 0, 30]);

    // Min date represented by time::Date (without large-dates feature)
    let long_before_epoch = time::Date::from_calendar_date(-9999, time::Month::January, 1).unwrap();
    let days_till_epoch = (unix_epoch - long_before_epoch).whole_days();
    assert_eq!(
        (2_u32.pow(31) - days_till_epoch as u32).to_be_bytes(),
        [127, 189, 75, 125]
    );
    assert_eq!(
        serialized(long_before_epoch),
        vec![0, 0, 0, 4, 127, 189, 75, 125]
    );

    // Max date represented by time::Date (without large-dates feature)
    let long_after_epoch = time::Date::from_calendar_date(9999, time::Month::December, 31).unwrap();
    let days_since_epoch = (long_after_epoch - unix_epoch).whole_days();
    assert_eq!(
        (2_u32.pow(31) + days_since_epoch as u32).to_be_bytes(),
        [128, 44, 192, 160]
    );
    assert_eq!(
        serialized(long_after_epoch),
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
        let bytes: Vec<u8> = serialized(test_time);

        let mut expected_bytes: Vec<u8> = vec![0, 0, 0, 8];
        expected_bytes.extend_from_slice(&test_val.to_be_bytes());

        assert_eq!(bytes, expected_bytes);
        assert_eq!(expected_bytes.len(), 12);
    }
}

#[cfg(feature = "chrono")]
#[test]
fn naive_time_serialization() {
    use chrono::NaiveTime;

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
        let bytes = serialized(time);

        let mut expected_bytes: Vec<u8> = vec![0, 0, 0, 8];
        expected_bytes.extend_from_slice(&expected);

        assert_eq!(bytes, expected_bytes)
    }

    // Leap second must return error on serialize
    let leap_second = NaiveTime::from_hms_nano_opt(23, 59, 59, 1_500_000_000).unwrap();
    let mut buffer = Vec::new();
    assert_eq!(leap_second.serialize(&mut buffer), Err(ValueTooBig))
}

#[cfg(feature = "time")]
#[test]
fn time_serialization() {
    let midnight_time: i64 = 0;
    let max_time: i64 = 24 * 60 * 60 * 1_000_000_000 - 1;
    let any_time: i64 = (3600 + 2 * 60 + 3) * 1_000_000_000 + 4;
    let test_cases = [
        (time::Time::MIDNIGHT, midnight_time.to_be_bytes()),
        (
            time::Time::from_hms_nano(23, 59, 59, 999_999_999).unwrap(),
            max_time.to_be_bytes(),
        ),
        (
            time::Time::from_hms_nano(1, 2, 3, 4).unwrap(),
            any_time.to_be_bytes(),
        ),
    ];
    for (time, expected) in test_cases {
        let bytes = serialized(time);

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
        let bytes: Vec<u8> = serialized(test_timestamp);

        let mut expected_bytes: Vec<u8> = vec![0, 0, 0, 8];
        expected_bytes.extend_from_slice(&test_val.to_be_bytes());

        assert_eq!(bytes, expected_bytes);
        assert_eq!(expected_bytes.len(), 12);
    }
}

#[cfg(feature = "chrono")]
#[test]
fn naive_date_time_serialization() {
    use chrono::NaiveDateTime;
    let test_cases = [
        (
            // Max time serialized without error
            NaiveDateTime::MAX,
            NaiveDateTime::MAX.timestamp_millis().to_be_bytes(),
        ),
        (
            // Min time serialized without error
            NaiveDateTime::MIN,
            NaiveDateTime::MIN.timestamp_millis().to_be_bytes(),
        ),
        (
            // UNIX epoch baseline
            NaiveDateTime::from_timestamp_opt(0, 0).unwrap(),
            0i64.to_be_bytes(),
        ),
        (
            // One second since UNIX epoch
            NaiveDateTime::from_timestamp_opt(1, 0).unwrap(),
            1000i64.to_be_bytes(),
        ),
        (
            // 1 nanosecond since UNIX epoch, lost during serialization
            NaiveDateTime::from_timestamp_opt(0, 1).unwrap(),
            0i64.to_be_bytes(),
        ),
        (
            // 1 millisecond since UNIX epoch
            NaiveDateTime::from_timestamp_opt(0, 1_000_000).unwrap(),
            1i64.to_be_bytes(),
        ),
        (
            // 2 days before UNIX epoch
            NaiveDateTime::from_timestamp_opt(-2 * 24 * 60 * 60, 0).unwrap(),
            (-2 * 24i64 * 60 * 60 * 1000).to_be_bytes(),
        ),
    ];
    for (datetime, expected) in test_cases {
        let test_datetime = datetime.and_utc();
        let bytes: Vec<u8> = serialized(test_datetime);

        let mut expected_bytes: Vec<u8> = vec![0, 0, 0, 8];
        expected_bytes.extend_from_slice(&expected);

        assert_eq!(bytes, expected_bytes);
        assert_eq!(expected_bytes.len(), 12);
    }
}

#[cfg(feature = "time")]
#[test]
fn offset_date_time_serialization() {
    use time::{Date, Month, OffsetDateTime, PrimitiveDateTime, Time};
    let offset_max =
        PrimitiveDateTime::MAX.assume_offset(time::UtcOffset::from_hms(-23, -59, -59).unwrap());
    let offset_min =
        PrimitiveDateTime::MIN.assume_offset(time::UtcOffset::from_hms(23, 59, 59).unwrap());
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
        let bytes: Vec<u8> = serialized(datetime);

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
        let uuid_serialized: Vec<u8> = serialized(uuid);

        let mut expected_serialized: Vec<u8> = vec![0, 0, 0, 16];
        expected_serialized.extend_from_slice(uuid_bytes.as_ref());

        assert_eq!(uuid_serialized, expected_serialized);
    }
}

#[test]
fn option_value() {
    assert_eq!(serialized(Some(32_i32)), vec![0, 0, 0, 4, 0, 0, 0, 32]);
    let null_i32: Option<i32> = None;
    assert_eq!(serialized(null_i32), &(-1_i32).to_be_bytes()[..]);
}

#[test]
fn unset_value() {
    assert_eq!(serialized(Unset), &(-2_i32).to_be_bytes()[..]);

    let unset_i32: MaybeUnset<i32> = MaybeUnset::Unset;
    assert_eq!(serialized(unset_i32), &(-2_i32).to_be_bytes()[..]);

    let set_i32: MaybeUnset<i32> = MaybeUnset::Set(32);
    assert_eq!(serialized(set_i32), vec![0, 0, 0, 4, 0, 0, 0, 32]);
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
    const EMPTY: SerializedValues = SerializedValues::new();
    assert_eq!(EMPTY.len(), 0);
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
    values.add_value(&8_i8).unwrap();
    {
        assert_eq!(values.len(), 1);
        assert!(!values.is_empty());

        let mut request = Vec::<u8>::new();
        values.write_to_request(&mut request);
        assert_eq!(request, vec![0, 1, 0, 0, 0, 1, 8]);

        assert_eq!(values.iter().collect::<Vec<_>>(), vec![Some([8].as_ref())]);
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
            vec![Some([8].as_ref()), Some([0, 16].as_ref())]
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
            vec![Some([8].as_ref()), Some([0, 16].as_ref())]
        );
    }
}

#[test]
fn unit_value_list() {
    let serialized_unit: SerializedValues =
        <() as ValueList>::serialized(&()).unwrap().into_owned();
    assert!(serialized_unit.is_empty());
}

#[test]
fn empty_array_value_list() {
    let serialized_arr: SerializedValues = <[u8; 0] as ValueList>::serialized(&[])
        .unwrap()
        .into_owned();
    assert!(serialized_arr.is_empty());
}

#[test]
fn slice_value_list() {
    let values: &[i32] = &[1, 2, 3];
    let serialized: SerializedValues = <&[i32] as ValueList>::serialized(&values)
        .unwrap()
        .into_owned();

    assert_eq!(
        serialized.iter().collect::<Vec<_>>(),
        vec![
            Some([0, 0, 0, 1].as_ref()),
            Some([0, 0, 0, 2].as_ref()),
            Some([0, 0, 0, 3].as_ref())
        ]
    );
}

#[test]
fn vec_value_list() {
    let values: Vec<i32> = vec![1, 2, 3];
    let serialized: SerializedValues = <Vec<i32> as ValueList>::serialized(&values)
        .unwrap()
        .into_owned();

    assert_eq!(
        serialized.iter().collect::<Vec<_>>(),
        vec![
            Some([0, 0, 0, 1].as_ref()),
            Some([0, 0, 0, 2].as_ref()),
            Some([0, 0, 0, 3].as_ref())
        ]
    );
}

#[test]
fn tuple_value_list() {
    fn check_i8_tuple(tuple: impl ValueList, expected: core::ops::Range<u8>) {
        let serialized: SerializedValues = tuple.serialized().unwrap().into_owned();
        assert_eq!(serialized.len() as usize, expected.len());

        let serialized_vals: Vec<u8> = serialized
            .iter()
            .map(|o: Option<&[u8]>| o.unwrap()[0])
            .collect();

        let expected: Vec<u8> = expected.collect();

        assert_eq!(serialized_vals, expected);
    }

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
fn ref_value_list() {
    let values: &[i32] = &[1, 2, 3];
    let serialized: SerializedValues = <&&[i32] as ValueList>::serialized(&&values)
        .unwrap()
        .into_owned();

    assert_eq!(
        serialized.iter().collect::<Vec<_>>(),
        vec![
            Some([0, 0, 0, 1].as_ref()),
            Some([0, 0, 0, 2].as_ref()),
            Some([0, 0, 0, 3].as_ref())
        ]
    );
}

#[test]
fn serialized_values_value_list() {
    let mut ser_values = SerializedValues::new();
    ser_values.add_value(&1_i32).unwrap();
    ser_values.add_value(&"qwertyuiop").unwrap();

    let ser_ser_values: Cow<SerializedValues> = ser_values.serialized().unwrap();
    assert!(matches!(ser_ser_values, Cow::Borrowed(_)));

    assert_eq!(&ser_values, ser_ser_values.as_ref());
}

#[test]
fn cow_serialized_values_value_list() {
    let cow_ser_values: Cow<SerializedValues> = Cow::Owned(SerializedValues::new());

    let serialized: Cow<SerializedValues> = cow_ser_values.serialized().unwrap();
    assert!(matches!(serialized, Cow::Borrowed(_)));

    assert_eq!(cow_ser_values.as_ref(), serialized.as_ref());
}

#[test]
fn slice_batch_values() {
    let batch_values: &[&[i8]] = &[&[1, 2], &[2, 3, 4, 5], &[6]];
    let mut it = batch_values.batch_values_iter();
    {
        let mut request: Vec<u8> = Vec::new();
        it.write_next_to_request(&mut request).unwrap().unwrap();
        assert_eq!(request, vec![0, 2, 0, 0, 0, 1, 1, 0, 0, 0, 1, 2]);
    }

    {
        let mut request: Vec<u8> = Vec::new();
        it.write_next_to_request(&mut request).unwrap().unwrap();
        assert_eq!(
            request,
            vec![0, 4, 0, 0, 0, 1, 2, 0, 0, 0, 1, 3, 0, 0, 0, 1, 4, 0, 0, 0, 1, 5]
        );
    }

    {
        let mut request: Vec<u8> = Vec::new();
        it.write_next_to_request(&mut request).unwrap().unwrap();
        assert_eq!(request, vec![0, 1, 0, 0, 0, 1, 6]);
    }

    assert_eq!(it.write_next_to_request(&mut Vec::new()), None);
}

#[test]
fn vec_batch_values() {
    let batch_values: Vec<Vec<i8>> = vec![vec![1, 2], vec![2, 3, 4, 5], vec![6]];

    let mut it = batch_values.batch_values_iter();
    {
        let mut request: Vec<u8> = Vec::new();
        it.write_next_to_request(&mut request).unwrap().unwrap();
        assert_eq!(request, vec![0, 2, 0, 0, 0, 1, 1, 0, 0, 0, 1, 2]);
    }

    {
        let mut request: Vec<u8> = Vec::new();
        it.write_next_to_request(&mut request).unwrap().unwrap();
        assert_eq!(
            request,
            vec![0, 4, 0, 0, 0, 1, 2, 0, 0, 0, 1, 3, 0, 0, 0, 1, 4, 0, 0, 0, 1, 5]
        );
    }

    {
        let mut request: Vec<u8> = Vec::new();
        it.write_next_to_request(&mut request).unwrap().unwrap();
        assert_eq!(request, vec![0, 1, 0, 0, 0, 1, 6]);
    }
}

#[test]
fn tuple_batch_values() {
    fn check_twoi32_tuple(tuple: impl BatchValues, size: usize) {
        let mut it = tuple.batch_values_iter();
        for i in 0..size {
            let mut request: Vec<u8> = Vec::new();
            it.write_next_to_request(&mut request).unwrap().unwrap();

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

    return check_ref_bv::<&&&&&[&[i8]]>(&&&&batch_values);
    fn check_ref_bv<B: BatchValues>(batch_values: B) {
        let mut it = <B as BatchValues>::batch_values_iter(&batch_values);

        let mut request: Vec<u8> = Vec::new();
        it.write_next_to_request(&mut request).unwrap().unwrap();
        assert_eq!(request, vec![0, 2, 0, 0, 0, 1, 1, 0, 0, 0, 1, 2]);
    }
}

#[test]
#[allow(clippy::needless_borrow)]
fn check_ref_tuple() {
    fn assert_has_batch_values<BV: BatchValues>(bv: BV) {
        let mut it = bv.batch_values_iter();
        let mut request: Vec<u8> = Vec::new();
        while let Some(res) = it.write_next_to_request(&mut request) {
            res.unwrap()
        }
    }
    let s = String::from("hello");
    let tuple: ((&str,),) = ((&s,),);
    assert_has_batch_values::<&_>(&tuple);
    let tuple2: ((&str, &str), (&str, &str)) = ((&s, &s), (&s, &s));
    assert_has_batch_values::<&_>(&tuple2);
}

#[test]
fn check_batch_values_iterator_is_not_lending() {
    // This is an interesting property if we want to improve the batch shard selection heuristic
    fn f(bv: impl BatchValues) {
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
    f(((10,), (11,)))
}
