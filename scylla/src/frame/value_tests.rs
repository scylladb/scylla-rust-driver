use super::{
    response::result::CqlValue,
    value::{
        BatchValues, Date, MaybeUnset, SerializeValuesError, SerializedValues, Time, Timestamp,
        Unset, Value, ValueList, ValueTooBig,
    },
};
use crate::{Session, SessionBuilder};
use bytes::BufMut;
use chrono::{Duration, NaiveDate};
use std::{borrow::Cow, convert::TryInto, env};
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
fn naive_date_serialization() {
    // 1970-01-31 is 2^31
    let unix_epoch: NaiveDate = NaiveDate::from_ymd(1970, 1, 1);
    assert_eq!(serialized(unix_epoch), vec![0, 0, 0, 4, 128, 0, 0, 0]);
    assert_eq!(2_u32.pow(31).to_be_bytes(), [128, 0, 0, 0]);

    // 1969-12-02 is 2^31 - 30
    let before_epoch: NaiveDate = NaiveDate::from_ymd(1969, 12, 2);
    assert_eq!(
        serialized(before_epoch),
        vec![0, 0, 0, 4, 127, 255, 255, 226]
    );
    assert_eq!((2_u32.pow(31) - 30).to_be_bytes(), [127, 255, 255, 226]);

    // 1970-01-31 is 2^31 + 30
    let after_epoch: NaiveDate = NaiveDate::from_ymd(1970, 1, 31);
    assert_eq!(serialized(after_epoch), vec![0, 0, 0, 4, 128, 0, 0, 30]);
    assert_eq!((2_u32.pow(31) + 30).to_be_bytes(), [128, 0, 0, 30]);
}

#[test]
fn date_serialization() {
    assert_eq!(serialized(Date(0)), vec![0, 0, 0, 4, 0, 0, 0, 0]);
    assert_eq!(
        serialized(Date(u32::max_value())),
        vec![0, 0, 0, 4, 255, 255, 255, 255]
    );
}

#[test]
fn time_serialization() {
    // Time is an i64 - nanoseconds since midnight
    // in range 0..=86399999999999

    let max_time: i64 = 24 * 60 * 60 * 1_000_000_000 - 1;
    assert_eq!(max_time, 86399999999999);

    // Check that basic values are serialized correctly
    // Invalid values are also serialized correctly - database will respond with an error
    for test_val in [0, 1, 15, 18463, max_time, -1, -324234, max_time + 16].iter() {
        let test_time: Time = Time(Duration::nanoseconds(*test_val));
        let bytes: Vec<u8> = serialized(test_time);

        let mut expected_bytes: Vec<u8> = vec![0, 0, 0, 8];
        expected_bytes.extend_from_slice(&test_val.to_be_bytes());

        assert_eq!(bytes, expected_bytes);
        assert_eq!(expected_bytes.len(), 12);
    }

    // Durations so long that nanoseconds don't fit in i64 cause an error
    let long_time = Time(Duration::milliseconds(i64::max_value()));
    assert_eq!(long_time.serialize(&mut Vec::new()), Err(ValueTooBig));
}

#[test]
fn timestamp_serialization() {
    // Timestamp is milliseconds since unix epoch represented as i64

    for test_val in &[
        0,
        -1,
        1,
        -45345346,
        453451,
        i64::min_value(),
        i64::max_value(),
    ] {
        let test_timestamp: Timestamp = Timestamp(Duration::milliseconds(*test_val));
        let bytes: Vec<u8> = serialized(test_timestamp);

        let mut expected_bytes: Vec<u8> = vec![0, 0, 0, 8];
        expected_bytes.extend_from_slice(&test_val.to_be_bytes());

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
    assert_eq!(serialized(&1_i32), serialized(1_i32));
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

    // Add a value thats too big, recover gracefully
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

    assert_eq!(<&[&[i8]] as BatchValues>::len(&batch_values), 3);

    {
        let mut request: Vec<u8> = Vec::new();
        batch_values.write_nth_to_request(0, &mut request).unwrap();
        assert_eq!(request, vec![0, 2, 0, 0, 0, 1, 1, 0, 0, 0, 1, 2]);
    }

    {
        let mut request: Vec<u8> = Vec::new();
        batch_values.write_nth_to_request(1, &mut request).unwrap();
        assert_eq!(
            request,
            vec![0, 4, 0, 0, 0, 1, 2, 0, 0, 0, 1, 3, 0, 0, 0, 1, 4, 0, 0, 0, 1, 5]
        );
    }

    {
        let mut request: Vec<u8> = Vec::new();
        batch_values.write_nth_to_request(2, &mut request).unwrap();
        assert_eq!(request, vec![0, 1, 0, 0, 0, 1, 6]);
    }
}

#[test]
fn vec_batch_values() {
    let batch_values: Vec<Vec<i8>> = vec![vec![1, 2], vec![2, 3, 4, 5], vec![6]];

    assert_eq!(<Vec<Vec<i8>> as BatchValues>::len(&batch_values), 3);

    {
        let mut request: Vec<u8> = Vec::new();
        batch_values.write_nth_to_request(0, &mut request).unwrap();
        assert_eq!(request, vec![0, 2, 0, 0, 0, 1, 1, 0, 0, 0, 1, 2]);
    }

    {
        let mut request: Vec<u8> = Vec::new();
        batch_values.write_nth_to_request(1, &mut request).unwrap();
        assert_eq!(
            request,
            vec![0, 4, 0, 0, 0, 1, 2, 0, 0, 0, 1, 3, 0, 0, 0, 1, 4, 0, 0, 0, 1, 5]
        );
    }

    {
        let mut request: Vec<u8> = Vec::new();
        batch_values.write_nth_to_request(2, &mut request).unwrap();
        assert_eq!(request, vec![0, 1, 0, 0, 0, 1, 6]);
    }
}

#[test]
fn tuple_batch_values() {
    fn check_twoi32_tuple(tuple: impl BatchValues, size: usize) {
        assert_eq!(tuple.len(), size);

        for i in 0..size {
            let mut request: Vec<u8> = Vec::new();
            tuple.write_nth_to_request(i, &mut request).unwrap();

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
fn ref_batch_values() {
    let batch_values: &[&[i8]] = &[&[1, 2], &[2, 3, 4, 5], &[6]];

    assert_eq!(<&&&&[&[i8]] as BatchValues>::len(&&&&batch_values), 3);

    {
        let mut request: Vec<u8> = Vec::new();
        <&&&&[&[i8]] as BatchValues>::write_nth_to_request(&&&&batch_values, 0, &mut request)
            .unwrap();
        assert_eq!(request, vec![0, 2, 0, 0, 0, 1, 1, 0, 0, 0, 1, 2]);
    }
}

#[tokio::test]
async fn test_cqlvalue_udt() {
    let uri = env::var("SCYLLA_URI").unwrap_or_else(|_| "127.0.0.1:9042".to_string());
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
        .query(
            "CREATE TYPE IF NOT EXISTS ks.cqlvalue_udt_type (int_val int, text_val text)",
            &[],
        )
        .await
        .unwrap();
    session
        .query(
            "CREATE TABLE IF NOT EXISTS ks.cqlvalue_udt_test (k int, my cqlvalue_udt_type, primary key (k))",
            &[],
        )
        .await
        .unwrap();

    let udt_cql_value = CqlValue::UserDefinedType {
        keyspace: "ks".to_string(),
        type_name: "cqlvalue_udt_type".to_string(),
        fields: vec![
            ("int_val".to_string(), Some(CqlValue::Int(42))),
            ("text_val".to_string(), Some(CqlValue::Text("hi".into()))),
        ],
    };

    session
        .query(
            "INSERT INTO ks.cqlvalue_udt_test (k, my) VALUES (5, ?)",
            (&udt_cql_value,),
        )
        .await
        .unwrap();

    let rows = session
        .query("SELECT my FROM ks.cqlvalue_udt_test", &[])
        .await
        .unwrap()
        .rows
        .unwrap();

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].columns.len(), 1);

    let received_udt_cql_value = rows[0].columns[0].as_ref().unwrap();

    assert_eq!(received_udt_cql_value, &udt_cql_value);
}
