use assert_matches::assert_matches;
use bytes::{BufMut, Bytes, BytesMut};
use uuid::Uuid;

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::fmt::Debug;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};

use crate::frame::response::result::{ColumnType, CqlValue};
use crate::frame::value::{
    Counter, CqlDate, CqlDecimal, CqlDuration, CqlTime, CqlTimestamp, CqlTimeuuid, CqlVarint,
};
use crate::types::deserialize::value::{TupleDeserializationErrorKind, TupleTypeCheckErrorKind};
use crate::types::deserialize::{DeserializationError, FrameSlice, TypeCheckError};
use crate::types::serialize::value::SerializeValue;
use crate::types::serialize::CellWriter;

use super::{
    mk_deser_err, BuiltinDeserializationError, BuiltinDeserializationErrorKind,
    BuiltinTypeCheckError, BuiltinTypeCheckErrorKind, DeserializeValue, ListlikeIterator,
    MapDeserializationErrorKind, MapIterator, MapTypeCheckErrorKind, MaybeEmpty,
    SetOrListDeserializationErrorKind, SetOrListTypeCheckErrorKind, UdtDeserializationErrorKind,
    UdtTypeCheckErrorKind,
};

#[test]
fn test_deserialize_bytes() {
    const ORIGINAL_BYTES: &[u8] = &[1, 5, 2, 4, 3];

    let bytes = make_bytes(ORIGINAL_BYTES);

    let decoded_slice = deserialize::<&[u8]>(&ColumnType::Blob, &bytes).unwrap();
    let decoded_vec = deserialize::<Vec<u8>>(&ColumnType::Blob, &bytes).unwrap();
    let decoded_bytes = deserialize::<Bytes>(&ColumnType::Blob, &bytes).unwrap();

    assert_eq!(decoded_slice, ORIGINAL_BYTES);
    assert_eq!(decoded_vec, ORIGINAL_BYTES);
    assert_eq!(decoded_bytes, ORIGINAL_BYTES);

    // ser/de identity

    // Nonempty blob
    assert_ser_de_identity(&ColumnType::Blob, &ORIGINAL_BYTES, &mut Bytes::new());

    // Empty blob
    assert_ser_de_identity(&ColumnType::Blob, &(&[] as &[u8]), &mut Bytes::new());
}

#[test]
fn test_deserialize_ascii() {
    const ASCII_TEXT: &str = "The quick brown fox jumps over the lazy dog";

    let ascii = make_bytes(ASCII_TEXT.as_bytes());

    for typ in [ColumnType::Ascii, ColumnType::Text].iter() {
        let decoded_str = deserialize::<&str>(typ, &ascii).unwrap();
        let decoded_string = deserialize::<String>(typ, &ascii).unwrap();

        assert_eq!(decoded_str, ASCII_TEXT);
        assert_eq!(decoded_string, ASCII_TEXT);

        // ser/de identity

        // Empty string
        assert_ser_de_identity(typ, &"", &mut Bytes::new());
        assert_ser_de_identity(typ, &"".to_owned(), &mut Bytes::new());

        // Nonempty string
        assert_ser_de_identity(typ, &ASCII_TEXT, &mut Bytes::new());
        assert_ser_de_identity(typ, &ASCII_TEXT.to_owned(), &mut Bytes::new());
    }
}

#[test]
fn test_deserialize_text() {
    const UNICODE_TEXT: &str = "Zażółć gęślą jaźń";

    let unicode = make_bytes(UNICODE_TEXT.as_bytes());

    // Should fail because it's not an ASCII string
    deserialize::<&str>(&ColumnType::Ascii, &unicode).unwrap_err();
    deserialize::<String>(&ColumnType::Ascii, &unicode).unwrap_err();

    let decoded_text_str = deserialize::<&str>(&ColumnType::Text, &unicode).unwrap();
    let decoded_text_string = deserialize::<String>(&ColumnType::Text, &unicode).unwrap();
    assert_eq!(decoded_text_str, UNICODE_TEXT);
    assert_eq!(decoded_text_string, UNICODE_TEXT);

    // ser/de identity

    assert_ser_de_identity(&ColumnType::Text, &UNICODE_TEXT, &mut Bytes::new());
    assert_ser_de_identity(
        &ColumnType::Text,
        &UNICODE_TEXT.to_owned(),
        &mut Bytes::new(),
    );
}

#[test]
fn test_integral() {
    let tinyint = make_bytes(&[0x01]);
    let decoded_tinyint = deserialize::<i8>(&ColumnType::TinyInt, &tinyint).unwrap();
    assert_eq!(decoded_tinyint, 0x01);

    let smallint = make_bytes(&[0x01, 0x02]);
    let decoded_smallint = deserialize::<i16>(&ColumnType::SmallInt, &smallint).unwrap();
    assert_eq!(decoded_smallint, 0x0102);

    let int = make_bytes(&[0x01, 0x02, 0x03, 0x04]);
    let decoded_int = deserialize::<i32>(&ColumnType::Int, &int).unwrap();
    assert_eq!(decoded_int, 0x01020304);

    let bigint = make_bytes(&[0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08]);
    let decoded_bigint = deserialize::<i64>(&ColumnType::BigInt, &bigint).unwrap();
    assert_eq!(decoded_bigint, 0x0102030405060708);

    // ser/de identity
    assert_ser_de_identity(&ColumnType::TinyInt, &42_i8, &mut Bytes::new());
    assert_ser_de_identity(&ColumnType::SmallInt, &2137_i16, &mut Bytes::new());
    assert_ser_de_identity(&ColumnType::Int, &21372137_i32, &mut Bytes::new());
    assert_ser_de_identity(&ColumnType::BigInt, &0_i64, &mut Bytes::new());
}

#[test]
fn test_bool() {
    for boolean in [true, false] {
        let boolean_bytes = make_bytes(&[boolean as u8]);
        let decoded_bool = deserialize::<bool>(&ColumnType::Boolean, &boolean_bytes).unwrap();
        assert_eq!(decoded_bool, boolean);

        // ser/de identity
        assert_ser_de_identity(&ColumnType::Boolean, &boolean, &mut Bytes::new());
    }
}

#[test]
fn test_floating_point() {
    let float = make_bytes(&[63, 0, 0, 0]);
    let decoded_float = deserialize::<f32>(&ColumnType::Float, &float).unwrap();
    assert_eq!(decoded_float, 0.5);

    let double = make_bytes(&[64, 0, 0, 0, 0, 0, 0, 0]);
    let decoded_double = deserialize::<f64>(&ColumnType::Double, &double).unwrap();
    assert_eq!(decoded_double, 2.0);

    // ser/de identity
    assert_ser_de_identity(&ColumnType::Float, &21.37_f32, &mut Bytes::new());
    assert_ser_de_identity(&ColumnType::Double, &2137.2137_f64, &mut Bytes::new());
}

#[test]
fn test_varlen_numbers() {
    // varint
    assert_ser_de_identity(
        &ColumnType::Varint,
        &CqlVarint::from_signed_bytes_be_slice(b"Ala ma kota"),
        &mut Bytes::new(),
    );

    #[cfg(feature = "num-bigint-03")]
    assert_ser_de_identity(
        &ColumnType::Varint,
        &num_bigint_03::BigInt::from_signed_bytes_be(b"Kot ma Ale"),
        &mut Bytes::new(),
    );

    #[cfg(feature = "num-bigint-04")]
    assert_ser_de_identity(
        &ColumnType::Varint,
        &num_bigint_04::BigInt::from_signed_bytes_be(b"Kot ma Ale"),
        &mut Bytes::new(),
    );

    // decimal
    assert_ser_de_identity(
        &ColumnType::Decimal,
        &CqlDecimal::from_signed_be_bytes_slice_and_exponent(b"Ala ma kota", 42),
        &mut Bytes::new(),
    );

    #[cfg(feature = "bigdecimal-04")]
    assert_ser_de_identity(
        &ColumnType::Decimal,
        &bigdecimal_04::BigDecimal::new(
            bigdecimal_04::num_bigint::BigInt::from_signed_bytes_be(b"Ala ma kota"),
            42,
        ),
        &mut Bytes::new(),
    );
}

#[test]
fn test_date_time_types() {
    // duration
    assert_ser_de_identity(
        &ColumnType::Duration,
        &CqlDuration {
            months: 21,
            days: 37,
            nanoseconds: 42,
        },
        &mut Bytes::new(),
    );

    // date
    assert_ser_de_identity(&ColumnType::Date, &CqlDate(0xbeaf), &mut Bytes::new());

    #[cfg(feature = "chrono-04")]
    assert_ser_de_identity(
        &ColumnType::Date,
        &chrono_04::NaiveDate::from_yo_opt(1999, 99).unwrap(),
        &mut Bytes::new(),
    );

    #[cfg(feature = "time-03")]
    assert_ser_de_identity(
        &ColumnType::Date,
        &time_03::Date::from_ordinal_date(1999, 99).unwrap(),
        &mut Bytes::new(),
    );

    // time
    assert_ser_de_identity(&ColumnType::Time, &CqlTime(0xdeed), &mut Bytes::new());

    #[cfg(feature = "chrono-04")]
    assert_ser_de_identity(
        &ColumnType::Time,
        &chrono_04::NaiveTime::from_hms_micro_opt(21, 37, 21, 37).unwrap(),
        &mut Bytes::new(),
    );

    #[cfg(feature = "time-03")]
    assert_ser_de_identity(
        &ColumnType::Time,
        &time_03::Time::from_hms_micro(21, 37, 21, 37).unwrap(),
        &mut Bytes::new(),
    );

    // timestamp
    assert_ser_de_identity(
        &ColumnType::Timestamp,
        &CqlTimestamp(0xceed),
        &mut Bytes::new(),
    );

    #[cfg(feature = "chrono-04")]
    assert_ser_de_identity(
        &ColumnType::Timestamp,
        &chrono_04::DateTime::<chrono_04::Utc>::from_timestamp_millis(0xdead_cafe_deaf).unwrap(),
        &mut Bytes::new(),
    );

    #[cfg(feature = "time-03")]
    assert_ser_de_identity(
        &ColumnType::Timestamp,
        &time_03::OffsetDateTime::from_unix_timestamp(0xdead_cafe).unwrap(),
        &mut Bytes::new(),
    );
}

#[test]
fn test_inet() {
    assert_ser_de_identity(
        &ColumnType::Inet,
        &IpAddr::V4(Ipv4Addr::BROADCAST),
        &mut Bytes::new(),
    );

    assert_ser_de_identity(
        &ColumnType::Inet,
        &IpAddr::V6(Ipv6Addr::LOCALHOST),
        &mut Bytes::new(),
    );
}

#[test]
fn test_uuid() {
    assert_ser_de_identity(
        &ColumnType::Uuid,
        &Uuid::from_u128(0xdead_cafe_deaf_feed_beaf_bead),
        &mut Bytes::new(),
    );

    assert_ser_de_identity(
        &ColumnType::Timeuuid,
        &CqlTimeuuid::from_u128(0xdead_cafe_deaf_feed_beaf_bead),
        &mut Bytes::new(),
    );
}

#[test]
fn test_null_and_empty() {
    // non-nullable emptiable deserialization, non-empty value
    let int = make_bytes(&[21, 37, 0, 0]);
    let decoded_int = deserialize::<MaybeEmpty<i32>>(&ColumnType::Int, &int).unwrap();
    assert_eq!(decoded_int, MaybeEmpty::Value((21 << 24) + (37 << 16)));

    // non-nullable emptiable deserialization, empty value
    let int = make_bytes(&[]);
    let decoded_int = deserialize::<MaybeEmpty<i32>>(&ColumnType::Int, &int).unwrap();
    assert_eq!(decoded_int, MaybeEmpty::Empty);

    // nullable non-emptiable deserialization, non-null value
    let int = make_bytes(&[21, 37, 0, 0]);
    let decoded_int = deserialize::<Option<i32>>(&ColumnType::Int, &int).unwrap();
    assert_eq!(decoded_int, Some((21 << 24) + (37 << 16)));

    // nullable non-emptiable deserialization, null value
    let int = make_null();
    let decoded_int = deserialize::<Option<i32>>(&ColumnType::Int, &int).unwrap();
    assert_eq!(decoded_int, None);

    // nullable emptiable deserialization, non-null non-empty value
    let int = make_bytes(&[]);
    let decoded_int = deserialize::<Option<MaybeEmpty<i32>>>(&ColumnType::Int, &int).unwrap();
    assert_eq!(decoded_int, Some(MaybeEmpty::Empty));

    // ser/de identity
    assert_ser_de_identity(&ColumnType::Int, &Some(12321_i32), &mut Bytes::new());
    assert_ser_de_identity(&ColumnType::Double, &None::<f64>, &mut Bytes::new());
    assert_ser_de_identity(
        &ColumnType::Set(Box::new(ColumnType::Ascii)),
        &None::<Vec<&str>>,
        &mut Bytes::new(),
    );
}

#[test]
fn test_maybe_empty() {
    let empty = make_bytes(&[]);
    let decoded_empty = deserialize::<MaybeEmpty<i8>>(&ColumnType::TinyInt, &empty).unwrap();
    assert_eq!(decoded_empty, MaybeEmpty::Empty);

    let non_empty = make_bytes(&[0x01]);
    let decoded_non_empty =
        deserialize::<MaybeEmpty<i8>>(&ColumnType::TinyInt, &non_empty).unwrap();
    assert_eq!(decoded_non_empty, MaybeEmpty::Value(0x01));
}

#[test]
fn test_cql_value() {
    assert_ser_de_identity(
        &ColumnType::Counter,
        &CqlValue::Counter(Counter(765)),
        &mut Bytes::new(),
    );

    assert_ser_de_identity(
        &ColumnType::Timestamp,
        &CqlValue::Timestamp(CqlTimestamp(2136)),
        &mut Bytes::new(),
    );

    assert_ser_de_identity(&ColumnType::Boolean, &CqlValue::Empty, &mut Bytes::new());

    assert_ser_de_identity(
        &ColumnType::Text,
        &CqlValue::Text("kremówki".to_owned()),
        &mut Bytes::new(),
    );
    assert_ser_de_identity(
        &ColumnType::Ascii,
        &CqlValue::Ascii("kremowy".to_owned()),
        &mut Bytes::new(),
    );

    assert_ser_de_identity(
        &ColumnType::Set(Box::new(ColumnType::Text)),
        &CqlValue::Set(vec![CqlValue::Text("Ala ma kota".to_owned())]),
        &mut Bytes::new(),
    );
}

#[test]
fn test_list_and_set() {
    let mut collection_contents = BytesMut::new();
    collection_contents.put_i32(3);
    append_bytes(&mut collection_contents, "quick".as_bytes());
    append_bytes(&mut collection_contents, "brown".as_bytes());
    append_bytes(&mut collection_contents, "fox".as_bytes());

    let collection = make_bytes(&collection_contents);

    let list_typ = ColumnType::List(Box::new(ColumnType::Ascii));
    let set_typ = ColumnType::Set(Box::new(ColumnType::Ascii));

    // iterator
    let mut iter = deserialize::<ListlikeIterator<&str>>(&list_typ, &collection).unwrap();
    assert_eq!(iter.next().transpose().unwrap(), Some("quick"));
    assert_eq!(iter.next().transpose().unwrap(), Some("brown"));
    assert_eq!(iter.next().transpose().unwrap(), Some("fox"));
    assert_eq!(iter.next().transpose().unwrap(), None);

    let expected_vec_str = vec!["quick", "brown", "fox"];
    let expected_vec_string = vec!["quick".to_string(), "brown".to_string(), "fox".to_string()];

    // list
    let decoded_vec_str = deserialize::<Vec<&str>>(&list_typ, &collection).unwrap();
    let decoded_vec_string = deserialize::<Vec<String>>(&list_typ, &collection).unwrap();
    assert_eq!(decoded_vec_str, expected_vec_str);
    assert_eq!(decoded_vec_string, expected_vec_string);

    // hash set
    let decoded_hash_str = deserialize::<HashSet<&str>>(&set_typ, &collection).unwrap();
    let decoded_hash_string = deserialize::<HashSet<String>>(&set_typ, &collection).unwrap();
    assert_eq!(
        decoded_hash_str,
        expected_vec_str.clone().into_iter().collect(),
    );
    assert_eq!(
        decoded_hash_string,
        expected_vec_string.clone().into_iter().collect(),
    );

    // btree set
    let decoded_btree_str = deserialize::<BTreeSet<&str>>(&set_typ, &collection).unwrap();
    let decoded_btree_string = deserialize::<BTreeSet<String>>(&set_typ, &collection).unwrap();
    assert_eq!(
        decoded_btree_str,
        expected_vec_str.clone().into_iter().collect(),
    );
    assert_eq!(
        decoded_btree_string,
        expected_vec_string.into_iter().collect(),
    );

    // Null collections are interpreted as empty collections, to retain convenience:
    // when an empty collection is sent to the DB, the DB nullifies the column instead.
    {
        let list_typ = ColumnType::List(Box::new(ColumnType::BigInt));
        let set_typ = ColumnType::Set(Box::new(ColumnType::BigInt));
        type CollTyp = i64;

        fn check<'frame, Collection: DeserializeValue<'frame>>(typ: &'frame ColumnType) {
            <Collection as DeserializeValue<'_>>::type_check(typ).unwrap();
            <Collection as DeserializeValue<'_>>::deserialize(typ, None).unwrap();
        }

        check::<Vec<CollTyp>>(&list_typ);
        check::<Vec<CollTyp>>(&set_typ);
        check::<HashSet<CollTyp>>(&set_typ);
        check::<BTreeSet<CollTyp>>(&set_typ);
    }

    // ser/de identity
    assert_ser_de_identity(&list_typ, &vec!["qwik"], &mut Bytes::new());
    assert_ser_de_identity(&set_typ, &vec!["qwik"], &mut Bytes::new());
    assert_ser_de_identity(
        &set_typ,
        &HashSet::<&str, std::collections::hash_map::RandomState>::from_iter(["qwik"]),
        &mut Bytes::new(),
    );
    assert_ser_de_identity(
        &set_typ,
        &BTreeSet::<&str>::from_iter(["qwik"]),
        &mut Bytes::new(),
    );
}

#[test]
fn test_map() {
    let mut collection_contents = BytesMut::new();
    collection_contents.put_i32(3);
    append_bytes(&mut collection_contents, &1i32.to_be_bytes());
    append_bytes(&mut collection_contents, "quick".as_bytes());
    append_bytes(&mut collection_contents, &2i32.to_be_bytes());
    append_bytes(&mut collection_contents, "brown".as_bytes());
    append_bytes(&mut collection_contents, &3i32.to_be_bytes());
    append_bytes(&mut collection_contents, "fox".as_bytes());

    let collection = make_bytes(&collection_contents);

    let typ = ColumnType::Map(Box::new(ColumnType::Int), Box::new(ColumnType::Ascii));

    // iterator
    let mut iter = deserialize::<MapIterator<i32, &str>>(&typ, &collection).unwrap();
    assert_eq!(iter.next().transpose().unwrap(), Some((1, "quick")));
    assert_eq!(iter.next().transpose().unwrap(), Some((2, "brown")));
    assert_eq!(iter.next().transpose().unwrap(), Some((3, "fox")));
    assert_eq!(iter.next().transpose().unwrap(), None);

    let expected_str = vec![(1, "quick"), (2, "brown"), (3, "fox")];
    let expected_string = vec![
        (1, "quick".to_string()),
        (2, "brown".to_string()),
        (3, "fox".to_string()),
    ];

    // hash set
    let decoded_hash_str = deserialize::<HashMap<i32, &str>>(&typ, &collection).unwrap();
    let decoded_hash_string = deserialize::<HashMap<i32, String>>(&typ, &collection).unwrap();
    assert_eq!(decoded_hash_str, expected_str.clone().into_iter().collect());
    assert_eq!(
        decoded_hash_string,
        expected_string.clone().into_iter().collect(),
    );

    // btree set
    let decoded_btree_str = deserialize::<BTreeMap<i32, &str>>(&typ, &collection).unwrap();
    let decoded_btree_string = deserialize::<BTreeMap<i32, String>>(&typ, &collection).unwrap();
    assert_eq!(
        decoded_btree_str,
        expected_str.clone().into_iter().collect(),
    );
    assert_eq!(decoded_btree_string, expected_string.into_iter().collect());

    // Null collections are interpreted as empty collections, to retain convenience:
    // when an empty collection is sent to the DB, the DB nullifies the column instead.
    {
        let map_typ = ColumnType::Map(Box::new(ColumnType::BigInt), Box::new(ColumnType::Ascii));
        type KeyTyp = i64;
        type ValueTyp<'s> = &'s str;

        fn check<'frame, Collection: DeserializeValue<'frame>>(typ: &'frame ColumnType) {
            <Collection as DeserializeValue<'_>>::type_check(typ).unwrap();
            <Collection as DeserializeValue<'_>>::deserialize(typ, None).unwrap();
        }

        check::<HashMap<KeyTyp, ValueTyp>>(&map_typ);
        check::<BTreeMap<KeyTyp, ValueTyp>>(&map_typ);
    }

    // ser/de identity
    assert_ser_de_identity(
        &typ,
        &HashMap::<i32, &str, std::collections::hash_map::RandomState>::from_iter([(-42, "qwik")]),
        &mut Bytes::new(),
    );
    assert_ser_de_identity(
        &typ,
        &BTreeMap::<i32, &str>::from_iter([(-42, "qwik")]),
        &mut Bytes::new(),
    );
}

#[test]
fn test_tuples() {
    let mut tuple_contents = BytesMut::new();
    append_bytes(&mut tuple_contents, &42i32.to_be_bytes());
    append_bytes(&mut tuple_contents, "foo".as_bytes());
    append_null(&mut tuple_contents);

    let tuple = make_bytes(&tuple_contents);

    let typ = ColumnType::Tuple(vec![ColumnType::Int, ColumnType::Ascii, ColumnType::Uuid]);

    let tup = deserialize::<(i32, &str, Option<Uuid>)>(&typ, &tuple).unwrap();
    assert_eq!(tup, (42, "foo", None));

    // ser/de identity

    // () does not implement SerializeValue, yet it does implement DeserializeValue.
    // assert_ser_de_identity(&ColumnType::Tuple(vec![]), &(), &mut Bytes::new());

    // nonempty, varied tuple
    assert_ser_de_identity(
        &ColumnType::Tuple(vec![
            ColumnType::List(Box::new(ColumnType::Boolean)),
            ColumnType::BigInt,
            ColumnType::Uuid,
            ColumnType::Inet,
        ]),
        &(
            vec![true, false, true],
            42_i64,
            Uuid::from_u128(0xdead_cafe_deaf_feed_beaf_bead),
            IpAddr::V6(Ipv6Addr::new(0xa, 0xb, 0xc, 0xd, 0xe, 0xf, 0x10, 0x11)),
        ),
        &mut Bytes::new(),
    );

    // nested tuples
    assert_ser_de_identity(
        &ColumnType::Tuple(vec![ColumnType::Tuple(vec![ColumnType::Tuple(vec![
            ColumnType::Text,
        ])])]),
        &((("",),),),
        &mut Bytes::new(),
    );
}

fn udt_def_with_fields(
    fields: impl IntoIterator<Item = (impl Into<String>, ColumnType)>,
) -> ColumnType {
    ColumnType::UserDefinedType {
        type_name: "udt".to_owned(),
        keyspace: "ks".to_owned(),
        field_types: fields.into_iter().map(|(s, t)| (s.into(), t)).collect(),
    }
}

#[must_use]
struct UdtSerializer {
    buf: BytesMut,
}

impl UdtSerializer {
    fn new() -> Self {
        Self {
            buf: BytesMut::default(),
        }
    }

    fn field(mut self, field_bytes: &[u8]) -> Self {
        append_bytes(&mut self.buf, field_bytes);
        self
    }

    fn null_field(mut self) -> Self {
        append_null(&mut self.buf);
        self
    }

    fn finalize(&self) -> Bytes {
        make_bytes(&self.buf)
    }
}

// Do not remove. It's not used in tests but we keep it here to check that
// we properly ignore warnings about unused variables, unnecessary `mut`s
// etc. that usually pop up when generating code for empty structs.
#[allow(unused)]
#[derive(scylla_macros::DeserializeValue)]
#[scylla(crate = crate)]
struct TestUdtWithNoFieldsUnordered {}

#[allow(unused)]
#[derive(scylla_macros::DeserializeValue)]
#[scylla(crate = crate, enforce_order)]
struct TestUdtWithNoFieldsOrdered {}

#[test]
fn test_udt_loose_ordering() {
    #[derive(scylla_macros::DeserializeValue, PartialEq, Eq, Debug)]
    #[scylla(crate = "crate")]
    struct Udt<'a> {
        a: &'a str,
        #[scylla(skip)]
        x: String,
        #[scylla(allow_missing)]
        b: Option<i32>,
        #[scylla(default_when_null)]
        c: i64,
    }

    // UDT fields in correct same order.
    {
        let udt_bytes = UdtSerializer::new()
            .field("The quick brown fox".as_bytes())
            .field(&42_i32.to_be_bytes())
            .field(&2137_i64.to_be_bytes())
            .finalize();
        let typ = udt_def_with_fields([
            ("a", ColumnType::Text),
            ("b", ColumnType::Int),
            ("c", ColumnType::BigInt),
        ]);

        let udt = deserialize::<Udt<'_>>(&typ, &udt_bytes).unwrap();
        assert_eq!(
            udt,
            Udt {
                a: "The quick brown fox",
                x: String::new(),
                b: Some(42),
                c: 2137,
            }
        );
    }

    // The last two UDT field are missing in serialized form - it should treat it
    // as if there were nulls at the end.
    {
        let udt_bytes = UdtSerializer::new()
            .field("The quick brown fox".as_bytes())
            .finalize();
        let typ = udt_def_with_fields([
            ("a", ColumnType::Text),
            ("b", ColumnType::Int),
            ("c", ColumnType::BigInt),
        ]);

        let udt = deserialize::<Udt<'_>>(&typ, &udt_bytes).unwrap();
        assert_eq!(
            udt,
            Udt {
                a: "The quick brown fox",
                x: String::new(),
                b: None,
                c: 0,
            }
        );
    }

    // UDT fields switched - should still work.
    {
        let udt_bytes = UdtSerializer::new()
            .field(&42_i32.to_be_bytes())
            .field("The quick brown fox".as_bytes())
            .field(&2137_i64.to_be_bytes())
            .finalize();
        let typ = udt_def_with_fields([
            ("b", ColumnType::Int),
            ("a", ColumnType::Text),
            ("c", ColumnType::BigInt),
        ]);

        let udt = deserialize::<Udt<'_>>(&typ, &udt_bytes).unwrap();
        assert_eq!(
            udt,
            Udt {
                a: "The quick brown fox",
                x: String::new(),
                b: Some(42),
                c: 2137,
            }
        );
    }

    // An excess UDT field - should still work.
    {
        let udt_bytes = UdtSerializer::new()
            .field(&12_i8.to_be_bytes())
            .field(&42_i32.to_be_bytes())
            .field("The quick brown fox".as_bytes())
            .field(&2137_i64.to_be_bytes())
            .finalize();
        let typ = udt_def_with_fields([
            ("d", ColumnType::TinyInt),
            ("b", ColumnType::Int),
            ("a", ColumnType::Text),
            ("c", ColumnType::BigInt),
        ]);

        Udt::type_check(&typ).unwrap();
        let udt = deserialize::<Udt<'_>>(&typ, &udt_bytes).unwrap();
        assert_eq!(
            udt,
            Udt {
                a: "The quick brown fox",
                x: String::new(),
                b: Some(42),
                c: 2137,
            }
        );
    }

    // Only field 'a' is present
    {
        let udt_bytes = UdtSerializer::new()
            .field("The quick brown fox".as_bytes())
            .finalize();
        let typ = udt_def_with_fields([("a", ColumnType::Text), ("c", ColumnType::BigInt)]);

        let udt = deserialize::<Udt<'_>>(&typ, &udt_bytes).unwrap();
        assert_eq!(
            udt,
            Udt {
                a: "The quick brown fox",
                x: String::new(),
                b: None,
                c: 0,
            }
        );
    }

    // Wrong column type
    {
        let typ = udt_def_with_fields([("a", ColumnType::Text)]);
        Udt::type_check(&typ).unwrap_err();
    }

    // Missing required column
    {
        let typ = udt_def_with_fields([("b", ColumnType::Int)]);
        Udt::type_check(&typ).unwrap_err();
    }
}

#[test]
fn test_udt_strict_ordering() {
    #[derive(scylla_macros::DeserializeValue, PartialEq, Eq, Debug)]
    #[scylla(crate = "crate", enforce_order)]
    struct Udt<'a> {
        #[scylla(default_when_null)]
        a: &'a str,
        #[scylla(skip)]
        x: String,
        #[scylla(allow_missing)]
        b: Option<i32>,
    }

    // UDT fields in correct same order
    {
        let udt_bytes = UdtSerializer::new()
            .field("The quick brown fox".as_bytes())
            .field(&42i32.to_be_bytes())
            .finalize();
        let typ = udt_def_with_fields([("a", ColumnType::Text), ("b", ColumnType::Int)]);

        let udt = deserialize::<Udt<'_>>(&typ, &udt_bytes).unwrap();
        assert_eq!(
            udt,
            Udt {
                a: "The quick brown fox",
                x: String::new(),
                b: Some(42),
            }
        );
    }

    // The last UDT field is missing in serialized form - it should treat
    // as if there were null at the end
    {
        let udt_bytes = UdtSerializer::new()
            .field("The quick brown fox".as_bytes())
            .finalize();
        let typ = udt_def_with_fields([("a", ColumnType::Text), ("b", ColumnType::Int)]);

        let udt = deserialize::<Udt<'_>>(&typ, &udt_bytes).unwrap();
        assert_eq!(
            udt,
            Udt {
                a: "The quick brown fox",
                x: String::new(),
                b: None,
            }
        );
    }

    // An excess field at the end of UDT
    {
        let udt_bytes = UdtSerializer::new()
            .field("The quick brown fox".as_bytes())
            .field(&42_i32.to_be_bytes())
            .field(&(true as i8).to_be_bytes())
            .finalize();
        let typ = udt_def_with_fields([
            ("a", ColumnType::Text),
            ("b", ColumnType::Int),
            ("d", ColumnType::Boolean),
        ]);
        let udt = deserialize::<Udt<'_>>(&typ, &udt_bytes).unwrap();
        assert_eq!(
            udt,
            Udt {
                a: "The quick brown fox",
                x: String::new(),
                b: Some(42),
            }
        );
    }

    // An excess field at the end of UDT, when such are forbidden
    {
        #[derive(scylla_macros::DeserializeValue, PartialEq, Eq, Debug)]
        #[scylla(crate = "crate", enforce_order, forbid_excess_udt_fields)]
        struct Udt<'a> {
            a: &'a str,
            #[scylla(skip)]
            x: String,
            b: Option<i32>,
        }

        let typ = udt_def_with_fields([
            ("a", ColumnType::Text),
            ("b", ColumnType::Int),
            ("d", ColumnType::Boolean),
        ]);

        Udt::type_check(&typ).unwrap_err();
    }

    // UDT fields switched - will not work
    {
        let typ = udt_def_with_fields([("b", ColumnType::Int), ("a", ColumnType::Text)]);
        Udt::type_check(&typ).unwrap_err();
    }

    // Wrong column type
    {
        let typ = udt_def_with_fields([("a", ColumnType::Int), ("b", ColumnType::Int)]);
        Udt::type_check(&typ).unwrap_err();
    }

    // Missing required column
    {
        let typ = udt_def_with_fields([("b", ColumnType::Int)]);
        Udt::type_check(&typ).unwrap_err();
    }

    // Missing non-required column
    {
        let udt_bytes = UdtSerializer::new().field(b"kotmaale").finalize();
        let typ = udt_def_with_fields([("a", ColumnType::Text)]);

        let udt = deserialize::<Udt<'_>>(&typ, &udt_bytes).unwrap();
        assert_eq!(
            udt,
            Udt {
                a: "kotmaale",
                x: String::new(),
                b: None,
            }
        );
    }

    // The first field is null, but `default_when_null` prevents failure.
    {
        let udt_bytes = UdtSerializer::new()
            .null_field()
            .field(&42i32.to_be_bytes())
            .finalize();
        let typ = udt_def_with_fields([("a", ColumnType::Text), ("b", ColumnType::Int)]);

        let udt = deserialize::<Udt<'_>>(&typ, &udt_bytes).unwrap();
        assert_eq!(
            udt,
            Udt {
                a: "",
                x: String::new(),
                b: Some(42),
            }
        );
    }
}

#[test]
fn test_udt_no_name_check() {
    #[derive(scylla_macros::DeserializeValue, PartialEq, Eq, Debug)]
    #[scylla(crate = "crate", enforce_order, skip_name_checks)]
    struct Udt<'a> {
        a: &'a str,
        #[scylla(skip)]
        x: String,
        b: Option<i32>,
    }

    // UDT fields in correct same order
    {
        let udt_bytes = UdtSerializer::new()
            .field("The quick brown fox".as_bytes())
            .field(&42i32.to_be_bytes())
            .finalize();
        let typ = udt_def_with_fields([("a", ColumnType::Text), ("b", ColumnType::Int)]);

        let udt = deserialize::<Udt<'_>>(&typ, &udt_bytes).unwrap();
        assert_eq!(
            udt,
            Udt {
                a: "The quick brown fox",
                x: String::new(),
                b: Some(42),
            }
        );
    }

    // Correct order of UDT fields, but different names - should still succeed
    {
        let udt_bytes = UdtSerializer::new()
            .field("The quick brown fox".as_bytes())
            .field(&42i32.to_be_bytes())
            .finalize();
        let typ = udt_def_with_fields([("k", ColumnType::Text), ("l", ColumnType::Int)]);

        let udt = deserialize::<Udt<'_>>(&typ, &udt_bytes).unwrap();
        assert_eq!(
            udt,
            Udt {
                a: "The quick brown fox",
                x: String::new(),
                b: Some(42),
            }
        );
    }
}

#[test]
fn test_udt_cross_rename_fields() {
    #[derive(scylla_macros::DeserializeValue, PartialEq, Eq, Debug)]
    #[scylla(crate = crate)]
    struct TestUdt {
        #[scylla(rename = "b")]
        a: i32,
        #[scylla(rename = "a")]
        b: String,
    }

    // UDT fields switched - should still work.
    {
        let udt_bytes = UdtSerializer::new()
            .field("The quick brown fox".as_bytes())
            .field(&42_i32.to_be_bytes())
            .finalize();
        let typ = udt_def_with_fields([("a", ColumnType::Text), ("b", ColumnType::Int)]);

        let udt = deserialize::<TestUdt>(&typ, &udt_bytes).unwrap();
        assert_eq!(
            udt,
            TestUdt {
                a: 42,
                b: "The quick brown fox".to_owned(),
            }
        );
    }
}

#[test]
fn test_custom_type_parser() {
    #[derive(Default, Debug, PartialEq, Eq)]
    struct SwappedPair<A, B>(B, A);
    impl<'frame, A, B> DeserializeValue<'frame> for SwappedPair<A, B>
    where
        A: DeserializeValue<'frame>,
        B: DeserializeValue<'frame>,
    {
        fn type_check(typ: &ColumnType) -> Result<(), TypeCheckError> {
            <(B, A) as DeserializeValue<'frame>>::type_check(typ)
        }

        fn deserialize(
            typ: &'frame ColumnType,
            v: Option<FrameSlice<'frame>>,
        ) -> Result<Self, DeserializationError> {
            <(B, A) as DeserializeValue<'frame>>::deserialize(typ, v).map(|(b, a)| Self(b, a))
        }
    }

    let mut tuple_contents = BytesMut::new();
    append_bytes(&mut tuple_contents, "foo".as_bytes());
    append_bytes(&mut tuple_contents, &42i32.to_be_bytes());
    let tuple = make_bytes(&tuple_contents);

    let typ = ColumnType::Tuple(vec![ColumnType::Ascii, ColumnType::Int]);

    let tup = deserialize::<SwappedPair<i32, &str>>(&typ, &tuple).unwrap();
    assert_eq!(tup, SwappedPair("foo", 42));
}

fn deserialize<'frame, T>(
    typ: &'frame ColumnType,
    bytes: &'frame Bytes,
) -> Result<T, DeserializationError>
where
    T: DeserializeValue<'frame>,
{
    <T as DeserializeValue<'frame>>::type_check(typ)
        .map_err(|typecheck_err| DeserializationError(typecheck_err.0))?;
    let mut frame_slice = FrameSlice::new(bytes);
    let value = frame_slice.read_cql_bytes().map_err(|err| {
        mk_deser_err::<T>(
            typ,
            BuiltinDeserializationErrorKind::RawCqlBytesReadError(err),
        )
    })?;
    <T as DeserializeValue<'frame>>::deserialize(typ, value)
}

fn make_bytes(cell: &[u8]) -> Bytes {
    let mut b = BytesMut::new();
    append_bytes(&mut b, cell);
    b.freeze()
}

fn serialize(typ: &ColumnType, value: &dyn SerializeValue) -> Bytes {
    let mut bytes = Bytes::new();
    serialize_to_buf(typ, value, &mut bytes);
    bytes
}

fn serialize_to_buf(typ: &ColumnType, value: &dyn SerializeValue, buf: &mut Bytes) {
    let mut v = Vec::new();
    let writer = CellWriter::new(&mut v);
    value.serialize(typ, writer).unwrap();
    *buf = v.into();
}

fn append_bytes(b: &mut impl BufMut, cell: &[u8]) {
    b.put_i32(cell.len() as i32);
    b.put_slice(cell);
}

fn make_null() -> Bytes {
    let mut b = BytesMut::new();
    append_null(&mut b);
    b.freeze()
}

fn append_null(b: &mut impl BufMut) {
    b.put_i32(-1);
}

fn assert_ser_de_identity<'f, T: SerializeValue + DeserializeValue<'f> + PartialEq + Debug>(
    typ: &'f ColumnType,
    v: &'f T,
    buf: &'f mut Bytes, // `buf` must be passed as a reference from outside, because otherwise
                        // we cannot specify the lifetime for DeserializeValue.
) {
    serialize_to_buf(typ, v, buf);
    let deserialized = deserialize::<T>(typ, buf).unwrap();
    assert_eq!(&deserialized, v);
}

/* Errors checks */

#[track_caller]
pub(crate) fn get_typeck_err_inner<'a>(
    err: &'a (dyn std::error::Error + 'static),
) -> &'a BuiltinTypeCheckError {
    match err.downcast_ref() {
        Some(err) => err,
        None => panic!("not a BuiltinTypeCheckError: {:?}", err),
    }
}

#[track_caller]
pub(crate) fn get_typeck_err(err: &DeserializationError) -> &BuiltinTypeCheckError {
    get_typeck_err_inner(err.0.as_ref())
}

#[track_caller]
pub(crate) fn get_deser_err(err: &DeserializationError) -> &BuiltinDeserializationError {
    match err.0.downcast_ref() {
        Some(err) => err,
        None => panic!("not a BuiltinDeserializationError: {:?}", err),
    }
}

macro_rules! assert_given_error {
    ($get_err:ident, $bytes:expr, $DestT:ty, $cql_typ:expr, $kind:pat) => {
        let cql_typ = $cql_typ.clone();
        let err = deserialize::<$DestT>(&cql_typ, $bytes).unwrap_err();
        let err = $get_err(&err);
        assert_eq!(err.rust_name, std::any::type_name::<$DestT>());
        assert_eq!(err.cql_type, cql_typ);
        assert_matches::assert_matches!(err.kind, $kind);
    };
}

macro_rules! assert_type_check_error {
    ($bytes:expr, $DestT:ty, $cql_typ:expr, $kind:pat) => {
        assert_given_error!(get_typeck_err, $bytes, $DestT, $cql_typ, $kind);
    };
}

macro_rules! assert_deser_error {
    ($bytes:expr, $DestT:ty, $cql_typ:expr, $kind:pat) => {
        assert_given_error!(get_deser_err, $bytes, $DestT, $cql_typ, $kind);
    };
}

#[test]
fn test_native_errors() {
    // Simple type mismatch
    {
        let v = 123_i32;
        let bytes = serialize(&ColumnType::Int, &v);

        // Incompatible types render type check error.
        assert_type_check_error!(
            &bytes,
            f64,
            ColumnType::Int,
            super::BuiltinTypeCheckErrorKind::MismatchedType {
                expected: &[ColumnType::Double],
            }
        );

        // ColumnType is said to be Double (8 bytes expected), but in reality the serialized form has 4 bytes only.
        assert_deser_error!(
            &bytes,
            f64,
            ColumnType::Double,
            BuiltinDeserializationErrorKind::ByteLengthMismatch {
                expected: 8,
                got: 4,
            }
        );

        // ColumnType is said to be Float, but in reality Int was serialized.
        // As these types have the same size, though, and every binary number in [0, 2^32] is a valid
        // value for both of them, this always succeeds.
        {
            deserialize::<f32>(&ColumnType::Float, &bytes).unwrap();
        }
    }

    // str (and also Uuid) are interesting because they accept two types.
    {
        let v = "Ala ma kota";
        let bytes = serialize(&ColumnType::Ascii, &v);

        assert_type_check_error!(
            &bytes,
            &str,
            ColumnType::Double,
            BuiltinTypeCheckErrorKind::MismatchedType {
                expected: &[ColumnType::Ascii, ColumnType::Text],
            }
        );

        // ColumnType is said to be BigInt (8 bytes expected), but in reality the serialized form
        // (the string) has 11 bytes.
        assert_deser_error!(
            &bytes,
            i64,
            ColumnType::BigInt,
            BuiltinDeserializationErrorKind::ByteLengthMismatch {
                expected: 8,
                got: 11, // str len
            }
        );
    }
    {
        // -126 is not a valid ASCII nor UTF-8 byte.
        let v = -126_i8;
        let bytes = serialize(&ColumnType::TinyInt, &v);

        assert_deser_error!(
            &bytes,
            &str,
            ColumnType::Ascii,
            BuiltinDeserializationErrorKind::ExpectedAscii
        );

        assert_deser_error!(
            &bytes,
            &str,
            ColumnType::Text,
            BuiltinDeserializationErrorKind::InvalidUtf8(_)
        );
    }
}

#[test]
fn test_set_or_list_errors() {
    // Not a set or list
    {
        assert_type_check_error!(
            &Bytes::new(),
            Vec<i64>,
            ColumnType::Float,
            BuiltinTypeCheckErrorKind::SetOrListError(SetOrListTypeCheckErrorKind::NotSetOrList)
        );

        // Type check of Rust set against CQL list must fail, because it would be lossy.
        assert_type_check_error!(
            &Bytes::new(),
            BTreeSet<i32>,
            ColumnType::List(Box::new(ColumnType::Int)),
            BuiltinTypeCheckErrorKind::SetOrListError(SetOrListTypeCheckErrorKind::NotSet)
        );
    }

    // Bad element type
    {
        assert_type_check_error!(
            &Bytes::new(),
            Vec<i64>,
            ColumnType::List(Box::new(ColumnType::Ascii)),
            BuiltinTypeCheckErrorKind::SetOrListError(
                SetOrListTypeCheckErrorKind::ElementTypeCheckFailed(_)
            )
        );

        let err = deserialize::<Vec<i64>>(
            &ColumnType::List(Box::new(ColumnType::Varint)),
            &Bytes::new(),
        )
        .unwrap_err();
        let err = get_typeck_err(&err);
        assert_eq!(err.rust_name, std::any::type_name::<Vec<i64>>());
        assert_eq!(err.cql_type, ColumnType::List(Box::new(ColumnType::Varint)),);
        let BuiltinTypeCheckErrorKind::SetOrListError(
            SetOrListTypeCheckErrorKind::ElementTypeCheckFailed(ref err),
        ) = err.kind
        else {
            panic!("unexpected error kind: {}", err.kind)
        };
        let err = get_typeck_err_inner(err.0.as_ref());
        assert_eq!(err.rust_name, std::any::type_name::<i64>());
        assert_eq!(err.cql_type, ColumnType::Varint);
        assert_matches!(
            err.kind,
            BuiltinTypeCheckErrorKind::MismatchedType {
                expected: &[ColumnType::BigInt, ColumnType::Counter]
            }
        );
    }

    {
        let ser_typ = ColumnType::List(Box::new(ColumnType::Int));
        let v = vec![123_i32];
        let bytes = serialize(&ser_typ, &v);

        {
            let err =
                deserialize::<Vec<i64>>(&ColumnType::List(Box::new(ColumnType::BigInt)), &bytes)
                    .unwrap_err();
            let err = get_deser_err(&err);
            assert_eq!(err.rust_name, std::any::type_name::<Vec<i64>>());
            assert_eq!(err.cql_type, ColumnType::List(Box::new(ColumnType::BigInt)),);
            let BuiltinDeserializationErrorKind::SetOrListError(
                SetOrListDeserializationErrorKind::ElementDeserializationFailed(err),
            ) = &err.kind
            else {
                panic!("unexpected error kind: {}", err.kind)
            };
            let err = get_deser_err(err);
            assert_eq!(err.rust_name, std::any::type_name::<i64>());
            assert_eq!(err.cql_type, ColumnType::BigInt);
            assert_matches!(
                err.kind,
                BuiltinDeserializationErrorKind::ByteLengthMismatch {
                    expected: 8,
                    got: 4
                }
            );
        }
    }
}

#[test]
fn test_map_errors() {
    // Not a map
    {
        let ser_typ = ColumnType::Float;
        let v = 2.12_f32;
        let bytes = serialize(&ser_typ, &v);

        assert_type_check_error!(
            &bytes,
            HashMap<i64, &str>,
            ser_typ,
            BuiltinTypeCheckErrorKind::MapError(
                MapTypeCheckErrorKind::NotMap,
            )
        );
    }

    // Key type mismatch
    {
        let err = deserialize::<HashMap<i64, bool>>(
            &ColumnType::Map(Box::new(ColumnType::Varint), Box::new(ColumnType::Boolean)),
            &Bytes::new(),
        )
        .unwrap_err();
        let err = get_typeck_err(&err);
        assert_eq!(err.rust_name, std::any::type_name::<HashMap<i64, bool>>());
        assert_eq!(
            err.cql_type,
            ColumnType::Map(Box::new(ColumnType::Varint), Box::new(ColumnType::Boolean))
        );
        let BuiltinTypeCheckErrorKind::MapError(MapTypeCheckErrorKind::KeyTypeCheckFailed(ref err)) =
            err.kind
        else {
            panic!("unexpected error kind: {}", err.kind)
        };
        let err = get_typeck_err_inner(err.0.as_ref());
        assert_eq!(err.rust_name, std::any::type_name::<i64>());
        assert_eq!(err.cql_type, ColumnType::Varint);
        assert_matches!(
            err.kind,
            BuiltinTypeCheckErrorKind::MismatchedType {
                expected: &[ColumnType::BigInt, ColumnType::Counter]
            }
        );
    }

    // Value type mismatch
    {
        let err = deserialize::<BTreeMap<i64, &str>>(
            &ColumnType::Map(Box::new(ColumnType::BigInt), Box::new(ColumnType::Boolean)),
            &Bytes::new(),
        )
        .unwrap_err();
        let err = get_typeck_err(&err);
        assert_eq!(err.rust_name, std::any::type_name::<BTreeMap<i64, &str>>());
        assert_eq!(
            err.cql_type,
            ColumnType::Map(Box::new(ColumnType::BigInt), Box::new(ColumnType::Boolean))
        );
        let BuiltinTypeCheckErrorKind::MapError(MapTypeCheckErrorKind::ValueTypeCheckFailed(
            ref err,
        )) = err.kind
        else {
            panic!("unexpected error kind: {}", err.kind)
        };
        let err = get_typeck_err_inner(err.0.as_ref());
        assert_eq!(err.rust_name, std::any::type_name::<&str>());
        assert_eq!(err.cql_type, ColumnType::Boolean);
        assert_matches!(
            err.kind,
            BuiltinTypeCheckErrorKind::MismatchedType {
                expected: &[ColumnType::Ascii, ColumnType::Text]
            }
        );
    }

    // Key length mismatch
    {
        let ser_typ = ColumnType::Map(Box::new(ColumnType::Int), Box::new(ColumnType::Boolean));
        let v = HashMap::from([(42, false), (2137, true)]);
        let bytes = serialize(&ser_typ, &v as &dyn SerializeValue);

        let err = deserialize::<HashMap<i64, bool>>(
            &ColumnType::Map(Box::new(ColumnType::BigInt), Box::new(ColumnType::Boolean)),
            &bytes,
        )
        .unwrap_err();
        let err = get_deser_err(&err);
        assert_eq!(err.rust_name, std::any::type_name::<HashMap<i64, bool>>());
        assert_eq!(
            err.cql_type,
            ColumnType::Map(Box::new(ColumnType::BigInt), Box::new(ColumnType::Boolean))
        );
        let BuiltinDeserializationErrorKind::MapError(
            MapDeserializationErrorKind::KeyDeserializationFailed(err),
        ) = &err.kind
        else {
            panic!("unexpected error kind: {}", err.kind)
        };
        let err = get_deser_err(err);
        assert_eq!(err.rust_name, std::any::type_name::<i64>());
        assert_eq!(err.cql_type, ColumnType::BigInt);
        assert_matches!(
            err.kind,
            BuiltinDeserializationErrorKind::ByteLengthMismatch {
                expected: 8,
                got: 4
            }
        );
    }

    // Value length mismatch
    {
        let ser_typ = ColumnType::Map(Box::new(ColumnType::Int), Box::new(ColumnType::Boolean));
        let v = HashMap::from([(42, false), (2137, true)]);
        let bytes = serialize(&ser_typ, &v as &dyn SerializeValue);

        let err = deserialize::<HashMap<i32, i16>>(
            &ColumnType::Map(Box::new(ColumnType::Int), Box::new(ColumnType::SmallInt)),
            &bytes,
        )
        .unwrap_err();
        let err = get_deser_err(&err);
        assert_eq!(err.rust_name, std::any::type_name::<HashMap<i32, i16>>());
        assert_eq!(
            err.cql_type,
            ColumnType::Map(Box::new(ColumnType::Int), Box::new(ColumnType::SmallInt))
        );
        let BuiltinDeserializationErrorKind::MapError(
            MapDeserializationErrorKind::ValueDeserializationFailed(err),
        ) = &err.kind
        else {
            panic!("unexpected error kind: {}", err.kind)
        };
        let err = get_deser_err(err);
        assert_eq!(err.rust_name, std::any::type_name::<i16>());
        assert_eq!(err.cql_type, ColumnType::SmallInt);
        assert_matches!(
            err.kind,
            BuiltinDeserializationErrorKind::ByteLengthMismatch {
                expected: 2,
                got: 1
            }
        );
    }
}

#[test]
fn test_tuple_errors() {
    // Not a tuple
    {
        assert_type_check_error!(
            &Bytes::new(),
            (i64,),
            ColumnType::BigInt,
            BuiltinTypeCheckErrorKind::TupleError(TupleTypeCheckErrorKind::NotTuple)
        );
    }
    // Wrong element count
    {
        assert_type_check_error!(
            &Bytes::new(),
            (i64,),
            ColumnType::Tuple(vec![]),
            BuiltinTypeCheckErrorKind::TupleError(TupleTypeCheckErrorKind::WrongElementCount {
                rust_type_el_count: 1,
                cql_type_el_count: 0,
            })
        );

        assert_type_check_error!(
            &Bytes::new(),
            (f32,),
            ColumnType::Tuple(vec![ColumnType::Float, ColumnType::Float]),
            BuiltinTypeCheckErrorKind::TupleError(TupleTypeCheckErrorKind::WrongElementCount {
                rust_type_el_count: 1,
                cql_type_el_count: 2,
            })
        );
    }

    // Bad field type
    {
        {
            let err = deserialize::<(i64,)>(
                &ColumnType::Tuple(vec![ColumnType::SmallInt]),
                &Bytes::new(),
            )
            .unwrap_err();
            let err = get_typeck_err(&err);
            assert_eq!(err.rust_name, std::any::type_name::<(i64,)>());
            assert_eq!(err.cql_type, ColumnType::Tuple(vec![ColumnType::SmallInt]));
            let BuiltinTypeCheckErrorKind::TupleError(
                TupleTypeCheckErrorKind::FieldTypeCheckFailed { ref err, position },
            ) = err.kind
            else {
                panic!("unexpected error kind: {}", err.kind)
            };
            assert_eq!(position, 0);
            let err = get_typeck_err_inner(err.0.as_ref());
            assert_eq!(err.rust_name, std::any::type_name::<i64>());
            assert_eq!(err.cql_type, ColumnType::SmallInt);
            assert_matches!(
                err.kind,
                BuiltinTypeCheckErrorKind::MismatchedType {
                    expected: &[ColumnType::BigInt, ColumnType::Counter]
                }
            );
        }
    }

    {
        let ser_typ = ColumnType::Tuple(vec![ColumnType::Int, ColumnType::Float]);
        let v = (123_i32, 123.123_f32);
        let bytes = serialize(&ser_typ, &v);

        {
            let err = deserialize::<(i32, f64)>(
                &ColumnType::Tuple(vec![ColumnType::Int, ColumnType::Double]),
                &bytes,
            )
            .unwrap_err();
            let err = get_deser_err(&err);
            assert_eq!(err.rust_name, std::any::type_name::<(i32, f64)>());
            assert_eq!(
                err.cql_type,
                ColumnType::Tuple(vec![ColumnType::Int, ColumnType::Double])
            );
            let BuiltinDeserializationErrorKind::TupleError(
                TupleDeserializationErrorKind::FieldDeserializationFailed {
                    ref err,
                    position: index,
                },
            ) = err.kind
            else {
                panic!("unexpected error kind: {}", err.kind)
            };
            assert_eq!(index, 1);
            let err = get_deser_err(err);
            assert_eq!(err.rust_name, std::any::type_name::<f64>());
            assert_eq!(err.cql_type, ColumnType::Double);
            assert_matches!(
                err.kind,
                BuiltinDeserializationErrorKind::ByteLengthMismatch {
                    expected: 8,
                    got: 4
                }
            );
        }
    }
}

#[test]
fn test_null_errors() {
    let ser_typ = ColumnType::Map(Box::new(ColumnType::Int), Box::new(ColumnType::Boolean));
    let v = HashMap::from([(42, false), (2137, true)]);
    let bytes = serialize(&ser_typ, &v as &dyn SerializeValue);

    deserialize::<MaybeEmpty<i32>>(&ser_typ, &bytes).unwrap_err();
}

#[test]
fn test_udt_errors() {
    // Loose ordering
    {
        #[derive(scylla_macros::DeserializeValue, PartialEq, Eq, Debug)]
        #[scylla(crate = "crate", forbid_excess_udt_fields)]
        struct Udt<'a> {
            a: &'a str,
            #[scylla(skip)]
            x: String,
            #[scylla(allow_missing)]
            b: Option<i32>,
            #[scylla(default_when_null)]
            c: bool,
        }

        // Type check errors
        {
            // Not UDT
            {
                let typ = ColumnType::Map(Box::new(ColumnType::Ascii), Box::new(ColumnType::Blob));
                let err = Udt::type_check(&typ).unwrap_err();
                let err = get_typeck_err_inner(err.0.as_ref());
                assert_eq!(err.rust_name, std::any::type_name::<Udt>());
                assert_eq!(err.cql_type, typ);
                let BuiltinTypeCheckErrorKind::UdtError(UdtTypeCheckErrorKind::NotUdt) = err.kind
                else {
                    panic!("unexpected error kind: {:?}", err.kind)
                };
            }

            // UDT missing fields
            {
                let typ = udt_def_with_fields([("c", ColumnType::Boolean)]);
                let err = Udt::type_check(&typ).unwrap_err();
                let err = get_typeck_err_inner(err.0.as_ref());
                assert_eq!(err.rust_name, std::any::type_name::<Udt>());
                assert_eq!(err.cql_type, typ);
                let BuiltinTypeCheckErrorKind::UdtError(
                    UdtTypeCheckErrorKind::ValuesMissingForUdtFields {
                        field_names: ref missing_fields,
                    },
                ) = err.kind
                else {
                    panic!("unexpected error kind: {:?}", err.kind)
                };
                assert_eq!(missing_fields.as_slice(), &["a"]);
            }

            // excess fields in UDT
            {
                let typ = udt_def_with_fields([
                    ("d", ColumnType::Boolean),
                    ("a", ColumnType::Text),
                    ("b", ColumnType::Int),
                ]);
                let err = Udt::type_check(&typ).unwrap_err();
                let err = get_typeck_err_inner(err.0.as_ref());
                assert_eq!(err.rust_name, std::any::type_name::<Udt>());
                assert_eq!(err.cql_type, typ);
                let BuiltinTypeCheckErrorKind::UdtError(UdtTypeCheckErrorKind::ExcessFieldInUdt {
                    ref db_field_name,
                }) = err.kind
                else {
                    panic!("unexpected error kind: {:?}", err.kind)
                };
                assert_eq!(db_field_name.as_str(), "d");
            }

            // missing UDT field
            {
                let typ = udt_def_with_fields([("b", ColumnType::Int), ("a", ColumnType::Text)]);
                let err = Udt::type_check(&typ).unwrap_err();
                let err = get_typeck_err_inner(err.0.as_ref());
                assert_eq!(err.rust_name, std::any::type_name::<Udt>());
                assert_eq!(err.cql_type, typ);
                let BuiltinTypeCheckErrorKind::UdtError(
                    UdtTypeCheckErrorKind::ValuesMissingForUdtFields { ref field_names },
                ) = err.kind
                else {
                    panic!("unexpected error kind: {:?}", err.kind)
                };
                assert_eq!(field_names, &["c"]);
            }

            // UDT fields incompatible types - field type check failed
            {
                let typ = udt_def_with_fields([("a", ColumnType::Blob), ("b", ColumnType::Int)]);
                let err = Udt::type_check(&typ).unwrap_err();
                let err = get_typeck_err_inner(err.0.as_ref());
                assert_eq!(err.rust_name, std::any::type_name::<Udt>());
                assert_eq!(err.cql_type, typ);
                let BuiltinTypeCheckErrorKind::UdtError(
                    UdtTypeCheckErrorKind::FieldTypeCheckFailed {
                        ref field_name,
                        ref err,
                    },
                ) = err.kind
                else {
                    panic!("unexpected error kind: {:?}", err.kind)
                };
                assert_eq!(field_name.as_str(), "a");
                let err = get_typeck_err_inner(err.0.as_ref());
                assert_eq!(err.rust_name, std::any::type_name::<&str>());
                assert_eq!(err.cql_type, ColumnType::Blob);
                assert_matches!(
                    err.kind,
                    BuiltinTypeCheckErrorKind::MismatchedType {
                        expected: &[ColumnType::Ascii, ColumnType::Text]
                    }
                );
            }
        }

        // Deserialization errors
        {
            // Got null
            {
                let typ = udt_def_with_fields([
                    ("c", ColumnType::Boolean),
                    ("a", ColumnType::Blob),
                    ("b", ColumnType::Int),
                ]);

                let err = Udt::deserialize(&typ, None).unwrap_err();
                let err = get_deser_err(&err);
                assert_eq!(err.rust_name, std::any::type_name::<Udt>());
                assert_eq!(err.cql_type, typ);
                assert_matches!(err.kind, BuiltinDeserializationErrorKind::ExpectedNonNull);
            }

            // UDT field deserialization failed
            {
                let typ =
                    udt_def_with_fields([("a", ColumnType::Ascii), ("c", ColumnType::Boolean)]);

                let udt_bytes = UdtSerializer::new()
                    .field("alamakota".as_bytes())
                    .field(&42_i16.to_be_bytes())
                    .finalize();

                let err = deserialize::<Udt>(&typ, &udt_bytes).unwrap_err();

                let err = get_deser_err(&err);
                assert_eq!(err.rust_name, std::any::type_name::<Udt>());
                assert_eq!(err.cql_type, typ);
                let BuiltinDeserializationErrorKind::UdtError(
                    UdtDeserializationErrorKind::FieldDeserializationFailed {
                        ref field_name,
                        ref err,
                    },
                ) = err.kind
                else {
                    panic!("unexpected error kind: {:?}", err.kind)
                };
                assert_eq!(field_name.as_str(), "c");
                let err = get_deser_err(err);
                assert_eq!(err.rust_name, std::any::type_name::<bool>());
                assert_eq!(err.cql_type, ColumnType::Boolean);
                assert_matches!(
                    err.kind,
                    BuiltinDeserializationErrorKind::ByteLengthMismatch {
                        expected: 1,
                        got: 2,
                    }
                );
            }
        }
    }

    // Strict ordering
    {
        #[derive(scylla_macros::DeserializeValue, PartialEq, Eq, Debug)]
        #[scylla(crate = "crate", enforce_order, forbid_excess_udt_fields)]
        struct Udt<'a> {
            a: &'a str,
            #[scylla(skip)]
            x: String,
            b: Option<i32>,
            #[scylla(allow_missing)]
            c: bool,
        }

        // Type check errors
        {
            // Not UDT
            {
                let typ = ColumnType::Map(Box::new(ColumnType::Ascii), Box::new(ColumnType::Blob));
                let err = Udt::type_check(&typ).unwrap_err();
                let err = get_typeck_err_inner(err.0.as_ref());
                assert_eq!(err.rust_name, std::any::type_name::<Udt>());
                assert_eq!(err.cql_type, typ);
                let BuiltinTypeCheckErrorKind::UdtError(UdtTypeCheckErrorKind::NotUdt) = err.kind
                else {
                    panic!("unexpected error kind: {:?}", err.kind)
                };
            }

            // UDT too few fields
            {
                let typ = udt_def_with_fields([("a", ColumnType::Text)]);
                let err = Udt::type_check(&typ).unwrap_err();
                let err = get_typeck_err_inner(err.0.as_ref());
                assert_eq!(err.rust_name, std::any::type_name::<Udt>());
                assert_eq!(err.cql_type, typ);
                let BuiltinTypeCheckErrorKind::UdtError(UdtTypeCheckErrorKind::TooFewFields {
                    ref required_fields,
                    ref present_fields,
                }) = err.kind
                else {
                    panic!("unexpected error kind: {:?}", err.kind)
                };
                assert_eq!(required_fields.as_slice(), &["a", "b"]);
                assert_eq!(present_fields.as_slice(), &["a".to_string()]);
            }

            // excess fields in UDT
            {
                let typ = udt_def_with_fields([
                    ("a", ColumnType::Text),
                    ("b", ColumnType::Int),
                    ("d", ColumnType::Boolean),
                ]);
                let err = Udt::type_check(&typ).unwrap_err();
                let err = get_typeck_err_inner(err.0.as_ref());
                assert_eq!(err.rust_name, std::any::type_name::<Udt>());
                assert_eq!(err.cql_type, typ);
                let BuiltinTypeCheckErrorKind::UdtError(UdtTypeCheckErrorKind::ExcessFieldInUdt {
                    ref db_field_name,
                }) = err.kind
                else {
                    panic!("unexpected error kind: {:?}", err.kind)
                };
                assert_eq!(db_field_name.as_str(), "d");
            }

            // UDT fields switched - field name mismatch
            {
                let typ = udt_def_with_fields([("b", ColumnType::Int), ("a", ColumnType::Text)]);
                let err = Udt::type_check(&typ).unwrap_err();
                let err = get_typeck_err_inner(err.0.as_ref());
                assert_eq!(err.rust_name, std::any::type_name::<Udt>());
                assert_eq!(err.cql_type, typ);
                let BuiltinTypeCheckErrorKind::UdtError(UdtTypeCheckErrorKind::FieldNameMismatch {
                    position,
                    ref rust_field_name,
                    ref db_field_name,
                }) = err.kind
                else {
                    panic!("unexpected error kind: {:?}", err.kind)
                };
                assert_eq!(position, 0);
                assert_eq!(rust_field_name.as_str(), "a".to_owned());
                assert_eq!(db_field_name.as_str(), "b".to_owned());
            }

            // UDT fields incompatible types - field type check failed
            {
                let typ = udt_def_with_fields([("a", ColumnType::Blob), ("b", ColumnType::Int)]);
                let err = Udt::type_check(&typ).unwrap_err();
                let err = get_typeck_err_inner(err.0.as_ref());
                assert_eq!(err.rust_name, std::any::type_name::<Udt>());
                assert_eq!(err.cql_type, typ);
                let BuiltinTypeCheckErrorKind::UdtError(
                    UdtTypeCheckErrorKind::FieldTypeCheckFailed {
                        ref field_name,
                        ref err,
                    },
                ) = err.kind
                else {
                    panic!("unexpected error kind: {:?}", err.kind)
                };
                assert_eq!(field_name.as_str(), "a");
                let err = get_typeck_err_inner(err.0.as_ref());
                assert_eq!(err.rust_name, std::any::type_name::<&str>());
                assert_eq!(err.cql_type, ColumnType::Blob);
                assert_matches!(
                    err.kind,
                    BuiltinTypeCheckErrorKind::MismatchedType {
                        expected: &[ColumnType::Ascii, ColumnType::Text]
                    }
                );
            }
        }

        // Deserialization errors
        {
            // Got null
            {
                let typ = udt_def_with_fields([
                    ("a", ColumnType::Text),
                    ("b", ColumnType::Int),
                    ("c", ColumnType::Boolean),
                ]);

                let err = Udt::deserialize(&typ, None).unwrap_err();
                let err = get_deser_err(&err);
                assert_eq!(err.rust_name, std::any::type_name::<Udt>());
                assert_eq!(err.cql_type, typ);
                assert_matches!(err.kind, BuiltinDeserializationErrorKind::ExpectedNonNull);
            }

            // Bad field format
            {
                let typ = udt_def_with_fields([
                    ("a", ColumnType::Text),
                    ("b", ColumnType::Int),
                    ("c", ColumnType::Boolean),
                ]);

                let udt_bytes = UdtSerializer::new()
                    .field(b"alamakota")
                    .field(&42_i64.to_be_bytes())
                    .field(&[true as u8])
                    .finalize();

                let udt_bytes_too_short = udt_bytes.slice(..udt_bytes.len() - 1);
                assert!(udt_bytes.len() > udt_bytes_too_short.len());

                let err = deserialize::<Udt>(&typ, &udt_bytes_too_short).unwrap_err();

                let err = get_deser_err(&err);
                assert_eq!(err.rust_name, std::any::type_name::<Udt>());
                assert_eq!(err.cql_type, typ);
                let BuiltinDeserializationErrorKind::RawCqlBytesReadError(_) = err.kind else {
                    panic!("unexpected error kind: {:?}", err.kind)
                };
            }

            // UDT field deserialization failed
            {
                let typ = udt_def_with_fields([
                    ("a", ColumnType::Text),
                    ("b", ColumnType::Int),
                    ("c", ColumnType::Boolean),
                ]);

                let udt_bytes = UdtSerializer::new()
                    .field(b"alamakota")
                    .field(&42_i64.to_be_bytes())
                    .field(&[true as u8])
                    .finalize();

                let err = deserialize::<Udt>(&typ, &udt_bytes).unwrap_err();

                let err = get_deser_err(&err);
                assert_eq!(err.rust_name, std::any::type_name::<Udt>());
                assert_eq!(err.cql_type, typ);
                let BuiltinDeserializationErrorKind::UdtError(
                    UdtDeserializationErrorKind::FieldDeserializationFailed {
                        ref field_name,
                        ref err,
                    },
                ) = err.kind
                else {
                    panic!("unexpected error kind: {:?}", err.kind)
                };
                assert_eq!(field_name.as_str(), "b");
                let err = get_deser_err(err);
                assert_eq!(err.rust_name, std::any::type_name::<i32>());
                assert_eq!(err.cql_type, ColumnType::Int);
                assert_matches!(
                    err.kind,
                    BuiltinDeserializationErrorKind::ByteLengthMismatch {
                        expected: 4,
                        got: 8,
                    }
                );
            }
        }
    }
}
