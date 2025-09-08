use assert_matches::assert_matches;
use bytes::{BufMut, Bytes, BytesMut};
use scylla_macros::DeserializeValue;
use uuid::Uuid;

use std::borrow::Cow;
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::fmt::Debug;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};
use std::str::FromStr;
use std::sync::Arc;

use crate::deserialize::value::{TupleDeserializationErrorKind, TupleTypeCheckErrorKind};
use crate::deserialize::{DeserializationError, FrameSlice, TypeCheckError};
use crate::frame::frame_errors::CustomTypeParseError;
use crate::frame::response::custom_type_parser::CustomTypeParser;
use crate::frame::response::result::NativeType::*;
use crate::frame::response::result::{CollectionType, ColumnType, NativeType, UserDefinedType};
use crate::serialize::value::SerializeValue;
use crate::serialize::CellWriter;
use crate::utils::parse::ParseErrorCause;
use crate::value::{
    Counter, CqlDate, CqlDecimal, CqlDecimalBorrowed, CqlDuration, CqlTime, CqlTimestamp,
    CqlTimeuuid, CqlValue, CqlVarint, CqlVarintBorrowed,
};

use super::{
    mk_deser_err, BuiltinDeserializationError, BuiltinDeserializationErrorKind,
    BuiltinTypeCheckError, BuiltinTypeCheckErrorKind, DeserializeValue, ListlikeIterator,
    MapDeserializationErrorKind, MapIterator, MapTypeCheckErrorKind, MaybeEmpty,
    SetOrListDeserializationErrorKind, SetOrListTypeCheckErrorKind, UdtDeserializationErrorKind,
    UdtTypeCheckErrorKind,
};

#[test]
fn test_custom_cassandra_type_parser() {
    let tests = vec![
    (
        "org.apache.cassandra.db.marshal.VectorType(org.apache.cassandra.db.marshal.Int32Type, 5)",
        ColumnType::Vector {
            typ: Box::new(ColumnType::Native(NativeType::Int)),
            dimensions: 5,
        },
    ),
    (  "636f6c756d6e:org.apache.cassandra.db.marshal.ListType(org.apache.cassandra.db.marshal.Int32Type)",
        ColumnType::Collection {
            frozen: false,
            typ: CollectionType::List(Box::new(ColumnType::Native(NativeType::Int))),
        }
    ),
    (
        "org.apache.cassandra.db.marshal.SetType(org.apache.cassandra.db.marshal.Int32Type)",
        ColumnType::Collection {
            frozen: false,
            typ: CollectionType::Set(Box::new(ColumnType::Native(NativeType::Int))),
        },
    ),
    (
        "org.apache.cassandra.db.marshal.MapType(org.apache.cassandra.db.marshal.Int32Type,org.apache.cassandra.db.marshal.Int32Type)",
        ColumnType::Collection {
            frozen: false,
            typ: CollectionType::Map(
                Box::new(ColumnType::Native(NativeType::Int)),
                Box::new(ColumnType::Native(NativeType::Int)),
            ),
        },
    ),
    (
        "org.apache.cassandra.db.marshal.DurationType",
        ColumnType::Native(NativeType::Duration)
    ),
    (
        "org.apache.cassandra.db.marshal.TupleType(org.apache.cassandra.db.marshal.Int32Type,org.apache.cassandra.db.marshal.Int32Type)",
        ColumnType::Tuple(vec![
            ColumnType::Native(NativeType::Int),
            ColumnType::Native(NativeType::Int)
        ]),
    ),
    (
        "org.apache.cassandra.db.marshal.UserType(keyspace1,61646472657373,737472656574:org.apache.cassandra.db.marshal.UTF8Type,63697479:org.apache.cassandra.db.marshal.UTF8Type,7a6970:org.apache.cassandra.db.marshal.Int32Type)",
        ColumnType::UserDefinedType {
            frozen: false,
            definition: Arc::new(UserDefinedType {
                name: "address".into(),
                keyspace: "keyspace1".into(),
                field_types: vec![
                    ("street".into(), ColumnType::Native(NativeType::Text)),
                    ("city".into(), ColumnType::Native(NativeType::Text)),
                    ("zip".into(), ColumnType::Native(NativeType::Int))
                ]
            }),
        }
    ),
    ( "org.apache.cassandra.db.marshal.MapType(org.apache.cassandra.db.marshal.Int32Type, org.apache.cassandra.db.marshal.TupleType(org.apache.cassandra.db.marshal.Int32Type,org.apache.cassandra.db.marshal.Int32Type))",
        ColumnType::Collection {
            frozen: false,
            typ: CollectionType::Map(
                Box::new(ColumnType::Native(NativeType::Int)),
                Box::new(ColumnType::Tuple(vec![
                    ColumnType::Native(NativeType::Int),
                    ColumnType::Native(NativeType::Int)
                ]))
            )
        }
    )
    ];

    for (input, expected) in tests {
        assert_eq!(CustomTypeParser::parse(input), Ok(expected));
    }
}

#[test]
fn test_custom_cassandra_type_parser_errors() {
    assert_eq!(
        CustomTypeParser::parse("org.apache.cassandra.db.marshal.RandomType"),
        Err(CustomTypeParseError::UnknownSimpleCustomTypeName(
            "RandomType".to_string()
        ))
    );

    assert_eq!(
        CustomTypeParser::parse("org.apache.cassandra.db.marshal.RandomType()"),
        Err(CustomTypeParseError::UnknownComplexCustomTypeName(
            "RandomType".to_string()
        ))
    );

    assert_eq!(
        CustomTypeParser::parse(
            "org.apache.cassandra.db.marshal.MapType(org.apache.cassandra.db.marshal.Int32Type)"
        ),
        Err(CustomTypeParseError::InvalidParameterCount {
            actual: 1,
            expected: 2
        })
    );

    assert_eq!(
        CustomTypeParser::parse("org.apache.cassandra.db.marshal.UserType(keyspace1,gg,org.apache.cassandra.db.marshal.UTF8Type,org.apache.cassandra.db.marshal.UTF8Type,org.apache.cassandra.db.marshal.Int32Type)"),
        Err(CustomTypeParseError::BadHexString("gg".to_string()))
    );

    assert_eq!(
        CustomTypeParser::parse("org.apache.cassandra.db.marshal.UserType(keyspace1,ff,org.apache.cassandra.db.marshal.UTF8Type,org.apache.cassandra.db.marshal.UTF8Type,org.apache.cassandra.db.marshal.Int32Type)"),
        Err(CustomTypeParseError::InvalidUtf8(vec![0xff]))
    );

    assert_eq!(
        CustomTypeParser::parse(
            "org.apache.cassandra.db.marshal.TupleType(org.apache.cassandra.db.marshal.Int32Type"
        ),
        Err(CustomTypeParseError::UnexpectedEndOfInput)
    );

    assert_eq!(
        CustomTypeParser::parse("org.apache.cassandra.db.marshal.UserType(keyspace1,61646472657373,737472656574#org.apache.cassandra.db.marshal.UTF8Type)"),
        Err(CustomTypeParseError::UnexpectedCharacter('#', ':'))
    );

    assert_eq!(
        CustomTypeParser::parse("org.apache.cassandra.db.marshal.VectorType(org.apache.cassandra.db.marshal.Int32Type, asdf)"),
        Err(CustomTypeParseError::IntegerParseError(ParseErrorCause::Other("Expected 16-bit unsigned integer")))
    )
}

#[test]
fn test_deserialize_bytes() {
    const ORIGINAL_BYTES: &[u8] = &[1, 5, 2, 4, 3];

    let bytes = make_bytes(ORIGINAL_BYTES);

    let decoded_slice =
        deserialize::<&[u8]>(&ColumnType::Native(NativeType::Blob), &bytes).unwrap();
    let decoded_vec =
        deserialize::<Vec<u8>>(&ColumnType::Native(NativeType::Blob), &bytes).unwrap();
    let decoded_bytes =
        deserialize::<Bytes>(&ColumnType::Native(NativeType::Blob), &bytes).unwrap();

    assert_eq!(decoded_slice, ORIGINAL_BYTES);
    assert_eq!(decoded_vec, ORIGINAL_BYTES);
    assert_eq!(decoded_bytes, ORIGINAL_BYTES);

    // ser/de identity

    // Nonempty blob
    assert_ser_de_identity(
        &ColumnType::Native(NativeType::Blob),
        &ORIGINAL_BYTES,
        &mut Bytes::new(),
    );

    // Empty blob
    assert_ser_de_identity(
        &ColumnType::Native(NativeType::Blob),
        &(&[] as &[u8]),
        &mut Bytes::new(),
    );
}

#[test]
fn test_deserialize_vector() {
    // ser/de identity

    // native types

    assert_ser_de_identity(
        &ColumnType::Vector {
            typ: Box::new(ColumnType::Native(NativeType::Ascii)),
            dimensions: 3,
        },
        &vec!["ala", "ma", "kota"],
        &mut Bytes::new(),
    );
    assert_ser_de_identity(
        &ColumnType::Vector {
            typ: Box::new(ColumnType::Native(NativeType::Boolean)),
            dimensions: 2,
        },
        &vec![true, false],
        &mut Bytes::new(),
    );
    assert_ser_de_identity(
        &ColumnType::Vector {
            typ: Box::new(ColumnType::Native(NativeType::Blob)),
            dimensions: 2,
        },
        &vec![vec![1_u8, 2_u8], vec![3_u8, 4_u8]],
        &mut Bytes::new(),
    );
    assert_ser_de_identity(
        &ColumnType::Vector {
            typ: Box::new(ColumnType::Native(NativeType::Counter)),
            dimensions: 2,
        },
        &vec![Counter(1234), Counter(5678)],
        &mut Bytes::new(),
    );
    assert_ser_de_identity(
        &ColumnType::Vector {
            typ: Box::new(ColumnType::Native(NativeType::Date)),
            dimensions: 2,
        },
        &vec![CqlDate(1234), CqlDate(5678)],
        &mut Bytes::new(),
    );
    assert_ser_de_identity(
        &ColumnType::Vector {
            typ: Box::new(ColumnType::Native(NativeType::Decimal)),
            dimensions: 2,
        },
        &vec![
            CqlDecimal::from_signed_be_bytes_slice_and_exponent(b"123", 42),
            CqlDecimal::from_signed_be_bytes_slice_and_exponent(b"123", 42),
        ],
        &mut Bytes::new(),
    );
    assert_ser_de_identity(
        &ColumnType::Vector {
            typ: Box::new(ColumnType::Native(NativeType::Double)),
            dimensions: 2,
        },
        &vec![0.1234, 0.5678],
        &mut Bytes::new(),
    );
    assert_ser_de_identity(
        &ColumnType::Vector {
            typ: Box::new(ColumnType::Native(NativeType::Duration)),
            dimensions: 2,
        },
        &vec![
            CqlDuration {
                months: 1,
                days: 2,
                nanoseconds: 3,
            },
            CqlDuration {
                months: 1,
                days: 2,
                nanoseconds: 3,
            },
        ],
        &mut Bytes::new(),
    );
    assert_ser_de_identity(
        &ColumnType::Vector {
            typ: Box::new(ColumnType::Native(NativeType::Float)),
            dimensions: 2,
        },
        &vec![0.1234_f32, 0.5678_f32],
        &mut Bytes::new(),
    );
    assert_ser_de_identity(
        &ColumnType::Vector {
            typ: Box::new(ColumnType::Native(NativeType::Int)),
            dimensions: 2,
        },
        &vec![1, 2],
        &mut Bytes::new(),
    );
    assert_ser_de_identity(
        &ColumnType::Vector {
            typ: Box::new(ColumnType::Native(NativeType::BigInt)),
            dimensions: 2,
        },
        &vec![1_i64, 2_i64],
        &mut Bytes::new(),
    );
    assert_ser_de_identity(
        &ColumnType::Vector {
            typ: Box::new(ColumnType::Native(NativeType::Text)),
            dimensions: 2,
        },
        &vec!["ala", "ma"],
        &mut Bytes::new(),
    );
    assert_ser_de_identity(
        &ColumnType::Vector {
            typ: Box::new(ColumnType::Native(NativeType::Timestamp)),
            dimensions: 2,
        },
        &vec![CqlTimestamp(1234), CqlTimestamp(5678)],
        &mut Bytes::new(),
    );
    assert_ser_de_identity(
        &ColumnType::Vector {
            typ: Box::new(ColumnType::Native(NativeType::Inet)),
            dimensions: 2,
        },
        &vec![
            IpAddr::V4(Ipv4Addr::BROADCAST),
            IpAddr::V6(Ipv6Addr::LOCALHOST),
        ],
        &mut Bytes::new(),
    );
    assert_ser_de_identity(
        &ColumnType::Vector {
            typ: Box::new(ColumnType::Native(NativeType::SmallInt)),
            dimensions: 2,
        },
        &vec![1_i16, 2_i16],
        &mut Bytes::new(),
    );
    assert_ser_de_identity(
        &ColumnType::Vector {
            typ: Box::new(ColumnType::Native(NativeType::TinyInt)),
            dimensions: 2,
        },
        &vec![1_i8, 2_i8],
        &mut Bytes::new(),
    );
    assert_ser_de_identity(
        &ColumnType::Vector {
            typ: Box::new(ColumnType::Native(NativeType::Time)),
            dimensions: 2,
        },
        &vec![CqlTime(1234), CqlTime(5678)],
        &mut Bytes::new(),
    );
    assert_ser_de_identity(
        &ColumnType::Vector {
            typ: Box::new(ColumnType::Native(NativeType::Timeuuid)),
            dimensions: 2,
        },
        &vec![CqlTimeuuid::from_u128(123), CqlTimeuuid::from_u128(456)],
        &mut Bytes::new(),
    );
    assert_ser_de_identity(
        &ColumnType::Vector {
            typ: Box::new(ColumnType::Native(NativeType::Uuid)),
            dimensions: 2,
        },
        &vec![Uuid::from_u128(123), Uuid::from_u128(456)],
        &mut Bytes::new(),
    );
    assert_ser_de_identity(
        &ColumnType::Vector {
            typ: Box::new(ColumnType::Native(NativeType::Varint)),
            dimensions: 2,
        },
        &vec![
            CqlVarint::from_signed_bytes_be(vec![1, 2]),
            CqlVarint::from_signed_bytes_be(vec![3, 4]),
        ],
        &mut Bytes::new(),
    );

    // collection types

    assert_ser_de_identity(
        &ColumnType::Vector {
            typ: Box::new(ColumnType::Collection {
                frozen: false,
                typ: CollectionType::List(Box::new(ColumnType::Native(NativeType::Int))),
            }),
            dimensions: 2,
        },
        &vec![vec![1, 2], vec![3, 4]],
        &mut Bytes::new(),
    );

    assert_ser_de_identity(
        &ColumnType::Vector {
            typ: Box::new(ColumnType::Collection {
                frozen: false,
                typ: CollectionType::Set(Box::new(ColumnType::Native(NativeType::Int))),
            }),
            dimensions: 2,
        },
        &vec![vec![1, 2], vec![3, 4]],
        &mut Bytes::new(),
    );
    assert_ser_de_identity(
        &ColumnType::Vector {
            typ: Box::new(ColumnType::Collection {
                frozen: false,
                typ: CollectionType::Map(
                    Box::new(ColumnType::Native(NativeType::Int)),
                    Box::new(ColumnType::Native(NativeType::Int)),
                ),
            }),
            dimensions: 2,
        },
        &vec![
            BTreeMap::from_iter(vec![(1, 2), (3, 4)]),
            BTreeMap::from_iter(vec![(5, 6), (7, 8)]),
        ],
        &mut Bytes::new(),
    );

    // tuple types

    assert_ser_de_identity(
        &ColumnType::Vector {
            typ: Box::new(ColumnType::Tuple(vec![
                ColumnType::Native(NativeType::Int),
                ColumnType::Native(NativeType::Int),
            ])),
            dimensions: 2,
        },
        &vec![(1, 2), (3, 4)],
        &mut Bytes::new(),
    );

    // nested vector

    assert_ser_de_identity(
        &ColumnType::Vector {
            typ: Box::new(ColumnType::Vector {
                typ: Box::new(ColumnType::Native(NativeType::Int)),
                dimensions: 2,
            }),
            dimensions: 2,
        },
        &vec![vec![1, 2], vec![3, 4]],
        &mut Bytes::new(),
    );

    assert_ser_de_identity(
        &ColumnType::Vector {
            typ: Box::new(ColumnType::Collection {
                frozen: false,
                typ: CollectionType::List(Box::new(ColumnType::Vector {
                    typ: Box::new(ColumnType::Native(NativeType::Int)),
                    dimensions: 2,
                })),
            }),
            dimensions: 2,
        },
        &vec![vec![vec![1, 2], vec![3, 4]], vec![vec![5, 6], vec![7, 8]]],
        &mut Bytes::new(),
    );

    //empty vector

    let vec: Vec<bool> = vec![];
    assert_ser_de_identity(
        &ColumnType::Vector {
            typ: Box::new(ColumnType::Native(NativeType::Boolean)),
            dimensions: 0,
        },
        &vec,
        &mut Bytes::new(),
    );

    // deser_cql_value

    let buf: Vec<u8> = vec![0, 0, 0, 1, 0, 0, 0, 2];
    let decoded_vec = super::deser_cql_value(
        &ColumnType::Vector {
            typ: Box::new(ColumnType::Native(Int)),
            dimensions: 2,
        },
        &mut buf.as_slice(),
    )
    .unwrap();
    assert_eq!(
        decoded_vec,
        CqlValue::Vector(vec![CqlValue::Int(1), CqlValue::Int(2)])
    );
}

#[test]
fn test_deserialize_ascii() {
    const ASCII_TEXT: &str = "The quick brown fox jumps over the lazy dog";

    let ascii = make_bytes(ASCII_TEXT.as_bytes());

    for typ in [
        ColumnType::Native(NativeType::Ascii),
        ColumnType::Native(NativeType::Text),
    ]
    .iter()
    {
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
    deserialize::<&str>(&ColumnType::Native(NativeType::Ascii), &unicode).unwrap_err();
    deserialize::<String>(&ColumnType::Native(NativeType::Ascii), &unicode).unwrap_err();

    let decoded_text_str =
        deserialize::<&str>(&ColumnType::Native(NativeType::Text), &unicode).unwrap();
    let decoded_text_string =
        deserialize::<String>(&ColumnType::Native(NativeType::Text), &unicode).unwrap();
    assert_eq!(decoded_text_str, UNICODE_TEXT);
    assert_eq!(decoded_text_string, UNICODE_TEXT);

    // ser/de identity

    assert_ser_de_identity(
        &ColumnType::Native(NativeType::Text),
        &UNICODE_TEXT,
        &mut Bytes::new(),
    );
    assert_ser_de_identity(
        &ColumnType::Native(NativeType::Text),
        &UNICODE_TEXT.to_owned(),
        &mut Bytes::new(),
    );
}

#[test]
fn test_integral() {
    let tinyint = make_bytes(&[0x01]);
    let decoded_tinyint =
        deserialize::<i8>(&ColumnType::Native(NativeType::TinyInt), &tinyint).unwrap();
    assert_eq!(decoded_tinyint, 0x01);

    let smallint = make_bytes(&[0x01, 0x02]);
    let decoded_smallint =
        deserialize::<i16>(&ColumnType::Native(NativeType::SmallInt), &smallint).unwrap();
    assert_eq!(decoded_smallint, 0x0102);

    let int = make_bytes(&[0x01, 0x02, 0x03, 0x04]);
    let decoded_int = deserialize::<i32>(&ColumnType::Native(NativeType::Int), &int).unwrap();
    assert_eq!(decoded_int, 0x01020304);

    let bigint = make_bytes(&[0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08]);
    let decoded_bigint =
        deserialize::<i64>(&ColumnType::Native(NativeType::BigInt), &bigint).unwrap();
    assert_eq!(decoded_bigint, 0x0102030405060708);

    // ser/de identity
    assert_ser_de_identity(
        &ColumnType::Native(NativeType::TinyInt),
        &42_i8,
        &mut Bytes::new(),
    );
    assert_ser_de_identity(
        &ColumnType::Native(NativeType::SmallInt),
        &2137_i16,
        &mut Bytes::new(),
    );
    assert_ser_de_identity(
        &ColumnType::Native(NativeType::Int),
        &21372137_i32,
        &mut Bytes::new(),
    );
    assert_ser_de_identity(
        &ColumnType::Native(NativeType::BigInt),
        &0_i64,
        &mut Bytes::new(),
    );
}

#[test]
fn test_bool() {
    for boolean in [true, false] {
        let boolean_bytes = make_bytes(&[boolean as u8]);
        let decoded_bool =
            deserialize::<bool>(&ColumnType::Native(NativeType::Boolean), &boolean_bytes).unwrap();
        assert_eq!(decoded_bool, boolean);

        // ser/de identity
        assert_ser_de_identity(
            &ColumnType::Native(NativeType::Boolean),
            &boolean,
            &mut Bytes::new(),
        );
    }
}

#[test]
fn test_floating_point() {
    let float = make_bytes(&[63, 0, 0, 0]);
    let decoded_float = deserialize::<f32>(&ColumnType::Native(NativeType::Float), &float).unwrap();
    assert_eq!(decoded_float, 0.5);

    let double = make_bytes(&[64, 0, 0, 0, 0, 0, 0, 0]);
    let decoded_double =
        deserialize::<f64>(&ColumnType::Native(NativeType::Double), &double).unwrap();
    assert_eq!(decoded_double, 2.0);

    // ser/de identity
    assert_ser_de_identity(
        &ColumnType::Native(NativeType::Float),
        &21.37_f32,
        &mut Bytes::new(),
    );
    assert_ser_de_identity(
        &ColumnType::Native(NativeType::Double),
        &2137.2137_f64,
        &mut Bytes::new(),
    );
}

#[test]
fn test_varlen_numbers() {
    // varint
    assert_ser_de_identity(
        &ColumnType::Native(NativeType::Varint),
        &CqlVarint::from_signed_bytes_be_slice(b"Ala ma kota"),
        &mut Bytes::new(),
    );

    assert_ser_de_identity(
        &ColumnType::Native(NativeType::Varint),
        &CqlVarintBorrowed::from_signed_bytes_be_slice(b"Ala ma kota"),
        &mut Bytes::new(),
    );

    #[cfg(feature = "num-bigint-03")]
    assert_ser_de_identity(
        &ColumnType::Native(NativeType::Varint),
        &num_bigint_03::BigInt::from_signed_bytes_be(b"Kot ma Ale"),
        &mut Bytes::new(),
    );

    #[cfg(feature = "num-bigint-04")]
    assert_ser_de_identity(
        &ColumnType::Native(NativeType::Varint),
        &num_bigint_04::BigInt::from_signed_bytes_be(b"Kot ma Ale"),
        &mut Bytes::new(),
    );

    // decimal
    assert_ser_de_identity(
        &ColumnType::Native(NativeType::Decimal),
        &CqlDecimal::from_signed_be_bytes_slice_and_exponent(b"Ala ma kota", 42),
        &mut Bytes::new(),
    );

    assert_ser_de_identity(
        &ColumnType::Native(NativeType::Decimal),
        &CqlDecimalBorrowed::from_signed_be_bytes_slice_and_exponent(b"Ala ma kota", 42),
        &mut Bytes::new(),
    );

    #[cfg(feature = "bigdecimal-04")]
    assert_ser_de_identity(
        &ColumnType::Native(NativeType::Decimal),
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
        &ColumnType::Native(NativeType::Duration),
        &CqlDuration {
            months: 21,
            days: 37,
            nanoseconds: 42,
        },
        &mut Bytes::new(),
    );

    // date
    assert_ser_de_identity(
        &ColumnType::Native(NativeType::Date),
        &CqlDate(0xbeaf),
        &mut Bytes::new(),
    );

    #[cfg(feature = "chrono-04")]
    assert_ser_de_identity(
        &ColumnType::Native(NativeType::Date),
        &chrono_04::NaiveDate::from_yo_opt(1999, 99).unwrap(),
        &mut Bytes::new(),
    );

    #[cfg(feature = "time-03")]
    assert_ser_de_identity(
        &ColumnType::Native(NativeType::Date),
        &time_03::Date::from_ordinal_date(1999, 99).unwrap(),
        &mut Bytes::new(),
    );

    // time
    assert_ser_de_identity(
        &ColumnType::Native(NativeType::Time),
        &CqlTime(0xdeed),
        &mut Bytes::new(),
    );

    #[cfg(feature = "chrono-04")]
    assert_ser_de_identity(
        &ColumnType::Native(NativeType::Time),
        &chrono_04::NaiveTime::from_hms_micro_opt(21, 37, 21, 37).unwrap(),
        &mut Bytes::new(),
    );

    #[cfg(feature = "time-03")]
    assert_ser_de_identity(
        &ColumnType::Native(NativeType::Time),
        &time_03::Time::from_hms_micro(21, 37, 21, 37).unwrap(),
        &mut Bytes::new(),
    );

    // timestamp
    assert_ser_de_identity(
        &ColumnType::Native(NativeType::Timestamp),
        &CqlTimestamp(0xceed),
        &mut Bytes::new(),
    );

    #[cfg(feature = "chrono-04")]
    assert_ser_de_identity(
        &ColumnType::Native(NativeType::Timestamp),
        &chrono_04::DateTime::<chrono_04::Utc>::from_timestamp_millis(0xdead_cafe_deaf).unwrap(),
        &mut Bytes::new(),
    );

    #[cfg(feature = "time-03")]
    assert_ser_de_identity(
        &ColumnType::Native(NativeType::Timestamp),
        &time_03::OffsetDateTime::from_unix_timestamp(0xdead_cafe).unwrap(),
        &mut Bytes::new(),
    );
}

#[test]
fn test_inet() {
    assert_ser_de_identity(
        &ColumnType::Native(NativeType::Inet),
        &IpAddr::V4(Ipv4Addr::BROADCAST),
        &mut Bytes::new(),
    );

    assert_ser_de_identity(
        &ColumnType::Native(NativeType::Inet),
        &IpAddr::V6(Ipv6Addr::LOCALHOST),
        &mut Bytes::new(),
    );
}

#[test]
fn test_uuid() {
    assert_ser_de_identity(
        &ColumnType::Native(NativeType::Uuid),
        &Uuid::from_u128(0xdead_cafe_deaf_feed_beaf_bead),
        &mut Bytes::new(),
    );

    assert_ser_de_identity(
        &ColumnType::Native(NativeType::Timeuuid),
        &CqlTimeuuid::from_u128(0xdead_cafe_deaf_feed_beaf_bead),
        &mut Bytes::new(),
    );
}

#[test]
fn test_null_and_empty() {
    // non-nullable emptiable deserialization, non-empty value
    let int = make_bytes(&[21, 37, 0, 0]);
    let decoded_int =
        deserialize::<MaybeEmpty<i32>>(&ColumnType::Native(NativeType::Int), &int).unwrap();
    assert_eq!(decoded_int, MaybeEmpty::Value((21 << 24) + (37 << 16)));

    // non-nullable emptiable deserialization, empty value
    let int = make_bytes(&[]);
    let decoded_int =
        deserialize::<MaybeEmpty<i32>>(&ColumnType::Native(NativeType::Int), &int).unwrap();
    assert_eq!(decoded_int, MaybeEmpty::Empty);

    // nullable non-emptiable deserialization, non-null value
    let int = make_bytes(&[21, 37, 0, 0]);
    let decoded_int =
        deserialize::<Option<i32>>(&ColumnType::Native(NativeType::Int), &int).unwrap();
    assert_eq!(decoded_int, Some((21 << 24) + (37 << 16)));

    // nullable non-emptiable deserialization, null value
    let int = make_null();
    let decoded_int =
        deserialize::<Option<i32>>(&ColumnType::Native(NativeType::Int), &int).unwrap();
    assert_eq!(decoded_int, None);

    // nullable emptiable deserialization, non-null non-empty value
    let int = make_bytes(&[]);
    let decoded_int =
        deserialize::<Option<MaybeEmpty<i32>>>(&ColumnType::Native(NativeType::Int), &int).unwrap();
    assert_eq!(decoded_int, Some(MaybeEmpty::Empty));

    // ser/de identity
    assert_ser_de_identity(
        &ColumnType::Native(NativeType::Int),
        &Some(12321_i32),
        &mut Bytes::new(),
    );
    assert_ser_de_identity(
        &ColumnType::Native(NativeType::Double),
        &None::<f64>,
        &mut Bytes::new(),
    );
    assert_ser_de_identity(
        &ColumnType::Collection {
            frozen: false,
            typ: CollectionType::Set(Box::new(ColumnType::Native(NativeType::Ascii))),
        },
        &None::<Vec<&str>>,
        &mut Bytes::new(),
    );
}

#[test]
fn test_maybe_empty() {
    let empty = make_bytes(&[]);
    let decoded_empty =
        deserialize::<MaybeEmpty<i8>>(&ColumnType::Native(NativeType::TinyInt), &empty).unwrap();
    assert_eq!(decoded_empty, MaybeEmpty::Empty);

    let non_empty = make_bytes(&[0x01]);
    let decoded_non_empty =
        deserialize::<MaybeEmpty<i8>>(&ColumnType::Native(NativeType::TinyInt), &non_empty)
            .unwrap();
    assert_eq!(decoded_non_empty, MaybeEmpty::Value(0x01));
}

#[test]
fn test_cql_value() {
    assert_ser_de_identity(
        &ColumnType::Native(NativeType::Counter),
        &CqlValue::Counter(Counter(765)),
        &mut Bytes::new(),
    );

    assert_ser_de_identity(
        &ColumnType::Native(NativeType::Timestamp),
        &CqlValue::Timestamp(CqlTimestamp(2136)),
        &mut Bytes::new(),
    );

    assert_ser_de_identity(
        &ColumnType::Native(NativeType::Boolean),
        &CqlValue::Empty,
        &mut Bytes::new(),
    );

    assert_ser_de_identity(
        &ColumnType::Native(NativeType::Text),
        &CqlValue::Text("kremówki".to_owned()),
        &mut Bytes::new(),
    );
    assert_ser_de_identity(
        &ColumnType::Native(NativeType::Ascii),
        &CqlValue::Ascii("kremowy".to_owned()),
        &mut Bytes::new(),
    );

    assert_ser_de_identity(
        &ColumnType::Collection {
            frozen: false,
            typ: CollectionType::Set(Box::new(ColumnType::Native(NativeType::Text))),
        },
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

    let list_typ = ColumnType::Collection {
        frozen: false,
        typ: CollectionType::List(Box::new(ColumnType::Native(NativeType::Ascii))),
    };
    let set_typ = ColumnType::Collection {
        frozen: false,
        typ: CollectionType::Set(Box::new(ColumnType::Native(NativeType::Ascii))),
    };

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
        let list_typ = ColumnType::Collection {
            frozen: false,
            typ: CollectionType::List(Box::new(ColumnType::Native(NativeType::BigInt))),
        };
        let set_typ = ColumnType::Collection {
            frozen: false,
            typ: CollectionType::Set(Box::new(ColumnType::Native(NativeType::BigInt))),
        };
        type CollTyp = i64;

        fn check<'frame, 'metadata, Collection: DeserializeValue<'frame, 'metadata>>(
            typ: &'metadata ColumnType<'metadata>,
        ) {
            <Collection as DeserializeValue<'_, '_>>::type_check(typ).unwrap();
            <Collection as DeserializeValue<'_, '_>>::deserialize(typ, None).unwrap();
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

    let typ = ColumnType::Collection {
        frozen: false,
        typ: CollectionType::Map(
            Box::new(ColumnType::Native(NativeType::Int)),
            Box::new(ColumnType::Native(NativeType::Ascii)),
        ),
    };

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
        let map_typ = ColumnType::Collection {
            frozen: false,
            typ: CollectionType::Map(
                Box::new(ColumnType::Native(NativeType::BigInt)),
                Box::new(ColumnType::Native(NativeType::Ascii)),
            ),
        };
        type KeyTyp = i64;
        type ValueTyp<'s> = &'s str;

        fn check<'frame, 'metadata, Collection: DeserializeValue<'frame, 'metadata>>(
            typ: &'metadata ColumnType<'metadata>,
        ) {
            <Collection as DeserializeValue<'_, '_>>::type_check(typ).unwrap();
            <Collection as DeserializeValue<'_, '_>>::deserialize(typ, None).unwrap();
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

    let typ = ColumnType::Tuple(vec![
        ColumnType::Native(NativeType::Int),
        ColumnType::Native(NativeType::Ascii),
        ColumnType::Native(NativeType::Uuid),
    ]);

    let tup = deserialize::<(i32, &str, Option<Uuid>)>(&typ, &tuple).unwrap();
    assert_eq!(tup, (42, "foo", None));

    // ser/de identity

    // () does not implement SerializeValue, yet it does implement DeserializeValue.
    // assert_ser_de_identity(&ColumnType::Tuple(vec![]), &(), &mut Bytes::new());

    // nonempty, varied tuple
    assert_ser_de_identity(
        &ColumnType::Tuple(vec![
            ColumnType::Collection {
                frozen: false,
                typ: CollectionType::List(Box::new(ColumnType::Native(NativeType::Boolean))),
            },
            ColumnType::Native(NativeType::BigInt),
            ColumnType::Native(NativeType::Uuid),
            ColumnType::Native(NativeType::Inet),
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
            ColumnType::Native(NativeType::Text),
        ])])]),
        &((("",),),),
        &mut Bytes::new(),
    );
}

#[test]
fn test_box() {
    {
        let int = make_bytes(&[0x01, 0x02, 0x03, 0x04]);
        let decoded_int: Box<i32> =
            deserialize::<Box<i32>>(&ColumnType::Native(NativeType::Int), &int).unwrap();
        assert_eq!(*decoded_int, 0x01020304);
    }

    {
        let text_bytes = make_bytes(b"abcd");
        let decoded_int: Box<str> =
            deserialize::<Box<str>>(&ColumnType::Native(NativeType::Text), &text_bytes).unwrap();
        assert_eq!(&*decoded_int, "abcd");
    }
}

#[test]
fn test_arc() {
    {
        let int = make_bytes(&[0x01, 0x02, 0x03, 0x04]);
        let decoded_int: Arc<i32> =
            deserialize::<Arc<i32>>(&ColumnType::Native(NativeType::Int), &int).unwrap();
        assert_eq!(*decoded_int, 0x01020304);
    }

    {
        let text_bytes = make_bytes(b"abcd");
        let decoded_int: Arc<str> =
            deserialize::<Arc<str>>(&ColumnType::Native(NativeType::Text), &text_bytes).unwrap();
        assert_eq!(&*decoded_int, "abcd");
    }
}

pub(crate) fn udt_def_with_fields(
    fields: impl IntoIterator<Item = (impl Into<Cow<'static, str>>, ColumnType<'static>)>,
) -> ColumnType<'static> {
    ColumnType::UserDefinedType {
        frozen: false,
        definition: Arc::new(UserDefinedType {
            name: "udt".into(),
            keyspace: "ks".into(),
            field_types: fields.into_iter().map(|(s, t)| (s.into(), t)).collect(),
        }),
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
#[derive(scylla_macros::DeserializeValue)]
#[scylla(crate = crate)]
struct TestUdtWithNoFieldsUnordered {}

#[derive(scylla_macros::DeserializeValue)]
#[scylla(crate = crate, flavor = "enforce_order")]
struct TestUdtWithNoFieldsOrdered {}

// If deserialize is never called, rust warns that the struct is never constructed.
// We don't want to `expect(dead_code)` on struct definitions, because that could silence
// some warnings that this test is supposed to prevent.
#[expect(unreachable_code, dead_code)]
fn dummy_deserialize_udts() {
    let _ = deserialize::<TestUdtWithNoFieldsUnordered>(todo!(), todo!()).unwrap();
    let _ = deserialize::<TestUdtWithNoFieldsOrdered>(todo!(), todo!()).unwrap();
}

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
            ("a", ColumnType::Native(NativeType::Text)),
            ("b", ColumnType::Native(NativeType::Int)),
            ("c", ColumnType::Native(NativeType::BigInt)),
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
            ("a", ColumnType::Native(NativeType::Text)),
            ("b", ColumnType::Native(NativeType::Int)),
            ("c", ColumnType::Native(NativeType::BigInt)),
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
            ("b", ColumnType::Native(NativeType::Int)),
            ("a", ColumnType::Native(NativeType::Text)),
            ("c", ColumnType::Native(NativeType::BigInt)),
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
            ("d", ColumnType::Native(NativeType::TinyInt)),
            ("b", ColumnType::Native(NativeType::Int)),
            ("a", ColumnType::Native(NativeType::Text)),
            ("c", ColumnType::Native(NativeType::BigInt)),
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
        let typ = udt_def_with_fields([
            ("a", ColumnType::Native(NativeType::Text)),
            ("c", ColumnType::Native(NativeType::BigInt)),
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

    // Wrong column type
    {
        let typ = udt_def_with_fields([("a", ColumnType::Native(NativeType::Text))]);
        Udt::type_check(&typ).unwrap_err();
    }

    // Missing required column
    {
        let typ = udt_def_with_fields([("b", ColumnType::Native(NativeType::Int))]);
        Udt::type_check(&typ).unwrap_err();
    }
}

#[test]
fn test_udt_strict_ordering() {
    #[derive(scylla_macros::DeserializeValue, PartialEq, Eq, Debug)]
    #[scylla(crate = "crate", flavor = "enforce_order")]
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
        let typ = udt_def_with_fields([
            ("a", ColumnType::Native(NativeType::Text)),
            ("b", ColumnType::Native(NativeType::Int)),
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

    // The last UDT field is missing in serialized form - it should treat
    // as if there were null at the end
    {
        let udt_bytes = UdtSerializer::new()
            .field("The quick brown fox".as_bytes())
            .finalize();
        let typ = udt_def_with_fields([
            ("a", ColumnType::Native(NativeType::Text)),
            ("b", ColumnType::Native(NativeType::Int)),
        ]);

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
            ("a", ColumnType::Native(NativeType::Text)),
            ("b", ColumnType::Native(NativeType::Int)),
            ("d", ColumnType::Native(NativeType::Boolean)),
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
        #[scylla(crate = "crate", flavor = "enforce_order", forbid_excess_udt_fields)]
        struct Udt<'a> {
            a: &'a str,
            #[scylla(skip)]
            x: String,
            b: Option<i32>,
        }

        let typ = udt_def_with_fields([
            ("a", ColumnType::Native(NativeType::Text)),
            ("b", ColumnType::Native(NativeType::Int)),
            ("d", ColumnType::Native(NativeType::Boolean)),
        ]);

        Udt::type_check(&typ).unwrap_err();
    }

    // UDT fields switched - will not work
    {
        let typ = udt_def_with_fields([
            ("b", ColumnType::Native(NativeType::Int)),
            ("a", ColumnType::Native(NativeType::Text)),
        ]);
        Udt::type_check(&typ).unwrap_err();
    }

    // Wrong column type
    {
        let typ = udt_def_with_fields([
            ("a", ColumnType::Native(NativeType::Int)),
            ("b", ColumnType::Native(NativeType::Int)),
        ]);
        Udt::type_check(&typ).unwrap_err();
    }

    // Missing required column
    {
        let typ = udt_def_with_fields([("b", ColumnType::Native(NativeType::Int))]);
        Udt::type_check(&typ).unwrap_err();
    }

    // Missing non-required column
    {
        let udt_bytes = UdtSerializer::new().field(b"kotmaale").finalize();
        let typ = udt_def_with_fields([("a", ColumnType::Native(NativeType::Text))]);

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
        let typ = udt_def_with_fields([
            ("a", ColumnType::Native(NativeType::Text)),
            ("b", ColumnType::Native(NativeType::Int)),
        ]);

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
    #[scylla(crate = "crate", flavor = "enforce_order", skip_name_checks)]
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
        let typ = udt_def_with_fields([
            ("a", ColumnType::Native(NativeType::Text)),
            ("b", ColumnType::Native(NativeType::Int)),
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

    // Correct order of UDT fields, but different names - should still succeed
    {
        let udt_bytes = UdtSerializer::new()
            .field("The quick brown fox".as_bytes())
            .field(&42i32.to_be_bytes())
            .finalize();
        let typ = udt_def_with_fields([
            ("k", ColumnType::Native(NativeType::Text)),
            ("l", ColumnType::Native(NativeType::Int)),
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
        let typ = udt_def_with_fields([
            ("a", ColumnType::Native(NativeType::Text)),
            ("b", ColumnType::Native(NativeType::Int)),
        ]);

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
    impl<'frame, 'metadata, A, B> DeserializeValue<'frame, 'metadata> for SwappedPair<A, B>
    where
        A: DeserializeValue<'frame, 'metadata>,
        B: DeserializeValue<'frame, 'metadata>,
    {
        fn type_check(typ: &ColumnType) -> Result<(), TypeCheckError> {
            <(B, A) as DeserializeValue<'frame, 'metadata>>::type_check(typ)
        }

        fn deserialize(
            typ: &'metadata ColumnType<'metadata>,
            v: Option<FrameSlice<'frame>>,
        ) -> Result<Self, DeserializationError> {
            <(B, A) as DeserializeValue<'frame, 'metadata>>::deserialize(typ, v)
                .map(|(b, a)| Self(b, a))
        }
    }

    let mut tuple_contents = BytesMut::new();
    append_bytes(&mut tuple_contents, "foo".as_bytes());
    append_bytes(&mut tuple_contents, &42i32.to_be_bytes());
    let tuple = make_bytes(&tuple_contents);

    let typ = ColumnType::Tuple(vec![
        ColumnType::Native(NativeType::Ascii),
        ColumnType::Native(NativeType::Int),
    ]);

    let tup = deserialize::<SwappedPair<i32, &str>>(&typ, &tuple).unwrap();
    assert_eq!(tup, SwappedPair("foo", 42));
}

pub(crate) fn deserialize<'frame, 'metadata, T>(
    typ: &'metadata ColumnType<'metadata>,
    bytes: &'frame Bytes,
) -> Result<T, DeserializationError>
where
    T: DeserializeValue<'frame, 'metadata>,
{
    <T as DeserializeValue<'frame, 'metadata>>::type_check(typ)
        .map_err(|typecheck_err| DeserializationError(typecheck_err.0))?;
    let mut frame_slice = FrameSlice::new(bytes);
    let value = frame_slice.read_cql_bytes().map_err(|err| {
        mk_deser_err::<T>(
            typ,
            BuiltinDeserializationErrorKind::RawCqlBytesReadError(err),
        )
    })?;
    <T as DeserializeValue<'frame, 'metadata>>::deserialize(typ, value)
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

fn assert_ser_de_identity<'f, T: SerializeValue + DeserializeValue<'f, 'f> + PartialEq + Debug>(
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
        None => panic!("not a BuiltinTypeCheckError: {err:?}"),
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
        None => panic!("not a BuiltinDeserializationError: {err:?}"),
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
        let bytes = serialize(&ColumnType::Native(NativeType::Int), &v);

        // Incompatible types render type check error.
        assert_type_check_error!(
            &bytes,
            f64,
            ColumnType::Native(NativeType::Int),
            super::BuiltinTypeCheckErrorKind::MismatchedType {
                expected: &[ColumnType::Native(NativeType::Double)],
            }
        );

        // ColumnType is said to be Double (8 bytes expected), but in reality the serialized form has 4 bytes only.
        assert_deser_error!(
            &bytes,
            f64,
            ColumnType::Native(NativeType::Double),
            BuiltinDeserializationErrorKind::ByteLengthMismatch {
                expected: 8,
                got: 4,
            }
        );

        // ColumnType is said to be Float, but in reality Int was serialized.
        // As these types have the same size, though, and every binary number in [0, 2^32] is a valid
        // value for both of them, this always succeeds.
        {
            deserialize::<f32>(&ColumnType::Native(NativeType::Float), &bytes).unwrap();
        }
    }

    // str (and also Uuid) are interesting because they accept two types.
    {
        let v = "Ala ma kota";
        let bytes = serialize(&ColumnType::Native(NativeType::Ascii), &v);

        assert_type_check_error!(
            &bytes,
            &str,
            ColumnType::Native(NativeType::Double),
            BuiltinTypeCheckErrorKind::MismatchedType {
                expected: &[
                    ColumnType::Native(NativeType::Ascii),
                    ColumnType::Native(NativeType::Text)
                ],
            }
        );

        // ColumnType is said to be BigInt (8 bytes expected), but in reality the serialized form
        // (the string) has 11 bytes.
        assert_deser_error!(
            &bytes,
            i64,
            ColumnType::Native(NativeType::BigInt),
            BuiltinDeserializationErrorKind::ByteLengthMismatch {
                expected: 8,
                got: 11, // str len
            }
        );
    }
    {
        // -126 is not a valid ASCII nor UTF-8 byte.
        let v = -126_i8;
        let bytes = serialize(&ColumnType::Native(NativeType::TinyInt), &v);

        assert_deser_error!(
            &bytes,
            &str,
            ColumnType::Native(NativeType::Ascii),
            BuiltinDeserializationErrorKind::ExpectedAscii
        );

        assert_deser_error!(
            &bytes,
            &str,
            ColumnType::Native(NativeType::Text),
            BuiltinDeserializationErrorKind::InvalidUtf8(_)
        );
    }
}

#[test]
fn test_option_errors() {
    // Type check correctly renames Rust type
    assert_type_check_error!(
        &Bytes::new(),
        Option<i32>,
        ColumnType::Native(NativeType::Text),
        BuiltinTypeCheckErrorKind::MismatchedType {
            expected: &[ColumnType::Native(NativeType::Int)]
        }
    );

    // Deserialize correctly renames Rust type
    let v = 123_i32;
    let bytes = serialize(&ColumnType::Native(NativeType::Int), &v);
    assert_deser_error!(
        &bytes,
        Option<f64>,
        ColumnType::Native(NativeType::Double),
        BuiltinDeserializationErrorKind::ByteLengthMismatch {
            expected: 8,
            got: 4,
        }
    );
}

#[test]
fn test_maybe_empty_errors() {
    // Type check correctly renames Rust type
    assert_type_check_error!(
        &Bytes::new(),
        MaybeEmpty<i32>,
        ColumnType::Native(NativeType::Text),
        BuiltinTypeCheckErrorKind::MismatchedType {
            expected: &[ColumnType::Native(NativeType::Int)]
        }
    );

    // Deserialize correctly renames Rust type
    let v = 123_i32;
    let bytes = serialize(&ColumnType::Native(NativeType::Int), &v);
    assert_deser_error!(
        &bytes,
        MaybeEmpty<f64>,
        ColumnType::Native(NativeType::Double),
        BuiltinDeserializationErrorKind::ByteLengthMismatch {
            expected: 8,
            got: 4,
        }
    );
}

#[cfg(feature = "secrecy-08")]
#[test]
fn test_secrecy_08_errors() {
    use secrecy_08::Secret;

    use crate::frame::frame_errors::LowLevelDeserializationError;
    // Type check correctly renames Rust type
    assert_type_check_error!(
        &Bytes::new(),
        Secret<String>,
        ColumnType::Native(NativeType::Int),
        BuiltinTypeCheckErrorKind::MismatchedType {
            expected: &[
                ColumnType::Native(NativeType::Ascii),
                ColumnType::Native(NativeType::Text)
            ]
        }
    );

    // Deserialize correctly renames Rust type
    let v = 123_i32;
    let bytes = serialize(&ColumnType::Native(NativeType::Int), &v);
    assert_deser_error!(
        &bytes,
        Secret<Vec<String>>,
        ColumnType::Collection {
            frozen: false,
            typ: CollectionType::List(Box::new(ColumnType::Native(NativeType::Text)))
        },
        BuiltinDeserializationErrorKind::RawCqlBytesReadError(
            LowLevelDeserializationError::IoError(_)
        )
    );
}

#[test]
fn test_set_or_list_general_type_errors() {
    // Types that are not even collections
    {
        assert_type_check_error!(
            &Bytes::new(),
            Vec<i64>,
            ColumnType::Native(NativeType::Float),
            BuiltinTypeCheckErrorKind::NotDeserializableToVec
        );

        assert_type_check_error!(
            &Bytes::new(),
            BTreeSet<i64>,
            ColumnType::Native(NativeType::Float),
            BuiltinTypeCheckErrorKind::SetOrListError(SetOrListTypeCheckErrorKind::NotSet)
        );

        assert_type_check_error!(
            &Bytes::new(),
            HashSet<i64>,
            ColumnType::Native(NativeType::Float),
            BuiltinTypeCheckErrorKind::SetOrListError(SetOrListTypeCheckErrorKind::NotSet)
        );

        assert_type_check_error!(
            &Bytes::new(),
            ListlikeIterator<i64>,
            ColumnType::Native(NativeType::Float),
            BuiltinTypeCheckErrorKind::SetOrListError(SetOrListTypeCheckErrorKind::NotSetOrList)
        );
    }

    // Type check of Rust set against CQL list must fail, because it would be lossy.
    assert_type_check_error!(
        &Bytes::new(),
        BTreeSet<i32>,
        ColumnType::Collection {
            frozen: false,
            typ: CollectionType::List(Box::new(ColumnType::Native(NativeType::Int))),
        },
        BuiltinTypeCheckErrorKind::SetOrListError(SetOrListTypeCheckErrorKind::NotSet)
    );

    assert_type_check_error!(
        &Bytes::new(),
        HashSet<i32>,
        ColumnType::Collection {
            frozen: false,
            typ: CollectionType::List(Box::new(ColumnType::Native(NativeType::Int))),
        },
        BuiltinTypeCheckErrorKind::SetOrListError(SetOrListTypeCheckErrorKind::NotSet)
    );
}

#[test]
fn test_set_or_list_elem_type_errors() {
    let cql_type = ColumnType::Collection {
        frozen: false,
        typ: CollectionType::Set(Box::new(ColumnType::Native(NativeType::Varint))),
    };
    // Explicitly passing frame and meta from outside is the only way I know to satisfy
    // the borrow checker. The issue otherwise is that the lifetime (specified by the caller)
    // could be bigger than lifetime of local arguments (frame, column type), and so they can't
    // be borrowed for so long.
    fn verify_elem_typck_err<'meta, 'frame, T: DeserializeValue<'frame, 'meta> + Debug>(
        frame: &'frame Bytes,
        meta: &'meta ColumnType<'meta>,
    ) {
        let err = deserialize::<T>(meta, frame).unwrap_err();
        let err = get_typeck_err(&err);
        assert_eq!(err.rust_name, std::any::type_name::<T>());
        assert_eq!(err.cql_type, *meta);
        let BuiltinTypeCheckErrorKind::SetOrListError(
            SetOrListTypeCheckErrorKind::ElementTypeCheckFailed(ref err),
        ) = err.kind
        else {
            panic!("unexpected error kind: {}", err.kind)
        };
        let err = get_typeck_err_inner(err.0.as_ref());
        assert_eq!(err.rust_name, std::any::type_name::<i64>());
        assert_eq!(err.cql_type, ColumnType::Native(NativeType::Varint));
        assert_matches!(
            err.kind,
            BuiltinTypeCheckErrorKind::MismatchedType {
                expected: &[ColumnType::Native(NativeType::BigInt)]
            }
        );
    }
    verify_elem_typck_err::<Vec<i64>>(&Bytes::new(), &cql_type);
    verify_elem_typck_err::<BTreeSet<i64>>(&Bytes::new(), &cql_type);
    verify_elem_typck_err::<HashSet<i64>>(&Bytes::new(), &cql_type);
    verify_elem_typck_err::<ListlikeIterator<i64>>(&Bytes::new(), &cql_type);
}

#[test]
fn test_set_or_list_elem_deser_errors() {
    let ser_typ = ColumnType::Collection {
        frozen: false,
        typ: CollectionType::List(Box::new(ColumnType::Native(NativeType::Int))),
    };
    let v = vec![123_i32];
    let bytes = serialize(&ser_typ, &v);
    let deser_type = ColumnType::Collection {
        frozen: false,
        typ: CollectionType::Set(Box::new(ColumnType::Native(NativeType::BigInt))),
    };

    // In contrary to the test above, here we could require owned deserialization, since
    // ListLikeIterator can't be tested with that function anyway. For consistency and future ease of
    // editing, I used the same approach as in the previous test anyway.
    fn verify_elem_deser_err<'meta, 'frame, T: DeserializeValue<'frame, 'meta> + Debug>(
        frame: &'frame Bytes,
        meta: &'meta ColumnType<'meta>,
    ) {
        let err = deserialize::<T>(meta, frame).unwrap_err();
        let err = get_deser_err(&err);
        assert_eq!(err.rust_name, std::any::type_name::<T>());
        assert_eq!(err.cql_type, *meta);
        let BuiltinDeserializationErrorKind::SetOrListError(
            SetOrListDeserializationErrorKind::ElementDeserializationFailed(err),
        ) = &err.kind
        else {
            panic!("unexpected error kind: {}", err.kind)
        };
        let err = get_deser_err(err);
        assert_eq!(err.rust_name, std::any::type_name::<i64>());
        assert_eq!(err.cql_type, ColumnType::Native(NativeType::BigInt));
        assert_matches!(
            err.kind,
            BuiltinDeserializationErrorKind::ByteLengthMismatch {
                expected: 8,
                got: 4
            }
        );
    }

    verify_elem_deser_err::<Vec<i64>>(&bytes, &deser_type);
    verify_elem_deser_err::<BTreeSet<i64>>(&bytes, &deser_type);
    verify_elem_deser_err::<HashSet<i64>>(&bytes, &deser_type);

    // ListlikeIterator has to be tested separately.
    {
        let mut iterator = deserialize::<ListlikeIterator<i64>>(&deser_type, &bytes).unwrap();
        let err = iterator.next().unwrap().unwrap_err();
        let err = get_deser_err(&err);
        assert_eq!(
            err.rust_name,
            std::any::type_name::<ListlikeIterator<i64>>()
        );
        assert_eq!(err.cql_type, deser_type);
        let BuiltinDeserializationErrorKind::SetOrListError(
            SetOrListDeserializationErrorKind::ElementDeserializationFailed(err),
        ) = &err.kind
        else {
            panic!("unexpected error kind: {}", err.kind)
        };
        let err = get_deser_err(err);
        assert_eq!(err.rust_name, std::any::type_name::<i64>());
        assert_eq!(err.cql_type, ColumnType::Native(NativeType::BigInt));
        assert_matches!(
            err.kind,
            BuiltinDeserializationErrorKind::ByteLengthMismatch {
                expected: 8,
                got: 4
            }
        );
    }
}

#[test]
fn test_map_errors() {
    // Not a map
    {
        let ser_typ = ColumnType::Native(NativeType::Float);
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
            &ColumnType::Collection {
                frozen: false,
                typ: CollectionType::Map(
                    Box::new(ColumnType::Native(NativeType::Varint)),
                    Box::new(ColumnType::Native(NativeType::Boolean)),
                ),
            },
            &Bytes::new(),
        )
        .unwrap_err();
        let err = get_typeck_err(&err);
        assert_eq!(err.rust_name, std::any::type_name::<HashMap<i64, bool>>());
        assert_eq!(
            err.cql_type,
            ColumnType::Collection {
                frozen: false,
                typ: CollectionType::Map(
                    Box::new(ColumnType::Native(NativeType::Varint)),
                    Box::new(ColumnType::Native(NativeType::Boolean))
                ),
            }
        );
        let BuiltinTypeCheckErrorKind::MapError(MapTypeCheckErrorKind::KeyTypeCheckFailed(ref err)) =
            err.kind
        else {
            panic!("unexpected error kind: {}", err.kind)
        };
        let err = get_typeck_err_inner(err.0.as_ref());
        assert_eq!(err.rust_name, std::any::type_name::<i64>());
        assert_eq!(err.cql_type, ColumnType::Native(NativeType::Varint));
        assert_matches!(
            err.kind,
            BuiltinTypeCheckErrorKind::MismatchedType {
                expected: &[ColumnType::Native(NativeType::BigInt)]
            }
        );
    }

    // Value type mismatch
    {
        let err = deserialize::<BTreeMap<i64, &str>>(
            &ColumnType::Collection {
                frozen: false,
                typ: CollectionType::Map(
                    Box::new(ColumnType::Native(NativeType::BigInt)),
                    Box::new(ColumnType::Native(NativeType::Boolean)),
                ),
            },
            &Bytes::new(),
        )
        .unwrap_err();
        let err = get_typeck_err(&err);
        assert_eq!(err.rust_name, std::any::type_name::<BTreeMap<i64, &str>>());
        assert_eq!(
            err.cql_type,
            ColumnType::Collection {
                frozen: false,
                typ: CollectionType::Map(
                    Box::new(ColumnType::Native(NativeType::BigInt)),
                    Box::new(ColumnType::Native(NativeType::Boolean))
                ),
            }
        );
        let BuiltinTypeCheckErrorKind::MapError(MapTypeCheckErrorKind::ValueTypeCheckFailed(
            ref err,
        )) = err.kind
        else {
            panic!("unexpected error kind: {}", err.kind)
        };
        let err = get_typeck_err_inner(err.0.as_ref());
        assert_eq!(err.rust_name, std::any::type_name::<&str>());
        assert_eq!(err.cql_type, ColumnType::Native(NativeType::Boolean));
        assert_matches!(
            err.kind,
            BuiltinTypeCheckErrorKind::MismatchedType {
                expected: &[
                    ColumnType::Native(NativeType::Ascii),
                    ColumnType::Native(NativeType::Text)
                ]
            }
        );
    }

    // Key length mismatch
    {
        let ser_typ = ColumnType::Collection {
            frozen: false,
            typ: CollectionType::Map(
                Box::new(ColumnType::Native(NativeType::Int)),
                Box::new(ColumnType::Native(NativeType::Boolean)),
            ),
        };
        let v = HashMap::from([(42, false), (2137, true)]);
        let bytes = serialize(&ser_typ, &v as &dyn SerializeValue);

        let err = deserialize::<HashMap<i64, bool>>(
            &ColumnType::Collection {
                frozen: false,
                typ: CollectionType::Map(
                    Box::new(ColumnType::Native(NativeType::BigInt)),
                    Box::new(ColumnType::Native(NativeType::Boolean)),
                ),
            },
            &bytes,
        )
        .unwrap_err();
        let err = get_deser_err(&err);
        assert_eq!(err.rust_name, std::any::type_name::<HashMap<i64, bool>>());
        assert_eq!(
            err.cql_type,
            ColumnType::Collection {
                frozen: false,
                typ: CollectionType::Map(
                    Box::new(ColumnType::Native(NativeType::BigInt)),
                    Box::new(ColumnType::Native(NativeType::Boolean))
                ),
            }
        );
        let BuiltinDeserializationErrorKind::MapError(
            MapDeserializationErrorKind::KeyDeserializationFailed(err),
        ) = &err.kind
        else {
            panic!("unexpected error kind: {}", err.kind)
        };
        let err = get_deser_err(err);
        assert_eq!(err.rust_name, std::any::type_name::<i64>());
        assert_eq!(err.cql_type, ColumnType::Native(NativeType::BigInt));
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
        let ser_typ = ColumnType::Collection {
            frozen: false,
            typ: CollectionType::Map(
                Box::new(ColumnType::Native(NativeType::Int)),
                Box::new(ColumnType::Native(NativeType::Boolean)),
            ),
        };
        let v = HashMap::from([(42, false), (2137, true)]);
        let bytes = serialize(&ser_typ, &v as &dyn SerializeValue);

        let err = deserialize::<HashMap<i32, i16>>(
            &ColumnType::Collection {
                frozen: false,
                typ: CollectionType::Map(
                    Box::new(ColumnType::Native(NativeType::Int)),
                    Box::new(ColumnType::Native(NativeType::SmallInt)),
                ),
            },
            &bytes,
        )
        .unwrap_err();
        let err = get_deser_err(&err);
        assert_eq!(err.rust_name, std::any::type_name::<HashMap<i32, i16>>());
        assert_eq!(
            err.cql_type,
            ColumnType::Collection {
                frozen: false,
                typ: CollectionType::Map(
                    Box::new(ColumnType::Native(NativeType::Int)),
                    Box::new(ColumnType::Native(NativeType::SmallInt))
                ),
            }
        );
        let BuiltinDeserializationErrorKind::MapError(
            MapDeserializationErrorKind::ValueDeserializationFailed(err),
        ) = &err.kind
        else {
            panic!("unexpected error kind: {}", err.kind)
        };
        let err = get_deser_err(err);
        assert_eq!(err.rust_name, std::any::type_name::<i16>());
        assert_eq!(err.cql_type, ColumnType::Native(NativeType::SmallInt));
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
            ColumnType::Native(NativeType::BigInt),
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
            ColumnType::Tuple(vec![
                ColumnType::Native(NativeType::Float),
                ColumnType::Native(NativeType::Float)
            ]),
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
                &ColumnType::Tuple(vec![ColumnType::Native(NativeType::SmallInt)]),
                &Bytes::new(),
            )
            .unwrap_err();
            let err = get_typeck_err(&err);
            assert_eq!(err.rust_name, std::any::type_name::<(i64,)>());
            assert_eq!(
                err.cql_type,
                ColumnType::Tuple(vec![ColumnType::Native(NativeType::SmallInt)])
            );
            let BuiltinTypeCheckErrorKind::TupleError(
                TupleTypeCheckErrorKind::FieldTypeCheckFailed { ref err, position },
            ) = err.kind
            else {
                panic!("unexpected error kind: {}", err.kind)
            };
            assert_eq!(position, 0);
            let err = get_typeck_err_inner(err.0.as_ref());
            assert_eq!(err.rust_name, std::any::type_name::<i64>());
            assert_eq!(err.cql_type, ColumnType::Native(NativeType::SmallInt));
            assert_matches!(
                err.kind,
                BuiltinTypeCheckErrorKind::MismatchedType {
                    expected: &[ColumnType::Native(NativeType::BigInt)]
                }
            );
        }
    }

    {
        let ser_typ = ColumnType::Tuple(vec![
            ColumnType::Native(NativeType::Int),
            ColumnType::Native(NativeType::Float),
        ]);
        let v = (123_i32, 123.123_f32);
        let bytes = serialize(&ser_typ, &v);

        {
            let err = deserialize::<(i32, f64)>(
                &ColumnType::Tuple(vec![
                    ColumnType::Native(NativeType::Int),
                    ColumnType::Native(NativeType::Double),
                ]),
                &bytes,
            )
            .unwrap_err();
            let err = get_deser_err(&err);
            assert_eq!(err.rust_name, std::any::type_name::<(i32, f64)>());
            assert_eq!(
                err.cql_type,
                ColumnType::Tuple(vec![
                    ColumnType::Native(NativeType::Int),
                    ColumnType::Native(NativeType::Double)
                ])
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
            assert_eq!(err.cql_type, ColumnType::Native(NativeType::Double));
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
    let ser_typ = ColumnType::Collection {
        frozen: false,
        typ: CollectionType::Map(
            Box::new(ColumnType::Native(NativeType::Int)),
            Box::new(ColumnType::Native(NativeType::Boolean)),
        ),
    };
    let v = HashMap::from([(42, false), (2137, true)]);
    let bytes = serialize(&ser_typ, &v as &dyn SerializeValue);

    deserialize::<MaybeEmpty<i32>>(&ser_typ, &bytes).unwrap_err();
}

#[test]
fn test_box_errors() {
    let v = 123_i32;
    let bytes = serialize(&ColumnType::Native(NativeType::Int), &v);

    // Incompatible types render type check error.
    assert_type_check_error!(
        &bytes,
        Box<f64>,
        ColumnType::Native(NativeType::Int),
        BuiltinTypeCheckErrorKind::MismatchedType {
            expected: &[ColumnType::Native(NativeType::Double)],
        }
    );

    // ColumnType is said to be Double (8 bytes expected), but in reality the serialized form has 4 bytes only.
    assert_deser_error!(
        &bytes,
        Box<f64>,
        ColumnType::Native(NativeType::Double),
        BuiltinDeserializationErrorKind::ByteLengthMismatch {
            expected: 8,
            got: 4,
        }
    );

    // Arc<str> is a special case, let's test it separately
    assert_type_check_error!(
        &bytes,
        Box<str>,
        ColumnType::Native(NativeType::Int),
        BuiltinTypeCheckErrorKind::MismatchedType {
            expected: &[
                ColumnType::Native(NativeType::Ascii),
                ColumnType::Native(NativeType::Text)
            ],
        }
    );

    // -126 is not a valid ASCII nor UTF-8 byte.
    let v = -126_i8;
    let bytes = serialize(&ColumnType::Native(NativeType::TinyInt), &v);

    assert_deser_error!(
        &bytes,
        Box<str>,
        ColumnType::Native(NativeType::Text),
        BuiltinDeserializationErrorKind::InvalidUtf8(_)
    );
}

#[test]
fn test_arc_errors() {
    let v = 123_i32;
    let bytes = serialize(&ColumnType::Native(NativeType::Int), &v);

    // Incompatible types render type check error.
    assert_type_check_error!(
        &bytes,
        Arc<f64>,
        ColumnType::Native(NativeType::Int),
        BuiltinTypeCheckErrorKind::MismatchedType {
            expected: &[ColumnType::Native(NativeType::Double)],
        }
    );

    // ColumnType is said to be Double (8 bytes expected), but in reality the serialized form has 4 bytes only.
    assert_deser_error!(
        &bytes,
        Arc<f64>,
        ColumnType::Native(NativeType::Double),
        BuiltinDeserializationErrorKind::ByteLengthMismatch {
            expected: 8,
            got: 4,
        }
    );

    // Arc<str> is a special case, let's test it separately
    assert_type_check_error!(
        &bytes,
        Arc<str>,
        ColumnType::Native(NativeType::Int),
        BuiltinTypeCheckErrorKind::MismatchedType {
            expected: &[
                ColumnType::Native(NativeType::Ascii),
                ColumnType::Native(NativeType::Text)
            ],
        }
    );

    // -126 is not a valid ASCII nor UTF-8 byte.
    let v = -126_i8;
    let bytes = serialize(&ColumnType::Native(NativeType::TinyInt), &v);

    assert_deser_error!(
        &bytes,
        Arc<str>,
        ColumnType::Native(NativeType::Text),
        BuiltinDeserializationErrorKind::InvalidUtf8(_)
    );
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
                let typ = ColumnType::Collection {
                    frozen: false,
                    typ: CollectionType::Map(
                        Box::new(ColumnType::Native(NativeType::Ascii)),
                        Box::new(ColumnType::Native(NativeType::Blob)),
                    ),
                };
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
                let typ = udt_def_with_fields([("c", ColumnType::Native(NativeType::Boolean))]);
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
                    ("d", ColumnType::Native(NativeType::Boolean)),
                    ("a", ColumnType::Native(NativeType::Text)),
                    ("b", ColumnType::Native(NativeType::Int)),
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
                let typ = udt_def_with_fields([
                    ("b", ColumnType::Native(NativeType::Int)),
                    ("a", ColumnType::Native(NativeType::Text)),
                ]);
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
                let typ = udt_def_with_fields([
                    ("a", ColumnType::Native(NativeType::Blob)),
                    ("b", ColumnType::Native(NativeType::Int)),
                ]);
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
                assert_eq!(err.cql_type, ColumnType::Native(NativeType::Blob));
                assert_matches!(
                    err.kind,
                    BuiltinTypeCheckErrorKind::MismatchedType {
                        expected: &[
                            ColumnType::Native(NativeType::Ascii),
                            ColumnType::Native(NativeType::Text)
                        ]
                    }
                );
            }
        }

        // Deserialization errors
        {
            // Got null
            {
                let typ = udt_def_with_fields([
                    ("c", ColumnType::Native(NativeType::Boolean)),
                    ("a", ColumnType::Native(NativeType::Blob)),
                    ("b", ColumnType::Native(NativeType::Int)),
                ]);

                let err = Udt::deserialize(&typ, None).unwrap_err();
                let err = get_deser_err(&err);
                assert_eq!(err.rust_name, std::any::type_name::<Udt>());
                assert_eq!(err.cql_type, typ);
                assert_matches!(err.kind, BuiltinDeserializationErrorKind::ExpectedNonNull);
            }

            // UDT field deserialization failed
            {
                let typ = udt_def_with_fields([
                    ("a", ColumnType::Native(NativeType::Ascii)),
                    ("c", ColumnType::Native(NativeType::Boolean)),
                ]);

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
                assert_eq!(err.cql_type, ColumnType::Native(NativeType::Boolean));
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
        #[scylla(crate = "crate", flavor = "enforce_order", forbid_excess_udt_fields)]
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
                let typ = ColumnType::Collection {
                    frozen: false,
                    typ: CollectionType::Map(
                        Box::new(ColumnType::Native(NativeType::Ascii)),
                        Box::new(ColumnType::Native(NativeType::Blob)),
                    ),
                };
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
                let typ = udt_def_with_fields([("a", ColumnType::Native(NativeType::Text))]);
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
                    ("a", ColumnType::Native(NativeType::Text)),
                    ("b", ColumnType::Native(NativeType::Int)),
                    ("d", ColumnType::Native(NativeType::Boolean)),
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
                let typ = udt_def_with_fields([
                    ("b", ColumnType::Native(NativeType::Int)),
                    ("a", ColumnType::Native(NativeType::Text)),
                ]);
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
                let typ = udt_def_with_fields([
                    ("a", ColumnType::Native(NativeType::Blob)),
                    ("b", ColumnType::Native(NativeType::Int)),
                ]);
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
                assert_eq!(err.cql_type, ColumnType::Native(NativeType::Blob));
                assert_matches!(
                    err.kind,
                    BuiltinTypeCheckErrorKind::MismatchedType {
                        expected: &[
                            ColumnType::Native(NativeType::Ascii),
                            ColumnType::Native(NativeType::Text)
                        ]
                    }
                );
            }
        }

        // Deserialization errors
        {
            // Got null
            {
                let typ = udt_def_with_fields([
                    ("a", ColumnType::Native(NativeType::Text)),
                    ("b", ColumnType::Native(NativeType::Int)),
                    ("c", ColumnType::Native(NativeType::Boolean)),
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
                    ("a", ColumnType::Native(NativeType::Text)),
                    ("b", ColumnType::Native(NativeType::Int)),
                    ("c", ColumnType::Native(NativeType::Boolean)),
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
                    ("a", ColumnType::Native(NativeType::Text)),
                    ("b", ColumnType::Native(NativeType::Int)),
                    ("c", ColumnType::Native(NativeType::Boolean)),
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
                assert_eq!(err.rust_name, std::any::type_name::<Option<i32>>());
                assert_eq!(err.cql_type, ColumnType::Native(NativeType::Int));
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

#[test]
fn metadata_does_not_bound_deserialized_values() {
    /* It's important to understand what is a _deserialized value_. It's not just
     * an implementor of DeserializeValue; there are some implementors of DeserializeValue
     * who are not yet final values, but partially deserialized types that support further
     * deserialization - _value deserializers_, such as `ListlikeIterator` or `UdtIterator`.
     * _Value deserializers_, because they still need to deserialize some value, are naturally
     * bound by 'metadata lifetime. However, _values_ are completely deserialized, so they
     * should not be bound by 'metadata - only by 'frame. This test asserts that.
     */

    // We don't care about the actual deserialized data - all `Err`s is OK.
    // This test's goal is only to compile, asserting that lifetimes are correct.
    let bytes = Bytes::new();

    // By this binding, we require that the deserialized values live longer than metadata.
    let _decoded_results = {
        // Metadata's lifetime is limited to this scope.

        // blob
        let blob_typ = ColumnType::Native(NativeType::Blob);
        let decoded_blob_res = deserialize::<&[u8]>(&blob_typ, &bytes);

        // str
        let str_typ = ColumnType::Native(NativeType::Ascii);
        let decoded_str_res = deserialize::<&str>(&str_typ, &bytes);

        // list
        let list_typ = ColumnType::Collection {
            frozen: false,
            typ: CollectionType::List(Box::new(ColumnType::Native(NativeType::Ascii))),
        };
        let decoded_vec_str_res = deserialize::<Vec<&str>>(&list_typ, &bytes);
        let decoded_vec_string_res = deserialize::<Vec<String>>(&list_typ, &bytes);

        // set
        let set_typ = ColumnType::Collection {
            frozen: false,
            typ: CollectionType::Set(Box::new(ColumnType::Native(NativeType::Ascii))),
        };
        let decoded_set_str_res = deserialize::<HashSet<&str>>(&set_typ, &bytes);
        let decoded_set_string_res = deserialize::<HashSet<String>>(&set_typ, &bytes);

        // map
        let map_typ = ColumnType::Collection {
            frozen: false,
            typ: CollectionType::Map(
                Box::new(ColumnType::Native(NativeType::Ascii)),
                Box::new(ColumnType::Native(NativeType::Int)),
            ),
        };
        let decoded_map_str_int_res = deserialize::<HashMap<&str, i32>>(&map_typ, &bytes);

        // UDT
        let udt_typ = ColumnType::UserDefinedType {
            frozen: false,
            definition: Arc::new(UserDefinedType {
                name: "udt".into(),
                keyspace: "ks".into(),
                field_types: vec![
                    ("bytes".into(), ColumnType::Native(NativeType::Blob)),
                    ("text".into(), ColumnType::Native(NativeType::Text)),
                ],
            }),
        };
        #[derive(DeserializeValue)]
        #[scylla(crate=crate)]
        struct Udt<'frame> {
            #[expect(dead_code)]
            bytes: &'frame [u8],
            #[expect(dead_code)]
            text: &'frame str,
        }
        let decoded_udt_res = deserialize::<Udt>(&udt_typ, &bytes);

        (
            decoded_blob_res,
            decoded_str_res,
            decoded_vec_str_res,
            decoded_vec_string_res,
            decoded_set_str_res,
            decoded_set_string_res,
            decoded_map_str_int_res,
            decoded_udt_res,
        )
    };
}

// Tests migrated from old frame/value_tests.rs file

#[test]
fn test_deserialize_text_types() {
    let buf: Vec<u8> = vec![0x41];
    let int_slice = &mut &buf[..];
    let ascii_serialized = super::deser_cql_value(&ColumnType::Native(Ascii), int_slice).unwrap();
    let text_serialized = super::deser_cql_value(&ColumnType::Native(Text), int_slice).unwrap();
    assert_eq!(ascii_serialized, CqlValue::Ascii("A".to_string()));
    assert_eq!(text_serialized, CqlValue::Text("A".to_string()));
}

#[test]
fn test_deserialize_uuid_inet_types() {
    let my_uuid = Uuid::parse_str("00000000000000000000000000000001").unwrap();

    let uuid_buf: Vec<u8> = vec![0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1];
    let uuid_slice = &mut &uuid_buf[..];
    let uuid_serialize = super::deser_cql_value(&ColumnType::Native(Uuid), uuid_slice).unwrap();
    assert_eq!(uuid_serialize, CqlValue::Uuid(my_uuid));

    let my_timeuuid = CqlTimeuuid::from_str("00000000000000000000000000000001").unwrap();
    let time_uuid_serialize =
        super::deser_cql_value(&ColumnType::Native(Timeuuid), uuid_slice).unwrap();
    assert_eq!(time_uuid_serialize, CqlValue::Timeuuid(my_timeuuid));

    let my_ip = "::1".parse().unwrap();
    let ip_buf: Vec<u8> = vec![0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1];
    let ip_slice = &mut &ip_buf[..];
    let ip_serialize = super::deser_cql_value(&ColumnType::Native(Inet), ip_slice).unwrap();
    assert_eq!(ip_serialize, CqlValue::Inet(my_ip));

    let max_ip = "ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff".parse().unwrap();
    let max_ip_buf: Vec<u8> = vec![
        255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
    ];
    let max_ip_slice = &mut &max_ip_buf[..];
    let max_ip_serialize = super::deser_cql_value(&ColumnType::Native(Inet), max_ip_slice).unwrap();
    assert_eq!(max_ip_serialize, CqlValue::Inet(max_ip));
}

#[test]
fn test_floating_points() {
    let float: f32 = 0.5;
    let double: f64 = 2.0;

    let float_buf: Vec<u8> = vec![63, 0, 0, 0];
    let float_slice = &mut &float_buf[..];
    let float_serialize = super::deser_cql_value(&ColumnType::Native(Float), float_slice).unwrap();
    assert_eq!(float_serialize, CqlValue::Float(float));

    let double_buf: Vec<u8> = vec![64, 0, 0, 0, 0, 0, 0, 0];
    let double_slice = &mut &double_buf[..];
    let double_serialize =
        super::deser_cql_value(&ColumnType::Native(Double), double_slice).unwrap();
    assert_eq!(double_serialize, CqlValue::Double(double));
}

#[cfg(any(feature = "num-bigint-03", feature = "num-bigint-04"))]
struct VarintTestCase {
    value: i32,
    encoding: Vec<u8>,
}

#[cfg(any(feature = "num-bigint-03", feature = "num-bigint-04"))]
fn varint_test_cases_from_spec() -> Vec<VarintTestCase> {
    /*
        Table taken from CQL Binary Protocol v4 spec

        Value | Encoding
        ------|---------
            0 |     0x00
            1 |     0x01
          127 |     0x7F
          128 |   0x0080
          129 |   0x0081
           -1 |     0xFF
         -128 |     0x80
         -129 |   0xFF7F
    */
    vec![
        VarintTestCase {
            value: 0,
            encoding: vec![0x00],
        },
        VarintTestCase {
            value: 1,
            encoding: vec![0x01],
        },
        VarintTestCase {
            value: 127,
            encoding: vec![0x7F],
        },
        VarintTestCase {
            value: 128,
            encoding: vec![0x00, 0x80],
        },
        VarintTestCase {
            value: 129,
            encoding: vec![0x00, 0x81],
        },
        VarintTestCase {
            value: -1,
            encoding: vec![0xFF],
        },
        VarintTestCase {
            value: -128,
            encoding: vec![0x80],
        },
        VarintTestCase {
            value: -129,
            encoding: vec![0xFF, 0x7F],
        },
    ]
}

#[cfg(feature = "num-bigint-03")]
#[test]
fn test_bigint03() {
    use num_bigint_03::ToBigInt;

    let tests = varint_test_cases_from_spec();

    for t in tests.iter() {
        let value = super::deser_cql_value(&ColumnType::Native(Varint), &mut &*t.encoding).unwrap();
        assert_eq!(CqlValue::Varint(t.value.to_bigint().unwrap().into()), value);
    }
}

#[cfg(feature = "num-bigint-04")]
#[test]
fn test_bigint04() {
    use num_bigint_04::ToBigInt;

    let tests = varint_test_cases_from_spec();

    for t in tests.iter() {
        let value = super::deser_cql_value(&ColumnType::Native(Varint), &mut &*t.encoding).unwrap();
        assert_eq!(CqlValue::Varint(t.value.to_bigint().unwrap().into()), value);
    }
}

#[cfg(feature = "bigdecimal-04")]
#[test]
fn test_decimal() {
    use bigdecimal_04::BigDecimal;
    struct Test<'a> {
        value: BigDecimal,
        encoding: &'a [u8],
    }

    let tests = [
        Test {
            value: BigDecimal::from_str("-1.28").unwrap(),
            encoding: &[0x0, 0x0, 0x0, 0x2, 0x80],
        },
        Test {
            value: BigDecimal::from_str("1.29").unwrap(),
            encoding: &[0x0, 0x0, 0x0, 0x2, 0x0, 0x81],
        },
        Test {
            value: BigDecimal::from_str("0").unwrap(),
            encoding: &[0x0, 0x0, 0x0, 0x0, 0x0],
        },
        Test {
            value: BigDecimal::from_str("123").unwrap(),
            encoding: &[0x0, 0x0, 0x0, 0x0, 0x7b],
        },
    ];

    for t in tests.iter() {
        let value =
            super::deser_cql_value(&ColumnType::Native(Decimal), &mut &*t.encoding).unwrap();
        assert_eq!(
            CqlValue::Decimal(t.value.clone().try_into().unwrap()),
            value
        );
    }
}

#[test]
fn test_deserialize_counter() {
    let counter: Vec<u8> = vec![0, 0, 0, 0, 0, 0, 1, 0];
    let counter_slice = &mut &counter[..];
    let counter_serialize =
        super::deser_cql_value(&ColumnType::Native(NativeType::Counter), counter_slice).unwrap();
    assert_eq!(counter_serialize, CqlValue::Counter(Counter(256)));
}

#[test]
fn test_deserialize_blob() {
    let blob: Vec<u8> = vec![0, 1, 2, 3];
    let blob_slice = &mut &blob[..];
    let blob_serialize = super::deser_cql_value(&ColumnType::Native(Blob), blob_slice).unwrap();
    assert_eq!(blob_serialize, CqlValue::Blob(blob));
}

#[test]
fn test_deserialize_bool() {
    let bool_buf: Vec<u8> = vec![0x00];
    let bool_slice = &mut &bool_buf[..];
    let bool_serialize = super::deser_cql_value(&ColumnType::Native(Boolean), bool_slice).unwrap();
    assert_eq!(bool_serialize, CqlValue::Boolean(false));

    let bool_buf: Vec<u8> = vec![0x01];
    let bool_slice = &mut &bool_buf[..];
    let bool_serialize = super::deser_cql_value(&ColumnType::Native(Boolean), bool_slice).unwrap();
    assert_eq!(bool_serialize, CqlValue::Boolean(true));
}

#[test]
fn test_deserialize_int_types() {
    let int_buf: Vec<u8> = vec![0, 0, 0, 4];
    let int_slice = &mut &int_buf[..];
    let int_serialized = super::deser_cql_value(&ColumnType::Native(Int), int_slice).unwrap();
    assert_eq!(int_serialized, CqlValue::Int(4));

    let smallint_buf: Vec<u8> = vec![0, 4];
    let smallint_slice = &mut &smallint_buf[..];
    let smallint_serialized =
        super::deser_cql_value(&ColumnType::Native(SmallInt), smallint_slice).unwrap();
    assert_eq!(smallint_serialized, CqlValue::SmallInt(4));

    let tinyint_buf: Vec<u8> = vec![4];
    let tinyint_slice = &mut &tinyint_buf[..];
    let tinyint_serialized =
        super::deser_cql_value(&ColumnType::Native(TinyInt), tinyint_slice).unwrap();
    assert_eq!(tinyint_serialized, CqlValue::TinyInt(4));

    let bigint_buf: Vec<u8> = vec![0, 0, 0, 0, 0, 0, 0, 4];
    let bigint_slice = &mut &bigint_buf[..];
    let bigint_serialized =
        super::deser_cql_value(&ColumnType::Native(BigInt), bigint_slice).unwrap();
    assert_eq!(bigint_serialized, CqlValue::BigInt(4));
}

#[test]
fn test_list_from_cql() {
    let my_vec: Vec<CqlValue> = vec![CqlValue::Int(20), CqlValue::Int(2), CqlValue::Int(13)];

    let cql: CqlValue = CqlValue::List(my_vec);
    let decoded = cql.into_vec().unwrap();

    assert_eq!(decoded[0], CqlValue::Int(20));
    assert_eq!(decoded[1], CqlValue::Int(2));
    assert_eq!(decoded[2], CqlValue::Int(13));
}

#[test]
fn test_set_from_cql() {
    let my_vec: Vec<CqlValue> = vec![CqlValue::Int(20), CqlValue::Int(2), CqlValue::Int(13)];

    let cql: CqlValue = CqlValue::Set(my_vec);
    let decoded = cql.as_set().unwrap();

    assert_eq!(decoded[0], CqlValue::Int(20));
    assert_eq!(decoded[1], CqlValue::Int(2));
    assert_eq!(decoded[2], CqlValue::Int(13));
}

#[test]
fn test_map_from_cql() {
    let my_vec: Vec<(CqlValue, CqlValue)> = vec![
        (CqlValue::Int(20), CqlValue::Int(21)),
        (CqlValue::Int(2), CqlValue::Int(3)),
    ];

    let cql: CqlValue = CqlValue::Map(my_vec);

    // Test borrowing.
    let decoded = cql.as_map().unwrap();

    assert_eq!(CqlValue::Int(20), decoded[0].0);
    assert_eq!(CqlValue::Int(21), decoded[0].1);

    assert_eq!(CqlValue::Int(2), decoded[1].0);
    assert_eq!(CqlValue::Int(3), decoded[1].1);

    // Test taking the ownership.
    let decoded = cql.into_pair_vec().unwrap();

    assert_eq!(CqlValue::Int(20), decoded[0].0);
    assert_eq!(CqlValue::Int(21), decoded[0].1);

    assert_eq!(CqlValue::Int(2), decoded[1].0);
    assert_eq!(CqlValue::Int(3), decoded[1].1);
}

#[test]
fn test_udt_from_cql() {
    let my_fields: Vec<(String, Option<CqlValue>)> = vec![
        ("fst".to_string(), Some(CqlValue::Int(10))),
        ("snd".to_string(), Some(CqlValue::Boolean(true))),
    ];

    let cql: CqlValue = CqlValue::UserDefinedType {
        keyspace: "".to_string(),
        name: "".to_string(),
        fields: my_fields,
    };

    // Test borrowing.
    let decoded = cql.as_udt().unwrap();

    assert_eq!("fst".to_string(), decoded[0].0);
    assert_eq!(Some(CqlValue::Int(10)), decoded[0].1);

    assert_eq!("snd".to_string(), decoded[1].0);
    assert_eq!(Some(CqlValue::Boolean(true)), decoded[1].1);

    let decoded = cql.into_udt_pair_vec().unwrap();

    assert_eq!("fst".to_string(), decoded[0].0);
    assert_eq!(Some(CqlValue::Int(10)), decoded[0].1);

    assert_eq!("snd".to_string(), decoded[1].0);
    assert_eq!(Some(CqlValue::Boolean(true)), decoded[1].1);
}

#[test]
fn test_deserialize_date() {
    // Date is correctly parsed from a 4 byte array
    let four_bytes: [u8; 4] = [12, 23, 34, 45];
    let date: CqlValue =
        super::deser_cql_value(&ColumnType::Native(Date), &mut four_bytes.as_ref()).unwrap();
    assert_eq!(
        date,
        CqlValue::Date(CqlDate(u32::from_be_bytes(four_bytes)))
    );

    // Date is parsed as u32 not i32, u32::MAX is u32::MAX
    let date: CqlValue = super::deser_cql_value(
        &ColumnType::Native(Date),
        &mut u32::MAX.to_be_bytes().as_ref(),
    )
    .unwrap();
    assert_eq!(date, CqlValue::Date(CqlDate(u32::MAX)));

    // Trying to parse a 0, 3 or 5 byte array fails
    super::deser_cql_value(&ColumnType::Native(Date), &mut [].as_ref()).unwrap();
    super::deser_cql_value(&ColumnType::Native(Date), &mut [1, 2, 3].as_ref()).unwrap_err();
    super::deser_cql_value(&ColumnType::Native(Date), &mut [1, 2, 3, 4, 5].as_ref()).unwrap_err();

    // Deserialize unix epoch
    let unix_epoch_bytes = 2_u32.pow(31).to_be_bytes();

    let date =
        super::deser_cql_value(&ColumnType::Native(Date), &mut unix_epoch_bytes.as_ref()).unwrap();
    assert_eq!(date.as_cql_date(), Some(CqlDate(1 << 31)));

    // 2^31 - 30 when converted to NaiveDate is 1969-12-02
    let before_epoch = CqlDate((1 << 31) - 30);
    let date: CqlValue = super::deser_cql_value(
        &ColumnType::Native(Date),
        &mut ((1_u32 << 31) - 30).to_be_bytes().as_ref(),
    )
    .unwrap();

    assert_eq!(date.as_cql_date(), Some(before_epoch));

    // 2^31 + 30 when converted to NaiveDate is 1970-01-31
    let after_epoch = CqlDate((1 << 31) + 30);
    let date = super::deser_cql_value(
        &ColumnType::Native(Date),
        &mut ((1_u32 << 31) + 30).to_be_bytes().as_ref(),
    )
    .unwrap();

    assert_eq!(date.as_cql_date(), Some(after_epoch));

    // Min date
    let min_date = CqlDate(u32::MIN);
    let date = super::deser_cql_value(
        &ColumnType::Native(Date),
        &mut u32::MIN.to_be_bytes().as_ref(),
    )
    .unwrap();
    assert_eq!(date.as_cql_date(), Some(min_date));

    // Max date
    let max_date = CqlDate(u32::MAX);
    let date = super::deser_cql_value(
        &ColumnType::Native(Date),
        &mut u32::MAX.to_be_bytes().as_ref(),
    )
    .unwrap();
    assert_eq!(date.as_cql_date(), Some(max_date));
}

#[cfg(feature = "chrono-04")]
#[test]
fn test_naive_date_04_from_cql() {
    use chrono_04::NaiveDate;

    // 2^31 when converted to NaiveDate is 1970-01-01
    let unix_epoch = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
    let date = super::deser_cql_value(
        &ColumnType::Native(Date),
        &mut (1u32 << 31).to_be_bytes().as_ref(),
    )
    .unwrap();

    assert_eq!(date.as_naive_date_04(), Some(unix_epoch));

    // 2^31 - 30 when converted to NaiveDate is 1969-12-02
    let before_epoch = NaiveDate::from_ymd_opt(1969, 12, 2).unwrap();
    let date = super::deser_cql_value(
        &ColumnType::Native(Date),
        &mut ((1u32 << 31) - 30).to_be_bytes().as_ref(),
    )
    .unwrap();

    assert_eq!(date.as_naive_date_04(), Some(before_epoch));

    // 2^31 + 30 when converted to NaiveDate is 1970-01-31
    let after_epoch = NaiveDate::from_ymd_opt(1970, 1, 31).unwrap();
    let date = super::deser_cql_value(
        &ColumnType::Native(Date),
        &mut ((1u32 << 31) + 30).to_be_bytes().as_ref(),
    )
    .unwrap();

    assert_eq!(date.as_naive_date_04(), Some(after_epoch));

    // 0 and u32::MAX are out of NaiveDate range, fails with an error, not panics
    assert_eq!(
        super::deser_cql_value(&ColumnType::Native(Date), &mut 0_u32.to_be_bytes().as_ref())
            .unwrap()
            .as_naive_date_04(),
        None
    );

    assert_eq!(
        super::deser_cql_value(
            &ColumnType::Native(Date),
            &mut u32::MAX.to_be_bytes().as_ref()
        )
        .unwrap()
        .as_naive_date_04(),
        None
    );
}

#[cfg(feature = "time-03")]
#[test]
fn test_date_03_from_cql() {
    use time_03::Date;
    use time_03::Month::*;

    // 2^31 when converted to time_03::Date is 1970-01-01
    let unix_epoch = Date::from_calendar_date(1970, January, 1).unwrap();
    let date = super::deser_cql_value(
        &ColumnType::Native(Date),
        &mut (1u32 << 31).to_be_bytes().as_ref(),
    )
    .unwrap();

    assert_eq!(date.as_date_03(), Some(unix_epoch));

    // 2^31 - 30 when converted to time_03::Date is 1969-12-02
    let before_epoch = Date::from_calendar_date(1969, December, 2).unwrap();
    let date = super::deser_cql_value(
        &ColumnType::Native(Date),
        &mut ((1u32 << 31) - 30).to_be_bytes().as_ref(),
    )
    .unwrap();

    assert_eq!(date.as_date_03(), Some(before_epoch));

    // 2^31 + 30 when converted to time_03::Date is 1970-01-31
    let after_epoch = Date::from_calendar_date(1970, January, 31).unwrap();
    let date = super::deser_cql_value(
        &ColumnType::Native(Date),
        &mut ((1u32 << 31) + 30).to_be_bytes().as_ref(),
    )
    .unwrap();

    assert_eq!(date.as_date_03(), Some(after_epoch));

    // 0 and u32::MAX are out of NaiveDate range, fails with an error, not panics
    assert_eq!(
        super::deser_cql_value(&ColumnType::Native(Date), &mut 0_u32.to_be_bytes().as_ref())
            .unwrap()
            .as_date_03(),
        None
    );

    assert_eq!(
        super::deser_cql_value(
            &ColumnType::Native(Date),
            &mut u32::MAX.to_be_bytes().as_ref()
        )
        .unwrap()
        .as_date_03(),
        None
    );
}

#[test]
fn test_deserialize_time() {
    // Time is an i64 - nanoseconds since midnight
    // in range 0..=86399999999999

    let max_time: i64 = 24 * 60 * 60 * 1_000_000_000 - 1;
    assert_eq!(max_time, 86399999999999);

    // Check that basic values are deserialized correctly
    for test_val in [0, 1, 18463, max_time].iter() {
        let bytes: [u8; 8] = test_val.to_be_bytes();
        let cql_value: CqlValue =
            super::deser_cql_value(&ColumnType::Native(Time), &mut &bytes[..]).unwrap();
        assert_eq!(cql_value, CqlValue::Time(CqlTime(*test_val)));
    }

    // Negative values cause an error
    // Values bigger than 86399999999999 cause an error
    for test_val in [-1, i64::MIN, max_time + 1, i64::MAX].iter() {
        let bytes: [u8; 8] = test_val.to_be_bytes();
        super::deser_cql_value(&ColumnType::Native(Time), &mut &bytes[..]).unwrap_err();
    }
}

#[cfg(feature = "chrono-04")]
#[test]
fn test_naive_time_04_from_cql() {
    use chrono_04::NaiveTime;

    // 0 when converted to NaiveTime is 0:0:0.0
    let midnight = NaiveTime::from_hms_nano_opt(0, 0, 0, 0).unwrap();
    let time = super::deser_cql_value(
        &ColumnType::Native(Time),
        &mut (0i64).to_be_bytes().as_ref(),
    )
    .unwrap();

    assert_eq!(time.as_naive_time_04(), Some(midnight));

    // 10:10:30.500,000,001
    let (h, m, s, n) = (10, 10, 30, 500_000_001);
    let midnight = NaiveTime::from_hms_nano_opt(h, m, s, n).unwrap();
    let time = super::deser_cql_value(
        &ColumnType::Native(Time),
        &mut ((h as i64 * 3600 + m as i64 * 60 + s as i64) * 1_000_000_000 + n as i64)
            .to_be_bytes()
            .as_ref(),
    )
    .unwrap();

    assert_eq!(time.as_naive_time_04(), Some(midnight));

    // 23:59:59.999,999,999
    let (h, m, s, n) = (23, 59, 59, 999_999_999);
    let midnight = NaiveTime::from_hms_nano_opt(h, m, s, n).unwrap();
    let time = super::deser_cql_value(
        &ColumnType::Native(Time),
        &mut ((h as i64 * 3600 + m as i64 * 60 + s as i64) * 1_000_000_000 + n as i64)
            .to_be_bytes()
            .as_ref(),
    )
    .unwrap();

    assert_eq!(time.as_naive_time_04(), Some(midnight));
}

#[cfg(feature = "time-03")]
#[test]
fn test_primitive_time_03_from_cql() {
    use time_03::Time;

    // 0 when converted to NaiveTime is 0:0:0.0
    let midnight = Time::from_hms_nano(0, 0, 0, 0).unwrap();
    let time = super::deser_cql_value(
        &ColumnType::Native(Time),
        &mut (0i64).to_be_bytes().as_ref(),
    )
    .unwrap();

    assert_eq!(time.as_time_03(), Some(midnight));

    // 10:10:30.500,000,001
    let (h, m, s, n) = (10, 10, 30, 500_000_001);
    let midnight = Time::from_hms_nano(h, m, s, n).unwrap();
    let time = super::deser_cql_value(
        &ColumnType::Native(Time),
        &mut ((h as i64 * 3600 + m as i64 * 60 + s as i64) * 1_000_000_000 + n as i64)
            .to_be_bytes()
            .as_ref(),
    )
    .unwrap();

    assert_eq!(time.as_time_03(), Some(midnight));

    // 23:59:59.999,999,999
    let (h, m, s, n) = (23, 59, 59, 999_999_999);
    let midnight = Time::from_hms_nano(h, m, s, n).unwrap();
    let time = super::deser_cql_value(
        &ColumnType::Native(Time),
        &mut ((h as i64 * 3600 + m as i64 * 60 + s as i64) * 1_000_000_000 + n as i64)
            .to_be_bytes()
            .as_ref(),
    )
    .unwrap();

    assert_eq!(time.as_time_03(), Some(midnight));
}

#[test]
fn test_timestamp_deserialize() {
    // Timestamp is an i64 - milliseconds since unix epoch

    // Check that test values are deserialized correctly
    for test_val in &[0, -1, 1, 74568745, -4584658, i64::MIN, i64::MAX] {
        let bytes: [u8; 8] = test_val.to_be_bytes();
        let cql_value: CqlValue =
            super::deser_cql_value(&ColumnType::Native(Timestamp), &mut &bytes[..]).unwrap();
        assert_eq!(cql_value, CqlValue::Timestamp(CqlTimestamp(*test_val)));
    }
}

#[cfg(feature = "chrono-04")]
#[test]
fn test_datetime_04_from_cql() {
    use chrono_04::{DateTime, NaiveDate, NaiveDateTime, NaiveTime};

    // 0 when converted to DateTime is 1970-01-01 0:00:00.00
    let unix_epoch = DateTime::from_timestamp(0, 0).unwrap();
    let date = super::deser_cql_value(
        &ColumnType::Native(Timestamp),
        &mut 0i64.to_be_bytes().as_ref(),
    )
    .unwrap();

    assert_eq!(date.as_datetime_04(), Some(unix_epoch));

    // When converted to NaiveDateTime, this is 1969-12-01 11:29:29.5
    let timestamp: i64 = -((((30 * 24 + 12) * 60 + 30) * 60 + 30) * 1000 + 500);
    let before_epoch = NaiveDateTime::new(
        NaiveDate::from_ymd_opt(1969, 12, 1).unwrap(),
        NaiveTime::from_hms_milli_opt(11, 29, 29, 500).unwrap(),
    )
    .and_utc();
    let date = super::deser_cql_value(
        &ColumnType::Native(Timestamp),
        &mut timestamp.to_be_bytes().as_ref(),
    )
    .unwrap();

    assert_eq!(date.as_datetime_04(), Some(before_epoch));

    // when converted to NaiveDateTime, this is is 1970-01-31 12:30:30.5
    let timestamp: i64 = (((30 * 24 + 12) * 60 + 30) * 60 + 30) * 1000 + 500;
    let after_epoch = NaiveDateTime::new(
        NaiveDate::from_ymd_opt(1970, 1, 31).unwrap(),
        NaiveTime::from_hms_milli_opt(12, 30, 30, 500).unwrap(),
    )
    .and_utc();
    let date = super::deser_cql_value(
        &ColumnType::Native(Timestamp),
        &mut timestamp.to_be_bytes().as_ref(),
    )
    .unwrap();

    assert_eq!(date.as_datetime_04(), Some(after_epoch));

    // 0 and u32::MAX are out of NaiveDate range, fails with an error, not panics
    assert_eq!(
        super::deser_cql_value(
            &ColumnType::Native(Timestamp),
            &mut i64::MIN.to_be_bytes().as_ref()
        )
        .unwrap()
        .as_datetime_04(),
        None
    );

    assert_eq!(
        super::deser_cql_value(
            &ColumnType::Native(Timestamp),
            &mut i64::MAX.to_be_bytes().as_ref()
        )
        .unwrap()
        .as_datetime_04(),
        None
    );
}

#[cfg(feature = "time-03")]
#[test]
fn test_offset_datetime_03_from_cql() {
    use time_03::{Date, Month::*, OffsetDateTime, PrimitiveDateTime, Time};

    // 0 when converted to OffsetDateTime is 1970-01-01 0:00:00.00
    let unix_epoch = OffsetDateTime::from_unix_timestamp(0).unwrap();
    let date = super::deser_cql_value(
        &ColumnType::Native(Timestamp),
        &mut 0i64.to_be_bytes().as_ref(),
    )
    .unwrap();

    assert_eq!(date.as_offset_date_time_03(), Some(unix_epoch));

    // When converted to NaiveDateTime, this is 1969-12-01 11:29:29.5
    let timestamp: i64 = -((((30 * 24 + 12) * 60 + 30) * 60 + 30) * 1000 + 500);
    let before_epoch = PrimitiveDateTime::new(
        Date::from_calendar_date(1969, December, 1).unwrap(),
        Time::from_hms_milli(11, 29, 29, 500).unwrap(),
    )
    .assume_utc();
    let date = super::deser_cql_value(
        &ColumnType::Native(Timestamp),
        &mut timestamp.to_be_bytes().as_ref(),
    )
    .unwrap();

    assert_eq!(date.as_offset_date_time_03(), Some(before_epoch));

    // when converted to NaiveDateTime, this is is 1970-01-31 12:30:30.5
    let timestamp: i64 = (((30 * 24 + 12) * 60 + 30) * 60 + 30) * 1000 + 500;
    let after_epoch = PrimitiveDateTime::new(
        Date::from_calendar_date(1970, January, 31).unwrap(),
        Time::from_hms_milli(12, 30, 30, 500).unwrap(),
    )
    .assume_utc();
    let date = super::deser_cql_value(
        &ColumnType::Native(Timestamp),
        &mut timestamp.to_be_bytes().as_ref(),
    )
    .unwrap();

    assert_eq!(date.as_offset_date_time_03(), Some(after_epoch));

    // 0 and u32::MAX are out of NaiveDate range, fails with an error, not panics
    assert_eq!(
        super::deser_cql_value(
            &ColumnType::Native(Timestamp),
            &mut i64::MIN.to_be_bytes().as_ref()
        )
        .unwrap()
        .as_offset_date_time_03(),
        None
    );

    assert_eq!(
        super::deser_cql_value(
            &ColumnType::Native(Timestamp),
            &mut i64::MAX.to_be_bytes().as_ref()
        )
        .unwrap()
        .as_offset_date_time_03(),
        None
    );
}

#[test]
fn test_serialize_empty() {
    use crate::serialize::value::SerializeValue;
    let empty = CqlValue::Empty;
    let mut v = Vec::new();
    SerializeValue::serialize(
        &empty,
        &ColumnType::Native(NativeType::Ascii),
        CellWriter::new(&mut v),
    )
    .unwrap();

    assert_eq!(v, vec![0, 0, 0, 0]);
}

#[test]
fn test_duration_deserialize() {
    let bytes = [0xc, 0x12, 0xe2, 0x8c, 0x39, 0xd2];
    let cql_value: CqlValue =
        super::deser_cql_value(&ColumnType::Native(Duration), &mut &bytes[..]).unwrap();
    assert_eq!(
        cql_value,
        CqlValue::Duration(CqlDuration {
            months: 6,
            days: 9,
            nanoseconds: 21372137
        })
    );
}

#[test]
fn test_deserialize_empty_payload() {
    for (test_type, res_cql) in [
        (ColumnType::Native(Ascii), CqlValue::Ascii("".to_owned())),
        (ColumnType::Native(Boolean), CqlValue::Empty),
        (ColumnType::Native(Blob), CqlValue::Blob(vec![])),
        (ColumnType::Native(NativeType::Counter), CqlValue::Empty),
        (ColumnType::Native(Date), CqlValue::Empty),
        (ColumnType::Native(Decimal), CqlValue::Empty),
        (ColumnType::Native(Double), CqlValue::Empty),
        (ColumnType::Native(Float), CqlValue::Empty),
        (ColumnType::Native(Int), CqlValue::Empty),
        (ColumnType::Native(BigInt), CqlValue::Empty),
        (ColumnType::Native(Text), CqlValue::Text("".to_owned())),
        (ColumnType::Native(Timestamp), CqlValue::Empty),
        (ColumnType::Native(Inet), CqlValue::Empty),
        (
            ColumnType::Collection {
                frozen: false,
                typ: CollectionType::List(Box::new(ColumnType::Native(Int))),
            },
            CqlValue::Empty,
        ),
        (
            ColumnType::Collection {
                frozen: false,
                typ: CollectionType::Map(
                    Box::new(ColumnType::Native(Int)),
                    Box::new(ColumnType::Native(Int)),
                ),
            },
            CqlValue::Empty,
        ),
        (
            ColumnType::Collection {
                frozen: false,
                typ: CollectionType::Set(Box::new(ColumnType::Native(Int))),
            },
            CqlValue::Empty,
        ),
        (
            ColumnType::UserDefinedType {
                frozen: false,
                definition: Arc::new(UserDefinedType {
                    name: "".into(),
                    keyspace: "".into(),
                    field_types: vec![],
                }),
            },
            CqlValue::Empty,
        ),
        (ColumnType::Native(SmallInt), CqlValue::Empty),
        (ColumnType::Native(TinyInt), CqlValue::Empty),
        (ColumnType::Native(Time), CqlValue::Empty),
        (ColumnType::Native(Timeuuid), CqlValue::Empty),
        (ColumnType::Tuple(vec![]), CqlValue::Empty),
        (ColumnType::Native(Uuid), CqlValue::Empty),
        (ColumnType::Native(Varint), CqlValue::Empty),
    ] {
        let cql_value: CqlValue = super::deser_cql_value(&test_type, &mut &[][..]).unwrap();

        assert_eq!(cql_value, res_cql);
    }
}

#[test]
fn test_timeuuid_deserialize() {
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

    for (uuid_str, uuid_bytes) in &tests {
        let cql_val: CqlValue =
            super::deser_cql_value(&ColumnType::Native(Timeuuid), &mut &uuid_bytes[..]).unwrap();

        match cql_val {
            CqlValue::Timeuuid(uuid) => {
                assert_eq!(uuid.as_bytes(), uuid_bytes);
                assert_eq!(CqlTimeuuid::from_str(uuid_str).unwrap(), uuid);
            }
            _ => panic!("Timeuuid parsed as wrong CqlValue"),
        }
    }
}
