use crate::frame::response::result::{CollectionType, ColumnType, NativeType, UserDefinedType};
use crate::serialize::value::{
    BuiltinSerializationError, BuiltinSerializationErrorKind, BuiltinTypeCheckError,
    BuiltinTypeCheckErrorKind, MapSerializationErrorKind, MapTypeCheckErrorKind, SerializeValue,
    SetOrListSerializationErrorKind, SetOrListTypeCheckErrorKind, TupleSerializationErrorKind,
    TupleTypeCheckErrorKind, UdtSerializationErrorKind, UdtTypeCheckErrorKind,
};
use crate::serialize::{CellWriter, SerializationError};
use crate::value::{
    Counter, CqlDate, CqlDuration, CqlTime, CqlTimestamp, CqlTimeuuid, CqlValue, CqlVarint,
    MaybeUnset, Unset,
};
use crate::SerializeValue;

use std::collections::hash_map::DefaultHasher;
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::hash::{BuildHasherDefault, Hash, Hasher};
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};
use std::str::FromStr;
use std::sync::Arc;

use assert_matches::assert_matches;
use uuid::Uuid;

#[test]
fn test_dyn_serialize_value() {
    let v: i32 = 123;
    let mut typed_data = Vec::new();
    let typed_data_writer = CellWriter::new(&mut typed_data);
    <_ as SerializeValue>::serialize(&v, &ColumnType::Native(NativeType::Int), typed_data_writer)
        .unwrap();

    let v = &v as &dyn SerializeValue;
    let mut erased_data = Vec::new();
    let erased_data_writer = CellWriter::new(&mut erased_data);
    <_ as SerializeValue>::serialize(&v, &ColumnType::Native(NativeType::Int), erased_data_writer)
        .unwrap();

    assert_eq!(typed_data, erased_data);
}

fn do_serialize_result<T: SerializeValue>(
    t: T,
    typ: &ColumnType,
) -> Result<Vec<u8>, SerializationError> {
    let mut ret = Vec::new();
    let writer = CellWriter::new(&mut ret);
    t.serialize(typ, writer).map(|_| ()).map(|()| ret)
}

pub(crate) fn do_serialize<T: SerializeValue>(t: T, typ: &ColumnType) -> Vec<u8> {
    do_serialize_result(t, typ).unwrap()
}

fn do_serialize_err<T: SerializeValue>(t: T, typ: &ColumnType) -> SerializationError {
    do_serialize_result(t, typ).unwrap_err()
}

fn get_typeck_err(err: &SerializationError) -> &BuiltinTypeCheckError {
    match err.0.downcast_ref() {
        Some(err) => err,
        None => panic!("not a BuiltinTypeCheckError: {}", err),
    }
}

pub(crate) fn get_ser_err(err: &SerializationError) -> &BuiltinSerializationError {
    match err.0.downcast_ref() {
        Some(err) => err,
        None => panic!("not a BuiltinSerializationError: {}", err),
    }
}

#[test]
fn test_native_errors() {
    // Simple type mismatch
    let v = 123_i32;
    let err = do_serialize_err(v, &ColumnType::Native(NativeType::Double));
    let err = get_typeck_err(&err);
    assert_eq!(err.rust_name, std::any::type_name::<i32>());
    assert_eq!(err.got, ColumnType::Native(NativeType::Double));
    assert_matches!(
        err.kind,
        BuiltinTypeCheckErrorKind::MismatchedType {
            expected: &[ColumnType::Native(NativeType::Int)],
        }
    );

    // str (and also Uuid) are interesting because they accept two types,
    // also check str here
    let v = "Ala ma kota";
    let err = do_serialize_err(v, &ColumnType::Native(NativeType::Double));
    let err = get_typeck_err(&err);
    assert_eq!(err.rust_name, std::any::type_name::<&str>());
    assert_eq!(err.got, ColumnType::Native(NativeType::Double));
    assert_matches!(
        err.kind,
        BuiltinTypeCheckErrorKind::MismatchedType {
            expected: &[
                ColumnType::Native(NativeType::Ascii),
                ColumnType::Native(NativeType::Text)
            ],
        }
    );

    // We'll skip testing for SizeOverflow as this would require producing
    // a value which is at least 2GB in size.
}

#[cfg(feature = "bigdecimal-04")]
#[test]
fn test_native_errors_bigdecimal_04() {
    use bigdecimal_04::num_bigint::BigInt;
    use bigdecimal_04::BigDecimal;

    // Value overflow (type out of representable range)
    let v = BigDecimal::new(BigInt::from(123), 1i64 << 40);
    let err = do_serialize_err(v, &ColumnType::Native(NativeType::Decimal));
    let err = get_ser_err(&err);
    assert_eq!(err.rust_name, std::any::type_name::<BigDecimal>());
    assert_eq!(err.got, ColumnType::Native(NativeType::Decimal));
    assert_matches!(err.kind, BuiltinSerializationErrorKind::ValueOverflow);
}

#[test]
fn test_set_or_list_errors() {
    // Not a set or list
    let v = vec![123_i32];
    let err = do_serialize_err(v, &ColumnType::Native(NativeType::Double));
    let err = get_typeck_err(&err);
    assert_eq!(err.rust_name, std::any::type_name::<Vec<i32>>());
    assert_eq!(err.got, ColumnType::Native(NativeType::Double));
    assert_matches!(
        err.kind,
        BuiltinTypeCheckErrorKind::SetOrListError(SetOrListTypeCheckErrorKind::NotSetOrList)
    );

    // Trick: Unset is a ZST, so [Unset; 1 << 33] is a ZST, too.
    // While it's probably incorrect to use Unset in a collection, this
    // allows us to trigger the right error without going out of memory.
    // Such an array is also created instantaneously.
    let v = &[Unset; 1 << 33] as &[Unset];
    let typ = ColumnType::Collection {
        frozen: false,
        typ: CollectionType::List(Box::new(ColumnType::Native(NativeType::Int))),
    };
    let err = do_serialize_err(v, &typ);
    let err = get_ser_err(&err);
    assert_eq!(err.rust_name, std::any::type_name::<&[Unset]>());
    assert_eq!(err.got, typ);
    assert_matches!(
        err.kind,
        BuiltinSerializationErrorKind::SetOrListError(
            SetOrListSerializationErrorKind::TooManyElements
        )
    );

    // Error during serialization of an element
    let v = vec![123_i32];
    let typ = ColumnType::Collection {
        frozen: false,
        typ: CollectionType::List(Box::new(ColumnType::Native(NativeType::Double))),
    };
    let err = do_serialize_err(v, &typ);
    let err = get_ser_err(&err);
    assert_eq!(err.rust_name, std::any::type_name::<Vec<i32>>());
    assert_eq!(err.got, typ);
    let BuiltinSerializationErrorKind::SetOrListError(
        SetOrListSerializationErrorKind::ElementSerializationFailed(err),
    ) = &err.kind
    else {
        panic!("unexpected error kind: {}", err.kind)
    };
    let err = get_typeck_err(err);
    assert_matches!(
        err.kind,
        BuiltinTypeCheckErrorKind::MismatchedType {
            expected: &[ColumnType::Native(NativeType::Int)],
        }
    );
}

#[test]
fn test_map_errors() {
    // Not a map
    let v = BTreeMap::from([("foo", "bar")]);
    let err = do_serialize_err(v, &ColumnType::Native(NativeType::Double));
    let err = get_typeck_err(&err);
    assert_eq!(err.rust_name, std::any::type_name::<BTreeMap<&str, &str>>());
    assert_eq!(err.got, ColumnType::Native(NativeType::Double));
    assert_matches!(
        err.kind,
        BuiltinTypeCheckErrorKind::MapError(MapTypeCheckErrorKind::NotMap)
    );

    // It's not practical to check the TooManyElements error as it would
    // require allocating a huge amount of memory.

    // Error during serialization of a key
    let v = BTreeMap::from([(123_i32, 456_i32)]);
    let typ = ColumnType::Collection {
        frozen: false,
        typ: CollectionType::Map(
            Box::new(ColumnType::Native(NativeType::Double)),
            Box::new(ColumnType::Native(NativeType::Int)),
        ),
    };
    let err = do_serialize_err(v, &typ);
    let err = get_ser_err(&err);
    assert_eq!(err.rust_name, std::any::type_name::<BTreeMap<i32, i32>>());
    assert_eq!(err.got, typ);
    let BuiltinSerializationErrorKind::MapError(MapSerializationErrorKind::KeySerializationFailed(
        err,
    )) = &err.kind
    else {
        panic!("unexpected error kind: {}", err.kind)
    };
    let err = get_typeck_err(err);
    assert_matches!(
        err.kind,
        BuiltinTypeCheckErrorKind::MismatchedType {
            expected: &[ColumnType::Native(NativeType::Int)],
        }
    );

    // Error during serialization of a value
    let v = BTreeMap::from([(123_i32, 456_i32)]);
    let typ = ColumnType::Collection {
        frozen: false,
        typ: CollectionType::Map(
            Box::new(ColumnType::Native(NativeType::Int)),
            Box::new(ColumnType::Native(NativeType::Double)),
        ),
    };
    let err = do_serialize_err(v, &typ);
    let err = get_ser_err(&err);
    assert_eq!(err.rust_name, std::any::type_name::<BTreeMap<i32, i32>>());
    assert_eq!(err.got, typ);
    let BuiltinSerializationErrorKind::MapError(
        MapSerializationErrorKind::ValueSerializationFailed(err),
    ) = &err.kind
    else {
        panic!("unexpected error kind: {}", err.kind)
    };
    let err = get_typeck_err(err);
    assert_matches!(
        err.kind,
        BuiltinTypeCheckErrorKind::MismatchedType {
            expected: &[ColumnType::Native(NativeType::Int)],
        }
    );
}

#[test]
fn test_tuple_errors() {
    // Not a tuple
    let v = (123_i32, 456_i32, 789_i32);
    let err = do_serialize_err(v, &ColumnType::Native(NativeType::Double));
    let err = get_typeck_err(&err);
    assert_eq!(err.rust_name, std::any::type_name::<(i32, i32, i32)>());
    assert_eq!(err.got, ColumnType::Native(NativeType::Double));
    assert_matches!(
        err.kind,
        BuiltinTypeCheckErrorKind::TupleError(TupleTypeCheckErrorKind::NotTuple)
    );

    // The Rust tuple has more elements than the CQL type
    let v = (123_i32, 456_i32, 789_i32);
    let typ = ColumnType::Tuple(vec![ColumnType::Native(NativeType::Int); 2]);
    let err = do_serialize_err(v, &typ);
    let err = get_typeck_err(&err);
    assert_eq!(err.rust_name, std::any::type_name::<(i32, i32, i32)>());
    assert_eq!(err.got, typ);
    assert_matches!(
        err.kind,
        BuiltinTypeCheckErrorKind::TupleError(TupleTypeCheckErrorKind::WrongElementCount {
            rust_type_el_count: 3,
            cql_type_el_count: 2,
        })
    );

    // Error during serialization of one of the elements
    let v = (123_i32, "Ala ma kota", 789.0_f64);
    let typ = ColumnType::Tuple(vec![
        ColumnType::Native(NativeType::Int),
        ColumnType::Native(NativeType::Text),
        ColumnType::Native(NativeType::Uuid),
    ]);
    let err = do_serialize_err(v, &typ);
    let err = get_ser_err(&err);
    assert_eq!(err.rust_name, std::any::type_name::<(i32, &str, f64)>());
    assert_eq!(err.got, typ);
    let BuiltinSerializationErrorKind::TupleError(
        TupleSerializationErrorKind::ElementSerializationFailed { index: 2, err },
    ) = &err.kind
    else {
        panic!("unexpected error kind: {}", err.kind)
    };
    let err = get_typeck_err(err);
    assert_matches!(
        err.kind,
        BuiltinTypeCheckErrorKind::MismatchedType {
            expected: &[ColumnType::Native(NativeType::Double)],
        }
    );
}

#[test]
fn test_cql_value_errors() {
    // Tried to encode Empty value into a non-emptyable type
    let v = CqlValue::Empty;
    let err = do_serialize_err(v, &ColumnType::Native(NativeType::Counter));
    let err = get_typeck_err(&err);
    assert_eq!(err.rust_name, std::any::type_name::<CqlValue>());
    assert_eq!(err.got, ColumnType::Native(NativeType::Counter));
    assert_matches!(err.kind, BuiltinTypeCheckErrorKind::NotEmptyable);

    // Handle tuples and UDTs in separate tests, as they have some
    // custom logic
}

#[test]
fn test_cql_value_tuple_errors() {
    // Not a tuple
    let v = CqlValue::Tuple(vec![
        Some(CqlValue::Int(123_i32)),
        Some(CqlValue::Int(456_i32)),
        Some(CqlValue::Int(789_i32)),
    ]);
    let err = do_serialize_err(v, &ColumnType::Native(NativeType::Double));
    let err = get_typeck_err(&err);
    assert_eq!(err.rust_name, std::any::type_name::<CqlValue>());
    assert_eq!(err.got, ColumnType::Native(NativeType::Double));
    assert_matches!(
        err.kind,
        BuiltinTypeCheckErrorKind::TupleError(TupleTypeCheckErrorKind::NotTuple)
    );

    // The Rust tuple has more elements than the CQL type
    let v = CqlValue::Tuple(vec![
        Some(CqlValue::Int(123_i32)),
        Some(CqlValue::Int(456_i32)),
        Some(CqlValue::Int(789_i32)),
    ]);
    let typ = ColumnType::Tuple(vec![ColumnType::Native(NativeType::Int); 2]);
    let err = do_serialize_err(v, &typ);
    let err = get_typeck_err(&err);
    assert_eq!(err.rust_name, std::any::type_name::<CqlValue>());
    assert_eq!(err.got, typ);
    assert_matches!(
        err.kind,
        BuiltinTypeCheckErrorKind::TupleError(TupleTypeCheckErrorKind::WrongElementCount {
            rust_type_el_count: 3,
            cql_type_el_count: 2,
        })
    );

    // Error during serialization of one of the elements
    let v = CqlValue::Tuple(vec![
        Some(CqlValue::Int(123_i32)),
        Some(CqlValue::Text("Ala ma kota".to_string())),
        Some(CqlValue::Double(789_f64)),
    ]);
    let typ = ColumnType::Tuple(vec![
        ColumnType::Native(NativeType::Int),
        ColumnType::Native(NativeType::Text),
        ColumnType::Native(NativeType::Uuid),
    ]);
    let err = do_serialize_err(v, &typ);
    let err = get_ser_err(&err);
    assert_eq!(err.rust_name, std::any::type_name::<CqlValue>());
    assert_eq!(err.got, typ);
    let BuiltinSerializationErrorKind::TupleError(
        TupleSerializationErrorKind::ElementSerializationFailed { index: 2, err },
    ) = &err.kind
    else {
        panic!("unexpected error kind: {}", err.kind)
    };
    let err = get_typeck_err(err);
    assert_matches!(
        err.kind,
        BuiltinTypeCheckErrorKind::MismatchedType {
            expected: &[ColumnType::Native(NativeType::Double)],
        }
    );
}

#[test]
fn test_cql_value_udt_errors() {
    // Not a UDT
    let v = CqlValue::UserDefinedType {
        keyspace: "ks".to_string(),
        name: "udt".to_string(),
        fields: vec![
            ("a".to_string(), Some(CqlValue::Int(123_i32))),
            ("b".to_string(), Some(CqlValue::Int(456_i32))),
            ("c".to_string(), Some(CqlValue::Int(789_i32))),
        ],
    };
    let err = do_serialize_err(v, &ColumnType::Native(NativeType::Double));
    let err = get_typeck_err(&err);
    assert_eq!(err.rust_name, std::any::type_name::<CqlValue>());
    assert_eq!(err.got, ColumnType::Native(NativeType::Double));
    assert_matches!(
        err.kind,
        BuiltinTypeCheckErrorKind::UdtError(UdtTypeCheckErrorKind::NotUdt)
    );

    // Wrong type name
    let v = CqlValue::UserDefinedType {
        keyspace: "ks".to_string(),
        name: "udt".to_string(),
        fields: vec![
            ("a".to_string(), Some(CqlValue::Int(123_i32))),
            ("b".to_string(), Some(CqlValue::Int(456_i32))),
            ("c".to_string(), Some(CqlValue::Int(789_i32))),
        ],
    };
    let typ = ColumnType::UserDefinedType {
        frozen: false,
        definition: Arc::new(UserDefinedType {
            name: "udt2".into(),
            keyspace: "ks".into(),
            field_types: vec![
                ("a".into(), ColumnType::Native(NativeType::Int)),
                ("b".into(), ColumnType::Native(NativeType::Int)),
                ("c".into(), ColumnType::Native(NativeType::Int)),
            ],
        }),
    };
    let err = do_serialize_err(v, &typ);
    let err = get_typeck_err(&err);
    assert_eq!(err.rust_name, std::any::type_name::<CqlValue>());
    assert_eq!(err.got, typ);
    let BuiltinTypeCheckErrorKind::UdtError(UdtTypeCheckErrorKind::NameMismatch {
        keyspace,
        type_name,
    }) = &err.kind
    else {
        panic!("unexpected error kind: {}", err.kind)
    };
    assert_eq!(keyspace, "ks");
    assert_eq!(type_name, "udt2");

    // Some fields are missing from the CQL type
    let v = CqlValue::UserDefinedType {
        keyspace: "ks".to_string(),
        name: "udt".to_string(),
        fields: vec![
            ("a".to_string(), Some(CqlValue::Int(123_i32))),
            ("b".to_string(), Some(CqlValue::Int(456_i32))),
            ("c".to_string(), Some(CqlValue::Int(789_i32))),
        ],
    };
    let typ = ColumnType::UserDefinedType {
        frozen: false,
        definition: Arc::new(UserDefinedType {
            name: "udt".into(),
            keyspace: "ks".into(),
            field_types: vec![
                ("a".into(), ColumnType::Native(NativeType::Int)),
                ("b".into(), ColumnType::Native(NativeType::Int)),
                // c is missing
            ],
        }),
    };
    let err = do_serialize_err(v, &typ);
    let err = get_typeck_err(&err);
    assert_eq!(err.rust_name, std::any::type_name::<CqlValue>());
    assert_eq!(err.got, typ);
    let BuiltinTypeCheckErrorKind::UdtError(UdtTypeCheckErrorKind::NoSuchFieldInUdt { field_name }) =
        &err.kind
    else {
        panic!("unexpected error kind: {}", err.kind)
    };
    assert_eq!(field_name, "c");

    // It is allowed for a Rust UDT to have less fields than the CQL UDT,
    // so skip UnexpectedFieldInDestination.

    // Error during serialization of one of the fields
    let v = CqlValue::UserDefinedType {
        keyspace: "ks".to_string(),
        name: "udt".to_string(),
        fields: vec![
            ("a".to_string(), Some(CqlValue::Int(123_i32))),
            ("b".to_string(), Some(CqlValue::Int(456_i32))),
            ("c".to_string(), Some(CqlValue::Int(789_i32))),
        ],
    };
    let typ = ColumnType::UserDefinedType {
        frozen: false,
        definition: Arc::new(UserDefinedType {
            name: "udt".into(),
            keyspace: "ks".into(),
            field_types: vec![
                ("a".into(), ColumnType::Native(NativeType::Int)),
                ("b".into(), ColumnType::Native(NativeType::Int)),
                ("c".into(), ColumnType::Native(NativeType::Double)),
            ],
        }),
    };
    let err = do_serialize_err(v, &typ);
    let err = get_ser_err(&err);
    assert_eq!(err.rust_name, std::any::type_name::<CqlValue>());
    assert_eq!(err.got, typ);
    let BuiltinSerializationErrorKind::UdtError(
        UdtSerializationErrorKind::FieldSerializationFailed { field_name, err },
    ) = &err.kind
    else {
        panic!("unexpected error kind: {}", err.kind)
    };
    assert_eq!(field_name, "c");
    let err = get_typeck_err(err);
    assert_matches!(
        err.kind,
        BuiltinTypeCheckErrorKind::MismatchedType {
            expected: &[ColumnType::Native(NativeType::Int)],
        }
    );
}

// Do not remove. It's not used in tests but we keep it here to check that
// we properly ignore warnings about unused variables, unnecessary `mut`s
// etc. that usually pop up when generating code for empty structs.
#[expect(unused)]
#[derive(SerializeValue)]
#[scylla(crate = crate)]
struct TestUdtWithNoFields {}

#[derive(SerializeValue, Debug, PartialEq, Eq, Default)]
#[scylla(crate = crate)]
struct TestUdtWithFieldSorting {
    a: String,
    b: i32,
    c: Vec<i64>,
}

#[test]
fn test_udt_serialization_with_field_sorting_correct_order() {
    let typ = ColumnType::UserDefinedType {
        frozen: false,
        definition: Arc::new(UserDefinedType {
            name: "typ".into(),
            keyspace: "ks".into(),
            field_types: vec![
                ("a".into(), ColumnType::Native(NativeType::Text)),
                ("b".into(), ColumnType::Native(NativeType::Int)),
                (
                    "c".into(),
                    ColumnType::Collection {
                        frozen: false,
                        typ: CollectionType::List(Box::new(ColumnType::Native(NativeType::BigInt))),
                    },
                ),
            ],
        }),
    };

    let reference = do_serialize(
        CqlValue::UserDefinedType {
            keyspace: "ks".to_string(),
            name: "typ".to_string(),
            fields: vec![
                (
                    "a".to_string(),
                    Some(CqlValue::Text(String::from("Ala ma kota"))),
                ),
                ("b".to_string(), Some(CqlValue::Int(42))),
                (
                    "c".to_string(),
                    Some(CqlValue::List(vec![
                        CqlValue::BigInt(1),
                        CqlValue::BigInt(2),
                        CqlValue::BigInt(3),
                    ])),
                ),
            ],
        },
        &typ,
    );
    let udt = do_serialize(
        TestUdtWithFieldSorting {
            a: "Ala ma kota".to_owned(),
            b: 42,
            c: vec![1, 2, 3],
        },
        &typ,
    );

    assert_eq!(reference, udt);
}

#[test]
fn test_udt_serialization_with_field_sorting_incorrect_order() {
    let typ = ColumnType::UserDefinedType {
        frozen: false,
        definition: Arc::new(UserDefinedType {
            name: "typ".into(),
            keyspace: "ks".into(),
            field_types: vec![
                // Two first columns are swapped
                ("b".into(), ColumnType::Native(NativeType::Int)),
                ("a".into(), ColumnType::Native(NativeType::Text)),
                (
                    "c".into(),
                    ColumnType::Collection {
                        frozen: false,
                        typ: CollectionType::List(Box::new(ColumnType::Native(NativeType::BigInt))),
                    },
                ),
            ],
        }),
    };

    let reference = do_serialize(
        CqlValue::UserDefinedType {
            keyspace: "ks".to_string(),
            name: "typ".to_string(),
            fields: vec![
                // FIXME: UDTs in CqlValue should also honor the order
                // For now, it's swapped here as well
                ("b".to_string(), Some(CqlValue::Int(42))),
                (
                    "a".to_string(),
                    Some(CqlValue::Text(String::from("Ala ma kota"))),
                ),
                (
                    "c".to_string(),
                    Some(CqlValue::List(vec![
                        CqlValue::BigInt(1),
                        CqlValue::BigInt(2),
                        CqlValue::BigInt(3),
                    ])),
                ),
            ],
        },
        &typ,
    );
    let udt = do_serialize(
        TestUdtWithFieldSorting {
            a: "Ala ma kota".to_owned(),
            b: 42,
            c: vec![1, 2, 3],
        },
        &typ,
    );

    assert_eq!(reference, udt);
}

#[test]
fn test_udt_serialization_with_missing_rust_fields_at_end() {
    let udt = TestUdtWithFieldSorting::default();

    let typ_normal = ColumnType::UserDefinedType {
        frozen: false,
        definition: Arc::new(UserDefinedType {
            name: "typ".into(),
            keyspace: "ks".into(),
            field_types: vec![
                ("a".into(), ColumnType::Native(NativeType::Text)),
                ("b".into(), ColumnType::Native(NativeType::Int)),
                (
                    "c".into(),
                    ColumnType::Collection {
                        frozen: false,
                        typ: CollectionType::List(Box::new(ColumnType::Native(NativeType::BigInt))),
                    },
                ),
            ],
        }),
    };

    let typ_unexpected_field = ColumnType::UserDefinedType {
        frozen: false,
        definition: Arc::new(UserDefinedType {
            name: "typ".into(),
            keyspace: "ks".into(),
            field_types: vec![
                ("a".into(), ColumnType::Native(NativeType::Text)),
                ("b".into(), ColumnType::Native(NativeType::Int)),
                (
                    "c".into(),
                    ColumnType::Collection {
                        frozen: false,
                        typ: CollectionType::List(Box::new(ColumnType::Native(NativeType::BigInt))),
                    },
                ),
                // Unexpected fields
                ("d".into(), ColumnType::Native(NativeType::Counter)),
                ("e".into(), ColumnType::Native(NativeType::Counter)),
            ],
        }),
    };

    let result_normal = do_serialize(&udt, &typ_normal);
    let result_additional_field = do_serialize(&udt, &typ_unexpected_field);

    assert_eq!(result_normal, result_additional_field);
}

#[derive(SerializeValue, Debug, PartialEq, Default)]
#[scylla(crate = crate)]
struct TestUdtWithFieldSorting2 {
    a: String,
    b: i32,
    d: Option<Counter>,
    c: Vec<i64>,
}

#[derive(SerializeValue, Debug, PartialEq, Default)]
#[scylla(crate = crate)]
struct TestUdtWithFieldSorting3 {
    a: String,
    b: i32,
    d: Option<Counter>,
    e: Option<f32>,
    c: Vec<i64>,
}

#[test]
fn test_udt_serialization_with_missing_rust_field_in_middle() {
    let udt = TestUdtWithFieldSorting::default();
    let udt2 = TestUdtWithFieldSorting2::default();
    let udt3 = TestUdtWithFieldSorting3::default();

    let typ = ColumnType::UserDefinedType {
        frozen: false,
        definition: Arc::new(UserDefinedType {
            name: "typ".into(),
            keyspace: "ks".into(),
            field_types: vec![
                ("a".into(), ColumnType::Native(NativeType::Text)),
                ("b".into(), ColumnType::Native(NativeType::Int)),
                // Unexpected fields
                ("d".into(), ColumnType::Native(NativeType::Counter)),
                ("e".into(), ColumnType::Native(NativeType::Float)),
                // Remaining normal field
                (
                    "c".into(),
                    ColumnType::Collection {
                        frozen: false,
                        typ: CollectionType::List(Box::new(ColumnType::Native(NativeType::BigInt))),
                    },
                ),
            ],
        }),
    };

    let result_1 = do_serialize(udt, &typ);
    let result_2 = do_serialize(udt2, &typ);
    let result_3 = do_serialize(udt3, &typ);

    assert_eq!(result_1, result_2);
    assert_eq!(result_2, result_3);
}

#[test]
fn test_udt_serialization_failing_type_check() {
    let typ_not_udt = ColumnType::Native(NativeType::Ascii);
    let udt = TestUdtWithFieldSorting::default();
    let mut data = Vec::new();

    let err = udt
        .serialize(&typ_not_udt, CellWriter::new(&mut data))
        .unwrap_err();
    let err = err.0.downcast_ref::<BuiltinTypeCheckError>().unwrap();
    assert_matches!(
        err.kind,
        BuiltinTypeCheckErrorKind::UdtError(UdtTypeCheckErrorKind::NotUdt)
    );

    let typ_without_c = ColumnType::UserDefinedType {
        frozen: false,
        definition: Arc::new(UserDefinedType {
            name: "typ".into(),
            keyspace: "ks".into(),
            field_types: vec![
                ("a".into(), ColumnType::Native(NativeType::Text)),
                ("b".into(), ColumnType::Native(NativeType::Int)),
                // Last field is missing
            ],
        }),
    };

    let err = udt
        .serialize(&typ_without_c, CellWriter::new(&mut data))
        .unwrap_err();
    let err = err.0.downcast_ref::<BuiltinTypeCheckError>().unwrap();
    assert_matches!(
        err.kind,
        BuiltinTypeCheckErrorKind::UdtError(UdtTypeCheckErrorKind::ValueMissingForUdtField { .. })
    );

    let typ_wrong_type = ColumnType::UserDefinedType {
        frozen: false,
        definition: Arc::new(UserDefinedType {
            name: "typ".into(),
            keyspace: "ks".into(),
            field_types: vec![
                ("a".into(), ColumnType::Native(NativeType::Text)),
                ("b".into(), ColumnType::Native(NativeType::Int)),
                ("c".into(), ColumnType::Native(NativeType::TinyInt)), // Wrong column type
            ],
        }),
    };

    let err = udt
        .serialize(&typ_wrong_type, CellWriter::new(&mut data))
        .unwrap_err();
    let err = err.0.downcast_ref::<BuiltinSerializationError>().unwrap();
    assert_matches!(
        err.kind,
        BuiltinSerializationErrorKind::UdtError(
            UdtSerializationErrorKind::FieldSerializationFailed { .. }
        )
    );
}

#[derive(SerializeValue)]
#[scylla(crate = crate)]
struct TestUdtWithGenerics<'a, T: SerializeValue> {
    a: &'a str,
    b: T,
}

#[test]
fn test_udt_serialization_with_generics() {
    // A minimal smoke test just to test that it works.
    fn check_with_type<T: SerializeValue>(typ: ColumnType, t: T, cql_t: CqlValue) {
        let typ = ColumnType::UserDefinedType {
            frozen: false,
            definition: Arc::new(UserDefinedType {
                name: "typ".into(),
                keyspace: "ks".into(),
                field_types: vec![
                    ("a".into(), ColumnType::Native(NativeType::Text)),
                    ("b".into(), typ),
                ],
            }),
        };
        let reference = do_serialize(
            CqlValue::UserDefinedType {
                keyspace: "ks".to_string(),
                name: "typ".to_string(),
                fields: vec![
                    (
                        "a".to_string(),
                        Some(CqlValue::Text(String::from("Ala ma kota"))),
                    ),
                    ("b".to_string(), Some(cql_t)),
                ],
            },
            &typ,
        );
        let udt = do_serialize(
            TestUdtWithGenerics {
                a: "Ala ma kota",
                b: t,
            },
            &typ,
        );
        assert_eq!(reference, udt);
    }

    check_with_type(
        ColumnType::Native(NativeType::Int),
        123_i32,
        CqlValue::Int(123_i32),
    );
    check_with_type(
        ColumnType::Native(NativeType::Double),
        123_f64,
        CqlValue::Double(123_f64),
    );
}

#[derive(SerializeValue, Debug, PartialEq, Eq, Default)]
#[scylla(crate = crate, flavor = "enforce_order")]
struct TestUdtWithEnforcedOrder {
    a: String,
    b: i32,
    c: Vec<i64>,
}

#[test]
fn test_udt_serialization_with_enforced_order_correct_order() {
    let typ = ColumnType::UserDefinedType {
        frozen: false,
        definition: Arc::new(UserDefinedType {
            name: "typ".into(),
            keyspace: "ks".into(),
            field_types: vec![
                ("a".into(), ColumnType::Native(NativeType::Text)),
                ("b".into(), ColumnType::Native(NativeType::Int)),
                (
                    "c".into(),
                    ColumnType::Collection {
                        frozen: false,
                        typ: CollectionType::List(Box::new(ColumnType::Native(NativeType::BigInt))),
                    },
                ),
            ],
        }),
    };

    let reference = do_serialize(
        CqlValue::UserDefinedType {
            keyspace: "ks".to_string(),
            name: "typ".to_string(),
            fields: vec![
                (
                    "a".to_string(),
                    Some(CqlValue::Text(String::from("Ala ma kota"))),
                ),
                ("b".to_string(), Some(CqlValue::Int(42))),
                (
                    "c".to_string(),
                    Some(CqlValue::List(vec![
                        CqlValue::BigInt(1),
                        CqlValue::BigInt(2),
                        CqlValue::BigInt(3),
                    ])),
                ),
            ],
        },
        &typ,
    );
    let udt = do_serialize(
        TestUdtWithEnforcedOrder {
            a: "Ala ma kota".to_owned(),
            b: 42,
            c: vec![1, 2, 3],
        },
        &typ,
    );

    assert_eq!(reference, udt);
}

#[test]
fn test_udt_serialization_with_enforced_order_additional_field() {
    let udt = TestUdtWithEnforcedOrder::default();

    let typ_normal = ColumnType::UserDefinedType {
        frozen: false,
        definition: Arc::new(UserDefinedType {
            name: "typ".into(),
            keyspace: "ks".into(),
            field_types: vec![
                ("a".into(), ColumnType::Native(NativeType::Text)),
                ("b".into(), ColumnType::Native(NativeType::Int)),
                (
                    "c".into(),
                    ColumnType::Collection {
                        frozen: false,
                        typ: CollectionType::List(Box::new(ColumnType::Native(NativeType::BigInt))),
                    },
                ),
            ],
        }),
    };

    let typ_unexpected_field = ColumnType::UserDefinedType {
        frozen: false,
        definition: Arc::new(UserDefinedType {
            name: "typ".into(),
            keyspace: "ks".into(),
            field_types: vec![
                ("a".into(), ColumnType::Native(NativeType::Text)),
                ("b".into(), ColumnType::Native(NativeType::Int)),
                (
                    "c".into(),
                    ColumnType::Collection {
                        frozen: false,
                        typ: CollectionType::List(Box::new(ColumnType::Native(NativeType::BigInt))),
                    },
                ),
                // Unexpected field
                ("d".into(), ColumnType::Native(NativeType::Counter)),
            ],
        }),
    };

    let result_normal = do_serialize(&udt, &typ_normal);
    let result_additional_field = do_serialize(&udt, &typ_unexpected_field);

    assert_eq!(result_normal, result_additional_field);
}

#[test]
fn test_udt_serialization_with_enforced_order_failing_type_check() {
    let typ_not_udt = ColumnType::Native(NativeType::Ascii);
    let udt = TestUdtWithEnforcedOrder::default();

    let mut data = Vec::new();

    let err = <_ as SerializeValue>::serialize(&udt, &typ_not_udt, CellWriter::new(&mut data))
        .unwrap_err();
    let err = err.0.downcast_ref::<BuiltinTypeCheckError>().unwrap();
    assert_matches!(
        err.kind,
        BuiltinTypeCheckErrorKind::UdtError(UdtTypeCheckErrorKind::NotUdt)
    );

    let typ = ColumnType::UserDefinedType {
        frozen: false,
        definition: Arc::new(UserDefinedType {
            name: "typ".into(),
            keyspace: "ks".into(),
            field_types: vec![
                // Two first columns are swapped
                ("b".into(), ColumnType::Native(NativeType::Int)),
                ("a".into(), ColumnType::Native(NativeType::Text)),
                (
                    "c".into(),
                    ColumnType::Collection {
                        frozen: false,
                        typ: CollectionType::List(Box::new(ColumnType::Native(NativeType::BigInt))),
                    },
                ),
            ],
        }),
    };

    let err = <_ as SerializeValue>::serialize(&udt, &typ, CellWriter::new(&mut data)).unwrap_err();
    let err = err.0.downcast_ref::<BuiltinTypeCheckError>().unwrap();
    assert_matches!(
        err.kind,
        BuiltinTypeCheckErrorKind::UdtError(UdtTypeCheckErrorKind::FieldNameMismatch { .. })
    );

    let typ_without_c = ColumnType::UserDefinedType {
        frozen: false,
        definition: Arc::new(UserDefinedType {
            name: "typ".into(),
            keyspace: "ks".into(),
            field_types: vec![
                ("a".into(), ColumnType::Native(NativeType::Text)),
                ("b".into(), ColumnType::Native(NativeType::Int)),
                // Last field is missing
            ],
        }),
    };

    let err = <_ as SerializeValue>::serialize(&udt, &typ_without_c, CellWriter::new(&mut data))
        .unwrap_err();
    let err = err.0.downcast_ref::<BuiltinTypeCheckError>().unwrap();
    assert_matches!(
        err.kind,
        BuiltinTypeCheckErrorKind::UdtError(UdtTypeCheckErrorKind::ValueMissingForUdtField { .. })
    );

    let typ_unexpected_field = ColumnType::UserDefinedType {
        frozen: false,
        definition: Arc::new(UserDefinedType {
            name: "typ".into(),
            keyspace: "ks".into(),
            field_types: vec![
                ("a".into(), ColumnType::Native(NativeType::Text)),
                ("b".into(), ColumnType::Native(NativeType::Int)),
                ("c".into(), ColumnType::Native(NativeType::TinyInt)), // Wrong column type
            ],
        }),
    };

    let err =
        <_ as SerializeValue>::serialize(&udt, &typ_unexpected_field, CellWriter::new(&mut data))
            .unwrap_err();
    let err = err.0.downcast_ref::<BuiltinSerializationError>().unwrap();
    assert_matches!(
        err.kind,
        BuiltinSerializationErrorKind::UdtError(
            UdtSerializationErrorKind::FieldSerializationFailed { .. }
        )
    );
}

#[derive(SerializeValue, Debug)]
#[scylla(crate = crate)]
struct TestUdtWithFieldRename {
    a: String,
    #[scylla(rename = "x")]
    b: i32,
}

#[derive(SerializeValue, Debug)]
#[scylla(crate = crate, flavor = "enforce_order")]
struct TestUdtWithFieldRenameAndEnforceOrder {
    a: String,
    #[scylla(rename = "x")]
    b: i32,
}

#[test]
fn test_udt_serialization_with_field_rename() {
    let typ = ColumnType::UserDefinedType {
        frozen: false,
        definition: Arc::new(UserDefinedType {
            name: "typ".into(),
            keyspace: "ks".into(),
            field_types: vec![
                ("x".into(), ColumnType::Native(NativeType::Int)),
                ("a".into(), ColumnType::Native(NativeType::Text)),
            ],
        }),
    };

    let mut reference = Vec::new();
    // Total length of the struct is 23
    reference.extend_from_slice(&23i32.to_be_bytes());
    // Field 'x'
    reference.extend_from_slice(&4i32.to_be_bytes());
    reference.extend_from_slice(&42i32.to_be_bytes());
    // Field 'a'
    reference.extend_from_slice(&("Ala ma kota".len() as i32).to_be_bytes());
    reference.extend_from_slice("Ala ma kota".as_bytes());

    let udt = do_serialize(
        TestUdtWithFieldRename {
            a: "Ala ma kota".to_owned(),
            b: 42,
        },
        &typ,
    );

    assert_eq!(reference, udt);
}

#[test]
fn test_udt_serialization_with_field_rename_and_enforce_order() {
    let typ = ColumnType::UserDefinedType {
        frozen: false,
        definition: Arc::new(UserDefinedType {
            name: "typ".into(),
            keyspace: "ks".into(),
            field_types: vec![
                ("a".into(), ColumnType::Native(NativeType::Text)),
                ("x".into(), ColumnType::Native(NativeType::Int)),
            ],
        }),
    };

    let mut reference = Vec::new();
    // Total length of the struct is 23
    reference.extend_from_slice(&23i32.to_be_bytes());
    // Field 'a'
    reference.extend_from_slice(&("Ala ma kota".len() as i32).to_be_bytes());
    reference.extend_from_slice("Ala ma kota".as_bytes());
    // Field 'x'
    reference.extend_from_slice(&4i32.to_be_bytes());
    reference.extend_from_slice(&42i32.to_be_bytes());

    let udt = do_serialize(
        TestUdtWithFieldRenameAndEnforceOrder {
            a: "Ala ma kota".to_owned(),
            b: 42,
        },
        &typ,
    );

    assert_eq!(reference, udt);
}

#[expect(unused)]
#[derive(SerializeValue, Debug)]
#[scylla(crate = crate, flavor = "enforce_order", skip_name_checks)]
struct TestUdtWithSkippedNameChecks {
    a: String,
    b: i32,
}

#[test]
fn test_udt_serialization_with_skipped_name_checks() {
    let typ = ColumnType::UserDefinedType {
        frozen: false,
        definition: Arc::new(UserDefinedType {
            name: "typ".into(),
            keyspace: "ks".into(),
            field_types: vec![
                ("a".into(), ColumnType::Native(NativeType::Text)),
                ("x".into(), ColumnType::Native(NativeType::Int)),
            ],
        }),
    };

    let mut reference = Vec::new();
    // Total length of the struct is 23
    reference.extend_from_slice(&23i32.to_be_bytes());
    // Field 'a'
    reference.extend_from_slice(&("Ala ma kota".len() as i32).to_be_bytes());
    reference.extend_from_slice("Ala ma kota".as_bytes());
    // Field 'x'
    reference.extend_from_slice(&4i32.to_be_bytes());
    reference.extend_from_slice(&42i32.to_be_bytes());

    let udt = do_serialize(
        TestUdtWithFieldRenameAndEnforceOrder {
            a: "Ala ma kota".to_owned(),
            b: 42,
        },
        &typ,
    );

    assert_eq!(reference, udt);
}

#[derive(SerializeValue, Debug, PartialEq, Eq, Default)]
#[scylla(crate = crate, forbid_excess_udt_fields)]
struct TestStrictUdtWithFieldSorting {
    a: String,
    b: i32,
    c: Vec<i64>,
}

#[test]
fn test_strict_udt_with_field_sorting_rejects_additional_field() {
    let udt = TestStrictUdtWithFieldSorting::default();
    let mut data = Vec::new();

    let typ_unexpected_field = ColumnType::UserDefinedType {
        frozen: false,
        definition: Arc::new(UserDefinedType {
            name: "typ".into(),
            keyspace: "ks".into(),
            field_types: vec![
                ("a".into(), ColumnType::Native(NativeType::Text)),
                ("b".into(), ColumnType::Native(NativeType::Int)),
                (
                    "c".into(),
                    ColumnType::Collection {
                        frozen: false,
                        typ: CollectionType::List(Box::new(ColumnType::Native(NativeType::BigInt))),
                    },
                ),
                // Unexpected field
                ("d".into(), ColumnType::Native(NativeType::Counter)),
            ],
        }),
    };

    let err = udt
        .serialize(&typ_unexpected_field, CellWriter::new(&mut data))
        .unwrap_err();
    let err = err.0.downcast_ref::<BuiltinTypeCheckError>().unwrap();
    assert_matches!(
        err.kind,
        BuiltinTypeCheckErrorKind::UdtError(UdtTypeCheckErrorKind::NoSuchFieldInUdt { .. })
    );

    let typ_unexpected_field_middle = ColumnType::UserDefinedType {
        frozen: false,
        definition: Arc::new(UserDefinedType {
            name: "typ".into(),
            keyspace: "ks".into(),
            field_types: vec![
                ("a".into(), ColumnType::Native(NativeType::Text)),
                ("b".into(), ColumnType::Native(NativeType::Int)),
                // Unexpected field
                ("b_c".into(), ColumnType::Native(NativeType::Counter)),
                (
                    "c".into(),
                    ColumnType::Collection {
                        frozen: false,
                        typ: CollectionType::List(Box::new(ColumnType::Native(NativeType::BigInt))),
                    },
                ),
            ],
        }),
    };

    let err = udt
        .serialize(&typ_unexpected_field_middle, CellWriter::new(&mut data))
        .unwrap_err();
    let err = err.0.downcast_ref::<BuiltinTypeCheckError>().unwrap();
    assert_matches!(
        err.kind,
        BuiltinTypeCheckErrorKind::UdtError(UdtTypeCheckErrorKind::NoSuchFieldInUdt { .. })
    );
}

#[derive(SerializeValue, Debug, PartialEq, Eq, Default)]
#[scylla(crate = crate, flavor = "enforce_order", forbid_excess_udt_fields)]
struct TestStrictUdtWithEnforcedOrder {
    a: String,
    b: i32,
    c: Vec<i64>,
}

#[test]
fn test_strict_udt_with_enforced_order_rejects_additional_field() {
    let udt = TestStrictUdtWithEnforcedOrder::default();
    let mut data = Vec::new();

    let typ_unexpected_field = ColumnType::UserDefinedType {
        frozen: false,
        definition: Arc::new(UserDefinedType {
            name: "typ".into(),
            keyspace: "ks".into(),
            field_types: vec![
                ("a".into(), ColumnType::Native(NativeType::Text)),
                ("b".into(), ColumnType::Native(NativeType::Int)),
                (
                    "c".into(),
                    ColumnType::Collection {
                        frozen: false,
                        typ: CollectionType::List(Box::new(ColumnType::Native(NativeType::BigInt))),
                    },
                ),
                // Unexpected field
                ("d".into(), ColumnType::Native(NativeType::Counter)),
            ],
        }),
    };

    let err =
        <_ as SerializeValue>::serialize(&udt, &typ_unexpected_field, CellWriter::new(&mut data))
            .unwrap_err();
    let err = err.0.downcast_ref::<BuiltinTypeCheckError>().unwrap();
    assert_matches!(
        err.kind,
        BuiltinTypeCheckErrorKind::UdtError(UdtTypeCheckErrorKind::NoSuchFieldInUdt { .. })
    );
}

#[derive(SerializeValue, Debug)]
#[scylla(crate = crate, flavor = "enforce_order", skip_name_checks)]
struct TestUdtWithSkippedFields {
    a: String,
    b: i32,
    #[scylla(skip)]
    #[expect(dead_code)]
    skipped: Vec<String>,
    c: Vec<i64>,
}

#[test]
fn test_row_serialization_with_skipped_field() {
    let typ = ColumnType::UserDefinedType {
        frozen: false,
        definition: Arc::new(UserDefinedType {
            name: "typ".into(),
            keyspace: "ks".into(),
            field_types: vec![
                ("a".into(), ColumnType::Native(NativeType::Text)),
                ("b".into(), ColumnType::Native(NativeType::Int)),
                (
                    "c".into(),
                    ColumnType::Collection {
                        frozen: false,
                        typ: CollectionType::List(Box::new(ColumnType::Native(NativeType::BigInt))),
                    },
                ),
            ],
        }),
    };

    let reference = do_serialize(
        TestUdtWithFieldSorting {
            a: "Ala ma kota".to_owned(),
            b: 42,
            c: vec![1, 2, 3],
        },
        &typ,
    );
    let row = do_serialize(
        TestUdtWithSkippedFields {
            a: "Ala ma kota".to_owned(),
            b: 42,
            skipped: vec!["abcd".to_owned(), "efgh".to_owned()],
            c: vec![1, 2, 3],
        },
        &typ,
    );

    assert_eq!(reference, row);
}

#[test]
fn test_udt_with_non_rust_ident() {
    #[derive(SerializeValue, Debug)]
    #[scylla(crate = crate)]
    struct UdtWithNonRustIdent {
        #[scylla(rename = "a$a")]
        a: i32,
    }

    let typ = ColumnType::UserDefinedType {
        frozen: false,
        definition: Arc::new(UserDefinedType {
            name: "typ".into(),
            keyspace: "ks".into(),
            field_types: vec![("a$a".into(), ColumnType::Native(NativeType::Int))],
        }),
    };
    let value = UdtWithNonRustIdent { a: 42 };

    let mut reference = Vec::new();
    // Total length of the struct
    reference.extend_from_slice(&8i32.to_be_bytes());
    // Field 'a'
    reference.extend_from_slice(&(std::mem::size_of_val(&value.a) as i32).to_be_bytes());
    reference.extend_from_slice(&value.a.to_be_bytes());

    let udt = do_serialize(value, &typ);

    assert_eq!(reference, udt);
}

// Tests migrated from old frame/value_tests.rs file

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
            assert_eq!(
                do_serialize(x, &ColumnType::Native(NativeType::Decimal)),
                repr
            );
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
    let err = do_serialize_err(leap_second, &ColumnType::Native(NativeType::Time));
    assert_matches!(
        get_ser_err(&err).kind,
        BuiltinSerializationErrorKind::ValueOverflow
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
        let bytes: Vec<u8> =
            do_serialize(test_timestamp, &ColumnType::Native(NativeType::Timestamp));

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
        let bytes: Vec<u8> =
            do_serialize(test_datetime, &ColumnType::Native(NativeType::Timestamp));

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

    let unset_option_i32: Option<i32> = None;
    assert_eq!(
        do_serialize(
            MaybeUnset::from_option(unset_option_i32),
            &ColumnType::Native(NativeType::Int)
        ),
        &(-2_i32).to_be_bytes()[..]
    );

    let set_option_i32: Option<i32> = Some(44);
    assert_eq!(
        do_serialize(
            MaybeUnset::from_option(set_option_i32),
            &ColumnType::Native(NativeType::Int)
        ),
        vec![0, 0, 0, 4, 0, 0, 0, 44]
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
