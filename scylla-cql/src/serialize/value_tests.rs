use crate::frame::response::result::{
    CollectionType, ColumnType, CqlValue, NativeType, UserDefinedType,
};
use crate::frame::value::{Counter, Unset};
use crate::serialize::value::{
    BuiltinSerializationError, BuiltinSerializationErrorKind, BuiltinTypeCheckError,
    BuiltinTypeCheckErrorKind, MapSerializationErrorKind, MapTypeCheckErrorKind,
    SetOrListSerializationErrorKind, SetOrListTypeCheckErrorKind, TupleSerializationErrorKind,
    TupleTypeCheckErrorKind,
};
use crate::serialize::{CellWriter, SerializationError};
use std::collections::BTreeMap;
use std::sync::Arc;

use assert_matches::assert_matches;
use scylla_macros::SerializeValue;

use super::{SerializeValue, UdtSerializationErrorKind, UdtTypeCheckErrorKind};

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

fn get_ser_err(err: &SerializationError) -> &BuiltinSerializationError {
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
#[allow(unused)]
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

#[allow(unused)]
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
    #[allow(dead_code)]
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
