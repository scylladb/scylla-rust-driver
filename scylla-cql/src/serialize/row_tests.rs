use std::collections::BTreeMap;

use crate::frame::response::result::{
    CollectionType, ColumnSpec, ColumnType, NativeType, TableSpec,
};
use crate::frame::types::RawValue;
use crate::frame::value::MaybeUnset;
use crate::serialize::{RowWriter, SerializationError};

use super::{
    BuiltinSerializationError, BuiltinSerializationErrorKind, BuiltinTypeCheckError,
    BuiltinTypeCheckErrorKind, RowSerializationContext, SerializeRow, SerializeValue,
};

use super::SerializedValues;
use assert_matches::assert_matches;
use scylla_macros::SerializeRow;

#[test]
fn test_dyn_serialize_row() {
    let row = (
        1i32,
        "Ala ma kota",
        None::<i64>,
        MaybeUnset::Unset::<String>,
    );
    let ctx = RowSerializationContext {
        columns: &[
            col("a", ColumnType::Native(NativeType::Int)),
            col("b", ColumnType::Native(NativeType::Text)),
            col("c", ColumnType::Native(NativeType::BigInt)),
            col("d", ColumnType::Native(NativeType::Ascii)),
        ],
    };

    let mut typed_data = Vec::new();
    let mut typed_data_writer = RowWriter::new(&mut typed_data);
    <_ as SerializeRow>::serialize(&row, &ctx, &mut typed_data_writer).unwrap();

    let row = &row as &dyn SerializeRow;
    let mut erased_data = Vec::new();
    let mut erased_data_writer = RowWriter::new(&mut erased_data);
    <_ as SerializeRow>::serialize(&row, &ctx, &mut erased_data_writer).unwrap();

    assert_eq!(
        typed_data_writer.value_count(),
        erased_data_writer.value_count(),
    );
    assert_eq!(typed_data, erased_data);
}

pub(crate) fn do_serialize<T: SerializeRow>(t: T, columns: &[ColumnSpec]) -> Vec<u8> {
    let ctx = RowSerializationContext { columns };
    let mut ret = Vec::new();
    let mut builder = RowWriter::new(&mut ret);
    t.serialize(&ctx, &mut builder).unwrap();
    ret
}

fn do_serialize_err<T: SerializeRow>(t: T, columns: &[ColumnSpec]) -> SerializationError {
    let ctx = RowSerializationContext { columns };
    let mut ret = Vec::new();
    let mut builder = RowWriter::new(&mut ret);
    t.serialize(&ctx, &mut builder).unwrap_err()
}

fn col<'a>(name: &'a str, typ: ColumnType<'a>) -> ColumnSpec<'a> {
    ColumnSpec::borrowed(name, typ, TableSpec::borrowed("ks", "tbl"))
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
fn test_tuple_errors() {
    // Unit
    #[allow(clippy::let_unit_value)] // The let binding below is intentional
    let v = ();
    let spec = [col("a", ColumnType::Native(NativeType::Text))];
    let err = do_serialize_err(v, &spec);
    let err = get_typeck_err(&err);
    assert_eq!(err.rust_name, std::any::type_name::<()>());
    assert_matches!(
        err.kind,
        BuiltinTypeCheckErrorKind::WrongColumnCount {
            rust_cols: 0,
            cql_cols: 1,
        }
    );

    // Non-unit tuple
    // Count mismatch
    let v = ("Ala ma kota",);
    let spec = [
        col("a", ColumnType::Native(NativeType::Text)),
        col("b", ColumnType::Native(NativeType::Text)),
    ];
    let err = do_serialize_err(v, &spec);
    let err = get_typeck_err(&err);
    assert_eq!(err.rust_name, std::any::type_name::<(&str,)>());
    assert_matches!(
        err.kind,
        BuiltinTypeCheckErrorKind::WrongColumnCount {
            rust_cols: 1,
            cql_cols: 2,
        }
    );

    // Serialization of one of the element fails
    let v = ("Ala ma kota", 123_i32);
    let spec = [
        col("a", ColumnType::Native(NativeType::Text)),
        col("b", ColumnType::Native(NativeType::Text)),
    ];
    let err = do_serialize_err(v, &spec);
    let err = get_ser_err(&err);
    assert_eq!(err.rust_name, std::any::type_name::<(&str, i32)>());
    let BuiltinSerializationErrorKind::ColumnSerializationFailed { name, err: _ } = &err.kind
    else {
        panic!("Expected BuiltinSerializationErrorKind::ColumnSerializationFailed")
    };
    assert_eq!(name, "b");
}

#[test]
fn test_slice_errors() {
    // Non-unit tuple
    // Count mismatch
    let v = vec!["Ala ma kota"];
    let spec = [
        col("a", ColumnType::Native(NativeType::Text)),
        col("b", ColumnType::Native(NativeType::Text)),
    ];
    let err = do_serialize_err(v, &spec);
    let err = get_typeck_err(&err);
    assert_eq!(err.rust_name, std::any::type_name::<Vec<&str>>());
    assert_matches!(
        err.kind,
        BuiltinTypeCheckErrorKind::WrongColumnCount {
            rust_cols: 1,
            cql_cols: 2,
        }
    );

    // Serialization of one of the element fails
    let v = vec!["Ala ma kota", "Kot ma pchły"];
    let spec = [
        col("a", ColumnType::Native(NativeType::Text)),
        col("b", ColumnType::Native(NativeType::Int)),
    ];
    let err = do_serialize_err(v, &spec);
    let err = get_ser_err(&err);
    assert_eq!(err.rust_name, std::any::type_name::<Vec<&str>>());
    let BuiltinSerializationErrorKind::ColumnSerializationFailed { name, err: _ } = &err.kind
    else {
        panic!("Expected BuiltinSerializationErrorKind::ColumnSerializationFailed")
    };
    assert_eq!(name, "b");
}

#[test]
fn test_map_errors() {
    // Missing value for a bind marker
    let v: BTreeMap<_, _> = vec![("a", 123_i32)].into_iter().collect();
    let spec = [
        col("a", ColumnType::Native(NativeType::Int)),
        col("b", ColumnType::Native(NativeType::Text)),
    ];
    let err = do_serialize_err(v, &spec);
    let err = get_typeck_err(&err);
    assert_eq!(err.rust_name, std::any::type_name::<BTreeMap<&str, i32>>());
    let BuiltinTypeCheckErrorKind::ValueMissingForColumn { name } = &err.kind else {
        panic!("unexpected error kind: {}", err.kind)
    };
    assert_eq!(name, "b");

    // Additional value, not present in the query
    let v: BTreeMap<_, _> = vec![("a", 123_i32), ("b", 456_i32)].into_iter().collect();
    let spec = [col("a", ColumnType::Native(NativeType::Int))];
    let err = do_serialize_err(v, &spec);
    let err = get_typeck_err(&err);
    assert_eq!(err.rust_name, std::any::type_name::<BTreeMap<&str, i32>>());
    let BuiltinTypeCheckErrorKind::NoColumnWithName { name } = &err.kind else {
        panic!("unexpected error kind: {}", err.kind)
    };
    assert_eq!(name, "b");

    // Serialization of one of the element fails
    let v: BTreeMap<_, _> = vec![("a", 123_i32), ("b", 456_i32)].into_iter().collect();
    let spec = [
        col("a", ColumnType::Native(NativeType::Int)),
        col("b", ColumnType::Native(NativeType::Text)),
    ];
    let err = do_serialize_err(v, &spec);
    let err = get_ser_err(&err);
    assert_eq!(err.rust_name, std::any::type_name::<BTreeMap<&str, i32>>());
    let BuiltinSerializationErrorKind::ColumnSerializationFailed { name, err: _ } = &err.kind
    else {
        panic!("Expected BuiltinSerializationErrorKind::ColumnSerializationFailed")
    };
    assert_eq!(name, "b");
}

// Do not remove. It's not used in tests but we keep it here to check that
// we properly ignore warnings about unused variables, unnecessary `mut`s
// etc. that usually pop up when generating code for empty structs.
#[allow(unused)]
#[derive(SerializeRow)]
#[scylla(crate = crate)]
struct TestRowWithNoColumns {}

#[derive(SerializeRow, Debug, PartialEq, Eq, Default)]
#[scylla(crate = crate)]
struct TestRowWithColumnSorting {
    a: String,
    b: i32,
    c: Vec<i64>,
}

#[test]
fn test_row_serialization_with_column_sorting_correct_order() {
    let spec = [
        col("a", ColumnType::Native(NativeType::Text)),
        col("b", ColumnType::Native(NativeType::Int)),
        col(
            "c",
            ColumnType::Collection {
                frozen: false,
                typ: CollectionType::List(Box::new(ColumnType::Native(NativeType::BigInt))),
            },
        ),
    ];

    let reference = do_serialize(("Ala ma kota", 42i32, vec![1i64, 2i64, 3i64]), &spec);
    let row = do_serialize(
        TestRowWithColumnSorting {
            a: "Ala ma kota".to_owned(),
            b: 42,
            c: vec![1, 2, 3],
        },
        &spec,
    );

    assert_eq!(reference, row);
}

#[test]
fn test_row_serialization_with_column_sorting_incorrect_order() {
    // The order of two last columns is swapped
    let spec = [
        col("a", ColumnType::Native(NativeType::Text)),
        col(
            "c",
            ColumnType::Collection {
                frozen: false,
                typ: CollectionType::List(Box::new(ColumnType::Native(NativeType::BigInt))),
            },
        ),
        col("b", ColumnType::Native(NativeType::Int)),
    ];

    let reference = do_serialize(("Ala ma kota", vec![1i64, 2i64, 3i64], 42i32), &spec);
    let row = do_serialize(
        TestRowWithColumnSorting {
            a: "Ala ma kota".to_owned(),
            b: 42,
            c: vec![1, 2, 3],
        },
        &spec,
    );

    assert_eq!(reference, row);
}

#[test]
fn test_row_serialization_failing_type_check() {
    let row = TestRowWithColumnSorting::default();
    let mut data = Vec::new();
    let mut row_writer = RowWriter::new(&mut data);

    let spec_without_c = [
        col("a", ColumnType::Native(NativeType::Text)),
        col("b", ColumnType::Native(NativeType::Int)),
        // Missing column c
    ];

    let ctx = RowSerializationContext {
        columns: &spec_without_c,
    };
    let err = <_ as SerializeRow>::serialize(&row, &ctx, &mut row_writer).unwrap_err();
    let err = err.0.downcast_ref::<BuiltinTypeCheckError>().unwrap();
    assert_matches!(
        err.kind,
        BuiltinTypeCheckErrorKind::ValueMissingForColumn { .. }
    );

    let spec_duplicate_column = [
        col("a", ColumnType::Native(NativeType::Text)),
        col("b", ColumnType::Native(NativeType::Int)),
        col(
            "c",
            ColumnType::Collection {
                frozen: false,
                typ: CollectionType::List(Box::new(ColumnType::Native(NativeType::BigInt))),
            },
        ),
        // Unexpected last column
        col("d", ColumnType::Native(NativeType::Counter)),
    ];

    let ctx = RowSerializationContext {
        columns: &spec_duplicate_column,
    };
    let err = <_ as SerializeRow>::serialize(&row, &ctx, &mut row_writer).unwrap_err();
    let err = err.0.downcast_ref::<BuiltinTypeCheckError>().unwrap();
    assert_matches!(err.kind, BuiltinTypeCheckErrorKind::NoColumnWithName { .. });

    let spec_wrong_type = [
        col("a", ColumnType::Native(NativeType::Text)),
        col("b", ColumnType::Native(NativeType::Int)),
        col("c", ColumnType::Native(NativeType::TinyInt)), // Wrong type
    ];

    let ctx = RowSerializationContext {
        columns: &spec_wrong_type,
    };
    let err = <_ as SerializeRow>::serialize(&row, &ctx, &mut row_writer).unwrap_err();
    let err = err.0.downcast_ref::<BuiltinSerializationError>().unwrap();
    assert_matches!(
        err.kind,
        BuiltinSerializationErrorKind::ColumnSerializationFailed { .. }
    );
}

#[derive(SerializeRow)]
#[scylla(crate = crate)]
struct TestRowWithGenerics<'a, T: SerializeValue> {
    a: &'a str,
    b: T,
}

#[test]
fn test_row_serialization_with_generics() {
    // A minimal smoke test just to test that it works.
    fn check_with_type<T: SerializeValue + Copy>(typ: ColumnType<'static>, t: T) {
        let spec = [
            col("a", ColumnType::Native(NativeType::Text)),
            col("b", typ),
        ];
        let reference = do_serialize(("Ala ma kota", t), &spec);
        let row = do_serialize(
            TestRowWithGenerics {
                a: "Ala ma kota",
                b: t,
            },
            &spec,
        );
        assert_eq!(reference, row);
    }

    check_with_type(ColumnType::Native(NativeType::Int), 123_i32);
    check_with_type(ColumnType::Native(NativeType::Double), 123_f64);
}

#[derive(SerializeRow, Debug, PartialEq, Eq, Default)]
#[scylla(crate = crate, flavor = "enforce_order")]
struct TestRowWithEnforcedOrder {
    a: String,
    b: i32,
    c: Vec<i64>,
}

#[test]
fn test_row_serialization_with_enforced_order_correct_order() {
    let spec = [
        col("a", ColumnType::Native(NativeType::Text)),
        col("b", ColumnType::Native(NativeType::Int)),
        col(
            "c",
            ColumnType::Collection {
                frozen: false,
                typ: CollectionType::List(Box::new(ColumnType::Native(NativeType::BigInt))),
            },
        ),
    ];

    let reference = do_serialize(("Ala ma kota", 42i32, vec![1i64, 2i64, 3i64]), &spec);
    let row = do_serialize(
        TestRowWithEnforcedOrder {
            a: "Ala ma kota".to_owned(),
            b: 42,
            c: vec![1, 2, 3],
        },
        &spec,
    );

    assert_eq!(reference, row);
}

#[test]
fn test_row_serialization_with_enforced_order_failing_type_check() {
    let row = TestRowWithEnforcedOrder::default();
    let mut data = Vec::new();
    let mut writer = RowWriter::new(&mut data);

    // The order of two last columns is swapped
    let spec = [
        col("a", ColumnType::Native(NativeType::Text)),
        col(
            "c",
            ColumnType::Collection {
                frozen: false,
                typ: CollectionType::List(Box::new(ColumnType::Native(NativeType::BigInt))),
            },
        ),
        col("b", ColumnType::Native(NativeType::Int)),
    ];
    let ctx = RowSerializationContext { columns: &spec };
    let err = <_ as SerializeRow>::serialize(&row, &ctx, &mut writer).unwrap_err();
    let err = err.0.downcast_ref::<BuiltinTypeCheckError>().unwrap();
    assert_matches!(
        err.kind,
        BuiltinTypeCheckErrorKind::ColumnNameMismatch { .. }
    );

    let spec_without_c = [
        col("a", ColumnType::Native(NativeType::Text)),
        col("b", ColumnType::Native(NativeType::Int)),
        // Missing column c
    ];

    let ctx = RowSerializationContext {
        columns: &spec_without_c,
    };
    let err = <_ as SerializeRow>::serialize(&row, &ctx, &mut writer).unwrap_err();
    let err = err.0.downcast_ref::<BuiltinTypeCheckError>().unwrap();
    assert_matches!(
        err.kind,
        BuiltinTypeCheckErrorKind::ValueMissingForColumn { .. }
    );

    let spec_duplicate_column = [
        col("a", ColumnType::Native(NativeType::Text)),
        col("b", ColumnType::Native(NativeType::Int)),
        col(
            "c",
            ColumnType::Collection {
                frozen: false,
                typ: CollectionType::List(Box::new(ColumnType::Native(NativeType::BigInt))),
            },
        ),
        // Unexpected last column
        col("d", ColumnType::Native(NativeType::Counter)),
    ];

    let ctx = RowSerializationContext {
        columns: &spec_duplicate_column,
    };
    let err = <_ as SerializeRow>::serialize(&row, &ctx, &mut writer).unwrap_err();
    let err = err.0.downcast_ref::<BuiltinTypeCheckError>().unwrap();
    assert_matches!(err.kind, BuiltinTypeCheckErrorKind::NoColumnWithName { .. });

    let spec_wrong_type = [
        col("a", ColumnType::Native(NativeType::Text)),
        col("b", ColumnType::Native(NativeType::Int)),
        col("c", ColumnType::Native(NativeType::TinyInt)), // Wrong type
    ];

    let ctx = RowSerializationContext {
        columns: &spec_wrong_type,
    };
    let err = <_ as SerializeRow>::serialize(&row, &ctx, &mut writer).unwrap_err();
    let err = err.0.downcast_ref::<BuiltinSerializationError>().unwrap();
    assert_matches!(
        err.kind,
        BuiltinSerializationErrorKind::ColumnSerializationFailed { .. }
    );
}

#[test]
fn test_empty_serialized_values() {
    let values = SerializedValues::new();
    assert!(values.is_empty());
    assert_eq!(values.element_count(), 0);
    assert_eq!(values.buffer_size(), 0);
    assert_eq!(values.iter().count(), 0);
}

#[test]
fn test_serialized_values_content() {
    let mut values = SerializedValues::new();
    values
        .add_value(&1234i32, &ColumnType::Native(NativeType::Int))
        .unwrap();
    values
        .add_value(&"abcdefg", &ColumnType::Native(NativeType::Ascii))
        .unwrap();
    let mut buf = Vec::new();
    values.write_to_request(&mut buf);
    assert_eq!(
        buf,
        [
            0, 2, // element count
            0, 0, 0, 4, // size of int
            0, 0, 4, 210, // content of int (1234)
            0, 0, 0, 7, // size of string
            97, 98, 99, 100, 101, 102, 103, // content of string ('abcdefg')
        ]
    )
}

#[test]
fn test_serialized_values_iter() {
    let mut values = SerializedValues::new();
    values
        .add_value(&1234i32, &ColumnType::Native(NativeType::Int))
        .unwrap();
    values
        .add_value(&"abcdefg", &ColumnType::Native(NativeType::Ascii))
        .unwrap();

    let mut iter = values.iter();
    assert_eq!(iter.next(), Some(RawValue::Value(&[0, 0, 4, 210])));
    assert_eq!(
        iter.next(),
        Some(RawValue::Value(&[97, 98, 99, 100, 101, 102, 103]))
    );
    assert_eq!(iter.next(), None);
}

#[test]
fn test_serialized_values_max_capacity() {
    let mut values = SerializedValues::new();
    for _ in 0..65535 {
        values
            .add_value(&123456789i64, &ColumnType::Native(NativeType::BigInt))
            .unwrap();
    }

    // Adding this value should fail, we reached max capacity
    values
        .add_value(&123456789i64, &ColumnType::Native(NativeType::BigInt))
        .unwrap_err();

    assert_eq!(values.iter().count(), 65535);
    assert!(values
        .iter()
        .all(|v| v == RawValue::Value(&[0, 0, 0, 0, 0x07, 0x5b, 0xcd, 0x15])))
}

#[derive(SerializeRow, Debug)]
#[scylla(crate = crate)]
struct TestRowWithColumnRename {
    a: String,
    #[scylla(rename = "x")]
    b: i32,
}

#[derive(SerializeRow, Debug)]
#[scylla(crate = crate, flavor = "enforce_order")]
struct TestRowWithColumnRenameAndEnforceOrder {
    a: String,
    #[scylla(rename = "x")]
    b: i32,
}

#[test]
fn test_row_serialization_with_column_rename() {
    let spec = [
        col("x", ColumnType::Native(NativeType::Int)),
        col("a", ColumnType::Native(NativeType::Text)),
    ];

    let reference = do_serialize((42i32, "Ala ma kota"), &spec);
    let row = do_serialize(
        TestRowWithColumnRename {
            a: "Ala ma kota".to_owned(),
            b: 42,
        },
        &spec,
    );

    assert_eq!(reference, row);
}

#[test]
fn test_row_serialization_with_column_rename_and_enforce_order() {
    let spec = [
        col("a", ColumnType::Native(NativeType::Text)),
        col("x", ColumnType::Native(NativeType::Int)),
    ];

    let reference = do_serialize(("Ala ma kota", 42i32), &spec);
    let row = do_serialize(
        TestRowWithColumnRenameAndEnforceOrder {
            a: "Ala ma kota".to_owned(),
            b: 42,
        },
        &spec,
    );

    assert_eq!(reference, row);
}

#[derive(SerializeRow, Debug)]
#[scylla(crate = crate, flavor = "enforce_order", skip_name_checks)]
struct TestRowWithSkippedNameChecks {
    a: String,
    b: i32,
}

#[test]
fn test_row_serialization_with_skipped_name_checks() {
    let spec = [
        col("a", ColumnType::Native(NativeType::Text)),
        col("x", ColumnType::Native(NativeType::Int)),
    ];

    let reference = do_serialize(("Ala ma kota", 42i32), &spec);
    let row = do_serialize(
        TestRowWithSkippedNameChecks {
            a: "Ala ma kota".to_owned(),
            b: 42,
        },
        &spec,
    );

    assert_eq!(reference, row);
}

#[test]
fn test_row_serialization_with_not_rust_idents() {
    #[derive(SerializeRow, Debug)]
    #[scylla(crate = crate)]
    struct RowWithTTL {
        #[scylla(rename = "[ttl]")]
        ttl: i32,
    }

    let spec = [col("[ttl]", ColumnType::Native(NativeType::Int))];

    let reference = do_serialize((42i32,), &spec);
    let row = do_serialize(RowWithTTL { ttl: 42 }, &spec);

    assert_eq!(reference, row);
}

#[derive(SerializeRow, Debug)]
#[scylla(crate = crate)]
struct TestRowWithSkippedFields {
    a: String,
    b: i32,
    #[scylla(skip)]
    #[allow(dead_code)]
    skipped: Vec<String>,
    c: Vec<i64>,
}

#[test]
fn test_row_serialization_with_skipped_field() {
    let spec = [
        col("a", ColumnType::Native(NativeType::Text)),
        col("b", ColumnType::Native(NativeType::Int)),
        col(
            "c",
            ColumnType::Collection {
                frozen: false,
                typ: CollectionType::List(Box::new(ColumnType::Native(NativeType::BigInt))),
            },
        ),
    ];

    let reference = do_serialize(
        TestRowWithColumnSorting {
            a: "Ala ma kota".to_owned(),
            b: 42,
            c: vec![1, 2, 3],
        },
        &spec,
    );
    let row = do_serialize(
        TestRowWithSkippedFields {
            a: "Ala ma kota".to_owned(),
            b: 42,
            skipped: vec!["abcd".to_owned(), "efgh".to_owned()],
            c: vec![1, 2, 3],
        },
        &spec,
    );

    assert_eq!(reference, row);
}

#[test]
fn test_row_serialization_with_boxed_tuple() {
    let spec = [
        col("a", ColumnType::Native(NativeType::Int)),
        col("b", ColumnType::Native(NativeType::Int)),
    ];

    let reference = do_serialize((42i32, 42i32), &spec);
    let row = do_serialize(Box::new((42i32, 42i32)), &spec);

    assert_eq!(reference, row);
}
