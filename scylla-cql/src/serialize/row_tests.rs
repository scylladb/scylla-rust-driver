use std::borrow::Cow;
use std::collections::BTreeMap;

use crate::frame::response::result::{
    CollectionType, ColumnSpec, ColumnType, NativeType, TableSpec,
};
use crate::frame::types::RawValue;
use crate::serialize::row::{
    BuiltinSerializationError, BuiltinSerializationErrorKind, BuiltinTypeCheckError,
    BuiltinTypeCheckErrorKind, RowSerializationContext, SerializeRow, SerializeValue,
    SerializedValues,
};
use crate::serialize::value::tests::get_ser_err as get_value_ser_err;
use crate::serialize::value::{
    mk_ser_err, BuiltinSerializationErrorKind as BuiltinValueSerializationErrorKind,
};
use crate::serialize::writers::WrittenCellProof;
use crate::serialize::{CellWriter, RowWriter, SerializationError};
use crate::value::MaybeUnset;
use crate::SerializeRow;

use assert_matches::assert_matches;

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

fn col<'a>(name: impl Into<Cow<'a, str>>, typ: ColumnType<'a>) -> ColumnSpec<'a> {
    ColumnSpec {
        name: name.into(),
        typ,
        table_spec: TableSpec::borrowed("ks", "tbl"),
    }
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
    assert_matches!(err.kind, BuiltinTypeCheckErrorKind::NoColumnWithName { .. });

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
    assert_matches!(
        err.kind,
        BuiltinTypeCheckErrorKind::ValueMissingForColumn { .. }
    );

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
    assert_matches!(err.kind, BuiltinTypeCheckErrorKind::NoColumnWithName { .. });

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
    assert_matches!(
        err.kind,
        BuiltinTypeCheckErrorKind::ValueMissingForColumn { .. }
    );

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

// Tests migrated from old frame/value_tests.rs file

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
                BuiltinValueSerializationErrorKind::SizeOverflow,
            ))
        }
    }

    let err = values
        .add_value(&TooBigValue, &ColumnType::Native(NativeType::Ascii))
        .unwrap_err();

    assert_matches!(
        get_value_ser_err(&err).kind,
        BuiltinValueSerializationErrorKind::SizeOverflow
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
        col("ala", ColumnType::Native(NativeType::Int)),
        col("ma", ColumnType::Native(NativeType::Int)),
        col("kota", ColumnType::Native(NativeType::Int)),
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
        col("ala", ColumnType::Native(NativeType::Int)),
        col("ma", ColumnType::Native(NativeType::Int)),
        col("kota", ColumnType::Native(NativeType::Int)),
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
            .map(|(i, _)| col(format!("col_{i}"), ColumnType::Native(NativeType::TinyInt)))
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
        col("ala", ColumnType::Native(NativeType::Int)),
        col("ma", ColumnType::Native(NativeType::Int)),
        col("kota", ColumnType::Native(NativeType::Int)),
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
        col("col_1", ColumnType::Native(NativeType::Int)),
        col("col_2", ColumnType::Native(NativeType::Int)),
        col("col_3", ColumnType::Native(NativeType::Int)),
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
fn test_row_serialization_nested_structs() {
    #[derive(SerializeRow, Debug)]
    #[scylla(crate = crate)]
    struct InnerColumnsOne {
        x: i32,
        y: f64,
    }

    #[derive(SerializeRow, Debug)]
    #[scylla(crate = crate)]
    struct InnerColumnsTwo {
        z: bool,
    }

    #[derive(SerializeRow, Debug)]
    #[scylla(crate = crate)]
    struct OuterColumns {
        #[scylla(flatten)]
        inner_one: InnerColumnsOne,
        a: String,
        #[scylla(flatten)]
        inner_two: InnerColumnsTwo,
    }

    let spec = [
        col("a", ColumnType::Native(NativeType::Text)),
        col("x", ColumnType::Native(NativeType::Int)),
        col("z", ColumnType::Native(NativeType::Boolean)),
        col("y", ColumnType::Native(NativeType::Double)),
    ];

    let value = OuterColumns {
        inner_one: InnerColumnsOne { x: 5, y: 1.0 },
        a: "something".to_owned(),
        inner_two: InnerColumnsTwo { z: true },
    };

    let reference = do_serialize(
        (
            &value.a,
            &value.inner_one.x,
            &value.inner_two.z,
            &value.inner_one.y,
        ),
        &spec,
    );

    let row = do_serialize(value, &spec);

    assert_eq!(reference, row);
}

#[test]
fn test_flatten_row_serialization_with_enforced_order_and_skip_namecheck() {
    #[derive(SerializeRow, Debug)]
    #[scylla(crate = crate, flavor = "enforce_order")]
    struct OuterColumns {
        a: String,
        #[scylla(flatten)]
        inner_one: InnerColumnsOne,
        d: i32,
        #[scylla(flatten)]
        inner_two: InnerColumnsTwo,
    }

    #[derive(SerializeRow, Debug)]
    #[scylla(crate = crate, flavor = "enforce_order", skip_name_checks)]
    struct InnerColumnsOne {
        potato: bool,
        carrot: f32,
    }

    #[derive(SerializeRow, Debug)]
    #[scylla(crate = crate, flavor = "enforce_order")]
    struct InnerColumnsTwo {
        e: String,
    }

    let value = OuterColumns {
        a: "A".to_owned(),
        inner_one: InnerColumnsOne {
            potato: false,
            carrot: 2.3,
        },
        d: 32,
        inner_two: InnerColumnsTwo { e: "E".to_owned() },
    };

    let spec = [
        col("a", ColumnType::Native(NativeType::Text)),
        col("b", ColumnType::Native(NativeType::Boolean)),
        col("c", ColumnType::Native(NativeType::Float)),
        col("d", ColumnType::Native(NativeType::Int)),
        col("e", ColumnType::Native(NativeType::Text)),
    ];

    let reference = do_serialize(
        (
            &value.a,
            &value.inner_one.potato,
            &value.inner_one.carrot,
            &value.d,
            &value.inner_two.e,
        ),
        &spec,
    );
    let row = do_serialize(value, &spec);

    assert_eq!(reference, row);
}
