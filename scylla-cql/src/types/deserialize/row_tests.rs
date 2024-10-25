use assert_matches::assert_matches;
use bytes::Bytes;
use scylla_macros::DeserializeRow;

use crate::frame::response::result::{ColumnSpec, ColumnType};
use crate::types::deserialize::row::BuiltinDeserializationErrorKind;
use crate::types::deserialize::{value, DeserializationError, FrameSlice};

use super::super::tests::{serialize_cells, spec};
use super::{BuiltinDeserializationError, ColumnIterator, CqlValue, DeserializeRow, Row};
use super::{BuiltinTypeCheckError, BuiltinTypeCheckErrorKind};

#[test]
fn test_tuple_deserialization() {
    // Empty tuple
    deserialize::<()>(&[], &Bytes::new()).unwrap();

    // 1-elem tuple
    let (a,) = deserialize::<(i32,)>(
        &[spec("i", ColumnType::Int)],
        &serialize_cells([val_int(123)]),
    )
    .unwrap();
    assert_eq!(a, 123);

    // 3-elem tuple
    let (a, b, c) = deserialize::<(i32, i32, i32)>(
        &[
            spec("i1", ColumnType::Int),
            spec("i2", ColumnType::Int),
            spec("i3", ColumnType::Int),
        ],
        &serialize_cells([val_int(123), val_int(456), val_int(789)]),
    )
    .unwrap();
    assert_eq!((a, b, c), (123, 456, 789));

    // Make sure that column type mismatch is detected
    deserialize::<(i32, String, i32)>(
        &[
            spec("i1", ColumnType::Int),
            spec("i2", ColumnType::Int),
            spec("i3", ColumnType::Int),
        ],
        &serialize_cells([val_int(123), val_int(456), val_int(789)]),
    )
    .unwrap_err();

    // Make sure that borrowing types compile and work correctly
    let specs = &[spec("s", ColumnType::Text)];
    let byts = serialize_cells([val_str("abc")]);
    let (s,) = deserialize::<(&str,)>(specs, &byts).unwrap();
    assert_eq!(s, "abc");
}

#[test]
fn test_deserialization_as_column_iterator() {
    let col_specs = [
        spec("i1", ColumnType::Int),
        spec("i2", ColumnType::Text),
        spec("i3", ColumnType::Counter),
    ];
    let serialized_values = serialize_cells([val_int(123), val_str("ScyllaDB"), None]);
    let mut iter = deserialize::<ColumnIterator>(&col_specs, &serialized_values).unwrap();

    let col1 = iter.next().unwrap().unwrap();
    assert_eq!(col1.spec.name(), "i1");
    assert_eq!(col1.spec.typ(), &ColumnType::Int);
    assert_eq!(col1.slice.unwrap().as_slice(), &123i32.to_be_bytes());

    let col2 = iter.next().unwrap().unwrap();
    assert_eq!(col2.spec.name(), "i2");
    assert_eq!(col2.spec.typ(), &ColumnType::Text);
    assert_eq!(col2.slice.unwrap().as_slice(), "ScyllaDB".as_bytes());

    let col3 = iter.next().unwrap().unwrap();
    assert_eq!(col3.spec.name(), "i3");
    assert_eq!(col3.spec.typ(), &ColumnType::Counter);
    assert!(col3.slice.is_none());

    assert!(iter.next().is_none());
}

// Do not remove. It's not used in tests but we keep it here to check that
// we properly ignore warnings about unused variables, unnecessary `mut`s
// etc. that usually pop up when generating code for empty structs.
#[allow(unused)]
#[derive(DeserializeRow)]
#[scylla(crate = crate)]
struct TestUdtWithNoFieldsUnordered {}

#[allow(unused)]
#[derive(DeserializeRow)]
#[scylla(crate = crate, enforce_order)]
struct TestUdtWithNoFieldsOrdered {}

#[test]
fn test_struct_deserialization_loose_ordering() {
    #[derive(DeserializeRow, PartialEq, Eq, Debug)]
    #[scylla(crate = "crate")]
    struct MyRow<'a> {
        a: &'a str,
        b: Option<i32>,
        #[scylla(skip)]
        c: String,
    }

    // Original order of columns
    let specs = &[spec("a", ColumnType::Text), spec("b", ColumnType::Int)];
    let byts = serialize_cells([val_str("abc"), val_int(123)]);
    let row = deserialize::<MyRow<'_>>(specs, &byts).unwrap();
    assert_eq!(
        row,
        MyRow {
            a: "abc",
            b: Some(123),
            c: String::new(),
        }
    );

    // Different order of columns - should still work
    let specs = &[spec("b", ColumnType::Int), spec("a", ColumnType::Text)];
    let byts = serialize_cells([val_int(123), val_str("abc")]);
    let row = deserialize::<MyRow<'_>>(specs, &byts).unwrap();
    assert_eq!(
        row,
        MyRow {
            a: "abc",
            b: Some(123),
            c: String::new(),
        }
    );

    // Missing column
    let specs = &[spec("a", ColumnType::Text)];
    MyRow::type_check(specs).unwrap_err();

    // Wrong column type
    let specs = &[spec("a", ColumnType::Int), spec("b", ColumnType::Int)];
    MyRow::type_check(specs).unwrap_err();
}

#[test]
fn test_struct_deserialization_strict_ordering() {
    #[derive(DeserializeRow, PartialEq, Eq, Debug)]
    #[scylla(crate = "crate", enforce_order)]
    struct MyRow<'a> {
        a: &'a str,
        b: Option<i32>,
        #[scylla(skip)]
        c: String,
    }

    // Correct order of columns
    let specs = &[spec("a", ColumnType::Text), spec("b", ColumnType::Int)];
    let byts = serialize_cells([val_str("abc"), val_int(123)]);
    let row = deserialize::<MyRow<'_>>(specs, &byts).unwrap();
    assert_eq!(
        row,
        MyRow {
            a: "abc",
            b: Some(123),
            c: String::new(),
        }
    );

    // Wrong order of columns
    let specs = &[spec("b", ColumnType::Int), spec("a", ColumnType::Text)];
    MyRow::type_check(specs).unwrap_err();

    // Missing column
    let specs = &[spec("a", ColumnType::Text)];
    MyRow::type_check(specs).unwrap_err();

    // Wrong column type
    let specs = &[spec("a", ColumnType::Int), spec("b", ColumnType::Int)];
    MyRow::type_check(specs).unwrap_err();
}

#[test]
fn test_struct_deserialization_no_name_check() {
    #[derive(DeserializeRow, PartialEq, Eq, Debug)]
    #[scylla(crate = "crate", enforce_order, skip_name_checks)]
    struct MyRow<'a> {
        a: &'a str,
        b: Option<i32>,
        #[scylla(skip)]
        c: String,
    }

    // Correct order of columns
    let specs = &[spec("a", ColumnType::Text), spec("b", ColumnType::Int)];
    let byts = serialize_cells([val_str("abc"), val_int(123)]);
    let row = deserialize::<MyRow<'_>>(specs, &byts).unwrap();
    assert_eq!(
        row,
        MyRow {
            a: "abc",
            b: Some(123),
            c: String::new(),
        }
    );

    // Correct order of columns, but different names - should still succeed
    let specs = &[spec("z", ColumnType::Text), spec("x", ColumnType::Int)];
    let byts = serialize_cells([val_str("abc"), val_int(123)]);
    let row = deserialize::<MyRow<'_>>(specs, &byts).unwrap();
    assert_eq!(
        row,
        MyRow {
            a: "abc",
            b: Some(123),
            c: String::new(),
        }
    );
}

#[test]
fn test_struct_deserialization_cross_rename_fields() {
    #[derive(scylla_macros::DeserializeRow, PartialEq, Eq, Debug)]
    #[scylla(crate = crate)]
    struct TestRow {
        #[scylla(rename = "b")]
        a: i32,
        #[scylla(rename = "a")]
        b: String,
    }

    // Columns switched wrt fields - should still work.
    {
        let row_bytes =
            serialize_cells(["The quick brown fox".as_bytes(), &42_i32.to_be_bytes()].map(Some));
        let specs = [spec("a", ColumnType::Text), spec("b", ColumnType::Int)];

        let row = deserialize::<TestRow>(&specs, &row_bytes).unwrap();
        assert_eq!(
            row,
            TestRow {
                a: 42,
                b: "The quick brown fox".to_owned(),
            }
        );
    }
}

fn val_int(i: i32) -> Option<Vec<u8>> {
    Some(i.to_be_bytes().to_vec())
}

fn val_str(s: &str) -> Option<Vec<u8>> {
    Some(s.as_bytes().to_vec())
}

fn deserialize<'frame, R>(
    specs: &'frame [ColumnSpec],
    byts: &'frame Bytes,
) -> Result<R, DeserializationError>
where
    R: DeserializeRow<'frame>,
{
    <R as DeserializeRow<'frame>>::type_check(specs)
        .map_err(|typecheck_err| DeserializationError(typecheck_err.0))?;
    let slice = FrameSlice::new(byts);
    let iter = ColumnIterator::new(specs, slice);
    <R as DeserializeRow<'frame>>::deserialize(iter)
}

#[track_caller]
pub(crate) fn get_typck_err_inner<'a>(
    err: &'a (dyn std::error::Error + 'static),
) -> &'a BuiltinTypeCheckError {
    match err.downcast_ref() {
        Some(err) => err,
        None => panic!("not a BuiltinTypeCheckError: {:?}", err),
    }
}

#[track_caller]
fn get_typck_err(err: &DeserializationError) -> &BuiltinTypeCheckError {
    get_typck_err_inner(err.0.as_ref())
}

#[track_caller]
fn get_deser_err(err: &DeserializationError) -> &BuiltinDeserializationError {
    match err.0.downcast_ref() {
        Some(err) => err,
        None => panic!("not a BuiltinDeserializationError: {:?}", err),
    }
}

#[test]
fn test_tuple_errors() {
    // Column type check failure
    {
        let col_name: &str = "i";
        let specs = &[spec(col_name, ColumnType::Int)];
        let err = deserialize::<(i64,)>(specs, &serialize_cells([val_int(123)])).unwrap_err();
        let err = get_typck_err(&err);
        assert_eq!(err.rust_name, std::any::type_name::<(i64,)>());
        assert_eq!(
            err.cql_types,
            specs
                .iter()
                .map(|spec| spec.typ().clone())
                .collect::<Vec<_>>()
        );
        let BuiltinTypeCheckErrorKind::ColumnTypeCheckFailed {
            column_index,
            column_name,
            err,
        } = &err.kind
        else {
            panic!("unexpected error kind: {}", err.kind)
        };
        assert_eq!(*column_index, 0);
        assert_eq!(column_name, col_name);
        let err = super::super::value::tests::get_typeck_err_inner(err.0.as_ref());
        assert_eq!(err.rust_name, std::any::type_name::<i64>());
        assert_eq!(err.cql_type, ColumnType::Int);
        assert_matches!(
            &err.kind,
            super::super::value::BuiltinTypeCheckErrorKind::MismatchedType {
                expected: &[ColumnType::BigInt]
            }
        );
    }

    // Column deserialization failure
    {
        let col_name: &str = "i";
        let err = deserialize::<(i64,)>(
            &[spec(col_name, ColumnType::BigInt)],
            &serialize_cells([val_int(123)]),
        )
        .unwrap_err();
        let err = get_deser_err(&err);
        assert_eq!(err.rust_name, std::any::type_name::<(i64,)>());
        let BuiltinDeserializationErrorKind::ColumnDeserializationFailed {
            column_name, err, ..
        } = &err.kind
        else {
            panic!("unexpected error kind: {}", err.kind)
        };
        assert_eq!(column_name, col_name);
        let err = super::super::value::tests::get_deser_err(err);
        assert_eq!(err.rust_name, std::any::type_name::<i64>());
        assert_eq!(err.cql_type, ColumnType::BigInt);
        assert_matches!(
            err.kind,
            super::super::value::BuiltinDeserializationErrorKind::ByteLengthMismatch {
                expected: 8,
                got: 4
            }
        );
    }

    // Raw column deserialization failure
    {
        let col_name: &str = "i";
        let err = deserialize::<(i64,)>(
            &[spec(col_name, ColumnType::BigInt)],
            &Bytes::from_static(b"alamakota"),
        )
        .unwrap_err();
        let err = get_deser_err(&err);
        assert_eq!(err.rust_name, std::any::type_name::<(i64,)>());
        let BuiltinDeserializationErrorKind::RawColumnDeserializationFailed {
            column_index: _column_index,
            column_name,
            err: _err,
        } = &err.kind
        else {
            panic!("unexpected error kind: {}", err.kind)
        };
        assert_eq!(column_name, col_name);
    }
}

#[test]
fn test_row_errors() {
    // Column type check failure - happens never, because Row consists of CqlValues,
    // which accept all CQL types.

    // Column deserialization failure
    {
        let col_name: &str = "i";
        let err = deserialize::<Row>(
            &[spec(col_name, ColumnType::BigInt)],
            &serialize_cells([val_int(123)]),
        )
        .unwrap_err();
        let err = get_deser_err(&err);
        assert_eq!(err.rust_name, std::any::type_name::<Row>());
        let BuiltinDeserializationErrorKind::ColumnDeserializationFailed {
            column_index: _column_index,
            column_name,
            err,
        } = &err.kind
        else {
            panic!("unexpected error kind: {}", err.kind)
        };
        assert_eq!(column_name, col_name);
        let err = super::super::value::tests::get_deser_err(err);
        assert_eq!(err.rust_name, std::any::type_name::<CqlValue>());
        assert_eq!(err.cql_type, ColumnType::BigInt);
        let super::super::value::BuiltinDeserializationErrorKind::ByteLengthMismatch {
            expected: 8,
            got: 4,
        } = &err.kind
        else {
            panic!("unexpected error kind: {}", err.kind)
        };
    }

    // Raw column deserialization failure
    {
        let col_name: &str = "i";
        let err = deserialize::<Row>(
            &[spec(col_name, ColumnType::BigInt)],
            &Bytes::from_static(b"alamakota"),
        )
        .unwrap_err();
        let err = get_deser_err(&err);
        assert_eq!(err.rust_name, std::any::type_name::<Row>());
        let BuiltinDeserializationErrorKind::RawColumnDeserializationFailed {
            column_index: _column_index,
            column_name,
            err: _err,
        } = &err.kind
        else {
            panic!("unexpected error kind: {}", err.kind)
        };
        assert_eq!(column_name, col_name);
    }
}

fn specs_to_types<'a>(specs: &[ColumnSpec<'a>]) -> Vec<ColumnType<'a>> {
    specs.iter().map(|spec| spec.typ().clone()).collect()
}

#[test]
fn test_struct_deserialization_errors() {
    // Loose ordering
    {
        #[derive(scylla_macros::DeserializeRow, PartialEq, Eq, Debug)]
        #[scylla(crate = "crate")]
        struct MyRow<'a> {
            a: &'a str,
            #[scylla(skip)]
            x: String,
            b: Option<i32>,
            #[scylla(rename = "c")]
            d: bool,
        }

        // Type check errors
        {
            // Missing column
            {
                let specs = [spec("a", ColumnType::Ascii), spec("b", ColumnType::Int)];
                let err = MyRow::type_check(&specs).unwrap_err();
                let err = get_typck_err_inner(err.0.as_ref());
                assert_eq!(err.rust_name, std::any::type_name::<MyRow>());
                assert_eq!(err.cql_types, specs_to_types(&specs));
                let BuiltinTypeCheckErrorKind::ValuesMissingForColumns {
                    column_names: ref missing_fields,
                } = err.kind
                else {
                    panic!("unexpected error kind: {:?}", err.kind)
                };
                assert_eq!(missing_fields.as_slice(), &["c"]);
            }

            // Duplicated column
            {
                let specs = [
                    spec("a", ColumnType::Ascii),
                    spec("b", ColumnType::Int),
                    spec("a", ColumnType::Ascii),
                ];

                let err = MyRow::type_check(&specs).unwrap_err();
                let err = get_typck_err_inner(err.0.as_ref());
                assert_eq!(err.rust_name, std::any::type_name::<MyRow>());
                assert_eq!(err.cql_types, specs_to_types(&specs));
                let BuiltinTypeCheckErrorKind::DuplicatedColumn {
                    column_index,
                    column_name,
                } = err.kind
                else {
                    panic!("unexpected error kind: {:?}", err.kind)
                };
                assert_eq!(column_index, 2);
                assert_eq!(column_name, "a");
            }

            // Unknown column
            {
                let specs = [
                    spec("d", ColumnType::Counter),
                    spec("a", ColumnType::Ascii),
                    spec("b", ColumnType::Int),
                ];

                let err = MyRow::type_check(&specs).unwrap_err();
                let err = get_typck_err_inner(err.0.as_ref());
                assert_eq!(err.rust_name, std::any::type_name::<MyRow>());
                assert_eq!(err.cql_types, specs_to_types(&specs));
                let BuiltinTypeCheckErrorKind::ColumnWithUnknownName {
                    column_index,
                    ref column_name,
                } = err.kind
                else {
                    panic!("unexpected error kind: {:?}", err.kind)
                };
                assert_eq!(column_index, 0);
                assert_eq!(column_name.as_str(), "d");
            }

            // Column incompatible types - column type check failed
            {
                let specs = [spec("b", ColumnType::Int), spec("a", ColumnType::Blob)];
                let err = MyRow::type_check(&specs).unwrap_err();
                let err = get_typck_err_inner(err.0.as_ref());
                assert_eq!(err.rust_name, std::any::type_name::<MyRow>());
                assert_eq!(err.cql_types, specs_to_types(&specs));
                let BuiltinTypeCheckErrorKind::ColumnTypeCheckFailed {
                    column_index,
                    ref column_name,
                    ref err,
                } = err.kind
                else {
                    panic!("unexpected error kind: {:?}", err.kind)
                };
                assert_eq!(column_index, 1);
                assert_eq!(column_name.as_str(), "a");
                let err = value::tests::get_typeck_err_inner(err.0.as_ref());
                assert_eq!(err.rust_name, std::any::type_name::<&str>());
                assert_eq!(err.cql_type, ColumnType::Blob);
                assert_matches!(
                    err.kind,
                    value::BuiltinTypeCheckErrorKind::MismatchedType {
                        expected: &[ColumnType::Ascii, ColumnType::Text]
                    }
                );
            }
        }

        // Deserialization errors
        {
            // Got null
            {
                let specs = [
                    spec("c", ColumnType::Boolean),
                    spec("a", ColumnType::Blob),
                    spec("b", ColumnType::Int),
                ];

                let err = MyRow::deserialize(ColumnIterator::new(
                    &specs,
                    FrameSlice::new(&serialize_cells([Some([true as u8])])),
                ))
                .unwrap_err();
                let err = get_deser_err(&err);
                assert_eq!(err.rust_name, std::any::type_name::<MyRow>());
                let BuiltinDeserializationErrorKind::RawColumnDeserializationFailed {
                    column_index,
                    ref column_name,
                    ..
                } = err.kind
                else {
                    panic!("unexpected error kind: {:?}", err.kind)
                };
                assert_eq!(column_index, 1);
                assert_eq!(column_name, "a");
            }

            // Column deserialization failed
            {
                let specs = [
                    spec("b", ColumnType::Int),
                    spec("a", ColumnType::Ascii),
                    spec("c", ColumnType::Boolean),
                ];

                let row_bytes = serialize_cells(
                    [
                        &0_i32.to_be_bytes(),
                        "alamakota".as_bytes(),
                        &42_i16.to_be_bytes(),
                    ]
                    .map(Some),
                );

                let err = deserialize::<MyRow>(&specs, &row_bytes).unwrap_err();

                let err = get_deser_err(&err);
                assert_eq!(err.rust_name, std::any::type_name::<MyRow>());
                let BuiltinDeserializationErrorKind::ColumnDeserializationFailed {
                    column_index,
                    ref column_name,
                    ref err,
                } = err.kind
                else {
                    panic!("unexpected error kind: {:?}", err.kind)
                };
                assert_eq!(column_index, 2);
                assert_eq!(column_name.as_str(), "c");
                let err = value::tests::get_deser_err(err);
                assert_eq!(err.rust_name, std::any::type_name::<bool>());
                assert_eq!(err.cql_type, ColumnType::Boolean);
                assert_matches!(
                    err.kind,
                    value::BuiltinDeserializationErrorKind::ByteLengthMismatch {
                        expected: 1,
                        got: 2,
                    }
                );
            }
        }
    }

    // Strict ordering
    {
        #[derive(scylla_macros::DeserializeRow, PartialEq, Eq, Debug)]
        #[scylla(crate = "crate", enforce_order)]
        struct MyRow<'a> {
            a: &'a str,
            #[scylla(skip)]
            x: String,
            b: Option<i32>,
            c: bool,
        }

        // Type check errors
        {
            // Too few columns
            {
                let specs = [spec("a", ColumnType::Text)];
                let err = MyRow::type_check(&specs).unwrap_err();
                let err = get_typck_err_inner(err.0.as_ref());
                assert_eq!(err.rust_name, std::any::type_name::<MyRow>());
                assert_eq!(err.cql_types, specs_to_types(&specs));
                let BuiltinTypeCheckErrorKind::WrongColumnCount {
                    rust_cols,
                    cql_cols,
                } = err.kind
                else {
                    panic!("unexpected error kind: {:?}", err.kind)
                };
                assert_eq!(rust_cols, 3);
                assert_eq!(cql_cols, 1);
            }

            // Excess columns
            {
                let specs = [
                    spec("a", ColumnType::Text),
                    spec("b", ColumnType::Int),
                    spec("c", ColumnType::Boolean),
                    spec("d", ColumnType::Counter),
                ];
                let err = MyRow::type_check(&specs).unwrap_err();
                let err = get_typck_err_inner(err.0.as_ref());
                assert_eq!(err.rust_name, std::any::type_name::<MyRow>());
                assert_eq!(err.cql_types, specs_to_types(&specs));
                let BuiltinTypeCheckErrorKind::WrongColumnCount {
                    rust_cols,
                    cql_cols,
                } = err.kind
                else {
                    panic!("unexpected error kind: {:?}", err.kind)
                };
                assert_eq!(rust_cols, 3);
                assert_eq!(cql_cols, 4);
            }

            // Renamed column name mismatch
            {
                let specs = [
                    spec("a", ColumnType::Text),
                    spec("b", ColumnType::Int),
                    spec("d", ColumnType::Boolean),
                ];
                let err = MyRow::type_check(&specs).unwrap_err();
                let err = get_typck_err_inner(err.0.as_ref());
                assert_eq!(err.rust_name, std::any::type_name::<MyRow>());
                let BuiltinTypeCheckErrorKind::ColumnNameMismatch {
                    field_index,
                    column_index,
                    rust_column_name,
                    ref db_column_name,
                } = err.kind
                else {
                    panic!("unexpected error kind: {:?}", err.kind)
                };
                assert_eq!(field_index, 3);
                assert_eq!(rust_column_name, "c");
                assert_eq!(column_index, 2);
                assert_eq!(db_column_name.as_str(), "d");
            }

            // Columns switched - column name mismatch
            {
                let specs = [
                    spec("b", ColumnType::Int),
                    spec("a", ColumnType::Text),
                    spec("c", ColumnType::Boolean),
                ];
                let err = MyRow::type_check(&specs).unwrap_err();
                let err = get_typck_err_inner(err.0.as_ref());
                assert_eq!(err.rust_name, std::any::type_name::<MyRow>());
                assert_eq!(err.cql_types, specs_to_types(&specs));
                let BuiltinTypeCheckErrorKind::ColumnNameMismatch {
                    field_index,
                    column_index,
                    rust_column_name,
                    ref db_column_name,
                } = err.kind
                else {
                    panic!("unexpected error kind: {:?}", err.kind)
                };
                assert_eq!(field_index, 0);
                assert_eq!(column_index, 0);
                assert_eq!(rust_column_name, "a");
                assert_eq!(db_column_name.as_str(), "b");
            }

            // Column incompatible types - column type check failed
            {
                let specs = [
                    spec("a", ColumnType::Blob),
                    spec("b", ColumnType::Int),
                    spec("c", ColumnType::Boolean),
                ];
                let err = MyRow::type_check(&specs).unwrap_err();
                let err = get_typck_err_inner(err.0.as_ref());
                assert_eq!(err.rust_name, std::any::type_name::<MyRow>());
                assert_eq!(err.cql_types, specs_to_types(&specs));
                let BuiltinTypeCheckErrorKind::ColumnTypeCheckFailed {
                    column_index,
                    ref column_name,
                    ref err,
                } = err.kind
                else {
                    panic!("unexpected error kind: {:?}", err.kind)
                };
                assert_eq!(column_index, 0);
                assert_eq!(column_name.as_str(), "a");
                let err = value::tests::get_typeck_err_inner(err.0.as_ref());
                assert_eq!(err.rust_name, std::any::type_name::<&str>());
                assert_eq!(err.cql_type, ColumnType::Blob);
                assert_matches!(
                    err.kind,
                    value::BuiltinTypeCheckErrorKind::MismatchedType {
                        expected: &[ColumnType::Ascii, ColumnType::Text]
                    }
                );
            }
        }

        // Deserialization errors
        {
            // Too few columns
            {
                let specs = [
                    spec("a", ColumnType::Text),
                    spec("b", ColumnType::Int),
                    spec("c", ColumnType::Boolean),
                ];

                let err = MyRow::deserialize(ColumnIterator::new(
                    &specs,
                    FrameSlice::new(&serialize_cells([Some([true as u8])])),
                ))
                .unwrap_err();
                let err = get_deser_err(&err);
                assert_eq!(err.rust_name, std::any::type_name::<MyRow>());
                let BuiltinDeserializationErrorKind::RawColumnDeserializationFailed {
                    column_index,
                    ref column_name,
                    ..
                } = err.kind
                else {
                    panic!("unexpected error kind: {:?}", err.kind)
                };
                assert_eq!(column_index, 1);
                assert_eq!(column_name, "b");
            }

            // Bad field format
            {
                let typ = [
                    spec("a", ColumnType::Text),
                    spec("b", ColumnType::Int),
                    spec("c", ColumnType::Boolean),
                ];

                let row_bytes = serialize_cells(
                    [(&b"alamakota"[..]), &42_i32.to_be_bytes(), &[true as u8]].map(Some),
                );

                let row_bytes_too_short = row_bytes.slice(..row_bytes.len() - 1);
                assert!(row_bytes.len() > row_bytes_too_short.len());

                let err = deserialize::<MyRow>(&typ, &row_bytes_too_short).unwrap_err();

                let err = get_deser_err(&err);
                assert_eq!(err.rust_name, std::any::type_name::<MyRow>());
                let BuiltinDeserializationErrorKind::RawColumnDeserializationFailed {
                    column_index,
                    ref column_name,
                    ..
                } = err.kind
                else {
                    panic!("unexpected error kind: {:?}", err.kind)
                };
                assert_eq!(column_index, 2);
                assert_eq!(column_name, "c");
            }

            // Column deserialization failed
            {
                let specs = [
                    spec("a", ColumnType::Text),
                    spec("b", ColumnType::Int),
                    spec("c", ColumnType::Boolean),
                ];

                let row_bytes = serialize_cells(
                    [&b"alamakota"[..], &42_i64.to_be_bytes(), &[true as u8]].map(Some),
                );

                let err = deserialize::<MyRow>(&specs, &row_bytes).unwrap_err();

                let err = get_deser_err(&err);
                assert_eq!(err.rust_name, std::any::type_name::<MyRow>());
                let BuiltinDeserializationErrorKind::ColumnDeserializationFailed {
                    column_index: field_index,
                    ref column_name,
                    ref err,
                } = err.kind
                else {
                    panic!("unexpected error kind: {:?}", err.kind)
                };
                assert_eq!(column_name.as_str(), "b");
                assert_eq!(field_index, 2);
                let err = value::tests::get_deser_err(err);
                assert_eq!(err.rust_name, std::any::type_name::<i32>());
                assert_eq!(err.cql_type, ColumnType::Int);
                assert_matches!(
                    err.kind,
                    value::BuiltinDeserializationErrorKind::ByteLengthMismatch {
                        expected: 4,
                        got: 8,
                    }
                );
            }
        }
    }
}
