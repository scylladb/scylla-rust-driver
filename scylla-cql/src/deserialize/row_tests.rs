use assert_matches::assert_matches;
use bytes::Bytes;
use scylla_macros::DeserializeRow;

use crate::deserialize::row::BuiltinDeserializationErrorKind;
use crate::deserialize::{DeserializationError, FrameSlice, value};
use crate::frame::response::result::{ColumnSpec, ColumnType, NativeType, TableSpec};

use super::super::tests::{serialize_cells, spec};
use super::{BuiltinDeserializationError, ColumnIterator, CqlValue, DeserializeRow, Row};
use super::{BuiltinTypeCheckError, BuiltinTypeCheckErrorKind};

#[test]
fn test_tuple_deserialization() {
    // Empty tuple
    deserialize::<()>(&[], &Bytes::new()).unwrap();

    // 1-elem tuple
    let (a,) = deserialize::<(i32,)>(
        &[spec("i", ColumnType::Native(NativeType::Int))],
        &serialize_cells([val_int(123)]),
    )
    .unwrap();
    assert_eq!(a, 123);

    // 3-elem tuple
    let (a, b, c) = deserialize::<(i32, i32, i32)>(
        &[
            spec("i1", ColumnType::Native(NativeType::Int)),
            spec("i2", ColumnType::Native(NativeType::Int)),
            spec("i3", ColumnType::Native(NativeType::Int)),
        ],
        &serialize_cells([val_int(123), val_int(456), val_int(789)]),
    )
    .unwrap();
    assert_eq!((a, b, c), (123, 456, 789));

    // Make sure that column type mismatch is detected
    deserialize::<(i32, String, i32)>(
        &[
            spec("i1", ColumnType::Native(NativeType::Int)),
            spec("i2", ColumnType::Native(NativeType::Int)),
            spec("i3", ColumnType::Native(NativeType::Int)),
        ],
        &serialize_cells([val_int(123), val_int(456), val_int(789)]),
    )
    .unwrap_err();

    // Make sure that borrowing types compile and work correctly
    let specs = &[spec("s", ColumnType::Native(NativeType::Text))];
    let byts = serialize_cells([val_str("abc")]);
    let (s,) = deserialize::<(&str,)>(specs, &byts).unwrap();
    assert_eq!(s, "abc");
}

#[test]
fn test_deserialization_as_column_iterator() {
    let col_specs = [
        spec("i1", ColumnType::Native(NativeType::Int)),
        spec("i2", ColumnType::Native(NativeType::Text)),
        spec("i3", ColumnType::Native(NativeType::Counter)),
    ];
    let serialized_values = serialize_cells([val_int(123), val_str("ScyllaDB"), None]);
    let mut iter = deserialize::<ColumnIterator>(&col_specs, &serialized_values).unwrap();

    let col1 = iter.next().unwrap().unwrap();
    assert_eq!(col1.spec.name(), "i1");
    assert_eq!(col1.spec.typ(), &ColumnType::Native(NativeType::Int));
    assert_eq!(col1.slice.unwrap().as_slice(), &123i32.to_be_bytes());

    let col2 = iter.next().unwrap().unwrap();
    assert_eq!(col2.spec.name(), "i2");
    assert_eq!(col2.spec.typ(), &ColumnType::Native(NativeType::Text));
    assert_eq!(col2.slice.unwrap().as_slice(), "ScyllaDB".as_bytes());

    let col3 = iter.next().unwrap().unwrap();
    assert_eq!(col3.spec.name(), "i3");
    assert_eq!(col3.spec.typ(), &ColumnType::Native(NativeType::Counter));
    assert!(col3.slice.is_none());

    assert!(iter.next().is_none());
}

// Do not remove. It's not used in tests but we keep it here to check that
// we properly ignore warnings about unused variables, unnecessary `mut`s
// etc. that usually pop up when generating code for empty structs.
#[derive(DeserializeRow)]
#[scylla(crate = crate)]
struct TestUdtWithNoFieldsUnordered {}

#[derive(DeserializeRow)]
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
fn test_struct_deserialization_loose_ordering() {
    #[derive(DeserializeRow, PartialEq, Eq, Debug)]
    #[scylla(crate = "crate")]
    struct MyRow<'a> {
        a: &'a str,
        b: Option<i32>,
        #[scylla(skip)]
        c: String,
        #[scylla(default_when_null)]
        d: i32,
        #[scylla(default_when_null)]
        e: &'a str,
        #[scylla(allow_missing)]
        f: &'a str,
    }

    // Original order of columns without field f
    let specs = &[
        spec("a", ColumnType::Native(NativeType::Text)),
        spec("b", ColumnType::Native(NativeType::Int)),
        spec("d", ColumnType::Native(NativeType::Int)),
        spec("e", ColumnType::Native(NativeType::Text)),
    ];
    let byts = serialize_cells([val_str("abc"), val_int(123), None, val_str("def")]);
    let row = deserialize::<MyRow<'_>>(specs, &byts).unwrap();
    assert_eq!(
        row,
        MyRow {
            a: "abc",
            b: Some(123),
            c: String::new(),
            d: 0,
            e: "def",
            f: "",
        }
    );

    // Different order of columns with field f - should still work with
    let specs = &[
        spec("e", ColumnType::Native(NativeType::Text)),
        spec("b", ColumnType::Native(NativeType::Int)),
        spec("d", ColumnType::Native(NativeType::Int)),
        spec("f", ColumnType::Native(NativeType::Text)),
        spec("a", ColumnType::Native(NativeType::Text)),
    ];
    let byts = serialize_cells([None, val_int(123), None, val_str("efg"), val_str("abc")]);
    let row = deserialize::<MyRow<'_>>(specs, &byts).unwrap();
    assert_eq!(
        row,
        MyRow {
            a: "abc",
            b: Some(123),
            c: String::new(),
            d: 0,
            e: "",
            f: "efg",
        }
    );

    // Missing column
    let specs = &[
        spec("a", ColumnType::Native(NativeType::Text)),
        spec("e", ColumnType::Native(NativeType::Text)),
    ];
    MyRow::type_check(specs).unwrap_err();

    // Missing both default_when_null column
    let specs = &[
        spec("a", ColumnType::Native(NativeType::Text)),
        spec("b", ColumnType::Native(NativeType::Int)),
    ];
    MyRow::type_check(specs).unwrap_err();

    // Wrong column type
    let specs = &[
        spec("a", ColumnType::Native(NativeType::Int)),
        spec("b", ColumnType::Native(NativeType::Int)),
    ];
    MyRow::type_check(specs).unwrap_err();
}

#[test]
fn test_struct_deserialization_strict_ordering() {
    #[derive(DeserializeRow, PartialEq, Eq, Debug)]
    #[scylla(crate = "crate", flavor = "enforce_order")]
    struct MyRow<'a> {
        a: &'a str,
        b: Option<i32>,
        #[scylla(skip)]
        c: String,
        #[scylla(default_when_null)]
        d: i32,
        #[scylla(allow_missing)]
        f: i32,
        #[scylla(default_when_null)]
        e: &'a str,
    }

    // Correct order of columns without field f
    let specs = &[
        spec("a", ColumnType::Native(NativeType::Text)),
        spec("b", ColumnType::Native(NativeType::Int)),
        spec("d", ColumnType::Native(NativeType::Int)),
        spec("e", ColumnType::Native(NativeType::Text)),
    ];
    let byts = serialize_cells([val_str("abc"), val_int(123), None, val_str("def")]);
    let row = deserialize::<MyRow<'_>>(specs, &byts).unwrap();
    assert_eq!(
        row,
        MyRow {
            a: "abc",
            b: Some(123),
            c: String::new(),
            d: 0,
            e: "def",
            f: 0,
        }
    );

    // Correct order of columns with field f
    let specs = &[
        spec("a", ColumnType::Native(NativeType::Text)),
        spec("b", ColumnType::Native(NativeType::Int)),
        spec("d", ColumnType::Native(NativeType::Int)),
        spec("f", ColumnType::Native(NativeType::Int)),
        spec("e", ColumnType::Native(NativeType::Text)),
    ];
    let byts = serialize_cells([
        val_str("abc"),
        val_int(123),
        None,
        val_int(234),
        val_str("def"),
    ]);
    let row = deserialize::<MyRow<'_>>(specs, &byts).unwrap();
    assert_eq!(
        row,
        MyRow {
            a: "abc",
            b: Some(123),
            c: String::new(),
            d: 0,
            e: "def",
            f: 234,
        }
    );

    // Wrong order of columns
    let specs = &[
        spec("b", ColumnType::Native(NativeType::Int)),
        spec("a", ColumnType::Native(NativeType::Text)),
        spec("d", ColumnType::Native(NativeType::Int)),
        spec("e", ColumnType::Native(NativeType::Text)),
    ];
    MyRow::type_check(specs).unwrap_err();

    // Missing column
    let specs = &[
        spec("a", ColumnType::Native(NativeType::Text)),
        spec("e", ColumnType::Native(NativeType::Text)),
    ];
    MyRow::type_check(specs).unwrap_err();

    // Missing both default_when_null column
    let specs = &[
        spec("a", ColumnType::Native(NativeType::Text)),
        spec("b", ColumnType::Native(NativeType::Int)),
    ];
    MyRow::type_check(specs).unwrap_err();

    // Wrong column type
    let specs = &[
        spec("a", ColumnType::Native(NativeType::Int)),
        spec("b", ColumnType::Native(NativeType::Int)),
    ];
    MyRow::type_check(specs).unwrap_err();
}

#[test]
fn test_struct_deserialization_no_name_check() {
    #[derive(DeserializeRow, PartialEq, Eq, Debug)]
    #[scylla(crate = "crate", flavor = "enforce_order", skip_name_checks)]
    struct MyRow<'a> {
        a: &'a str,
        b: Option<i32>,
        #[scylla(skip)]
        c: String,
    }

    // Correct order of columns
    let specs = &[
        spec("a", ColumnType::Native(NativeType::Text)),
        spec("b", ColumnType::Native(NativeType::Int)),
    ];
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
    let specs = &[
        spec("z", ColumnType::Native(NativeType::Text)),
        spec("x", ColumnType::Native(NativeType::Int)),
    ];
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
        let specs = [
            spec("a", ColumnType::Native(NativeType::Text)),
            spec("b", ColumnType::Native(NativeType::Int)),
        ];

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

pub(crate) fn deserialize<'frame, 'metadata, R>(
    specs: &'metadata [ColumnSpec<'metadata>],
    byts: &'frame Bytes,
) -> Result<R, DeserializationError>
where
    R: DeserializeRow<'frame, 'metadata>,
{
    <R as DeserializeRow<'frame, 'metadata>>::type_check(specs)
        .map_err(|typecheck_err| DeserializationError(typecheck_err.0))?;
    let slice = FrameSlice::new(byts);
    let iter = ColumnIterator::new(specs, slice);
    <R as DeserializeRow<'frame, 'metadata>>::deserialize(iter)
}

#[track_caller]
pub(crate) fn get_typck_err_inner<'a>(
    err: &'a (dyn std::error::Error + 'static),
) -> &'a BuiltinTypeCheckError {
    match err.downcast_ref() {
        Some(err) => err,
        None => panic!("not a BuiltinTypeCheckError: {err:?}"),
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
        None => panic!("not a BuiltinDeserializationError: {err:?}"),
    }
}

#[test]
fn test_tuple_errors() {
    // Column type check failure
    {
        let col_name: &str = "i";
        let specs = &[spec(col_name, ColumnType::Native(NativeType::Int))];
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
        assert_eq!(err.cql_type, ColumnType::Native(NativeType::Int));
        assert_matches!(
            &err.kind,
            &super::super::value::BuiltinTypeCheckErrorKind::MismatchedType {
                expected: &[ColumnType::Native(NativeType::BigInt)]
            }
        );
    }

    // Column deserialization failure
    {
        let col_name: &str = "i";
        let err = deserialize::<(i64,)>(
            &[spec(col_name, ColumnType::Native(NativeType::BigInt))],
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
        assert_eq!(err.cql_type, ColumnType::Native(NativeType::BigInt));
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
            &[spec(col_name, ColumnType::Native(NativeType::BigInt))],
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
            &[spec(col_name, ColumnType::Native(NativeType::BigInt))],
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
        assert_eq!(err.rust_name, std::any::type_name::<Option<CqlValue>>());
        assert_eq!(err.cql_type, ColumnType::Native(NativeType::BigInt));
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
            &[spec(col_name, ColumnType::Native(NativeType::BigInt))],
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
            #[scylla(default_when_null)]
            e: Option<i32>,
        }

        // Type check errors
        {
            // Missing column
            {
                let specs = [
                    spec("a", ColumnType::Native(NativeType::Ascii)),
                    spec("b", ColumnType::Native(NativeType::Int)),
                ];
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
                assert_eq!(missing_fields.as_slice(), &["c", "e"]);
            }

            // Duplicated column
            {
                let specs = [
                    spec("a", ColumnType::Native(NativeType::Ascii)),
                    spec("b", ColumnType::Native(NativeType::Int)),
                    spec("a", ColumnType::Native(NativeType::Ascii)),
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
                    spec("d", ColumnType::Native(NativeType::Counter)),
                    spec("a", ColumnType::Native(NativeType::Ascii)),
                    spec("b", ColumnType::Native(NativeType::Int)),
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
                let specs = [
                    spec("b", ColumnType::Native(NativeType::Int)),
                    spec("a", ColumnType::Native(NativeType::Blob)),
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
                assert_eq!(column_index, 1);
                assert_eq!(column_name.as_str(), "a");
                let err = value::tests::get_typeck_err_inner(err.0.as_ref());
                assert_eq!(err.rust_name, std::any::type_name::<&str>());
                assert_eq!(err.cql_type, ColumnType::Native(NativeType::Blob));
                assert_matches!(
                    err.kind,
                    value::BuiltinTypeCheckErrorKind::MismatchedType {
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
                let specs = [
                    spec("c", ColumnType::Native(NativeType::Boolean)),
                    spec("a", ColumnType::Native(NativeType::Blob)),
                    spec("b", ColumnType::Native(NativeType::Int)),
                    spec("e", ColumnType::Native(NativeType::Int)),
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
                    spec("b", ColumnType::Native(NativeType::Int)),
                    spec("a", ColumnType::Native(NativeType::Ascii)),
                    spec("c", ColumnType::Native(NativeType::Boolean)),
                    spec("e", ColumnType::Native(NativeType::Int)),
                ];

                let row_bytes = serialize_cells(
                    [
                        &0_i32.to_be_bytes(),
                        "alamakota".as_bytes(),
                        &42_i16.to_be_bytes(),
                        &13_i32.to_be_bytes(),
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
                assert_eq!(err.cql_type, ColumnType::Native(NativeType::Boolean));
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
        #[scylla(crate = "crate", flavor = "enforce_order")]
        struct MyRow<'a> {
            a: &'a str,
            #[scylla(skip)]
            x: String,
            b: Option<i32>,
            c: bool,
            #[scylla(default_when_null)]
            d: Option<i32>,
        }

        // Type check errors
        {
            // Too few columns
            {
                let specs = [spec("a", ColumnType::Native(NativeType::Text))];
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
                assert_eq!(rust_cols, 4);
                assert_eq!(cql_cols, 1);
            }

            // Excess columns
            {
                let specs = [
                    spec("a", ColumnType::Native(NativeType::Text)),
                    spec("b", ColumnType::Native(NativeType::Int)),
                    spec("c", ColumnType::Native(NativeType::Boolean)),
                    spec("d", ColumnType::Native(NativeType::Int)),
                    spec("e", ColumnType::Native(NativeType::Counter)),
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
                assert_eq!(rust_cols, 4);
                assert_eq!(cql_cols, 5);
            }

            // Renamed column name mismatch
            {
                let specs = [
                    spec("a", ColumnType::Native(NativeType::Text)),
                    spec("b", ColumnType::Native(NativeType::Int)),
                    spec("e", ColumnType::Native(NativeType::Boolean)),
                    spec("d", ColumnType::Native(NativeType::Int)),
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
                assert_eq!(db_column_name.as_str(), "e");
            }

            // Columns switched - column name mismatch
            {
                let specs = [
                    spec("b", ColumnType::Native(NativeType::Int)),
                    spec("a", ColumnType::Native(NativeType::Text)),
                    spec("c", ColumnType::Native(NativeType::Boolean)),
                    spec("d", ColumnType::Native(NativeType::Int)),
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
                    spec("a", ColumnType::Native(NativeType::Blob)),
                    spec("b", ColumnType::Native(NativeType::Int)),
                    spec("c", ColumnType::Native(NativeType::Boolean)),
                    spec("d", ColumnType::Native(NativeType::Int)),
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
                assert_eq!(err.cql_type, ColumnType::Native(NativeType::Blob));
                assert_matches!(
                    err.kind,
                    value::BuiltinTypeCheckErrorKind::MismatchedType {
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
            // Too few columns
            {
                let specs = [
                    spec("a", ColumnType::Native(NativeType::Text)),
                    spec("b", ColumnType::Native(NativeType::Int)),
                    spec("c", ColumnType::Native(NativeType::Boolean)),
                    spec("d", ColumnType::Native(NativeType::Int)),
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
                    spec("a", ColumnType::Native(NativeType::Text)),
                    spec("b", ColumnType::Native(NativeType::Int)),
                    spec("c", ColumnType::Native(NativeType::Boolean)),
                    spec("d", ColumnType::Native(NativeType::Int)),
                ];

                let row_bytes = serialize_cells(
                    [
                        (&b"alamakota"[..]),
                        &42_i32.to_be_bytes(),
                        &[true as u8],
                        &13_i32.to_be_bytes(),
                    ]
                    .map(Some),
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
                assert_eq!(column_index, 3);
                assert_eq!(column_name, "d");
            }

            // Column deserialization failed
            {
                let specs = [
                    spec("a", ColumnType::Native(NativeType::Text)),
                    spec("b", ColumnType::Native(NativeType::Int)),
                    spec("c", ColumnType::Native(NativeType::Boolean)),
                    spec("d", ColumnType::Native(NativeType::Int)),
                ];

                let row_bytes = serialize_cells(
                    [
                        &b"alamakota"[..],
                        &42_i64.to_be_bytes(),
                        &[true as u8],
                        &13_i32.to_be_bytes(),
                    ]
                    .map(Some),
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
                assert_eq!(err.rust_name, std::any::type_name::<Option<i32>>());
                assert_eq!(err.cql_type, ColumnType::Native(NativeType::Int));
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

#[test]
fn metadata_does_not_bound_deserialized_rows() {
    /* It's important to understand what is a _deserialized row_. It's not just
     * an implementor of `DeserializeRow`; there are some implementors of `DeserializeRow`
     * who are not yet final rows, but partially deserialized rows that support further
     * deserialization - _row deserializers_, such as `ColumnIterator`.
     * _Row deserializers_, because they still need to deserialize some row, are naturally
     * bound by 'metadata lifetime. However, _rows_ are completely deserialized, so they
     * should not be bound by 'metadata - only by 'frame. This test asserts that.
     */

    // We don't care about the actual deserialized data - all `Err`s is OK.
    // This test's goal is only to compile, asserting that lifetimes are correct.
    let bytes = Bytes::new();

    // By this binding, we require that the deserialized rows live longer than metadata.
    let _decoded_results = {
        // Metadata's lifetime is limited to this scope.

        fn col_spec<'a>(name: &'a str, typ: ColumnType<'a>) -> ColumnSpec<'a> {
            ColumnSpec::borrowed(name, typ, TableSpec::borrowed("ks", "tbl"))
        }

        let row_typ = &[
            col_spec("bytes", ColumnType::Native(NativeType::Blob)),
            col_spec("text", ColumnType::Native(NativeType::Text)),
        ];

        // Tuple
        let decoded_tuple_res = deserialize::<(&[u8], &str)>(row_typ, &bytes);

        // Custom struct
        #[derive(DeserializeRow)]
        #[scylla(crate=crate)]
        struct MyRow<'frame> {
            #[expect(dead_code)]
            bytes: &'frame [u8],
            #[expect(dead_code)]
            text: &'frame str,
        }
        let decoded_custom_struct_res = deserialize::<MyRow>(row_typ, &bytes);

        (decoded_tuple_res, decoded_custom_struct_res)
    };
}
