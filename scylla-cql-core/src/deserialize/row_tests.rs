use assert_matches::assert_matches;
use bytes::Bytes;

use crate::deserialize::row::BuiltinDeserializationErrorKind;
use crate::deserialize::{DeserializationError, FrameSlice, TypeCheckError};
use crate::frame::response::result::{ColumnSpec, ColumnType, NativeType, TableSpec};

use super::super::tests::{serialize_cells, spec};
use super::{BuiltinDeserializationError, ColumnIterator, DeserializeRow};
use super::{BuiltinTypeCheckError, BuiltinTypeCheckErrorKind};
use crate::value::{CqlValue, Row};

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

fn val_int(i: i32) -> Option<Vec<u8>> {
    Some(i.to_be_bytes().to_vec())
}

fn val_str(s: &str) -> Option<Vec<u8>> {
    Some(s.as_bytes().to_vec())
}

/// Wrapper for type check vs. deserialization errors, since we no longer convert
/// TypeCheckError into DeserializationError.
#[derive(Debug)]
enum TestDeserializeError {
    TypeCheck(TypeCheckError),
    Deserialization(DeserializationError),
}

fn deserialize<'frame, 'metadata, R>(
    specs: &'metadata [ColumnSpec<'metadata>],
    byts: &'frame Bytes,
) -> Result<R, TestDeserializeError>
where
    R: DeserializeRow<'frame, 'metadata>,
{
    <R as DeserializeRow<'frame, 'metadata>>::type_check(specs)
        .map_err(TestDeserializeError::TypeCheck)?;
    let slice = FrameSlice::new(byts);
    let iter = ColumnIterator::new(specs, slice);
    <R as DeserializeRow<'frame, 'metadata>>::deserialize(iter)
        .map_err(TestDeserializeError::Deserialization)
}

#[track_caller]
fn get_typck_err_inner(err: &TypeCheckError) -> &BuiltinTypeCheckError {
    match err.downcast_ref() {
        Some(err) => err,
        None => panic!("not a BuiltinTypeCheckError: {err:?}"),
    }
}

#[track_caller]
fn get_typck_err(err: &TestDeserializeError) -> &BuiltinTypeCheckError {
    match err {
        TestDeserializeError::TypeCheck(err) => get_typck_err_inner(err),
        other => panic!("expected TypeCheck error, got: {other:?}"),
    }
}

#[track_caller]
fn get_deser_err_inner(err: &DeserializationError) -> &BuiltinDeserializationError {
    match err.downcast_ref() {
        Some(err) => err,
        None => panic!("not a BuiltinDeserializationError: {err:?}"),
    }
}

#[track_caller]
fn get_deser_err(err: &TestDeserializeError) -> &BuiltinDeserializationError {
    match err {
        TestDeserializeError::Deserialization(err) => get_deser_err_inner(err),
        other => panic!("expected Deserialization error, got: {other:?}"),
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
        let err = super::super::value::tests::get_typeck_err_inner(err);
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
        let err = super::super::value::tests::get_deser_err_inner(err);
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
        let err = super::super::value::tests::get_deser_err_inner(err);
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

        deserialize::<(&[u8], &str)>(row_typ, &bytes)
    };
}
