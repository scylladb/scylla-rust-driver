use std::collections::BTreeMap;

use crate::frame::response::result::{ColumnSpec, ColumnType, NativeType, TableSpec};
use crate::frame::types::RawValue;
use crate::serialize::row::{
    BuiltinSerializationError, BuiltinSerializationErrorKind, BuiltinTypeCheckError,
    BuiltinTypeCheckErrorKind, RowSerializationContext, SerializeRow, SerializedValues,
};
use crate::serialize::value::SerializeValue;
use crate::serialize::writers::WrittenCellProof;
use crate::serialize::{CellWriter, RowWriter, SerializationError};
use crate::value::MaybeUnset;

use assert_matches::assert_matches;

pub(crate) fn do_serialize<T: SerializeRow>(t: T, columns: &[ColumnSpec]) -> Vec<u8> {
    let ctx = RowSerializationContext::from_specs(columns);
    let mut ret = Vec::new();
    let mut builder = RowWriter::new(&mut ret);
    t.serialize(&ctx, &mut builder).unwrap();
    ret
}

fn do_serialize_err<T: SerializeRow>(t: T, columns: &[ColumnSpec]) -> SerializationError {
    let ctx = RowSerializationContext::from_specs(columns);
    let mut ret = Vec::new();
    let mut builder = RowWriter::new(&mut ret);
    t.serialize(&ctx, &mut builder).unwrap_err()
}

fn col<'a>(name: &'a str, typ: ColumnType<'a>) -> ColumnSpec<'a> {
    ColumnSpec::borrowed(name, typ, TableSpec::borrowed("ks", "tbl"))
}

fn col_owned(name: String, typ: ColumnType<'static>) -> ColumnSpec<'static> {
    ColumnSpec::owned(name, typ, TableSpec::borrowed("ks", "tbl"))
}

fn get_typeck_err(err: &SerializationError) -> &BuiltinTypeCheckError {
    match err.downcast_ref() {
        Some(err) => err,
        None => panic!("not a BuiltinTypeCheckError: {err}"),
    }
}

fn get_ser_err(err: &SerializationError) -> &BuiltinSerializationError {
    match err.downcast_ref() {
        Some(err) => err,
        None => panic!("not a BuiltinSerializationError: {err}"),
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
    let columns = [
        col("a", ColumnType::Native(NativeType::Int)),
        col("b", ColumnType::Native(NativeType::Text)),
        col("c", ColumnType::Native(NativeType::BigInt)),
        col("d", ColumnType::Native(NativeType::Ascii)),
    ];
    let ctx = RowSerializationContext::from_specs(&columns);

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
    assert!(
        values
            .iter()
            .all(|v| v == RawValue::Value(&[0, 0, 0, 0, 0x07, 0x5b, 0xcd, 0x15]))
    )
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

    #[derive(Debug, thiserror::Error)]
    #[error("Value too big")]
    struct TooBigError;

    // Add a value that's too big, recover gracefully
    struct TooBigValue;
    impl SerializeValue for TooBigValue {
        fn serialize<'b>(
            &self,
            _typ: &ColumnType,
            writer: CellWriter<'b>,
        ) -> Result<WrittenCellProof<'b>, SerializationError> {
            // serialize some
            writer.into_value_builder().append_bytes(&[1u8]);

            // then throw an error
            Err(SerializationError::new(TooBigError))
        }
    }

    let err = values
        .add_value(&TooBigValue, &ColumnType::Native(NativeType::Ascii))
        .unwrap_err();

    assert_matches!(err.downcast_ref::<TooBigError>(), Some(TooBigError));

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
    let ctx = RowSerializationContext::from_specs(columns);
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
            .map(|(i, _)| col_owned(format!("col_{i}"), ColumnType::Native(NativeType::TinyInt)))
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
