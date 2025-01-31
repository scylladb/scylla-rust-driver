use crate::frame::response::result::NativeType;
use crate::frame::types::RawValue;
use crate::serialize::batch::{BatchValues, BatchValuesIterator};
use crate::serialize::row::{RowSerializationContext, SerializeRow, SerializedValues};
use crate::serialize::value::tests::get_ser_err;
use crate::serialize::value::{mk_ser_err, BuiltinSerializationErrorKind, SerializeValue};
use crate::serialize::writers::WrittenCellProof;
use crate::serialize::{CellWriter, RowWriter, SerializationError};

use super::response::result::{ColumnSpec, ColumnType, TableSpec};
#[cfg(test)]
use assert_matches::assert_matches;
use bytes::BufMut;

use std::collections::BTreeMap;
use std::{borrow::Cow, convert::TryInto};

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
                BuiltinSerializationErrorKind::SizeOverflow,
            ))
        }
    }

    let err = values
        .add_value(&TooBigValue, &ColumnType::Native(NativeType::Ascii))
        .unwrap_err();

    assert_matches!(
        get_ser_err(&err).kind,
        BuiltinSerializationErrorKind::SizeOverflow
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
        col_spec("ala", ColumnType::Native(NativeType::Int)),
        col_spec("ma", ColumnType::Native(NativeType::Int)),
        col_spec("kota", ColumnType::Native(NativeType::Int)),
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
        col_spec("ala", ColumnType::Native(NativeType::Int)),
        col_spec("ma", ColumnType::Native(NativeType::Int)),
        col_spec("kota", ColumnType::Native(NativeType::Int)),
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

fn col_spec<'a>(name: impl Into<Cow<'a, str>>, typ: ColumnType<'a>) -> ColumnSpec<'a> {
    ColumnSpec {
        name: name.into(),
        typ,
        table_spec: TableSpec::borrowed("ks", "tbl"),
    }
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
            .map(|(i, _)| col_spec(format!("col_{i}"), ColumnType::Native(NativeType::TinyInt)))
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
        col_spec("ala", ColumnType::Native(NativeType::Int)),
        col_spec("ma", ColumnType::Native(NativeType::Int)),
        col_spec("kota", ColumnType::Native(NativeType::Int)),
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
        col_spec("col_1", ColumnType::Native(NativeType::Int)),
        col_spec("col_2", ColumnType::Native(NativeType::Int)),
        col_spec("col_3", ColumnType::Native(NativeType::Int)),
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

fn make_batch_value_iter<BV: BatchValues>(bv: &BV) -> BV::BatchValuesIter<'_> {
    <BV as BatchValues>::batch_values_iter(bv)
}

fn serialize_batch_value_iterator<'a>(
    bvi: &mut impl BatchValuesIterator<'a>,

    columns: &[ColumnSpec],
) -> Vec<u8> {
    fn serialize_bvi<'bv>(
        bvi: &mut impl BatchValuesIterator<'bv>,
        ctx: &RowSerializationContext,
    ) -> Vec<u8> {
        let mut data = vec![0, 0];
        let mut writer = RowWriter::new(&mut data);
        bvi.serialize_next(ctx, &mut writer).unwrap().unwrap();
        let value_count: u16 = writer.value_count().try_into().unwrap();
        data[0..2].copy_from_slice(&value_count.to_be_bytes());
        data
    }

    let ctx = RowSerializationContext { columns };
    serialize_bvi(bvi, &ctx)
}

#[test]
fn slice_batch_values() {
    let batch_values: &[&[i8]] = &[&[1, 2], &[2, 3, 4, 5], &[6]];

    let mut iter = make_batch_value_iter(&batch_values);
    {
        let cols = &[
            col_spec("a", ColumnType::Native(NativeType::TinyInt)),
            col_spec("b", ColumnType::Native(NativeType::TinyInt)),
        ];
        let request = serialize_batch_value_iterator(&mut iter, cols);
        assert_eq!(request, vec![0, 2, 0, 0, 0, 1, 1, 0, 0, 0, 1, 2]);
    }

    {
        let cols = &[
            col_spec("a", ColumnType::Native(NativeType::TinyInt)),
            col_spec("b", ColumnType::Native(NativeType::TinyInt)),
            col_spec("c", ColumnType::Native(NativeType::TinyInt)),
            col_spec("d", ColumnType::Native(NativeType::TinyInt)),
        ];
        let request = serialize_batch_value_iterator(&mut iter, cols);
        assert_eq!(
            request,
            vec![0, 4, 0, 0, 0, 1, 2, 0, 0, 0, 1, 3, 0, 0, 0, 1, 4, 0, 0, 0, 1, 5]
        );
    }

    {
        let cols = &[col_spec("a", ColumnType::Native(NativeType::TinyInt))];
        let request = serialize_batch_value_iterator(&mut iter, cols);
        assert_eq!(request, vec![0, 1, 0, 0, 0, 1, 6]);
    }

    assert_matches!(
        iter.serialize_next(
            &RowSerializationContext::empty(),
            &mut RowWriter::new(&mut Vec::new())
        ),
        None
    );

    let ctx = RowSerializationContext { columns: &[] };
    let mut data = Vec::new();
    let mut writer = RowWriter::new(&mut data);
    assert!(iter.serialize_next(&ctx, &mut writer).is_none());
}

#[test]
fn vec_batch_values() {
    let batch_values: Vec<Vec<i8>> = vec![vec![1, 2], vec![2, 3, 4, 5], vec![6]];

    let mut iter = make_batch_value_iter(&batch_values);
    {
        let cols = &[
            col_spec("a", ColumnType::Native(NativeType::TinyInt)),
            col_spec("b", ColumnType::Native(NativeType::TinyInt)),
        ];
        let request = serialize_batch_value_iterator(&mut iter, cols);
        assert_eq!(request, vec![0, 2, 0, 0, 0, 1, 1, 0, 0, 0, 1, 2]);
    }

    {
        let cols = &[
            col_spec("a", ColumnType::Native(NativeType::TinyInt)),
            col_spec("b", ColumnType::Native(NativeType::TinyInt)),
            col_spec("c", ColumnType::Native(NativeType::TinyInt)),
            col_spec("d", ColumnType::Native(NativeType::TinyInt)),
        ];
        let request = serialize_batch_value_iterator(&mut iter, cols);
        assert_eq!(
            request,
            vec![0, 4, 0, 0, 0, 1, 2, 0, 0, 0, 1, 3, 0, 0, 0, 1, 4, 0, 0, 0, 1, 5]
        );
    }

    {
        let cols = &[col_spec("a", ColumnType::Native(NativeType::TinyInt))];
        let request = serialize_batch_value_iterator(&mut iter, cols);
        assert_eq!(request, vec![0, 1, 0, 0, 0, 1, 6]);
    }
}

#[test]
fn tuple_batch_values() {
    fn check_twoi32_tuple(tuple: impl BatchValues, size: usize) {
        let mut iter = make_batch_value_iter(&tuple);

        for i in 0..size {
            let cols = &[
                col_spec("a", ColumnType::Native(NativeType::Int)),
                col_spec("b", ColumnType::Native(NativeType::Int)),
            ];

            let request = serialize_batch_value_iterator(&mut iter, cols);

            let mut expected: Vec<u8> = Vec::new();
            let i: i32 = i.try_into().unwrap();
            expected.put_i16(2);
            expected.put_i32(4);
            expected.put_i32(i + 1);
            expected.put_i32(4);
            expected.put_i32(2 * (i + 1));

            assert_eq!(request, expected);
        }
    }

    // rustfmt wants to have each tuple inside a tuple in a separate line
    // so we end up with 170 lines of tuples
    // FIXME: Is there some cargo fmt flag to fix this?

    check_twoi32_tuple(((1, 2),), 1);
    check_twoi32_tuple(((1, 2), (2, 4)), 2);
    check_twoi32_tuple(((1, 2), (2, 4), (3, 6)), 3);
    check_twoi32_tuple(((1, 2), (2, 4), (3, 6), (4, 8)), 4);
    check_twoi32_tuple(((1, 2), (2, 4), (3, 6), (4, 8), (5, 10)), 5);
    check_twoi32_tuple(((1, 2), (2, 4), (3, 6), (4, 8), (5, 10), (6, 12)), 6);
    check_twoi32_tuple(
        ((1, 2), (2, 4), (3, 6), (4, 8), (5, 10), (6, 12), (7, 14)),
        7,
    );
    check_twoi32_tuple(
        (
            (1, 2),
            (2, 4),
            (3, 6),
            (4, 8),
            (5, 10),
            (6, 12),
            (7, 14),
            (8, 16),
        ),
        8,
    );
    check_twoi32_tuple(
        (
            (1, 2),
            (2, 4),
            (3, 6),
            (4, 8),
            (5, 10),
            (6, 12),
            (7, 14),
            (8, 16),
            (9, 18),
        ),
        9,
    );
    check_twoi32_tuple(
        (
            (1, 2),
            (2, 4),
            (3, 6),
            (4, 8),
            (5, 10),
            (6, 12),
            (7, 14),
            (8, 16),
            (9, 18),
            (10, 20),
        ),
        10,
    );
    check_twoi32_tuple(
        (
            (1, 2),
            (2, 4),
            (3, 6),
            (4, 8),
            (5, 10),
            (6, 12),
            (7, 14),
            (8, 16),
            (9, 18),
            (10, 20),
            (11, 22),
        ),
        11,
    );
    check_twoi32_tuple(
        (
            (1, 2),
            (2, 4),
            (3, 6),
            (4, 8),
            (5, 10),
            (6, 12),
            (7, 14),
            (8, 16),
            (9, 18),
            (10, 20),
            (11, 22),
            (12, 24),
        ),
        12,
    );
    check_twoi32_tuple(
        (
            (1, 2),
            (2, 4),
            (3, 6),
            (4, 8),
            (5, 10),
            (6, 12),
            (7, 14),
            (8, 16),
            (9, 18),
            (10, 20),
            (11, 22),
            (12, 24),
            (13, 26),
        ),
        13,
    );
    check_twoi32_tuple(
        (
            (1, 2),
            (2, 4),
            (3, 6),
            (4, 8),
            (5, 10),
            (6, 12),
            (7, 14),
            (8, 16),
            (9, 18),
            (10, 20),
            (11, 22),
            (12, 24),
            (13, 26),
            (14, 28),
        ),
        14,
    );
    check_twoi32_tuple(
        (
            (1, 2),
            (2, 4),
            (3, 6),
            (4, 8),
            (5, 10),
            (6, 12),
            (7, 14),
            (8, 16),
            (9, 18),
            (10, 20),
            (11, 22),
            (12, 24),
            (13, 26),
            (14, 28),
            (15, 30),
        ),
        15,
    );
    check_twoi32_tuple(
        (
            (1, 2),
            (2, 4),
            (3, 6),
            (4, 8),
            (5, 10),
            (6, 12),
            (7, 14),
            (8, 16),
            (9, 18),
            (10, 20),
            (11, 22),
            (12, 24),
            (13, 26),
            (14, 28),
            (15, 30),
            (16, 32),
        ),
        16,
    );
}

#[test]
#[allow(clippy::needless_borrow)]
fn ref_batch_values() {
    let batch_values: &[&[i8]] = &[&[1, 2], &[2, 3, 4, 5], &[6]];
    let cols = &[
        col_spec("a", ColumnType::Native(NativeType::TinyInt)),
        col_spec("b", ColumnType::Native(NativeType::TinyInt)),
    ];

    return check_ref_bv::<&&&&&[&[i8]]>(&&&&batch_values, cols);
    fn check_ref_bv<B: BatchValues>(batch_values: B, cols: &[ColumnSpec]) {
        let mut iter = make_batch_value_iter(&batch_values);

        let request = serialize_batch_value_iterator(&mut iter, cols);
        assert_eq!(request, vec![0, 2, 0, 0, 0, 1, 1, 0, 0, 0, 1, 2]);
    }
}

#[test]
#[allow(clippy::needless_borrow)]
fn check_ref_tuple() {
    fn assert_has_batch_values<BV: BatchValues>(bv: BV, cols: &[&[ColumnSpec]]) {
        let mut iter = make_batch_value_iter(&bv);

        for cols in cols {
            serialize_batch_value_iterator(&mut iter, cols);
        }
    }
    let s = String::from("hello");
    let tuple: ((&str,),) = ((&s,),);
    let cols: &[&[ColumnSpec]] = &[&[col_spec("a", ColumnType::Native(NativeType::Text))]];
    assert_has_batch_values::<&_>(&tuple, cols);
    let tuple2: ((&str, &str), (&str, &str)) = ((&s, &s), (&s, &s));
    let cols: &[&[ColumnSpec]] = &[
        &[
            col_spec("a", ColumnType::Native(NativeType::Text)),
            col_spec("b", ColumnType::Native(NativeType::Text)),
        ],
        &[
            col_spec("a", ColumnType::Native(NativeType::Text)),
            col_spec("b", ColumnType::Native(NativeType::Text)),
        ],
    ];
    assert_has_batch_values::<&_>(&tuple2, cols);
}

#[test]
fn check_batch_values_iterator_is_not_lending() {
    // This is an interesting property if we want to improve the batch shard selection heuristic
    fn g(bv: impl BatchValues) {
        let mut it = bv.batch_values_iter();
        let mut it2 = bv.batch_values_iter();

        let columns = &[col_spec("a", ColumnType::Native(NativeType::Int))];
        let ctx = RowSerializationContext { columns };
        let mut data = Vec::new();
        let mut writer = RowWriter::new(&mut data);

        // Make sure we can hold all these at the same time
        let v = vec![
            it.serialize_next(&ctx, &mut writer).unwrap().unwrap(),
            it2.serialize_next(&ctx, &mut writer).unwrap().unwrap(),
            it.serialize_next(&ctx, &mut writer).unwrap().unwrap(),
            it2.serialize_next(&ctx, &mut writer).unwrap().unwrap(),
        ];
        let _ = v;
    }
    g(((10,), (11,)));
}
