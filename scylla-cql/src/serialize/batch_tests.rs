use crate::frame::response::result::NativeType;
use crate::frame::response::result::{ColumnSpec, ColumnType, TableSpec};
use crate::serialize::batch::{BatchValues, BatchValuesIterator};
use crate::serialize::row::RowSerializationContext;
use crate::serialize::RowWriter;

use assert_matches::assert_matches;
use bytes::BufMut;

use std::{borrow::Cow, convert::TryInto};

fn col<'a>(name: impl Into<Cow<'a, str>>, typ: ColumnType<'a>) -> ColumnSpec<'a> {
    ColumnSpec {
        name: name.into(),
        typ,
        table_spec: TableSpec::borrowed("ks", "tbl"),
    }
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
            col("a", ColumnType::Native(NativeType::TinyInt)),
            col("b", ColumnType::Native(NativeType::TinyInt)),
        ];
        let request = serialize_batch_value_iterator(&mut iter, cols);
        assert_eq!(request, vec![0, 2, 0, 0, 0, 1, 1, 0, 0, 0, 1, 2]);
    }

    {
        let cols = &[
            col("a", ColumnType::Native(NativeType::TinyInt)),
            col("b", ColumnType::Native(NativeType::TinyInt)),
            col("c", ColumnType::Native(NativeType::TinyInt)),
            col("d", ColumnType::Native(NativeType::TinyInt)),
        ];
        let request = serialize_batch_value_iterator(&mut iter, cols);
        assert_eq!(
            request,
            vec![0, 4, 0, 0, 0, 1, 2, 0, 0, 0, 1, 3, 0, 0, 0, 1, 4, 0, 0, 0, 1, 5]
        );
    }

    {
        let cols = &[col("a", ColumnType::Native(NativeType::TinyInt))];
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
            col("a", ColumnType::Native(NativeType::TinyInt)),
            col("b", ColumnType::Native(NativeType::TinyInt)),
        ];
        let request = serialize_batch_value_iterator(&mut iter, cols);
        assert_eq!(request, vec![0, 2, 0, 0, 0, 1, 1, 0, 0, 0, 1, 2]);
    }

    {
        let cols = &[
            col("a", ColumnType::Native(NativeType::TinyInt)),
            col("b", ColumnType::Native(NativeType::TinyInt)),
            col("c", ColumnType::Native(NativeType::TinyInt)),
            col("d", ColumnType::Native(NativeType::TinyInt)),
        ];
        let request = serialize_batch_value_iterator(&mut iter, cols);
        assert_eq!(
            request,
            vec![0, 4, 0, 0, 0, 1, 2, 0, 0, 0, 1, 3, 0, 0, 0, 1, 4, 0, 0, 0, 1, 5]
        );
    }

    {
        let cols = &[col("a", ColumnType::Native(NativeType::TinyInt))];
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
                col("a", ColumnType::Native(NativeType::Int)),
                col("b", ColumnType::Native(NativeType::Int)),
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
        col("a", ColumnType::Native(NativeType::TinyInt)),
        col("b", ColumnType::Native(NativeType::TinyInt)),
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
    let cols: &[&[ColumnSpec]] = &[&[col("a", ColumnType::Native(NativeType::Text))]];
    assert_has_batch_values::<&_>(&tuple, cols);
    let tuple2: ((&str, &str), (&str, &str)) = ((&s, &s), (&s, &s));
    let cols: &[&[ColumnSpec]] = &[
        &[
            col("a", ColumnType::Native(NativeType::Text)),
            col("b", ColumnType::Native(NativeType::Text)),
        ],
        &[
            col("a", ColumnType::Native(NativeType::Text)),
            col("b", ColumnType::Native(NativeType::Text)),
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

        let columns = &[col("a", ColumnType::Native(NativeType::Int))];
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
