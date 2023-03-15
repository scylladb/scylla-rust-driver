//! Provides types for dealing with row deserialization.

use super::value::DeserializeCql;
use super::FrameSlice;
use crate::frame::frame_errors::ParseError;
use crate::frame::response::result::CqlValue;
use crate::frame::response::result::{ColumnSpec, Row};

/// Represents a raw, unparsed column value.
#[non_exhaustive]
pub struct RawColumn<'frame> {
    pub spec: &'frame ColumnSpec,
    pub slice: Option<FrameSlice<'frame>>,
}

/// Iterates over columns of a single row.
#[derive(Clone, Debug)]
pub struct ColumnIterator<'frame> {
    specs: std::slice::Iter<'frame, ColumnSpec>,
    slice: FrameSlice<'frame>,
}

impl<'frame> ColumnIterator<'frame> {
    /// Creates a new iterator over a single row.
    ///
    /// - `specs` - information about columns of the serialized response,
    /// - `slice` - a `FrameSlice` which points to the serialized row.
    #[inline]
    pub fn new(specs: &'frame [ColumnSpec], slice: FrameSlice<'frame>) -> Self {
        Self {
            specs: specs.iter(),
            slice,
        }
    }

    /// Returns the remaining number of rows that this iterator is expected
    /// to return.
    #[inline]
    pub fn columns_remaining(&self) -> usize {
        self.specs.len()
    }
}

impl<'frame> Iterator for ColumnIterator<'frame> {
    type Item = Result<RawColumn<'frame>, ParseError>;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        let spec = self.specs.next()?;
        Some(
            self.slice
                .read_cql_bytes()
                .map(|slice| RawColumn { spec, slice }),
        )
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        (0, Some(self.specs.len()))
    }
}

/// A type that can be deserialized from a row that was returned from a query.
///
/// For tips on how to write a custom implementation of this trait, see the
/// documentation of the parent module.
///
/// The crate also provides a derive macro which allows to automatically
/// implement the trait for a custom type. For more details on what the macro
/// is capable of, see its documentation.
pub trait DeserializeRow<'frame>
where
    Self: Sized,
{
    /// Checks that the schema of the result matches what this type expects.
    ///
    /// This function can check whether column types and names match the
    /// expectations.
    fn type_check(specs: &[ColumnSpec]) -> Result<(), ParseError>;

    /// Deserializes a row from given column iterator.
    ///
    /// This function can assume that the driver called `type_check` to verify
    /// the row's type. Note that `deserialize` is not an unsafe function,
    /// so it should not use the assumption about `type_check` being called
    /// as an excuse to run `unsafe` code.
    fn deserialize(row: ColumnIterator<'frame>) -> Result<Self, ParseError>;
}

impl<'frame> DeserializeRow<'frame> for Row {
    #[inline]
    fn type_check(_specs: &[ColumnSpec]) -> Result<(), ParseError> {
        // CqlValues accept all types, no type checking needed
        Ok(())
    }

    #[inline]
    fn deserialize(mut row: ColumnIterator<'frame>) -> Result<Self, ParseError> {
        let mut columns = Vec::with_capacity(row.size_hint().0);
        while let Some(column) = row.next().transpose()? {
            columns.push(<Option<CqlValue>>::deserialize(
                &column.spec.typ,
                column.slice,
            )?);
        }
        Ok(Self { columns })
    }
}

impl<'frame> DeserializeRow<'frame> for ColumnIterator<'frame> {
    #[inline]
    fn type_check(_specs: &[ColumnSpec]) -> Result<(), ParseError> {
        Ok(())
    }

    #[inline]
    fn deserialize(row: ColumnIterator<'frame>) -> Result<Self, ParseError> {
        Ok(row)
    }
}

macro_rules! impl_tuple {
    ($($Ti:ident),*; $($idx:literal),*; $($idf:ident),*) => {
        impl<'frame, $($Ti),*> DeserializeRow<'frame> for ($($Ti,)*)
        where
            $($Ti: DeserializeCql<'frame>),*
        {
            fn type_check(specs: &[ColumnSpec]) -> Result<(), ParseError> {
                if let [$($idf),*] = &specs {
                    $(
                        <$Ti as DeserializeCql<'frame>>::type_check(&$idf.typ)?;
                    )*
                    return Ok(());
                }
                const TUPLE_LEN: usize = [0, $($idx),*].len() - 1;
                return Err(ParseError::BadIncomingData(format!(
                    "Expected {} columns, but got {:?}",
                    TUPLE_LEN, specs.len(),
                )));
            }

            fn deserialize(mut row: ColumnIterator<'frame>) -> Result<Self, ParseError> {
                const TUPLE_LEN: usize = [0, $($idx),*].len() - 1;
                let ret = (
                    $({
                        let column = row.next().ok_or_else(|| ParseError::BadIncomingData(
                            format!("Expected {} values, got {}", TUPLE_LEN, $idx)
                        ))??;
                        <$Ti as DeserializeCql<'frame>>::deserialize(&column.spec.typ, column.slice)?
                    },)*
                );
                if row.next().is_some() {
                    return Err(ParseError::BadIncomingData(
                        format!("Expected {} values, but got more", TUPLE_LEN)
                    ));
                }
                Ok(ret)
            }
        }
    }
}

macro_rules! impl_tuple_multiple {
    (;;) => {
        impl_tuple!(;;);
    };
    ($TN:ident $(,$Ti:ident)*; $idx_n:literal $(,$idx:literal)*; $idf_n:ident $(,$idf:ident)*) => {
        impl_tuple_multiple!($($Ti),*; $($idx),*; $($idf),*);
        impl_tuple!($TN $(,$Ti)*; $idx_n $(,$idx)*; $idf_n $(,$idf)*);
    }
}

impl_tuple_multiple!(
    T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15;
    0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15;
    t0, t1, t2, t3, t4, t5, t6, t7, t8, t9, t10, t11, t12, t13, t14, t15
);

#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use crate::frame::frame_errors::ParseError;
    use crate::frame::response::result::{ColumnSpec, ColumnType, TableSpec};
    use crate::types::deserialize::FrameSlice;

    use super::super::tests::serialize_cells;
    use super::{ColumnIterator, DeserializeRow};

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
        assert_eq!(col1.spec.name, "i1");
        assert_eq!(col1.spec.typ, ColumnType::Int);
        assert_eq!(col1.slice.unwrap().as_slice(), &123i32.to_be_bytes());

        let col2 = iter.next().unwrap().unwrap();
        assert_eq!(col2.spec.name, "i2");
        assert_eq!(col2.spec.typ, ColumnType::Text);
        assert_eq!(col2.slice.unwrap().as_slice(), "ScyllaDB".as_bytes());

        let col3 = iter.next().unwrap().unwrap();
        assert_eq!(col3.spec.name, "i3");
        assert_eq!(col3.spec.typ, ColumnType::Counter);
        assert!(col3.slice.is_none());

        assert!(iter.next().is_none());
    }

    fn val_int(i: i32) -> Option<Vec<u8>> {
        Some(i.to_be_bytes().to_vec())
    }

    fn val_str(s: &str) -> Option<Vec<u8>> {
        Some(s.as_bytes().to_vec())
    }

    fn spec(name: &str, typ: ColumnType) -> ColumnSpec {
        ColumnSpec {
            name: name.to_owned(),
            typ,
            table_spec: TableSpec {
                ks_name: "ks".to_owned(),
                table_name: "tbl".to_owned(),
            },
        }
    }

    fn deserialize<'frame, R>(
        specs: &'frame [ColumnSpec],
        byts: &'frame Bytes,
    ) -> Result<R, ParseError>
    where
        R: DeserializeRow<'frame>,
    {
        <R as DeserializeRow<'frame>>::type_check(specs)?;
        let slice = FrameSlice::new(byts);
        let iter = ColumnIterator::new(specs, slice);
        <R as DeserializeRow<'frame>>::deserialize(iter)
    }
}
