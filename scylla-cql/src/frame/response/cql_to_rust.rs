use super::result::{CqlValue, Row};
use crate::frame::value::Counter;
use bigdecimal::BigDecimal;
use chrono::{Duration, NaiveDate};
use num_bigint::BigInt;
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::hash::Hash;
use std::net::IpAddr;
use thiserror::Error;
use uuid::Uuid;

#[derive(Error, Debug, Clone, PartialEq, Eq)]
pub enum FromRowError {
    #[error("{err} in the column with index {column}")]
    BadCqlVal { err: FromCqlValError, column: usize },
    #[error("Wrong row size: expected {expected}, actual {actual}")]
    WrongRowSize { expected: usize, actual: usize },
}

#[derive(Error, Debug, PartialEq, Eq)]
pub enum CqlTypeError {
    #[error("Invalid number of set elements: {0}")]
    InvalidNumberOfElements(i32),
}

/// This trait defines a way to convert CqlValue or Option<CqlValue> into some rust type
// We can't use From trait because impl From<Option<CqlValue>> for String {...}
// is forbidden since neither From nor String are defined in this crate
pub trait FromCqlVal<T>: Sized {
    fn from_cql(cql_val: T) -> Result<Self, FromCqlValError>;
}

#[derive(Error, Debug, Clone, PartialEq, Eq)]
pub enum FromCqlValError {
    #[error("Bad CQL type")]
    BadCqlType,
    #[error("Value is null")]
    ValIsNull,
}

/// This trait defines a way to convert CQL Row into some rust type
pub trait FromRow: Sized {
    fn from_row(row: Row) -> Result<Self, FromRowError>;
}

// CqlValue can be converted to CqlValue
impl FromCqlVal<CqlValue> for CqlValue {
    fn from_cql(cql_val: CqlValue) -> Result<CqlValue, FromCqlValError> {
        Ok(cql_val)
    }
}

// Implement from_cql<Option<CqlValue>> for every type that has from_cql<CqlValue>
// This tries to unwrap the option or fails with an error
impl<T: FromCqlVal<CqlValue>> FromCqlVal<Option<CqlValue>> for T {
    fn from_cql(cql_val_opt: Option<CqlValue>) -> Result<Self, FromCqlValError> {
        T::from_cql(cql_val_opt.ok_or(FromCqlValError::ValIsNull)?)
    }
}

// Implement from_cql<Option<CqlValue>> for Option<T> for every type that has from_cql<CqlValue>
// Value inside Option gets mapped from CqlValue to T
impl<T: FromCqlVal<CqlValue>> FromCqlVal<Option<CqlValue>> for Option<T> {
    fn from_cql(cql_val_opt: Option<CqlValue>) -> Result<Self, FromCqlValError> {
        match cql_val_opt {
            Some(CqlValue::Empty) => Ok(None),
            Some(cql_val) => Ok(Some(T::from_cql(cql_val)?)),
            None => Ok(None),
        }
    }
}
/// This macro implements FromCqlVal given a type and method of CqlValue that returns this type.
///
/// It can be useful in client code in case you have an extension trait for CqlValue
/// and you would like to convert one of its methods into a FromCqlVal impl.
/// The conversion method must return an `Option<T>`. `None` values will be
/// converted to `CqlValue::BadCqlType`.
///
/// # Example
/// ```
/// # use scylla_cql::frame::response::result::CqlValue;
/// # use scylla_cql::impl_from_cql_value_from_method;
/// struct MyBytes(Vec<u8>);
///
/// trait CqlValueExt {
///     fn into_my_bytes(self) -> Option<MyBytes>;
/// }
///
/// impl CqlValueExt for CqlValue {
///     fn into_my_bytes(self) -> Option<MyBytes> {
///         Some(MyBytes(self.into_blob()?))
///     }
/// }
///
/// impl_from_cql_value_from_method!(MyBytes, into_my_bytes);
/// ```
#[macro_export]
macro_rules! impl_from_cql_value_from_method {
    ($T:ty, $convert_func:ident) => {
        impl
            $crate::frame::response::cql_to_rust::FromCqlVal<
                $crate::frame::response::result::CqlValue,
            > for $T
        {
            fn from_cql(
                cql_val: $crate::frame::response::result::CqlValue,
            ) -> std::result::Result<$T, $crate::frame::response::cql_to_rust::FromCqlValError>
            {
                cql_val
                    .$convert_func()
                    .ok_or($crate::frame::response::cql_to_rust::FromCqlValError::BadCqlType)
            }
        }
    };
}

impl_from_cql_value_from_method!(i32, as_int); // i32::from_cql<CqlValue>
impl_from_cql_value_from_method!(i64, as_bigint); // i64::from_cql<CqlValue>
impl_from_cql_value_from_method!(Counter, as_counter); // Counter::from_cql<CqlValue>
impl_from_cql_value_from_method!(i16, as_smallint); // i16::from_cql<CqlValue>
impl_from_cql_value_from_method!(BigInt, into_varint); // BigInt::from_cql<CqlValue>
impl_from_cql_value_from_method!(i8, as_tinyint); // i8::from_cql<CqlValue>
impl_from_cql_value_from_method!(NaiveDate, as_date); // NaiveDate::from_cql<CqlValue>
impl_from_cql_value_from_method!(f32, as_float); // f32::from_cql<CqlValue>
impl_from_cql_value_from_method!(f64, as_double); // f64::from_cql<CqlValue>
impl_from_cql_value_from_method!(bool, as_boolean); // bool::from_cql<CqlValue>
impl_from_cql_value_from_method!(String, into_string); // String::from_cql<CqlValue>
impl_from_cql_value_from_method!(Vec<u8>, into_blob); // Vec<u8>::from_cql<CqlValue>
impl_from_cql_value_from_method!(IpAddr, as_inet); // IpAddr::from_cql<CqlValue>
impl_from_cql_value_from_method!(Uuid, as_uuid); // Uuid::from_cql<CqlValue>
impl_from_cql_value_from_method!(BigDecimal, into_decimal); // BigDecimal::from_cql<CqlValue>
impl_from_cql_value_from_method!(Duration, as_duration); // Duration::from_cql<CqlValue>

impl FromCqlVal<CqlValue> for crate::frame::value::Time {
    fn from_cql(cql_val: CqlValue) -> Result<Self, FromCqlValError> {
        match cql_val {
            CqlValue::Time(d) => Ok(Self(d)),
            _ => Err(FromCqlValError::BadCqlType),
        }
    }
}

impl FromCqlVal<CqlValue> for crate::frame::value::Timestamp {
    fn from_cql(cql_val: CqlValue) -> Result<Self, FromCqlValError> {
        match cql_val {
            CqlValue::Timestamp(d) => Ok(Self(d)),
            _ => Err(FromCqlValError::BadCqlType),
        }
    }
}

// Vec<T>::from_cql<CqlValue>
impl<T: FromCqlVal<CqlValue>> FromCqlVal<CqlValue> for Vec<T> {
    fn from_cql(cql_val: CqlValue) -> Result<Self, FromCqlValError> {
        cql_val
            .into_vec()
            .ok_or(FromCqlValError::BadCqlType)?
            .into_iter()
            .map(T::from_cql)
            .collect::<Result<Vec<T>, FromCqlValError>>()
    }
}

impl<T1: FromCqlVal<CqlValue> + Eq + Hash, T2: FromCqlVal<CqlValue>> FromCqlVal<CqlValue>
    for HashMap<T1, T2>
{
    fn from_cql(cql_val: CqlValue) -> Result<Self, FromCqlValError> {
        let vec = cql_val.into_pair_vec().ok_or(FromCqlValError::BadCqlType)?;
        let mut res = HashMap::with_capacity(vec.len());
        for (key, value) in vec {
            res.insert(T1::from_cql(key)?, T2::from_cql(value)?);
        }
        Ok(res)
    }
}

impl<T: FromCqlVal<CqlValue> + Eq + Hash> FromCqlVal<CqlValue> for HashSet<T> {
    fn from_cql(cql_val: CqlValue) -> Result<Self, FromCqlValError> {
        cql_val
            .into_vec()
            .ok_or(FromCqlValError::BadCqlType)?
            .into_iter()
            .map(T::from_cql)
            .collect::<Result<HashSet<T>, FromCqlValError>>()
    }
}

impl<T: FromCqlVal<CqlValue> + Ord> FromCqlVal<CqlValue> for BTreeSet<T> {
    fn from_cql(cql_val: CqlValue) -> Result<Self, FromCqlValError> {
        cql_val
            .into_vec()
            .ok_or(FromCqlValError::BadCqlType)?
            .into_iter()
            .map(T::from_cql)
            .collect::<Result<BTreeSet<T>, FromCqlValError>>()
    }
}

impl<K: FromCqlVal<CqlValue> + Ord, V: FromCqlVal<CqlValue>> FromCqlVal<CqlValue>
    for BTreeMap<K, V>
{
    fn from_cql(cql_val: CqlValue) -> Result<Self, FromCqlValError> {
        let vec = cql_val.into_pair_vec().ok_or(FromCqlValError::BadCqlType)?;
        let mut res = BTreeMap::new();
        for (key, value) in vec {
            res.insert(K::from_cql(key)?, V::from_cql(value)?);
        }
        Ok(res)
    }
}

macro_rules! replace_expr {
    ($_t:tt $sub:expr) => {
        $sub
    };
}

// This macro implements FromRow for tuple of types that have FromCqlVal
macro_rules! impl_tuple_from_row {
    ( $($Ti:tt),+ ) => {
        impl<$($Ti),+> FromRow for ($($Ti,)+)
        where
            $($Ti: FromCqlVal<Option<CqlValue>>),+
        {
            fn from_row(row: Row) -> Result<Self, FromRowError> {
                // From what I know, it is not possible yet to get the number of metavariable
                // repetitions (https://github.com/rust-lang/lang-team/issues/28#issue-644523674)
                // This is a workaround
                let expected_len = <[()]>::len(&[$(replace_expr!(($Ti) ())),*]);

                if expected_len != row.columns.len() {
                    return Err(FromRowError::WrongRowSize {
                        expected: expected_len,
                        actual: row.columns.len(),
                    });
                }
                let mut vals_iter = row.columns.into_iter().enumerate();

                Ok((
                    $(
                        {
                            let (col_ix, col_value) = vals_iter
                                .next()
                                .unwrap(); // vals_iter size is checked before this code is reached,
                                           // so it is safe to unwrap


                            $Ti::from_cql(col_value)
                                .map_err(|e| FromRowError::BadCqlVal {
                                    err: e,
                                    column: col_ix,
                                })?
                        }
                    ,)+
                ))
            }
        }
    }
}

// Implement FromRow for tuples of size up to 16
impl_tuple_from_row!(T1);
impl_tuple_from_row!(T1, T2);
impl_tuple_from_row!(T1, T2, T3);
impl_tuple_from_row!(T1, T2, T3, T4);
impl_tuple_from_row!(T1, T2, T3, T4, T5);
impl_tuple_from_row!(T1, T2, T3, T4, T5, T6);
impl_tuple_from_row!(T1, T2, T3, T4, T5, T6, T7);
impl_tuple_from_row!(T1, T2, T3, T4, T5, T6, T7, T8);
impl_tuple_from_row!(T1, T2, T3, T4, T5, T6, T7, T8, T9);
impl_tuple_from_row!(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10);
impl_tuple_from_row!(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11);
impl_tuple_from_row!(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12);
impl_tuple_from_row!(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13);
impl_tuple_from_row!(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14);
impl_tuple_from_row!(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15);
impl_tuple_from_row!(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16);

macro_rules! impl_tuple_from_cql {
    ( $($Ti:tt),+ ) => {
        impl<$($Ti),+> FromCqlVal<CqlValue> for ($($Ti,)+)
        where
            $($Ti: FromCqlVal<Option<CqlValue>>),+
        {
            fn from_cql(cql_val: CqlValue) -> Result<Self, FromCqlValError> {
                let tuple_fields = match cql_val {
                    CqlValue::Tuple(fields) => fields,
                    _ => return Err(FromCqlValError::BadCqlType)
                };

                let mut tuple_fields_iter = tuple_fields.into_iter();

                Ok((
                    $(
                        $Ti::from_cql(tuple_fields_iter.next().ok_or(FromCqlValError::BadCqlType) ?) ?
                    ,)+
                ))
            }
        }
    }
}

impl_tuple_from_cql!(T1);
impl_tuple_from_cql!(T1, T2);
impl_tuple_from_cql!(T1, T2, T3);
impl_tuple_from_cql!(T1, T2, T3, T4);
impl_tuple_from_cql!(T1, T2, T3, T4, T5);
impl_tuple_from_cql!(T1, T2, T3, T4, T5, T6);
impl_tuple_from_cql!(T1, T2, T3, T4, T5, T6, T7);
impl_tuple_from_cql!(T1, T2, T3, T4, T5, T6, T7, T8);
impl_tuple_from_cql!(T1, T2, T3, T4, T5, T6, T7, T8, T9);
impl_tuple_from_cql!(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10);
impl_tuple_from_cql!(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11);
impl_tuple_from_cql!(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12);
impl_tuple_from_cql!(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13);
impl_tuple_from_cql!(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14);
impl_tuple_from_cql!(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15);
impl_tuple_from_cql!(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16);

#[cfg(test)]
mod tests {
    use super::{CqlValue, FromCqlVal, FromCqlValError, FromRow, FromRowError, Row};
    use crate as scylla;
    use crate::frame::value::Counter;
    use crate::macros::FromRow;
    use bigdecimal::BigDecimal;
    use chrono::{Duration, NaiveDate};
    use num_bigint::{BigInt, ToBigInt};
    use std::collections::HashSet;
    use std::net::{IpAddr, Ipv4Addr};
    use std::str::FromStr;
    use uuid::Uuid;

    #[test]
    fn i32_from_cql() {
        assert_eq!(Ok(1234), i32::from_cql(CqlValue::Int(1234)));
    }

    #[test]
    fn bool_from_cql() {
        assert_eq!(Ok(true), bool::from_cql(CqlValue::Boolean(true)));
        assert_eq!(Ok(false), bool::from_cql(CqlValue::Boolean(false)));
    }

    #[test]
    fn floatingpoints_from_cql() {
        let float: f32 = 2.13;
        let double: f64 = 4.26;
        assert_eq!(Ok(float), f32::from_cql(CqlValue::Float(float)));
        assert_eq!(Ok(double), f64::from_cql(CqlValue::Double(double)));
    }

    #[test]
    fn i64_from_cql() {
        assert_eq!(Ok(1234), i64::from_cql(CqlValue::BigInt(1234)));
    }

    #[test]
    fn i8_from_cql() {
        assert_eq!(Ok(6), i8::from_cql(CqlValue::TinyInt(6)));
    }

    #[test]
    fn i16_from_cql() {
        assert_eq!(Ok(16), i16::from_cql(CqlValue::SmallInt(16)));
    }

    #[test]
    fn string_from_cql() {
        assert_eq!(
            Ok("ascii_test".to_string()),
            String::from_cql(CqlValue::Ascii("ascii_test".to_string()))
        );
        assert_eq!(
            Ok("text_test".to_string()),
            String::from_cql(CqlValue::Text("text_test".to_string()))
        );
    }

    #[test]
    fn ip_addr_from_cql() {
        let ip_addr = IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1));
        assert_eq!(Ok(ip_addr), IpAddr::from_cql(CqlValue::Inet(ip_addr)));
    }

    #[test]
    fn varint_from_cql() {
        let big_int = 0.to_bigint().unwrap();
        assert_eq!(
            Ok(big_int),
            BigInt::from_cql(CqlValue::Varint(0.to_bigint().unwrap()))
        );
    }

    #[test]
    fn decimal_from_cql() {
        let decimal = BigDecimal::from_str("123.4").unwrap();
        assert_eq!(
            Ok(decimal.clone()),
            BigDecimal::from_cql(CqlValue::Decimal(decimal))
        );
    }

    #[test]
    fn counter_from_cql() {
        let counter = Counter(1);
        assert_eq!(Ok(counter), Counter::from_cql(CqlValue::Counter(counter)));
    }

    #[test]
    fn naive_date_from_cql() {
        let unix_epoch: CqlValue = CqlValue::Date(2_u32.pow(31));
        assert_eq!(
            Ok(NaiveDate::from_ymd_opt(1970, 1, 1).unwrap()),
            NaiveDate::from_cql(unix_epoch)
        );

        let before_epoch: CqlValue = CqlValue::Date(2_u32.pow(31) - 30);
        assert_eq!(
            Ok(NaiveDate::from_ymd_opt(1969, 12, 2).unwrap()),
            NaiveDate::from_cql(before_epoch)
        );

        let after_epoch: CqlValue = CqlValue::Date(2_u32.pow(31) + 30);
        assert_eq!(
            Ok(NaiveDate::from_ymd_opt(1970, 1, 31).unwrap()),
            NaiveDate::from_cql(after_epoch)
        );

        let min_date: CqlValue = CqlValue::Date(0);
        assert!(NaiveDate::from_cql(min_date).is_err());

        let max_date: CqlValue = CqlValue::Date(u32::MAX);
        assert!(NaiveDate::from_cql(max_date).is_err());
    }

    #[test]
    fn duration_from_cql() {
        let time_duration = Duration::nanoseconds(86399999999999);
        assert_eq!(
            time_duration,
            Duration::from_cql(CqlValue::Time(time_duration)).unwrap(),
        );

        let timestamp_duration = Duration::milliseconds(i64::MIN);
        assert_eq!(
            timestamp_duration,
            Duration::from_cql(CqlValue::Timestamp(timestamp_duration)).unwrap(),
        );

        let timestamp_i64 = 997;
        assert_eq!(
            timestamp_i64,
            i64::from_cql(CqlValue::Timestamp(Duration::milliseconds(timestamp_i64))).unwrap()
        )
    }

    #[test]
    fn time_from_cql() {
        use crate::frame::value::Time;
        let time_duration = Duration::nanoseconds(86399999999999);
        assert_eq!(
            time_duration,
            Time::from_cql(CqlValue::Time(time_duration)).unwrap().0,
        );
    }

    #[test]
    fn timestamp_from_cql() {
        use crate::frame::value::Timestamp;
        let timestamp_duration = Duration::milliseconds(86399999999999);
        assert_eq!(
            timestamp_duration,
            Timestamp::from_cql(CqlValue::Timestamp(timestamp_duration))
                .unwrap()
                .0,
        );
    }

    #[test]
    fn uuid_from_cql() {
        let test_uuid: Uuid = Uuid::parse_str("8e14e760-7fa8-11eb-bc66-000000000001").unwrap();

        assert_eq!(
            test_uuid,
            Uuid::from_cql(CqlValue::Uuid(test_uuid)).unwrap()
        );

        assert_eq!(
            test_uuid,
            Uuid::from_cql(CqlValue::Timeuuid(test_uuid)).unwrap()
        );
    }

    #[test]
    fn vec_from_cql() {
        let cql_val = CqlValue::Set(vec![CqlValue::Int(1), CqlValue::Int(2), CqlValue::Int(3)]);
        assert_eq!(Ok(vec![1, 2, 3]), Vec::<i32>::from_cql(cql_val));
    }

    #[test]
    fn set_from_cql() {
        let cql_val = CqlValue::Set(vec![
            CqlValue::Int(1),
            CqlValue::Int(2),
            CqlValue::Int(3),
            CqlValue::Int(1),
            CqlValue::Int(2),
            CqlValue::Int(3),
        ]);
        assert_eq!(
            Ok(vec![1, 2, 3]),
            HashSet::<i32>::from_cql(cql_val).map(|value| {
                let mut values = value.into_iter().collect::<Vec<_>>();
                values.sort_unstable();
                values
            })
        );
    }

    #[test]
    fn tuple_from_row() {
        let row = Row {
            columns: vec![
                Some(CqlValue::Int(1)),
                Some(CqlValue::Text("some_text".to_string())),
                None,
            ],
        };

        let (a, b, c) = <(i32, Option<String>, Option<i64>)>::from_row(row).unwrap();
        assert_eq!(a, 1);
        assert_eq!(b, Some("some_text".to_string()));
        assert_eq!(c, None);
    }

    #[test]
    fn from_cql_null() {
        assert_eq!(i32::from_cql(None), Err(FromCqlValError::ValIsNull));
    }

    #[test]
    fn from_cql_wrong_type() {
        assert_eq!(
            i32::from_cql(CqlValue::BigInt(1234)),
            Err(FromCqlValError::BadCqlType)
        );
    }

    #[test]
    fn from_cql_empty_value() {
        assert_eq!(
            i32::from_cql(CqlValue::Empty),
            Err(FromCqlValError::BadCqlType)
        );

        assert_eq!(<Option<i32>>::from_cql(Some(CqlValue::Empty)), Ok(None));
    }

    #[test]
    fn from_row_null() {
        let row = Row {
            columns: vec![None],
        };

        assert_eq!(
            <(i32,)>::from_row(row),
            Err(FromRowError::BadCqlVal {
                err: FromCqlValError::ValIsNull,
                column: 0
            })
        );
    }

    #[test]
    fn from_row_wrong_type() {
        let row = Row {
            columns: vec![Some(CqlValue::Int(1234))],
        };

        assert_eq!(
            <(String,)>::from_row(row),
            Err(FromRowError::BadCqlVal {
                err: FromCqlValError::BadCqlType,
                column: 0
            })
        );
    }

    #[test]
    fn from_row_too_large() {
        let row = Row {
            columns: vec![Some(CqlValue::Int(1234)), Some(CqlValue::Int(1234))],
        };

        assert_eq!(
            <(i32,)>::from_row(row),
            Err(FromRowError::WrongRowSize {
                expected: 1,
                actual: 2
            })
        );
    }

    #[test]
    fn from_row_too_short() {
        let row = Row {
            columns: vec![Some(CqlValue::Int(1234)), Some(CqlValue::Int(1234))],
        };

        assert_eq!(
            <(i32, i32, i32)>::from_row(row),
            Err(FromRowError::WrongRowSize {
                expected: 3,
                actual: 2
            })
        );
    }

    #[test]
    fn struct_from_row() {
        #[derive(FromRow)]
        struct MyRow {
            a: i32,
            b: Option<String>,
            c: Option<Vec<i32>>,
        }

        let row = Row {
            columns: vec![
                Some(CqlValue::Int(16)),
                None,
                Some(CqlValue::Set(vec![CqlValue::Int(1), CqlValue::Int(2)])),
            ],
        };

        let my_row: MyRow = MyRow::from_row(row).unwrap();

        assert_eq!(my_row.a, 16);
        assert_eq!(my_row.b, None);
        assert_eq!(my_row.c, Some(vec![1, 2]));
    }

    #[test]
    fn struct_from_row_wrong_size() {
        #[derive(FromRow, PartialEq, Eq, Debug)]
        struct MyRow {
            a: i32,
            b: Option<String>,
            c: Option<Vec<i32>>,
        }

        let too_short_row = Row {
            columns: vec![Some(CqlValue::Int(16)), None],
        };

        let too_large_row = Row {
            columns: vec![
                Some(CqlValue::Int(16)),
                None,
                Some(CqlValue::Set(vec![CqlValue::Int(1), CqlValue::Int(2)])),
                Some(CqlValue::Set(vec![CqlValue::Int(1), CqlValue::Int(2)])),
            ],
        };

        assert_eq!(
            MyRow::from_row(too_short_row),
            Err(FromRowError::WrongRowSize {
                expected: 3,
                actual: 2
            })
        );

        assert_eq!(
            MyRow::from_row(too_large_row),
            Err(FromRowError::WrongRowSize {
                expected: 3,
                actual: 4
            })
        );
    }
}
