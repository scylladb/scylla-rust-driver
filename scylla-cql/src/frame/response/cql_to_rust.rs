#![allow(deprecated)]

use super::result::{CqlValue, Row};
use crate::frame::value::{
    Counter, CqlDate, CqlDecimal, CqlDuration, CqlTime, CqlTimestamp, CqlTimeuuid, CqlVarint,
};
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::hash::{BuildHasher, Hash};
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

/// This trait defines a way to convert CqlValue or `Option<CqlValue>` into some rust type
// We can't use From trait because impl From<Option<CqlValue>> for String {...}
// is forbidden since neither From nor String are defined in this crate
#[deprecated(
    since = "0.15.0",
    note = "Legacy deserialization API is inefficient and is going to be removed soon"
)]
pub trait FromCqlVal<T>: Sized {
    fn from_cql(cql_val: T) -> Result<Self, FromCqlValError>;
}

#[derive(Error, Debug, Clone, PartialEq, Eq)]
pub enum FromCqlValError {
    #[error("Bad CQL type")]
    BadCqlType,
    #[error("Value is null")]
    ValIsNull,
    #[error("Bad Value")]
    BadVal,
}

/// This trait defines a way to convert CQL Row into some rust type
#[deprecated(
    since = "0.15.0",
    note = "Legacy deserialization API is inefficient and is going to be removed soon"
)]
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
impl_from_cql_value_from_method!(CqlVarint, into_cql_varint); // CqlVarint::from_cql<CqlValue>
impl_from_cql_value_from_method!(i8, as_tinyint); // i8::from_cql<CqlValue>
impl_from_cql_value_from_method!(f32, as_float); // f32::from_cql<CqlValue>
impl_from_cql_value_from_method!(f64, as_double); // f64::from_cql<CqlValue>
impl_from_cql_value_from_method!(bool, as_boolean); // bool::from_cql<CqlValue>
impl_from_cql_value_from_method!(String, into_string); // String::from_cql<CqlValue>
impl_from_cql_value_from_method!(Vec<u8>, into_blob); // Vec<u8>::from_cql<CqlValue>
impl_from_cql_value_from_method!(IpAddr, as_inet); // IpAddr::from_cql<CqlValue>
impl_from_cql_value_from_method!(Uuid, as_uuid); // Uuid::from_cql<CqlValue>
impl_from_cql_value_from_method!(CqlTimeuuid, as_timeuuid); // CqlTimeuuid::from_cql<CqlValue>
impl_from_cql_value_from_method!(CqlDecimal, into_cql_decimal); // CqlDecimal::from_cql<CqlValue>
impl_from_cql_value_from_method!(CqlDuration, as_cql_duration); // CqlDuration::from_cql<CqlValue>
impl_from_cql_value_from_method!(CqlDate, as_cql_date); // CqlDate::from_cql<CqlValue>
impl_from_cql_value_from_method!(CqlTime, as_cql_time); // CqlTime::from_cql<CqlValue>
impl_from_cql_value_from_method!(CqlTimestamp, as_cql_timestamp); // CqlTimestamp::from_cql<CqlValue>

impl<const N: usize> FromCqlVal<CqlValue> for [u8; N] {
    fn from_cql(cql_val: CqlValue) -> Result<Self, FromCqlValError> {
        let val = cql_val.into_blob().ok_or(FromCqlValError::BadCqlType)?;
        val.try_into().map_err(|_| FromCqlValError::BadVal)
    }
}

#[cfg(feature = "num-bigint-03")]
impl FromCqlVal<CqlValue> for num_bigint_03::BigInt {
    fn from_cql(cql_val: CqlValue) -> Result<Self, FromCqlValError> {
        match cql_val {
            CqlValue::Varint(cql_varint) => Ok(cql_varint.into()),
            _ => Err(FromCqlValError::BadCqlType),
        }
    }
}

#[cfg(feature = "num-bigint-04")]
impl FromCqlVal<CqlValue> for num_bigint_04::BigInt {
    fn from_cql(cql_val: CqlValue) -> Result<Self, FromCqlValError> {
        match cql_val {
            CqlValue::Varint(cql_varint) => Ok(cql_varint.into()),
            _ => Err(FromCqlValError::BadCqlType),
        }
    }
}

#[cfg(feature = "bigdecimal-04")]
impl FromCqlVal<CqlValue> for bigdecimal_04::BigDecimal {
    fn from_cql(cql_val: CqlValue) -> Result<Self, FromCqlValError> {
        match cql_val {
            CqlValue::Decimal(cql_decimal) => Ok(cql_decimal.into()),
            _ => Err(FromCqlValError::BadCqlType),
        }
    }
}

#[cfg(feature = "chrono-04")]
impl FromCqlVal<CqlValue> for chrono_04::NaiveDate {
    fn from_cql(cql_val: CqlValue) -> Result<Self, FromCqlValError> {
        match cql_val {
            CqlValue::Date(cql_date) => cql_date.try_into().map_err(|_| FromCqlValError::BadVal),
            _ => Err(FromCqlValError::BadCqlType),
        }
    }
}

#[cfg(feature = "time-03")]
impl FromCqlVal<CqlValue> for time_03::Date {
    fn from_cql(cql_val: CqlValue) -> Result<Self, FromCqlValError> {
        match cql_val {
            CqlValue::Date(cql_date) => cql_date.try_into().map_err(|_| FromCqlValError::BadVal),
            _ => Err(FromCqlValError::BadCqlType),
        }
    }
}

#[cfg(feature = "chrono-04")]
impl FromCqlVal<CqlValue> for chrono_04::NaiveTime {
    fn from_cql(cql_val: CqlValue) -> Result<Self, FromCqlValError> {
        match cql_val {
            CqlValue::Time(cql_time) => cql_time.try_into().map_err(|_| FromCqlValError::BadVal),
            _ => Err(FromCqlValError::BadCqlType),
        }
    }
}

#[cfg(feature = "time-03")]
impl FromCqlVal<CqlValue> for time_03::Time {
    fn from_cql(cql_val: CqlValue) -> Result<Self, FromCqlValError> {
        match cql_val {
            CqlValue::Time(cql_time) => cql_time.try_into().map_err(|_| FromCqlValError::BadVal),
            _ => Err(FromCqlValError::BadCqlType),
        }
    }
}

#[cfg(feature = "chrono-04")]
impl FromCqlVal<CqlValue> for chrono_04::DateTime<chrono_04::Utc> {
    fn from_cql(cql_val: CqlValue) -> Result<Self, FromCqlValError> {
        cql_val
            .as_cql_timestamp()
            .ok_or(FromCqlValError::BadCqlType)?
            .try_into()
            .map_err(|_| FromCqlValError::BadVal)
    }
}

#[cfg(feature = "time-03")]
impl FromCqlVal<CqlValue> for time_03::OffsetDateTime {
    fn from_cql(cql_val: CqlValue) -> Result<Self, FromCqlValError> {
        cql_val
            .as_cql_timestamp()
            .ok_or(FromCqlValError::BadCqlType)?
            .try_into()
            .map_err(|_| FromCqlValError::BadVal)
    }
}

#[cfg(feature = "secrecy-08")]
impl<V: FromCqlVal<CqlValue> + secrecy_08::Zeroize> FromCqlVal<CqlValue> for secrecy_08::Secret<V> {
    fn from_cql(cql_val: CqlValue) -> Result<Self, FromCqlValError> {
        Ok(secrecy_08::Secret::new(FromCqlVal::from_cql(cql_val)?))
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

impl<T1: FromCqlVal<CqlValue> + Eq + Hash, T2: FromCqlVal<CqlValue>, T3: BuildHasher + Default>
    FromCqlVal<CqlValue> for HashMap<T1, T2, T3>
{
    fn from_cql(cql_val: CqlValue) -> Result<Self, FromCqlValError> {
        let vec = cql_val.into_pair_vec().ok_or(FromCqlValError::BadCqlType)?;
        let mut res = HashMap::with_capacity_and_hasher(vec.len(), T3::default());
        for (key, value) in vec {
            res.insert(T1::from_cql(key)?, T2::from_cql(value)?);
        }
        Ok(res)
    }
}

impl<T: FromCqlVal<CqlValue> + Eq + Hash, S: BuildHasher + Default> FromCqlVal<CqlValue>
    for HashSet<T, S>
{
    fn from_cql(cql_val: CqlValue) -> Result<Self, FromCqlValError> {
        cql_val
            .into_vec()
            .ok_or(FromCqlValError::BadCqlType)?
            .into_iter()
            .map(T::from_cql)
            .collect::<Result<HashSet<T, S>, FromCqlValError>>()
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
    use crate::frame::value::{Counter, CqlDate, CqlDuration, CqlTime, CqlTimestamp, CqlTimeuuid};
    use crate::macros::FromRow;
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
    fn u8_array_from_cql() {
        let val = [1u8; 4];
        assert_eq!(Ok(val), <[u8; 4]>::from_cql(CqlValue::Blob(val.to_vec())));
    }

    #[test]
    fn ip_addr_from_cql() {
        let ip_addr = IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1));
        assert_eq!(Ok(ip_addr), IpAddr::from_cql(CqlValue::Inet(ip_addr)));
    }

    #[cfg(feature = "num-bigint-03")]
    #[test]
    fn varint03_from_cql() {
        use num_bigint_03::ToBigInt;

        let big_int = 0.to_bigint().unwrap();
        assert_eq!(
            Ok(big_int),
            num_bigint_03::BigInt::from_cql(CqlValue::Varint(0.to_bigint().unwrap().into()))
        );
    }

    #[cfg(feature = "num-bigint-04")]
    #[test]
    fn varint04_from_cql() {
        use num_bigint_04::ToBigInt;

        let big_int = 0.to_bigint().unwrap();
        assert_eq!(
            Ok(big_int),
            num_bigint_04::BigInt::from_cql(CqlValue::Varint(0.to_bigint().unwrap().into()))
        );
    }

    #[cfg(feature = "bigdecimal-04")]
    #[test]
    fn decimal_from_cql() {
        let decimal = bigdecimal_04::BigDecimal::from_str("123.4").unwrap();
        assert_eq!(
            Ok(decimal.clone()),
            bigdecimal_04::BigDecimal::from_cql(CqlValue::Decimal(decimal.try_into().unwrap()))
        );
    }

    #[test]
    fn counter_from_cql() {
        let counter = Counter(1);
        assert_eq!(Ok(counter), Counter::from_cql(CqlValue::Counter(counter)));
    }

    #[cfg(feature = "chrono-04")]
    #[test]
    fn naive_date_04_from_cql() {
        use chrono_04::NaiveDate;

        let unix_epoch: CqlValue = CqlValue::Date(CqlDate(2_u32.pow(31)));
        assert_eq!(
            Ok(NaiveDate::from_ymd_opt(1970, 1, 1).unwrap()),
            NaiveDate::from_cql(unix_epoch)
        );

        let before_epoch: CqlValue = CqlValue::Date(CqlDate(2_u32.pow(31) - 30));
        assert_eq!(
            Ok(NaiveDate::from_ymd_opt(1969, 12, 2).unwrap()),
            NaiveDate::from_cql(before_epoch)
        );

        let after_epoch: CqlValue = CqlValue::Date(CqlDate(2_u32.pow(31) + 30));
        assert_eq!(
            Ok(NaiveDate::from_ymd_opt(1970, 1, 31).unwrap()),
            NaiveDate::from_cql(after_epoch)
        );

        let min_date: CqlValue = CqlValue::Date(CqlDate(0));
        assert!(NaiveDate::from_cql(min_date).is_err());

        let max_date: CqlValue = CqlValue::Date(CqlDate(u32::MAX));
        assert!(NaiveDate::from_cql(max_date).is_err());
    }

    #[test]
    fn cql_date_from_cql() {
        let unix_epoch: CqlValue = CqlValue::Date(CqlDate(2_u32.pow(31)));
        assert_eq!(Ok(CqlDate(2_u32.pow(31))), CqlDate::from_cql(unix_epoch));

        let min_date: CqlValue = CqlValue::Date(CqlDate(0));
        assert_eq!(Ok(CqlDate(0)), CqlDate::from_cql(min_date));

        let max_date: CqlValue = CqlValue::Date(CqlDate(u32::MAX));
        assert_eq!(Ok(CqlDate(u32::MAX)), CqlDate::from_cql(max_date));
    }

    #[cfg(feature = "time-03")]
    #[test]
    fn date_03_from_cql() {
        // UNIX epoch
        let unix_epoch = CqlValue::Date(CqlDate(1 << 31));
        assert_eq!(
            Ok(time_03::Date::from_ordinal_date(1970, 1).unwrap()),
            time_03::Date::from_cql(unix_epoch)
        );

        // 7 days after UNIX epoch
        let after_epoch = CqlValue::Date(CqlDate((1 << 31) + 7));
        assert_eq!(
            Ok(time_03::Date::from_ordinal_date(1970, 8).unwrap()),
            time_03::Date::from_cql(after_epoch)
        );

        // 3 days before UNIX epoch
        let before_epoch = CqlValue::Date(CqlDate((1 << 31) - 3));
        assert_eq!(
            Ok(time_03::Date::from_ordinal_date(1969, 363).unwrap()),
            time_03::Date::from_cql(before_epoch)
        );

        // Min possible stored date. Since value is out of `time_03::Date` range, it should return `BadVal` error
        let min_date = CqlValue::Date(CqlDate(u32::MIN));
        assert_eq!(
            Err(FromCqlValError::BadVal),
            time_03::Date::from_cql(min_date)
        );

        // Max possible stored date. Since value is out of `time_03::Date` range, it should return `BadVal` error
        let max_date = CqlValue::Date(CqlDate(u32::MAX));
        assert_eq!(
            Err(FromCqlValError::BadVal),
            time_03::Date::from_cql(max_date)
        );

        // Different CQL type. Since value can't be cast, it should return `BadCqlType` error
        let bad_type = CqlValue::Double(0.5);
        assert_eq!(
            Err(FromCqlValError::BadCqlType),
            time_03::Date::from_cql(bad_type)
        );
    }

    #[test]
    fn cql_duration_from_cql() {
        let cql_duration = CqlDuration {
            months: 3,
            days: 2,
            nanoseconds: 1,
        };
        assert_eq!(
            cql_duration,
            CqlDuration::from_cql(CqlValue::Duration(cql_duration)).unwrap(),
        );
    }

    #[test]
    fn cql_time_from_cql() {
        let time_ns = 86399999999999;
        let cql_value = CqlValue::Time(CqlTime(time_ns));
        assert_eq!(time_ns, CqlTime::from_cql(cql_value).unwrap().0);
    }

    #[cfg(feature = "chrono-04")]
    #[test]
    fn naive_time_04_from_cql() {
        use chrono_04::NaiveTime;

        // Midnight
        let midnight = CqlValue::Time(CqlTime(0));
        assert_eq!(Ok(NaiveTime::MIN), NaiveTime::from_cql(midnight));

        // 7:15:21.123456789
        let morning = CqlValue::Time(CqlTime(
            (7 * 3600 + 15 * 60 + 21) * 1_000_000_000 + 123_456_789,
        ));
        assert_eq!(
            Ok(NaiveTime::from_hms_nano_opt(7, 15, 21, 123_456_789).unwrap()),
            NaiveTime::from_cql(morning)
        );

        // 23:59:59.999999999
        let late_night = CqlValue::Time(CqlTime(
            (23 * 3600 + 59 * 60 + 59) * 1_000_000_000 + 999_999_999,
        ));
        assert_eq!(
            Ok(NaiveTime::from_hms_nano_opt(23, 59, 59, 999_999_999).unwrap()),
            NaiveTime::from_cql(late_night)
        );

        // Bad values. Since value is out of `chrono_04::NaiveTime` range, it should return `BadVal` error
        let bad_time1 = CqlValue::Time(CqlTime(-1));
        assert_eq!(Err(FromCqlValError::BadVal), NaiveTime::from_cql(bad_time1));
        let bad_time2 = CqlValue::Time(CqlTime(i64::MAX));
        assert_eq!(Err(FromCqlValError::BadVal), NaiveTime::from_cql(bad_time2));

        // Different CQL type. Since value can't be cast, it should return `BadCqlType` error
        let bad_type = CqlValue::Double(0.5);
        assert_eq!(
            Err(FromCqlValError::BadCqlType),
            NaiveTime::from_cql(bad_type)
        );
    }

    #[cfg(feature = "time-03")]
    #[test]
    fn time_03_from_cql() {
        // Midnight
        let midnight = CqlValue::Time(CqlTime(0));
        assert_eq!(
            Ok(time_03::Time::MIDNIGHT),
            time_03::Time::from_cql(midnight)
        );

        // 7:15:21.123456789
        let morning = CqlValue::Time(CqlTime(
            (7 * 3600 + 15 * 60 + 21) * 1_000_000_000 + 123_456_789,
        ));
        assert_eq!(
            Ok(time_03::Time::from_hms_nano(7, 15, 21, 123_456_789).unwrap()),
            time_03::Time::from_cql(morning)
        );

        // 23:59:59.999999999
        let late_night = CqlValue::Time(CqlTime(
            (23 * 3600 + 59 * 60 + 59) * 1_000_000_000 + 999_999_999,
        ));
        assert_eq!(
            Ok(time_03::Time::from_hms_nano(23, 59, 59, 999_999_999).unwrap()),
            time_03::Time::from_cql(late_night)
        );

        // Bad values. Since value is out of `time_03::Time` range, it should return `BadVal` error
        let bad_time1 = CqlValue::Time(CqlTime(-1));
        assert_eq!(
            Err(FromCqlValError::BadVal),
            time_03::Time::from_cql(bad_time1)
        );
        let bad_time2 = CqlValue::Time(CqlTime(i64::MAX));
        assert_eq!(
            Err(FromCqlValError::BadVal),
            time_03::Time::from_cql(bad_time2)
        );

        // Different CQL type. Since value can't be cast, it should return `BadCqlType` error
        let bad_type = CqlValue::Double(0.5);
        assert_eq!(
            Err(FromCqlValError::BadCqlType),
            time_03::Time::from_cql(bad_type)
        );
    }

    #[test]
    fn cql_timestamp_from_cql() {
        let timestamp_ms = 86399999999999;
        assert_eq!(
            timestamp_ms,
            CqlTimestamp::from_cql(CqlValue::Timestamp(CqlTimestamp(timestamp_ms)))
                .unwrap()
                .0,
        );
    }

    #[cfg(feature = "chrono-04")]
    #[test]
    fn datetime_04_from_cql() {
        use chrono_04::{DateTime, NaiveDate, Utc};
        let naivedatetime_utc = NaiveDate::from_ymd_opt(2022, 12, 31)
            .unwrap()
            .and_hms_opt(2, 0, 0)
            .unwrap();
        let datetime_utc = DateTime::<Utc>::from_naive_utc_and_offset(naivedatetime_utc, Utc);

        assert_eq!(
            datetime_utc,
            DateTime::<Utc>::from_cql(CqlValue::Timestamp(CqlTimestamp(
                datetime_utc.timestamp_millis()
            )))
            .unwrap()
        );
    }

    #[cfg(feature = "time-03")]
    #[test]
    fn offset_datetime_03_from_cql() {
        // UNIX epoch
        let unix_epoch = CqlValue::Timestamp(CqlTimestamp(0));
        assert_eq!(
            Ok(time_03::OffsetDateTime::UNIX_EPOCH),
            time_03::OffsetDateTime::from_cql(unix_epoch)
        );

        // 1 day 2 hours 3 minutes 4 seconds and 5 nanoseconds before UNIX epoch
        let before_epoch = CqlValue::Timestamp(CqlTimestamp(-(26 * 3600 + 3 * 60 + 4) * 1000 - 5));
        assert_eq!(
            Ok(time_03::OffsetDateTime::UNIX_EPOCH
                - time_03::Duration::new(26 * 3600 + 3 * 60 + 4, 5 * 1_000_000)),
            time_03::OffsetDateTime::from_cql(before_epoch)
        );

        // 6 days 7 hours 8 minutes 9 seconds and 10 nanoseconds after UNIX epoch
        let after_epoch =
            CqlValue::Timestamp(CqlTimestamp(((6 * 24 + 7) * 3600 + 8 * 60 + 9) * 1000 + 10));
        assert_eq!(
            Ok(time_03::PrimitiveDateTime::new(
                time_03::Date::from_ordinal_date(1970, 7).unwrap(),
                time_03::Time::from_hms_milli(7, 8, 9, 10).unwrap()
            )
            .assume_utc()),
            time_03::OffsetDateTime::from_cql(after_epoch)
        );

        // Min possible stored timestamp. Since value is out of `time_03::OffsetDateTime` range, it should return `BadVal` error
        let min_timestamp = CqlValue::Timestamp(CqlTimestamp(i64::MIN));
        assert_eq!(
            Err(FromCqlValError::BadVal),
            time_03::OffsetDateTime::from_cql(min_timestamp)
        );

        // Max possible stored timestamp. Since value is out of `time_03::OffsetDateTime` range, it should return `BadVal` error
        let max_timestamp = CqlValue::Timestamp(CqlTimestamp(i64::MAX));
        assert_eq!(
            Err(FromCqlValError::BadVal),
            time_03::OffsetDateTime::from_cql(max_timestamp)
        );

        // Different CQL type. Since value can't be cast, it should return `BadCqlType` error
        let bad_type = CqlValue::Double(0.5);
        assert_eq!(
            Err(FromCqlValError::BadCqlType),
            time_03::OffsetDateTime::from_cql(bad_type)
        );
    }

    #[test]
    fn uuid_from_cql() {
        let uuid_str = "8e14e760-7fa8-11eb-bc66-000000000001";
        let test_uuid: Uuid = Uuid::parse_str(uuid_str).unwrap();
        let test_time_uuid = CqlTimeuuid::from_str(uuid_str).unwrap();

        assert_eq!(
            test_uuid,
            Uuid::from_cql(CqlValue::Uuid(test_uuid)).unwrap()
        );

        assert_eq!(
            test_time_uuid,
            CqlTimeuuid::from_cql(CqlValue::Timeuuid(test_time_uuid)).unwrap()
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

    // Enabling `expect_used` clippy lint,
    // validates that `derive(FromRow)` macro definition does do not violates such rule under the hood.
    // Could be removed after such rule will be applied for the whole crate.
    // <https://rust-lang.github.io/rust-clippy/master/index.html#/expect_used>
    #[deny(clippy::expect_used)]
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

    // Enabling `expect_used` clippy lint,
    // validates that `derive(FromRow)` macro definition does do not violates such rule under the hood.
    // Could be removed after such rule will be applied for the whole crate.
    // <https://rust-lang.github.io/rust-clippy/master/index.html#/expect_used>
    #[deny(clippy::expect_used)]
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

    // Enabling `expect_used` clippy lint,
    // validates that `derive(FromRow)` macro definition does do not violates such rule under the hood.
    // Could be removed after such rule will be applied for the whole crate.
    // <https://rust-lang.github.io/rust-clippy/master/index.html#/expect_used>
    #[deny(clippy::expect_used)]
    #[test]
    fn unnamed_struct_from_row() {
        #[derive(FromRow)]
        struct MyRow(i32, Option<String>, Option<Vec<i32>>);

        let row = Row {
            columns: vec![
                Some(CqlValue::Int(16)),
                None,
                Some(CqlValue::Set(vec![CqlValue::Int(1), CqlValue::Int(2)])),
            ],
        };

        let my_row: MyRow = MyRow::from_row(row).unwrap();

        assert_eq!(my_row.0, 16);
        assert_eq!(my_row.1, None);
        assert_eq!(my_row.2, Some(vec![1, 2]));
    }
}
