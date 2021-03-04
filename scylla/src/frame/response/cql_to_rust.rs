use super::result::{CQLValue, Row};
use num_bigint::BigInt;
use std::collections::HashMap;
use std::hash::Hash;
use std::net::IpAddr;
use thiserror::Error;
use uuid::Uuid;

#[derive(Error, Debug, Clone, PartialEq, Eq)]
pub enum FromRowError {
    #[error("Bad CQL value")]
    BadCQLVal(#[from] FromCQLValError),
    #[error("Row too short")]
    RowTooShort,
}

#[derive(Error, Debug, PartialEq, Eq)]
pub enum CQLTypeError {
    #[error("Invalid number of set elements: {0}")]
    InvalidNumberOfElements(i32),
}

/// This trait defines a way to convert CQLValue or Option<CQLValue> into some rust type  
// We can't use From trait because impl From<Option<CQLValue>> for String {...}
// is forbidden since neither From nor String are defined in this crate
pub trait FromCQLVal<T>: Sized {
    fn from_cql(cql_val: T) -> Result<Self, FromCQLValError>;
}

#[derive(Error, Debug, Clone, PartialEq, Eq)]
pub enum FromCQLValError {
    #[error("Bad CQL type")]
    BadCQLType,
    #[error("Value is null")]
    ValIsNull,
}

/// This trait defines a way to convert CQL Row into some rust type
pub trait FromRow: Sized {
    fn from_row(row: Row) -> Result<Self, FromRowError>;
}

// Implement from_cql<Option<CQLValue>> for every type that has from_cql<CQLValue>
// This tries to unwrap the option or fails with an error
impl<T: FromCQLVal<CQLValue>> FromCQLVal<Option<CQLValue>> for T {
    fn from_cql(cql_val_opt: Option<CQLValue>) -> Result<Self, FromCQLValError> {
        T::from_cql(cql_val_opt.ok_or(FromCQLValError::ValIsNull)?)
    }
}

// Implement from_cql<Option<CQLValue>> for Option<T> for every type that has from_cql<CQLValue>
// Value inside Option gets mapped from CQLValue to T
impl<T: FromCQLVal<CQLValue>> FromCQLVal<Option<CQLValue>> for Option<T> {
    fn from_cql(cql_val_opt: Option<CQLValue>) -> Result<Self, FromCQLValError> {
        match cql_val_opt {
            Some(cql_val) => Ok(Some(T::from_cql(cql_val)?)),
            None => Ok(None),
        }
    }
}
// This macro implements FromCQLVal given a type and method of CQLValue that returns this type
macro_rules! impl_from_cql_val {
    ($T:ty, $convert_func:ident) => {
        impl FromCQLVal<CQLValue> for $T {
            fn from_cql(cql_val: CQLValue) -> Result<$T, FromCQLValError> {
                cql_val.$convert_func().ok_or(FromCQLValError::BadCQLType)
            }
        }
    };
}

impl_from_cql_val!(i32, as_int); // i32::from_cql<CQLValue>
impl_from_cql_val!(i64, as_bigint); // i64::from_cql<CQLValue>
impl_from_cql_val!(i16, as_smallint); // i16::from_cql<CQLValue>
impl_from_cql_val!(BigInt, as_varint); // BigInt::from_cql<CQLValue>
impl_from_cql_val!(i8, as_tinyint); // i8::from_cql<CQLValue>
impl_from_cql_val!(u32, as_date); // u32::from_cql<CQLValue>
impl_from_cql_val!(f32, as_float); // f32::from_cql<CQLValue>
impl_from_cql_val!(f64, as_double); // f64::from_cql<CQLValue>
impl_from_cql_val!(bool, as_boolean); // bool::from_cql<CQLValue>
impl_from_cql_val!(String, into_string); // String::from_cql<CQLValue>
impl_from_cql_val!(IpAddr, as_inet); // IpAddr::from_cql<CQLValue>
impl_from_cql_val!(Uuid, as_uuid); // Uuid::from_cql<CQLValue>

// Vec<T>::from_cql<CQLValue>
impl<T: FromCQLVal<CQLValue>> FromCQLVal<CQLValue> for Vec<T> {
    fn from_cql(cql_val: CQLValue) -> Result<Self, FromCQLValError> {
        cql_val
            .into_vec()
            .ok_or(FromCQLValError::BadCQLType)?
            .into_iter()
            .map(T::from_cql)
            .collect::<Result<Vec<T>, FromCQLValError>>()
    }
}

impl<T1: FromCQLVal<CQLValue> + Eq + Hash, T2: FromCQLVal<CQLValue>> FromCQLVal<CQLValue>
    for HashMap<T1, T2>
{
    fn from_cql(cql_val: CQLValue) -> Result<Self, FromCQLValError> {
        let vec = cql_val.into_pair_vec().ok_or(FromCQLValError::BadCQLType)?;
        let mut res = HashMap::with_capacity(vec.len());
        for (key, value) in vec {
            res.insert(T1::from_cql(key)?, T2::from_cql(value)?);
        }
        Ok(res)
    }
}

// This macro implements FromRow for tuple of types that have FromCQLVal
macro_rules! impl_tuple_from_row {
    ( $($Ti:tt),+ ) => {
        impl<$($Ti),+> FromRow for ($($Ti,)+)
        where
            $($Ti: FromCQLVal<Option<CQLValue>>),+
        {
            fn from_row(row: Row) -> Result<Self, FromRowError> {
                let mut vals_iter = row.columns.into_iter();

                Ok((
                    $(
                        $Ti::from_cql(vals_iter
                                      .next()
                                      .ok_or(FromRowError::RowTooShort) ?
                                     ) ?
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
        impl<$($Ti),+> FromCQLVal<CQLValue> for ($($Ti,)+)
        where
            $($Ti: FromCQLVal<CQLValue>),+
        {
            fn from_cql(cql_val: CQLValue) -> Result<Self, FromCQLValError> {
                let tuple_fields = match cql_val {
                    CQLValue::Tuple(fields) => fields,
                    _ => return Err(FromCQLValError::BadCQLType)
                };

                let mut tuple_fields_iter = tuple_fields.into_iter();

                Ok((
                    $(
                        $Ti::from_cql(tuple_fields_iter.next().ok_or(FromCQLValError::BadCQLType) ?) ?
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

/*impl_from_cql_val!(Uuid, as_uuid); // Uuid::from_cql<CQLValue>

*/
#[cfg(test)]
mod tests {
    use super::{CQLValue, FromCQLVal, FromCQLValError, FromRow, FromRowError, Row};
    use crate as scylla;
    use crate::macros::FromRow;
    use num_bigint::{BigInt, ToBigInt};
    use std::net::{IpAddr, Ipv4Addr};
    use uuid::Uuid;

    #[test]
    fn uuid_from_cql() {
        let my_uuid = Uuid::parse_str("936DA01F9ABD4d9d80C702AF85C822A8").unwrap();
        assert_eq!(Ok(my_uuid), Uuid::from_cql(CQLValue::Uuid(my_uuid)));
    }

    #[test]
    fn i32_from_cql() {
        assert_eq!(Ok(1234), i32::from_cql(CQLValue::Int(1234)));
    }

    #[test]
    fn bool_from_cql() {
        assert_eq!(Ok(true), bool::from_cql(CQLValue::Boolean(true)));
        assert_eq!(Ok(false), bool::from_cql(CQLValue::Boolean(false)));
    }

    #[test]
    fn floatingpoints_from_cql() {
        let float: f32 = 2.13;
        let double: f64 = 4.26;
        assert_eq!(Ok(float), f32::from_cql(CQLValue::Float(float)));
        assert_eq!(Ok(double), f64::from_cql(CQLValue::Double(double)));
    }

    #[test]
    fn i64_from_cql() {
        assert_eq!(Ok(1234), i64::from_cql(CQLValue::BigInt(1234)));
    }

    #[test]
    fn i8_from_cql() {
        assert_eq!(Ok(6), i8::from_cql(CQLValue::TinyInt(6)));
    }

    #[test]
    fn i16_from_cql() {
        assert_eq!(Ok(16), i16::from_cql(CQLValue::SmallInt(16)));
    }

    #[test]
    fn string_from_cql() {
        assert_eq!(
            Ok("ascii_test".to_string()),
            String::from_cql(CQLValue::Ascii("ascii_test".to_string()))
        );
        assert_eq!(
            Ok("text_test".to_string()),
            String::from_cql(CQLValue::Text("text_test".to_string()))
        );
    }

    #[test]
    fn ip_addr_from_cql() {
        let ip_addr = IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1));
        assert_eq!(Ok(ip_addr), IpAddr::from_cql(CQLValue::Inet(ip_addr)));
    }

    #[test]
    fn varint_from_cql() {
        let big_int = 0.to_bigint().unwrap();
        assert_eq!(
            Ok(big_int),
            BigInt::from_cql(CQLValue::Varint(0.to_bigint().unwrap()))
        );
    }

    #[test]
    fn vec_from_cql() {
        let cql_val = CQLValue::Set(vec![CQLValue::Int(1), CQLValue::Int(2), CQLValue::Int(3)]);
        assert_eq!(Ok(vec![1, 2, 3]), Vec::<i32>::from_cql(cql_val));
    }

    #[test]
    fn tuple_from_row() {
        let row = Row {
            columns: vec![
                Some(CQLValue::Int(1)),
                Some(CQLValue::Text("some_text".to_string())),
                None,
            ],
        };

        let (a, b, c) = <(i32, Option<String>, Option<i64>)>::from_row(row).unwrap();
        assert_eq!(a, 1);
        assert_eq!(b, Some("some_text".to_string()));
        assert_eq!(c, None);

        let row2 = Row {
            columns: vec![Some(CQLValue::Int(1)), Some(CQLValue::Int(2))],
        };

        let (d,) = <(i32,)>::from_row(row2).unwrap();
        assert_eq!(d, 1);
    }

    #[test]
    fn from_cql_null() {
        assert_eq!(i32::from_cql(None), Err(FromCQLValError::ValIsNull));
    }

    #[test]
    fn from_cql_wrong_type() {
        assert_eq!(
            i32::from_cql(CQLValue::BigInt(1234)),
            Err(FromCQLValError::BadCQLType)
        );
    }

    #[test]
    fn from_row_null() {
        let row = Row {
            columns: vec![None],
        };

        assert_eq!(
            <(i32,)>::from_row(row),
            Err(FromRowError::BadCQLVal(FromCQLValError::ValIsNull))
        );
    }

    #[test]
    fn from_row_wrong_type() {
        let row = Row {
            columns: vec![Some(CQLValue::Int(1234))],
        };

        assert_eq!(
            <(String,)>::from_row(row),
            Err(FromRowError::BadCQLVal(FromCQLValError::BadCQLType))
        );
    }

    #[test]
    fn from_row_too_short() {
        let row = Row {
            columns: vec![Some(CQLValue::Int(1234))],
        };

        assert_eq!(<(i32, i32)>::from_row(row), Err(FromRowError::RowTooShort));
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
                Some(CQLValue::Int(16)),
                None,
                Some(CQLValue::Set(vec![CQLValue::Int(1), CQLValue::Int(2)])),
            ],
        };

        let my_row: MyRow = MyRow::from_row(row).unwrap();

        assert_eq!(my_row.a, 16);
        assert_eq!(my_row.b, None);
        assert_eq!(my_row.c, Some(vec![1, 2]));
    }
}
