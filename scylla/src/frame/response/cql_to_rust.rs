use super::result::{CQLValue, Row};
use std::net::IpAddr;

/// This trait defines a way to convert CQLValue or Option<CQLValue> into some rust type  
// We can't use From trait because impl From<Option<CQLValue>> for String {...}
// is forbidden since neither From nor String are defined in this crate
pub trait FromCQLVal<T>: Sized {
    fn from_cql(cql_val: T) -> Result<Self, FromCQLValError>;
}

#[derive(Debug, PartialEq, Eq)]
pub enum FromCQLValError {
    BadCQLType,
    ValIsNull,
}

/// This trait defines a way to convert CQL Row into some rust type
pub trait FromRow: Sized {
    fn from_row(row: Row) -> Result<Self, FromRowError>;
}

#[derive(Debug, PartialEq, Eq)]
pub enum FromRowError {
    BadCQLVal(FromCQLValError),
    RowTooShort,
}

// TODO - replace with some error crate
impl std::fmt::Display for FromCQLValError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl std::fmt::Display for FromRowError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl std::error::Error for FromCQLValError {}
impl std::error::Error for FromRowError {}

impl From<FromCQLValError> for FromRowError {
    fn from(from_cql_err: FromCQLValError) -> FromRowError {
        FromRowError::BadCQLVal(from_cql_err)
    }
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
impl_from_cql_val!(String, into_string); // String::from_cql<CQLValue>
impl_from_cql_val!(IpAddr, as_inet); // IpAddr::from_cql<CQLValue>

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

// This macro implements TreFrom<Row> for tuple of types that have FromCQLVal
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

// Implement From<Row> for tuples of size up to 16
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

#[cfg(test)]
mod tests {
    use super::{CQLValue, FromCQLVal, FromCQLValError, FromRow, FromRowError, Row};
    use crate as scylla;
    use crate::macros::FromRow;
    use std::net::{IpAddr, Ipv4Addr};

    #[test]
    fn i32_from_cql() {
        assert_eq!(Ok(1234), i32::from_cql(CQLValue::Int(1234)));
    }

    #[test]
    fn i64_from_cql() {
        assert_eq!(Ok(1234), i64::from_cql(CQLValue::BigInt(1234)));
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
