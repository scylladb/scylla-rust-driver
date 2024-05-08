//! Provides types for dealing with CQL value deserialization.

use std::{
    collections::{BTreeMap, BTreeSet, HashMap, HashSet},
    hash::{BuildHasher, Hash},
    net::IpAddr,
};

use bytes::Bytes;
use uuid::Uuid;

use std::fmt::Display;

use thiserror::Error;

use super::{make_error_replace_rust_name, DeserializationError, FrameSlice, TypeCheckError};
use crate::frame::frame_errors::ParseError;
use crate::frame::response::result::{deser_cql_value, ColumnType, CqlValue};
use crate::frame::types;
use crate::frame::value::{
    Counter, CqlDate, CqlDecimal, CqlDuration, CqlTime, CqlTimestamp, CqlTimeuuid, CqlVarint,
};

/// A type that can be deserialized from a column value inside a row that was
/// returned from a query.
///
/// For tips on how to write a custom implementation of this trait, see the
/// documentation of the parent module.
///
/// The crate also provides a derive macro which allows to automatically
/// implement the trait for a custom type. For more details on what the macro
/// is capable of, see its documentation.
pub trait DeserializeValue<'frame>
where
    Self: Sized,
{
    /// Checks that the column type matches what this type expects.
    fn type_check(typ: &ColumnType) -> Result<(), TypeCheckError>;

    /// Deserialize a column value from given serialized representation.
    ///
    /// This function can assume that the driver called `type_check` to verify
    /// the column's type. Note that `deserialize` is not an unsafe function,
    /// so it should not use the assumption about `type_check` being called
    /// as an excuse to run `unsafe` code.
    fn deserialize(
        typ: &'frame ColumnType,
        v: Option<FrameSlice<'frame>>,
    ) -> Result<Self, DeserializationError>;
}

impl<'frame> DeserializeValue<'frame> for CqlValue {
    fn type_check(_typ: &ColumnType) -> Result<(), TypeCheckError> {
        // CqlValue accepts all possible CQL types
        Ok(())
    }

    fn deserialize(
        typ: &'frame ColumnType,
        v: Option<FrameSlice<'frame>>,
    ) -> Result<Self, DeserializationError> {
        let mut val = ensure_not_null_slice::<Self>(typ, v)?;
        let cql = deser_cql_value(typ, &mut val).map_err(|err| {
            mk_deser_err::<Self>(typ, BuiltinDeserializationErrorKind::GenericParseError(err))
        })?;
        Ok(cql)
    }
}

// Option represents nullability of CQL values:
// None corresponds to null,
// Some(val) to non-null values.
impl<'frame, T> DeserializeValue<'frame> for Option<T>
where
    T: DeserializeValue<'frame>,
{
    fn type_check(typ: &ColumnType) -> Result<(), TypeCheckError> {
        T::type_check(typ)
    }

    fn deserialize(
        typ: &'frame ColumnType,
        v: Option<FrameSlice<'frame>>,
    ) -> Result<Self, DeserializationError> {
        v.map(|_| T::deserialize(typ, v)).transpose()
    }
}

/// Values that may be empty or not.
///
/// In CQL, some types can have a special value of "empty", represented as
/// a serialized value of length 0. An example of this are integral types:
/// the "int" type can actually hold 2^32 + 1 possible values because of this
/// quirk. Note that this is distinct from being NULL.
///
/// Rust types that cannot represent an empty value (e.g. i32) should implement
/// this trait in order to be deserialized as [MaybeEmpty].
pub trait Emptiable {}

/// A value that may be empty or not.
///
/// `MaybeEmpty` was introduced to help support the quirk described in [Emptiable]
/// for Rust types which can't represent the empty, additional value.
#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Clone, Copy)]
pub enum MaybeEmpty<T: Emptiable> {
    Empty,
    Value(T),
}

impl<'frame, T> DeserializeValue<'frame> for MaybeEmpty<T>
where
    T: DeserializeValue<'frame> + Emptiable,
{
    #[inline]
    fn type_check(typ: &ColumnType) -> Result<(), TypeCheckError> {
        <T as DeserializeValue<'frame>>::type_check(typ)
    }

    fn deserialize(
        typ: &'frame ColumnType,
        v: Option<FrameSlice<'frame>>,
    ) -> Result<Self, DeserializationError> {
        let val = ensure_not_null_slice::<Self>(typ, v)?;
        if val.is_empty() {
            Ok(MaybeEmpty::Empty)
        } else {
            let v = <T as DeserializeValue<'frame>>::deserialize(typ, v)?;
            Ok(MaybeEmpty::Value(v))
        }
    }
}

macro_rules! impl_strict_type {
    ($t:ty, [$($cql:ident)|+], $conv:expr $(, $l:lifetime)?) => {
        impl<$($l,)? 'frame> DeserializeValue<'frame> for $t
        where
            $('frame: $l)?
        {
            fn type_check(typ: &ColumnType) -> Result<(), TypeCheckError> {
                // TODO: Format the CQL type names in the same notation
                // that ScyllaDB/Cassandra uses internally and include them
                // in such form in the error message
                exact_type_check!(typ, $($cql),*);
                Ok(())
            }

            fn deserialize(
                typ: &'frame ColumnType,
                v: Option<FrameSlice<'frame>>,
            ) -> Result<Self, DeserializationError> {
                $conv(typ, v)
            }
        }
    };

    // Convenience pattern for omitting brackets if type-checking as single types.
    ($t:ty, $cql:ident, $conv:expr $(, $l:lifetime)?) => {
        impl_strict_type!($t, [$cql], $conv $(, $l)*);
    };
}

macro_rules! impl_emptiable_strict_type {
    ($t:ty, [$($cql:ident)|+], $conv:expr $(, $l:lifetime)?) => {
        impl<$($l,)?> Emptiable for $t {}

        impl_strict_type!($t, [$($cql)|*], $conv $(, $l)*);
    };

    // Convenience pattern for omitting brackets if type-checking as single types.
    ($t:ty, $cql:ident, $conv:expr $(, $l:lifetime)?) => {
        impl_emptiable_strict_type!($t, [$cql], $conv $(, $l)*);
    };

}

// fixed numeric types

macro_rules! impl_fixed_numeric_type {
    ($t:ty, [$($cql:ident)|+]) => {
        impl_emptiable_strict_type!(
            $t,
            [$($cql)|*],
            |typ: &'frame ColumnType, v: Option<FrameSlice<'frame>>| {
                const SIZE: usize = std::mem::size_of::<$t>();
                let val = ensure_not_null_slice::<Self>(typ, v)?;
                let arr = ensure_exact_length::<Self, SIZE>(typ, val)?;
                Ok(<$t>::from_be_bytes(*arr))
            }
        );
    };

    // Convenience pattern for omitting brackets if type-checking as single types.
    ($t:ty, $cql:ident) => {
        impl_fixed_numeric_type!($t, [$cql]);
    };
}

impl_emptiable_strict_type!(
    bool,
    Boolean,
    |typ: &'frame ColumnType, v: Option<FrameSlice<'frame>>| {
        let val = ensure_not_null_slice::<Self>(typ, v)?;
        let arr = ensure_exact_length::<Self, 1>(typ, val)?;
        Ok(arr[0] != 0x00)
    }
);

impl_fixed_numeric_type!(i8, TinyInt);
impl_fixed_numeric_type!(i16, SmallInt);
impl_fixed_numeric_type!(i32, Int);
impl_fixed_numeric_type!(i64, [BigInt | Counter]);
impl_fixed_numeric_type!(f32, Float);
impl_fixed_numeric_type!(f64, Double);

// other numeric types

impl_emptiable_strict_type!(
    CqlVarint,
    Varint,
    |typ: &'frame ColumnType, v: Option<FrameSlice<'frame>>| {
        let val = ensure_not_null_slice::<Self>(typ, v)?;
        Ok(CqlVarint::from_signed_bytes_be_slice(val))
    }
);

#[cfg(feature = "num-bigint-03")]
impl_emptiable_strict_type!(num_bigint_03::BigInt, Varint, |typ: &'frame ColumnType,
                                                            v: Option<
    FrameSlice<'frame>,
>| {
    let val = ensure_not_null_slice::<Self>(typ, v)?;
    Ok(num_bigint_03::BigInt::from_signed_bytes_be(val))
});

#[cfg(feature = "num-bigint-04")]
impl_emptiable_strict_type!(num_bigint_04::BigInt, Varint, |typ: &'frame ColumnType,
                                                            v: Option<
    FrameSlice<'frame>,
>| {
    let val = ensure_not_null_slice::<Self>(typ, v)?;
    Ok(num_bigint_04::BigInt::from_signed_bytes_be(val))
});

impl_emptiable_strict_type!(
    CqlDecimal,
    Decimal,
    |typ: &'frame ColumnType, v: Option<FrameSlice<'frame>>| {
        let mut val = ensure_not_null_slice::<Self>(typ, v)?;
        let scale = types::read_int(&mut val).map_err(|err| {
            mk_deser_err::<Self>(
                typ,
                BuiltinDeserializationErrorKind::GenericParseError(err.into()),
            )
        })?;
        Ok(CqlDecimal::from_signed_be_bytes_slice_and_exponent(
            val, scale,
        ))
    }
);

#[cfg(feature = "bigdecimal-04")]
impl_emptiable_strict_type!(
    bigdecimal_04::BigDecimal,
    Decimal,
    |typ: &'frame ColumnType, v: Option<FrameSlice<'frame>>| {
        let mut val = ensure_not_null_slice::<Self>(typ, v)?;
        let scale = types::read_int(&mut val).map_err(|err| {
            mk_deser_err::<Self>(
                typ,
                BuiltinDeserializationErrorKind::GenericParseError(err.into()),
            )
        })? as i64;
        let int_value = bigdecimal_04::num_bigint::BigInt::from_signed_bytes_be(val);
        Ok(bigdecimal_04::BigDecimal::from((int_value, scale)))
    }
);

// blob

impl_strict_type!(
    &'a [u8],
    Blob,
    |typ: &'frame ColumnType, v: Option<FrameSlice<'frame>>| {
        let val = ensure_not_null_slice::<Self>(typ, v)?;
        Ok(val)
    },
    'a
);
impl_strict_type!(
    Vec<u8>,
    Blob,
    |typ: &'frame ColumnType, v: Option<FrameSlice<'frame>>| {
        let val = ensure_not_null_slice::<Self>(typ, v)?;
        Ok(val.to_vec())
    }
);
impl_strict_type!(
    Bytes,
    Blob,
    |typ: &'frame ColumnType, v: Option<FrameSlice<'frame>>| {
        let val = ensure_not_null_owned::<Self>(typ, v)?;
        Ok(val)
    }
);

// string

macro_rules! impl_string_type {
    ($t:ty, $conv:expr $(, $l:lifetime)?) => {
        impl_strict_type!(
            $t,
            [Ascii | Text],
            $conv
            $(, $l)?
        );
    }
}

fn check_ascii<T>(typ: &ColumnType, s: &[u8]) -> Result<(), DeserializationError> {
    if matches!(typ, ColumnType::Ascii) && !s.is_ascii() {
        return Err(mk_deser_err::<T>(
            typ,
            BuiltinDeserializationErrorKind::ExpectedAscii,
        ));
    }
    Ok(())
}

impl_string_type!(
    &'a str,
    |typ: &'frame ColumnType, v: Option<FrameSlice<'frame>>| {
        let val = ensure_not_null_slice::<Self>(typ, v)?;
        check_ascii::<&str>(typ, val)?;
        let s = std::str::from_utf8(val).map_err(|err| {
            mk_deser_err::<Self>(typ, BuiltinDeserializationErrorKind::InvalidUtf8(err))
        })?;
        Ok(s)
    },
    'a
);
impl_string_type!(
    String,
    |typ: &'frame ColumnType, v: Option<FrameSlice<'frame>>| {
        let val = ensure_not_null_slice::<Self>(typ, v)?;
        check_ascii::<String>(typ, val)?;
        let s = std::str::from_utf8(val).map_err(|err| {
            mk_deser_err::<Self>(typ, BuiltinDeserializationErrorKind::InvalidUtf8(err))
        })?;
        Ok(s.to_string())
    }
);

// TODO: Consider support for deserialization of string::String<Bytes>

// counter

impl_strict_type!(
    Counter,
    Counter,
    |typ: &'frame ColumnType, v: Option<FrameSlice<'frame>>| {
        let val = ensure_not_null_slice::<Self>(typ, v)?;
        let arr = ensure_exact_length::<Self, 8>(typ, val)?;
        let counter = i64::from_be_bytes(*arr);
        Ok(Counter(counter))
    }
);

// date and time types

// duration
impl_strict_type!(
    CqlDuration,
    Duration,
    |typ: &'frame ColumnType, v: Option<FrameSlice<'frame>>| {
        let mut val = ensure_not_null_slice::<Self>(typ, v)?;

        macro_rules! mk_err {
            ($err: expr) => {
                mk_deser_err::<Self>(typ, $err)
            };
        }

        let months_i64 = types::vint_decode(&mut val).map_err(|err| {
            mk_err!(BuiltinDeserializationErrorKind::GenericParseError(
                err.into()
            ))
        })?;
        let months = i32::try_from(months_i64)
            .map_err(|_| mk_err!(BuiltinDeserializationErrorKind::ValueOverflow))?;

        let days_i64 = types::vint_decode(&mut val).map_err(|err| {
            mk_err!(BuiltinDeserializationErrorKind::GenericParseError(
                err.into()
            ))
        })?;
        let days = i32::try_from(days_i64)
            .map_err(|_| mk_err!(BuiltinDeserializationErrorKind::ValueOverflow))?;

        let nanoseconds = types::vint_decode(&mut val).map_err(|err| {
            mk_err!(BuiltinDeserializationErrorKind::GenericParseError(
                err.into()
            ))
        })?;

        Ok(CqlDuration {
            months,
            days,
            nanoseconds,
        })
    }
);

impl_emptiable_strict_type!(
    CqlDate,
    Date,
    |typ: &'frame ColumnType, v: Option<FrameSlice<'frame>>| {
        let val = ensure_not_null_slice::<Self>(typ, v)?;
        let arr = ensure_exact_length::<Self, 4>(typ, val)?;
        let days = u32::from_be_bytes(*arr);
        Ok(CqlDate(days))
    }
);

#[cfg(any(feature = "chrono", feature = "time"))]
fn get_days_since_epoch_from_date_column<T>(
    typ: &ColumnType,
    v: Option<FrameSlice<'_>>,
) -> Result<i64, DeserializationError> {
    let val = ensure_not_null_slice::<T>(typ, v)?;
    let arr = ensure_exact_length::<T, 4>(typ, val)?;
    let days = u32::from_be_bytes(*arr);
    let days_since_epoch = days as i64 - (1i64 << 31);
    Ok(days_since_epoch)
}

#[cfg(feature = "chrono")]
impl_emptiable_strict_type!(
    chrono::NaiveDate,
    Date,
    |typ: &'frame ColumnType, v: Option<FrameSlice<'frame>>| {
        let fail = || mk_deser_err::<Self>(typ, BuiltinDeserializationErrorKind::ValueOverflow);
        let days_since_epoch =
            chrono::Duration::try_days(get_days_since_epoch_from_date_column::<Self>(typ, v)?)
                .ok_or_else(fail)?;
        chrono::NaiveDate::from_ymd_opt(1970, 1, 1)
            .unwrap()
            .checked_add_signed(days_since_epoch)
            .ok_or_else(fail)
    }
);

#[cfg(feature = "time")]
impl_emptiable_strict_type!(
    time::Date,
    Date,
    |typ: &'frame ColumnType, v: Option<FrameSlice<'frame>>| {
        let days_since_epoch =
            time::Duration::days(get_days_since_epoch_from_date_column::<Self>(typ, v)?);
        time::Date::from_calendar_date(1970, time::Month::January, 1)
            .unwrap()
            .checked_add(days_since_epoch)
            .ok_or_else(|| {
                mk_deser_err::<Self>(typ, BuiltinDeserializationErrorKind::ValueOverflow)
            })
    }
);

fn get_nanos_from_time_column<T>(
    typ: &ColumnType,
    v: Option<FrameSlice<'_>>,
) -> Result<i64, DeserializationError> {
    let val = ensure_not_null_slice::<T>(typ, v)?;
    let arr = ensure_exact_length::<T, 8>(typ, val)?;
    let nanoseconds = i64::from_be_bytes(*arr);

    // Valid values are in the range 0 to 86399999999999
    if !(0..=86399999999999).contains(&nanoseconds) {
        return Err(mk_deser_err::<T>(
            typ,
            BuiltinDeserializationErrorKind::ValueOverflow,
        ));
    }

    Ok(nanoseconds)
}

impl_emptiable_strict_type!(
    CqlTime,
    Time,
    |typ: &'frame ColumnType, v: Option<FrameSlice<'frame>>| {
        let nanoseconds = get_nanos_from_time_column::<Self>(typ, v)?;

        Ok(CqlTime(nanoseconds))
    }
);

#[cfg(feature = "chrono")]
impl_emptiable_strict_type!(
    chrono::NaiveTime,
    Time,
    |typ: &'frame ColumnType, v: Option<FrameSlice<'frame>>| {
        let nanoseconds = get_nanos_from_time_column::<chrono::NaiveTime>(typ, v)?;

        let naive_time: chrono::NaiveTime = CqlTime(nanoseconds).try_into().map_err(|_| {
            mk_deser_err::<Self>(typ, BuiltinDeserializationErrorKind::ValueOverflow)
        })?;
        Ok(naive_time)
    }
);

#[cfg(feature = "time")]
impl_emptiable_strict_type!(
    time::Time,
    Time,
    |typ: &'frame ColumnType, v: Option<FrameSlice<'frame>>| {
        let nanoseconds = get_nanos_from_time_column::<time::Time>(typ, v)?;

        let time: time::Time = CqlTime(nanoseconds).try_into().map_err(|_| {
            mk_deser_err::<Self>(typ, BuiltinDeserializationErrorKind::ValueOverflow)
        })?;
        Ok(time)
    }
);

fn get_millis_from_timestamp_column<T>(
    typ: &ColumnType,
    v: Option<FrameSlice<'_>>,
) -> Result<i64, DeserializationError> {
    let val = ensure_not_null_slice::<T>(typ, v)?;
    let arr = ensure_exact_length::<T, 8>(typ, val)?;
    let millis = i64::from_be_bytes(*arr);

    Ok(millis)
}

impl_emptiable_strict_type!(
    CqlTimestamp,
    Timestamp,
    |typ: &'frame ColumnType, v: Option<FrameSlice<'frame>>| {
        let millis = get_millis_from_timestamp_column::<Self>(typ, v)?;
        Ok(CqlTimestamp(millis))
    }
);

#[cfg(feature = "chrono")]
impl_emptiable_strict_type!(
    chrono::DateTime<chrono::Utc>,
    Timestamp,
    |typ: &'frame ColumnType, v: Option<FrameSlice<'frame>>| {
        use chrono::TimeZone as _;

        let millis = get_millis_from_timestamp_column::<Self>(typ, v)?;
        match chrono::Utc.timestamp_millis_opt(millis) {
            chrono::LocalResult::Single(datetime) => Ok(datetime),
            _ => Err(mk_deser_err::<Self>(
                typ,
                BuiltinDeserializationErrorKind::ValueOverflow,
            )),
        }
    }
);

#[cfg(feature = "time")]
impl_emptiable_strict_type!(
    time::OffsetDateTime,
    Timestamp,
    |typ: &'frame ColumnType, v: Option<FrameSlice<'frame>>| {
        let millis = get_millis_from_timestamp_column::<Self>(typ, v)?;
        time::OffsetDateTime::from_unix_timestamp_nanos(millis as i128 * 1_000_000)
            .map_err(|_| mk_deser_err::<Self>(typ, BuiltinDeserializationErrorKind::ValueOverflow))
    }
);

// inet

impl_emptiable_strict_type!(
    IpAddr,
    Inet,
    |typ: &'frame ColumnType, v: Option<FrameSlice<'frame>>| {
        let val = ensure_not_null_slice::<Self>(typ, v)?;
        if let Ok(ipv4) = <[u8; 4]>::try_from(val) {
            Ok(IpAddr::from(ipv4))
        } else if let Ok(ipv6) = <[u8; 16]>::try_from(val) {
            Ok(IpAddr::from(ipv6))
        } else {
            Err(mk_deser_err::<Self>(
                typ,
                BuiltinDeserializationErrorKind::BadInetLength { got: val.len() },
            ))
        }
    }
);

// uuid

impl_emptiable_strict_type!(
    Uuid,
    Uuid,
    |typ: &'frame ColumnType, v: Option<FrameSlice<'frame>>| {
        let val = ensure_not_null_slice::<Self>(typ, v)?;
        let arr = ensure_exact_length::<Self, 16>(typ, val)?;
        let i = u128::from_be_bytes(*arr);
        Ok(uuid::Uuid::from_u128(i))
    }
);

impl_emptiable_strict_type!(
    CqlTimeuuid,
    Timeuuid,
    |typ: &'frame ColumnType, v: Option<FrameSlice<'frame>>| {
        let val = ensure_not_null_slice::<Self>(typ, v)?;
        let arr = ensure_exact_length::<Self, 16>(typ, val)?;
        let i = u128::from_be_bytes(*arr);
        Ok(CqlTimeuuid::from(uuid::Uuid::from_u128(i)))
    }
);

// secrecy
#[cfg(feature = "secret")]
impl<'frame, T> DeserializeValue<'frame> for secrecy::Secret<T>
where
    T: DeserializeValue<'frame> + secrecy::Zeroize,
{
    fn type_check(typ: &ColumnType) -> Result<(), TypeCheckError> {
        <T as DeserializeValue<'frame>>::type_check(typ)
    }

    fn deserialize(
        typ: &'frame ColumnType,
        v: Option<FrameSlice<'frame>>,
    ) -> Result<Self, DeserializationError> {
        <T as DeserializeValue<'frame>>::deserialize(typ, v).map(secrecy::Secret::new)
    }
}

// collections

make_error_replace_rust_name!(
    typck_error_replace_rust_name,
    TypeCheckError,
    BuiltinTypeCheckError
);

make_error_replace_rust_name!(
    deser_error_replace_rust_name,
    DeserializationError,
    BuiltinDeserializationError
);

// lists and sets

/// An iterator over either a CQL set or list.
pub struct ListlikeIterator<'frame, T> {
    coll_typ: &'frame ColumnType,
    elem_typ: &'frame ColumnType,
    raw_iter: FixedLengthBytesSequenceIterator<'frame>,
    phantom_data: std::marker::PhantomData<T>,
}

impl<'frame, T> ListlikeIterator<'frame, T> {
    fn new(
        coll_typ: &'frame ColumnType,
        elem_typ: &'frame ColumnType,
        count: usize,
        slice: FrameSlice<'frame>,
    ) -> Self {
        Self {
            coll_typ,
            elem_typ,
            raw_iter: FixedLengthBytesSequenceIterator::new(count, slice),
            phantom_data: std::marker::PhantomData,
        }
    }
}

impl<'frame, T> DeserializeValue<'frame> for ListlikeIterator<'frame, T>
where
    T: DeserializeValue<'frame>,
{
    fn type_check(typ: &ColumnType) -> Result<(), TypeCheckError> {
        match typ {
            ColumnType::List(el_t) | ColumnType::Set(el_t) => {
                <T as DeserializeValue<'frame>>::type_check(el_t).map_err(|err| {
                    mk_typck_err::<Self>(
                        typ,
                        SetOrListTypeCheckErrorKind::ElementTypeCheckFailed(err),
                    )
                })
            }
            _ => Err(mk_typck_err::<Self>(
                typ,
                BuiltinTypeCheckErrorKind::SetOrListError(
                    SetOrListTypeCheckErrorKind::NotSetOrList,
                ),
            )),
        }
    }

    fn deserialize(
        typ: &'frame ColumnType,
        v: Option<FrameSlice<'frame>>,
    ) -> Result<Self, DeserializationError> {
        let mut v = ensure_not_null_frame_slice::<Self>(typ, v)?;
        let count = types::read_int_length(v.as_slice_mut()).map_err(|err| {
            mk_deser_err::<Self>(
                typ,
                SetOrListDeserializationErrorKind::LengthDeserializationFailed(
                    DeserializationError::new(err),
                ),
            )
        })?;
        let elem_typ = match typ {
            ColumnType::List(elem_typ) | ColumnType::Set(elem_typ) => elem_typ,
            _ => {
                unreachable!("Typecheck should have prevented this scenario!")
            }
        };
        Ok(Self::new(typ, elem_typ, count, v))
    }
}

impl<'frame, T> Iterator for ListlikeIterator<'frame, T>
where
    T: DeserializeValue<'frame>,
{
    type Item = Result<T, DeserializationError>;

    fn next(&mut self) -> Option<Self::Item> {
        let raw = self.raw_iter.next()?.map_err(|err| {
            mk_deser_err::<Self>(
                self.coll_typ,
                BuiltinDeserializationErrorKind::GenericParseError(err),
            )
        });
        Some(raw.and_then(|raw| {
            T::deserialize(self.elem_typ, raw).map_err(|err| {
                mk_deser_err::<Self>(
                    self.coll_typ,
                    SetOrListDeserializationErrorKind::ElementDeserializationFailed(err),
                )
            })
        }))
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        self.raw_iter.size_hint()
    }
}

impl<'frame, T> DeserializeValue<'frame> for Vec<T>
where
    T: DeserializeValue<'frame>,
{
    fn type_check(typ: &ColumnType) -> Result<(), TypeCheckError> {
        // It makes sense for both Set and List to deserialize to Vec.
        ListlikeIterator::<'frame, T>::type_check(typ)
            .map_err(typck_error_replace_rust_name::<Self>)
    }

    fn deserialize(
        typ: &'frame ColumnType,
        v: Option<FrameSlice<'frame>>,
    ) -> Result<Self, DeserializationError> {
        ListlikeIterator::<'frame, T>::deserialize(typ, v)
            .and_then(|it| it.collect::<Result<_, DeserializationError>>())
            .map_err(deser_error_replace_rust_name::<Self>)
    }
}

impl<'frame, T> DeserializeValue<'frame> for BTreeSet<T>
where
    T: DeserializeValue<'frame> + Ord,
{
    fn type_check(typ: &ColumnType) -> Result<(), TypeCheckError> {
        // It only makes sense for Set to deserialize to BTreeSet.
        // Deserializing List straight to BTreeSet would be lossy.
        match typ {
            ColumnType::Set(el_t) => <T as DeserializeValue<'frame>>::type_check(el_t)
                .map_err(typck_error_replace_rust_name::<Self>),
            _ => Err(mk_typck_err::<Self>(
                typ,
                SetOrListTypeCheckErrorKind::NotSet,
            )),
        }
    }

    fn deserialize(
        typ: &'frame ColumnType,
        v: Option<FrameSlice<'frame>>,
    ) -> Result<Self, DeserializationError> {
        ListlikeIterator::<'frame, T>::deserialize(typ, v)
            .and_then(|it| it.collect::<Result<_, DeserializationError>>())
            .map_err(deser_error_replace_rust_name::<Self>)
    }
}

impl<'frame, T, S> DeserializeValue<'frame> for HashSet<T, S>
where
    T: DeserializeValue<'frame> + Eq + Hash,
    S: BuildHasher + Default + 'frame,
{
    fn type_check(typ: &ColumnType) -> Result<(), TypeCheckError> {
        // It only makes sense for Set to deserialize to HashSet.
        // Deserializing List straight to HashSet would be lossy.
        match typ {
            ColumnType::Set(el_t) => <T as DeserializeValue<'frame>>::type_check(el_t)
                .map_err(typck_error_replace_rust_name::<Self>),
            _ => Err(mk_typck_err::<Self>(
                typ,
                SetOrListTypeCheckErrorKind::NotSet,
            )),
        }
    }

    fn deserialize(
        typ: &'frame ColumnType,
        v: Option<FrameSlice<'frame>>,
    ) -> Result<Self, DeserializationError> {
        ListlikeIterator::<'frame, T>::deserialize(typ, v)
            .and_then(|it| it.collect::<Result<_, DeserializationError>>())
            .map_err(deser_error_replace_rust_name::<Self>)
    }
}

/// An iterator over a CQL map.
pub struct MapIterator<'frame, K, V> {
    coll_typ: &'frame ColumnType,
    k_typ: &'frame ColumnType,
    v_typ: &'frame ColumnType,
    raw_iter: FixedLengthBytesSequenceIterator<'frame>,
    phantom_data_k: std::marker::PhantomData<K>,
    phantom_data_v: std::marker::PhantomData<V>,
}

impl<'frame, K, V> MapIterator<'frame, K, V> {
    fn new(
        coll_typ: &'frame ColumnType,
        k_typ: &'frame ColumnType,
        v_typ: &'frame ColumnType,
        count: usize,
        slice: FrameSlice<'frame>,
    ) -> Self {
        Self {
            coll_typ,
            k_typ,
            v_typ,
            raw_iter: FixedLengthBytesSequenceIterator::new(count, slice),
            phantom_data_k: std::marker::PhantomData,
            phantom_data_v: std::marker::PhantomData,
        }
    }
}

impl<'frame, K, V> DeserializeValue<'frame> for MapIterator<'frame, K, V>
where
    K: DeserializeValue<'frame>,
    V: DeserializeValue<'frame>,
{
    fn type_check(typ: &ColumnType) -> Result<(), TypeCheckError> {
        match typ {
            ColumnType::Map(k_t, v_t) => {
                <K as DeserializeValue<'frame>>::type_check(k_t).map_err(|err| {
                    mk_typck_err::<Self>(typ, MapTypeCheckErrorKind::KeyTypeCheckFailed(err))
                })?;
                <V as DeserializeValue<'frame>>::type_check(v_t).map_err(|err| {
                    mk_typck_err::<Self>(typ, MapTypeCheckErrorKind::ValueTypeCheckFailed(err))
                })?;
                Ok(())
            }
            _ => Err(mk_typck_err::<Self>(typ, MapTypeCheckErrorKind::NotMap)),
        }
    }

    fn deserialize(
        typ: &'frame ColumnType,
        v: Option<FrameSlice<'frame>>,
    ) -> Result<Self, DeserializationError> {
        let mut v = ensure_not_null_frame_slice::<Self>(typ, v)?;
        let count = types::read_int_length(v.as_slice_mut()).map_err(|err| {
            mk_deser_err::<Self>(
                typ,
                MapDeserializationErrorKind::LengthDeserializationFailed(
                    DeserializationError::new(err),
                ),
            )
        })?;
        let (k_typ, v_typ) = match typ {
            ColumnType::Map(k_t, v_t) => (k_t, v_t),
            _ => {
                unreachable!("Typecheck should have prevented this scenario!")
            }
        };
        Ok(Self::new(typ, k_typ, v_typ, 2 * count, v))
    }
}

impl<'frame, K, V> Iterator for MapIterator<'frame, K, V>
where
    K: DeserializeValue<'frame>,
    V: DeserializeValue<'frame>,
{
    type Item = Result<(K, V), DeserializationError>;

    fn next(&mut self) -> Option<Self::Item> {
        let raw_k = match self.raw_iter.next() {
            Some(Ok(raw_k)) => raw_k,
            Some(Err(err)) => {
                return Some(Err(mk_deser_err::<Self>(
                    self.coll_typ,
                    BuiltinDeserializationErrorKind::GenericParseError(err),
                )));
            }
            None => return None,
        };
        let raw_v = match self.raw_iter.next() {
            Some(Ok(raw_v)) => raw_v,
            Some(Err(err)) => {
                return Some(Err(mk_deser_err::<Self>(
                    self.coll_typ,
                    BuiltinDeserializationErrorKind::GenericParseError(err),
                )));
            }
            None => return None,
        };

        let do_next = || -> Result<(K, V), DeserializationError> {
            let k = K::deserialize(self.k_typ, raw_k).map_err(|err| {
                mk_deser_err::<Self>(
                    self.coll_typ,
                    MapDeserializationErrorKind::KeyDeserializationFailed(err),
                )
            })?;
            let v = V::deserialize(self.v_typ, raw_v).map_err(|err| {
                mk_deser_err::<Self>(
                    self.coll_typ,
                    MapDeserializationErrorKind::ValueDeserializationFailed(err),
                )
            })?;
            Ok((k, v))
        };
        Some(do_next())
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.raw_iter.size_hint()
    }
}

impl<'frame, K, V> DeserializeValue<'frame> for BTreeMap<K, V>
where
    K: DeserializeValue<'frame> + Ord,
    V: DeserializeValue<'frame>,
{
    fn type_check(typ: &ColumnType) -> Result<(), TypeCheckError> {
        MapIterator::<'frame, K, V>::type_check(typ).map_err(typck_error_replace_rust_name::<Self>)
    }

    fn deserialize(
        typ: &'frame ColumnType,
        v: Option<FrameSlice<'frame>>,
    ) -> Result<Self, DeserializationError> {
        MapIterator::<'frame, K, V>::deserialize(typ, v)
            .and_then(|it| it.collect::<Result<_, DeserializationError>>())
            .map_err(deser_error_replace_rust_name::<Self>)
    }
}

impl<'frame, K, V, S> DeserializeValue<'frame> for HashMap<K, V, S>
where
    K: DeserializeValue<'frame> + Eq + Hash,
    V: DeserializeValue<'frame>,
    S: BuildHasher + Default + 'frame,
{
    fn type_check(typ: &ColumnType) -> Result<(), TypeCheckError> {
        MapIterator::<'frame, K, V>::type_check(typ).map_err(typck_error_replace_rust_name::<Self>)
    }

    fn deserialize(
        typ: &'frame ColumnType,
        v: Option<FrameSlice<'frame>>,
    ) -> Result<Self, DeserializationError> {
        MapIterator::<'frame, K, V>::deserialize(typ, v)
            .and_then(|it| it.collect::<Result<_, DeserializationError>>())
            .map_err(deser_error_replace_rust_name::<Self>)
    }
}

// tuples

// Implements tuple deserialization.
// The generated impl expects that the serialized data contains exactly the given amount of values.
macro_rules! impl_tuple {
    ($($Ti:ident),*; $($idx:literal),*; $($idf:ident),*) => {
        impl<'frame, $($Ti),*> DeserializeValue<'frame> for ($($Ti,)*)
        where
            $($Ti: DeserializeValue<'frame>),*
        {
            fn type_check(typ: &ColumnType) -> Result<(), TypeCheckError> {
                const TUPLE_LEN: usize = (&[$($idx),*] as &[i32]).len();
                let [$($idf),*] = ensure_tuple_type::<($($Ti,)*), TUPLE_LEN>(typ)?;
                $(
                    <$Ti>::type_check($idf).map_err(|err| mk_typck_err::<Self>(
                        typ,
                        TupleTypeCheckErrorKind::FieldTypeCheckFailed {
                            position: $idx,
                            err,
                        }
                    ))?;
                )*
                Ok(())
            }

            fn deserialize(typ: &'frame ColumnType, v: Option<FrameSlice<'frame>>) -> Result<Self, DeserializationError> {
                const TUPLE_LEN: usize = (&[$($idx),*] as &[i32]).len();
                // Safety: we are allowed to assume that type_check() was already called.
                let [$($idf),*] = ensure_tuple_type::<($($Ti,)*), TUPLE_LEN>(typ)
                    .expect("Type check should have prevented this!");

                // Ignore the warning for the zero-sized tuple
                #[allow(unused)]
                let mut v = ensure_not_null_frame_slice::<Self>(typ, v)?;
                let ret = (
                    $(
                        v.read_cql_bytes()
                            .map_err(|err| DeserializationError::new(err))
                            .and_then(|cql_bytes| <$Ti>::deserialize($idf, cql_bytes))
                            .map_err(|err| mk_deser_err::<Self>(
                                typ,
                                TupleDeserializationErrorKind::FieldDeserializationFailed {
                                    position: $idx,
                                    err,
                                }
                            )
                        )?,
                    )*
                );
                Ok(ret)
            }
        }
    }
}

// Implements tuple deserialization for all tuple sizes up to predefined size.
// Accepts 3 lists, (see usage below the definition):
// - type parameters for the consecutive fields,
// - indices of the consecutive fields,
// - consecutive names for variables corresponding to each field.
//
// The idea is to recursively build prefixes of those lists (starting with an empty prefix)
// and for each prefix, implement deserialization for generic tuple represented by it.
// The < > brackets aid syntactically to separate the prefixes (positioned inside them)
// from the remaining suffixes (positioned beyond them).
macro_rules! impl_tuple_multiple {
    // The entry point to the macro.
    // Begins with implementing deserialization for (), then proceeds to the main recursive call.
    ($($Ti:ident),*; $($idx:literal),*; $($idf:ident),*) => {
        impl_tuple!(;;);
        impl_tuple_multiple!(
            $($Ti),* ; < > ;
            $($idx),*; < > ;
            $($idf),*; < >
        );
    };

    // The termination condition. No more fields given to extend the tuple with.
    (;< $($Ti:ident,)* >;;< $($idx:literal,)* >;;< $($idf:ident,)* >) => {};

    // The recursion. Upon each call, a new field is appended to the tuple
    // and deserialization is implemented for it.
    (
        $T_head:ident $(,$T_suffix:ident)*; < $($T_prefix:ident,)* > ;
        $idx_head:literal $(,$idx_suffix:literal)*; < $($idx_prefix:literal,)* >;
        $idf_head:ident $(,$idf_suffix:ident)* ; <$($idf_prefix:ident,)*>
    ) => {
        impl_tuple!(
            $($T_prefix,)* $T_head;
            $($idx_prefix, )* $idx_head;
            $($idf_prefix, )* $idf_head
        );
        impl_tuple_multiple!(
            $($T_suffix),* ; < $($T_prefix,)* $T_head, > ;
            $($idx_suffix),*; < $($idx_prefix, )* $idx_head, > ;
            $($idf_suffix),*; < $($idf_prefix, )* $idf_head, >
        );
    }
}

pub(super) use impl_tuple_multiple;

// Implements tuple deserialization for all tuple sizes up to 16.
impl_tuple_multiple!(
    T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15;
    0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15;
    t0, t1, t2, t3, t4, t5, t6, t7, t8, t9, t10, t11, t12, t13, t14, t15
);

// Utilities

fn ensure_not_null_frame_slice<'frame, T>(
    typ: &ColumnType,
    v: Option<FrameSlice<'frame>>,
) -> Result<FrameSlice<'frame>, DeserializationError> {
    v.ok_or_else(|| mk_deser_err::<T>(typ, BuiltinDeserializationErrorKind::ExpectedNonNull))
}

fn ensure_not_null_slice<'frame, T>(
    typ: &ColumnType,
    v: Option<FrameSlice<'frame>>,
) -> Result<&'frame [u8], DeserializationError> {
    ensure_not_null_frame_slice::<T>(typ, v).map(|frame_slice| frame_slice.as_slice())
}

fn ensure_not_null_owned<T>(
    typ: &ColumnType,
    v: Option<FrameSlice>,
) -> Result<Bytes, DeserializationError> {
    ensure_not_null_frame_slice::<T>(typ, v).map(|frame_slice| frame_slice.to_bytes())
}

fn ensure_exact_length<'frame, T, const SIZE: usize>(
    typ: &ColumnType,
    v: &'frame [u8],
) -> Result<&'frame [u8; SIZE], DeserializationError> {
    v.try_into().map_err(|_| {
        mk_deser_err::<T>(
            typ,
            BuiltinDeserializationErrorKind::ByteLengthMismatch {
                expected: SIZE,
                got: v.len(),
            },
        )
    })
}

fn ensure_tuple_type<T, const SIZE: usize>(
    typ: &ColumnType,
) -> Result<&[ColumnType; SIZE], TypeCheckError> {
    if let ColumnType::Tuple(typs_v) = typ {
        typs_v.as_slice().try_into().map_err(|_| {
            BuiltinTypeCheckErrorKind::TupleError(TupleTypeCheckErrorKind::WrongElementCount {
                rust_type_el_count: SIZE,
                cql_type_el_count: typs_v.len(),
            })
        })
    } else {
        Err(BuiltinTypeCheckErrorKind::TupleError(
            TupleTypeCheckErrorKind::NotTuple,
        ))
    }
    .map_err(|kind| mk_typck_err::<T>(typ, kind))
}

// Helper iterators

/// Iterates over a sequence of `[bytes]` items from a frame subslice, expecting
/// a particular number of items.
///
/// The iterator does not consider it to be an error if there are some bytes
/// remaining in the slice after parsing requested amount of items.
#[derive(Clone, Copy, Debug)]
pub struct FixedLengthBytesSequenceIterator<'frame> {
    slice: FrameSlice<'frame>,
    remaining: usize,
}

impl<'frame> FixedLengthBytesSequenceIterator<'frame> {
    fn new(count: usize, slice: FrameSlice<'frame>) -> Self {
        Self {
            slice,
            remaining: count,
        }
    }
}

impl<'frame> Iterator for FixedLengthBytesSequenceIterator<'frame> {
    type Item = Result<Option<FrameSlice<'frame>>, ParseError>;

    fn next(&mut self) -> Option<Self::Item> {
        self.remaining = self.remaining.checked_sub(1)?;
        Some(self.slice.read_cql_bytes())
    }
}

// Error facilities

/// Type checking of one of the built-in types failed.
#[derive(Debug, Error, Clone)]
#[error("Failed to type check Rust type {rust_name} against CQL type {cql_type:?}: {kind}")]
pub struct BuiltinTypeCheckError {
    /// Name of the Rust type being deserialized.
    pub rust_name: &'static str,

    /// The CQL type that the Rust type was being deserialized from.
    pub cql_type: ColumnType,

    /// Detailed information about the failure.
    pub kind: BuiltinTypeCheckErrorKind,
}

fn mk_typck_err<T>(
    cql_type: &ColumnType,
    kind: impl Into<BuiltinTypeCheckErrorKind>,
) -> TypeCheckError {
    mk_typck_err_named(std::any::type_name::<T>(), cql_type, kind)
}

fn mk_typck_err_named(
    name: &'static str,
    cql_type: &ColumnType,
    kind: impl Into<BuiltinTypeCheckErrorKind>,
) -> TypeCheckError {
    TypeCheckError::new(BuiltinTypeCheckError {
        rust_name: name,
        cql_type: cql_type.clone(),
        kind: kind.into(),
    })
}

macro_rules! exact_type_check {
    ($typ:ident, $($cql:tt),*) => {
        match $typ {
            $(ColumnType::$cql)|* => {},
            _ => return Err(mk_typck_err::<Self>(
                $typ,
                BuiltinTypeCheckErrorKind::MismatchedType {
                    expected: &[$(ColumnType::$cql),*],
                }
            ))
        }
    };
}
use exact_type_check;

/// Describes why type checking some of the built-in types failed.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum BuiltinTypeCheckErrorKind {
    /// Expected one from a list of particular types.
    MismatchedType {
        /// The list of types that the Rust type can deserialize from.
        expected: &'static [ColumnType],
    },

    /// A type check failure specific to a CQL set or list.
    SetOrListError(SetOrListTypeCheckErrorKind),

    /// A type check failure specific to a CQL map.
    MapError(MapTypeCheckErrorKind),

    /// A type check failure specific to a CQL tuple.
    TupleError(TupleTypeCheckErrorKind),
}

impl From<SetOrListTypeCheckErrorKind> for BuiltinTypeCheckErrorKind {
    #[inline]
    fn from(value: SetOrListTypeCheckErrorKind) -> Self {
        BuiltinTypeCheckErrorKind::SetOrListError(value)
    }
}

impl From<MapTypeCheckErrorKind> for BuiltinTypeCheckErrorKind {
    #[inline]
    fn from(value: MapTypeCheckErrorKind) -> Self {
        BuiltinTypeCheckErrorKind::MapError(value)
    }
}

impl From<TupleTypeCheckErrorKind> for BuiltinTypeCheckErrorKind {
    #[inline]
    fn from(value: TupleTypeCheckErrorKind) -> Self {
        BuiltinTypeCheckErrorKind::TupleError(value)
    }
}

impl Display for BuiltinTypeCheckErrorKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            BuiltinTypeCheckErrorKind::MismatchedType { expected } => {
                write!(f, "expected one of the CQL types: {expected:?}")
            }
            BuiltinTypeCheckErrorKind::SetOrListError(err) => err.fmt(f),
            BuiltinTypeCheckErrorKind::MapError(err) => err.fmt(f),
            BuiltinTypeCheckErrorKind::TupleError(err) => err.fmt(f),
        }
    }
}

/// Describes why type checking of a set or list type failed.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum SetOrListTypeCheckErrorKind {
    /// The CQL type is neither a set not a list.
    NotSetOrList,
    /// The CQL type is not a set.
    NotSet,
    /// Incompatible element types.
    ElementTypeCheckFailed(TypeCheckError),
}

impl Display for SetOrListTypeCheckErrorKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SetOrListTypeCheckErrorKind::NotSetOrList => {
                f.write_str("the CQL type the Rust type was attempted to be type checked against was neither a set nor a list")
            }
            SetOrListTypeCheckErrorKind::NotSet => {
                f.write_str("the CQL type the Rust type was attempted to be type checked against was not a set")
            }
            SetOrListTypeCheckErrorKind::ElementTypeCheckFailed(err) => {
                write!(f, "the set or list element types between the CQL type and the Rust type failed to type check against each other: {}", err)
            }
        }
    }
}

/// Describes why type checking of a map type failed.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum MapTypeCheckErrorKind {
    /// The CQL type is not a map.
    NotMap,
    /// Incompatible key types.
    KeyTypeCheckFailed(TypeCheckError),
    /// Incompatible value types.
    ValueTypeCheckFailed(TypeCheckError),
}

impl Display for MapTypeCheckErrorKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            MapTypeCheckErrorKind::NotMap => {
                f.write_str("the CQL type the Rust type was attempted to be type checked against was neither a map")
            }
            MapTypeCheckErrorKind::KeyTypeCheckFailed(err) => {
                write!(f, "the map key types between the CQL type and the Rust type failed to type check against each other: {}", err)
            },
            MapTypeCheckErrorKind::ValueTypeCheckFailed(err) => {
                write!(f, "the map value types between the CQL type and the Rust type failed to type check against each other: {}", err)
            },
        }
    }
}

/// Describes why type checking of a tuple failed.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum TupleTypeCheckErrorKind {
    /// The CQL type is not a tuple.
    NotTuple,

    /// The tuple has the wrong element count.
    WrongElementCount {
        /// The number of elements that the Rust tuple has.
        rust_type_el_count: usize,

        /// The number of elements that the CQL tuple type has.
        cql_type_el_count: usize,
    },

    /// The CQL type and the Rust type of a tuple field failed to type check against each other.
    FieldTypeCheckFailed {
        /// The index of the field whose type check failed.
        position: usize,

        /// The type check error that occured.
        err: TypeCheckError,
    },
}

impl Display for TupleTypeCheckErrorKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TupleTypeCheckErrorKind::NotTuple => write!(
                f,
                "the CQL type the tuple was attempted to be serialized to is not a tuple"
            ),
            TupleTypeCheckErrorKind::WrongElementCount {
                rust_type_el_count,
                cql_type_el_count,
            } => write!(
                f,
                "wrong tuple element count: CQL type has {cql_type_el_count}, the Rust tuple has {rust_type_el_count}"
            ),

            TupleTypeCheckErrorKind::FieldTypeCheckFailed { position, err } => write!(
                f,
                "the CQL type and the Rust type of the tuple field {} failed to type check against each other: {}",
                position,
                err
            )
        }
    }
}

/// Deserialization of one of the built-in types failed.
#[derive(Debug, Error)]
#[error("Failed to deserialize Rust type {rust_name} from CQL type {cql_type:?}: {kind}")]
pub struct BuiltinDeserializationError {
    /// Name of the Rust type being deserialized.
    pub rust_name: &'static str,

    /// The CQL type that the Rust type was being deserialized from.
    pub cql_type: ColumnType,

    /// Detailed information about the failure.
    pub kind: BuiltinDeserializationErrorKind,
}

fn mk_deser_err<T>(
    cql_type: &ColumnType,
    kind: impl Into<BuiltinDeserializationErrorKind>,
) -> DeserializationError {
    mk_deser_err_named(std::any::type_name::<T>(), cql_type, kind)
}

fn mk_deser_err_named(
    name: &'static str,
    cql_type: &ColumnType,
    kind: impl Into<BuiltinDeserializationErrorKind>,
) -> DeserializationError {
    DeserializationError::new(BuiltinDeserializationError {
        rust_name: name,
        cql_type: cql_type.clone(),
        kind: kind.into(),
    })
}

/// Describes why deserialization of some of the built-in types failed.
#[derive(Debug)]
#[non_exhaustive]
pub enum BuiltinDeserializationErrorKind {
    /// A generic deserialization failure - legacy error type.
    GenericParseError(ParseError),

    /// Expected non-null value, got null.
    ExpectedNonNull,

    /// The length of read value in bytes is different than expected for the Rust type.
    ByteLengthMismatch { expected: usize, got: usize },

    /// Expected valid ASCII string.
    ExpectedAscii,

    /// Invalid UTF-8 string.
    InvalidUtf8(std::str::Utf8Error),

    /// The read value is out of range supported by the Rust type.
    // TODO: consider storing additional info here (what exactly did not fit and why)
    ValueOverflow,

    /// The length of read value in bytes is not suitable for IP address.
    BadInetLength { got: usize },

    /// A deserialization failure specific to a CQL set or list.
    SetOrListError(SetOrListDeserializationErrorKind),

    /// A deserialization failure specific to a CQL map.
    MapError(MapDeserializationErrorKind),

    /// A deserialization failure specific to a CQL tuple.
    TupleError(TupleDeserializationErrorKind),
}

impl Display for BuiltinDeserializationErrorKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            BuiltinDeserializationErrorKind::GenericParseError(err) => err.fmt(f),
            BuiltinDeserializationErrorKind::ExpectedNonNull => {
                f.write_str("expected a non-null value, got null")
            }
            BuiltinDeserializationErrorKind::ByteLengthMismatch { expected, got } => write!(
                f,
                "the CQL type requires {} bytes, but got {}",
                expected, got,
            ),
            BuiltinDeserializationErrorKind::ExpectedAscii => {
                f.write_str("expected a valid ASCII string")
            }
            BuiltinDeserializationErrorKind::InvalidUtf8(err) => err.fmt(f),
            BuiltinDeserializationErrorKind::ValueOverflow => {
                // TODO: consider storing Arc<dyn Display/Debug> of the offending value
                // inside this variant for debug purposes.
                f.write_str("read value is out of representable range")
            }
            BuiltinDeserializationErrorKind::BadInetLength { got } => write!(
                f,
                "the length of read value in bytes ({got}) is not suitable for IP address; expected 4 or 16"
            ),
            BuiltinDeserializationErrorKind::SetOrListError(err) => err.fmt(f),
            BuiltinDeserializationErrorKind::MapError(err) => err.fmt(f),
            BuiltinDeserializationErrorKind::TupleError(err) => err.fmt(f),
        }
    }
}

/// Describes why deserialization of a set or list type failed.
#[derive(Debug)]
#[non_exhaustive]
pub enum SetOrListDeserializationErrorKind {
    /// Failed to deserialize set or list's length.
    LengthDeserializationFailed(DeserializationError),

    /// One of the elements of the set/list failed to deserialize.
    ElementDeserializationFailed(DeserializationError),
}

impl Display for SetOrListDeserializationErrorKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SetOrListDeserializationErrorKind::LengthDeserializationFailed(err) => {
                write!(f, "failed to deserialize set or list's length: {}", err)
            }
            SetOrListDeserializationErrorKind::ElementDeserializationFailed(err) => {
                write!(f, "failed to deserialize one of the elements: {}", err)
            }
        }
    }
}

impl From<SetOrListDeserializationErrorKind> for BuiltinDeserializationErrorKind {
    #[inline]
    fn from(err: SetOrListDeserializationErrorKind) -> Self {
        Self::SetOrListError(err)
    }
}

/// Describes why deserialization of a map type failed.
#[derive(Debug)]
#[non_exhaustive]
pub enum MapDeserializationErrorKind {
    /// Failed to deserialize map's length.
    LengthDeserializationFailed(DeserializationError),

    /// One of the keys in the map failed to deserialize.
    KeyDeserializationFailed(DeserializationError),

    /// One of the values in the map failed to deserialize.
    ValueDeserializationFailed(DeserializationError),
}

impl Display for MapDeserializationErrorKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            MapDeserializationErrorKind::LengthDeserializationFailed(err) => {
                write!(f, "failed to deserialize map's length: {}", err)
            }
            MapDeserializationErrorKind::KeyDeserializationFailed(err) => {
                write!(f, "failed to deserialize one of the keys: {}", err)
            }
            MapDeserializationErrorKind::ValueDeserializationFailed(err) => {
                write!(f, "failed to deserialize one of the values: {}", err)
            }
        }
    }
}

impl From<MapDeserializationErrorKind> for BuiltinDeserializationErrorKind {
    fn from(err: MapDeserializationErrorKind) -> Self {
        Self::MapError(err)
    }
}

/// Describes why deserialization of a tuple failed.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum TupleDeserializationErrorKind {
    /// One of the tuple fields failed to deserialize.
    FieldDeserializationFailed {
        /// Index of the tuple field that failed to deserialize.
        position: usize,

        /// The error that caused the tuple field deserialization to fail.
        err: DeserializationError,
    },
}

impl Display for TupleDeserializationErrorKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TupleDeserializationErrorKind::FieldDeserializationFailed {
                position: index,
                err,
            } => {
                write!(f, "field no. {index} failed to deserialize: {err}")
            }
        }
    }
}

impl From<TupleDeserializationErrorKind> for BuiltinDeserializationErrorKind {
    fn from(err: TupleDeserializationErrorKind) -> Self {
        Self::TupleError(err)
    }
}

#[cfg(test)]
mod tests {
    use bytes::{BufMut, Bytes, BytesMut};
    use uuid::Uuid;

    use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
    use std::fmt::Debug;
    use std::net::{IpAddr, Ipv6Addr};

    use crate::frame::response::cql_to_rust::FromCqlVal;
    use crate::frame::response::result::{deser_cql_value, ColumnType, CqlValue};
    use crate::frame::types;
    use crate::frame::value::{
        Counter, CqlDate, CqlDecimal, CqlDuration, CqlTime, CqlTimestamp, CqlTimeuuid, CqlVarint,
    };
    use crate::types::deserialize::{DeserializationError, FrameSlice};
    use crate::types::serialize::value::SerializeValue;
    use crate::types::serialize::CellWriter;

    use super::{
        mk_deser_err, BuiltinDeserializationErrorKind, DeserializeValue, ListlikeIterator,
        MapIterator, MaybeEmpty,
    };

    #[test]
    fn test_deserialize_bytes() {
        const ORIGINAL_BYTES: &[u8] = &[1, 5, 2, 4, 3];

        let bytes = make_bytes(ORIGINAL_BYTES);

        let decoded_slice = deserialize::<&[u8]>(&ColumnType::Blob, &bytes).unwrap();
        let decoded_vec = deserialize::<Vec<u8>>(&ColumnType::Blob, &bytes).unwrap();
        let decoded_bytes = deserialize::<Bytes>(&ColumnType::Blob, &bytes).unwrap();

        assert_eq!(decoded_slice, ORIGINAL_BYTES);
        assert_eq!(decoded_vec, ORIGINAL_BYTES);
        assert_eq!(decoded_bytes, ORIGINAL_BYTES);
    }

    #[test]
    fn test_deserialize_ascii() {
        const ASCII_TEXT: &str = "The quick brown fox jumps over the lazy dog";

        let ascii = make_bytes(ASCII_TEXT.as_bytes());

        let decoded_ascii_str = deserialize::<&str>(&ColumnType::Ascii, &ascii).unwrap();
        let decoded_ascii_string = deserialize::<String>(&ColumnType::Ascii, &ascii).unwrap();
        let decoded_text_str = deserialize::<&str>(&ColumnType::Text, &ascii).unwrap();
        let decoded_text_string = deserialize::<String>(&ColumnType::Text, &ascii).unwrap();

        assert_eq!(decoded_ascii_str, ASCII_TEXT);
        assert_eq!(decoded_ascii_string, ASCII_TEXT);
        assert_eq!(decoded_text_str, ASCII_TEXT);
        assert_eq!(decoded_text_string, ASCII_TEXT);
    }

    #[test]
    fn test_deserialize_text() {
        const UNICODE_TEXT: &str = "Zażółć gęślą jaźń";

        let unicode = make_bytes(UNICODE_TEXT.as_bytes());

        // Should fail because it's not an ASCII string
        deserialize::<&str>(&ColumnType::Ascii, &unicode).unwrap_err();
        deserialize::<String>(&ColumnType::Ascii, &unicode).unwrap_err();

        let decoded_text_str = deserialize::<&str>(&ColumnType::Text, &unicode).unwrap();
        let decoded_text_string = deserialize::<String>(&ColumnType::Text, &unicode).unwrap();
        assert_eq!(decoded_text_str, UNICODE_TEXT);
        assert_eq!(decoded_text_string, UNICODE_TEXT);
    }

    #[test]
    fn test_integral() {
        let tinyint = make_bytes(&[0x01]);
        let decoded_tinyint = deserialize::<i8>(&ColumnType::TinyInt, &tinyint).unwrap();
        assert_eq!(decoded_tinyint, 0x01);

        let smallint = make_bytes(&[0x01, 0x02]);
        let decoded_smallint = deserialize::<i16>(&ColumnType::SmallInt, &smallint).unwrap();
        assert_eq!(decoded_smallint, 0x0102);

        let int = make_bytes(&[0x01, 0x02, 0x03, 0x04]);
        let decoded_int = deserialize::<i32>(&ColumnType::Int, &int).unwrap();
        assert_eq!(decoded_int, 0x01020304);

        let bigint = make_bytes(&[0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08]);
        let decoded_bigint = deserialize::<i64>(&ColumnType::BigInt, &bigint).unwrap();
        assert_eq!(decoded_bigint, 0x0102030405060708);
    }

    #[test]
    fn test_bool() {
        for boolean in [true, false] {
            let boolean_bytes = make_bytes(&[boolean as u8]);
            let decoded_bool = deserialize::<bool>(&ColumnType::Boolean, &boolean_bytes).unwrap();
            assert_eq!(decoded_bool, boolean);
        }
    }

    #[test]
    fn test_floating_point() {
        let float = make_bytes(&[63, 0, 0, 0]);
        let decoded_float = deserialize::<f32>(&ColumnType::Float, &float).unwrap();
        assert_eq!(decoded_float, 0.5);

        let double = make_bytes(&[64, 0, 0, 0, 0, 0, 0, 0]);
        let decoded_double = deserialize::<f64>(&ColumnType::Double, &double).unwrap();
        assert_eq!(decoded_double, 2.0);
    }

    #[test]
    fn test_null_and_empty() {
        // non-nullable emptiable deserialization, non-empty value
        let int = make_bytes(&[21, 37, 0, 0]);
        let decoded_int = deserialize::<MaybeEmpty<i32>>(&ColumnType::Int, &int).unwrap();
        assert_eq!(decoded_int, MaybeEmpty::Value((21 << 24) + (37 << 16)));

        // non-nullable emptiable deserialization, empty value
        let int = make_bytes(&[]);
        let decoded_int = deserialize::<MaybeEmpty<i32>>(&ColumnType::Int, &int).unwrap();
        assert_eq!(decoded_int, MaybeEmpty::Empty);

        // nullable non-emptiable deserialization, non-null value
        let int = make_bytes(&[21, 37, 0, 0]);
        let decoded_int = deserialize::<Option<i32>>(&ColumnType::Int, &int).unwrap();
        assert_eq!(decoded_int, Some((21 << 24) + (37 << 16)));

        // nullable non-emptiable deserialization, null value
        let int = make_null();
        let decoded_int = deserialize::<Option<i32>>(&ColumnType::Int, &int).unwrap();
        assert_eq!(decoded_int, None);

        // nullable emptiable deserialization, non-null non-empty value
        let int = make_bytes(&[]);
        let decoded_int = deserialize::<Option<MaybeEmpty<i32>>>(&ColumnType::Int, &int).unwrap();
        assert_eq!(decoded_int, Some(MaybeEmpty::Empty));
    }

    #[test]
    fn test_from_cql_value_compatibility() {
        // This test should have a sub-case for each type
        // that implements FromCqlValue

        // fixed size integers
        for i in 0..7 {
            let v: i8 = 1 << i;
            compat_check::<i8>(&ColumnType::TinyInt, make_bytes(&v.to_be_bytes()));
            compat_check::<i8>(&ColumnType::TinyInt, make_bytes(&(-v).to_be_bytes()));
        }
        for i in 0..15 {
            let v: i16 = 1 << i;
            compat_check::<i16>(&ColumnType::SmallInt, make_bytes(&v.to_be_bytes()));
            compat_check::<i16>(&ColumnType::SmallInt, make_bytes(&(-v).to_be_bytes()));
        }
        for i in 0..31 {
            let v: i32 = 1 << i;
            compat_check::<i32>(&ColumnType::Int, make_bytes(&v.to_be_bytes()));
            compat_check::<i32>(&ColumnType::Int, make_bytes(&(-v).to_be_bytes()));
        }
        for i in 0..63 {
            let v: i64 = 1 << i;
            compat_check::<i64>(&ColumnType::BigInt, make_bytes(&v.to_be_bytes()));
            compat_check::<i64>(&ColumnType::BigInt, make_bytes(&(-v).to_be_bytes()));
        }

        // bool
        compat_check::<bool>(&ColumnType::Boolean, make_bytes(&[0]));
        compat_check::<bool>(&ColumnType::Boolean, make_bytes(&[1]));

        // fixed size floating point types
        compat_check::<f32>(&ColumnType::Float, make_bytes(&123f32.to_be_bytes()));
        compat_check::<f32>(&ColumnType::Float, make_bytes(&(-123f32).to_be_bytes()));
        compat_check::<f64>(&ColumnType::Double, make_bytes(&123f64.to_be_bytes()));
        compat_check::<f64>(&ColumnType::Double, make_bytes(&(-123f64).to_be_bytes()));

        // big integers
        const PI_STR: &[u8] = b"3.1415926535897932384626433832795028841971693993751058209749445923";
        let num1 = &PI_STR[2..];
        let num2 = [b'-']
            .into_iter()
            .chain(PI_STR[2..].iter().copied())
            .collect::<Vec<_>>();
        let num3 = &b"0"[..];

        // native - CqlVarint
        {
            let num1 = CqlVarint::from_signed_bytes_be_slice(num1);
            let num2 = CqlVarint::from_signed_bytes_be_slice(&num2);
            let num3 = CqlVarint::from_signed_bytes_be_slice(num3);
            compat_check_serialized::<CqlVarint>(&ColumnType::Varint, &num1);
            compat_check_serialized::<CqlVarint>(&ColumnType::Varint, &num2);
            compat_check_serialized::<CqlVarint>(&ColumnType::Varint, &num3);
        }

        #[cfg(feature = "num-bigint-03")]
        {
            use num_bigint_03::BigInt;

            let num1 = BigInt::parse_bytes(num1, 10).unwrap();
            let num2 = BigInt::parse_bytes(&num2, 10).unwrap();
            let num3 = BigInt::parse_bytes(num3, 10).unwrap();
            compat_check_serialized::<BigInt>(&ColumnType::Varint, &num1);
            compat_check_serialized::<BigInt>(&ColumnType::Varint, &num2);
            compat_check_serialized::<BigInt>(&ColumnType::Varint, &num3);
        }

        #[cfg(feature = "num-bigint-04")]
        {
            use num_bigint_04::BigInt;

            let num1 = BigInt::parse_bytes(num1, 10).unwrap();
            let num2 = BigInt::parse_bytes(&num2, 10).unwrap();
            let num3 = BigInt::parse_bytes(num3, 10).unwrap();
            compat_check_serialized::<BigInt>(&ColumnType::Varint, &num1);
            compat_check_serialized::<BigInt>(&ColumnType::Varint, &num2);
            compat_check_serialized::<BigInt>(&ColumnType::Varint, &num3);
        }

        // big decimals
        {
            let scale1 = 0;
            let scale2 = -42;
            let scale3 = 2137;
            let num1 = CqlDecimal::from_signed_be_bytes_slice_and_exponent(num1, scale1);
            let num2 = CqlDecimal::from_signed_be_bytes_and_exponent(num2, scale2);
            let num3 = CqlDecimal::from_signed_be_bytes_slice_and_exponent(num3, scale3);
            compat_check_serialized::<CqlDecimal>(&ColumnType::Decimal, &num1);
            compat_check_serialized::<CqlDecimal>(&ColumnType::Decimal, &num2);
            compat_check_serialized::<CqlDecimal>(&ColumnType::Decimal, &num3);
        }

        // native - CqlDecimal

        #[cfg(feature = "bigdecimal-04")]
        {
            use bigdecimal_04::BigDecimal;

            let num1 = PI_STR.to_vec();
            let num2 = vec![b'-']
                .into_iter()
                .chain(PI_STR.iter().copied())
                .collect::<Vec<_>>();
            let num3 = b"0.0".to_vec();

            let num1 = BigDecimal::parse_bytes(&num1, 10).unwrap();
            let num2 = BigDecimal::parse_bytes(&num2, 10).unwrap();
            let num3 = BigDecimal::parse_bytes(&num3, 10).unwrap();
            compat_check_serialized::<BigDecimal>(&ColumnType::Decimal, &num1);
            compat_check_serialized::<BigDecimal>(&ColumnType::Decimal, &num2);
            compat_check_serialized::<BigDecimal>(&ColumnType::Decimal, &num3);
        }

        // blob
        compat_check::<Vec<u8>>(&ColumnType::Blob, make_bytes(&[]));
        compat_check::<Vec<u8>>(&ColumnType::Blob, make_bytes(&[1, 9, 2, 8, 3, 7, 4, 6, 5]));

        // text types
        for typ in &[ColumnType::Ascii, ColumnType::Text] {
            compat_check::<String>(typ, make_bytes("".as_bytes()));
            compat_check::<String>(typ, make_bytes("foo".as_bytes()));
            compat_check::<String>(typ, make_bytes("superfragilisticexpialidocious".as_bytes()));
        }

        // counters
        for i in 0..63 {
            let v: i64 = 1 << i;
            compat_check::<Counter>(&ColumnType::Counter, make_bytes(&v.to_be_bytes()));
        }

        // duration
        let duration1 = CqlDuration {
            days: 123,
            months: 456,
            nanoseconds: 789,
        };
        let duration2 = CqlDuration {
            days: 987,
            months: 654,
            nanoseconds: 321,
        };
        compat_check_serialized::<CqlDuration>(&ColumnType::Duration, &duration1);
        compat_check_serialized::<CqlDuration>(&ColumnType::Duration, &duration2);

        // date
        let date1 = (2u32.pow(31)).to_be_bytes();
        let date2 = (2u32.pow(31) - 30).to_be_bytes();
        let date3 = (2u32.pow(31) + 30).to_be_bytes();

        compat_check::<CqlDate>(&ColumnType::Date, make_bytes(&date1));
        compat_check::<CqlDate>(&ColumnType::Date, make_bytes(&date2));
        compat_check::<CqlDate>(&ColumnType::Date, make_bytes(&date3));

        #[cfg(feature = "chrono")]
        {
            compat_check::<chrono::NaiveDate>(&ColumnType::Date, make_bytes(&date1));
            compat_check::<chrono::NaiveDate>(&ColumnType::Date, make_bytes(&date2));
            compat_check::<chrono::NaiveDate>(&ColumnType::Date, make_bytes(&date3));
        }

        #[cfg(feature = "time")]
        {
            compat_check::<time::Date>(&ColumnType::Date, make_bytes(&date1));
            compat_check::<time::Date>(&ColumnType::Date, make_bytes(&date2));
            compat_check::<time::Date>(&ColumnType::Date, make_bytes(&date3));
        }

        // time
        let time1 = CqlTime(0);
        let time2 = CqlTime(123456789);
        let time3 = CqlTime(86399999999999); // maximum allowed

        compat_check_serialized::<CqlTime>(&ColumnType::Time, &time1);
        compat_check_serialized::<CqlTime>(&ColumnType::Time, &time2);
        compat_check_serialized::<CqlTime>(&ColumnType::Time, &time3);

        #[cfg(feature = "chrono")]
        {
            compat_check_serialized::<chrono::NaiveTime>(&ColumnType::Time, &time1);
            compat_check_serialized::<chrono::NaiveTime>(&ColumnType::Time, &time2);
            compat_check_serialized::<chrono::NaiveTime>(&ColumnType::Time, &time3);
        }

        #[cfg(feature = "time")]
        {
            compat_check_serialized::<time::Time>(&ColumnType::Time, &time1);
            compat_check_serialized::<time::Time>(&ColumnType::Time, &time2);
            compat_check_serialized::<time::Time>(&ColumnType::Time, &time3);
        }

        // timestamp
        let timestamp1 = CqlTimestamp(0);
        let timestamp2 = CqlTimestamp(123456789);
        let timestamp3 = CqlTimestamp(98765432123456);

        compat_check_serialized::<CqlTimestamp>(&ColumnType::Timestamp, &timestamp1);
        compat_check_serialized::<CqlTimestamp>(&ColumnType::Timestamp, &timestamp2);
        compat_check_serialized::<CqlTimestamp>(&ColumnType::Timestamp, &timestamp3);

        #[cfg(feature = "chrono")]
        {
            compat_check_serialized::<chrono::DateTime<chrono::Utc>>(
                &ColumnType::Timestamp,
                &timestamp1,
            );
            compat_check_serialized::<chrono::DateTime<chrono::Utc>>(
                &ColumnType::Timestamp,
                &timestamp2,
            );
            compat_check_serialized::<chrono::DateTime<chrono::Utc>>(
                &ColumnType::Timestamp,
                &timestamp3,
            );
        }

        #[cfg(feature = "time")]
        {
            compat_check_serialized::<time::OffsetDateTime>(&ColumnType::Timestamp, &timestamp1);
            compat_check_serialized::<time::OffsetDateTime>(&ColumnType::Timestamp, &timestamp2);
            compat_check_serialized::<time::OffsetDateTime>(&ColumnType::Timestamp, &timestamp3);
        }

        // inet
        let ipv4 = IpAddr::from([127u8, 0, 0, 1]);
        let ipv6: IpAddr = Ipv6Addr::LOCALHOST.into();
        compat_check::<IpAddr>(&ColumnType::Inet, make_ip_address(ipv4));
        compat_check::<IpAddr>(&ColumnType::Inet, make_ip_address(ipv6));

        // uuid and timeuuid
        // new_v4 generates random UUIDs, so these are different cases
        for uuid in std::iter::repeat_with(Uuid::new_v4).take(3) {
            compat_check_serialized::<Uuid>(&ColumnType::Uuid, &uuid);
            compat_check_serialized::<CqlTimeuuid>(&ColumnType::Timeuuid, &CqlTimeuuid::from(uuid));
        }

        // empty values
        // ...are implemented via MaybeEmpty and are handled in other tests

        // nulls, represented via Option
        compat_check_serialized::<Option<i32>>(&ColumnType::Int, &123i32);
        compat_check::<Option<i32>>(&ColumnType::Int, make_null());

        // collections
        let mut list = BytesMut::new();
        list.put_i32(3);
        append_bytes(&mut list, &123i32.to_be_bytes());
        append_bytes(&mut list, &456i32.to_be_bytes());
        append_bytes(&mut list, &789i32.to_be_bytes());
        let list = make_bytes(&list);
        let list_type = ColumnType::List(Box::new(ColumnType::Int));
        compat_check::<Vec<i32>>(&list_type, list.clone());
        // Support for deserialization List -> {Hash,BTree}Set was removed not to cause confusion.
        // Such deserialization would be lossy, which is unwanted.

        let mut set = BytesMut::new();
        set.put_i32(3);
        append_bytes(&mut set, &123i32.to_be_bytes());
        append_bytes(&mut set, &456i32.to_be_bytes());
        append_bytes(&mut set, &789i32.to_be_bytes());
        let set = make_bytes(&set);
        let set_type = ColumnType::Set(Box::new(ColumnType::Int));
        compat_check::<Vec<i32>>(&set_type, set.clone());
        compat_check::<BTreeSet<i32>>(&set_type, set.clone());
        compat_check::<HashSet<i32>>(&set_type, set);

        let mut map = BytesMut::new();
        map.put_i32(3);
        append_bytes(&mut map, &123i32.to_be_bytes());
        append_bytes(&mut map, "quick".as_bytes());
        append_bytes(&mut map, &456i32.to_be_bytes());
        append_bytes(&mut map, "brown".as_bytes());
        append_bytes(&mut map, &789i32.to_be_bytes());
        append_bytes(&mut map, "fox".as_bytes());
        let map = make_bytes(&map);
        let map_type = ColumnType::Map(Box::new(ColumnType::Int), Box::new(ColumnType::Text));
        compat_check::<BTreeMap<i32, String>>(&map_type, map.clone());
        compat_check::<HashMap<i32, String>>(&map_type, map);
    }

    #[test]
    fn test_maybe_empty() {
        let empty = make_bytes(&[]);
        let decoded_empty = deserialize::<MaybeEmpty<i8>>(&ColumnType::TinyInt, &empty).unwrap();
        assert_eq!(decoded_empty, MaybeEmpty::Empty);

        let non_empty = make_bytes(&[0x01]);
        let decoded_non_empty =
            deserialize::<MaybeEmpty<i8>>(&ColumnType::TinyInt, &non_empty).unwrap();
        assert_eq!(decoded_non_empty, MaybeEmpty::Value(0x01));
    }

    #[test]
    fn test_list_and_set() {
        let mut collection_contents = BytesMut::new();
        collection_contents.put_i32(3);
        append_bytes(&mut collection_contents, "quick".as_bytes());
        append_bytes(&mut collection_contents, "brown".as_bytes());
        append_bytes(&mut collection_contents, "fox".as_bytes());

        let collection = make_bytes(&collection_contents);

        let list_typ = ColumnType::List(Box::new(ColumnType::Ascii));
        let set_typ = ColumnType::Set(Box::new(ColumnType::Ascii));

        // iterator
        let mut iter = deserialize::<ListlikeIterator<&str>>(&list_typ, &collection).unwrap();
        assert_eq!(iter.next().transpose().unwrap(), Some("quick"));
        assert_eq!(iter.next().transpose().unwrap(), Some("brown"));
        assert_eq!(iter.next().transpose().unwrap(), Some("fox"));
        assert_eq!(iter.next().transpose().unwrap(), None);

        let expected_vec_str = vec!["quick", "brown", "fox"];
        let expected_vec_string = vec!["quick".to_string(), "brown".to_string(), "fox".to_string()];

        // list
        let decoded_vec_str = deserialize::<Vec<&str>>(&list_typ, &collection).unwrap();
        let decoded_vec_string = deserialize::<Vec<String>>(&list_typ, &collection).unwrap();
        assert_eq!(decoded_vec_str, expected_vec_str);
        assert_eq!(decoded_vec_string, expected_vec_string);

        // hash set
        let decoded_hash_str = deserialize::<HashSet<&str>>(&set_typ, &collection).unwrap();
        let decoded_hash_string = deserialize::<HashSet<String>>(&set_typ, &collection).unwrap();
        assert_eq!(
            decoded_hash_str,
            expected_vec_str.clone().into_iter().collect(),
        );
        assert_eq!(
            decoded_hash_string,
            expected_vec_string.clone().into_iter().collect(),
        );

        // btree set
        let decoded_btree_str = deserialize::<BTreeSet<&str>>(&set_typ, &collection).unwrap();
        let decoded_btree_string = deserialize::<BTreeSet<String>>(&set_typ, &collection).unwrap();
        assert_eq!(
            decoded_btree_str,
            expected_vec_str.clone().into_iter().collect(),
        );
        assert_eq!(
            decoded_btree_string,
            expected_vec_string.into_iter().collect(),
        );
    }

    #[test]
    fn test_map() {
        let mut collection_contents = BytesMut::new();
        collection_contents.put_i32(3);
        append_bytes(&mut collection_contents, &1i32.to_be_bytes());
        append_bytes(&mut collection_contents, "quick".as_bytes());
        append_bytes(&mut collection_contents, &2i32.to_be_bytes());
        append_bytes(&mut collection_contents, "brown".as_bytes());
        append_bytes(&mut collection_contents, &3i32.to_be_bytes());
        append_bytes(&mut collection_contents, "fox".as_bytes());

        let collection = make_bytes(&collection_contents);

        let typ = ColumnType::Map(Box::new(ColumnType::Int), Box::new(ColumnType::Ascii));

        // iterator
        let mut iter = deserialize::<MapIterator<i32, &str>>(&typ, &collection).unwrap();
        assert_eq!(iter.next().transpose().unwrap(), Some((1, "quick")));
        assert_eq!(iter.next().transpose().unwrap(), Some((2, "brown")));
        assert_eq!(iter.next().transpose().unwrap(), Some((3, "fox")));
        assert_eq!(iter.next().transpose().unwrap(), None);

        let expected_str = vec![(1, "quick"), (2, "brown"), (3, "fox")];
        let expected_string = vec![
            (1, "quick".to_string()),
            (2, "brown".to_string()),
            (3, "fox".to_string()),
        ];

        // hash set
        let decoded_hash_str = deserialize::<HashMap<i32, &str>>(&typ, &collection).unwrap();
        let decoded_hash_string = deserialize::<HashMap<i32, String>>(&typ, &collection).unwrap();
        assert_eq!(decoded_hash_str, expected_str.clone().into_iter().collect());
        assert_eq!(
            decoded_hash_string,
            expected_string.clone().into_iter().collect(),
        );

        // btree set
        let decoded_btree_str = deserialize::<BTreeMap<i32, &str>>(&typ, &collection).unwrap();
        let decoded_btree_string = deserialize::<BTreeMap<i32, String>>(&typ, &collection).unwrap();
        assert_eq!(
            decoded_btree_str,
            expected_str.clone().into_iter().collect(),
        );
        assert_eq!(decoded_btree_string, expected_string.into_iter().collect(),);
    }

    #[test]
    fn test_tuples() {
        let mut tuple_contents = BytesMut::new();
        append_bytes(&mut tuple_contents, &42i32.to_be_bytes());
        append_bytes(&mut tuple_contents, "foo".as_bytes());
        append_null(&mut tuple_contents);

        let tuple = make_bytes(&tuple_contents);

        let typ = ColumnType::Tuple(vec![ColumnType::Int, ColumnType::Ascii, ColumnType::Uuid]);

        let tup = deserialize::<(i32, &str, Option<Uuid>)>(&typ, &tuple).unwrap();
        assert_eq!(tup, (42, "foo", None));
    }

    // Checks that both new and old serialization framework
    // produces the same results in this case
    fn compat_check<T>(typ: &ColumnType, raw: Bytes)
    where
        T: for<'f> DeserializeValue<'f>,
        T: FromCqlVal<Option<CqlValue>>,
        T: Debug + PartialEq,
    {
        let mut slice = raw.as_ref();
        let mut cell = types::read_bytes_opt(&mut slice).unwrap();
        let old = T::from_cql(
            cell.as_mut()
                .map(|c| deser_cql_value(typ, c))
                .transpose()
                .unwrap(),
        )
        .unwrap();
        let new = deserialize::<T>(typ, &raw).unwrap();
        assert_eq!(old, new);
    }

    fn compat_check_serialized<T>(typ: &ColumnType, val: &dyn SerializeValue)
    where
        T: for<'f> DeserializeValue<'f>,
        T: FromCqlVal<Option<CqlValue>>,
        T: Debug + PartialEq,
    {
        let raw = serialize(typ, val);
        compat_check::<T>(typ, raw);
    }

    fn deserialize<'frame, T>(
        typ: &'frame ColumnType,
        bytes: &'frame Bytes,
    ) -> Result<T, DeserializationError>
    where
        T: DeserializeValue<'frame>,
    {
        <T as DeserializeValue<'frame>>::type_check(typ)
            .map_err(|typecheck_err| DeserializationError(typecheck_err.0))?;
        let mut frame_slice = FrameSlice::new(bytes);
        let value = frame_slice.read_cql_bytes().map_err(|err| {
            mk_deser_err::<T>(typ, BuiltinDeserializationErrorKind::GenericParseError(err))
        })?;
        <T as DeserializeValue<'frame>>::deserialize(typ, value)
    }

    fn make_bytes(cell: &[u8]) -> Bytes {
        let mut b = BytesMut::new();
        append_bytes(&mut b, cell);
        b.freeze()
    }

    fn serialize(typ: &ColumnType, value: &dyn SerializeValue) -> Bytes {
        let mut bytes = Bytes::new();
        serialize_to_buf(typ, value, &mut bytes);
        bytes
    }

    fn serialize_to_buf(typ: &ColumnType, value: &dyn SerializeValue, buf: &mut Bytes) {
        let mut v = Vec::new();
        let writer = CellWriter::new(&mut v);
        value.serialize(typ, writer).unwrap();
        *buf = v.into();
    }

    fn make_ip_address(ip: IpAddr) -> Bytes {
        match ip {
            IpAddr::V4(v4) => make_bytes(&v4.octets()),
            IpAddr::V6(v6) => make_bytes(&v6.octets()),
        }
    }

    fn append_bytes(b: &mut impl BufMut, cell: &[u8]) {
        b.put_i32(cell.len() as i32);
        b.put_slice(cell);
    }

    fn make_null() -> Bytes {
        let mut b = BytesMut::new();
        append_null(&mut b);
        b.freeze()
    }

    fn append_null(b: &mut impl BufMut) {
        b.put_i32(-1);
    }
}
