//! Provides types for dealing with CQL value deserialization.

use std::{
    borrow::Cow,
    collections::{BTreeMap, BTreeSet, HashMap, HashSet},
    hash::{BuildHasher, Hash},
    net::IpAddr,
    sync::Arc,
};

use bytes::Bytes;
use uuid::Uuid;

use std::fmt::Display;

use thiserror::Error;

use super::{make_error_replace_rust_name, DeserializationError, FrameSlice, TypeCheckError};
use crate::frame::frame_errors::LowLevelDeserializationError;
use crate::frame::response::result::CollectionType;
use crate::frame::response::result::UserDefinedType;
use crate::frame::response::result::{ColumnType, NativeType};
use crate::frame::types;
use crate::value::CqlVarintBorrowed;
use crate::value::{
    deser_cql_value, Counter, CqlDate, CqlDecimal, CqlDecimalBorrowed, CqlDuration, CqlTime,
    CqlTimestamp, CqlTimeuuid, CqlValue, CqlVarint,
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
pub trait DeserializeValue<'frame, 'metadata>
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
        typ: &'metadata ColumnType<'metadata>,
        v: Option<FrameSlice<'frame>>,
    ) -> Result<Self, DeserializationError>;
}

impl<'frame, 'metadata> DeserializeValue<'frame, 'metadata> for CqlValue {
    fn type_check(_typ: &ColumnType) -> Result<(), TypeCheckError> {
        // CqlValue accepts all possible CQL types
        Ok(())
    }

    fn deserialize(
        typ: &'metadata ColumnType<'metadata>,
        v: Option<FrameSlice<'frame>>,
    ) -> Result<Self, DeserializationError> {
        let mut val = ensure_not_null_slice::<Self>(typ, v)?;
        let cql = deser_cql_value(typ, &mut val).map_err(deser_error_replace_rust_name::<Self>)?;
        Ok(cql)
    }
}

// Option represents nullability of CQL values:
// None corresponds to null,
// Some(val) to non-null values.
impl<'frame, 'metadata, T> DeserializeValue<'frame, 'metadata> for Option<T>
where
    T: DeserializeValue<'frame, 'metadata>,
{
    fn type_check(typ: &ColumnType) -> Result<(), TypeCheckError> {
        T::type_check(typ)
    }

    fn deserialize(
        typ: &'metadata ColumnType<'metadata>,
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

impl<'frame, 'metadata, T> DeserializeValue<'frame, 'metadata> for MaybeEmpty<T>
where
    T: DeserializeValue<'frame, 'metadata> + Emptiable,
{
    #[inline]
    fn type_check(typ: &ColumnType) -> Result<(), TypeCheckError> {
        <T as DeserializeValue<'frame, 'metadata>>::type_check(typ)
    }

    fn deserialize(
        typ: &'metadata ColumnType<'metadata>,
        v: Option<FrameSlice<'frame>>,
    ) -> Result<Self, DeserializationError> {
        let val = ensure_not_null_slice::<Self>(typ, v)?;
        if val.is_empty() {
            Ok(MaybeEmpty::Empty)
        } else {
            let v = <T as DeserializeValue<'frame, 'metadata>>::deserialize(typ, v)?;
            Ok(MaybeEmpty::Value(v))
        }
    }
}

macro_rules! impl_strict_type {
    ($t:ty, [$($cql:ident)|+], $conv:expr $(, $l:lifetime)?) => {
        impl<$($l,)? 'frame, 'metadata> DeserializeValue<'frame, 'metadata> for $t
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
                typ: &'metadata ColumnType<'metadata>,
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
            |typ: &'metadata ColumnType<'metadata>, v: Option<FrameSlice<'frame>>| {
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
    |typ: &'metadata ColumnType<'metadata>, v: Option<FrameSlice<'frame>>| {
        let val = ensure_not_null_slice::<Self>(typ, v)?;
        let arr = ensure_exact_length::<Self, 1>(typ, val)?;
        Ok(arr[0] != 0x00)
    }
);

impl_fixed_numeric_type!(i8, TinyInt);
impl_fixed_numeric_type!(i16, SmallInt);
impl_fixed_numeric_type!(i32, Int);
impl_fixed_numeric_type!(i64, BigInt);
impl_fixed_numeric_type!(f32, Float);
impl_fixed_numeric_type!(f64, Double);

// other numeric types

impl_emptiable_strict_type!(
    CqlVarint,
    Varint,
    |typ: &'metadata ColumnType<'metadata>, v: Option<FrameSlice<'frame>>| {
        let val = ensure_not_null_slice::<Self>(typ, v)?;
        Ok(CqlVarint::from_signed_bytes_be_slice(val))
    }
);

impl_emptiable_strict_type!(
    CqlVarintBorrowed<'b>,
    Varint,
    |typ: &'metadata ColumnType<'metadata>, v: Option<FrameSlice<'frame>>| {
        let val = ensure_not_null_slice::<Self>(typ, v)?;
        Ok(CqlVarintBorrowed::from_signed_bytes_be_slice(val))
    },
    'b
);

#[cfg(feature = "num-bigint-03")]
impl_emptiable_strict_type!(
    num_bigint_03::BigInt,
    Varint,
    |typ: &'metadata ColumnType<'metadata>, v: Option<FrameSlice<'frame>>| {
        let val = ensure_not_null_slice::<Self>(typ, v)?;
        Ok(num_bigint_03::BigInt::from_signed_bytes_be(val))
    }
);

#[cfg(feature = "num-bigint-04")]
impl_emptiable_strict_type!(
    num_bigint_04::BigInt,
    Varint,
    |typ: &'metadata ColumnType<'metadata>, v: Option<FrameSlice<'frame>>| {
        let val = ensure_not_null_slice::<Self>(typ, v)?;
        Ok(num_bigint_04::BigInt::from_signed_bytes_be(val))
    }
);

impl_emptiable_strict_type!(
    CqlDecimal,
    Decimal,
    |typ: &'metadata ColumnType<'metadata>, v: Option<FrameSlice<'frame>>| {
        let mut val = ensure_not_null_slice::<Self>(typ, v)?;
        let scale = types::read_int(&mut val).map_err(|err| {
            mk_deser_err::<Self>(
                typ,
                BuiltinDeserializationErrorKind::BadDecimalScale(err.into()),
            )
        })?;
        Ok(CqlDecimal::from_signed_be_bytes_slice_and_exponent(
            val, scale,
        ))
    }
);

impl_emptiable_strict_type!(
    CqlDecimalBorrowed<'b>,
    Decimal,
    |typ: &'metadata ColumnType<'metadata>, v: Option<FrameSlice<'frame>>| {
        let mut val = ensure_not_null_slice::<Self>(typ, v)?;
        let scale = types::read_int(&mut val).map_err(|err| {
            mk_deser_err::<Self>(
                typ,
                BuiltinDeserializationErrorKind::BadDecimalScale(err.into()),
            )
        })?;
        Ok(CqlDecimalBorrowed::from_signed_be_bytes_slice_and_exponent(
            val, scale,
        ))
    },
    'b
);

#[cfg(feature = "bigdecimal-04")]
impl_emptiable_strict_type!(
    bigdecimal_04::BigDecimal,
    Decimal,
    |typ: &'metadata ColumnType<'metadata>, v: Option<FrameSlice<'frame>>| {
        let mut val = ensure_not_null_slice::<Self>(typ, v)?;
        let scale = types::read_int(&mut val).map_err(|err| {
            mk_deser_err::<Self>(
                typ,
                BuiltinDeserializationErrorKind::BadDecimalScale(err.into()),
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
    |typ: &'metadata ColumnType<'metadata>, v: Option<FrameSlice<'frame>>| {
        let val = ensure_not_null_slice::<Self>(typ, v)?;
        Ok(val)
    },
    'a
);
impl_strict_type!(
    Vec<u8>,
    Blob,
    |typ: &'metadata ColumnType<'metadata>, v: Option<FrameSlice<'frame>>| {
        let val = ensure_not_null_slice::<Self>(typ, v)?;
        Ok(val.to_vec())
    }
);
impl_strict_type!(
    Bytes,
    Blob,
    |typ: &'metadata ColumnType<'metadata>, v: Option<FrameSlice<'frame>>| {
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
    if matches!(typ, ColumnType::Native(NativeType::Ascii)) && !s.is_ascii() {
        return Err(mk_deser_err::<T>(
            typ,
            BuiltinDeserializationErrorKind::ExpectedAscii,
        ));
    }
    Ok(())
}

impl_string_type!(
    &'a str,
    |typ: &'metadata ColumnType<'metadata>, v: Option<FrameSlice<'frame>>| {
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
    |typ: &'metadata ColumnType<'metadata>, v: Option<FrameSlice<'frame>>| {
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
    |typ: &'metadata ColumnType<'metadata>, v: Option<FrameSlice<'frame>>| {
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
    |typ: &'metadata ColumnType<'metadata>, v: Option<FrameSlice<'frame>>| {
        let mut val = ensure_not_null_slice::<Self>(typ, v)?;

        macro_rules! mk_err {
            ($err: expr) => {
                mk_deser_err::<Self>(typ, $err)
            };
        }

        let months_i64 = types::vint_decode(&mut val).map_err(|err| {
            mk_err!(BuiltinDeserializationErrorKind::BadDate {
                date_field: "months",
                err: err.into()
            })
        })?;
        let months = i32::try_from(months_i64)
            .map_err(|_| mk_err!(BuiltinDeserializationErrorKind::ValueOverflow))?;

        let days_i64 = types::vint_decode(&mut val).map_err(|err| {
            mk_err!(BuiltinDeserializationErrorKind::BadDate {
                date_field: "days",
                err: err.into()
            })
        })?;
        let days = i32::try_from(days_i64)
            .map_err(|_| mk_err!(BuiltinDeserializationErrorKind::ValueOverflow))?;

        let nanoseconds = types::vint_decode(&mut val).map_err(|err| {
            mk_err!(BuiltinDeserializationErrorKind::BadDate {
                date_field: "nanoseconds",
                err: err.into()
            })
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
    |typ: &'metadata ColumnType<'metadata>, v: Option<FrameSlice<'frame>>| {
        let val = ensure_not_null_slice::<Self>(typ, v)?;
        let arr = ensure_exact_length::<Self, 4>(typ, val)?;
        let days = u32::from_be_bytes(*arr);
        Ok(CqlDate(days))
    }
);

#[cfg(any(feature = "chrono-04", feature = "time-03"))]
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

#[cfg(feature = "chrono-04")]
impl_emptiable_strict_type!(chrono_04::NaiveDate, Date, |typ: &'metadata ColumnType<
    'metadata,
>,
                                                         v: Option<
    FrameSlice<'frame>,
>| {
    let fail = || mk_deser_err::<Self>(typ, BuiltinDeserializationErrorKind::ValueOverflow);
    let days_since_epoch =
        chrono_04::Duration::try_days(get_days_since_epoch_from_date_column::<Self>(typ, v)?)
            .ok_or_else(fail)?;
    chrono_04::NaiveDate::from_ymd_opt(1970, 1, 1)
        .unwrap()
        .checked_add_signed(days_since_epoch)
        .ok_or_else(fail)
});

#[cfg(feature = "time-03")]
impl_emptiable_strict_type!(
    time_03::Date,
    Date,
    |typ: &'metadata ColumnType<'metadata>, v: Option<FrameSlice<'frame>>| {
        let days_since_epoch =
            time_03::Duration::days(get_days_since_epoch_from_date_column::<Self>(typ, v)?);
        time_03::Date::from_calendar_date(1970, time_03::Month::January, 1)
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
    |typ: &'metadata ColumnType<'metadata>, v: Option<FrameSlice<'frame>>| {
        let nanoseconds = get_nanos_from_time_column::<Self>(typ, v)?;

        Ok(CqlTime(nanoseconds))
    }
);

#[cfg(feature = "chrono-04")]
impl_emptiable_strict_type!(chrono_04::NaiveTime, Time, |typ: &'metadata ColumnType<
    'metadata,
>,
                                                         v: Option<
    FrameSlice<'frame>,
>| {
    let nanoseconds = get_nanos_from_time_column::<chrono_04::NaiveTime>(typ, v)?;

    let naive_time: chrono_04::NaiveTime = CqlTime(nanoseconds)
        .try_into()
        .map_err(|_| mk_deser_err::<Self>(typ, BuiltinDeserializationErrorKind::ValueOverflow))?;
    Ok(naive_time)
});

#[cfg(feature = "time-03")]
impl_emptiable_strict_type!(
    time_03::Time,
    Time,
    |typ: &'metadata ColumnType<'metadata>, v: Option<FrameSlice<'frame>>| {
        let nanoseconds = get_nanos_from_time_column::<time_03::Time>(typ, v)?;

        let time: time_03::Time = CqlTime(nanoseconds).try_into().map_err(|_| {
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
    |typ: &'metadata ColumnType<'metadata>, v: Option<FrameSlice<'frame>>| {
        let millis = get_millis_from_timestamp_column::<Self>(typ, v)?;
        Ok(CqlTimestamp(millis))
    }
);

#[cfg(feature = "chrono-04")]
impl_emptiable_strict_type!(
    chrono_04::DateTime<chrono_04::Utc>,
    Timestamp,
    |typ: &'metadata ColumnType<'metadata>, v: Option<FrameSlice<'frame>>| {
        use chrono_04::TimeZone as _;

        let millis = get_millis_from_timestamp_column::<Self>(typ, v)?;
        match chrono_04::Utc.timestamp_millis_opt(millis) {
            chrono_04::LocalResult::Single(datetime) => Ok(datetime),
            _ => Err(mk_deser_err::<Self>(
                typ,
                BuiltinDeserializationErrorKind::ValueOverflow,
            )),
        }
    }
);

#[cfg(feature = "time-03")]
impl_emptiable_strict_type!(
    time_03::OffsetDateTime,
    Timestamp,
    |typ: &'metadata ColumnType<'metadata>, v: Option<FrameSlice<'frame>>| {
        let millis = get_millis_from_timestamp_column::<Self>(typ, v)?;
        time_03::OffsetDateTime::from_unix_timestamp_nanos(millis as i128 * 1_000_000)
            .map_err(|_| mk_deser_err::<Self>(typ, BuiltinDeserializationErrorKind::ValueOverflow))
    }
);

// inet

impl_emptiable_strict_type!(
    IpAddr,
    Inet,
    |typ: &'metadata ColumnType<'metadata>, v: Option<FrameSlice<'frame>>| {
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
    |typ: &'metadata ColumnType<'metadata>, v: Option<FrameSlice<'frame>>| {
        let val = ensure_not_null_slice::<Self>(typ, v)?;
        let arr = ensure_exact_length::<Self, 16>(typ, val)?;
        let i = u128::from_be_bytes(*arr);
        Ok(uuid::Uuid::from_u128(i))
    }
);

impl_emptiable_strict_type!(
    CqlTimeuuid,
    Timeuuid,
    |typ: &'metadata ColumnType<'metadata>, v: Option<FrameSlice<'frame>>| {
        let val = ensure_not_null_slice::<Self>(typ, v)?;
        let arr = ensure_exact_length::<Self, 16>(typ, val)?;
        let i = u128::from_be_bytes(*arr);
        Ok(CqlTimeuuid::from(uuid::Uuid::from_u128(i)))
    }
);

// secrecy
#[cfg(feature = "secrecy-08")]
impl<'frame, 'metadata, T> DeserializeValue<'frame, 'metadata> for secrecy_08::Secret<T>
where
    T: DeserializeValue<'frame, 'metadata> + secrecy_08::Zeroize,
{
    fn type_check(typ: &ColumnType) -> Result<(), TypeCheckError> {
        <T as DeserializeValue<'frame, 'metadata>>::type_check(typ)
    }

    fn deserialize(
        typ: &'metadata ColumnType<'metadata>,
        v: Option<FrameSlice<'frame>>,
    ) -> Result<Self, DeserializationError> {
        <T as DeserializeValue<'frame, 'metadata>>::deserialize(typ, v).map(secrecy_08::Secret::new)
    }
}

// collections

make_error_replace_rust_name!(
    pub(crate),
    typck_error_replace_rust_name,
    TypeCheckError,
    BuiltinTypeCheckError
);

make_error_replace_rust_name!(
    pub,
    deser_error_replace_rust_name,
    DeserializationError,
    BuiltinDeserializationError
);

// lists and sets

/// An iterator over either a CQL set or list.
pub struct ListlikeIterator<'frame, 'metadata, T> {
    coll_typ: &'metadata ColumnType<'metadata>,
    elem_typ: &'metadata ColumnType<'metadata>,
    raw_iter: FixedLengthBytesSequenceIterator<'frame>,
    phantom_data: std::marker::PhantomData<T>,
}

impl<'frame, 'metadata, T> ListlikeIterator<'frame, 'metadata, T> {
    fn new(
        coll_typ: &'metadata ColumnType<'metadata>,
        elem_typ: &'metadata ColumnType<'metadata>,
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

    fn empty(
        coll_typ: &'metadata ColumnType<'metadata>,
        elem_typ: &'metadata ColumnType<'metadata>,
    ) -> Self {
        Self {
            coll_typ,
            elem_typ,
            raw_iter: FixedLengthBytesSequenceIterator::empty(),
            phantom_data: std::marker::PhantomData,
        }
    }
}

impl<'frame, 'metadata, T> DeserializeValue<'frame, 'metadata>
    for ListlikeIterator<'frame, 'metadata, T>
where
    T: DeserializeValue<'frame, 'metadata>,
{
    fn type_check(typ: &ColumnType) -> Result<(), TypeCheckError> {
        match typ {
            ColumnType::Collection {
                frozen: false,
                typ: CollectionType::List(el_t),
            }
            | ColumnType::Collection {
                frozen: false,
                typ: CollectionType::Set(el_t),
            } => <T as DeserializeValue<'frame, 'metadata>>::type_check(el_t).map_err(|err| {
                mk_typck_err::<Self>(
                    typ,
                    SetOrListTypeCheckErrorKind::ElementTypeCheckFailed(err),
                )
            }),
            _ => Err(mk_typck_err::<Self>(
                typ,
                BuiltinTypeCheckErrorKind::SetOrListError(
                    SetOrListTypeCheckErrorKind::NotSetOrList,
                ),
            )),
        }
    }

    fn deserialize(
        typ: &'metadata ColumnType<'metadata>,
        v: Option<FrameSlice<'frame>>,
    ) -> Result<Self, DeserializationError> {
        let elem_typ = match typ {
            ColumnType::Collection {
                frozen: false,
                typ: CollectionType::List(elem_typ),
            }
            | ColumnType::Collection {
                frozen: false,
                typ: CollectionType::Set(elem_typ),
            } => elem_typ,
            _ => {
                unreachable!("Typecheck should have prevented this scenario!")
            }
        };

        let mut v = if let Some(v) = v {
            v
        } else {
            return Ok(Self::empty(typ, elem_typ));
        };

        let count = types::read_int_length(v.as_slice_mut()).map_err(|err| {
            mk_deser_err::<Self>(
                typ,
                SetOrListDeserializationErrorKind::LengthDeserializationFailed(
                    DeserializationError::new(err),
                ),
            )
        })?;

        Ok(Self::new(typ, elem_typ, count, v))
    }
}

impl<'frame, 'metadata, T> Iterator for ListlikeIterator<'frame, 'metadata, T>
where
    T: DeserializeValue<'frame, 'metadata>,
{
    type Item = Result<T, DeserializationError>;

    fn next(&mut self) -> Option<Self::Item> {
        let raw = self.raw_iter.next()?.map_err(|err| {
            mk_deser_err::<Self>(
                self.coll_typ,
                BuiltinDeserializationErrorKind::RawCqlBytesReadError(err),
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

impl<'frame, 'metadata, T> DeserializeValue<'frame, 'metadata> for Vec<T>
where
    T: DeserializeValue<'frame, 'metadata>,
{
    fn type_check(typ: &ColumnType) -> Result<(), TypeCheckError> {
        // It makes sense for Set, List and Vector to deserialize to Vec.
        match typ {
            ColumnType::Collection {
                typ: CollectionType::List(_) | CollectionType::Set(_),
                ..
            } => ListlikeIterator::<'frame, 'metadata, T>::type_check(typ)
                .map_err(typck_error_replace_rust_name::<Self>),
            ColumnType::Vector { .. } => VectorIterator::<'frame, 'metadata, T>::type_check(typ)
                .map_err(typck_error_replace_rust_name::<Self>),
            _ => Err(mk_typck_err::<Self>(
                typ,
                BuiltinTypeCheckErrorKind::NotDeserializableToVector,
            )),
        }
    }

    fn deserialize(
        typ: &'metadata ColumnType<'metadata>,
        v: Option<FrameSlice<'frame>>,
    ) -> Result<Self, DeserializationError> {
        match typ {
            ColumnType::Collection {
                typ: CollectionType::List(_) | CollectionType::Set(_),
                ..
            } => ListlikeIterator::<'frame, 'metadata, T>::deserialize(typ, v)
                .and_then(|it| it.collect::<Result<_, DeserializationError>>())
                .map_err(deser_error_replace_rust_name::<Self>),
            ColumnType::Vector { .. } => {
                VectorIterator::<'frame, 'metadata, T>::deserialize(typ, v)
                    .and_then(|it| it.collect::<Result<_, DeserializationError>>())
                    .map_err(deser_error_replace_rust_name::<Self>)
            }
            _ => unreachable!("Should be prevented by typecheck"),
        }
    }
}

impl<'frame, 'metadata, T> DeserializeValue<'frame, 'metadata> for BTreeSet<T>
where
    T: DeserializeValue<'frame, 'metadata> + Ord,
{
    fn type_check(typ: &ColumnType) -> Result<(), TypeCheckError> {
        // It only makes sense for Set to deserialize to BTreeSet.
        // Deserializing List straight to BTreeSet would be lossy.
        match typ {
            ColumnType::Collection {
                frozen: false,
                typ: CollectionType::Set(el_t),
            } => <T as DeserializeValue<'frame, 'metadata>>::type_check(el_t)
                .map_err(typck_error_replace_rust_name::<Self>),
            _ => Err(mk_typck_err::<Self>(
                typ,
                SetOrListTypeCheckErrorKind::NotSet,
            )),
        }
    }

    fn deserialize(
        typ: &'metadata ColumnType<'metadata>,
        v: Option<FrameSlice<'frame>>,
    ) -> Result<Self, DeserializationError> {
        ListlikeIterator::<'frame, 'metadata, T>::deserialize(typ, v)
            .and_then(|it| it.collect::<Result<_, DeserializationError>>())
            .map_err(deser_error_replace_rust_name::<Self>)
    }
}

impl<'frame, 'metadata, T, S> DeserializeValue<'frame, 'metadata> for HashSet<T, S>
where
    T: DeserializeValue<'frame, 'metadata> + Eq + Hash,
    S: BuildHasher + Default + 'frame,
{
    fn type_check(typ: &ColumnType) -> Result<(), TypeCheckError> {
        // It only makes sense for Set to deserialize to HashSet.
        // Deserializing List straight to HashSet would be lossy.
        match typ {
            ColumnType::Collection {
                frozen: false,
                typ: CollectionType::Set(el_t),
            } => <T as DeserializeValue<'frame, 'metadata>>::type_check(el_t)
                .map_err(typck_error_replace_rust_name::<Self>),
            _ => Err(mk_typck_err::<Self>(
                typ,
                SetOrListTypeCheckErrorKind::NotSet,
            )),
        }
    }

    fn deserialize(
        typ: &'metadata ColumnType<'metadata>,
        v: Option<FrameSlice<'frame>>,
    ) -> Result<Self, DeserializationError> {
        ListlikeIterator::<'frame, 'metadata, T>::deserialize(typ, v)
            .and_then(|it| it.collect::<Result<_, DeserializationError>>())
            .map_err(deser_error_replace_rust_name::<Self>)
    }
}

/// A deserialization iterator over a CQL vector.
///
/// Deserialization of a vector is done in two ways, depending on the element type:
/// 1. If the element type is of a fixed-length, the iterator reads
///    a fixed, known number of bytes for each element and deserializes
///    according to the element type. The element count and element size
///    are not encoded as the count is known from the vector type
///    and the size is known from the element type.
///
/// 2. If the element type is of a variable-length, the iterator reads an vint
///    (variable-length integer) for each element, which indicates the number of bytes
///    to read for that element. Then, the iterator reads the specified number of bytes
///    and deserializes according to the element type. This, however, is in contradiction
///    with the CQL protocol, which specifies that the element count is encoded as a 4 byte int!
///    Alas, Cassandra does not respect the protocol, so ScyllaDB and this driver have to follow.
///
/// It would be nice to have a rule to determine if the element type is fixed-length or not,
/// however, we only have a heuristic. There are a few types that should, for all intents and purposes,
/// be considered fixed-length, but are not, e.g TinyInt. See ColumnType::type_size() for the list.
pub struct VectorIterator<'frame, 'metadata, T> {
    collection_type: &'metadata ColumnType<'metadata>,
    element_type: &'metadata ColumnType<'metadata>,
    remaining: usize,
    element_length: Option<usize>,
    slice: FrameSlice<'frame>,
    phantom_data: std::marker::PhantomData<T>,
}

impl<'frame, 'metadata, T> VectorIterator<'frame, 'metadata, T> {
    fn new(
        collection_type: &'metadata ColumnType<'metadata>,
        element_type: &'metadata ColumnType<'metadata>,
        count: usize,
        element_length: Option<usize>,
        slice: FrameSlice<'frame>,
    ) -> Self {
        Self {
            collection_type,
            element_type,
            remaining: count,
            element_length,
            slice,
            phantom_data: std::marker::PhantomData,
        }
    }
}

impl<'frame, 'metadata, T> DeserializeValue<'frame, 'metadata>
    for VectorIterator<'frame, 'metadata, T>
where
    T: DeserializeValue<'frame, 'metadata>,
{
    fn type_check(typ: &ColumnType) -> Result<(), TypeCheckError> {
        match typ {
            ColumnType::Vector { typ: t, .. } => {
                <T as DeserializeValue<'frame, 'metadata>>::type_check(t).map_err(|err| {
                    mk_typck_err::<Self>(typ, VectorTypeCheckErrorKind::ElementTypeCheckFailed(err))
                })?;
                Ok(())
            }
            _ => Err(mk_typck_err::<Self>(
                typ,
                VectorTypeCheckErrorKind::NotVector,
            )),
        }
    }

    fn deserialize(
        typ: &'metadata ColumnType<'metadata>,
        v: Option<FrameSlice<'frame>>,
    ) -> Result<Self, DeserializationError> {
        let (element_type, dimensions) = match typ {
            ColumnType::Vector {
                typ: element_type,
                dimensions,
            } => (element_type, dimensions),
            _ => {
                unreachable!("Typecheck should have prevented this scenario!")
            }
        };

        let v = ensure_not_null_frame_slice::<Self>(typ, v)?;

        Ok(Self::new(
            typ,
            element_type,
            *dimensions as usize,
            element_type.type_size(),
            v,
        ))
    }
}

impl<'frame, 'metadata, T> Iterator for VectorIterator<'frame, 'metadata, T>
where
    T: DeserializeValue<'frame, 'metadata>,
{
    type Item = Result<T, DeserializationError>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.element_length {
            Some(element_length) => {
                self.remaining = self.remaining.checked_sub(1)?;
                let raw = self.slice.read_n_bytes(element_length).map_err(|err| {
                    mk_deser_err::<Self>(
                        self.collection_type,
                        BuiltinDeserializationErrorKind::RawCqlBytesReadError(err),
                    )
                });
                Some(raw.and_then(|raw| {
                    T::deserialize(self.element_type, raw).map_err(|err| {
                        mk_deser_err::<Self>(
                            self.collection_type,
                            VectorDeserializationErrorKind::ElementDeserializationFailed(err),
                        )
                    })
                }))
            }
            None => {
                self.remaining = self.remaining.checked_sub(1)?;
                let size = types::unsigned_vint_decode(self.slice.as_slice_mut()).map_err(|err| {
                    mk_deser_err::<Self>(
                        self.collection_type,
                        BuiltinDeserializationErrorKind::RawCqlBytesReadError(
                            LowLevelDeserializationError::IoError(Arc::new(err)),
                        ),
                    )
                });
                let raw = size.and_then(|size| {
                    self.slice
                        .read_n_bytes(size.try_into().unwrap())
                        .map_err(|err| {
                            mk_deser_err::<Self>(
                                self.collection_type,
                                BuiltinDeserializationErrorKind::RawCqlBytesReadError(err),
                            )
                        })
                });

                Some(raw.and_then(|raw| {
                    T::deserialize(self.element_type, raw).map_err(|err| {
                        mk_deser_err::<Self>(
                            self.element_type,
                            VectorDeserializationErrorKind::ElementDeserializationFailed(err),
                        )
                    })
                }))
            }
        }
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.remaining, Some(self.remaining))
    }
}

/// An iterator over a CQL map.
pub struct MapIterator<'frame, 'metadata, K, V> {
    coll_typ: &'metadata ColumnType<'metadata>,
    k_typ: &'metadata ColumnType<'metadata>,
    v_typ: &'metadata ColumnType<'metadata>,
    raw_iter: FixedLengthBytesSequenceIterator<'frame>,
    phantom_data_k: std::marker::PhantomData<K>,
    phantom_data_v: std::marker::PhantomData<V>,
}

impl<'frame, 'metadata, K, V> MapIterator<'frame, 'metadata, K, V> {
    fn new(
        coll_typ: &'metadata ColumnType<'metadata>,
        k_typ: &'metadata ColumnType<'metadata>,
        v_typ: &'metadata ColumnType<'metadata>,
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

    fn empty(
        coll_typ: &'metadata ColumnType<'metadata>,
        k_typ: &'metadata ColumnType<'metadata>,
        v_typ: &'metadata ColumnType<'metadata>,
    ) -> Self {
        Self {
            coll_typ,
            k_typ,
            v_typ,
            raw_iter: FixedLengthBytesSequenceIterator::empty(),
            phantom_data_k: std::marker::PhantomData,
            phantom_data_v: std::marker::PhantomData,
        }
    }
}

impl<'frame, 'metadata, K, V> DeserializeValue<'frame, 'metadata>
    for MapIterator<'frame, 'metadata, K, V>
where
    K: DeserializeValue<'frame, 'metadata>,
    V: DeserializeValue<'frame, 'metadata>,
{
    fn type_check(typ: &ColumnType) -> Result<(), TypeCheckError> {
        match typ {
            ColumnType::Collection {
                frozen: false,
                typ: CollectionType::Map(k_t, v_t),
            } => {
                <K as DeserializeValue<'frame, 'metadata>>::type_check(k_t).map_err(|err| {
                    mk_typck_err::<Self>(typ, MapTypeCheckErrorKind::KeyTypeCheckFailed(err))
                })?;
                <V as DeserializeValue<'frame, 'metadata>>::type_check(v_t).map_err(|err| {
                    mk_typck_err::<Self>(typ, MapTypeCheckErrorKind::ValueTypeCheckFailed(err))
                })?;
                Ok(())
            }
            _ => Err(mk_typck_err::<Self>(typ, MapTypeCheckErrorKind::NotMap)),
        }
    }

    fn deserialize(
        typ: &'metadata ColumnType<'metadata>,
        v: Option<FrameSlice<'frame>>,
    ) -> Result<Self, DeserializationError> {
        let (k_typ, v_typ) = match typ {
            ColumnType::Collection {
                frozen: false,
                typ: CollectionType::Map(k_t, v_t),
            } => (k_t, v_t),
            _ => {
                unreachable!("Typecheck should have prevented this scenario!")
            }
        };

        let mut v = if let Some(v) = v {
            v
        } else {
            return Ok(Self::empty(typ, k_typ, v_typ));
        };

        let count = types::read_int_length(v.as_slice_mut()).map_err(|err| {
            mk_deser_err::<Self>(
                typ,
                MapDeserializationErrorKind::LengthDeserializationFailed(
                    DeserializationError::new(err),
                ),
            )
        })?;

        Ok(Self::new(typ, k_typ, v_typ, 2 * count, v))
    }
}

impl<'frame, 'metadata, K, V> Iterator for MapIterator<'frame, 'metadata, K, V>
where
    K: DeserializeValue<'frame, 'metadata>,
    V: DeserializeValue<'frame, 'metadata>,
{
    type Item = Result<(K, V), DeserializationError>;

    fn next(&mut self) -> Option<Self::Item> {
        let raw_k = match self.raw_iter.next() {
            Some(Ok(raw_k)) => raw_k,
            Some(Err(err)) => {
                return Some(Err(mk_deser_err::<Self>(
                    self.coll_typ,
                    BuiltinDeserializationErrorKind::RawCqlBytesReadError(err),
                )));
            }
            None => return None,
        };
        let raw_v = match self.raw_iter.next() {
            Some(Ok(raw_v)) => raw_v,
            Some(Err(err)) => {
                return Some(Err(mk_deser_err::<Self>(
                    self.coll_typ,
                    BuiltinDeserializationErrorKind::RawCqlBytesReadError(err),
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

impl<'frame, 'metadata, K, V> DeserializeValue<'frame, 'metadata> for BTreeMap<K, V>
where
    K: DeserializeValue<'frame, 'metadata> + Ord,
    V: DeserializeValue<'frame, 'metadata>,
{
    fn type_check(typ: &ColumnType) -> Result<(), TypeCheckError> {
        MapIterator::<'frame, 'metadata, K, V>::type_check(typ)
            .map_err(typck_error_replace_rust_name::<Self>)
    }

    fn deserialize(
        typ: &'metadata ColumnType<'metadata>,
        v: Option<FrameSlice<'frame>>,
    ) -> Result<Self, DeserializationError> {
        MapIterator::<'frame, 'metadata, K, V>::deserialize(typ, v)
            .and_then(|it| it.collect::<Result<_, DeserializationError>>())
            .map_err(deser_error_replace_rust_name::<Self>)
    }
}

impl<'frame, 'metadata, K, V, S> DeserializeValue<'frame, 'metadata> for HashMap<K, V, S>
where
    K: DeserializeValue<'frame, 'metadata> + Eq + Hash,
    V: DeserializeValue<'frame, 'metadata>,
    S: BuildHasher + Default + 'frame,
{
    fn type_check(typ: &ColumnType) -> Result<(), TypeCheckError> {
        MapIterator::<'frame, 'metadata, K, V>::type_check(typ)
            .map_err(typck_error_replace_rust_name::<Self>)
    }

    fn deserialize(
        typ: &'metadata ColumnType<'metadata>,
        v: Option<FrameSlice<'frame>>,
    ) -> Result<Self, DeserializationError> {
        MapIterator::<'frame, 'metadata, K, V>::deserialize(typ, v)
            .and_then(|it| it.collect::<Result<_, DeserializationError>>())
            .map_err(deser_error_replace_rust_name::<Self>)
    }
}

// tuples

// Implements tuple deserialization.
// The generated impl expects that the serialized data contains exactly the given amount of values.
macro_rules! impl_tuple {
    ($($Ti:ident),*; $($idx:literal),*; $($idf:ident),*) => {
        impl<'frame, 'metadata, $($Ti),*> DeserializeValue<'frame, 'metadata> for ($($Ti,)*)
        where
            $($Ti: DeserializeValue<'frame, 'metadata>),*
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

            fn deserialize(typ: &'metadata ColumnType<'metadata>, v: Option<FrameSlice<'frame>>) -> Result<Self, DeserializationError> {
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

// udts

/// An iterator over fields of a User Defined Type.
///
/// # Note
///
/// A serialized UDT will generally have one value for each field, but it is
/// allowed to have fewer. This iterator differentiates null values
/// from non-existent values in the following way:
///
/// - `None` - missing from the serialized form
/// - `Some(None)` - present, but null
/// - `Some(Some(...))` - non-null, present value
pub struct UdtIterator<'frame, 'metadata> {
    all_fields: &'metadata [(Cow<'metadata, str>, ColumnType<'metadata>)],
    type_name: &'metadata str,
    keyspace: &'metadata str,
    remaining_fields: &'metadata [(Cow<'metadata, str>, ColumnType<'metadata>)],
    raw_iter: BytesSequenceIterator<'frame>,
}

impl<'frame, 'metadata> UdtIterator<'frame, 'metadata> {
    fn new(
        fields: &'metadata [(Cow<'metadata, str>, ColumnType<'metadata>)],
        type_name: &'metadata str,
        keyspace: &'metadata str,
        slice: FrameSlice<'frame>,
    ) -> Self {
        Self {
            all_fields: fields,
            remaining_fields: fields,
            type_name,
            keyspace,
            raw_iter: BytesSequenceIterator::new(slice),
        }
    }

    #[inline]
    pub fn fields(&self) -> &'metadata [(Cow<'metadata, str>, ColumnType<'metadata>)] {
        self.remaining_fields
    }
}

impl<'frame, 'metadata> DeserializeValue<'frame, 'metadata> for UdtIterator<'frame, 'metadata> {
    fn type_check(typ: &ColumnType) -> Result<(), TypeCheckError> {
        match typ {
            ColumnType::UserDefinedType { .. } => Ok(()),
            _ => Err(mk_typck_err::<Self>(typ, UdtTypeCheckErrorKind::NotUdt)),
        }
    }

    fn deserialize(
        typ: &'metadata ColumnType<'metadata>,
        v: Option<FrameSlice<'frame>>,
    ) -> Result<Self, DeserializationError> {
        let v = ensure_not_null_frame_slice::<Self>(typ, v)?;
        let (fields, type_name, keyspace) = match typ {
            ColumnType::UserDefinedType {
                definition: udt, ..
            } => (
                udt.field_types.as_ref(),
                udt.name.as_ref(),
                udt.keyspace.as_ref(),
            ),
            _ => {
                unreachable!("Typecheck should have prevented this scenario!")
            }
        };
        Ok(Self::new(fields, type_name, keyspace, v))
    }
}

impl<'frame, 'metadata> Iterator for UdtIterator<'frame, 'metadata> {
    type Item = (
        &'metadata (Cow<'metadata, str>, ColumnType<'metadata>),
        Result<Option<Option<FrameSlice<'frame>>>, DeserializationError>,
    );

    fn next(&mut self) -> Option<Self::Item> {
        // TODO: Should we fail when there are too many fields?
        let (head, fields) = self.remaining_fields.split_first()?;
        self.remaining_fields = fields;
        let raw_res = match self.raw_iter.next() {
            // The field is there and it was parsed correctly
            Some(Ok(raw)) => Ok(Some(raw)),

            // There were some bytes but they didn't parse as correct field value
            Some(Err(err)) => Err(mk_deser_err::<Self>(
                &ColumnType::UserDefinedType {
                    frozen: false,
                    definition: Arc::new(UserDefinedType {
                        name: self.type_name.into(),
                        keyspace: self.keyspace.into(),
                        field_types: self.all_fields.to_owned(),
                    }),
                },
                BuiltinDeserializationErrorKind::RawCqlBytesReadError(err),
            )),

            // The field is just missing from the serialized form
            None => Ok(None),
        };
        Some((head, raw_res))
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.raw_iter.size_hint()
    }
}

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

fn ensure_tuple_type<'a, 'b, T, const SIZE: usize>(
    typ: &'b ColumnType<'a>,
) -> Result<&'b [ColumnType<'a>; SIZE], TypeCheckError> {
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

    fn empty() -> Self {
        Self {
            slice: FrameSlice::new_empty(),
            remaining: 0,
        }
    }
}

impl<'frame> Iterator for FixedLengthBytesSequenceIterator<'frame> {
    type Item = Result<Option<FrameSlice<'frame>>, LowLevelDeserializationError>;

    fn next(&mut self) -> Option<Self::Item> {
        self.remaining = self.remaining.checked_sub(1)?;
        Some(self.slice.read_cql_bytes())
    }
}

/// Iterates over a sequence of `[bytes]` items from a frame subslice.
///
/// The `[bytes]` items are parsed until the end of subslice is reached.
#[derive(Clone, Copy, Debug)]
pub struct BytesSequenceIterator<'frame> {
    slice: FrameSlice<'frame>,
}

impl<'frame> BytesSequenceIterator<'frame> {
    fn new(slice: FrameSlice<'frame>) -> Self {
        Self { slice }
    }
}

impl<'frame> From<FrameSlice<'frame>> for BytesSequenceIterator<'frame> {
    #[inline]
    fn from(slice: FrameSlice<'frame>) -> Self {
        Self::new(slice)
    }
}

impl<'frame> Iterator for BytesSequenceIterator<'frame> {
    type Item = Result<Option<FrameSlice<'frame>>, LowLevelDeserializationError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.slice.as_slice().is_empty() {
            None
        } else {
            Some(self.slice.read_cql_bytes())
        }
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
    pub cql_type: ColumnType<'static>,

    /// Detailed information about the failure.
    pub kind: BuiltinTypeCheckErrorKind,
}

// Not part of the public API; used in derive macros.
#[doc(hidden)]
pub fn mk_typck_err<T>(
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
        cql_type: cql_type.clone().into_owned(),
        kind: kind.into(),
    })
}

macro_rules! exact_type_check {
    ($typ:ident, $($cql:tt),*) => {
        match $typ {
            $(ColumnType::Native(NativeType::$cql))|* => {},
            _ => return Err(mk_typck_err::<Self>(
                $typ,
                BuiltinTypeCheckErrorKind::MismatchedType {
                    expected: &[$(ColumnType::Native(NativeType::$cql)),*],
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
        expected: &'static [ColumnType<'static>],
    },

    /// A type check failure specific to a CQL set or list.
    SetOrListError(SetOrListTypeCheckErrorKind),

    /// A type check failure specific to a CQL vector.
    VectorError(VectorTypeCheckErrorKind),

    /// A type check failure specific to a CQL map.
    MapError(MapTypeCheckErrorKind),

    /// A type check failure specific to a CQL tuple.
    TupleError(TupleTypeCheckErrorKind),

    /// A type check failure specific to a CQL UDT.
    UdtError(UdtTypeCheckErrorKind),

    /// A type check detected type not deserializable to a vector.
    NotDeserializableToVector,
}

impl From<SetOrListTypeCheckErrorKind> for BuiltinTypeCheckErrorKind {
    #[inline]
    fn from(value: SetOrListTypeCheckErrorKind) -> Self {
        BuiltinTypeCheckErrorKind::SetOrListError(value)
    }
}

impl From<VectorTypeCheckErrorKind> for BuiltinTypeCheckErrorKind {
    #[inline]
    fn from(value: VectorTypeCheckErrorKind) -> Self {
        BuiltinTypeCheckErrorKind::VectorError(value)
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

impl From<UdtTypeCheckErrorKind> for BuiltinTypeCheckErrorKind {
    #[inline]
    fn from(value: UdtTypeCheckErrorKind) -> Self {
        BuiltinTypeCheckErrorKind::UdtError(value)
    }
}

impl Display for BuiltinTypeCheckErrorKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            BuiltinTypeCheckErrorKind::MismatchedType { expected } => {
                write!(f, "expected one of the CQL types: {expected:?}")
            }
            BuiltinTypeCheckErrorKind::SetOrListError(err) => err.fmt(f),
            BuiltinTypeCheckErrorKind::VectorError(err) => err.fmt(f),
            BuiltinTypeCheckErrorKind::MapError(err) => err.fmt(f),
            BuiltinTypeCheckErrorKind::TupleError(err) => err.fmt(f),
            BuiltinTypeCheckErrorKind::UdtError(err) => err.fmt(f),
            BuiltinTypeCheckErrorKind::NotDeserializableToVector => {
                f.write_str("the CQL type is not deserializable to a vector")
            }
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

/// Describes why type checking a vector type failed.
#[derive(Error, Debug, Clone)]
#[non_exhaustive]
pub enum VectorTypeCheckErrorKind {
    /// The CQL type is not a vector.
    #[error(
        "the CQL type the Rust type was attempted to be type checked against was not a vector"
    )]
    NotVector,
    /// Incompatible element types.
    #[error("the vector element types between the CQL type and the Rust type failed to type check against each other: {0}")]
    ElementTypeCheckFailed(TypeCheckError),
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

        /// The type check error that occurred.
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

/// Describes why type checking of a user defined type failed.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum UdtTypeCheckErrorKind {
    /// The CQL type is not a user defined type.
    NotUdt,

    /// The CQL UDT type does not have some fields that is required in the Rust struct.
    ValuesMissingForUdtFields {
        /// Names of fields that the Rust struct requires but are missing in the CQL UDT.
        field_names: Vec<&'static str>,
    },

    /// A different field name was expected at given position.
    FieldNameMismatch {
        /// Index of the field in the Rust struct.
        position: usize,

        /// The name of the Rust field.
        rust_field_name: String,

        /// The name of the CQL UDT field.
        db_field_name: String,
    },

    /// UDT contains an excess field, which does not correspond to any Rust struct's field.
    ExcessFieldInUdt {
        /// The name of the CQL UDT field.
        db_field_name: String,
    },

    /// Duplicated field in serialized data.
    DuplicatedField {
        /// The name of the duplicated field.
        field_name: String,
    },

    /// Fewer fields present in the UDT than required by the Rust type.
    TooFewFields {
        // TODO: decide whether we are OK with restricting to `&'static str` here.
        required_fields: Vec<&'static str>,
        present_fields: Vec<String>,
    },

    /// Type check failed between UDT and Rust type field.
    FieldTypeCheckFailed {
        /// The name of the field whose type check failed.
        field_name: String,

        /// Inner type check error that occurred.
        err: TypeCheckError,
    },
}

impl Display for UdtTypeCheckErrorKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            UdtTypeCheckErrorKind::NotUdt => write!(
                f,
                "the CQL type the Rust type was attempted to be type checked against is not a UDT"
            ),
            UdtTypeCheckErrorKind::ValuesMissingForUdtFields { field_names } => {
                write!(f, "the fields {field_names:?} are missing from the DB data but are required by the Rust type")
            },
            UdtTypeCheckErrorKind::FieldNameMismatch { rust_field_name, db_field_name, position } => write!(
                f,
                "expected field with name {db_field_name} at position {position}, but the Rust field name is {rust_field_name}"
            ),
            UdtTypeCheckErrorKind::ExcessFieldInUdt { db_field_name } => write!(
                f,
                "UDT contains an excess field {}, which does not correspond to any Rust struct's field.",
                db_field_name
            ),
            UdtTypeCheckErrorKind::DuplicatedField { field_name } => write!(
                f,
                "field {} occurs more than once in CQL UDT type",
                field_name
            ),
            UdtTypeCheckErrorKind::TooFewFields { required_fields, present_fields } => write!(
                f,
                "fewer fields present in the UDT than required by the Rust type: UDT has {:?}, Rust type requires {:?}",
                present_fields,
                required_fields,
            ),
            UdtTypeCheckErrorKind::FieldTypeCheckFailed { field_name, err } => write!(
                f,
                "the UDT field {} types between the CQL type and the Rust type failed to type check against each other: {}",
                field_name,
                err
            ),
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
    pub cql_type: ColumnType<'static>,

    /// Detailed information about the failure.
    pub kind: BuiltinDeserializationErrorKind,
}

// Not part of the public API; used in derive macros.
#[doc(hidden)]
pub fn mk_deser_err<T>(
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
        cql_type: cql_type.clone().into_owned(),
        kind: kind.into(),
    })
}

/// Describes why deserialization of some of the built-in types failed.
#[derive(Debug)]
#[non_exhaustive]
pub enum BuiltinDeserializationErrorKind {
    /// Failed to deserialize one of date's fields.
    BadDate {
        date_field: &'static str,
        err: LowLevelDeserializationError,
    },

    /// Failed to deserialize decimal's scale.
    BadDecimalScale(LowLevelDeserializationError),

    /// Failed to deserialize raw bytes of cql value.
    RawCqlBytesReadError(LowLevelDeserializationError),

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

    /// A deserialization failure specific to a CQL vector.
    VectorError(VectorDeserializationErrorKind),

    /// A deserialization failure specific to a CQL map.
    MapError(MapDeserializationErrorKind),

    /// A deserialization failure specific to a CQL tuple.
    TupleError(TupleDeserializationErrorKind),

    /// A deserialization failure specific to a CQL UDT.
    UdtError(UdtDeserializationErrorKind),

    /// Deserialization of this CQL type is not supported by the driver.
    Unsupported,
}

impl Display for BuiltinDeserializationErrorKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            BuiltinDeserializationErrorKind::BadDate { date_field, err } => write!(f, "malformed {} during 'date' deserialization: {}", date_field, err),
            BuiltinDeserializationErrorKind::BadDecimalScale(err) => write!(f, "malformed decimal's scale: {}", err),
            BuiltinDeserializationErrorKind::RawCqlBytesReadError(err) => write!(f, "failed to read raw cql value bytes: {}", err),
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
            BuiltinDeserializationErrorKind::VectorError(err) => err.fmt(f),
            BuiltinDeserializationErrorKind::MapError(err) => err.fmt(f),
            BuiltinDeserializationErrorKind::TupleError(err) => err.fmt(f),
            BuiltinDeserializationErrorKind::UdtError(err) => err.fmt(f),
            BuiltinDeserializationErrorKind::Unsupported => {
                f.write_str("deserialization of this CQL type is not supported by the driver")
            }
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

/// Describes why deserialization of a vector type failed.
#[derive(Error, Debug)]
#[non_exhaustive]
pub enum VectorDeserializationErrorKind {
    /// One of the elements of the vector failed to deserialize.
    #[error("failed to deserialize one of the elements: {0}")]
    ElementDeserializationFailed(DeserializationError),
}

impl From<VectorDeserializationErrorKind> for BuiltinDeserializationErrorKind {
    #[inline]
    fn from(err: VectorDeserializationErrorKind) -> Self {
        Self::VectorError(err)
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

/// Describes why deserialization of a user defined type failed.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum UdtDeserializationErrorKind {
    /// One of the fields failed to deserialize.
    FieldDeserializationFailed {
        /// Name of the field which failed to deserialize.
        field_name: String,

        /// The error that caused the UDT field deserialization to fail.
        err: DeserializationError,
    },
}

impl Display for UdtDeserializationErrorKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            UdtDeserializationErrorKind::FieldDeserializationFailed { field_name, err } => {
                write!(f, "field {field_name} failed to deserialize: {err}")
            }
        }
    }
}

impl From<UdtDeserializationErrorKind> for BuiltinDeserializationErrorKind {
    fn from(err: UdtDeserializationErrorKind) -> Self {
        Self::UdtError(err)
    }
}

#[cfg(test)]
#[path = "value_tests.rs"]
pub(crate) mod tests;

/// ```compile_fail
///
/// #[derive(scylla_macros::DeserializeValue)]
/// #[scylla(crate = scylla_cql, skip_name_checks)]
/// struct TestUdt {}
/// ```
fn _test_udt_bad_attributes_skip_name_check_requires_enforce_order() {}

/// ```compile_fail
///
/// #[derive(scylla_macros::DeserializeValue)]
/// #[scylla(crate = scylla_cql, flavor = "enforce_order", skip_name_checks)]
/// struct TestUdt {
///     #[scylla(rename = "b")]
///     a: i32,
/// }
/// ```
fn _test_udt_bad_attributes_skip_name_check_conflicts_with_rename() {}

/// ```compile_fail
///
/// #[derive(scylla_macros::DeserializeValue)]
/// #[scylla(crate = scylla_cql)]
/// struct TestUdt {
///     #[scylla(rename = "b")]
///     a: i32,
///     b: String,
/// }
/// ```
fn _test_udt_bad_attributes_rename_collision_with_field() {}

/// ```compile_fail
///
/// #[derive(scylla_macros::DeserializeValue)]
/// #[scylla(crate = scylla_cql)]
/// struct TestUdt {
///     #[scylla(rename = "c")]
///     a: i32,
///     #[scylla(rename = "c")]
///     b: String,
/// }
/// ```
fn _test_udt_bad_attributes_rename_collision_with_another_rename() {}

/// ```compile_fail
///
/// #[derive(scylla_macros::DeserializeValue)]
/// #[scylla(crate = scylla_cql, flavor = "enforce_order", skip_name_checks)]
/// struct TestUdt {
///     a: i32,
///     #[scylla(allow_missing)]
///     b: bool,
///     c: String,
/// }
/// ```
fn _test_udt_bad_attributes_name_skip_name_checks_limitations_on_allow_missing() {}

/// ```
/// #[derive(scylla_macros::DeserializeValue)]
/// #[scylla(crate = scylla_cql)]
/// struct TestUdt {
///     a: i32,
///     #[scylla(allow_missing)]
///     b: bool,
///     c: String,
/// }
/// ```
fn _test_udt_unordered_flavour_no_limitations_on_allow_missing() {}
