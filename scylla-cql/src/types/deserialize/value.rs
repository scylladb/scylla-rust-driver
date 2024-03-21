//! Provides types for dealing with CQL value deserialization.

use std::{
    collections::{BTreeMap, BTreeSet, HashMap, HashSet},
    hash::{BuildHasher, Hash},
    net::IpAddr,
};

use bytes::Bytes;

#[cfg(feature = "chrono")]
use chrono::{DateTime, NaiveDate, NaiveTime, TimeZone as _, Utc};
use uuid::Uuid;

use super::FrameSlice;
use crate::frame::{
    frame_errors::ParseError,
    response::result::{deser_cql_value, ColumnType, CqlValue},
    types,
    value::{Counter, CqlDate, CqlDuration, CqlTime, CqlTimestamp, CqlTimeuuid, CqlVarint},
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
pub trait DeserializeCql<'frame>
where
    Self: Sized,
{
    /// Checks that the column type matches what this type expects.
    fn type_check(typ: &ColumnType) -> Result<(), ParseError>;

    /// Deserialize a column value from given serialized representation.
    ///
    /// This function can assume that the driver called `type_check` to verify
    /// the column's type. Note that `deserialize` is not an unsafe function,
    /// so it should not use the assumption about `type_check` being called
    /// as an excuse to run `unsafe` code.
    fn deserialize(
        typ: &'frame ColumnType,
        v: Option<FrameSlice<'frame>>,
    ) -> Result<Self, ParseError>;
}

impl<'frame> DeserializeCql<'frame> for CqlValue {
    fn type_check(_typ: &ColumnType) -> Result<(), ParseError> {
        // CqlValue accepts all possible CQL types
        Ok(())
    }

    fn deserialize(
        typ: &'frame ColumnType,
        v: Option<FrameSlice<'frame>>,
    ) -> Result<Self, ParseError> {
        let mut val = ensure_not_null(v)?;
        let cql = deser_cql_value(typ, &mut val)?;
        Ok(cql)
    }
}

impl<'frame, T> DeserializeCql<'frame> for Option<T>
where
    T: DeserializeCql<'frame>,
{
    fn type_check(typ: &ColumnType) -> Result<(), ParseError> {
        T::type_check(typ)
    }

    fn deserialize(
        typ: &'frame ColumnType,
        v: Option<FrameSlice<'frame>>,
    ) -> Result<Self, ParseError> {
        v.map(|_| T::deserialize(typ, v)).transpose()
    }
}

macro_rules! impl_strict_type {
    ($cql_name:literal, $t:ty, $cql_type:pat, $conv:expr $(, $l:lifetime)?) => {
        impl<$($l,)? 'frame> DeserializeCql<'frame> for $t
        where
            $('frame: $l)?
        {
            fn type_check(typ: &ColumnType) -> Result<(), ParseError> {
                // TODO: Format the CQL type names in the same notation
                // that ScyllaDB/Casssandra uses internally and include them
                // in such form in the error message
                match typ {
                    $cql_type => Ok(()),
                    _ => Err(ParseError::BadIncomingData(format!(
                        "Expected {}, got {:?}",
                        $cql_name, typ,
                    ))),
                }
            }

            fn deserialize(
                typ: &'frame ColumnType,
                v: Option<FrameSlice<'frame>>,
            ) -> Result<Self, ParseError> {
                $conv(typ, v)
            }
        }
    };
}

// fixed numeric types

macro_rules! impl_fixed_numeric_type {
    ($cql_name:literal, $t:ty, $col_type:pat) => {
        impl_strict_type!(
            $cql_name,
            $t,
            $col_type,
            |_typ: &'frame ColumnType, v: Option<FrameSlice<'frame>>| {
                const SIZE: usize = std::mem::size_of::<$t>();
                let val = ensure_not_null(v)?;
                let arr = ensure_exact_length::<SIZE>($cql_name, val)?;
                Ok(<$t>::from_be_bytes(arr))
            }
        );
    };
}

impl_strict_type!(
    "boolean",
    bool,
    ColumnType::Boolean,
    |_typ: &'frame ColumnType, v: Option<FrameSlice<'frame>>| {
        let val = ensure_not_null(v)?;
        let arr = ensure_exact_length::<1>("boolean", val)?;
        Ok(arr[0] != 0x00)
    }
);

impl_fixed_numeric_type!("tinyint", i8, ColumnType::TinyInt);
impl_fixed_numeric_type!("smallint", i16, ColumnType::SmallInt);
impl_fixed_numeric_type!("int", i32, ColumnType::Int);
impl_fixed_numeric_type!(
    "bigint or counter",
    i64,
    ColumnType::BigInt | ColumnType::Counter
);
impl_fixed_numeric_type!("float", f32, ColumnType::Float);
impl_fixed_numeric_type!("double", f64, ColumnType::Double);

// other numeric types

impl_strict_type!(
    "varint",
    CqlVarint,
    ColumnType::Varint,
    |_typ: &'frame ColumnType, v: Option<FrameSlice<'frame>>| {
        let val = ensure_not_null(v)?;
        Ok(CqlVarint::from_signed_bytes_be_slice(val))
    }
);

#[cfg(feature = "num-bigint-03")]
impl_strict_type!(
    "varint",
    num_bigint_03::BigInt,
    ColumnType::Varint,
    |_typ: &'frame ColumnType, v: Option<FrameSlice<'frame>>| {
        let val = ensure_not_null(v)?;
        Ok(num_bigint_03::BigInt::from_signed_bytes_be(val))
    }
);

#[cfg(feature = "num-bigint-04")]
impl_strict_type!(
    "varint",
    num_bigint_04::BigInt,
    ColumnType::Varint,
    |_typ: &'frame ColumnType, v: Option<FrameSlice<'frame>>| {
        let val = ensure_not_null(v)?;
        Ok(num_bigint_04::BigInt::from_signed_bytes_be(val))
    }
);

#[cfg(feature = "bigdecimal-04")]
impl_strict_type!(
    "decimal",
    bigdecimal_04::BigDecimal,
    ColumnType::Decimal,
    |_typ: &'frame ColumnType, v: Option<FrameSlice<'frame>>| {
        let mut val = ensure_not_null(v)?;
        let scale = types::read_int(&mut val)? as i64;
        let int_value = bigdecimal_04::num_bigint::BigInt::from_signed_bytes_be(val);
        Ok(bigdecimal_04::BigDecimal::from((int_value, scale)))
    }
);

// blob

impl_strict_type!(
    "blob",
    &'a [u8],
    ColumnType::Blob,
    |_typ: &'frame ColumnType, v: Option<FrameSlice<'frame>>| {
        let val = ensure_not_null(v)?;
        Ok(val)
    },
    'a
);
impl_strict_type!(
    "blob",
    Vec<u8>,
    ColumnType::Blob,
    |_typ: &'frame ColumnType, v: Option<FrameSlice<'frame>>| {
        let val = ensure_not_null(v)?;
        Ok(val.to_vec())
    }
);
impl_strict_type!(
    "blob",
    Bytes,
    ColumnType::Blob,
    |_typ: &'frame ColumnType, v: Option<FrameSlice<'frame>>| {
        let val = ensure_not_null_owned(v)?;
        Ok(val)
    }
);

// string

macro_rules! impl_string_type {
    ($t:ty, $conv:expr $(, $l:lifetime)?) => {
        impl_strict_type!(
            "ascii or text",
            $t,
            ColumnType::Ascii | ColumnType::Text,
            $conv
            $(, $l)?
        );
    };
}

impl_string_type!(
    &'a str,
    |typ: &'frame ColumnType, v: Option<FrameSlice<'frame>>| {
        let val = ensure_not_null(v)?;
        check_ascii(typ, val)?;
        Ok(std::str::from_utf8(val)?)
    },
    'a
);
impl_string_type!(
    String,
    |typ: &'frame ColumnType, v: Option<FrameSlice<'frame>>| {
        let val = ensure_not_null(v)?;
        check_ascii(typ, val)?;
        Ok(std::str::from_utf8(val)?.to_string())
    }
);

// TODO: Deserialization for string::String<Bytes>

fn check_ascii(typ: &ColumnType, s: &[u8]) -> Result<(), ParseError> {
    if matches!(typ, ColumnType::Ascii) && !s.is_ascii() {
        return Err(ParseError::BadIncomingData(
            "Expected a valid ASCII string".to_string(),
        ));
    }
    Ok(())
}

// counter

impl_strict_type!(
    "counter",
    Counter,
    ColumnType::Counter,
    |_typ: &'frame ColumnType, v: Option<FrameSlice<'frame>>| {
        let val = ensure_not_null(v)?;
        let arr = ensure_exact_length::<8>("counter", val)?;
        let counter = i64::from_be_bytes(arr);
        Ok(Counter(counter))
    }
);

// date and time types

// duration
impl_strict_type!(
    "duration",
    CqlDuration,
    ColumnType::Duration,
    |_typ: &'frame ColumnType, v: Option<FrameSlice<'frame>>| {
        let mut val = ensure_not_null(v)?;
        let months = i32::try_from(types::vint_decode(&mut val)?)?;
        let days = i32::try_from(types::vint_decode(&mut val)?)?;
        let nanoseconds = types::vint_decode(&mut val)?;

        Ok(CqlDuration {
            months,
            days,
            nanoseconds,
        })
    }
);

impl_strict_type!(
    "date",
    CqlDate,
    ColumnType::Date,
    |_typ: &'frame ColumnType, v: Option<FrameSlice<'frame>>| {
        let val = ensure_not_null(v)?;
        let arr = ensure_exact_length::<4>("date", val)?;
        let days = u32::from_be_bytes(arr);
        Ok(CqlDate(days))
    }
);

#[cfg(any(feature = "chrono", feature = "time"))]
fn get_days_since_epoch_from_date_column(v: Option<FrameSlice<'_>>) -> Result<i64, ParseError> {
    let val = ensure_not_null(v)?;
    let arr = ensure_exact_length::<4>("date", val)?;
    let days = u32::from_be_bytes(arr);
    let days_since_epoch = days as i64 - (1i64 << 31);
    Ok(days_since_epoch)
}

#[cfg(feature = "chrono")]
impl_strict_type!(
    "date",
    NaiveDate,
    ColumnType::Date,
    |_typ: &'frame ColumnType, v: Option<FrameSlice<'frame>>| {
        let fail = || {
            ParseError::BadIncomingData(
                "Value is out of representable range for NaiveDate".to_string(),
            )
        };
        let days_since_epoch =
            chrono::Duration::try_days(get_days_since_epoch_from_date_column(v)?)
                .ok_or_else(fail)?;
        NaiveDate::from_ymd_opt(1970, 1, 1)
            .unwrap()
            .checked_add_signed(days_since_epoch)
            .ok_or_else(fail)
    }
);

#[cfg(feature = "time")]
impl_strict_type!(
    "date",
    time::Date,
    ColumnType::Date,
    |_typ: &'frame ColumnType, v: Option<FrameSlice<'frame>>| {
        let days_since_epoch = time::Duration::days(get_days_since_epoch_from_date_column(v)?);
        time::Date::from_calendar_date(1970, time::Month::January, 1)
            .unwrap()
            .checked_add(days_since_epoch)
            .ok_or_else(|| {
                ParseError::BadIncomingData(
                    "Value is out of representable range for time::Date".to_string(),
                )
            })
    }
);

fn get_millis_from_timestamp_column(v: Option<FrameSlice<'_>>) -> Result<i64, ParseError> {
    let val = ensure_not_null(v)?;
    let arr = ensure_exact_length::<8>("timestamp", val)?;
    let millis = i64::from_be_bytes(arr);

    Ok(millis)
}

impl_strict_type!(
    "timestamp",
    CqlTimestamp,
    ColumnType::Timestamp,
    |_typ: &'frame ColumnType, v: Option<FrameSlice<'frame>>| {
        let millis = get_millis_from_timestamp_column(v)?;
        Ok(CqlTimestamp(millis))
    }
);

#[cfg(feature = "chrono")]
impl_strict_type!(
    "timestamp",
    DateTime<Utc>,
    ColumnType::Timestamp,
    |_typ: &'frame ColumnType, v: Option<FrameSlice<'frame>>| {
        let millis = get_millis_from_timestamp_column(v)?;
        match Utc.timestamp_millis_opt(millis) {
            chrono::LocalResult::Single(datetime) => Ok(datetime),
            _ => Err(ParseError::BadIncomingData(format!(
                "Timestamp {} is out of the representable range for DateTime<Utc>",
                millis
            ))),
        }
    }
);

#[cfg(feature = "time")]
impl_strict_type!(
    "timestamp",
    time::OffsetDateTime,
    ColumnType::Timestamp,
    |_typ: &'frame ColumnType, v: Option<FrameSlice<'frame>>| {
        let millis = get_millis_from_timestamp_column(v)?;
        time::OffsetDateTime::from_unix_timestamp_nanos(millis as i128 * 1_000_000).map_err(|_| {
            ParseError::BadIncomingData(format!(
                "Timestamp {} is out of the representable range for time::OffsetDateTime",
                millis
            ))
        })
    }
);

fn get_nanos_from_time_column(v: Option<FrameSlice<'_>>) -> Result<i64, ParseError> {
    let val = ensure_not_null(v)?;
    let arr = ensure_exact_length::<8>("date", val)?;
    let nanoseconds = i64::from_be_bytes(arr);

    // Valid values are in the range 0 to 86399999999999
    if !(0..=86399999999999).contains(&nanoseconds) {
        return Err(ParseError::BadIncomingData(format!(
            "Invalid time value; only 0 to 86399999999999 allowed: {}.",
            nanoseconds,
        )));
    }

    Ok(nanoseconds)
}

impl_strict_type!(
    "time",
    CqlTime,
    ColumnType::Time,
    |_typ: &'frame ColumnType, v: Option<FrameSlice<'frame>>| {
        let nanoseconds = get_nanos_from_time_column(v)?;

        Ok(CqlTime(nanoseconds))
    }
);

#[cfg(feature = "chrono")]
impl_strict_type!(
    "time",
    NaiveTime,
    ColumnType::Time,
    |_typ: &'frame ColumnType, v: Option<FrameSlice<'frame>>| {
        let nanoseconds = get_nanos_from_time_column(v)?;

        let naive_time: NaiveTime = CqlTime(nanoseconds).try_into().map_err(|err| {
            ParseError::BadIncomingData(format!(
                "Value is out of representable range for NaiveTime: {}",
                err
            ))
        })?;
        Ok(naive_time)
    }
);

#[cfg(feature = "time")]
impl_strict_type!(
    "time",
    time::Time,
    ColumnType::Time,
    |_typ: &'frame ColumnType, v: Option<FrameSlice<'frame>>| {
        let nanoseconds = get_nanos_from_time_column(v)?;

        let time: time::Time = CqlTime(nanoseconds).try_into().map_err(|err| {
            ParseError::BadIncomingData(format!(
                "Value is out of representable range for time::Time: {}",
                err
            ))
        })?;
        Ok(time)
    }
);

// inet

impl_strict_type!(
    "inet",
    IpAddr,
    ColumnType::Inet,
    |_typ: &'frame ColumnType, v: Option<FrameSlice<'frame>>| {
        let val = ensure_not_null(v)?;
        if let Ok(ipv4) = <[u8; 4]>::try_from(val) {
            Ok(IpAddr::from(ipv4))
        } else if let Ok(ipv16) = <[u8; 16]>::try_from(val) {
            Ok(IpAddr::from(ipv16))
        } else {
            Err(ParseError::BadIncomingData(format!(
                "Invalid inet bytes length: {}",
                val.len(),
            )))
        }
    }
);

// uuid

impl_strict_type!(
    "uuid",
    Uuid,
    ColumnType::Uuid,
    |_typ: &'frame ColumnType, v: Option<FrameSlice<'frame>>| {
        let val = ensure_not_null(v)?;
        let arr = ensure_exact_length::<16>("uuid", val)?;
        let i = u128::from_be_bytes(arr);
        Ok(uuid::Uuid::from_u128(i))
    }
);

impl_strict_type!(
    "timeuuid",
    CqlTimeuuid,
    ColumnType::Timeuuid,
    |_typ: &'frame ColumnType, v: Option<FrameSlice<'frame>>| {
        let val = ensure_not_null(v)?;
        let arr = ensure_exact_length::<16>("timeuuid", val)?;
        let i = u128::from_be_bytes(arr);
        Ok(CqlTimeuuid::from(uuid::Uuid::from_u128(i)))
    }
);

/// A value that may be empty or not.
///
/// In CQL, some types can have a special value of "empty", represented as
/// a serialized value of length 0. An example of this are integral types:
/// the "int" type can actually hold 2^32 + 1 possible values because of this
/// quirk. Note that this is distinct from being NULL.
///
/// `MaybeEmpty` was introduced to help support this quirk for Rust types
/// which can't represent the empty, additional value.
#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Clone, Copy)]
pub enum MaybeEmpty<T> {
    Empty,
    Value(T),
}

impl<'frame, T> DeserializeCql<'frame> for MaybeEmpty<T>
where
    T: DeserializeCql<'frame>,
{
    fn type_check(typ: &ColumnType) -> Result<(), ParseError> {
        <T as DeserializeCql<'frame>>::type_check(typ)
    }

    fn deserialize(
        typ: &'frame ColumnType,
        v: Option<FrameSlice<'frame>>,
    ) -> Result<Self, ParseError> {
        let val = ensure_not_null(v)?;
        if val.is_empty() {
            Ok(MaybeEmpty::Empty)
        } else {
            let v = <T as DeserializeCql<'frame>>::deserialize(typ, v)?;
            Ok(MaybeEmpty::Value(v))
        }
    }
}

// secrecy
#[cfg(feature = "secret")]
impl<'frame, T> DeserializeCql<'frame> for secrecy::Secret<T>
where
    T: DeserializeCql<'frame> + secrecy::Zeroize,
{
    fn type_check(typ: &ColumnType) -> Result<(), ParseError> {
        <T as DeserializeCql<'frame>>::type_check(typ)
    }

    fn deserialize(
        typ: &'frame ColumnType,
        v: Option<FrameSlice<'frame>>,
    ) -> Result<Self, ParseError> {
        <T as DeserializeCql<'frame>>::deserialize(typ, v).map(secrecy::Secret::new)
    }
}

// collections

// lists and sets

/// An iterator over either a CQL set or list.
pub struct ListlikeIterator<'frame, T> {
    elem_typ: &'frame ColumnType,
    raw_iter: FixedLengthBytesSequenceIterator<'frame>,
    phantom_data: std::marker::PhantomData<T>,
}

impl<'frame, T> ListlikeIterator<'frame, T> {
    pub fn new(elem_typ: &'frame ColumnType, count: usize, slice: FrameSlice<'frame>) -> Self {
        Self {
            elem_typ,
            raw_iter: FixedLengthBytesSequenceIterator::new(count, slice),
            phantom_data: std::marker::PhantomData,
        }
    }
}

impl<'frame, T> DeserializeCql<'frame> for ListlikeIterator<'frame, T>
where
    T: DeserializeCql<'frame>,
{
    fn type_check(typ: &ColumnType) -> Result<(), ParseError> {
        match typ {
            ColumnType::List(el_t) | ColumnType::Set(el_t) => {
                <T as DeserializeCql<'frame>>::type_check(el_t)
            }
            _ => Err(ParseError::BadIncomingData(format!(
                "Expected list or set, got {:?}",
                typ,
            ))),
        }
    }

    fn deserialize(
        typ: &'frame ColumnType,
        v: Option<FrameSlice<'frame>>,
    ) -> Result<Self, ParseError> {
        let v = ensure_not_null_slice(v)?;
        let mut mem = v.as_slice();
        let count = types::read_int_length(&mut mem)?;
        let elem_typ = match typ {
            ColumnType::List(elem_typ) | ColumnType::Set(elem_typ) => elem_typ,
            _ => {
                return Err(ParseError::BadIncomingData(format!(
                    "Expected list or set, got {:?}",
                    typ,
                )))
            }
        };
        Ok(Self::new(
            elem_typ,
            count,
            FrameSlice::new_subslice(mem, v.as_bytes_ref()),
        ))
    }
}

impl<'frame, T> Iterator for ListlikeIterator<'frame, T>
where
    T: DeserializeCql<'frame>,
{
    type Item = Result<T, ParseError>;

    fn next(&mut self) -> Option<Self::Item> {
        let raw = self.raw_iter.next()?;
        Some(raw.and_then(|raw| T::deserialize(self.elem_typ, raw)))
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.raw_iter.size_hint()
    }
}

impl<'frame, T> DeserializeCql<'frame> for Vec<T>
where
    T: DeserializeCql<'frame>,
{
    fn type_check(typ: &ColumnType) -> Result<(), ParseError> {
        // It makes sense for both Set and List to deserialize to Vec.
        ListlikeIterator::<'frame, T>::type_check(typ)
    }

    fn deserialize(
        typ: &'frame ColumnType,
        v: Option<FrameSlice<'frame>>,
    ) -> Result<Self, ParseError> {
        ListlikeIterator::<'frame, T>::deserialize(typ, v)?.collect()
    }
}

impl<'frame, T> DeserializeCql<'frame> for BTreeSet<T>
where
    T: DeserializeCql<'frame> + Ord,
{
    fn type_check(typ: &ColumnType) -> Result<(), ParseError> {
        // It only makes sense for Set to deserialize to BTreeSet.
        // Deserializing List straight to BTreeSet would be lossy.
        match typ {
            ColumnType::Set(el_t) => <T as DeserializeCql<'frame>>::type_check(el_t),
            _ => Err(ParseError::BadIncomingData(format!(
                "Expected set, got {:?}",
                typ,
            ))),
        }
    }

    fn deserialize(
        typ: &'frame ColumnType,
        v: Option<FrameSlice<'frame>>,
    ) -> Result<Self, ParseError> {
        ListlikeIterator::<'frame, T>::deserialize(typ, v)?.collect()
    }
}

impl<'frame, T, S> DeserializeCql<'frame> for HashSet<T, S>
where
    T: DeserializeCql<'frame> + Eq + Hash,
    S: BuildHasher + Default + 'frame,
{
    fn type_check(typ: &ColumnType) -> Result<(), ParseError> {
        // It only makes sense for Set to deserialize to HashSet.
        // Deserializing List straight to HashSet would be lossy.
        match typ {
            ColumnType::Set(el_t) => <T as DeserializeCql<'frame>>::type_check(el_t),
            _ => Err(ParseError::BadIncomingData(format!(
                "Expected set, got {:?}",
                typ,
            ))),
        }
    }

    fn deserialize(
        typ: &'frame ColumnType,
        v: Option<FrameSlice<'frame>>,
    ) -> Result<Self, ParseError> {
        ListlikeIterator::<'frame, T>::deserialize(typ, v)?.collect()
    }
}

/// An iterator over a CQL map.
pub struct MapIterator<'frame, K, V> {
    k_typ: &'frame ColumnType,
    v_typ: &'frame ColumnType,
    raw_iter: FixedLengthBytesSequenceIterator<'frame>,
    phantom_data_k: std::marker::PhantomData<K>,
    phantom_data_v: std::marker::PhantomData<V>,
}

impl<'frame, K, V> MapIterator<'frame, K, V> {
    pub fn new(
        k_typ: &'frame ColumnType,
        v_typ: &'frame ColumnType,
        count: usize,
        slice: FrameSlice<'frame>,
    ) -> Self {
        Self {
            k_typ,
            v_typ,
            raw_iter: FixedLengthBytesSequenceIterator::new(count, slice),
            phantom_data_k: std::marker::PhantomData,
            phantom_data_v: std::marker::PhantomData,
        }
    }
}

impl<'frame, K, V> DeserializeCql<'frame> for MapIterator<'frame, K, V>
where
    K: DeserializeCql<'frame>,
    V: DeserializeCql<'frame>,
{
    fn type_check(typ: &ColumnType) -> Result<(), ParseError> {
        match typ {
            ColumnType::Map(k_t, v_t) => {
                <K as DeserializeCql<'frame>>::type_check(k_t)?;
                <V as DeserializeCql<'frame>>::type_check(v_t)?;
                Ok(())
            }
            _ => Err(ParseError::BadIncomingData(format!(
                "Expected map, got {:?}",
                typ,
            ))),
        }
    }

    fn deserialize(
        typ: &'frame ColumnType,
        v: Option<FrameSlice<'frame>>,
    ) -> Result<Self, ParseError> {
        let v = ensure_not_null_slice(v)?;
        let mut mem = v.as_slice();
        let count = types::read_int_length(&mut mem)?;
        let (k_typ, v_typ) = match typ {
            ColumnType::Map(k_t, v_t) => (k_t, v_t),
            _ => {
                return Err(ParseError::BadIncomingData(format!(
                    "Expected map, got {:?}",
                    typ,
                )))
            }
        };
        Ok(Self::new(
            k_typ,
            v_typ,
            2 * count,
            FrameSlice::new_subslice(mem, v.as_bytes_ref()),
        ))
    }
}

impl<'frame, K, V> Iterator for MapIterator<'frame, K, V>
where
    K: DeserializeCql<'frame>,
    V: DeserializeCql<'frame>,
{
    type Item = Result<(K, V), ParseError>;

    fn next(&mut self) -> Option<Self::Item> {
        let raw_k = match self.raw_iter.next() {
            Some(Ok(raw_k)) => raw_k,
            Some(Err(err)) => return Some(Err(err)),
            None => return None,
        };
        let raw_v = match self.raw_iter.next() {
            Some(Ok(raw_v)) => raw_v,
            Some(Err(err)) => return Some(Err(err)),
            None => return None,
        };
        let do_next = || -> Result<(K, V), ParseError> {
            let k = K::deserialize(self.k_typ, raw_k)?;
            let v = V::deserialize(self.v_typ, raw_v)?;
            Ok((k, v))
        };
        do_next().map(Some).transpose()
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.raw_iter.size_hint()
    }
}

impl<'frame, K, V> DeserializeCql<'frame> for BTreeMap<K, V>
where
    K: DeserializeCql<'frame> + Ord,
    V: DeserializeCql<'frame>,
{
    fn type_check(typ: &ColumnType) -> Result<(), ParseError> {
        MapIterator::<'frame, K, V>::type_check(typ)
    }

    fn deserialize(
        typ: &'frame ColumnType,
        v: Option<FrameSlice<'frame>>,
    ) -> Result<Self, ParseError> {
        MapIterator::<'frame, K, V>::deserialize(typ, v)?.collect()
    }
}

impl<'frame, K, V, S> DeserializeCql<'frame> for HashMap<K, V, S>
where
    K: DeserializeCql<'frame> + Eq + Hash,
    V: DeserializeCql<'frame>,
    S: BuildHasher + Default + 'frame,
{
    fn type_check(typ: &ColumnType) -> Result<(), ParseError> {
        MapIterator::<'frame, K, V>::type_check(typ)
    }

    fn deserialize(
        typ: &'frame ColumnType,
        v: Option<FrameSlice<'frame>>,
    ) -> Result<Self, ParseError> {
        MapIterator::<'frame, K, V>::deserialize(typ, v)?.collect()
    }
}

// Utilities

fn ensure_not_null(v: Option<FrameSlice>) -> Result<&[u8], ParseError> {
    match v {
        Some(v) => Ok(v.as_slice()),
        None => Err(ParseError::BadIncomingData(
            "Expected a non-null value".to_string(),
        )),
    }
}

fn ensure_not_null_owned(v: Option<FrameSlice>) -> Result<Bytes, ParseError> {
    match v {
        Some(v) => Ok(v.to_bytes()),
        None => Err(ParseError::BadIncomingData(
            "Expected a non-null value".to_string(),
        )),
    }
}

fn ensure_not_null_slice(v: Option<FrameSlice>) -> Result<FrameSlice, ParseError> {
    match v {
        Some(v) => Ok(v),
        None => Err(ParseError::BadIncomingData(
            "Expected a non-null value".to_string(),
        )),
    }
}

fn ensure_exact_length<const SIZE: usize>(
    cql_name: &str,
    v: &[u8],
) -> Result<[u8; SIZE], ParseError> {
    v.try_into().map_err(|_| {
        ParseError::BadIncomingData(format!(
            "The type {} requires {} bytes, but got {}",
            cql_name,
            SIZE,
            v.len(),
        ))
    })
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
    pub fn new(count: usize, slice: FrameSlice<'frame>) -> Self {
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

#[cfg(test)]
mod tests {
    use bytes::{BufMut, Bytes, BytesMut};

    #[cfg(feature = "chrono")]
    use chrono::{DateTime, NaiveDate, NaiveTime, Utc};
    use uuid::Uuid;

    use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
    use std::fmt::Debug;
    use std::net::{IpAddr, Ipv6Addr};

    use crate::frame::frame_errors::ParseError;
    use crate::frame::response::cql_to_rust::FromCqlVal;
    use crate::frame::response::result::{deser_cql_value, ColumnType, CqlValue};
    use crate::frame::types;
    use crate::frame::value::{
        Counter, CqlDate, CqlDuration, CqlTime, CqlTimestamp, CqlTimeuuid, CqlVarint,
    };
    use crate::types::deserialize::value::{ListlikeIterator, MapIterator, MaybeEmpty};
    use crate::types::deserialize::FrameSlice;
    use crate::types::serialize::value::SerializeCql;
    use crate::types::serialize::CellWriter;

    use super::DeserializeCql;

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
    fn test_floating_point() {
        let float = make_bytes(&[63, 0, 0, 0]);
        let decoded_float = deserialize::<f32>(&ColumnType::Float, &float).unwrap();
        assert_eq!(decoded_float, 0.5);

        let double = make_bytes(&[64, 0, 0, 0, 0, 0, 0, 0]);
        let decoded_double = deserialize::<f64>(&ColumnType::Double, &double).unwrap();
        assert_eq!(decoded_double, 2.0);
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
        let num1 = PI_STR[2..].to_vec();
        let num2 = vec![b'-']
            .into_iter()
            .chain(PI_STR[2..].iter().copied())
            .collect::<Vec<_>>();
        let num3 = b"0".to_vec();

        // native - CqlVarint
        {
            let num1 = CqlVarint::from_signed_bytes_be(num1.clone());
            let num2 = CqlVarint::from_signed_bytes_be(num2.clone());
            let num3 = CqlVarint::from_signed_bytes_be(num3.clone());
            compat_check_serialized::<CqlVarint>(&ColumnType::Varint, &num1);
            compat_check_serialized::<CqlVarint>(&ColumnType::Varint, &num2);
            compat_check_serialized::<CqlVarint>(&ColumnType::Varint, &num3);
        }

        #[cfg(feature = "num-bigint-03")]
        {
            use num_bigint_03::BigInt;

            let num1 = BigInt::parse_bytes(&num1, 10).unwrap();
            let num2 = BigInt::parse_bytes(&num2, 10).unwrap();
            let num3 = BigInt::parse_bytes(&num3, 10).unwrap();
            compat_check_serialized::<BigInt>(&ColumnType::Varint, &num1);
            compat_check_serialized::<BigInt>(&ColumnType::Varint, &num2);
            compat_check_serialized::<BigInt>(&ColumnType::Varint, &num3);
        }

        #[cfg(feature = "num-bigint-04")]
        {
            use num_bigint_04::BigInt;

            let num1 = BigInt::parse_bytes(&num1, 10).unwrap();
            let num2 = BigInt::parse_bytes(&num2, 10).unwrap();
            let num3 = BigInt::parse_bytes(&num3, 10).unwrap();
            compat_check_serialized::<BigInt>(&ColumnType::Varint, &num1);
            compat_check_serialized::<BigInt>(&ColumnType::Varint, &num2);
            compat_check_serialized::<BigInt>(&ColumnType::Varint, &num3);
        }

        // big decimals
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
            compat_check::<NaiveDate>(&ColumnType::Date, make_bytes(&date1));
            compat_check::<NaiveDate>(&ColumnType::Date, make_bytes(&date2));
            compat_check::<NaiveDate>(&ColumnType::Date, make_bytes(&date3));
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
            compat_check_serialized::<NaiveTime>(&ColumnType::Time, &time1);
            compat_check_serialized::<NaiveTime>(&ColumnType::Time, &time2);
            compat_check_serialized::<NaiveTime>(&ColumnType::Time, &time3);
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
            compat_check_serialized::<DateTime<Utc>>(&ColumnType::Timestamp, &timestamp1);
            compat_check_serialized::<DateTime<Utc>>(&ColumnType::Timestamp, &timestamp2);
            compat_check_serialized::<DateTime<Utc>>(&ColumnType::Timestamp, &timestamp3);
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
        let uuid1 = Uuid::new_v4();
        let uuid2 = Uuid::new_v4();
        let uuid3 = Uuid::new_v4();
        compat_check_serialized::<Uuid>(&ColumnType::Uuid, &uuid1);
        compat_check_serialized::<Uuid>(&ColumnType::Uuid, &uuid2);
        compat_check_serialized::<Uuid>(&ColumnType::Uuid, &uuid3);
        compat_check_serialized::<CqlTimeuuid>(&ColumnType::Timeuuid, &CqlTimeuuid::from(uuid1));
        compat_check_serialized::<CqlTimeuuid>(&ColumnType::Timeuuid, &CqlTimeuuid::from(uuid2));
        compat_check_serialized::<CqlTimeuuid>(&ColumnType::Timeuuid, &CqlTimeuuid::from(uuid3));

        // empty values
        // ...are implemented via MaybeEmpty and are handled in other tests

        // nulls, represented via option
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

    // Checks that both new and old serialization framework
    // produces the same results in this case
    fn compat_check<T>(typ: &ColumnType, raw: Bytes)
    where
        T: for<'f> DeserializeCql<'f>,
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

    fn compat_check_serialized<T>(typ: &ColumnType, val: &dyn SerializeCql)
    where
        T: for<'f> DeserializeCql<'f>,
        T: FromCqlVal<Option<CqlValue>>,
        T: Debug + PartialEq,
    {
        let raw = serialize(typ, val);
        compat_check::<T>(typ, raw);
    }

    fn deserialize<'frame, T>(typ: &'frame ColumnType, byts: &'frame Bytes) -> Result<T, ParseError>
    where
        T: DeserializeCql<'frame>,
    {
        <T as DeserializeCql<'frame>>::type_check(typ)?;
        let mut buf = byts.as_ref();
        let cell = types::read_bytes_opt(&mut buf)?;
        let value = cell.map(|cell| FrameSlice::new_subslice(cell, byts));
        <T as DeserializeCql<'frame>>::deserialize(typ, value)
    }

    fn make_bytes(cell: &[u8]) -> Bytes {
        let mut b = BytesMut::new();
        append_bytes(&mut b, cell);
        b.freeze()
    }

    fn serialize(typ: &ColumnType, value: &dyn SerializeCql) -> Bytes {
        let mut v = Vec::new();
        let writer = CellWriter::new(&mut v);
        value.serialize(typ, writer).unwrap();
        v.into()
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
