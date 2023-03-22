use crate::frame::types;
use bigdecimal::BigDecimal;
use bytes::BufMut;
use chrono::prelude::*;
use chrono::Duration;
use num_bigint::BigInt;
use std::borrow::Cow;
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::convert::TryInto;
use std::net::IpAddr;
use thiserror::Error;
use uuid::Uuid;

use super::response::result::CqlValue;
use super::types::vint_encode;

#[cfg(feature = "secret")]
use secrecy::{ExposeSecret, Secret, Zeroize};

/// Every value being sent in a query must implement this trait
/// serialize() should write the Value as [bytes] to the provided buffer
pub trait Value {
    fn serialize(&self, buf: &mut Vec<u8>) -> Result<(), ValueTooBig>;
}

#[derive(Debug, Error, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
#[error("Value too big to be sent in a request - max 2GiB allowed")]
pub struct ValueTooBig;

/// Represents an unset value
pub struct Unset;

/// Represents an counter value
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct Counter(pub i64);

/// Enum providing a way to represent a value that might be unset
#[derive(Clone, Copy)]
pub enum MaybeUnset<V: Value> {
    Unset,
    Set(V),
}

/// Wrapper that allows to send dates outside of NaiveDate range (-262145-1-1 to 262143-12-31)
/// Days since -5877641-06-23 i.e. 2^31 days before unix epoch
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct Date(pub u32);

/// Wrapper used to differentiate between Time and Timestamp as sending values
/// Milliseconds since unix epoch
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct Timestamp(pub Duration);

/// Wrapper used to differentiate between Time and Timestamp as sending values
/// Nanoseconds since midnight
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct Time(pub Duration);

/// Keeps a buffer with serialized Values
/// Allows adding new Values and iterating over serialized ones
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct SerializedValues {
    serialized_values: Vec<u8>,
    values_num: i16,
    contains_names: bool,
}

/// Represents a CQL Duration value
#[derive(Clone, Debug, Copy, PartialEq, Eq)]
pub struct CqlDuration {
    pub months: i32,
    pub days: i32,
    pub nanoseconds: i64,
}

#[derive(Debug, Error, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum SerializeValuesError {
    #[error("Too many values to add, max 32 767 values can be sent in a request")]
    TooManyValues,
    #[error("Mixing named and not named values is not allowed")]
    MixingNamedAndNotNamedValues,
    #[error(transparent)]
    ValueTooBig(#[from] ValueTooBig),
    #[error("Parsing serialized values failed")]
    ParseError,
}

pub type SerializedResult<'a> = Result<Cow<'a, SerializedValues>, SerializeValuesError>;

/// Represents list of values to be sent in a query
/// gets serialized and but into request
pub trait ValueList {
    /// Provides a view of ValueList as SerializedValues
    /// returns `Cow<SerializedValues>` to make impl ValueList for SerializedValues efficient
    fn serialized(&self) -> SerializedResult<'_>;

    fn write_to_request(&self, buf: &mut impl BufMut) -> Result<(), SerializeValuesError> {
        let serialized = self.serialized()?;
        SerializedValues::write_to_request(&serialized, buf);

        Ok(())
    }
}

impl SerializedValues {
    /// Creates empty value list
    pub const fn new() -> Self {
        SerializedValues {
            serialized_values: Vec::new(),
            values_num: 0,
            contains_names: false,
        }
    }

    pub fn with_capacity(capacity: usize) -> Self {
        SerializedValues {
            serialized_values: Vec::with_capacity(capacity),
            values_num: 0,
            contains_names: false,
        }
    }

    pub fn has_names(&self) -> bool {
        self.contains_names
    }

    /// A const empty instance, useful for taking references
    pub const EMPTY: &'static SerializedValues = &SerializedValues::new();

    /// Serializes value and appends it to the list
    pub fn add_value(&mut self, val: &impl Value) -> Result<(), SerializeValuesError> {
        if self.contains_names {
            return Err(SerializeValuesError::MixingNamedAndNotNamedValues);
        }
        if self.values_num == i16::MAX {
            return Err(SerializeValuesError::TooManyValues);
        }

        let len_before_serialize: usize = self.serialized_values.len();

        if let Err(e) = val.serialize(&mut self.serialized_values) {
            self.serialized_values.resize(len_before_serialize, 0);
            return Err(SerializeValuesError::from(e));
        }

        self.values_num += 1;
        Ok(())
    }

    pub fn add_named_value(
        &mut self,
        name: &str,
        val: &impl Value,
    ) -> Result<(), SerializeValuesError> {
        if self.values_num > 0 && !self.contains_names {
            return Err(SerializeValuesError::MixingNamedAndNotNamedValues);
        }
        self.contains_names = true;
        if self.values_num == i16::MAX {
            return Err(SerializeValuesError::TooManyValues);
        }

        let len_before_serialize: usize = self.serialized_values.len();

        types::write_string(name, &mut self.serialized_values)
            .map_err(|_| SerializeValuesError::ParseError)?;

        if let Err(e) = val.serialize(&mut self.serialized_values) {
            self.serialized_values.resize(len_before_serialize, 0);
            return Err(SerializeValuesError::from(e));
        }

        self.values_num += 1;
        Ok(())
    }

    pub fn iter(&self) -> impl Iterator<Item = Option<&[u8]>> {
        SerializedValuesIterator {
            serialized_values: &self.serialized_values,
            contains_names: self.contains_names,
        }
    }

    pub fn write_to_request(&self, buf: &mut impl BufMut) {
        buf.put_i16(self.values_num);
        buf.put(&self.serialized_values[..]);
    }

    pub fn is_empty(&self) -> bool {
        self.values_num == 0
    }

    pub fn len(&self) -> i16 {
        self.values_num
    }

    pub fn size(&self) -> usize {
        self.serialized_values.len()
    }
}

#[derive(Clone, Copy)]
pub struct SerializedValuesIterator<'a> {
    serialized_values: &'a [u8],
    contains_names: bool,
}

impl<'a> Iterator for SerializedValuesIterator<'a> {
    type Item = Option<&'a [u8]>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.serialized_values.is_empty() {
            return None;
        }

        // In case of named values, skip names
        if self.contains_names {
            types::read_short_bytes(&mut self.serialized_values).expect("badly encoded value name");
        }

        Some(types::read_bytes_opt(&mut self.serialized_values).expect("badly encoded value"))
    }
}

/// Represents List of ValueList for Batch statement
pub trait BatchValues {
    /// For some unknown reason, this type, when not resolved to a concrete type for a given async function,
    /// cannot live across await boundaries while maintaining the corresponding future `Send`, unless `'r: 'static`
    ///
    /// See <https://github.com/scylladb/scylla-rust-driver/issues/599> for more details
    type BatchValuesIter<'r>: BatchValuesIterator<'r>
    where
        Self: 'r;
    fn batch_values_iter(&self) -> Self::BatchValuesIter<'_>;
}

/// An iterator-like for `ValueList`
///
/// An instance of this can be easily obtained from `IT: Iterator<Item: ValueList>`: that would be
/// `BatchValuesIteratorFromIterator<IT>`
///
/// It's just essentially making methods from `ValueList` accessible instead of being an actual iterator because of
/// compiler limitations that would otherwise be very complex to overcome.
/// (specifically, types being different would require yielding enums for tuple impls)
pub trait BatchValuesIterator<'a> {
    fn next_serialized(&mut self) -> Option<SerializedResult<'a>>;
    fn write_next_to_request(
        &mut self,
        buf: &mut impl BufMut,
    ) -> Option<Result<(), SerializeValuesError>>;
    fn skip_next(&mut self) -> Option<()>;
}

/// Implements `BatchValuesIterator` from an `Iterator` over references to things that implement `ValueList`
///
/// Essentially used internally by this lib to provide implementors of `BatchValuesIterator` for cases
/// that always serialize the same concrete `ValueList` type
pub struct BatchValuesIteratorFromIterator<IT: Iterator> {
    it: IT,
}

impl<'r, 'a: 'r, IT, VL> BatchValuesIterator<'r> for BatchValuesIteratorFromIterator<IT>
where
    IT: Iterator<Item = &'a VL>,
    VL: ValueList + 'a,
{
    fn next_serialized(&mut self) -> Option<SerializedResult<'r>> {
        self.it.next().map(|vl| vl.serialized())
    }
    fn write_next_to_request(
        &mut self,
        buf: &mut impl BufMut,
    ) -> Option<Result<(), SerializeValuesError>> {
        self.it.next().map(|vl| vl.write_to_request(buf))
    }
    fn skip_next(&mut self) -> Option<()> {
        self.it.next().map(|_| ())
    }
}

impl<IT> From<IT> for BatchValuesIteratorFromIterator<IT>
where
    IT: Iterator,
    IT::Item: ValueList,
{
    fn from(it: IT) -> Self {
        BatchValuesIteratorFromIterator { it }
    }
}

//
//  Value impls
//

// Implement Value for primitive types
impl Value for i8 {
    fn serialize(&self, buf: &mut Vec<u8>) -> Result<(), ValueTooBig> {
        buf.put_i32(1);
        buf.put_i8(*self);
        Ok(())
    }
}

impl Value for i16 {
    fn serialize(&self, buf: &mut Vec<u8>) -> Result<(), ValueTooBig> {
        buf.put_i32(2);
        buf.put_i16(*self);
        Ok(())
    }
}

impl Value for i32 {
    fn serialize(&self, buf: &mut Vec<u8>) -> Result<(), ValueTooBig> {
        buf.put_i32(4);
        buf.put_i32(*self);
        Ok(())
    }
}

impl Value for i64 {
    fn serialize(&self, buf: &mut Vec<u8>) -> Result<(), ValueTooBig> {
        buf.put_i32(8);
        buf.put_i64(*self);
        Ok(())
    }
}

impl Value for BigDecimal {
    fn serialize(&self, buf: &mut Vec<u8>) -> Result<(), ValueTooBig> {
        let (value, scale) = self.as_bigint_and_exponent();

        let serialized = value.to_signed_bytes_be();
        let serialized_len: i32 = serialized.len().try_into().map_err(|_| ValueTooBig)?;

        buf.put_i32(serialized_len + 4);
        buf.put_i32(scale.try_into().map_err(|_| ValueTooBig)?);
        buf.extend_from_slice(&serialized);

        Ok(())
    }
}

impl Value for NaiveDate {
    fn serialize(&self, buf: &mut Vec<u8>) -> Result<(), ValueTooBig> {
        buf.put_i32(4);
        let unix_epoch = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();

        let days: u32 = self
            .signed_duration_since(unix_epoch)
            .num_days()
            .checked_add(1 << 31)
            .and_then(|days| days.try_into().ok()) // convert to u32
            .ok_or(ValueTooBig)?;

        buf.put_u32(days);
        Ok(())
    }
}

impl Value for Date {
    fn serialize(&self, buf: &mut Vec<u8>) -> Result<(), ValueTooBig> {
        buf.put_i32(4);
        buf.put_u32(self.0);
        Ok(())
    }
}

impl Value for Timestamp {
    fn serialize(&self, buf: &mut Vec<u8>) -> Result<(), ValueTooBig> {
        buf.put_i32(8);
        buf.put_i64(self.0.num_milliseconds());
        Ok(())
    }
}

impl Value for Time {
    fn serialize(&self, buf: &mut Vec<u8>) -> Result<(), ValueTooBig> {
        buf.put_i32(8);
        buf.put_i64(self.0.num_nanoseconds().ok_or(ValueTooBig)?);
        Ok(())
    }
}

impl Value for DateTime<Utc> {
    fn serialize(&self, buf: &mut Vec<u8>) -> Result<(), ValueTooBig> {
        buf.put_i32(8);
        buf.put_i64(self.timestamp_millis());
        Ok(())
    }
}

#[cfg(feature = "secret")]
impl<V: Value + Zeroize> Value for Secret<V> {
    fn serialize(&self, buf: &mut Vec<u8>) -> Result<(), ValueTooBig> {
        self.expose_secret().serialize(buf)
    }
}

impl Value for bool {
    fn serialize(&self, buf: &mut Vec<u8>) -> Result<(), ValueTooBig> {
        buf.put_i32(1);
        let false_bytes: &[u8] = &[0x00];
        let true_bytes: &[u8] = &[0x01];
        if *self {
            buf.put(true_bytes);
        } else {
            buf.put(false_bytes);
        }

        Ok(())
    }
}

impl Value for f32 {
    fn serialize(&self, buf: &mut Vec<u8>) -> Result<(), ValueTooBig> {
        buf.put_i32(4);
        buf.put_f32(*self);
        Ok(())
    }
}

impl Value for f64 {
    fn serialize(&self, buf: &mut Vec<u8>) -> Result<(), ValueTooBig> {
        buf.put_i32(8);
        buf.put_f64(*self);
        Ok(())
    }
}

impl Value for Uuid {
    fn serialize(&self, buf: &mut Vec<u8>) -> Result<(), ValueTooBig> {
        buf.put_i32(16);
        buf.extend_from_slice(self.as_bytes());
        Ok(())
    }
}

impl Value for BigInt {
    fn serialize(&self, buf: &mut Vec<u8>) -> Result<(), ValueTooBig> {
        let serialized = self.to_signed_bytes_be();
        let serialized_len: i32 = serialized.len().try_into().map_err(|_| ValueTooBig)?;

        buf.put_i32(serialized_len);
        buf.extend_from_slice(&serialized);

        Ok(())
    }
}

impl Value for &str {
    fn serialize(&self, buf: &mut Vec<u8>) -> Result<(), ValueTooBig> {
        let str_bytes: &[u8] = self.as_bytes();
        let val_len: i32 = str_bytes.len().try_into().map_err(|_| ValueTooBig)?;

        buf.put_i32(val_len);
        buf.put(str_bytes);

        Ok(())
    }
}

impl Value for Vec<u8> {
    fn serialize(&self, buf: &mut Vec<u8>) -> Result<(), ValueTooBig> {
        let val_len: i32 = self.len().try_into().map_err(|_| ValueTooBig)?;
        buf.put_i32(val_len);

        buf.extend_from_slice(self);

        Ok(())
    }
}

impl Value for IpAddr {
    fn serialize(&self, buf: &mut Vec<u8>) -> Result<(), ValueTooBig> {
        match self {
            IpAddr::V4(addr) => {
                buf.put_i32(4);
                buf.extend_from_slice(&addr.octets());
            }
            IpAddr::V6(addr) => {
                buf.put_i32(16);
                buf.extend_from_slice(&addr.octets());
            }
        }

        Ok(())
    }
}

impl Value for String {
    fn serialize(&self, buf: &mut Vec<u8>) -> Result<(), ValueTooBig> {
        <&str as Value>::serialize(&self.as_str(), buf)
    }
}

/// Every `Option<T>` can be serialized as None -> NULL, Some(val) -> val.serialize()
impl<T: Value> Value for Option<T> {
    fn serialize(&self, buf: &mut Vec<u8>) -> Result<(), ValueTooBig> {
        match self {
            Some(val) => <T as Value>::serialize(val, buf),
            None => {
                buf.put_i32(-1);
                Ok(())
            }
        }
    }
}

impl Value for Unset {
    fn serialize(&self, buf: &mut Vec<u8>) -> Result<(), ValueTooBig> {
        // Unset serializes itself to empty value with length = -2
        buf.put_i32(-2);
        Ok(())
    }
}

impl Value for Counter {
    fn serialize(&self, buf: &mut Vec<u8>) -> Result<(), ValueTooBig> {
        self.0.serialize(buf)
    }
}

impl Value for CqlDuration {
    fn serialize(&self, buf: &mut Vec<u8>) -> Result<(), ValueTooBig> {
        let bytes_num_pos: usize = buf.len();
        buf.put_i32(0);

        vint_encode(self.months as i64, buf);
        vint_encode(self.days as i64, buf);
        vint_encode(self.nanoseconds, buf);

        let written_bytes: usize = buf.len() - bytes_num_pos - 4;
        let written_bytes_i32: i32 = written_bytes.try_into().map_err(|_| ValueTooBig)?;
        buf[bytes_num_pos..(bytes_num_pos + 4)].copy_from_slice(&written_bytes_i32.to_be_bytes());

        Ok(())
    }
}

impl<V: Value> Value for MaybeUnset<V> {
    fn serialize(&self, buf: &mut Vec<u8>) -> Result<(), ValueTooBig> {
        match self {
            MaybeUnset::Set(v) => v.serialize(buf),
            MaybeUnset::Unset => Unset.serialize(buf),
        }
    }
}

// Every &impl Value and &dyn Value should also implement Value
impl<T: Value + ?Sized> Value for &T {
    fn serialize(&self, buf: &mut Vec<u8>) -> Result<(), ValueTooBig> {
        <T as Value>::serialize(*self, buf)
    }
}

// Every Boxed Value should also implement Value
impl<T: Value + ?Sized> Value for Box<T> {
    fn serialize(&self, buf: &mut Vec<u8>) -> Result<(), ValueTooBig> {
        <T as Value>::serialize(self.as_ref(), buf)
    }
}

fn serialize_map<K: Value, V: Value>(
    kv_iter: impl Iterator<Item = (K, V)>,
    kv_count: usize,
    buf: &mut Vec<u8>,
) -> Result<(), ValueTooBig> {
    let bytes_num_pos: usize = buf.len();
    buf.put_i32(0);

    buf.put_i32(kv_count.try_into().map_err(|_| ValueTooBig)?);
    for (key, value) in kv_iter {
        <K as Value>::serialize(&key, buf)?;
        <V as Value>::serialize(&value, buf)?;
    }

    let written_bytes: usize = buf.len() - bytes_num_pos - 4;
    let written_bytes_i32: i32 = written_bytes.try_into().map_err(|_| ValueTooBig)?;
    buf[bytes_num_pos..(bytes_num_pos + 4)].copy_from_slice(&written_bytes_i32.to_be_bytes());

    Ok(())
}

fn serialize_list_or_set<'a, V: 'a + Value>(
    elements_iter: impl Iterator<Item = &'a V>,
    element_count: usize,
    buf: &mut Vec<u8>,
) -> Result<(), ValueTooBig> {
    let bytes_num_pos: usize = buf.len();
    buf.put_i32(0);

    buf.put_i32(element_count.try_into().map_err(|_| ValueTooBig)?);
    for value in elements_iter {
        <V as Value>::serialize(value, buf)?;
    }

    let written_bytes: usize = buf.len() - bytes_num_pos - 4;
    let written_bytes_i32: i32 = written_bytes.try_into().map_err(|_| ValueTooBig)?;
    buf[bytes_num_pos..(bytes_num_pos + 4)].copy_from_slice(&written_bytes_i32.to_be_bytes());

    Ok(())
}

impl<V: Value> Value for HashSet<V> {
    fn serialize(&self, buf: &mut Vec<u8>) -> Result<(), ValueTooBig> {
        serialize_list_or_set(self.iter(), self.len(), buf)
    }
}

impl<K: Value, V: Value> Value for HashMap<K, V> {
    fn serialize(&self, buf: &mut Vec<u8>) -> Result<(), ValueTooBig> {
        serialize_map(self.iter(), self.len(), buf)
    }
}

impl<V: Value> Value for BTreeSet<V> {
    fn serialize(&self, buf: &mut Vec<u8>) -> Result<(), ValueTooBig> {
        serialize_list_or_set(self.iter(), self.len(), buf)
    }
}

impl<K: Value, V: Value> Value for BTreeMap<K, V> {
    fn serialize(&self, buf: &mut Vec<u8>) -> Result<(), ValueTooBig> {
        serialize_map(self.iter(), self.len(), buf)
    }
}

impl<T: Value> Value for Vec<T> {
    fn serialize(&self, buf: &mut Vec<u8>) -> Result<(), ValueTooBig> {
        serialize_list_or_set(self.iter(), self.len(), buf)
    }
}

impl<T: Value> Value for &[T] {
    fn serialize(&self, buf: &mut Vec<u8>) -> Result<(), ValueTooBig> {
        serialize_list_or_set(self.iter(), self.len(), buf)
    }
}

fn serialize_tuple<V: Value>(
    elem_iter: impl Iterator<Item = V>,
    buf: &mut Vec<u8>,
) -> Result<(), ValueTooBig> {
    let bytes_num_pos: usize = buf.len();
    buf.put_i32(0);

    for elem in elem_iter {
        elem.serialize(buf)?;
    }

    let written_bytes: usize = buf.len() - bytes_num_pos - 4;
    let written_bytes_i32: i32 = written_bytes.try_into().map_err(|_| ValueTooBig)?;
    buf[bytes_num_pos..(bytes_num_pos + 4)].copy_from_slice(&written_bytes_i32.to_be_bytes());

    Ok(())
}

fn serialize_empty(buf: &mut Vec<u8>) -> Result<(), ValueTooBig> {
    buf.put_i32(0);
    Ok(())
}

impl Value for CqlValue {
    fn serialize(&self, buf: &mut Vec<u8>) -> Result<(), ValueTooBig> {
        match self {
            CqlValue::Map(m) => serialize_map(m.iter().map(|(k, v)| (k, v)), m.len(), buf),
            CqlValue::Tuple(t) => serialize_tuple(t.iter(), buf),

            // A UDT value is composed of successive [bytes] values, one for each field of the UDT
            // value (in the order defined by the type), so they serialize in a same way tuples do.
            CqlValue::UserDefinedType { fields, .. } => {
                serialize_tuple(fields.iter().map(|(_, value)| value), buf)
            }

            CqlValue::Date(d) => Date(*d).serialize(buf),
            CqlValue::Duration(d) => d.serialize(buf),
            CqlValue::Timestamp(t) => Timestamp(*t).serialize(buf),
            CqlValue::Time(t) => Time(*t).serialize(buf),

            CqlValue::Ascii(s) | CqlValue::Text(s) => s.serialize(buf),
            CqlValue::List(v) | CqlValue::Set(v) => v.serialize(buf),

            CqlValue::Blob(b) => b.serialize(buf),
            CqlValue::Boolean(b) => b.serialize(buf),
            CqlValue::Counter(c) => c.serialize(buf),
            CqlValue::Decimal(d) => d.serialize(buf),
            CqlValue::Double(d) => d.serialize(buf),
            CqlValue::Float(f) => f.serialize(buf),
            CqlValue::Int(i) => i.serialize(buf),
            CqlValue::BigInt(i) => i.serialize(buf),
            CqlValue::Inet(i) => i.serialize(buf),
            CqlValue::SmallInt(s) => s.serialize(buf),
            CqlValue::TinyInt(t) => t.serialize(buf),
            CqlValue::Timeuuid(t) => t.serialize(buf),
            CqlValue::Uuid(u) => u.serialize(buf),
            CqlValue::Varint(v) => v.serialize(buf),

            CqlValue::Empty => serialize_empty(buf),
        }
    }
}

macro_rules! impl_value_for_tuple {
    ( $($Ti:ident),* ; $($FieldI:tt),* ) => {
    impl<$($Ti),+> Value for ($($Ti,)+)
        where
            $($Ti: Value),+
        {
            fn serialize(&self, buf: &mut Vec<u8>) -> Result<(), ValueTooBig> {
                let bytes_num_pos: usize = buf.len();
                buf.put_i32(0);
                $(
                    <$Ti as Value>::serialize(&self.$FieldI, buf)?;
                )*

                let written_bytes: usize = buf.len() - bytes_num_pos - 4;
                let written_bytes_i32: i32 = written_bytes.try_into().map_err(|_| ValueTooBig) ?;
                buf[bytes_num_pos..(bytes_num_pos+4)].copy_from_slice(&written_bytes_i32.to_be_bytes());

                Ok(())
            }
        }
    }
}

impl_value_for_tuple!(T0; 0);
impl_value_for_tuple!(T0, T1; 0, 1);
impl_value_for_tuple!(T0, T1, T2; 0, 1, 2);
impl_value_for_tuple!(T0, T1, T2, T3; 0, 1, 2, 3);
impl_value_for_tuple!(T0, T1, T2, T3, T4; 0, 1, 2, 3, 4);
impl_value_for_tuple!(T0, T1, T2, T3, T4, T5; 0, 1, 2, 3, 4, 5);
impl_value_for_tuple!(T0, T1, T2, T3, T4, T5, T6; 0, 1, 2, 3, 4, 5, 6);
impl_value_for_tuple!(T0, T1, T2, T3, T4, T5, T6, T7; 0, 1, 2, 3, 4, 5, 6, 7);
impl_value_for_tuple!(T0, T1, T2, T3, T4, T5, T6, T7, T8; 0, 1, 2, 3, 4, 5, 6, 7, 8);
impl_value_for_tuple!(T0, T1, T2, T3, T4, T5, T6, T7, T8, T9;
                           0, 1, 2, 3, 4, 5, 6, 7, 8, 9);
impl_value_for_tuple!(T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10;
                           0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
impl_value_for_tuple!(T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11;
                           0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11);
impl_value_for_tuple!(T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12;
                           0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12);
impl_value_for_tuple!(T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13;
                           0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13);
impl_value_for_tuple!(T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14;
                           0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14);
impl_value_for_tuple!(T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15;
                           0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15);

//
//  ValueList impls
//

// Implement ValueList for the unit type
impl ValueList for () {
    fn serialized(&self) -> SerializedResult<'_> {
        Ok(Cow::Owned(SerializedValues::new()))
    }
}

// Implement ValueList for &[] - u8 because otherwise rust can't infer type
impl ValueList for [u8; 0] {
    fn serialized(&self) -> SerializedResult<'_> {
        Ok(Cow::Owned(SerializedValues::new()))
    }
}

// Implement ValueList for slices of Value types
impl<T: Value> ValueList for &[T] {
    fn serialized(&self) -> SerializedResult<'_> {
        let mut result = SerializedValues::with_capacity(self.len());
        for val in *self {
            result.add_value(val)?;
        }

        Ok(Cow::Owned(result))
    }
}

// Implement ValueList for Vec<Value>
impl<T: Value> ValueList for Vec<T> {
    fn serialized(&self) -> SerializedResult<'_> {
        let mut result = SerializedValues::with_capacity(self.len());
        for val in self {
            result.add_value(val)?;
        }

        Ok(Cow::Owned(result))
    }
}

// Implement ValueList for maps, which serializes named values
macro_rules! impl_value_list_for_map {
    ($map_type:ident, $key_type:ty) => {
        impl<T: Value> ValueList for $map_type<$key_type, T> {
            fn serialized(&self) -> SerializedResult<'_> {
                let mut result = SerializedValues::with_capacity(self.len());
                for (key, val) in self {
                    result.add_named_value(key, val)?;
                }

                Ok(Cow::Owned(result))
            }
        }
    };
}

impl_value_list_for_map!(HashMap, String);
impl_value_list_for_map!(HashMap, &str);
impl_value_list_for_map!(BTreeMap, String);
impl_value_list_for_map!(BTreeMap, &str);

// Implement ValueList for tuples of Values of size up to 16

// Here is an example implementation for (T0, )
// Further variants are done using a macro
impl<T0: Value> ValueList for (T0,) {
    fn serialized(&self) -> SerializedResult<'_> {
        let mut result = SerializedValues::with_capacity(1);
        result.add_value(&self.0)?;
        Ok(Cow::Owned(result))
    }
}

macro_rules! impl_value_list_for_tuple {
    ( $($Ti:ident),* ; $($FieldI:tt),* ; $size: expr) => {
        impl<$($Ti),+> ValueList for ($($Ti,)+)
        where
            $($Ti: Value),+
        {
            fn serialized(&self) -> SerializedResult<'_> {
                let mut result = SerializedValues::with_capacity($size);
                $(
                    result.add_value(&self.$FieldI) ?;
                )*
                Ok(Cow::Owned(result))
            }
        }
    }
}

impl_value_list_for_tuple!(T0, T1; 0, 1; 2);
impl_value_list_for_tuple!(T0, T1, T2; 0, 1, 2; 3);
impl_value_list_for_tuple!(T0, T1, T2, T3; 0, 1, 2, 3; 4);
impl_value_list_for_tuple!(T0, T1, T2, T3, T4; 0, 1, 2, 3, 4; 5);
impl_value_list_for_tuple!(T0, T1, T2, T3, T4, T5; 0, 1, 2, 3, 4, 5; 6);
impl_value_list_for_tuple!(T0, T1, T2, T3, T4, T5, T6; 0, 1, 2, 3, 4, 5, 6; 7);
impl_value_list_for_tuple!(T0, T1, T2, T3, T4, T5, T6, T7; 0, 1, 2, 3, 4, 5, 6, 7; 8);
impl_value_list_for_tuple!(T0, T1, T2, T3, T4, T5, T6, T7, T8; 0, 1, 2, 3, 4, 5, 6, 7, 8; 9);
impl_value_list_for_tuple!(T0, T1, T2, T3, T4, T5, T6, T7, T8, T9;
                           0, 1, 2, 3, 4, 5, 6, 7, 8, 9; 10);
impl_value_list_for_tuple!(T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10;
                           0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10; 11);
impl_value_list_for_tuple!(T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11;
                           0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11; 12);
impl_value_list_for_tuple!(T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12;
                           0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12; 13);
impl_value_list_for_tuple!(T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13;
                           0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13; 14);
impl_value_list_for_tuple!(T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14;
                           0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14; 15);
impl_value_list_for_tuple!(T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15;
                           0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15; 16);

// Every &impl ValueList should also implement ValueList
impl<T: ValueList> ValueList for &T {
    fn serialized(&self) -> SerializedResult<'_> {
        <T as ValueList>::serialized(*self)
    }
}

impl ValueList for SerializedValues {
    fn serialized(&self) -> SerializedResult<'_> {
        Ok(Cow::Borrowed(self))
    }
}

impl<'b> ValueList for Cow<'b, SerializedValues> {
    fn serialized(&self) -> SerializedResult<'_> {
        Ok(Cow::Borrowed(self.as_ref()))
    }
}

//
// BatchValues impls
//

/// Implements `BatchValues` from an `Iterator` over references to things that implement `ValueList`
///
/// This is to avoid requiring allocating a new `Vec` containing all the `ValueList`s directly:
/// with this, one can write:
/// `session.batch(&batch, BatchValuesFromIterator::from(lines_to_insert.iter().map(|l| &l.value_list)))`
/// where `lines_to_insert` may also contain e.g. data to pick the statement...
///
/// The underlying iterator will always be cloned at least once, once to compute the length if it can't be known
/// in advance, and be re-cloned at every retry.
/// It is consequently expected that the provided iterator is cheap to clone (e.g. `slice.iter().map(...)`).
pub struct BatchValuesFromIter<'a, IT> {
    it: IT,
    _spooky: std::marker::PhantomData<&'a ()>,
}

impl<'a, IT, VL> BatchValuesFromIter<'a, IT>
where
    IT: Iterator<Item = &'a VL> + Clone,
    VL: ValueList + 'a,
{
    pub fn new(into_iter: impl IntoIterator<IntoIter = IT>) -> Self {
        Self {
            it: into_iter.into_iter(),
            _spooky: std::marker::PhantomData,
        }
    }
}

impl<'a, IT, VL> From<IT> for BatchValuesFromIter<'a, IT>
where
    IT: Iterator<Item = &'a VL> + Clone,
    VL: ValueList + 'a,
{
    fn from(it: IT) -> Self {
        Self::new(it)
    }
}

impl<'a, IT, VL> BatchValues for BatchValuesFromIter<'a, IT>
where
    IT: Iterator<Item = &'a VL> + Clone,
    VL: ValueList + 'a,
{
    type BatchValuesIter<'r> = BatchValuesIteratorFromIterator<IT> where Self: 'r;
    fn batch_values_iter(&self) -> Self::BatchValuesIter<'_> {
        self.it.clone().into()
    }
}

// Implement BatchValues for slices of ValueList types
impl<T: ValueList> BatchValues for [T] {
    type BatchValuesIter<'r> = BatchValuesIteratorFromIterator<std::slice::Iter<'r, T>> where Self: 'r;
    fn batch_values_iter(&self) -> Self::BatchValuesIter<'_> {
        self.iter().into()
    }
}

// Implement BatchValues for Vec<ValueList>
impl<T: ValueList> BatchValues for Vec<T> {
    type BatchValuesIter<'r> = BatchValuesIteratorFromIterator<std::slice::Iter<'r, T>> where Self: 'r;
    fn batch_values_iter(&self) -> Self::BatchValuesIter<'_> {
        BatchValues::batch_values_iter(self.as_slice())
    }
}

// Here is an example implementation for (T0, )
// Further variants are done using a macro
impl<T0: ValueList> BatchValues for (T0,) {
    type BatchValuesIter<'r> = BatchValuesIteratorFromIterator<std::iter::Once<&'r T0>> where Self: 'r;
    fn batch_values_iter(&self) -> Self::BatchValuesIter<'_> {
        std::iter::once(&self.0).into()
    }
}

pub struct TupleValuesIter<'a, T> {
    tuple: &'a T,
    idx: usize,
}

macro_rules! impl_batch_values_for_tuple {
    ( $($Ti:ident),* ; $($FieldI:tt),* ; $TupleSize:tt) => {
        impl<$($Ti),+> BatchValues for ($($Ti,)+)
        where
            $($Ti: ValueList),+
        {
            type BatchValuesIter<'r> = TupleValuesIter<'r, ($($Ti,)+)> where Self: 'r;
            fn batch_values_iter(&self) -> Self::BatchValuesIter<'_> {
                TupleValuesIter {
                    tuple: self,
                    idx: 0,
                }
            }
        }
        impl<'r, $($Ti),+> BatchValuesIterator<'r> for TupleValuesIter<'r, ($($Ti,)+)>
        where
            $($Ti: ValueList),+
        {
            fn next_serialized(&mut self) -> Option<SerializedResult<'r>> {
                let serialized_value_res = match self.idx {
                    $(
                        $FieldI => self.tuple.$FieldI.serialized(),
                    )*
                    _ => return None,
                };
                self.idx += 1;
                Some(serialized_value_res)
            }
            fn write_next_to_request(
                &mut self,
                buf: &mut impl BufMut,
            ) -> Option<Result<(), SerializeValuesError>> {
                let ret = match self.idx {
                    $(
                        $FieldI => self.tuple.$FieldI.write_to_request(buf),
                    )*
                    _ => return None,
                };
                self.idx += 1;
                Some(ret)
            }
            fn skip_next(&mut self) -> Option<()> {
                if self.idx < $TupleSize {
                    self.idx += 1;
                    Some(())
                } else {
                    None
                }
            }
        }
    }
}

impl_batch_values_for_tuple!(T0, T1; 0, 1; 2);
impl_batch_values_for_tuple!(T0, T1, T2; 0, 1, 2; 3);
impl_batch_values_for_tuple!(T0, T1, T2, T3; 0, 1, 2, 3; 4);
impl_batch_values_for_tuple!(T0, T1, T2, T3, T4; 0, 1, 2, 3, 4; 5);
impl_batch_values_for_tuple!(T0, T1, T2, T3, T4, T5; 0, 1, 2, 3, 4, 5; 6);
impl_batch_values_for_tuple!(T0, T1, T2, T3, T4, T5, T6; 0, 1, 2, 3, 4, 5, 6; 7);
impl_batch_values_for_tuple!(T0, T1, T2, T3, T4, T5, T6, T7; 0, 1, 2, 3, 4, 5, 6, 7; 8);
impl_batch_values_for_tuple!(T0, T1, T2, T3, T4, T5, T6, T7, T8; 0, 1, 2, 3, 4, 5, 6, 7, 8; 9);
impl_batch_values_for_tuple!(T0, T1, T2, T3, T4, T5, T6, T7, T8, T9;
                             0, 1, 2, 3, 4, 5, 6, 7, 8, 9; 10);
impl_batch_values_for_tuple!(T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10;
                             0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10; 11);
impl_batch_values_for_tuple!(T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11;
                             0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11; 12);
impl_batch_values_for_tuple!(T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12;
                             0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12; 13);
impl_batch_values_for_tuple!(T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13;
                             0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13; 14);
impl_batch_values_for_tuple!(T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14;
                             0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14; 15);
impl_batch_values_for_tuple!(T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15;
                             0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15; 16);

// Every &impl BatchValues should also implement BatchValues
impl<'a, T: BatchValues + ?Sized> BatchValues for &'a T {
    type BatchValuesIter<'r> = <T as BatchValues>::BatchValuesIter<'r> where Self: 'r;
    fn batch_values_iter(&self) -> Self::BatchValuesIter<'_> {
        <T as BatchValues>::batch_values_iter(*self)
    }
}

/// Allows reusing already-serialized first value
///
/// We'll need to build a `SerializedValues` for the first ~`ValueList` of a batch to figure out the shard (#448).
/// Once that is done, we can use that instead of re-serializing.
///
/// This struct implements both `BatchValues` and `BatchValuesIterator` for that purpose
pub struct BatchValuesFirstSerialized<'f, T> {
    first: Option<&'f SerializedValues>,
    rest: T,
}

impl<'f, T: BatchValues> BatchValuesFirstSerialized<'f, T> {
    pub fn new(batch_values: T, already_serialized_first: Option<&'f SerializedValues>) -> Self {
        Self {
            first: already_serialized_first,
            rest: batch_values,
        }
    }
}

impl<'f, BV: BatchValues> BatchValues for BatchValuesFirstSerialized<'f, BV> {
    type BatchValuesIter<'r> =
        BatchValuesFirstSerialized<'f, <BV as BatchValues>::BatchValuesIter<'r>> where Self: 'r;
    fn batch_values_iter(&self) -> Self::BatchValuesIter<'_> {
        BatchValuesFirstSerialized {
            first: self.first,
            rest: self.rest.batch_values_iter(),
        }
    }
}

impl<'a, 'f: 'a, IT: BatchValuesIterator<'a>> BatchValuesIterator<'a>
    for BatchValuesFirstSerialized<'f, IT>
{
    fn next_serialized(&mut self) -> Option<SerializedResult<'a>> {
        match self.first.take() {
            Some(first) => {
                self.rest.skip_next();
                Some(Ok(Cow::Borrowed(first)))
            }
            None => self.rest.next_serialized(),
        }
    }
    fn write_next_to_request(
        &mut self,
        buf: &mut impl BufMut,
    ) -> Option<Result<(), SerializeValuesError>> {
        match self.first.take() {
            Some(first) => {
                self.rest.skip_next();
                first.write_to_request(buf);
                Some(Ok(()))
            }
            None => self.rest.write_next_to_request(buf),
        }
    }
    fn skip_next(&mut self) -> Option<()> {
        self.rest.skip_next();
        self.first.take().map(|_| ())
    }
}
