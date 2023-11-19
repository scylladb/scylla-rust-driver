use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::fmt::Display;
use std::hash::BuildHasher;
use std::net::IpAddr;
use std::sync::Arc;

use bigdecimal::BigDecimal;
use num_bigint::BigInt;
use thiserror::Error;
use uuid::Uuid;

#[cfg(feature = "chrono")]
use chrono::{DateTime, NaiveDate, NaiveTime, Utc};

#[cfg(feature = "secret")]
use secrecy::{Secret, Zeroize};

use crate::frame::response::result::{ColumnType, CqlValue};
use crate::frame::value::{
    Counter, CqlDate, CqlDuration, CqlTime, CqlTimestamp, MaybeUnset, Unset, Value,
};

use super::{CellWriter, SerializationError};

pub trait SerializeCql {
    /// Given a CQL type, checks if it _might_ be possible to serialize to that type.
    ///
    /// This function is intended to serve as an optimization in the future,
    /// if we were ever to introduce prepared statements parametrized by types.
    ///
    /// Some types cannot be type checked without knowing the exact value,
    /// this is the case e.g. for `CqlValue`. It's also fine to do it later in
    /// `serialize`.
    fn preliminary_type_check(typ: &ColumnType) -> Result<(), SerializationError>;

    /// Serializes the value to given CQL type.
    ///
    /// The function may assume that `preliminary_type_check` was called,
    /// though it must not do anything unsafe if this assumption does not hold.
    fn serialize<W: CellWriter>(
        &self,
        typ: &ColumnType,
        writer: W,
    ) -> Result<W::WrittenCellProof, SerializationError>;
}

macro_rules! fallback_impl_contents {
    () => {
        fn preliminary_type_check(_typ: &ColumnType) -> Result<(), SerializationError> {
            Ok(())
        }

        fn serialize<W: CellWriter>(
            &self,
            _typ: &ColumnType,
            writer: W,
        ) -> Result<W::WrittenCellProof, SerializationError> {
            serialize_legacy_value(self, writer)
        }
    };
}

macro_rules! fallback_tuples {
    () => {};
    ($th:ident$(, $($tt:ident),*)?) => {
        fallback_tuples!($($($tt),*)?);
        impl<$th: Value$(, $($tt: Value),*)?> SerializeCql for ($th, $($($tt),*)?) {
            fallback_impl_contents!();
        }
    };
}

macro_rules! impl_exact_preliminary_type_check {
    ($($cql:tt),*) => {
        fn preliminary_type_check(typ: &ColumnType) -> Result<(), SerializationError> {
            match typ {
                $(ColumnType::$cql)|* => Ok(()),
                _ => Err(mk_typck_err::<Self>(
                    typ,
                    BuiltinTypeCheckErrorKind::MismatchedType {
                        expected: &[$(ColumnType::$cql),*],
                    }
                ))
            }
        }
    };
}

macro_rules! impl_serialize_via_writer {
    (|$me:ident, $writer:ident| $e:expr) => {
        impl_serialize_via_writer!(|$me, _typ, $writer| $e);
    };
    (|$me:ident, $typ:ident, $writer:ident| $e:expr) => {
        fn serialize<W: CellWriter>(
            &self,
            typ: &ColumnType,
            writer: W,
        ) -> Result<W::WrittenCellProof, SerializationError> {
            let $writer = writer;
            let $typ = typ;
            let $me = self;
            let proof = $e;
            Ok(proof)
        }
    };
}

impl SerializeCql for i8 {
    impl_exact_preliminary_type_check!(TinyInt);
    impl_serialize_via_writer!(|me, writer| writer.set_value(me.to_be_bytes().as_slice()).unwrap());
}
impl SerializeCql for i16 {
    impl_exact_preliminary_type_check!(SmallInt);
    impl_serialize_via_writer!(|me, writer| writer.set_value(me.to_be_bytes().as_slice()).unwrap());
}
impl SerializeCql for i32 {
    impl_exact_preliminary_type_check!(Int);
    impl_serialize_via_writer!(|me, writer| writer.set_value(me.to_be_bytes().as_slice()).unwrap());
}
impl SerializeCql for i64 {
    impl_exact_preliminary_type_check!(BigInt);
    impl_serialize_via_writer!(|me, writer| writer.set_value(me.to_be_bytes().as_slice()).unwrap());
}
impl SerializeCql for BigDecimal {
    fallback_impl_contents!();
}
impl SerializeCql for CqlDate {
    fallback_impl_contents!();
}
impl SerializeCql for CqlTimestamp {
    fallback_impl_contents!();
}
impl SerializeCql for CqlTime {
    fallback_impl_contents!();
}
#[cfg(feature = "chrono")]
impl SerializeCql for NaiveDate {
    fallback_impl_contents!();
}
#[cfg(feature = "chrono")]
impl SerializeCql for DateTime<Utc> {
    fallback_impl_contents!();
}
#[cfg(feature = "chrono")]
impl SerializeCql for NaiveTime {
    fallback_impl_contents!();
}
#[cfg(feature = "chrono")]
impl SerializeCql for time::Date {
    fallback_impl_contents!();
}
#[cfg(feature = "chrono")]
impl SerializeCql for time::OffsetDateTime {
    fallback_impl_contents!();
}
#[cfg(feature = "chrono")]
impl SerializeCql for time::Time {
    fallback_impl_contents!();
}
#[cfg(feature = "secret")]
impl<V: Value + Zeroize> SerializeCql for Secret<V> {
    fallback_impl_contents!();
}
impl SerializeCql for bool {
    impl_exact_preliminary_type_check!(Boolean);
    impl_serialize_via_writer!(|me, writer| writer.set_value(&[*me as u8]).unwrap());
}
impl SerializeCql for f32 {
    impl_exact_preliminary_type_check!(Float);
    impl_serialize_via_writer!(|me, writer| writer.set_value(me.to_be_bytes().as_slice()).unwrap());
}
impl SerializeCql for f64 {
    impl_exact_preliminary_type_check!(Double);
    impl_serialize_via_writer!(|me, writer| writer.set_value(me.to_be_bytes().as_slice()).unwrap());
}
impl SerializeCql for Uuid {
    impl_exact_preliminary_type_check!(Uuid, Timeuuid);
    impl_serialize_via_writer!(|me, writer| writer.set_value(me.as_bytes().as_ref()).unwrap());
}
impl SerializeCql for BigInt {
    fallback_impl_contents!();
}
impl SerializeCql for &str {
    impl_exact_preliminary_type_check!(Ascii, Text);
    impl_serialize_via_writer!(|me, typ, writer| {
        writer
            .set_value(me.as_bytes())
            .map_err(|_| mk_ser_err::<Self>(typ, BuiltinSerializationErrorKind::SizeOverflow))?
    });
}
impl SerializeCql for Vec<u8> {
    impl_exact_preliminary_type_check!(Blob);
    impl_serialize_via_writer!(|me, typ, writer| {
        writer
            .set_value(me.as_ref())
            .map_err(|_| mk_ser_err::<Self>(typ, BuiltinSerializationErrorKind::SizeOverflow))?
    });
}
impl SerializeCql for &[u8] {
    impl_exact_preliminary_type_check!(Blob);
    impl_serialize_via_writer!(|me, typ, writer| {
        writer
            .set_value(me)
            .map_err(|_| mk_ser_err::<Self>(typ, BuiltinSerializationErrorKind::SizeOverflow))?
    });
}
impl<const N: usize> SerializeCql for [u8; N] {
    impl_exact_preliminary_type_check!(Blob);
    impl_serialize_via_writer!(|me, typ, writer| {
        writer
            .set_value(me.as_ref())
            .map_err(|_| mk_ser_err::<Self>(typ, BuiltinSerializationErrorKind::SizeOverflow))?
    });
}
impl SerializeCql for IpAddr {
    impl_exact_preliminary_type_check!(Inet);
    impl_serialize_via_writer!(|me, writer| {
        match me {
            IpAddr::V4(ip) => writer.set_value(&ip.octets()).unwrap(),
            IpAddr::V6(ip) => writer.set_value(&ip.octets()).unwrap(),
        }
    });
}
impl SerializeCql for String {
    impl_exact_preliminary_type_check!(Ascii, Text);
    impl_serialize_via_writer!(|me, typ, writer| {
        writer
            .set_value(me.as_bytes())
            .map_err(|_| mk_ser_err::<Self>(typ, BuiltinSerializationErrorKind::SizeOverflow))?
    });
}
impl<T: Value> SerializeCql for Option<T> {
    fallback_impl_contents!();
}
impl SerializeCql for Unset {
    fallback_impl_contents!();
}
impl SerializeCql for Counter {
    impl_exact_preliminary_type_check!(Counter);
    impl_serialize_via_writer!(|me, writer| {
        writer.set_value(me.0.to_be_bytes().as_slice()).unwrap()
    });
}
impl SerializeCql for CqlDuration {
    fallback_impl_contents!();
}
impl<V: Value> SerializeCql for MaybeUnset<V> {
    fallback_impl_contents!();
}
impl<T: Value + ?Sized> SerializeCql for &T {
    fallback_impl_contents!();
}
impl<T: Value + ?Sized> SerializeCql for Box<T> {
    fallback_impl_contents!();
}
impl<V: Value, S: BuildHasher + Default> SerializeCql for HashSet<V, S> {
    fallback_impl_contents!();
}
impl<K: Value, V: Value, S: BuildHasher> SerializeCql for HashMap<K, V, S> {
    fallback_impl_contents!();
}
impl<V: Value> SerializeCql for BTreeSet<V> {
    fallback_impl_contents!();
}
impl<K: Value, V: Value> SerializeCql for BTreeMap<K, V> {
    fallback_impl_contents!();
}
impl<T: Value> SerializeCql for Vec<T> {
    fallback_impl_contents!();
}
impl<T: Value> SerializeCql for &[T] {
    fallback_impl_contents!();
}
impl SerializeCql for CqlValue {
    fallback_impl_contents!();
}

fallback_tuples!(T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15);

pub fn serialize_legacy_value<T: Value, W: CellWriter>(
    v: &T,
    writer: W,
) -> Result<W::WrittenCellProof, SerializationError> {
    // It's an inefficient and slightly tricky but correct implementation.
    let mut buf = Vec::new();
    <T as Value>::serialize(v, &mut buf).map_err(|err| SerializationError(Arc::new(err)))?;

    // Analyze the output.
    // All this dance shows how unsafe our previous interface was...
    if buf.len() < 4 {
        return Err(SerializationError(Arc::new(
            ValueToSerializeCqlAdapterError::TooShort { size: buf.len() },
        )));
    }

    let (len_bytes, contents) = buf.split_at(4);
    let len = i32::from_be_bytes(len_bytes.try_into().unwrap());
    match len {
        -2 => Ok(writer.set_unset()),
        -1 => Ok(writer.set_null()),
        len if len >= 0 => {
            if contents.len() != len as usize {
                Err(SerializationError(Arc::new(
                    ValueToSerializeCqlAdapterError::DeclaredVsActualSizeMismatch {
                        declared: len as usize,
                        actual: contents.len(),
                    },
                )))
            } else {
                Ok(writer.set_value(contents).unwrap()) // len <= i32::MAX, so unwrap will succeed
            }
        }
        _ => Err(SerializationError(Arc::new(
            ValueToSerializeCqlAdapterError::InvalidDeclaredSize { size: len },
        ))),
    }
}

/// Type checking of one of the built-in types failed.
#[derive(Debug, Error, Clone)]
#[error("Failed to type check Rust type {rust_name} against CQL type {got:?}: {kind}")]
pub struct BuiltinTypeCheckError {
    /// Name of the Rust type being serialized.
    pub rust_name: &'static str,

    /// The CQL type that the Rust type was being serialized to.
    pub got: ColumnType,

    /// Detailed information about the failure.
    pub kind: BuiltinTypeCheckErrorKind,
}

fn mk_typck_err<T>(
    got: &ColumnType,
    kind: impl Into<BuiltinTypeCheckErrorKind>,
) -> SerializationError {
    mk_typck_err_named(std::any::type_name::<T>(), got, kind)
}

fn mk_typck_err_named(
    name: &'static str,
    got: &ColumnType,
    kind: impl Into<BuiltinTypeCheckErrorKind>,
) -> SerializationError {
    SerializationError::new(BuiltinTypeCheckError {
        rust_name: name,
        got: got.clone(),
        kind: kind.into(),
    })
}

/// Serialization of one of the built-in types failed.
#[derive(Debug, Error, Clone)]
#[error("Failed to serialize Rust type {rust_name} into CQL type {got:?}: {kind}")]
pub struct BuiltinSerializationError {
    /// Name of the Rust type being serialized.
    pub rust_name: &'static str,

    /// The CQL type that the Rust type was being serialized to.
    pub got: ColumnType,

    /// Detailed information about the failure.
    pub kind: BuiltinSerializationErrorKind,
}

fn mk_ser_err<T>(
    got: &ColumnType,
    kind: impl Into<BuiltinSerializationErrorKind>,
) -> SerializationError {
    mk_ser_err_named(std::any::type_name::<T>(), got, kind)
}

fn mk_ser_err_named(
    name: &'static str,
    got: &ColumnType,
    kind: impl Into<BuiltinSerializationErrorKind>,
) -> SerializationError {
    SerializationError::new(BuiltinSerializationError {
        rust_name: name,
        got: got.clone(),
        kind: kind.into(),
    })
}

/// Describes why type checking some of the built-in types has failed.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum BuiltinTypeCheckErrorKind {
    /// Expected one from a list of particular types.
    MismatchedType { expected: &'static [ColumnType] },
}

impl Display for BuiltinTypeCheckErrorKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            BuiltinTypeCheckErrorKind::MismatchedType { expected } => {
                write!(f, "expected one of the CQL types: {expected:?}")
            }
        }
    }
}

/// Describes why serialization of some of the built-in types has failed.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum BuiltinSerializationErrorKind {
    /// The size of the Rust value is too large to fit in the CQL serialization
    /// format (over i32::MAX bytes).
    SizeOverflow,
}

impl Display for BuiltinSerializationErrorKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            BuiltinSerializationErrorKind::SizeOverflow => {
                write!(
                    f,
                    "the Rust value is too big to be serialized in the CQL protocol format"
                )
            }
        }
    }
}

#[derive(Error, Debug)]
pub enum ValueToSerializeCqlAdapterError {
    #[error("Output produced by the Value trait is too short to be considered a value: {size} < 4 minimum bytes")]
    TooShort { size: usize },

    #[error("Mismatch between the declared value size vs. actual size: {declared} != {actual}")]
    DeclaredVsActualSizeMismatch { declared: usize, actual: usize },

    #[error("Invalid declared value size: {size}")]
    InvalidDeclaredSize { size: i32 },
}

#[cfg(test)]
mod tests {
    use crate::frame::response::result::ColumnType;
    use crate::frame::value::{MaybeUnset, Value};
    use crate::types::serialize::BufBackedCellWriter;

    use super::SerializeCql;

    fn check_compat<V: Value + SerializeCql>(v: V) {
        let mut legacy_data = Vec::new();
        <V as Value>::serialize(&v, &mut legacy_data).unwrap();

        let mut new_data = Vec::new();
        let new_data_writer = BufBackedCellWriter::new(&mut new_data);
        <V as SerializeCql>::serialize(&v, &ColumnType::Int, new_data_writer).unwrap();

        assert_eq!(legacy_data, new_data);
    }

    #[test]
    fn test_legacy_fallback() {
        check_compat(123i32);
        check_compat(None::<i32>);
        check_compat(MaybeUnset::Unset::<i32>);
    }
}
