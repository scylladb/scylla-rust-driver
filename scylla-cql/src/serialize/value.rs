//! Contains the [`SerializeValue`] trait and its implementations.

// Note: When editing above doc-comment edit the corresponding comment on
// re-export module in scylla crate too.

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::fmt::Display;
use std::hash::BuildHasher;
use std::net::IpAddr;
use std::ops::Deref as _;
use std::sync::Arc;

use thiserror::Error;
use uuid::Uuid;

use crate::frame::response::result::{ColumnType, CqlValue, NativeType};
use crate::frame::types::vint_encode;
#[allow(deprecated)]
use crate::frame::value::{
    Counter, CqlDate, CqlDecimal, CqlDecimalBorrowed, CqlDuration, CqlTime, CqlTimestamp,
    CqlTimeuuid, CqlVarint, CqlVarintBorrowed, MaybeUnset, Unset, Value,
};

#[cfg(feature = "chrono-04")]
use crate::frame::value::ValueOverflow;

use super::writers::WrittenCellProof;
use super::{CellWriter, SerializationError};

/// A type that can be serialized and sent along with a CQL statement.
///
/// This is a low-level trait that is exposed to the specifics to the CQL
/// protocol and usually does not have to be implemented directly. See the
/// chapter on "Query Values" in the driver docs for information about how
/// this trait is supposed to be used.
pub trait SerializeValue {
    /// Serializes the value to given CQL type.
    ///
    /// The value should produce a `[value]`, according to the [CQL protocol
    /// specification](https://github.com/apache/cassandra/blob/trunk/doc/native_protocol_v4.spec),
    /// containing the serialized value. See section 6 of the document on how
    /// the contents of the `[value]` should look like.
    ///
    /// The value produced should match the type provided by `typ`. If the
    /// value cannot be serialized to that type, an error should be returned.
    ///
    /// The [`CellWriter`] provided to the method ensures that the value produced
    /// will be properly framed (i.e. incorrectly written value should not
    /// cause the rest of the request to be misinterpreted), but otherwise
    /// the implementor of the trait is responsible for producing the value
    /// in a correct format.
    fn serialize<'b>(
        &self,
        typ: &ColumnType,
        writer: CellWriter<'b>,
    ) -> Result<WrittenCellProof<'b>, SerializationError>;
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

macro_rules! impl_serialize_via_writer {
    (|$me:ident, $writer:ident| $e:expr) => {
        impl_serialize_via_writer!(|$me, _typ, $writer| $e);
    };
    (|$me:ident, $typ:ident, $writer:ident| $e:expr) => {
        fn serialize<'b>(
            &self,
            typ: &ColumnType,
            writer: CellWriter<'b>,
        ) -> Result<WrittenCellProof<'b>, SerializationError> {
            let $writer = writer;
            let $typ = typ;
            let $me = self;
            let proof = $e;
            Ok(proof)
        }
    };
}

impl SerializeValue for i8 {
    impl_serialize_via_writer!(|me, typ, writer| {
        exact_type_check!(typ, TinyInt);
        writer.set_value(me.to_be_bytes().as_slice()).unwrap()
    });
}
impl SerializeValue for i16 {
    impl_serialize_via_writer!(|me, typ, writer| {
        exact_type_check!(typ, SmallInt);
        writer.set_value(me.to_be_bytes().as_slice()).unwrap()
    });
}
impl SerializeValue for i32 {
    impl_serialize_via_writer!(|me, typ, writer| {
        exact_type_check!(typ, Int);
        writer.set_value(me.to_be_bytes().as_slice()).unwrap()
    });
}
impl SerializeValue for i64 {
    impl_serialize_via_writer!(|me, typ, writer| {
        exact_type_check!(typ, BigInt);
        writer.set_value(me.to_be_bytes().as_slice()).unwrap()
    });
}
impl SerializeValue for CqlDecimal {
    impl_serialize_via_writer!(|me, typ, writer| {
        exact_type_check!(typ, Decimal);
        let mut builder = writer.into_value_builder();
        let (bytes, scale) = me.as_signed_be_bytes_slice_and_exponent();
        builder.append_bytes(&scale.to_be_bytes());
        builder.append_bytes(bytes);
        builder
            .finish()
            .map_err(|_| mk_ser_err::<Self>(typ, BuiltinSerializationErrorKind::SizeOverflow))?
    });
}
impl SerializeValue for CqlDecimalBorrowed<'_> {
    impl_serialize_via_writer!(|me, typ, writer| {
        exact_type_check!(typ, Decimal);
        let mut builder = writer.into_value_builder();
        let (bytes, scale) = me.as_signed_be_bytes_slice_and_exponent();
        builder.append_bytes(&scale.to_be_bytes());
        builder.append_bytes(bytes);
        builder
            .finish()
            .map_err(|_| mk_ser_err::<Self>(typ, BuiltinSerializationErrorKind::SizeOverflow))?
    });
}
#[cfg(feature = "bigdecimal-04")]
impl SerializeValue for bigdecimal_04::BigDecimal {
    impl_serialize_via_writer!(|me, typ, writer| {
        exact_type_check!(typ, Decimal);
        let mut builder = writer.into_value_builder();
        let (value, scale) = me.as_bigint_and_exponent();
        let scale: i32 = scale
            .try_into()
            .map_err(|_| mk_ser_err::<Self>(typ, BuiltinSerializationErrorKind::ValueOverflow))?;
        builder.append_bytes(&scale.to_be_bytes());
        builder.append_bytes(&value.to_signed_bytes_be());
        builder
            .finish()
            .map_err(|_| mk_ser_err::<Self>(typ, BuiltinSerializationErrorKind::SizeOverflow))?
    });
}
impl SerializeValue for CqlDate {
    impl_serialize_via_writer!(|me, typ, writer| {
        exact_type_check!(typ, Date);
        writer.set_value(me.0.to_be_bytes().as_slice()).unwrap()
    });
}
impl SerializeValue for CqlTimestamp {
    impl_serialize_via_writer!(|me, typ, writer| {
        exact_type_check!(typ, Timestamp);
        writer.set_value(me.0.to_be_bytes().as_slice()).unwrap()
    });
}
impl SerializeValue for CqlTime {
    impl_serialize_via_writer!(|me, typ, writer| {
        exact_type_check!(typ, Time);
        writer.set_value(me.0.to_be_bytes().as_slice()).unwrap()
    });
}
#[cfg(feature = "chrono-04")]
impl SerializeValue for chrono_04::NaiveDate {
    impl_serialize_via_writer!(|me, typ, writer| {
        exact_type_check!(typ, Date);
        <CqlDate as SerializeValue>::serialize(&(*me).into(), typ, writer)?
    });
}
#[cfg(feature = "chrono-04")]
impl SerializeValue for chrono_04::DateTime<chrono_04::Utc> {
    impl_serialize_via_writer!(|me, typ, writer| {
        exact_type_check!(typ, Timestamp);
        <CqlTimestamp as SerializeValue>::serialize(&(*me).into(), typ, writer)?
    });
}
#[cfg(feature = "chrono-04")]
impl SerializeValue for chrono_04::NaiveTime {
    impl_serialize_via_writer!(|me, typ, writer| {
        exact_type_check!(typ, Time);
        let cql_time = CqlTime::try_from(*me).map_err(|_: ValueOverflow| {
            mk_ser_err::<Self>(typ, BuiltinSerializationErrorKind::ValueOverflow)
        })?;
        <CqlTime as SerializeValue>::serialize(&cql_time, typ, writer)?
    });
}
#[cfg(feature = "time-03")]
impl SerializeValue for time_03::Date {
    impl_serialize_via_writer!(|me, typ, writer| {
        exact_type_check!(typ, Date);
        <CqlDate as SerializeValue>::serialize(&(*me).into(), typ, writer)?
    });
}
#[cfg(feature = "time-03")]
impl SerializeValue for time_03::OffsetDateTime {
    impl_serialize_via_writer!(|me, typ, writer| {
        exact_type_check!(typ, Timestamp);
        <CqlTimestamp as SerializeValue>::serialize(&(*me).into(), typ, writer)?
    });
}
#[cfg(feature = "time-03")]
impl SerializeValue for time_03::Time {
    impl_serialize_via_writer!(|me, typ, writer| {
        exact_type_check!(typ, Time);
        <CqlTime as SerializeValue>::serialize(&(*me).into(), typ, writer)?
    });
}
#[cfg(feature = "secrecy-08")]
impl<V: SerializeValue + secrecy_08::Zeroize> SerializeValue for secrecy_08::Secret<V> {
    fn serialize<'b>(
        &self,
        typ: &ColumnType,
        writer: CellWriter<'b>,
    ) -> Result<WrittenCellProof<'b>, SerializationError> {
        use secrecy_08::ExposeSecret;
        V::serialize(self.expose_secret(), typ, writer)
    }
}
impl SerializeValue for bool {
    impl_serialize_via_writer!(|me, typ, writer| {
        exact_type_check!(typ, Boolean);
        writer.set_value(&[*me as u8]).unwrap()
    });
}
impl SerializeValue for f32 {
    impl_serialize_via_writer!(|me, typ, writer| {
        exact_type_check!(typ, Float);
        writer.set_value(me.to_be_bytes().as_slice()).unwrap()
    });
}
impl SerializeValue for f64 {
    impl_serialize_via_writer!(|me, typ, writer| {
        exact_type_check!(typ, Double);
        writer.set_value(me.to_be_bytes().as_slice()).unwrap()
    });
}
impl SerializeValue for Uuid {
    impl_serialize_via_writer!(|me, typ, writer| {
        exact_type_check!(typ, Uuid);
        writer.set_value(me.as_bytes().as_ref()).unwrap()
    });
}
impl SerializeValue for CqlTimeuuid {
    impl_serialize_via_writer!(|me, typ, writer| {
        exact_type_check!(typ, Timeuuid);
        writer.set_value(me.as_bytes().as_ref()).unwrap()
    });
}
impl SerializeValue for CqlVarint {
    impl_serialize_via_writer!(|me, typ, writer| {
        exact_type_check!(typ, Varint);
        writer
            .set_value(me.as_signed_bytes_be_slice())
            .map_err(|_| mk_ser_err::<Self>(typ, BuiltinSerializationErrorKind::SizeOverflow))?
    });
}
impl SerializeValue for CqlVarintBorrowed<'_> {
    impl_serialize_via_writer!(|me, typ, writer| {
        exact_type_check!(typ, Varint);
        writer
            .set_value(me.as_signed_bytes_be_slice())
            .map_err(|_| mk_ser_err::<Self>(typ, BuiltinSerializationErrorKind::SizeOverflow))?
    });
}
#[cfg(feature = "num-bigint-03")]
impl SerializeValue for num_bigint_03::BigInt {
    impl_serialize_via_writer!(|me, typ, writer| {
        exact_type_check!(typ, Varint);
        // TODO: The allocation here can be avoided and we can reimplement
        // `to_signed_bytes_be` by using `to_u64_digits` and a bit of custom
        // logic. Need better tests in order to do this.
        writer
            .set_value(me.to_signed_bytes_be().as_slice())
            .map_err(|_| mk_ser_err::<Self>(typ, BuiltinSerializationErrorKind::SizeOverflow))?
    });
}
#[cfg(feature = "num-bigint-04")]
impl SerializeValue for num_bigint_04::BigInt {
    impl_serialize_via_writer!(|me, typ, writer| {
        exact_type_check!(typ, Varint);
        // TODO: See above comment for num-bigint-03.
        writer
            .set_value(me.to_signed_bytes_be().as_slice())
            .map_err(|_| mk_ser_err::<Self>(typ, BuiltinSerializationErrorKind::SizeOverflow))?
    });
}
impl SerializeValue for &str {
    impl_serialize_via_writer!(|me, typ, writer| {
        exact_type_check!(typ, Ascii, Text);
        writer
            .set_value(me.as_bytes())
            .map_err(|_| mk_ser_err::<Self>(typ, BuiltinSerializationErrorKind::SizeOverflow))?
    });
}
impl SerializeValue for Vec<u8> {
    impl_serialize_via_writer!(|me, typ, writer| {
        exact_type_check!(typ, Blob);
        writer
            .set_value(me.as_ref())
            .map_err(|_| mk_ser_err::<Self>(typ, BuiltinSerializationErrorKind::SizeOverflow))?
    });
}
impl SerializeValue for &[u8] {
    impl_serialize_via_writer!(|me, typ, writer| {
        exact_type_check!(typ, Blob);
        writer
            .set_value(me)
            .map_err(|_| mk_ser_err::<Self>(typ, BuiltinSerializationErrorKind::SizeOverflow))?
    });
}
impl<const N: usize> SerializeValue for [u8; N] {
    impl_serialize_via_writer!(|me, typ, writer| {
        exact_type_check!(typ, Blob);
        writer
            .set_value(me.as_ref())
            .map_err(|_| mk_ser_err::<Self>(typ, BuiltinSerializationErrorKind::SizeOverflow))?
    });
}
impl SerializeValue for IpAddr {
    impl_serialize_via_writer!(|me, typ, writer| {
        exact_type_check!(typ, Inet);
        match me {
            IpAddr::V4(ip) => writer.set_value(&ip.octets()).unwrap(),
            IpAddr::V6(ip) => writer.set_value(&ip.octets()).unwrap(),
        }
    });
}
impl SerializeValue for String {
    impl_serialize_via_writer!(|me, typ, writer| {
        exact_type_check!(typ, Ascii, Text);
        writer
            .set_value(me.as_bytes())
            .map_err(|_| mk_ser_err::<Self>(typ, BuiltinSerializationErrorKind::SizeOverflow))?
    });
}
impl<T: SerializeValue> SerializeValue for Option<T> {
    fn serialize<'b>(
        &self,
        typ: &ColumnType,
        writer: CellWriter<'b>,
    ) -> Result<WrittenCellProof<'b>, SerializationError> {
        match self {
            Some(v) => v.serialize(typ, writer),
            None => Ok(writer.set_null()),
        }
    }
}
impl SerializeValue for Unset {
    impl_serialize_via_writer!(|_me, writer| writer.set_unset());
}
impl SerializeValue for Counter {
    impl_serialize_via_writer!(|me, typ, writer| {
        exact_type_check!(typ, Counter);
        writer.set_value(me.0.to_be_bytes().as_slice()).unwrap()
    });
}
impl SerializeValue for CqlDuration {
    impl_serialize_via_writer!(|me, typ, writer| {
        exact_type_check!(typ, Duration);
        // TODO: adjust vint_encode to use CellValueBuilder or something like that
        let mut buf = Vec::with_capacity(27); // worst case size is 27
        vint_encode(me.months as i64, &mut buf);
        vint_encode(me.days as i64, &mut buf);
        vint_encode(me.nanoseconds, &mut buf);
        writer.set_value(buf.as_slice()).unwrap()
    });
}
impl<V: SerializeValue> SerializeValue for MaybeUnset<V> {
    fn serialize<'b>(
        &self,
        typ: &ColumnType,
        writer: CellWriter<'b>,
    ) -> Result<WrittenCellProof<'b>, SerializationError> {
        match self {
            MaybeUnset::Set(v) => v.serialize(typ, writer),
            MaybeUnset::Unset => Ok(writer.set_unset()),
        }
    }
}
impl<T: SerializeValue + ?Sized> SerializeValue for &T {
    fn serialize<'b>(
        &self,
        typ: &ColumnType,
        writer: CellWriter<'b>,
    ) -> Result<WrittenCellProof<'b>, SerializationError> {
        T::serialize(*self, typ, writer)
    }
}
impl<T: SerializeValue + ?Sized> SerializeValue for Box<T> {
    fn serialize<'b>(
        &self,
        typ: &ColumnType,
        writer: CellWriter<'b>,
    ) -> Result<WrittenCellProof<'b>, SerializationError> {
        T::serialize(&**self, typ, writer)
    }
}
impl<V: SerializeValue, S: BuildHasher + Default> SerializeValue for HashSet<V, S> {
    fn serialize<'b>(
        &self,
        typ: &ColumnType,
        writer: CellWriter<'b>,
    ) -> Result<WrittenCellProof<'b>, SerializationError> {
        serialize_sequence(
            std::any::type_name::<Self>(),
            self.len(),
            self.iter(),
            typ,
            writer,
        )
    }
}
impl<K: SerializeValue, V: SerializeValue, S: BuildHasher> SerializeValue for HashMap<K, V, S> {
    fn serialize<'b>(
        &self,
        typ: &ColumnType,
        writer: CellWriter<'b>,
    ) -> Result<WrittenCellProof<'b>, SerializationError> {
        serialize_mapping(
            std::any::type_name::<Self>(),
            self.len(),
            self.iter(),
            typ,
            writer,
        )
    }
}
impl<V: SerializeValue> SerializeValue for BTreeSet<V> {
    fn serialize<'b>(
        &self,
        typ: &ColumnType,
        writer: CellWriter<'b>,
    ) -> Result<WrittenCellProof<'b>, SerializationError> {
        serialize_sequence(
            std::any::type_name::<Self>(),
            self.len(),
            self.iter(),
            typ,
            writer,
        )
    }
}
impl<K: SerializeValue, V: SerializeValue> SerializeValue for BTreeMap<K, V> {
    fn serialize<'b>(
        &self,
        typ: &ColumnType,
        writer: CellWriter<'b>,
    ) -> Result<WrittenCellProof<'b>, SerializationError> {
        serialize_mapping(
            std::any::type_name::<Self>(),
            self.len(),
            self.iter(),
            typ,
            writer,
        )
    }
}
impl<T: SerializeValue> SerializeValue for Vec<T> {
    fn serialize<'b>(
        &self,
        typ: &ColumnType,
        writer: CellWriter<'b>,
    ) -> Result<WrittenCellProof<'b>, SerializationError> {
        serialize_sequence(
            std::any::type_name::<Self>(),
            self.len(),
            self.iter(),
            typ,
            writer,
        )
    }
}
impl<'a, T: SerializeValue + 'a> SerializeValue for &'a [T] {
    fn serialize<'b>(
        &self,
        typ: &ColumnType,
        writer: CellWriter<'b>,
    ) -> Result<WrittenCellProof<'b>, SerializationError> {
        serialize_sequence(
            std::any::type_name::<Self>(),
            self.len(),
            self.iter(),
            typ,
            writer,
        )
    }
}
impl SerializeValue for CqlValue {
    fn serialize<'b>(
        &self,
        typ: &ColumnType,
        writer: CellWriter<'b>,
    ) -> Result<WrittenCellProof<'b>, SerializationError> {
        serialize_cql_value(self, typ, writer).map_err(fix_cql_value_name_in_err)
    }
}

fn serialize_cql_value<'b>(
    value: &CqlValue,
    typ: &ColumnType,
    writer: CellWriter<'b>,
) -> Result<WrittenCellProof<'b>, SerializationError> {
    if let ColumnType::Custom(_) = typ {
        return Err(mk_typck_err::<CqlValue>(
            typ,
            BuiltinTypeCheckErrorKind::CustomTypeUnsupported,
        ));
    }
    match value {
        CqlValue::Ascii(a) => <_ as SerializeValue>::serialize(&a, typ, writer),
        CqlValue::Boolean(b) => <_ as SerializeValue>::serialize(&b, typ, writer),
        CqlValue::Blob(b) => <_ as SerializeValue>::serialize(&b, typ, writer),
        CqlValue::Counter(c) => <_ as SerializeValue>::serialize(&c, typ, writer),
        CqlValue::Decimal(d) => <_ as SerializeValue>::serialize(&d, typ, writer),
        CqlValue::Date(d) => <_ as SerializeValue>::serialize(&d, typ, writer),
        CqlValue::Double(d) => <_ as SerializeValue>::serialize(&d, typ, writer),
        CqlValue::Duration(d) => <_ as SerializeValue>::serialize(&d, typ, writer),
        CqlValue::Empty => {
            if !typ.supports_special_empty_value() {
                return Err(mk_typck_err::<CqlValue>(
                    typ,
                    BuiltinTypeCheckErrorKind::NotEmptyable,
                ));
            }
            Ok(writer.set_value(&[]).unwrap())
        }
        CqlValue::Float(f) => <_ as SerializeValue>::serialize(&f, typ, writer),
        CqlValue::Int(i) => <_ as SerializeValue>::serialize(&i, typ, writer),
        CqlValue::BigInt(b) => <_ as SerializeValue>::serialize(&b, typ, writer),
        CqlValue::Text(t) => <_ as SerializeValue>::serialize(&t, typ, writer),
        CqlValue::Timestamp(t) => <_ as SerializeValue>::serialize(&t, typ, writer),
        CqlValue::Inet(i) => <_ as SerializeValue>::serialize(&i, typ, writer),
        CqlValue::List(l) => <_ as SerializeValue>::serialize(&l, typ, writer),
        CqlValue::Map(m) => serialize_mapping(
            std::any::type_name::<CqlValue>(),
            m.len(),
            m.iter().map(|p| (&p.0, &p.1)),
            typ,
            writer,
        ),
        CqlValue::Set(s) => <_ as SerializeValue>::serialize(&s, typ, writer),
        CqlValue::UserDefinedType {
            keyspace,
            type_name,
            fields,
        } => serialize_udt(typ, keyspace, type_name, fields, writer),
        CqlValue::SmallInt(s) => <_ as SerializeValue>::serialize(&s, typ, writer),
        CqlValue::TinyInt(t) => <_ as SerializeValue>::serialize(&t, typ, writer),
        CqlValue::Time(t) => <_ as SerializeValue>::serialize(&t, typ, writer),
        CqlValue::Timeuuid(t) => <_ as SerializeValue>::serialize(&t, typ, writer),
        CqlValue::Tuple(t) => {
            // We allow serializing tuples that have less fields
            // than the database tuple, but not the other way around.
            let fields = match typ {
                ColumnType::Tuple(fields) => {
                    if fields.len() < t.len() {
                        return Err(mk_typck_err::<CqlValue>(
                            typ,
                            TupleTypeCheckErrorKind::WrongElementCount {
                                rust_type_el_count: t.len(),
                                cql_type_el_count: fields.len(),
                            },
                        ));
                    }
                    fields
                }
                _ => {
                    return Err(mk_typck_err::<CqlValue>(
                        typ,
                        TupleTypeCheckErrorKind::NotTuple,
                    ))
                }
            };
            serialize_tuple_like(typ, fields.iter(), t.iter(), writer)
        }
        CqlValue::Uuid(u) => <_ as SerializeValue>::serialize(&u, typ, writer),
        CqlValue::Varint(v) => <_ as SerializeValue>::serialize(&v, typ, writer),
    }
}

fn fix_cql_value_name_in_err(mut err: SerializationError) -> SerializationError {
    // The purpose of this function is to change the `rust_name` field
    // in the error to CqlValue. Most of the time, the `err` given to the
    // function here will be the sole owner of the data, so theoretically
    // we could fix this in place.

    let rust_name = std::any::type_name::<CqlValue>();

    match Arc::get_mut(&mut err.0) {
        Some(err_mut) => {
            if let Some(err) = err_mut.downcast_mut::<BuiltinTypeCheckError>() {
                err.rust_name = rust_name;
            } else if let Some(err) = err_mut.downcast_mut::<BuiltinSerializationError>() {
                err.rust_name = rust_name;
            }
        }
        None => {
            // The `None` case shouldn't happen considering how we are using
            // the function in the code now, but let's provide it here anyway
            // for correctness.
            if let Some(err) = err.0.downcast_ref::<BuiltinTypeCheckError>() {
                if err.rust_name != rust_name {
                    return SerializationError::new(BuiltinTypeCheckError {
                        rust_name,
                        ..err.clone()
                    });
                }
            }
            if let Some(err) = err.0.downcast_ref::<BuiltinSerializationError>() {
                if err.rust_name != rust_name {
                    return SerializationError::new(BuiltinSerializationError {
                        rust_name,
                        ..err.clone()
                    });
                }
            }
        }
    };

    err
}

fn serialize_udt<'b>(
    typ: &ColumnType,
    keyspace: &str,
    type_name: &str,
    values: &[(String, Option<CqlValue>)],
    writer: CellWriter<'b>,
) -> Result<WrittenCellProof<'b>, SerializationError> {
    let (dst_type_name, dst_keyspace, field_types) = match typ {
        ColumnType::UserDefinedType {
            type_name,
            keyspace,
            field_types,
        } => (type_name, keyspace, field_types),
        _ => return Err(mk_typck_err::<CqlValue>(typ, UdtTypeCheckErrorKind::NotUdt)),
    };

    if keyspace != dst_keyspace || type_name != dst_type_name {
        return Err(mk_typck_err::<CqlValue>(
            typ,
            UdtTypeCheckErrorKind::NameMismatch {
                keyspace: dst_keyspace.clone().into_owned(),
                type_name: dst_type_name.clone().into_owned(),
            },
        ));
    }

    // Allow columns present in the CQL type which are not present in CqlValue,
    // but not the other way around
    let mut indexed_fields: HashMap<_, _> = values.iter().map(|(k, v)| (k.as_str(), v)).collect();

    let mut builder = writer.into_value_builder();
    for (fname, ftyp) in field_types {
        // Take a value from the original list.
        // If a field is missing, write null instead.
        let fvalue = indexed_fields
            .remove(fname.deref())
            .and_then(|x| x.as_ref());

        let writer = builder.make_sub_writer();
        match fvalue {
            None => writer.set_null(),
            Some(v) => serialize_cql_value(v, ftyp, writer).map_err(|err| {
                let err = fix_cql_value_name_in_err(err);
                mk_ser_err::<CqlValue>(
                    typ,
                    UdtSerializationErrorKind::FieldSerializationFailed {
                        field_name: fname.clone().into_owned(),
                        err,
                    },
                )
            })?,
        };
    }

    // If there are some leftover fields, it's an error.
    if !indexed_fields.is_empty() {
        // In order to have deterministic errors, return an error about
        // the lexicographically smallest field.
        let fname = indexed_fields.keys().min().unwrap();
        return Err(mk_typck_err::<CqlValue>(
            typ,
            UdtTypeCheckErrorKind::NoSuchFieldInUdt {
                field_name: fname.to_string(),
            },
        ));
    }

    builder
        .finish()
        .map_err(|_| mk_ser_err::<CqlValue>(typ, BuiltinSerializationErrorKind::SizeOverflow))
}

fn serialize_tuple_like<'t, 'b>(
    typ: &ColumnType,
    field_types: impl Iterator<Item = &'t ColumnType<'t>>,
    field_values: impl Iterator<Item = &'t Option<CqlValue>>,
    writer: CellWriter<'b>,
) -> Result<WrittenCellProof<'b>, SerializationError> {
    let mut builder = writer.into_value_builder();

    for (index, (el, el_typ)) in field_values.zip(field_types).enumerate() {
        let sub = builder.make_sub_writer();
        match el {
            None => sub.set_null(),
            Some(el) => serialize_cql_value(el, el_typ, sub).map_err(|err| {
                let err = fix_cql_value_name_in_err(err);
                mk_ser_err::<CqlValue>(
                    typ,
                    TupleSerializationErrorKind::ElementSerializationFailed { index, err },
                )
            })?,
        };
    }

    builder
        .finish()
        .map_err(|_| mk_ser_err::<CqlValue>(typ, BuiltinSerializationErrorKind::SizeOverflow))
}

macro_rules! impl_tuple {
    (
        $($typs:ident),*;
        $($fidents:ident),*;
        $($tidents:ident),*;
        $length:expr
    ) => {
        impl<$($typs: SerializeValue),*> SerializeValue for ($($typs,)*) {
            fn serialize<'b>(
                &self,
                typ: &ColumnType,
                writer: CellWriter<'b>,
            ) -> Result<WrittenCellProof<'b>, SerializationError> {
                let ($($tidents,)*) = match typ {
                    ColumnType::Tuple(typs) => match typs.as_slice() {
                        [$($tidents),*] => ($($tidents,)*),
                        _ => return Err(mk_typck_err::<Self>(
                            typ,
                            TupleTypeCheckErrorKind::WrongElementCount {
                                rust_type_el_count: $length,
                                cql_type_el_count: typs.len(),
                            }
                        ))
                    }
                    _ => return Err(mk_typck_err::<Self>(
                        typ,
                        TupleTypeCheckErrorKind::NotTuple,
                    ))
                };
                let ($($fidents,)*) = self;
                let mut builder = writer.into_value_builder();
                let index = 0;
                $(
                    <$typs as SerializeValue>::serialize($fidents, $tidents, builder.make_sub_writer())
                        .map_err(|err| mk_ser_err::<Self>(
                            typ,
                            TupleSerializationErrorKind::ElementSerializationFailed {
                                index,
                                err,
                            }
                        ))?;
                    let index = index + 1;
                )*
                let _ = index;
                builder
                    .finish()
                    .map_err(|_| mk_ser_err::<Self>(typ, BuiltinSerializationErrorKind::SizeOverflow))
            }
        }
    };
}

macro_rules! impl_tuples {
    (;;;$length:expr) => {};
    (
        $typ:ident$(, $($typs:ident),*)?;
        $fident:ident$(, $($fidents:ident),*)?;
        $tident:ident$(, $($tidents:ident),*)?;
        $length:expr
    ) => {
        impl_tuples!(
            $($($typs),*)?;
            $($($fidents),*)?;
            $($($tidents),*)?;
            $length - 1
        );
        impl_tuple!(
            $typ$(, $($typs),*)?;
            $fident$(, $($fidents),*)?;
            $tident$(, $($tidents),*)?;
            $length
        );
    };
}

impl_tuples!(
    T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15;
    f0, f1, f2, f3, f4, f5, f6, f7, f8, f9, f10, f11, f12, f13, f14, f15;
    t0, t1, t2, t3, t4, t5, t6, t7, t8, t9, t10, t11, t12, t13, t14, t15;
    16
);

fn serialize_sequence<'t, 'b, T: SerializeValue + 't>(
    rust_name: &'static str,
    len: usize,
    iter: impl Iterator<Item = &'t T>,
    typ: &ColumnType,
    writer: CellWriter<'b>,
) -> Result<WrittenCellProof<'b>, SerializationError> {
    let elt = match typ {
        ColumnType::List(elt) | ColumnType::Set(elt) => elt,
        _ => {
            return Err(mk_typck_err_named(
                rust_name,
                typ,
                SetOrListTypeCheckErrorKind::NotSetOrList,
            ));
        }
    };

    let mut builder = writer.into_value_builder();

    let element_count: i32 = len.try_into().map_err(|_| {
        mk_ser_err_named(
            rust_name,
            typ,
            SetOrListSerializationErrorKind::TooManyElements,
        )
    })?;
    builder.append_bytes(&element_count.to_be_bytes());

    for el in iter {
        T::serialize(el, elt, builder.make_sub_writer()).map_err(|err| {
            mk_ser_err_named(
                rust_name,
                typ,
                SetOrListSerializationErrorKind::ElementSerializationFailed(err),
            )
        })?;
    }

    builder
        .finish()
        .map_err(|_| mk_ser_err_named(rust_name, typ, BuiltinSerializationErrorKind::SizeOverflow))
}

fn serialize_mapping<'t, 'b, K: SerializeValue + 't, V: SerializeValue + 't>(
    rust_name: &'static str,
    len: usize,
    iter: impl Iterator<Item = (&'t K, &'t V)>,
    typ: &ColumnType,
    writer: CellWriter<'b>,
) -> Result<WrittenCellProof<'b>, SerializationError> {
    let (ktyp, vtyp) = match typ {
        ColumnType::Map(k, v) => (k, v),
        _ => {
            return Err(mk_typck_err_named(
                rust_name,
                typ,
                MapTypeCheckErrorKind::NotMap,
            ));
        }
    };

    let mut builder = writer.into_value_builder();

    let element_count: i32 = len.try_into().map_err(|_| {
        mk_ser_err_named(rust_name, typ, MapSerializationErrorKind::TooManyElements)
    })?;
    builder.append_bytes(&element_count.to_be_bytes());

    for (k, v) in iter {
        K::serialize(k, ktyp, builder.make_sub_writer()).map_err(|err| {
            mk_ser_err_named(
                rust_name,
                typ,
                MapSerializationErrorKind::KeySerializationFailed(err),
            )
        })?;
        V::serialize(v, vtyp, builder.make_sub_writer()).map_err(|err| {
            mk_ser_err_named(
                rust_name,
                typ,
                MapSerializationErrorKind::ValueSerializationFailed(err),
            )
        })?;
    }

    builder
        .finish()
        .map_err(|_| mk_ser_err_named(rust_name, typ, BuiltinSerializationErrorKind::SizeOverflow))
}

/// Implements the [`SerializeValue`] trait for a type, provided that the type
/// already implements the legacy [`Value`](crate::frame::value::Value) trait.
///
/// # Note
///
/// The translation from one trait to another encounters a performance penalty
/// and does not utilize the stronger guarantees of `SerializeValue`. Before
/// resorting to this macro, you should consider other options instead:
///
/// - If the impl was generated using the `Value` procedural macro, you should
///   switch to the `SerializeValue` procedural macro. *The new macro behaves
///   differently by default, so please read its documentation first!*
/// - If the impl was written by hand, it is still preferable to rewrite it
///   manually. You have an opportunity to make your serialization logic
///   type-safe and potentially improve performance.
///
/// Basically, you should consider using the macro if you have a hand-written
/// impl and the moment it is not easy/not desirable to rewrite it.
///
/// # Example
///
/// ```rust
/// # use scylla_cql::frame::value::{Value, ValueTooBig};
/// # use scylla_cql::impl_serialize_value_via_value;
/// struct NoGenerics {}
/// impl Value for NoGenerics {
///     fn serialize<'b>(&self, _buf: &mut Vec<u8>) -> Result<(), ValueTooBig> {
///         Ok(())
///     }
/// }
/// impl_serialize_value_via_value!(NoGenerics);
///
/// // Generic types are also supported. You must specify the bounds if the
/// // struct/enum contains any.
/// struct WithGenerics<T, U: Clone>(T, U);
/// impl<T: Value, U: Clone + Value> Value for WithGenerics<T, U> {
///     fn serialize<'b>(&self, buf: &mut Vec<u8>) -> Result<(), ValueTooBig> {
///         self.0.serialize(buf)?;
///         self.1.clone().serialize(buf)?;
///         Ok(())
///     }
/// }
/// impl_serialize_value_via_value!(WithGenerics<T, U: Clone>);
/// ```
#[deprecated(
    since = "0.15.1",
    note = "Legacy serialization API is not type-safe and is going to be removed soon"
)]
#[macro_export]
macro_rules! impl_serialize_value_via_value {
    ($t:ident$(<$($targ:tt $(: $tbound:tt)?),*>)?) => {
        impl $(<$($targ $(: $tbound)?),*>)? $crate::serialize::value::SerializeValue
        for $t$(<$($targ),*>)?
        where
            Self: $crate::frame::value::Value,
        {
            fn serialize<'b>(
                &self,
                _typ: &$crate::frame::response::result::ColumnType,
                writer: $crate::serialize::writers::CellWriter<'b>,
            ) -> ::std::result::Result<
                $crate::serialize::writers::WrittenCellProof<'b>,
                $crate::serialize::SerializationError,
            > {
                $crate::serialize::value::serialize_legacy_value(self, writer)
            }
        }
    };
}

/// Implements [`SerializeValue`] if the type wrapped over implements [`Value`].
///
/// See the [`impl_serialize_value_via_value`] macro on information about
/// the properties of the [`SerializeValue`] implementation.
#[deprecated(
    since = "0.15.1",
    note = "Legacy serialization API is not type-safe and is going to be removed soon"
)]
pub struct ValueAdapter<T>(pub T);

#[allow(deprecated)]
impl<T> SerializeValue for ValueAdapter<T>
where
    T: Value,
{
    #[inline]
    fn serialize<'b>(
        &self,
        _typ: &ColumnType,
        writer: CellWriter<'b>,
    ) -> Result<WrittenCellProof<'b>, SerializationError> {
        serialize_legacy_value(&self.0, writer)
    }
}

/// Serializes a value implementing [`Value`] by using the [`CellWriter`]
/// interface.
///
/// The function first serializes the value with [`Value::serialize`], then
/// parses the result and serializes it again with given `CellWriter`. It is
/// a lazy and inefficient way to implement `CellWriter` via an existing `Value`
/// impl.
///
/// Returns an error if the result of the `Value::serialize` call was not
/// a properly encoded `[value]` as defined in the CQL protocol spec.
///
/// See [`impl_serialize_value_via_value`] which generates a boilerplate
/// [`SerializeValue`] implementation that uses this function.
#[deprecated(
    since = "0.15.1",
    note = "Legacy serialization API is not type-safe and is going to be removed soon"
)]
#[allow(deprecated)]
pub fn serialize_legacy_value<'b, T: Value>(
    v: &T,
    writer: CellWriter<'b>,
) -> Result<WrittenCellProof<'b>, SerializationError> {
    // It's an inefficient and slightly tricky but correct implementation.
    let mut buf = Vec::new();
    <T as Value>::serialize(v, &mut buf)
        .map_err(|_| SerializationError::new(ValueToSerializeValueAdapterError::TooBig))?;

    // Analyze the output.
    // All this dance shows how unsafe our previous interface was...
    if buf.len() < 4 {
        return Err(SerializationError(Arc::new(
            ValueToSerializeValueAdapterError::TooShort { size: buf.len() },
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
                    ValueToSerializeValueAdapterError::DeclaredVsActualSizeMismatch {
                        declared: len as usize,
                        actual: contents.len(),
                    },
                )))
            } else {
                Ok(writer.set_value(contents).unwrap()) // len <= i32::MAX, so unwrap will succeed
            }
        }
        _ => Err(SerializationError(Arc::new(
            ValueToSerializeValueAdapterError::InvalidDeclaredSize { size: len },
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
    pub got: ColumnType<'static>,

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
        got: got.clone().into_owned(),
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
    pub got: ColumnType<'static>,

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
        got: got.clone().into_owned(),
        kind: kind.into(),
    })
}

/// Describes why type checking some of the built-in types has failed.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum BuiltinTypeCheckErrorKind {
    /// Expected one from a list of particular types.
    MismatchedType {
        /// The list of types that the Rust type can serialize as.
        expected: &'static [ColumnType<'static>],
    },

    /// Expected a type that can be empty.
    NotEmptyable,

    /// A type check failure specific to a CQL set or list.
    SetOrListError(SetOrListTypeCheckErrorKind),

    /// A type check failure specific to a CQL map.
    MapError(MapTypeCheckErrorKind),

    /// A type check failure specific to a CQL tuple.
    TupleError(TupleTypeCheckErrorKind),

    /// A type check failure specific to a CQL UDT.
    UdtError(UdtTypeCheckErrorKind),

    /// Custom CQL type - unsupported
    // TODO: Should we actually support it? Counters used to be implemented like that.
    CustomTypeUnsupported,
}

impl From<SetOrListTypeCheckErrorKind> for BuiltinTypeCheckErrorKind {
    fn from(value: SetOrListTypeCheckErrorKind) -> Self {
        BuiltinTypeCheckErrorKind::SetOrListError(value)
    }
}

impl From<MapTypeCheckErrorKind> for BuiltinTypeCheckErrorKind {
    fn from(value: MapTypeCheckErrorKind) -> Self {
        BuiltinTypeCheckErrorKind::MapError(value)
    }
}

impl From<TupleTypeCheckErrorKind> for BuiltinTypeCheckErrorKind {
    fn from(value: TupleTypeCheckErrorKind) -> Self {
        BuiltinTypeCheckErrorKind::TupleError(value)
    }
}

impl From<UdtTypeCheckErrorKind> for BuiltinTypeCheckErrorKind {
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
            BuiltinTypeCheckErrorKind::NotEmptyable => {
                f.write_str("the separate empty representation is not valid for this type")
            }
            BuiltinTypeCheckErrorKind::SetOrListError(err) => err.fmt(f),
            BuiltinTypeCheckErrorKind::MapError(err) => err.fmt(f),
            BuiltinTypeCheckErrorKind::TupleError(err) => err.fmt(f),
            BuiltinTypeCheckErrorKind::UdtError(err) => err.fmt(f),
            BuiltinTypeCheckErrorKind::CustomTypeUnsupported => {
                f.write_str("custom CQL types are unsupported")
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

    /// The Rust value is out of range supported by the CQL type.
    ValueOverflow,

    /// A serialization failure specific to a CQL set or list.
    SetOrListError(SetOrListSerializationErrorKind),

    /// A serialization failure specific to a CQL map.
    MapError(MapSerializationErrorKind),

    /// A serialization failure specific to a CQL tuple.
    TupleError(TupleSerializationErrorKind),

    /// A serialization failure specific to a CQL UDT.
    UdtError(UdtSerializationErrorKind),
}

impl From<SetOrListSerializationErrorKind> for BuiltinSerializationErrorKind {
    fn from(value: SetOrListSerializationErrorKind) -> Self {
        BuiltinSerializationErrorKind::SetOrListError(value)
    }
}

impl From<MapSerializationErrorKind> for BuiltinSerializationErrorKind {
    fn from(value: MapSerializationErrorKind) -> Self {
        BuiltinSerializationErrorKind::MapError(value)
    }
}

impl From<TupleSerializationErrorKind> for BuiltinSerializationErrorKind {
    fn from(value: TupleSerializationErrorKind) -> Self {
        BuiltinSerializationErrorKind::TupleError(value)
    }
}

impl From<UdtSerializationErrorKind> for BuiltinSerializationErrorKind {
    fn from(value: UdtSerializationErrorKind) -> Self {
        BuiltinSerializationErrorKind::UdtError(value)
    }
}

impl Display for BuiltinSerializationErrorKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            BuiltinSerializationErrorKind::SizeOverflow => {
                f.write_str("the Rust value is too big to be serialized in the CQL protocol format")
            }
            BuiltinSerializationErrorKind::ValueOverflow => {
                f.write_str("the Rust value is out of range supported by the CQL type")
            }
            BuiltinSerializationErrorKind::SetOrListError(err) => err.fmt(f),
            BuiltinSerializationErrorKind::MapError(err) => err.fmt(f),
            BuiltinSerializationErrorKind::TupleError(err) => err.fmt(f),
            BuiltinSerializationErrorKind::UdtError(err) => err.fmt(f),
        }
    }
}

/// Describes why type checking of a map type failed.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum MapTypeCheckErrorKind {
    /// The CQL type is not a map.
    NotMap,
}

impl Display for MapTypeCheckErrorKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            MapTypeCheckErrorKind::NotMap => f.write_str(
                "the CQL type the Rust type was attempted to be type checked against was not a map",
            ),
        }
    }
}

/// Describes why serialization of a map type failed.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum MapSerializationErrorKind {
    /// The many contains too many items, exceeding the protocol limit (i32::MAX).
    TooManyElements,

    /// One of the keys in the map failed to serialize.
    KeySerializationFailed(SerializationError),

    /// One of the values in the map failed to serialize.
    ValueSerializationFailed(SerializationError),
}

impl Display for MapSerializationErrorKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            MapSerializationErrorKind::TooManyElements => {
                f.write_str("the map contains too many elements to fit in CQL representation")
            }
            MapSerializationErrorKind::KeySerializationFailed(err) => {
                write!(f, "failed to serialize one of the keys: {}", err)
            }
            MapSerializationErrorKind::ValueSerializationFailed(err) => {
                write!(f, "failed to serialize one of the values: {}", err)
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
}

impl Display for SetOrListTypeCheckErrorKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SetOrListTypeCheckErrorKind::NotSetOrList => {
                f.write_str("the CQL type the Rust type was attempted to be type checked against was neither a set or a list")
            }
        }
    }
}

/// Describes why serialization of a set or list type failed.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum SetOrListSerializationErrorKind {
    /// The set/list contains too many items, exceeding the protocol limit (i32::MAX).
    TooManyElements,

    /// One of the elements of the set/list failed to serialize.
    ElementSerializationFailed(SerializationError),
}

impl Display for SetOrListSerializationErrorKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SetOrListSerializationErrorKind::TooManyElements => f.write_str(
                "the collection contains too many elements to fit in CQL representation",
            ),
            SetOrListSerializationErrorKind::ElementSerializationFailed(err) => {
                write!(f, "failed to serialize one of the elements: {err}")
            }
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
    ///
    /// Note that it is allowed to write a Rust tuple with less elements
    /// than the corresponding CQL type, but not more. The additional, unknown
    /// elements will be set to null.
    WrongElementCount {
        /// The number of elements that the Rust tuple has.
        rust_type_el_count: usize,

        /// The number of elements that the CQL tuple type has.
        cql_type_el_count: usize,
    },
}

impl Display for TupleTypeCheckErrorKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TupleTypeCheckErrorKind::NotTuple => f.write_str(
                "the CQL type the Rust type was attempted to be type checked against is not a tuple"
            ),
            TupleTypeCheckErrorKind::WrongElementCount { rust_type_el_count, cql_type_el_count } => write!(
                f,
                "wrong tuple element count: CQL type has {cql_type_el_count}, the Rust tuple has {rust_type_el_count}"
            ),
        }
    }
}

/// Describes why serialize of a tuple failed.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum TupleSerializationErrorKind {
    /// One of the tuple elements failed to serialize.
    ElementSerializationFailed {
        /// Index of the tuple element that failed to serialize.
        index: usize,

        /// The error that caused the tuple field serialization to fail.
        err: SerializationError,
    },
}

impl Display for TupleSerializationErrorKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TupleSerializationErrorKind::ElementSerializationFailed { index, err } => {
                write!(f, "element no. {index} failed to serialize: {err}")
            }
        }
    }
}

/// Describes why type checking of a user defined type failed.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum UdtTypeCheckErrorKind {
    /// The CQL type is not a user defined type.
    NotUdt,

    /// The name of the UDT being serialized to does not match.
    NameMismatch {
        /// Keyspace in which the UDT was defined.
        keyspace: String,

        /// Name of the UDT.
        type_name: String,
    },

    /// The Rust data does not have a field that is required in the CQL UDT type.
    ValueMissingForUdtField {
        /// Name of field that the CQL UDT requires but is missing in the Rust struct.
        field_name: String,
    },

    /// The Rust data contains a field that is not present in the UDT.
    NoSuchFieldInUdt {
        /// Name of the Rust struct field that is missing in the UDT.
        field_name: String,
    },

    /// A different field name was expected at given position.
    FieldNameMismatch {
        /// The name of the Rust field.
        rust_field_name: String,

        /// The name of the CQL UDT field.
        db_field_name: String,
    },
}

impl Display for UdtTypeCheckErrorKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            UdtTypeCheckErrorKind::NotUdt => f.write_str("the CQL type the Rust type was attempted to be type checked against is not a UDT"),
            UdtTypeCheckErrorKind::NameMismatch {
                keyspace,
                type_name,
            } => write!(
                f,
                "the Rust UDT name does not match the actual CQL UDT name ({keyspace}.{type_name})"
            ),
            UdtTypeCheckErrorKind::ValueMissingForUdtField { field_name } => {
                write!(f, "the field {field_name} is missing in the Rust data but is required by the CQL UDT type")
            }
            UdtTypeCheckErrorKind::NoSuchFieldInUdt { field_name } => write!(
                f,
                "the field {field_name} that is present in the Rust data is not present in the CQL type"
            ),
            UdtTypeCheckErrorKind::FieldNameMismatch { rust_field_name, db_field_name } => write!(
                f,
                "expected field with name {db_field_name} at given position, but the Rust field name is {rust_field_name}"
            ),
        }
    }
}

/// Describes why serialization of a user defined type failed.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum UdtSerializationErrorKind {
    /// One of the fields failed to serialize.
    FieldSerializationFailed {
        /// Name of the field which failed to serialize.
        field_name: String,

        /// The error that caused the UDT field serialization to fail.
        err: SerializationError,
    },
}

impl Display for UdtSerializationErrorKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            UdtSerializationErrorKind::FieldSerializationFailed { field_name, err } => {
                write!(f, "field {field_name} failed to serialize: {err}")
            }
        }
    }
}

/// Describes a failure to translate the output of the [`Value`] legacy trait
/// into an output of the [`SerializeValue`] trait.
#[deprecated(
    since = "0.15.1",
    note = "Legacy serialization API is not type-safe and is going to be removed soon"
)]
#[allow(deprecated)]
#[derive(Error, Debug)]
pub enum ValueToSerializeValueAdapterError {
    /// The value is too bit to be serialized as it exceeds the maximum 2GB size limit.
    #[error("The value is too big to be serialized as it exceeds the maximum 2GB size limit")]
    TooBig,

    /// Output produced by the Value trait is less than 4 bytes in size and cannot be considered to be a proper CQL-encoded value.
    #[error("Output produced by the Value trait is too short to be considered a value: {size} < 4 minimum bytes")]
    TooShort {
        /// Size of the produced data.
        size: usize,
    },

    /// Mismatch between the value size written at the beginning and the actual size of the data appended to the Vec.
    #[error("Mismatch between the declared value size vs. actual size: {declared} != {actual}")]
    DeclaredVsActualSizeMismatch {
        /// The declared size of the output.
        declared: usize,

        /// The actual size of the output.
        actual: usize,
    },

    /// The value size written at the beginning is invalid (it is negative and less than -2).
    #[error("Invalid declared value size: {size}")]
    InvalidDeclaredSize {
        /// Declared size of the output.
        size: i32,
    },
}

mod doctests {
    /// ```compile_fail
    ///
    /// #[derive(scylla_macros::SerializeValue)]
    /// #[scylla(crate = scylla_cql, skip_name_checks)]
    /// struct TestUdt {}
    /// ```
    fn _test_udt_bad_attributes_skip_name_check_requires_enforce_order() {}

    /// ```compile_fail
    ///
    /// #[derive(scylla_macros::SerializeValue)]
    /// #[scylla(crate = scylla_cql, flavor = "enforce_order", skip_name_checks)]
    /// struct TestUdt {
    ///     #[scylla(rename = "b")]
    ///     a: i32,
    /// }
    /// ```
    fn _test_udt_bad_attributes_skip_name_check_conflicts_with_rename() {}

    /// ```compile_fail
    ///
    /// #[derive(scylla_macros::SerializeValue)]
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
    /// #[derive(scylla_macros::SerializeValue)]
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
    /// #[derive(scylla_macros::SerializeValue)]
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
    ///
    /// #[derive(scylla_macros::SerializeValue)]
    /// #[scylla(crate = scylla_cql, flavor = "enforce_order", skip_name_checks)]
    /// struct TestUdt {
    ///     a: i32,
    ///     #[scylla(allow_missing)]
    ///     b: bool,
    ///     #[scylla(allow_missing)]
    ///     c: String,
    /// }
    /// ```
    fn _test_udt_good_attributes_name_skip_name_checks_limitations_on_allow_missing() {}

    /// ```
    /// #[derive(scylla_macros::SerializeValue)]
    /// #[scylla(crate = scylla_cql)]
    /// struct TestUdt {
    ///     a: i32,
    ///     #[scylla(allow_missing)]
    ///     b: bool,
    ///     c: String,
    /// }
    /// ```
    fn _test_udt_unordered_flavour_no_limitations_on_allow_missing() {}

    /// ```
    /// #[derive(scylla_macros::SerializeValue)]
    /// #[scylla(crate = scylla_cql)]
    /// struct TestUdt {
    ///     a: i32,
    ///     #[scylla(default_when_null)]
    ///     b: bool,
    ///     c: String,
    /// }
    /// ```
    fn _test_udt_default_when_null_is_accepted() {}
}

#[cfg(test)]
pub(crate) mod tests {
    use std::collections::BTreeMap;

    use crate::frame::response::result::{ColumnType, CqlValue, NativeType};
    #[allow(deprecated)]
    use crate::frame::value::{Counter, MaybeUnset, Unset, Value, ValueTooBig};
    #[allow(deprecated)]
    use crate::serialize::value::{
        BuiltinSerializationError, BuiltinSerializationErrorKind, BuiltinTypeCheckError,
        BuiltinTypeCheckErrorKind, MapSerializationErrorKind, MapTypeCheckErrorKind,
        SetOrListSerializationErrorKind, SetOrListTypeCheckErrorKind, TupleSerializationErrorKind,
        TupleTypeCheckErrorKind, ValueAdapter,
    };
    use crate::serialize::{CellWriter, SerializationError};

    use assert_matches::assert_matches;
    use scylla_macros::SerializeValue;

    use super::{SerializeValue, UdtSerializationErrorKind, UdtTypeCheckErrorKind};

    #[allow(deprecated)]
    fn check_compat<V: Value + SerializeValue>(v: V) {
        let mut legacy_data = Vec::new();
        <V as Value>::serialize(&v, &mut legacy_data).unwrap();

        let mut new_data = Vec::new();
        let new_data_writer = CellWriter::new(&mut new_data);
        <V as SerializeValue>::serialize(&v, &ColumnType::Native(NativeType::Int), new_data_writer)
            .unwrap();

        assert_eq!(legacy_data, new_data);
    }

    #[test]
    fn test_legacy_fallback() {
        check_compat(123i32);
        check_compat(None::<i32>);
        check_compat(MaybeUnset::Unset::<i32>);
    }

    #[test]
    fn test_dyn_serialize_value() {
        let v: i32 = 123;
        let mut typed_data = Vec::new();
        let typed_data_writer = CellWriter::new(&mut typed_data);
        <_ as SerializeValue>::serialize(
            &v,
            &ColumnType::Native(NativeType::Int),
            typed_data_writer,
        )
        .unwrap();

        let v = &v as &dyn SerializeValue;
        let mut erased_data = Vec::new();
        let erased_data_writer = CellWriter::new(&mut erased_data);
        <_ as SerializeValue>::serialize(
            &v,
            &ColumnType::Native(NativeType::Int),
            erased_data_writer,
        )
        .unwrap();

        assert_eq!(typed_data, erased_data);
    }

    fn do_serialize_result<T: SerializeValue>(
        t: T,
        typ: &ColumnType,
    ) -> Result<Vec<u8>, SerializationError> {
        let mut ret = Vec::new();
        let writer = CellWriter::new(&mut ret);
        t.serialize(typ, writer).map(|_| ()).map(|()| ret)
    }

    pub(crate) fn do_serialize<T: SerializeValue>(t: T, typ: &ColumnType) -> Vec<u8> {
        do_serialize_result(t, typ).unwrap()
    }

    fn do_serialize_err<T: SerializeValue>(t: T, typ: &ColumnType) -> SerializationError {
        do_serialize_result(t, typ).unwrap_err()
    }

    #[allow(deprecated)]
    #[test]
    fn test_legacy_wrapper() {
        struct Foo;
        impl Value for Foo {
            fn serialize(&self, buf: &mut Vec<u8>) -> Result<(), ValueTooBig> {
                let s = "Ala ma kota";
                buf.extend_from_slice(&(s.len() as i32).to_be_bytes());
                buf.extend_from_slice(s.as_bytes());
                Ok(())
            }
        }

        let buf = do_serialize(ValueAdapter(Foo), &ColumnType::Native(NativeType::Text));
        let expected = vec![
            0, 0, 0, 11, // Length of the value
            65, 108, 97, 32, 109, 97, 32, 107, 111, 116, 97, // The string
        ];
        assert_eq!(buf, expected);
    }

    fn get_typeck_err(err: &SerializationError) -> &BuiltinTypeCheckError {
        match err.0.downcast_ref() {
            Some(err) => err,
            None => panic!("not a BuiltinTypeCheckError: {}", err),
        }
    }

    fn get_ser_err(err: &SerializationError) -> &BuiltinSerializationError {
        match err.0.downcast_ref() {
            Some(err) => err,
            None => panic!("not a BuiltinSerializationError: {}", err),
        }
    }

    #[test]
    fn test_native_errors() {
        // Simple type mismatch
        let v = 123_i32;
        let err = do_serialize_err(v, &ColumnType::Native(NativeType::Double));
        let err = get_typeck_err(&err);
        assert_eq!(err.rust_name, std::any::type_name::<i32>());
        assert_eq!(err.got, ColumnType::Native(NativeType::Double));
        assert_matches!(
            err.kind,
            BuiltinTypeCheckErrorKind::MismatchedType {
                expected: &[ColumnType::Native(NativeType::Int)],
            }
        );

        // str (and also Uuid) are interesting because they accept two types,
        // also check str here
        let v = "Ala ma kota";
        let err = do_serialize_err(v, &ColumnType::Native(NativeType::Double));
        let err = get_typeck_err(&err);
        assert_eq!(err.rust_name, std::any::type_name::<&str>());
        assert_eq!(err.got, ColumnType::Native(NativeType::Double));
        assert_matches!(
            err.kind,
            BuiltinTypeCheckErrorKind::MismatchedType {
                expected: &[
                    ColumnType::Native(NativeType::Ascii),
                    ColumnType::Native(NativeType::Text)
                ],
            }
        );

        // We'll skip testing for SizeOverflow as this would require producing
        // a value which is at least 2GB in size.
    }

    #[cfg(feature = "bigdecimal-04")]
    #[test]
    fn test_native_errors_bigdecimal_04() {
        use bigdecimal_04::num_bigint::BigInt;
        use bigdecimal_04::BigDecimal;

        // Value overflow (type out of representable range)
        let v = BigDecimal::new(BigInt::from(123), 1i64 << 40);
        let err = do_serialize_err(v, &ColumnType::Native(NativeType::Decimal));
        let err = get_ser_err(&err);
        assert_eq!(err.rust_name, std::any::type_name::<BigDecimal>());
        assert_eq!(err.got, ColumnType::Native(NativeType::Decimal));
        assert_matches!(err.kind, BuiltinSerializationErrorKind::ValueOverflow);
    }

    #[test]
    fn test_set_or_list_errors() {
        // Not a set or list
        let v = vec![123_i32];
        let err = do_serialize_err(v, &ColumnType::Native(NativeType::Double));
        let err = get_typeck_err(&err);
        assert_eq!(err.rust_name, std::any::type_name::<Vec<i32>>());
        assert_eq!(err.got, ColumnType::Native(NativeType::Double));
        assert_matches!(
            err.kind,
            BuiltinTypeCheckErrorKind::SetOrListError(SetOrListTypeCheckErrorKind::NotSetOrList)
        );

        // Trick: Unset is a ZST, so [Unset; 1 << 33] is a ZST, too.
        // While it's probably incorrect to use Unset in a collection, this
        // allows us to trigger the right error without going out of memory.
        // Such an array is also created instantaneously.
        let v = &[Unset; 1 << 33] as &[Unset];
        let typ = ColumnType::List(Box::new(ColumnType::Native(NativeType::Int)));
        let err = do_serialize_err(v, &typ);
        let err = get_ser_err(&err);
        assert_eq!(err.rust_name, std::any::type_name::<&[Unset]>());
        assert_eq!(err.got, typ);
        assert_matches!(
            err.kind,
            BuiltinSerializationErrorKind::SetOrListError(
                SetOrListSerializationErrorKind::TooManyElements
            )
        );

        // Error during serialization of an element
        let v = vec![123_i32];
        let typ = ColumnType::List(Box::new(ColumnType::Native(NativeType::Double)));
        let err = do_serialize_err(v, &typ);
        let err = get_ser_err(&err);
        assert_eq!(err.rust_name, std::any::type_name::<Vec<i32>>());
        assert_eq!(err.got, typ);
        let BuiltinSerializationErrorKind::SetOrListError(
            SetOrListSerializationErrorKind::ElementSerializationFailed(err),
        ) = &err.kind
        else {
            panic!("unexpected error kind: {}", err.kind)
        };
        let err = get_typeck_err(err);
        assert_matches!(
            err.kind,
            BuiltinTypeCheckErrorKind::MismatchedType {
                expected: &[ColumnType::Native(NativeType::Int)],
            }
        );
    }

    #[test]
    fn test_map_errors() {
        // Not a map
        let v = BTreeMap::from([("foo", "bar")]);
        let err = do_serialize_err(v, &ColumnType::Native(NativeType::Double));
        let err = get_typeck_err(&err);
        assert_eq!(err.rust_name, std::any::type_name::<BTreeMap<&str, &str>>());
        assert_eq!(err.got, ColumnType::Native(NativeType::Double));
        assert_matches!(
            err.kind,
            BuiltinTypeCheckErrorKind::MapError(MapTypeCheckErrorKind::NotMap)
        );

        // It's not practical to check the TooManyElements error as it would
        // require allocating a huge amount of memory.

        // Error during serialization of a key
        let v = BTreeMap::from([(123_i32, 456_i32)]);
        let typ = ColumnType::Map(
            Box::new(ColumnType::Native(NativeType::Double)),
            Box::new(ColumnType::Native(NativeType::Int)),
        );
        let err = do_serialize_err(v, &typ);
        let err = get_ser_err(&err);
        assert_eq!(err.rust_name, std::any::type_name::<BTreeMap<i32, i32>>());
        assert_eq!(err.got, typ);
        let BuiltinSerializationErrorKind::MapError(
            MapSerializationErrorKind::KeySerializationFailed(err),
        ) = &err.kind
        else {
            panic!("unexpected error kind: {}", err.kind)
        };
        let err = get_typeck_err(err);
        assert_matches!(
            err.kind,
            BuiltinTypeCheckErrorKind::MismatchedType {
                expected: &[ColumnType::Native(NativeType::Int)],
            }
        );

        // Error during serialization of a value
        let v = BTreeMap::from([(123_i32, 456_i32)]);
        let typ = ColumnType::Map(
            Box::new(ColumnType::Native(NativeType::Int)),
            Box::new(ColumnType::Native(NativeType::Double)),
        );
        let err = do_serialize_err(v, &typ);
        let err = get_ser_err(&err);
        assert_eq!(err.rust_name, std::any::type_name::<BTreeMap<i32, i32>>());
        assert_eq!(err.got, typ);
        let BuiltinSerializationErrorKind::MapError(
            MapSerializationErrorKind::ValueSerializationFailed(err),
        ) = &err.kind
        else {
            panic!("unexpected error kind: {}", err.kind)
        };
        let err = get_typeck_err(err);
        assert_matches!(
            err.kind,
            BuiltinTypeCheckErrorKind::MismatchedType {
                expected: &[ColumnType::Native(NativeType::Int)],
            }
        );
    }

    #[test]
    fn test_tuple_errors() {
        // Not a tuple
        let v = (123_i32, 456_i32, 789_i32);
        let err = do_serialize_err(v, &ColumnType::Native(NativeType::Double));
        let err = get_typeck_err(&err);
        assert_eq!(err.rust_name, std::any::type_name::<(i32, i32, i32)>());
        assert_eq!(err.got, ColumnType::Native(NativeType::Double));
        assert_matches!(
            err.kind,
            BuiltinTypeCheckErrorKind::TupleError(TupleTypeCheckErrorKind::NotTuple)
        );

        // The Rust tuple has more elements than the CQL type
        let v = (123_i32, 456_i32, 789_i32);
        let typ = ColumnType::Tuple(vec![ColumnType::Native(NativeType::Int); 2]);
        let err = do_serialize_err(v, &typ);
        let err = get_typeck_err(&err);
        assert_eq!(err.rust_name, std::any::type_name::<(i32, i32, i32)>());
        assert_eq!(err.got, typ);
        assert_matches!(
            err.kind,
            BuiltinTypeCheckErrorKind::TupleError(TupleTypeCheckErrorKind::WrongElementCount {
                rust_type_el_count: 3,
                cql_type_el_count: 2,
            })
        );

        // Error during serialization of one of the elements
        let v = (123_i32, "Ala ma kota", 789.0_f64);
        let typ = ColumnType::Tuple(vec![
            ColumnType::Native(NativeType::Int),
            ColumnType::Native(NativeType::Text),
            ColumnType::Native(NativeType::Uuid),
        ]);
        let err = do_serialize_err(v, &typ);
        let err = get_ser_err(&err);
        assert_eq!(err.rust_name, std::any::type_name::<(i32, &str, f64)>());
        assert_eq!(err.got, typ);
        let BuiltinSerializationErrorKind::TupleError(
            TupleSerializationErrorKind::ElementSerializationFailed { index: 2, err },
        ) = &err.kind
        else {
            panic!("unexpected error kind: {}", err.kind)
        };
        let err = get_typeck_err(err);
        assert_matches!(
            err.kind,
            BuiltinTypeCheckErrorKind::MismatchedType {
                expected: &[ColumnType::Native(NativeType::Double)],
            }
        );
    }

    #[test]
    fn test_cql_value_errors() {
        // Tried to encode Empty value into a non-emptyable type
        let v = CqlValue::Empty;
        let err = do_serialize_err(v, &ColumnType::Native(NativeType::Counter));
        let err = get_typeck_err(&err);
        assert_eq!(err.rust_name, std::any::type_name::<CqlValue>());
        assert_eq!(err.got, ColumnType::Native(NativeType::Counter));
        assert_matches!(err.kind, BuiltinTypeCheckErrorKind::NotEmptyable);

        // Handle tuples and UDTs in separate tests, as they have some
        // custom logic
    }

    #[test]
    fn test_cql_value_tuple_errors() {
        // Not a tuple
        let v = CqlValue::Tuple(vec![
            Some(CqlValue::Int(123_i32)),
            Some(CqlValue::Int(456_i32)),
            Some(CqlValue::Int(789_i32)),
        ]);
        let err = do_serialize_err(v, &ColumnType::Native(NativeType::Double));
        let err = get_typeck_err(&err);
        assert_eq!(err.rust_name, std::any::type_name::<CqlValue>());
        assert_eq!(err.got, ColumnType::Native(NativeType::Double));
        assert_matches!(
            err.kind,
            BuiltinTypeCheckErrorKind::TupleError(TupleTypeCheckErrorKind::NotTuple)
        );

        // The Rust tuple has more elements than the CQL type
        let v = CqlValue::Tuple(vec![
            Some(CqlValue::Int(123_i32)),
            Some(CqlValue::Int(456_i32)),
            Some(CqlValue::Int(789_i32)),
        ]);
        let typ = ColumnType::Tuple(vec![ColumnType::Native(NativeType::Int); 2]);
        let err = do_serialize_err(v, &typ);
        let err = get_typeck_err(&err);
        assert_eq!(err.rust_name, std::any::type_name::<CqlValue>());
        assert_eq!(err.got, typ);
        assert_matches!(
            err.kind,
            BuiltinTypeCheckErrorKind::TupleError(TupleTypeCheckErrorKind::WrongElementCount {
                rust_type_el_count: 3,
                cql_type_el_count: 2,
            })
        );

        // Error during serialization of one of the elements
        let v = CqlValue::Tuple(vec![
            Some(CqlValue::Int(123_i32)),
            Some(CqlValue::Text("Ala ma kota".to_string())),
            Some(CqlValue::Double(789_f64)),
        ]);
        let typ = ColumnType::Tuple(vec![
            ColumnType::Native(NativeType::Int),
            ColumnType::Native(NativeType::Text),
            ColumnType::Native(NativeType::Uuid),
        ]);
        let err = do_serialize_err(v, &typ);
        let err = get_ser_err(&err);
        assert_eq!(err.rust_name, std::any::type_name::<CqlValue>());
        assert_eq!(err.got, typ);
        let BuiltinSerializationErrorKind::TupleError(
            TupleSerializationErrorKind::ElementSerializationFailed { index: 2, err },
        ) = &err.kind
        else {
            panic!("unexpected error kind: {}", err.kind)
        };
        let err = get_typeck_err(err);
        assert_matches!(
            err.kind,
            BuiltinTypeCheckErrorKind::MismatchedType {
                expected: &[ColumnType::Native(NativeType::Double)],
            }
        );
    }

    #[test]
    fn test_cql_value_udt_errors() {
        // Not a UDT
        let v = CqlValue::UserDefinedType {
            keyspace: "ks".to_string(),
            type_name: "udt".to_string(),
            fields: vec![
                ("a".to_string(), Some(CqlValue::Int(123_i32))),
                ("b".to_string(), Some(CqlValue::Int(456_i32))),
                ("c".to_string(), Some(CqlValue::Int(789_i32))),
            ],
        };
        let err = do_serialize_err(v, &ColumnType::Native(NativeType::Double));
        let err = get_typeck_err(&err);
        assert_eq!(err.rust_name, std::any::type_name::<CqlValue>());
        assert_eq!(err.got, ColumnType::Native(NativeType::Double));
        assert_matches!(
            err.kind,
            BuiltinTypeCheckErrorKind::UdtError(UdtTypeCheckErrorKind::NotUdt)
        );

        // Wrong type name
        let v = CqlValue::UserDefinedType {
            keyspace: "ks".to_string(),
            type_name: "udt".to_string(),
            fields: vec![
                ("a".to_string(), Some(CqlValue::Int(123_i32))),
                ("b".to_string(), Some(CqlValue::Int(456_i32))),
                ("c".to_string(), Some(CqlValue::Int(789_i32))),
            ],
        };
        let typ = ColumnType::UserDefinedType {
            type_name: "udt2".into(),
            keyspace: "ks".into(),
            field_types: vec![
                ("a".into(), ColumnType::Native(NativeType::Int)),
                ("b".into(), ColumnType::Native(NativeType::Int)),
                ("c".into(), ColumnType::Native(NativeType::Int)),
            ],
        };
        let err = do_serialize_err(v, &typ);
        let err = get_typeck_err(&err);
        assert_eq!(err.rust_name, std::any::type_name::<CqlValue>());
        assert_eq!(err.got, typ);
        let BuiltinTypeCheckErrorKind::UdtError(UdtTypeCheckErrorKind::NameMismatch {
            keyspace,
            type_name,
        }) = &err.kind
        else {
            panic!("unexpected error kind: {}", err.kind)
        };
        assert_eq!(keyspace, "ks");
        assert_eq!(type_name, "udt2");

        // Some fields are missing from the CQL type
        let v = CqlValue::UserDefinedType {
            keyspace: "ks".to_string(),
            type_name: "udt".to_string(),
            fields: vec![
                ("a".to_string(), Some(CqlValue::Int(123_i32))),
                ("b".to_string(), Some(CqlValue::Int(456_i32))),
                ("c".to_string(), Some(CqlValue::Int(789_i32))),
            ],
        };
        let typ = ColumnType::UserDefinedType {
            type_name: "udt".into(),
            keyspace: "ks".into(),
            field_types: vec![
                ("a".into(), ColumnType::Native(NativeType::Int)),
                ("b".into(), ColumnType::Native(NativeType::Int)),
                // c is missing
            ],
        };
        let err = do_serialize_err(v, &typ);
        let err = get_typeck_err(&err);
        assert_eq!(err.rust_name, std::any::type_name::<CqlValue>());
        assert_eq!(err.got, typ);
        let BuiltinTypeCheckErrorKind::UdtError(UdtTypeCheckErrorKind::NoSuchFieldInUdt {
            field_name,
        }) = &err.kind
        else {
            panic!("unexpected error kind: {}", err.kind)
        };
        assert_eq!(field_name, "c");

        // It is allowed for a Rust UDT to have less fields than the CQL UDT,
        // so skip UnexpectedFieldInDestination.

        // Error during serialization of one of the fields
        let v = CqlValue::UserDefinedType {
            keyspace: "ks".to_string(),
            type_name: "udt".to_string(),
            fields: vec![
                ("a".to_string(), Some(CqlValue::Int(123_i32))),
                ("b".to_string(), Some(CqlValue::Int(456_i32))),
                ("c".to_string(), Some(CqlValue::Int(789_i32))),
            ],
        };
        let typ = ColumnType::UserDefinedType {
            type_name: "udt".into(),
            keyspace: "ks".into(),
            field_types: vec![
                ("a".into(), ColumnType::Native(NativeType::Int)),
                ("b".into(), ColumnType::Native(NativeType::Int)),
                ("c".into(), ColumnType::Native(NativeType::Double)),
            ],
        };
        let err = do_serialize_err(v, &typ);
        let err = get_ser_err(&err);
        assert_eq!(err.rust_name, std::any::type_name::<CqlValue>());
        assert_eq!(err.got, typ);
        let BuiltinSerializationErrorKind::UdtError(
            UdtSerializationErrorKind::FieldSerializationFailed { field_name, err },
        ) = &err.kind
        else {
            panic!("unexpected error kind: {}", err.kind)
        };
        assert_eq!(field_name, "c");
        let err = get_typeck_err(err);
        assert_matches!(
            err.kind,
            BuiltinTypeCheckErrorKind::MismatchedType {
                expected: &[ColumnType::Native(NativeType::Int)],
            }
        );
    }

    // Do not remove. It's not used in tests but we keep it here to check that
    // we properly ignore warnings about unused variables, unnecessary `mut`s
    // etc. that usually pop up when generating code for empty structs.
    #[allow(unused)]
    #[derive(SerializeValue)]
    #[scylla(crate = crate)]
    struct TestUdtWithNoFields {}

    #[derive(SerializeValue, Debug, PartialEq, Eq, Default)]
    #[scylla(crate = crate)]
    struct TestUdtWithFieldSorting {
        a: String,
        b: i32,
        c: Vec<i64>,
    }

    #[test]
    fn test_udt_serialization_with_field_sorting_correct_order() {
        let typ = ColumnType::UserDefinedType {
            type_name: "typ".into(),
            keyspace: "ks".into(),
            field_types: vec![
                ("a".into(), ColumnType::Native(NativeType::Text)),
                ("b".into(), ColumnType::Native(NativeType::Int)),
                (
                    "c".into(),
                    ColumnType::List(Box::new(ColumnType::Native(NativeType::BigInt))),
                ),
            ],
        };

        let reference = do_serialize(
            CqlValue::UserDefinedType {
                keyspace: "ks".to_string(),
                type_name: "typ".to_string(),
                fields: vec![
                    (
                        "a".to_string(),
                        Some(CqlValue::Text(String::from("Ala ma kota"))),
                    ),
                    ("b".to_string(), Some(CqlValue::Int(42))),
                    (
                        "c".to_string(),
                        Some(CqlValue::List(vec![
                            CqlValue::BigInt(1),
                            CqlValue::BigInt(2),
                            CqlValue::BigInt(3),
                        ])),
                    ),
                ],
            },
            &typ,
        );
        let udt = do_serialize(
            TestUdtWithFieldSorting {
                a: "Ala ma kota".to_owned(),
                b: 42,
                c: vec![1, 2, 3],
            },
            &typ,
        );

        assert_eq!(reference, udt);
    }

    #[test]
    fn test_udt_serialization_with_field_sorting_incorrect_order() {
        let typ = ColumnType::UserDefinedType {
            type_name: "typ".into(),
            keyspace: "ks".into(),
            field_types: vec![
                // Two first columns are swapped
                ("b".into(), ColumnType::Native(NativeType::Int)),
                ("a".into(), ColumnType::Native(NativeType::Text)),
                (
                    "c".into(),
                    ColumnType::List(Box::new(ColumnType::Native(NativeType::BigInt))),
                ),
            ],
        };

        let reference = do_serialize(
            CqlValue::UserDefinedType {
                keyspace: "ks".to_string(),
                type_name: "typ".to_string(),
                fields: vec![
                    // FIXME: UDTs in CqlValue should also honor the order
                    // For now, it's swapped here as well
                    ("b".to_string(), Some(CqlValue::Int(42))),
                    (
                        "a".to_string(),
                        Some(CqlValue::Text(String::from("Ala ma kota"))),
                    ),
                    (
                        "c".to_string(),
                        Some(CqlValue::List(vec![
                            CqlValue::BigInt(1),
                            CqlValue::BigInt(2),
                            CqlValue::BigInt(3),
                        ])),
                    ),
                ],
            },
            &typ,
        );
        let udt = do_serialize(
            TestUdtWithFieldSorting {
                a: "Ala ma kota".to_owned(),
                b: 42,
                c: vec![1, 2, 3],
            },
            &typ,
        );

        assert_eq!(reference, udt);
    }

    #[test]
    fn test_udt_serialization_with_missing_rust_fields_at_end() {
        let udt = TestUdtWithFieldSorting::default();

        let typ_normal = ColumnType::UserDefinedType {
            type_name: "typ".into(),
            keyspace: "ks".into(),
            field_types: vec![
                ("a".into(), ColumnType::Native(NativeType::Text)),
                ("b".into(), ColumnType::Native(NativeType::Int)),
                (
                    "c".into(),
                    ColumnType::List(Box::new(ColumnType::Native(NativeType::BigInt))),
                ),
            ],
        };

        let typ_unexpected_field = ColumnType::UserDefinedType {
            type_name: "typ".into(),
            keyspace: "ks".into(),
            field_types: vec![
                ("a".into(), ColumnType::Native(NativeType::Text)),
                ("b".into(), ColumnType::Native(NativeType::Int)),
                (
                    "c".into(),
                    ColumnType::List(Box::new(ColumnType::Native(NativeType::BigInt))),
                ),
                // Unexpected fields
                ("d".into(), ColumnType::Native(NativeType::Counter)),
                ("e".into(), ColumnType::Native(NativeType::Counter)),
            ],
        };

        let result_normal = do_serialize(&udt, &typ_normal);
        let result_additional_field = do_serialize(&udt, &typ_unexpected_field);

        assert_eq!(result_normal, result_additional_field);
    }

    #[derive(SerializeValue, Debug, PartialEq, Default)]
    #[scylla(crate = crate)]
    struct TestUdtWithFieldSorting2 {
        a: String,
        b: i32,
        d: Option<Counter>,
        c: Vec<i64>,
    }

    #[derive(SerializeValue, Debug, PartialEq, Default)]
    #[scylla(crate = crate)]
    struct TestUdtWithFieldSorting3 {
        a: String,
        b: i32,
        d: Option<Counter>,
        e: Option<f32>,
        c: Vec<i64>,
    }

    #[test]
    fn test_udt_serialization_with_missing_rust_field_in_middle() {
        let udt = TestUdtWithFieldSorting::default();
        let udt2 = TestUdtWithFieldSorting2::default();
        let udt3 = TestUdtWithFieldSorting3::default();

        let typ = ColumnType::UserDefinedType {
            type_name: "typ".into(),
            keyspace: "ks".into(),
            field_types: vec![
                ("a".into(), ColumnType::Native(NativeType::Text)),
                ("b".into(), ColumnType::Native(NativeType::Int)),
                // Unexpected fields
                ("d".into(), ColumnType::Native(NativeType::Counter)),
                ("e".into(), ColumnType::Native(NativeType::Float)),
                // Remaining normal field
                (
                    "c".into(),
                    ColumnType::List(Box::new(ColumnType::Native(NativeType::BigInt))),
                ),
            ],
        };

        let result_1 = do_serialize(udt, &typ);
        let result_2 = do_serialize(udt2, &typ);
        let result_3 = do_serialize(udt3, &typ);

        assert_eq!(result_1, result_2);
        assert_eq!(result_2, result_3);
    }

    #[test]
    fn test_udt_serialization_failing_type_check() {
        let typ_not_udt = ColumnType::Native(NativeType::Ascii);
        let udt = TestUdtWithFieldSorting::default();
        let mut data = Vec::new();

        let err = udt
            .serialize(&typ_not_udt, CellWriter::new(&mut data))
            .unwrap_err();
        let err = err.0.downcast_ref::<BuiltinTypeCheckError>().unwrap();
        assert_matches!(
            err.kind,
            BuiltinTypeCheckErrorKind::UdtError(UdtTypeCheckErrorKind::NotUdt)
        );

        let typ_without_c = ColumnType::UserDefinedType {
            type_name: "typ".into(),
            keyspace: "ks".into(),
            field_types: vec![
                ("a".into(), ColumnType::Native(NativeType::Text)),
                ("b".into(), ColumnType::Native(NativeType::Int)),
                // Last field is missing
            ],
        };

        let err = udt
            .serialize(&typ_without_c, CellWriter::new(&mut data))
            .unwrap_err();
        let err = err.0.downcast_ref::<BuiltinTypeCheckError>().unwrap();
        assert_matches!(
            err.kind,
            BuiltinTypeCheckErrorKind::UdtError(
                UdtTypeCheckErrorKind::ValueMissingForUdtField { .. }
            )
        );

        let typ_wrong_type = ColumnType::UserDefinedType {
            type_name: "typ".into(),
            keyspace: "ks".into(),
            field_types: vec![
                ("a".into(), ColumnType::Native(NativeType::Text)),
                ("b".into(), ColumnType::Native(NativeType::Int)),
                ("c".into(), ColumnType::Native(NativeType::TinyInt)), // Wrong column type
            ],
        };

        let err = udt
            .serialize(&typ_wrong_type, CellWriter::new(&mut data))
            .unwrap_err();
        let err = err.0.downcast_ref::<BuiltinSerializationError>().unwrap();
        assert_matches!(
            err.kind,
            BuiltinSerializationErrorKind::UdtError(
                UdtSerializationErrorKind::FieldSerializationFailed { .. }
            )
        );
    }

    #[derive(SerializeValue)]
    #[scylla(crate = crate)]
    struct TestUdtWithGenerics<'a, T: SerializeValue> {
        a: &'a str,
        b: T,
    }

    #[test]
    fn test_udt_serialization_with_generics() {
        // A minimal smoke test just to test that it works.
        fn check_with_type<T: SerializeValue>(typ: ColumnType, t: T, cql_t: CqlValue) {
            let typ = ColumnType::UserDefinedType {
                type_name: "typ".into(),
                keyspace: "ks".into(),
                field_types: vec![
                    ("a".into(), ColumnType::Native(NativeType::Text)),
                    ("b".into(), typ),
                ],
            };
            let reference = do_serialize(
                CqlValue::UserDefinedType {
                    keyspace: "ks".to_string(),
                    type_name: "typ".to_string(),
                    fields: vec![
                        (
                            "a".to_string(),
                            Some(CqlValue::Text(String::from("Ala ma kota"))),
                        ),
                        ("b".to_string(), Some(cql_t)),
                    ],
                },
                &typ,
            );
            let udt = do_serialize(
                TestUdtWithGenerics {
                    a: "Ala ma kota",
                    b: t,
                },
                &typ,
            );
            assert_eq!(reference, udt);
        }

        check_with_type(
            ColumnType::Native(NativeType::Int),
            123_i32,
            CqlValue::Int(123_i32),
        );
        check_with_type(
            ColumnType::Native(NativeType::Double),
            123_f64,
            CqlValue::Double(123_f64),
        );
    }

    #[derive(SerializeValue, Debug, PartialEq, Eq, Default)]
    #[scylla(crate = crate, flavor = "enforce_order")]
    struct TestUdtWithEnforcedOrder {
        a: String,
        b: i32,
        c: Vec<i64>,
    }

    #[test]
    fn test_udt_serialization_with_enforced_order_correct_order() {
        let typ = ColumnType::UserDefinedType {
            type_name: "typ".into(),
            keyspace: "ks".into(),
            field_types: vec![
                ("a".into(), ColumnType::Native(NativeType::Text)),
                ("b".into(), ColumnType::Native(NativeType::Int)),
                (
                    "c".into(),
                    ColumnType::List(Box::new(ColumnType::Native(NativeType::BigInt))),
                ),
            ],
        };

        let reference = do_serialize(
            CqlValue::UserDefinedType {
                keyspace: "ks".to_string(),
                type_name: "typ".to_string(),
                fields: vec![
                    (
                        "a".to_string(),
                        Some(CqlValue::Text(String::from("Ala ma kota"))),
                    ),
                    ("b".to_string(), Some(CqlValue::Int(42))),
                    (
                        "c".to_string(),
                        Some(CqlValue::List(vec![
                            CqlValue::BigInt(1),
                            CqlValue::BigInt(2),
                            CqlValue::BigInt(3),
                        ])),
                    ),
                ],
            },
            &typ,
        );
        let udt = do_serialize(
            TestUdtWithEnforcedOrder {
                a: "Ala ma kota".to_owned(),
                b: 42,
                c: vec![1, 2, 3],
            },
            &typ,
        );

        assert_eq!(reference, udt);
    }

    #[test]
    fn test_udt_serialization_with_enforced_order_additional_field() {
        let udt = TestUdtWithEnforcedOrder::default();

        let typ_normal = ColumnType::UserDefinedType {
            type_name: "typ".into(),
            keyspace: "ks".into(),
            field_types: vec![
                ("a".into(), ColumnType::Native(NativeType::Text)),
                ("b".into(), ColumnType::Native(NativeType::Int)),
                (
                    "c".into(),
                    ColumnType::List(Box::new(ColumnType::Native(NativeType::BigInt))),
                ),
            ],
        };

        let typ_unexpected_field = ColumnType::UserDefinedType {
            type_name: "typ".into(),
            keyspace: "ks".into(),
            field_types: vec![
                ("a".into(), ColumnType::Native(NativeType::Text)),
                ("b".into(), ColumnType::Native(NativeType::Int)),
                (
                    "c".into(),
                    ColumnType::List(Box::new(ColumnType::Native(NativeType::BigInt))),
                ),
                // Unexpected field
                ("d".into(), ColumnType::Native(NativeType::Counter)),
            ],
        };

        let result_normal = do_serialize(&udt, &typ_normal);
        let result_additional_field = do_serialize(&udt, &typ_unexpected_field);

        assert_eq!(result_normal, result_additional_field);
    }

    #[test]
    fn test_udt_serialization_with_enforced_order_failing_type_check() {
        let typ_not_udt = ColumnType::Native(NativeType::Ascii);
        let udt = TestUdtWithEnforcedOrder::default();

        let mut data = Vec::new();

        let err = <_ as SerializeValue>::serialize(&udt, &typ_not_udt, CellWriter::new(&mut data))
            .unwrap_err();
        let err = err.0.downcast_ref::<BuiltinTypeCheckError>().unwrap();
        assert_matches!(
            err.kind,
            BuiltinTypeCheckErrorKind::UdtError(UdtTypeCheckErrorKind::NotUdt)
        );

        let typ = ColumnType::UserDefinedType {
            type_name: "typ".into(),
            keyspace: "ks".into(),
            field_types: vec![
                // Two first columns are swapped
                ("b".into(), ColumnType::Native(NativeType::Int)),
                ("a".into(), ColumnType::Native(NativeType::Text)),
                (
                    "c".into(),
                    ColumnType::List(Box::new(ColumnType::Native(NativeType::BigInt))),
                ),
            ],
        };

        let err =
            <_ as SerializeValue>::serialize(&udt, &typ, CellWriter::new(&mut data)).unwrap_err();
        let err = err.0.downcast_ref::<BuiltinTypeCheckError>().unwrap();
        assert_matches!(
            err.kind,
            BuiltinTypeCheckErrorKind::UdtError(UdtTypeCheckErrorKind::FieldNameMismatch { .. })
        );

        let typ_without_c = ColumnType::UserDefinedType {
            type_name: "typ".into(),
            keyspace: "ks".into(),
            field_types: vec![
                ("a".into(), ColumnType::Native(NativeType::Text)),
                ("b".into(), ColumnType::Native(NativeType::Int)),
                // Last field is missing
            ],
        };

        let err =
            <_ as SerializeValue>::serialize(&udt, &typ_without_c, CellWriter::new(&mut data))
                .unwrap_err();
        let err = err.0.downcast_ref::<BuiltinTypeCheckError>().unwrap();
        assert_matches!(
            err.kind,
            BuiltinTypeCheckErrorKind::UdtError(
                UdtTypeCheckErrorKind::ValueMissingForUdtField { .. }
            )
        );

        let typ_unexpected_field = ColumnType::UserDefinedType {
            type_name: "typ".into(),
            keyspace: "ks".into(),
            field_types: vec![
                ("a".into(), ColumnType::Native(NativeType::Text)),
                ("b".into(), ColumnType::Native(NativeType::Int)),
                ("c".into(), ColumnType::Native(NativeType::TinyInt)), // Wrong column type
            ],
        };

        let err = <_ as SerializeValue>::serialize(
            &udt,
            &typ_unexpected_field,
            CellWriter::new(&mut data),
        )
        .unwrap_err();
        let err = err.0.downcast_ref::<BuiltinSerializationError>().unwrap();
        assert_matches!(
            err.kind,
            BuiltinSerializationErrorKind::UdtError(
                UdtSerializationErrorKind::FieldSerializationFailed { .. }
            )
        );
    }

    #[derive(SerializeValue, Debug)]
    #[scylla(crate = crate)]
    struct TestUdtWithFieldRename {
        a: String,
        #[scylla(rename = "x")]
        b: i32,
    }

    #[derive(SerializeValue, Debug)]
    #[scylla(crate = crate, flavor = "enforce_order")]
    struct TestUdtWithFieldRenameAndEnforceOrder {
        a: String,
        #[scylla(rename = "x")]
        b: i32,
    }

    #[test]
    fn test_udt_serialization_with_field_rename() {
        let typ = ColumnType::UserDefinedType {
            type_name: "typ".into(),
            keyspace: "ks".into(),
            field_types: vec![
                ("x".into(), ColumnType::Native(NativeType::Int)),
                ("a".into(), ColumnType::Native(NativeType::Text)),
            ],
        };

        let mut reference = Vec::new();
        // Total length of the struct is 23
        reference.extend_from_slice(&23i32.to_be_bytes());
        // Field 'x'
        reference.extend_from_slice(&4i32.to_be_bytes());
        reference.extend_from_slice(&42i32.to_be_bytes());
        // Field 'a'
        reference.extend_from_slice(&("Ala ma kota".len() as i32).to_be_bytes());
        reference.extend_from_slice("Ala ma kota".as_bytes());

        let udt = do_serialize(
            TestUdtWithFieldRename {
                a: "Ala ma kota".to_owned(),
                b: 42,
            },
            &typ,
        );

        assert_eq!(reference, udt);
    }

    #[test]
    fn test_udt_serialization_with_field_rename_and_enforce_order() {
        let typ = ColumnType::UserDefinedType {
            type_name: "typ".into(),
            keyspace: "ks".into(),
            field_types: vec![
                ("a".into(), ColumnType::Native(NativeType::Text)),
                ("x".into(), ColumnType::Native(NativeType::Int)),
            ],
        };

        let mut reference = Vec::new();
        // Total length of the struct is 23
        reference.extend_from_slice(&23i32.to_be_bytes());
        // Field 'a'
        reference.extend_from_slice(&("Ala ma kota".len() as i32).to_be_bytes());
        reference.extend_from_slice("Ala ma kota".as_bytes());
        // Field 'x'
        reference.extend_from_slice(&4i32.to_be_bytes());
        reference.extend_from_slice(&42i32.to_be_bytes());

        let udt = do_serialize(
            TestUdtWithFieldRenameAndEnforceOrder {
                a: "Ala ma kota".to_owned(),
                b: 42,
            },
            &typ,
        );

        assert_eq!(reference, udt);
    }

    #[allow(unused)]
    #[derive(SerializeValue, Debug)]
    #[scylla(crate = crate, flavor = "enforce_order", skip_name_checks)]
    struct TestUdtWithSkippedNameChecks {
        a: String,
        b: i32,
    }

    #[test]
    fn test_udt_serialization_with_skipped_name_checks() {
        let typ = ColumnType::UserDefinedType {
            type_name: "typ".into(),
            keyspace: "ks".into(),
            field_types: vec![
                ("a".into(), ColumnType::Native(NativeType::Text)),
                ("x".into(), ColumnType::Native(NativeType::Int)),
            ],
        };

        let mut reference = Vec::new();
        // Total length of the struct is 23
        reference.extend_from_slice(&23i32.to_be_bytes());
        // Field 'a'
        reference.extend_from_slice(&("Ala ma kota".len() as i32).to_be_bytes());
        reference.extend_from_slice("Ala ma kota".as_bytes());
        // Field 'x'
        reference.extend_from_slice(&4i32.to_be_bytes());
        reference.extend_from_slice(&42i32.to_be_bytes());

        let udt = do_serialize(
            TestUdtWithFieldRenameAndEnforceOrder {
                a: "Ala ma kota".to_owned(),
                b: 42,
            },
            &typ,
        );

        assert_eq!(reference, udt);
    }

    #[derive(SerializeValue, Debug, PartialEq, Eq, Default)]
    #[scylla(crate = crate, forbid_excess_udt_fields)]
    struct TestStrictUdtWithFieldSorting {
        a: String,
        b: i32,
        c: Vec<i64>,
    }

    #[test]
    fn test_strict_udt_with_field_sorting_rejects_additional_field() {
        let udt = TestStrictUdtWithFieldSorting::default();
        let mut data = Vec::new();

        let typ_unexpected_field = ColumnType::UserDefinedType {
            type_name: "typ".into(),
            keyspace: "ks".into(),
            field_types: vec![
                ("a".into(), ColumnType::Native(NativeType::Text)),
                ("b".into(), ColumnType::Native(NativeType::Int)),
                (
                    "c".into(),
                    ColumnType::List(Box::new(ColumnType::Native(NativeType::BigInt))),
                ),
                // Unexpected field
                ("d".into(), ColumnType::Native(NativeType::Counter)),
            ],
        };

        let err = udt
            .serialize(&typ_unexpected_field, CellWriter::new(&mut data))
            .unwrap_err();
        let err = err.0.downcast_ref::<BuiltinTypeCheckError>().unwrap();
        assert_matches!(
            err.kind,
            BuiltinTypeCheckErrorKind::UdtError(UdtTypeCheckErrorKind::NoSuchFieldInUdt { .. })
        );

        let typ_unexpected_field_middle = ColumnType::UserDefinedType {
            type_name: "typ".into(),
            keyspace: "ks".into(),
            field_types: vec![
                ("a".into(), ColumnType::Native(NativeType::Text)),
                ("b".into(), ColumnType::Native(NativeType::Int)),
                // Unexpected field
                ("b_c".into(), ColumnType::Native(NativeType::Counter)),
                (
                    "c".into(),
                    ColumnType::List(Box::new(ColumnType::Native(NativeType::BigInt))),
                ),
            ],
        };

        let err = udt
            .serialize(&typ_unexpected_field_middle, CellWriter::new(&mut data))
            .unwrap_err();
        let err = err.0.downcast_ref::<BuiltinTypeCheckError>().unwrap();
        assert_matches!(
            err.kind,
            BuiltinTypeCheckErrorKind::UdtError(UdtTypeCheckErrorKind::NoSuchFieldInUdt { .. })
        );
    }

    #[derive(SerializeValue, Debug, PartialEq, Eq, Default)]
    #[scylla(crate = crate, flavor = "enforce_order", forbid_excess_udt_fields)]
    struct TestStrictUdtWithEnforcedOrder {
        a: String,
        b: i32,
        c: Vec<i64>,
    }

    #[test]
    fn test_strict_udt_with_enforced_order_rejects_additional_field() {
        let udt = TestStrictUdtWithEnforcedOrder::default();
        let mut data = Vec::new();

        let typ_unexpected_field = ColumnType::UserDefinedType {
            type_name: "typ".into(),
            keyspace: "ks".into(),
            field_types: vec![
                ("a".into(), ColumnType::Native(NativeType::Text)),
                ("b".into(), ColumnType::Native(NativeType::Int)),
                (
                    "c".into(),
                    ColumnType::List(Box::new(ColumnType::Native(NativeType::BigInt))),
                ),
                // Unexpected field
                ("d".into(), ColumnType::Native(NativeType::Counter)),
            ],
        };

        let err = <_ as SerializeValue>::serialize(
            &udt,
            &typ_unexpected_field,
            CellWriter::new(&mut data),
        )
        .unwrap_err();
        let err = err.0.downcast_ref::<BuiltinTypeCheckError>().unwrap();
        assert_matches!(
            err.kind,
            BuiltinTypeCheckErrorKind::UdtError(UdtTypeCheckErrorKind::NoSuchFieldInUdt { .. })
        );
    }

    #[derive(SerializeValue, Debug)]
    #[scylla(crate = crate, flavor = "enforce_order", skip_name_checks)]
    struct TestUdtWithSkippedFields {
        a: String,
        b: i32,
        #[scylla(skip)]
        #[allow(dead_code)]
        skipped: Vec<String>,
        c: Vec<i64>,
    }

    #[test]
    fn test_row_serialization_with_skipped_field() {
        let typ = ColumnType::UserDefinedType {
            type_name: "typ".into(),
            keyspace: "ks".into(),
            field_types: vec![
                ("a".into(), ColumnType::Native(NativeType::Text)),
                ("b".into(), ColumnType::Native(NativeType::Int)),
                (
                    "c".into(),
                    ColumnType::List(Box::new(ColumnType::Native(NativeType::BigInt))),
                ),
            ],
        };

        let reference = do_serialize(
            TestUdtWithFieldSorting {
                a: "Ala ma kota".to_owned(),
                b: 42,
                c: vec![1, 2, 3],
            },
            &typ,
        );
        let row = do_serialize(
            TestUdtWithSkippedFields {
                a: "Ala ma kota".to_owned(),
                b: 42,
                skipped: vec!["abcd".to_owned(), "efgh".to_owned()],
                c: vec![1, 2, 3],
            },
            &typ,
        );

        assert_eq!(reference, row);
    }

    #[test]
    fn test_udt_with_non_rust_ident() {
        #[derive(SerializeValue, Debug)]
        #[scylla(crate = crate)]
        struct UdtWithNonRustIdent {
            #[scylla(rename = "a$a")]
            a: i32,
        }

        let typ = ColumnType::UserDefinedType {
            type_name: "typ".into(),
            keyspace: "ks".into(),
            field_types: vec![("a$a".into(), ColumnType::Native(NativeType::Int))],
        };
        let value = UdtWithNonRustIdent { a: 42 };

        let mut reference = Vec::new();
        // Total length of the struct
        reference.extend_from_slice(&8i32.to_be_bytes());
        // Field 'a'
        reference.extend_from_slice(&(std::mem::size_of_val(&value.a) as i32).to_be_bytes());
        reference.extend_from_slice(&value.a.to_be_bytes());

        let udt = do_serialize(value, &typ);

        assert_eq!(reference, udt);
    }
}
