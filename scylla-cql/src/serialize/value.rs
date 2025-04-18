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

use crate::frame::response::result::{CollectionType, ColumnType, NativeType};
use crate::frame::types::{unsigned_vint_encode, vint_encode};
use crate::value::{
    Counter, CqlDate, CqlDecimal, CqlDecimalBorrowed, CqlDuration, CqlTime, CqlTimestamp,
    CqlTimeuuid, CqlValue, CqlVarint, CqlVarintBorrowed, MaybeUnset, Unset,
};

#[cfg(feature = "chrono-04")]
use crate::value::ValueOverflow;

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
        match typ {
            ColumnType::Collection {
                typ: CollectionType::List(_) | CollectionType::Set(_),
                ..
            } => serialize_sequence(
                std::any::type_name::<Self>(),
                self.len(),
                self.iter(),
                typ,
                writer,
            ),

            ColumnType::Vector {
                typ: element_type,
                dimensions,
            } => serialize_vector(
                std::any::type_name::<Self>(),
                self.len(),
                self.iter(),
                element_type,
                dimensions,
                typ,
                writer,
            ),

            _ => Err(mk_typck_err_named(
                std::any::type_name::<Self>(),
                typ,
                SetOrListTypeCheckErrorKind::NotSetOrList,
            )),
        }
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
            name: type_name,
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
        CqlValue::Vector(v) => <_ as SerializeValue>::serialize(&v, typ, writer),
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
            definition: udt, ..
        } => (&udt.name, &udt.keyspace, &udt.field_types),
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
        ColumnType::Collection {
            frozen: false,
            typ: CollectionType::List(elt),
        }
        | ColumnType::Collection {
            frozen: false,
            typ: CollectionType::Set(elt),
        } => elt,
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

fn serialize_vector<'t, 'b, T: SerializeValue + 't>(
    rust_name: &'static str,
    len: usize,
    iter: impl Iterator<Item = &'t T>,
    element_type: &ColumnType,
    dimensions: &u16,
    typ: &ColumnType,
    writer: CellWriter<'b>,
) -> Result<WrittenCellProof<'b>, SerializationError> {
    if len != *dimensions as usize {
        return Err(mk_ser_err_named(
            rust_name,
            typ,
            VectorSerializationErrorKind::InvalidNumberOfElements(len, *dimensions),
        ));
    }
    let mut builder = writer.into_value_builder();
    match element_type.type_size() {
        Some(_) => {
            for element in iter {
                T::serialize(
                    element,
                    element_type,
                    builder.make_sub_writer_without_size(),
                )
                .map_err(|err| {
                    mk_ser_err_named(
                        rust_name,
                        typ,
                        VectorSerializationErrorKind::ElementSerializationFailed(err),
                    )
                })?;
            }
        }
        None => {
            for element in iter {
                let mut element_buffer = Vec::new();
                let inner_writer = CellWriter::new_without_size(&mut element_buffer);
                T::serialize(element, element_type, inner_writer).map_err(|err| {
                    mk_ser_err_named(
                        rust_name,
                        typ,
                        VectorSerializationErrorKind::ElementSerializationFailed(err),
                    )
                })?;
                let mut element_length_buffer = Vec::new();
                unsigned_vint_encode(
                    element_buffer.len().try_into().unwrap(),
                    &mut element_length_buffer,
                );
                builder.append_bytes(element_length_buffer.as_slice());
                builder.append_bytes(element_buffer.as_slice());
            }
        }
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
        ColumnType::Collection {
            frozen: false,
            typ: CollectionType::Map(k, v),
        } => (k, v),
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

pub(crate) fn mk_ser_err<T>(
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

    /// A serialization failure specific to a CQL set or list.
    VectorError(VectorSerializationErrorKind),

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

impl From<VectorSerializationErrorKind> for BuiltinSerializationErrorKind {
    fn from(value: VectorSerializationErrorKind) -> Self {
        BuiltinSerializationErrorKind::VectorError(value)
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
            BuiltinSerializationErrorKind::VectorError(err) => err.fmt(f),
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

/// Describes why serialization of a vector type failed.
#[derive(Error, Debug, Clone)]
#[non_exhaustive]
pub enum VectorSerializationErrorKind {
    /// The number of elements in the serialized collection does not match
    /// the number of vector dimensions
    #[error(
        "number of vector elements ({0}) does not match the number of declared dimensions ({1})"
    )]
    InvalidNumberOfElements(usize, u16),

    /// One of the elements of the vector failed to serialize.
    #[error("failed to serialize one of the elements: {0}")]
    ElementSerializationFailed(SerializationError),
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
#[path = "value_tests.rs"]
pub(crate) mod tests;
