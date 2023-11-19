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
use crate::frame::types::vint_encode;
use crate::frame::value::{
    Counter, CqlDate, CqlDuration, CqlTime, CqlTimestamp, MaybeUnset, Unset, Value,
};

#[cfg(feature = "chrono")]
use crate::frame::value::ValueOverflow;

use super::{CellValueBuilder, CellWriter, SerializationError};

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
    impl_exact_preliminary_type_check!(Decimal);
    impl_serialize_via_writer!(|me, typ, writer| {
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
impl SerializeCql for CqlDate {
    impl_exact_preliminary_type_check!(Date);
    impl_serialize_via_writer!(|me, writer| {
        writer.set_value(me.0.to_be_bytes().as_slice()).unwrap()
    });
}
impl SerializeCql for CqlTimestamp {
    impl_exact_preliminary_type_check!(Timestamp);
    impl_serialize_via_writer!(|me, writer| {
        writer.set_value(me.0.to_be_bytes().as_slice()).unwrap()
    });
}
impl SerializeCql for CqlTime {
    impl_exact_preliminary_type_check!(Time);
    impl_serialize_via_writer!(|me, writer| {
        writer.set_value(me.0.to_be_bytes().as_slice()).unwrap()
    });
}
#[cfg(feature = "chrono")]
impl SerializeCql for NaiveDate {
    impl_exact_preliminary_type_check!(Date);
    impl_serialize_via_writer!(|me, typ, writer| {
        <CqlDate as SerializeCql>::serialize(&(*me).into(), typ, writer)?
    });
}
#[cfg(feature = "chrono")]
impl SerializeCql for DateTime<Utc> {
    impl_exact_preliminary_type_check!(Timestamp);
    impl_serialize_via_writer!(|me, typ, writer| {
        <CqlTimestamp as SerializeCql>::serialize(&(*me).into(), typ, writer)?
    });
}
#[cfg(feature = "chrono")]
impl SerializeCql for NaiveTime {
    impl_exact_preliminary_type_check!(Time);
    impl_serialize_via_writer!(|me, typ, writer| {
        let cql_time = CqlTime::try_from(*me).map_err(|_: ValueOverflow| {
            mk_ser_err::<Self>(typ, BuiltinSerializationErrorKind::ValueOverflow)
        })?;
        <CqlTime as SerializeCql>::serialize(&cql_time, typ, writer)?
    });
}
#[cfg(feature = "chrono")]
impl SerializeCql for time::Date {
    impl_exact_preliminary_type_check!(Date);
    impl_serialize_via_writer!(|me, typ, writer| {
        <CqlDate as SerializeCql>::serialize(&(*me).into(), typ, writer)?
    });
}
#[cfg(feature = "chrono")]
impl SerializeCql for time::OffsetDateTime {
    impl_exact_preliminary_type_check!(Timestamp);
    impl_serialize_via_writer!(|me, typ, writer| {
        <CqlTimestamp as SerializeCql>::serialize(&(*me).into(), typ, writer)?
    });
}
#[cfg(feature = "chrono")]
impl SerializeCql for time::Time {
    impl_exact_preliminary_type_check!(Time);
    impl_serialize_via_writer!(|me, typ, writer| {
        <CqlTime as SerializeCql>::serialize(&(*me).into(), typ, writer)?
    });
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
    impl_exact_preliminary_type_check!(Varint);
    impl_serialize_via_writer!(|me, typ, writer| {
        // TODO: The allocation here can be avoided and we can reimplement
        // `to_signed_bytes_be` by using `to_u64_digits` and a bit of custom
        // logic. Need better tests in order to do this.
        writer
            .set_value(me.to_signed_bytes_be().as_slice())
            .map_err(|_| mk_ser_err::<Self>(typ, BuiltinSerializationErrorKind::SizeOverflow))?
    });
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
    impl_exact_preliminary_type_check!(Duration);
    impl_serialize_via_writer!(|me, writer| {
        // TODO: adjust vint_encode to use CellValueBuilder or something like that
        let mut buf = Vec::with_capacity(27); // worst case size is 27
        vint_encode(me.months as i64, &mut buf);
        vint_encode(me.days as i64, &mut buf);
        vint_encode(me.nanoseconds, &mut buf);
        writer.set_value(buf.as_slice()).unwrap()
    });
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
impl<V: SerializeCql, S: BuildHasher + Default> SerializeCql for HashSet<V, S> {
    fn preliminary_type_check(typ: &ColumnType) -> Result<(), SerializationError> {
        match typ {
            ColumnType::Set(elt) => V::preliminary_type_check(elt).map_err(|err| {
                mk_typck_err::<Self>(
                    typ,
                    SetOrListTypeCheckErrorKind::ElementTypeCheckFailed(err),
                )
            }),
            _ => Err(mk_typck_err::<Self>(
                typ,
                SetOrListTypeCheckErrorKind::NotSetOrList,
            )),
        }
    }

    fn serialize<W: CellWriter>(
        &self,
        typ: &ColumnType,
        writer: W,
    ) -> Result<W::WrittenCellProof, SerializationError> {
        serialize_sequence(
            std::any::type_name::<Self>(),
            self.len(),
            self.iter(),
            typ,
            writer,
        )
    }
}
impl<K: SerializeCql, V: SerializeCql, S: BuildHasher> SerializeCql for HashMap<K, V, S> {
    fn preliminary_type_check(typ: &ColumnType) -> Result<(), SerializationError> {
        match typ {
            ColumnType::Map(k, v) => {
                K::preliminary_type_check(k).map_err(|err| {
                    mk_typck_err::<Self>(typ, MapTypeCheckErrorKind::KeyTypeCheckFailed(err))
                })?;
                V::preliminary_type_check(v).map_err(|err| {
                    mk_typck_err::<Self>(typ, MapTypeCheckErrorKind::ValueTypeCheckFailed(err))
                })?;
                Ok(())
            }
            _ => Err(mk_typck_err::<Self>(typ, MapTypeCheckErrorKind::NotMap)),
        }
    }

    fn serialize<W: CellWriter>(
        &self,
        typ: &ColumnType,
        writer: W,
    ) -> Result<W::WrittenCellProof, SerializationError> {
        serialize_mapping(
            std::any::type_name::<Self>(),
            self.len(),
            self.iter(),
            typ,
            writer,
        )
    }
}
impl<V: SerializeCql> SerializeCql for BTreeSet<V> {
    fn preliminary_type_check(typ: &ColumnType) -> Result<(), SerializationError> {
        match typ {
            ColumnType::Set(elt) => V::preliminary_type_check(elt).map_err(|err| {
                mk_typck_err::<Self>(
                    typ,
                    SetOrListTypeCheckErrorKind::ElementTypeCheckFailed(err),
                )
            }),
            _ => Err(mk_typck_err::<Self>(
                typ,
                SetOrListTypeCheckErrorKind::NotSetOrList,
            )),
        }
    }

    fn serialize<W: CellWriter>(
        &self,
        typ: &ColumnType,
        writer: W,
    ) -> Result<W::WrittenCellProof, SerializationError> {
        serialize_sequence(
            std::any::type_name::<Self>(),
            self.len(),
            self.iter(),
            typ,
            writer,
        )
    }
}
impl<K: SerializeCql, V: SerializeCql> SerializeCql for BTreeMap<K, V> {
    fn preliminary_type_check(typ: &ColumnType) -> Result<(), SerializationError> {
        match typ {
            ColumnType::Map(k, v) => {
                K::preliminary_type_check(k).map_err(|err| {
                    mk_typck_err::<Self>(typ, MapTypeCheckErrorKind::KeyTypeCheckFailed(err))
                })?;
                V::preliminary_type_check(v).map_err(|err| {
                    mk_typck_err::<Self>(typ, MapTypeCheckErrorKind::ValueTypeCheckFailed(err))
                })?;
                Ok(())
            }
            _ => Err(mk_typck_err::<Self>(typ, MapTypeCheckErrorKind::NotMap)),
        }
    }

    fn serialize<W: CellWriter>(
        &self,
        typ: &ColumnType,
        writer: W,
    ) -> Result<W::WrittenCellProof, SerializationError> {
        serialize_mapping(
            std::any::type_name::<Self>(),
            self.len(),
            self.iter(),
            typ,
            writer,
        )
    }
}
impl<T: SerializeCql> SerializeCql for Vec<T> {
    fn preliminary_type_check(typ: &ColumnType) -> Result<(), SerializationError> {
        match typ {
            ColumnType::List(elt) | ColumnType::Set(elt) => {
                T::preliminary_type_check(elt).map_err(|err| {
                    mk_typck_err::<Self>(
                        typ,
                        SetOrListTypeCheckErrorKind::ElementTypeCheckFailed(err),
                    )
                })
            }
            _ => Err(mk_typck_err::<Self>(
                typ,
                SetOrListTypeCheckErrorKind::NotSetOrList,
            )),
        }
    }

    fn serialize<W: CellWriter>(
        &self,
        typ: &ColumnType,
        writer: W,
    ) -> Result<W::WrittenCellProof, SerializationError> {
        serialize_sequence(
            std::any::type_name::<Self>(),
            self.len(),
            self.iter(),
            typ,
            writer,
        )
    }
}
impl<'a, T: SerializeCql + 'a> SerializeCql for &'a [T] {
    fn preliminary_type_check(typ: &ColumnType) -> Result<(), SerializationError> {
        match typ {
            ColumnType::List(elt) | ColumnType::Set(elt) => {
                T::preliminary_type_check(elt).map_err(|err| {
                    mk_typck_err::<Self>(
                        typ,
                        SetOrListTypeCheckErrorKind::ElementTypeCheckFailed(err),
                    )
                })
            }
            _ => Err(mk_typck_err::<Self>(
                typ,
                SetOrListTypeCheckErrorKind::NotSetOrList,
            )),
        }
    }

    fn serialize<W: CellWriter>(
        &self,
        typ: &ColumnType,
        writer: W,
    ) -> Result<W::WrittenCellProof, SerializationError> {
        serialize_sequence(
            std::any::type_name::<Self>(),
            self.len(),
            self.iter(),
            typ,
            writer,
        )
    }
}
impl SerializeCql for CqlValue {
    fallback_impl_contents!();
}

macro_rules! impl_tuple {
    (
        $($typs:ident),*;
        $($fidents:ident),*;
        $($tidents:ident),*;
        $length:expr
    ) => {
        impl<$($typs: SerializeCql),*> SerializeCql for ($($typs,)*) {
            fn preliminary_type_check(typ: &ColumnType) -> Result<(), SerializationError> {
                match typ {
                    ColumnType::Tuple(typs) => match typs.as_slice() {
                        [$($tidents),*, ..] => {
                            let index = 0;
                            $(
                                <$typs as SerializeCql>::preliminary_type_check($tidents)
                                    .map_err(|err|
                                        mk_typck_err::<Self>(
                                            typ,
                                            TupleTypeCheckErrorKind::ElementTypeCheckFailed {
                                                index,
                                                err,
                                            }
                                        )
                                    )?;
                                let index = index + 1;
                            )*
                            let _ = index;
                        }
                        _ => return Err(mk_typck_err::<Self>(
                            typ,
                            TupleTypeCheckErrorKind::WrongElementCount {
                                actual: $length,
                                asked_for: typs.len(),
                            }
                        ))
                    }
                    _ => return Err(mk_typck_err::<Self>(
                        typ,
                        TupleTypeCheckErrorKind::NotTuple
                    )),
                };
                Ok(())
            }

            fn serialize<W: CellWriter>(
                &self,
                typ: &ColumnType,
                writer: W,
            ) -> Result<W::WrittenCellProof, SerializationError> {
                let ($($tidents,)*) = match typ {
                    ColumnType::Tuple(typs) => match typs.as_slice() {
                        [$($tidents),*] => ($($tidents,)*),
                        _ => return Err(mk_typck_err::<Self>(
                            typ,
                            TupleTypeCheckErrorKind::WrongElementCount {
                                actual: $length,
                                asked_for: typs.len(),
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
                    <$typs as SerializeCql>::serialize($fidents, $tidents, builder.make_sub_writer())
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

fn serialize_sequence<'t, T: SerializeCql + 't, W: CellWriter>(
    rust_name: &'static str,
    len: usize,
    iter: impl Iterator<Item = &'t T>,
    typ: &ColumnType,
    writer: W,
) -> Result<W::WrittenCellProof, SerializationError> {
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

fn serialize_mapping<'t, K: SerializeCql + 't, V: SerializeCql + 't, W: CellWriter>(
    rust_name: &'static str,
    len: usize,
    iter: impl Iterator<Item = (&'t K, &'t V)>,
    typ: &ColumnType,
    writer: W,
) -> Result<W::WrittenCellProof, SerializationError> {
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

    /// A type check failure specific to a CQL set or list.
    SetOrListError(SetOrListTypeCheckErrorKind),

    /// A type check failure specific to a CQL map.
    MapError(MapTypeCheckErrorKind),

    /// A type check failure specific to a CQL tuple.
    TupleError(TupleTypeCheckErrorKind),
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

impl Display for BuiltinSerializationErrorKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            BuiltinSerializationErrorKind::SizeOverflow => {
                write!(
                    f,
                    "the Rust value is too big to be serialized in the CQL protocol format"
                )
            }
            BuiltinSerializationErrorKind::ValueOverflow => {
                write!(
                    f,
                    "the Rust value is out of range supported by the CQL type"
                )
            }
            BuiltinSerializationErrorKind::SetOrListError(err) => err.fmt(f),
            BuiltinSerializationErrorKind::MapError(err) => err.fmt(f),
            BuiltinSerializationErrorKind::TupleError(err) => err.fmt(f),
        }
    }
}

#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum MapTypeCheckErrorKind {
    /// The CQL type is not a map.
    NotMap,

    /// Checking the map key type failed.
    KeyTypeCheckFailed(SerializationError),

    /// Checking the map value type failed.
    ValueTypeCheckFailed(SerializationError),
}

impl Display for MapTypeCheckErrorKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            MapTypeCheckErrorKind::NotMap => {
                write!(
                    f,
                    "the CQL type the map was attempted to be serialized to was not map"
                )
            }
            MapTypeCheckErrorKind::KeyTypeCheckFailed(err) => {
                write!(f, "failed to type check one of the keys: {}", err)
            }
            MapTypeCheckErrorKind::ValueTypeCheckFailed(err) => {
                write!(f, "failed to type check one of the values: {}", err)
            }
        }
    }
}

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
                write!(
                    f,
                    "the map contains too many elements to fit in CQL representation"
                )
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

#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum SetOrListTypeCheckErrorKind {
    /// The CQL type is neither a set not a list.
    NotSetOrList,

    /// Checking the type of the set/list element failed.
    ElementTypeCheckFailed(SerializationError),
}

impl Display for SetOrListTypeCheckErrorKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SetOrListTypeCheckErrorKind::NotSetOrList => {
                write!(
                    f,
                    "the CQL type the tuple was attempted to was neither a set or a list"
                )
            }
            SetOrListTypeCheckErrorKind::ElementTypeCheckFailed(err) => {
                write!(f, "failed to type check one of the elements: {err}")
            }
        }
    }
}

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
            SetOrListSerializationErrorKind::TooManyElements => {
                write!(
                    f,
                    "the collection contains too many elements to fit in CQL representation"
                )
            }
            SetOrListSerializationErrorKind::ElementSerializationFailed(err) => {
                write!(f, "failed to serialize one of the elements: {err}")
            }
        }
    }
}

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
    WrongElementCount { actual: usize, asked_for: usize },

    /// One of the tuple elements failed to type check.
    ElementTypeCheckFailed {
        index: usize,
        err: SerializationError,
    },
}

impl Display for TupleTypeCheckErrorKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TupleTypeCheckErrorKind::NotTuple => write!(
                f,
                "the CQL type the tuple was attempted to be serialized to is not a tuple"
            ),
            TupleTypeCheckErrorKind::WrongElementCount { actual, asked_for } => write!(
                f,
                "wrong tuple element count: CQL type has {asked_for}, the Rust tuple has {actual}"
            ),
            TupleTypeCheckErrorKind::ElementTypeCheckFailed { index, err } => {
                write!(f, "element no. {index} failed to type check: {err}")
            }
        }
    }
}

#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum TupleSerializationErrorKind {
    /// One of the tuple elements failed to serialize.
    ElementSerializationFailed {
        index: usize,
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
