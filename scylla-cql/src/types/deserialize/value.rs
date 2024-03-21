//! Provides types for dealing with CQL value deserialization.

use std::fmt::Display;

use thiserror::Error;

use super::{DeserializationError, FrameSlice, TypeCheckError};
use crate::frame::frame_errors::ParseError;
use crate::frame::response::result::ColumnType;

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
pub enum BuiltinTypeCheckErrorKind {}

impl Display for BuiltinTypeCheckErrorKind {
    fn fmt(&self, _f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        Ok(())
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
}

impl Display for BuiltinDeserializationErrorKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            BuiltinDeserializationErrorKind::GenericParseError(err) => err.fmt(f),
        }
    }
}

#[cfg(test)]
mod tests {
    use bytes::{BufMut, Bytes, BytesMut};

    use std::fmt::Debug;

    use crate::frame::response::cql_to_rust::FromCqlVal;
    use crate::frame::response::result::{deser_cql_value, ColumnType, CqlValue};
    use crate::frame::types;
    use crate::types::deserialize::{DeserializationError, FrameSlice};
    use crate::types::serialize::value::SerializeValue;
    use crate::types::serialize::CellWriter;

    use super::{mk_deser_err, BuiltinDeserializationErrorKind, DeserializeValue};

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

    fn append_bytes(b: &mut impl BufMut, cell: &[u8]) {
        b.put_i32(cell.len() as i32);
        b.put_slice(cell);
    }
}
