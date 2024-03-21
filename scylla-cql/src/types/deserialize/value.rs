//! Provides types for dealing with CQL value deserialization.

use super::FrameSlice;
use crate::frame::{
    frame_errors::ParseError,
    response::result::{deser_cql_value, ColumnType, CqlValue},
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

// Utilities

fn ensure_not_null(v: Option<FrameSlice>) -> Result<&[u8], ParseError> {
    match v {
        Some(v) => Ok(v.as_slice()),
        None => Err(ParseError::BadIncomingData(
            "Expected a non-null value".to_string(),
        )),
    }
}

#[cfg(test)]
mod tests {
    use bytes::{BufMut, Bytes, BytesMut};

    use std::fmt::Debug;

    use crate::frame::frame_errors::ParseError;
    use crate::frame::response::cql_to_rust::FromCqlVal;
    use crate::frame::response::result::{deser_cql_value, ColumnType, CqlValue};
    use crate::frame::types;
    use crate::types::deserialize::FrameSlice;
    use crate::types::serialize::value::SerializeCql;
    use crate::types::serialize::CellWriter;

    use super::DeserializeCql;

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

    fn append_bytes(b: &mut impl BufMut, cell: &[u8]) {
        b.put_i32(cell.len() as i32);
        b.put_slice(cell);
    }
}
