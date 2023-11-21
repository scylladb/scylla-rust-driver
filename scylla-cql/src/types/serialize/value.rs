use std::sync::Arc;

use thiserror::Error;

use crate::frame::response::result::ColumnType;
use crate::frame::value::Value;

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

impl<T: Value> SerializeCql for T {
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
                Ok(writer.set_value(contents))
            }
        }
        _ => Err(SerializationError(Arc::new(
            ValueToSerializeCqlAdapterError::InvalidDeclaredSize { size: len },
        ))),
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
