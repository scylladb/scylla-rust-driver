use std::sync::Arc;

use crate::frame::response::result::ColumnType;
use crate::frame::value::Value;

use super::SerializationError;

pub trait SerializeCql {
    fn preliminary_type_check(typ: &ColumnType) -> Result<(), SerializationError>;
    fn serialize(&self, typ: &ColumnType, buf: &mut Vec<u8>) -> Result<(), SerializationError>;
}

impl<T: Value> SerializeCql for T {
    fn preliminary_type_check(_typ: &ColumnType) -> Result<(), SerializationError> {
        Ok(())
    }

    fn serialize(&self, _typ: &ColumnType, buf: &mut Vec<u8>) -> Result<(), SerializationError> {
        self.serialize(buf)
            .map_err(|err| Arc::new(err) as SerializationError)
    }
}
