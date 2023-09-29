use std::sync::Arc;

use crate::frame::response::result::ColumnSpec;
use crate::frame::value::ValueList;

use super::SerializationError;

pub struct RowSerializationContext<'a> {
    columns: &'a [ColumnSpec],
}

impl<'a> RowSerializationContext<'a> {
    #[inline]
    pub fn columns(&self) -> &'a [ColumnSpec] {
        self.columns
    }

    // TODO: change RowSerializationContext to make this faster
    #[inline]
    pub fn column_by_name(&self, target: &str) -> Option<&ColumnSpec> {
        self.columns.iter().find(|&c| c.name == target)
    }
}

pub trait SerializeRow {
    fn preliminary_type_check(ctx: &RowSerializationContext<'_>) -> Result<(), SerializationError>;
    fn serialize(
        &self,
        ctx: &RowSerializationContext<'_>,
        out: &mut Vec<u8>,
    ) -> Result<(), SerializationError>;
}

impl<T: ValueList> SerializeRow for T {
    fn preliminary_type_check(
        _ctx: &RowSerializationContext<'_>,
    ) -> Result<(), SerializationError> {
        Ok(())
    }

    fn serialize(
        &self,
        _ctx: &RowSerializationContext<'_>,
        out: &mut Vec<u8>,
    ) -> Result<(), SerializationError> {
        self.write_to_request(out)
            .map_err(|err| Arc::new(err) as SerializationError)
    }
}
