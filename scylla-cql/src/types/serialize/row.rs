use std::{collections::HashMap, sync::Arc};

use thiserror::Error;

use crate::frame::value::ValueList;
use crate::frame::{response::result::ColumnSpec, types::RawValue};

use super::{CellWriter, RowWriter, SerializationError};

/// Contains information needed to serialize a row.
pub struct RowSerializationContext<'a> {
    columns: &'a [ColumnSpec],
}

impl<'a> RowSerializationContext<'a> {
    /// Returns column/bind marker specifications for given query.
    #[inline]
    pub fn columns(&self) -> &'a [ColumnSpec] {
        self.columns
    }

    /// Looks up and returns a column/bind marker by name.
    // TODO: change RowSerializationContext to make this faster
    #[inline]
    pub fn column_by_name(&self, target: &str) -> Option<&ColumnSpec> {
        self.columns.iter().find(|&c| c.name == target)
    }
}

pub trait SerializeRow {
    /// Checks if it _might_ be possible to serialize the row according to the
    /// information in the context.
    ///
    /// This function is intended to serve as an optimization in the future,
    /// if we were ever to introduce prepared statements parametrized by types.
    ///
    /// Sometimes, a row cannot be fully type checked right away without knowing
    /// the exact values of the columns (e.g. when deserializing to `CqlValue`),
    /// but it's fine to do full type checking later in `serialize`.
    fn preliminary_type_check(ctx: &RowSerializationContext<'_>) -> Result<(), SerializationError>;

    /// Serializes the row according to the information in the given context.
    ///
    /// The function may assume that `preliminary_type_check` was called,
    /// though it must not do anything unsafe if this assumption does not hold.
    fn serialize<W: RowWriter>(
        &self,
        ctx: &RowSerializationContext<'_>,
        writer: &mut W,
    ) -> Result<(), SerializationError>;
}

impl<T: ValueList> SerializeRow for T {
    fn preliminary_type_check(
        _ctx: &RowSerializationContext<'_>,
    ) -> Result<(), SerializationError> {
        Ok(())
    }

    fn serialize<W: RowWriter>(
        &self,
        ctx: &RowSerializationContext<'_>,
        writer: &mut W,
    ) -> Result<(), SerializationError> {
        serialize_legacy_row(self, ctx, writer)
    }
}

pub fn serialize_legacy_row<T: ValueList>(
    r: &T,
    ctx: &RowSerializationContext<'_>,
    writer: &mut impl RowWriter,
) -> Result<(), SerializationError> {
    let serialized =
        <T as ValueList>::serialized(r).map_err(|err| SerializationError(Arc::new(err)))?;

    let mut append_value = |value: RawValue| {
        let cell_writer = writer.make_cell_writer();
        let _proof = match value {
            RawValue::Null => cell_writer.set_null(),
            RawValue::Unset => cell_writer.set_unset(),
            RawValue::Value(v) => cell_writer.set_value(v),
        };
    };

    if !serialized.has_names() {
        serialized.iter().for_each(append_value);
    } else {
        let values_by_name = serialized
            .iter_name_value_pairs()
            .map(|(k, v)| (k.unwrap(), v))
            .collect::<HashMap<_, _>>();

        for col in ctx.columns() {
            let val = values_by_name.get(col.name.as_str()).ok_or_else(|| {
                SerializationError(Arc::new(
                    ValueListToSerializeRowAdapterError::NoBindMarkerWithName {
                        name: col.name.clone(),
                    },
                ))
            })?;
            append_value(*val);
        }
    }

    Ok(())
}

#[derive(Error, Debug)]
pub enum ValueListToSerializeRowAdapterError {
    #[error("There is no bind marker with name {name}, but a value for it was provided")]
    NoBindMarkerWithName { name: String },
}

#[cfg(test)]
mod tests {
    use crate::frame::response::result::{ColumnSpec, ColumnType, TableSpec};
    use crate::frame::value::{MaybeUnset, SerializedValues, ValueList};
    use crate::types::serialize::BufBackedRowWriter;

    use super::{RowSerializationContext, SerializeRow};

    fn col_spec(name: &str, typ: ColumnType) -> ColumnSpec {
        ColumnSpec {
            table_spec: TableSpec {
                ks_name: "ks".to_string(),
                table_name: "tbl".to_string(),
            },
            name: name.to_string(),
            typ,
        }
    }

    #[test]
    fn test_legacy_fallback() {
        let row = (
            1i32,
            "Ala ma kota",
            None::<i64>,
            MaybeUnset::Unset::<String>,
        );

        let mut legacy_data = Vec::new();
        <_ as ValueList>::write_to_request(&row, &mut legacy_data).unwrap();

        let mut new_data = Vec::new();
        let mut new_data_writer = BufBackedRowWriter::new(&mut new_data);
        let ctx = RowSerializationContext { columns: &[] };
        <_ as SerializeRow>::serialize(&row, &ctx, &mut new_data_writer).unwrap();
        assert_eq!(new_data_writer.value_count(), 4);

        // Skip the value count
        assert_eq!(&legacy_data[2..], new_data);
    }

    #[test]
    fn test_legacy_fallback_with_names() {
        let sorted_row = (
            1i32,
            "Ala ma kota",
            None::<i64>,
            MaybeUnset::Unset::<String>,
        );

        let mut sorted_row_data = Vec::new();
        <_ as ValueList>::write_to_request(&sorted_row, &mut sorted_row_data).unwrap();

        let mut unsorted_row = SerializedValues::new();
        unsorted_row.add_named_value("a", &1i32).unwrap();
        unsorted_row.add_named_value("b", &"Ala ma kota").unwrap();
        unsorted_row
            .add_named_value("d", &MaybeUnset::Unset::<String>)
            .unwrap();
        unsorted_row.add_named_value("c", &None::<i64>).unwrap();

        let mut unsorted_row_data = Vec::new();
        let mut unsorted_row_data_writer = BufBackedRowWriter::new(&mut unsorted_row_data);
        let ctx = RowSerializationContext {
            columns: &[
                col_spec("a", ColumnType::Int),
                col_spec("b", ColumnType::Text),
                col_spec("c", ColumnType::BigInt),
                col_spec("d", ColumnType::Ascii),
            ],
        };
        <_ as SerializeRow>::serialize(&unsorted_row, &ctx, &mut unsorted_row_data_writer).unwrap();
        assert_eq!(unsorted_row_data_writer.value_count(), 4);

        // Skip the value count
        assert_eq!(&sorted_row_data[2..], unsorted_row_data);
    }
}
