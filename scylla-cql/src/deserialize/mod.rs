#![doc = include_str!("README.md")]

pub use scylla_cql_core::deserialize::frame_slice;
pub mod result;
pub use scylla_cql_core::deserialize::row;
pub use scylla_cql_core::deserialize::value;

pub use frame_slice::FrameSlice;

// Re-export error types and macro from scylla-cql-core.
pub use scylla_cql_core::deserialize::{DeserializationError, TypeCheckError};

#[cfg(test)]
pub(crate) mod tests {
    use bytes::{Bytes, BytesMut};

    use crate::frame::response::result::{ColumnSpec, ColumnType, TableSpec};
    use crate::frame::types;

    pub(super) static CELL1: &[u8] = &[1, 2, 3];
    pub(super) static CELL2: &[u8] = &[4, 5, 6, 7];

    pub(crate) fn serialize_cells(
        cells: impl IntoIterator<Item = Option<impl AsRef<[u8]>>>,
    ) -> Bytes {
        let mut bytes = BytesMut::new();
        for cell in cells {
            types::write_bytes_opt(cell, &mut bytes).unwrap();
        }
        bytes.freeze()
    }

    pub(crate) const fn spec<'a>(name: &'a str, typ: ColumnType<'a>) -> ColumnSpec<'a> {
        ColumnSpec::borrowed(name, typ, TableSpec::borrowed("ks", "tbl"))
    }
}
