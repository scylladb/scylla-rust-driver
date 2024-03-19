//! Provides types for dealing with CQL value deserialization.

use super::FrameSlice;
use crate::frame::{frame_errors::ParseError, response::result::ColumnType};

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

#[cfg(test)]
mod tests {}
