//! Provides types for dealing with row deserialization.

use super::FrameSlice;
use crate::frame::frame_errors::ParseError;
use crate::frame::response::result::ColumnSpec;

/// Represents a raw, unparsed column value.
#[non_exhaustive]
pub struct RawColumn<'frame> {
    pub spec: &'frame ColumnSpec,
    pub slice: Option<FrameSlice<'frame>>,
}

/// Iterates over columns of a single row.
#[derive(Clone, Debug)]
pub struct ColumnIterator<'frame> {
    specs: std::slice::Iter<'frame, ColumnSpec>,
    slice: FrameSlice<'frame>,
}

impl<'frame> ColumnIterator<'frame> {
    /// Creates a new iterator over a single row.
    ///
    /// - `specs` - information about columns of the serialized response,
    /// - `slice` - a `FrameSlice` which points to the serialized row.
    #[inline]
    pub fn new(specs: &'frame [ColumnSpec], slice: FrameSlice<'frame>) -> Self {
        Self {
            specs: specs.iter(),
            slice,
        }
    }

    /// Returns the remaining number of rows that this iterator is expected
    /// to return.
    #[inline]
    pub fn columns_remaining(&self) -> usize {
        self.specs.len()
    }
}

impl<'frame> Iterator for ColumnIterator<'frame> {
    type Item = Result<RawColumn<'frame>, ParseError>;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        let spec = self.specs.next()?;
        Some(
            self.slice
                .read_cql_bytes()
                .map(|slice| RawColumn { spec, slice }),
        )
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        self.specs.size_hint()
    }
}

/// A type that can be deserialized from a row that was returned from a query.
///
/// For tips on how to write a custom implementation of this trait, see the
/// documentation of the parent module.
///
/// The crate also provides a derive macro which allows to automatically
/// implement the trait for a custom type. For more details on what the macro
/// is capable of, see its documentation.
pub trait DeserializeRow<'frame>
where
    Self: Sized,
{
    /// Checks that the schema of the result matches what this type expects.
    ///
    /// This function can check whether column types and names match the
    /// expectations.
    fn type_check(specs: &[ColumnSpec]) -> Result<(), ParseError>;

    /// Deserializes a row from given column iterator.
    ///
    /// This function can assume that the driver called `type_check` to verify
    /// the row's type. Note that `deserialize` is not an unsafe function,
    /// so it should not use the assumption about `type_check` being called
    /// as an excuse to run `unsafe` code.
    fn deserialize(row: ColumnIterator<'frame>) -> Result<Self, ParseError>;
}

impl<'frame> DeserializeRow<'frame> for ColumnIterator<'frame> {
    #[inline]
    fn type_check(_specs: &[ColumnSpec]) -> Result<(), ParseError> {
        Ok(())
    }

    #[inline]
    fn deserialize(row: ColumnIterator<'frame>) -> Result<Self, ParseError> {
        Ok(row)
    }
}

#[cfg(test)]
mod tests {}
