//! Provides types for dealing with query result deserialization.
//!
//! Those yield raw rows, whose deserialization is handled by
//! the `row` module.

use bytes::Bytes;

use crate::frame::response::result::{
    DeserializedMetadataAndRawRows, ResultMetadata, ResultMetadataHolder,
};

use super::row::{BuiltinDeserializationErrorKind, ColumnIterator, mk_deser_err};
use super::{DeserializationError, FrameSlice};

pub use scylla_cql_core::deserialize::result::{RawRowIterator, TypedRowIterator};

// Technically not an iterator because it returns items that borrow from it,
// and the std Iterator interface does not allow for that.
/// A _lending_ iterator over serialized rows.
///
/// This type is similar to `RawRowIterator`, but keeps ownership of the serialized
/// result. Because it returns `ColumnIterator`s that need to borrow from it,
/// it does not implement the `Iterator` trait (there is no type in the standard
/// library to represent this concept yet).
#[derive(Debug)]
pub struct RawRowLendingIterator {
    metadata: ResultMetadataHolder,
    remaining: usize,
    at: usize,
    raw_rows: Bytes,
}

impl RawRowLendingIterator {
    /// Creates a new `RawRowLendingIterator`, consuming given `RawRows`.
    #[inline]
    pub fn new(raw_rows: DeserializedMetadataAndRawRows) -> Self {
        let (metadata, rows_count, raw_rows) = raw_rows.into_inner();
        Self {
            metadata,
            remaining: rows_count,
            at: 0,
            raw_rows,
        }
    }

    /// Returns a `ColumnIterator` that represents the next row.
    ///
    /// Note: the `ColumnIterator` borrows from the `RawRowLendingIterator`.
    /// The column iterator must be consumed before the rows iterator can
    /// continue.
    #[inline]
    #[expect(clippy::should_implement_trait)] // https://github.com/rust-lang/rust-clippy/issues/5004
    pub fn next(&mut self) -> Option<Result<ColumnIterator<'_, '_>, DeserializationError>> {
        self.remaining = self.remaining.checked_sub(1)?;

        // First create the slice encompassing the whole frame.
        let mut remaining_frame = FrameSlice::new(&self.raw_rows);
        // Then slice it to encompass the remaining suffix of the frame.
        *remaining_frame.as_slice_mut() = &remaining_frame.as_slice()[self.at..];
        // Ideally, we would prefer to preserve the FrameSlice between calls to `next()`,
        // but borrowing from oneself is impossible, so we have to recreate it this way.

        let iter = ColumnIterator::new(self.metadata.inner().col_specs(), remaining_frame);

        // Skip the row here, manually
        for (column_index, spec) in self.metadata.inner().col_specs().iter().enumerate() {
            let remaining_frame_len_before_column_read = remaining_frame.as_slice().len();
            if let Err(err) = remaining_frame.read_cql_bytes() {
                return Some(Err(mk_deser_err::<Self>(
                    BuiltinDeserializationErrorKind::RawColumnDeserializationFailed {
                        column_index,
                        column_name: spec.name().to_owned(),
                        err: DeserializationError::new(err),
                    },
                )));
            } else {
                let remaining_frame_len_after_column_read = remaining_frame.as_slice().len();
                self.at +=
                    remaining_frame_len_before_column_read - remaining_frame_len_after_column_read;
            }
        }

        Some(Ok(iter))
    }

    /// Returns the bounds on the remaining length of the iterator.
    ///
    /// This is analogous to [Iterator::size_hint].
    #[inline]
    pub fn size_hint(&self) -> (usize, Option<usize>) {
        // next() is written in a way that it does not progress on error, so once an error
        // is encountered, the same error keeps being returned until `self.remaining`
        // elements are yielded in total.
        (self.remaining, Some(self.remaining))
    }

    /// Returns the metadata associated with the response (paging state and
    /// column specifications).
    #[inline]
    pub fn metadata(&self) -> &ResultMetadata<'_> {
        self.metadata.inner()
    }

    /// Returns the remaining number of rows that this iterator is expected
    /// to produce.
    #[inline]
    pub fn rows_remaining(&self) -> usize {
        self.remaining
    }
}

#[cfg(test)]
mod tests {
    use crate::frame::response::result::{
        ColumnType, DeserializedMetadataAndRawRows, NativeType, ResultMetadata,
    };

    use super::super::tests::{CELL1, CELL2, serialize_cells, spec};
    use super::RawRowLendingIterator;

    #[test]
    fn test_raw_row_lending_iterator_basic_parse() {
        let raw_data = serialize_cells([Some(CELL1), Some(CELL2), Some(CELL2), Some(CELL1)]);
        let specs = [
            spec("b1", ColumnType::Native(NativeType::Blob)),
            spec("b2", ColumnType::Native(NativeType::Blob)),
        ];
        let mut iter = RawRowLendingIterator::new(DeserializedMetadataAndRawRows::new_for_test(
            ResultMetadata::new_for_test(specs.len(), specs.to_vec()),
            2,
            raw_data,
        ));

        let mut row1 = iter.next().unwrap().unwrap();
        let c11 = row1.next().unwrap().unwrap();
        assert_eq!(c11.slice.unwrap().as_slice(), CELL1);
        let c12 = row1.next().unwrap().unwrap();
        assert_eq!(c12.slice.unwrap().as_slice(), CELL2);
        assert!(row1.next().is_none());

        let mut row2 = iter.next().unwrap().unwrap();
        let c21 = row2.next().unwrap().unwrap();
        assert_eq!(c21.slice.unwrap().as_slice(), CELL2);
        let c22 = row2.next().unwrap().unwrap();
        assert_eq!(c22.slice.unwrap().as_slice(), CELL1);
        assert!(row2.next().is_none());

        assert!(iter.next().is_none());
    }

    #[test]
    fn test_raw_row_lending_iterator_too_few_rows() {
        let raw_data = serialize_cells([Some(CELL1), Some(CELL2)]);
        let specs = [
            spec("b1", ColumnType::Native(NativeType::Blob)),
            spec("b2", ColumnType::Native(NativeType::Blob)),
        ];
        let mut iter = RawRowLendingIterator::new(DeserializedMetadataAndRawRows::new_for_test(
            ResultMetadata::new_for_test(specs.len(), specs.to_vec()),
            2,
            raw_data,
        ));

        iter.next().unwrap().unwrap();
        iter.next().unwrap().unwrap_err();
    }
}
