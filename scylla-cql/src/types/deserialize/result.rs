use crate::frame::response::result::ColumnSpec;

use super::row::{mk_deser_err, BuiltinDeserializationErrorKind, ColumnIterator};
use super::{DeserializationError, FrameSlice};

/// Iterates over the whole result, returning rows.
pub struct RowIterator<'frame> {
    specs: &'frame [ColumnSpec],
    remaining: usize,
    slice: FrameSlice<'frame>,
}

impl<'frame> RowIterator<'frame> {
    /// Creates a new iterator over rows from a serialized response.
    ///
    /// - `remaining` - number of the remaining rows in the serialized response,
    /// - `specs` - information about columns of the serialized response,
    /// - `slice` - a [FrameSlice] that points to the serialized rows data.
    #[inline]
    pub fn new(remaining: usize, specs: &'frame [ColumnSpec], slice: FrameSlice<'frame>) -> Self {
        Self {
            specs,
            remaining,
            slice,
        }
    }

    /// Returns information about the columns of rows that are iterated over.
    #[inline]
    pub fn specs(&self) -> &'frame [ColumnSpec] {
        self.specs
    }

    /// Returns the remaining number of rows that this iterator is supposed
    /// to return.
    #[inline]
    pub fn rows_remaining(&self) -> usize {
        self.remaining
    }
}

impl<'frame> Iterator for RowIterator<'frame> {
    type Item = Result<ColumnIterator<'frame>, DeserializationError>;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        self.remaining = self.remaining.checked_sub(1)?;

        let iter = ColumnIterator::new(self.specs, self.slice);

        // Skip the row here, manually
        for (column_index, spec) in self.specs.iter().enumerate() {
            if let Err(err) = self.slice.read_cql_bytes() {
                return Some(Err(mk_deser_err::<Self>(
                    BuiltinDeserializationErrorKind::RawColumnDeserializationFailed {
                        column_index,
                        column_name: spec.name.clone(),
                        err: DeserializationError::new(err),
                    },
                )));
            }
        }

        Some(Ok(iter))
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        // The iterator will always return exactly `self.remaining`
        // elements: Oks until an error is encountered and then Errs
        // containing that same first encountered error.
        (self.remaining, Some(self.remaining))
    }
}

#[cfg(test)]
mod tests {
    use crate::frame::response::result::ColumnType;

    use super::super::tests::{serialize_cells, spec, CELL1, CELL2};
    use super::{FrameSlice, RowIterator};

    #[test]
    fn test_row_iterator_basic_parse() {
        let raw_data = serialize_cells([Some(CELL1), Some(CELL2), Some(CELL2), Some(CELL1)]);
        let specs = [spec("b1", ColumnType::Blob), spec("b2", ColumnType::Blob)];
        let mut iter = RowIterator::new(2, &specs, FrameSlice::new(&raw_data));

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
    fn test_row_iterator_too_few_rows() {
        let raw_data = serialize_cells([Some(CELL1), Some(CELL2)]);
        let specs = [spec("b1", ColumnType::Blob), spec("b2", ColumnType::Blob)];
        let mut iter = RowIterator::new(2, &specs, FrameSlice::new(&raw_data));

        iter.next().unwrap().unwrap();
        assert!(iter.next().unwrap().is_err());
    }
}
