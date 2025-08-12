//! Provides types for dealing with query result deserialization.
//!
//! Those yield raw rows, whose deserialization is handled by
//! the `row` module.

use bytes::Bytes;

use crate::frame::response::result::{
    ColumnSpec, DeserializedMetadataAndRawRows, ResultMetadata, ResultMetadataHolder,
};

use super::row::{mk_deser_err, BuiltinDeserializationErrorKind, ColumnIterator, DeserializeRow};
use super::{DeserializationError, FrameSlice, TypeCheckError};
use std::marker::PhantomData;

/// Iterates over the whole result, returning raw rows.
#[derive(Debug)]
pub struct RawRowIterator<'frame, 'metadata> {
    specs: &'metadata [ColumnSpec<'metadata>],
    remaining: usize,
    slice: FrameSlice<'frame>,
}

impl<'frame, 'metadata> RawRowIterator<'frame, 'metadata> {
    /// Creates a new iterator over raw rows from a serialized response.
    ///
    /// - `remaining` - number of the remaining rows in the serialized response,
    /// - `specs` - information about columns of the serialized response,
    /// - `slice` - a [FrameSlice] that points to the serialized rows data.
    #[inline]
    pub fn new(
        remaining: usize,
        specs: &'metadata [ColumnSpec<'metadata>],
        slice: FrameSlice<'frame>,
    ) -> Self {
        Self {
            specs,
            remaining,
            slice,
        }
    }

    /// Returns information about the columns of rows that are iterated over.
    #[inline]
    pub fn specs(&self) -> &'metadata [ColumnSpec<'metadata>] {
        self.specs
    }

    /// Returns the remaining number of rows that this iterator is supposed
    /// to return.
    #[inline]
    pub fn rows_remaining(&self) -> usize {
        self.remaining
    }
}

impl<'frame, 'metadata> Iterator for RawRowIterator<'frame, 'metadata> {
    type Item = Result<ColumnIterator<'frame, 'metadata>, DeserializationError>;

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
                        column_name: spec.name().to_owned(),
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

/// A typed version of [RawRowIterator] which deserializes the rows before
/// returning them.
#[derive(Debug)]
pub struct TypedRowIterator<'frame, 'metadata, R> {
    inner: RawRowIterator<'frame, 'metadata>,
    _phantom: PhantomData<R>,
}

impl<'frame, 'metadata, R> TypedRowIterator<'frame, 'metadata, R>
where
    R: DeserializeRow<'frame, 'metadata>,
{
    /// Creates a new [TypedRowIterator] from given [RawRowIterator].
    ///
    /// Calls `R::type_check` and fails if the type check fails.
    #[inline]
    pub fn new(raw: RawRowIterator<'frame, 'metadata>) -> Result<Self, TypeCheckError> {
        R::type_check(raw.specs())?;
        Ok(Self {
            inner: raw,
            _phantom: PhantomData,
        })
    }

    /// Returns information about the columns of rows that are iterated over.
    #[inline]
    pub fn specs(&self) -> &'metadata [ColumnSpec<'metadata>] {
        self.inner.specs()
    }

    /// Returns the remaining number of rows that this iterator is supposed
    /// to return.
    #[inline]
    pub fn rows_remaining(&self) -> usize {
        self.inner.rows_remaining()
    }
}

impl<'frame, 'metadata, R> Iterator for TypedRowIterator<'frame, 'metadata, R>
where
    R: DeserializeRow<'frame, 'metadata>,
{
    type Item = Result<R, DeserializationError>;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        self.inner
            .next()
            .map(|raw| raw.and_then(|raw| R::deserialize(raw)))
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        self.inner.size_hint()
    }
}

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

    use bytes::Bytes;
    use std::ops::Deref;
    use std::sync::LazyLock;

    use crate::frame::response::result::{
        ColumnSpec, ColumnType, DeserializedMetadataAndRawRows, NativeType, ResultMetadata,
    };

    use super::super::tests::{serialize_cells, spec, CELL1, CELL2};
    use super::{
        ColumnIterator, DeserializationError, FrameSlice, RawRowIterator, RawRowLendingIterator,
        TypedRowIterator,
    };

    trait LendingIterator {
        type Item<'borrow>
        where
            Self: 'borrow;
        fn lend_next(&mut self) -> Option<Result<Self::Item<'_>, DeserializationError>>;
    }

    impl<'frame, 'metadata> LendingIterator for RawRowIterator<'frame, 'metadata> {
        type Item<'borrow>
            = ColumnIterator<'borrow, 'borrow>
        where
            Self: 'borrow;

        fn lend_next(&mut self) -> Option<Result<ColumnIterator<'_, '_>, DeserializationError>> {
            self.next()
        }
    }

    impl LendingIterator for RawRowLendingIterator {
        type Item<'borrow> = ColumnIterator<'borrow, 'borrow>;

        fn lend_next(&mut self) -> Option<Result<ColumnIterator<'_, '_>, DeserializationError>> {
            self.next()
        }
    }

    #[test]
    fn test_row_iterators_basic_parse() {
        // Those statics are required because of a compiler bug-limitation about GATs:
        // https://blog.rust-lang.org/2022/10/28/gats-stabilization.html#implied-static-requirement-from-higher-ranked-trait-bounds
        // the following type higher-ranked lifetime constraint implies 'static lifetime.
        //
        // I: for<'item> LendingIterator<Item<'item> = ColumnIterator<'item>>,
        //
        // The bug is said to be a lot of effort to fix, so in tests let's just use `LazyLock`
        // to obtain 'static references.

        static SPECS: &[ColumnSpec<'static>] = &[
            spec("b1", ColumnType::Native(NativeType::Blob)),
            spec("b2", ColumnType::Native(NativeType::Blob)),
        ];
        static RAW_DATA: LazyLock<Bytes> =
            LazyLock::new(|| serialize_cells([Some(CELL1), Some(CELL2), Some(CELL2), Some(CELL1)]));
        let raw_data = RAW_DATA.deref();
        let specs = SPECS;

        let row_iter = RawRowIterator::new(2, specs, FrameSlice::new(raw_data));
        let lending_row_iter =
            RawRowLendingIterator::new(DeserializedMetadataAndRawRows::new_for_test(
                ResultMetadata::new_for_test(specs.len(), specs.to_vec()),
                2,
                raw_data.clone(),
            ));
        check(row_iter);
        check(lending_row_iter);

        fn check<I>(mut iter: I)
        where
            I: for<'item> LendingIterator<Item<'item> = ColumnIterator<'item, 'item>>,
        {
            let mut row1 = iter.lend_next().unwrap().unwrap();
            let c11 = row1.next().unwrap().unwrap();
            assert_eq!(c11.slice.unwrap().as_slice(), CELL1);
            let c12 = row1.next().unwrap().unwrap();
            assert_eq!(c12.slice.unwrap().as_slice(), CELL2);
            assert!(row1.next().is_none());

            let mut row2 = iter.lend_next().unwrap().unwrap();
            let c21 = row2.next().unwrap().unwrap();
            assert_eq!(c21.slice.unwrap().as_slice(), CELL2);
            let c22 = row2.next().unwrap().unwrap();
            assert_eq!(c22.slice.unwrap().as_slice(), CELL1);
            assert!(row2.next().is_none());

            assert!(iter.lend_next().is_none());
        }
    }

    #[test]
    fn test_row_iterators_too_few_rows() {
        static SPECS: &[ColumnSpec<'static>] = &[
            spec("b1", ColumnType::Native(NativeType::Blob)),
            spec("b2", ColumnType::Native(NativeType::Blob)),
        ];
        static RAW_DATA: LazyLock<Bytes> =
            LazyLock::new(|| serialize_cells([Some(CELL1), Some(CELL2)]));

        let raw_data = RAW_DATA.deref();
        let specs = SPECS;

        let row_iter = RawRowIterator::new(2, specs, FrameSlice::new(raw_data));
        let lending_row_iter =
            RawRowLendingIterator::new(DeserializedMetadataAndRawRows::new_for_test(
                ResultMetadata::new_for_test(specs.len(), specs.to_vec()),
                2,
                raw_data.clone(),
            ));
        check(row_iter);
        check(lending_row_iter);

        fn check<I>(mut iter: I)
        where
            I: for<'item> LendingIterator<Item<'item> = ColumnIterator<'item, 'item>>,
        {
            iter.lend_next().unwrap().unwrap();
            iter.lend_next().unwrap().unwrap_err();
        }
    }

    #[test]
    fn test_typed_row_iterator_basic_parse() {
        let raw_data = serialize_cells([Some(CELL1), Some(CELL2), Some(CELL2), Some(CELL1)]);
        let specs = [
            spec("b1", ColumnType::Native(NativeType::Blob)),
            spec("b2", ColumnType::Native(NativeType::Blob)),
        ];
        let iter = RawRowIterator::new(2, &specs, FrameSlice::new(&raw_data));
        let mut iter = TypedRowIterator::<'_, '_, (&[u8], Vec<u8>)>::new(iter).unwrap();

        let (c11, c12) = iter.next().unwrap().unwrap();
        assert_eq!(c11, CELL1);
        assert_eq!(c12, CELL2);

        let (c21, c22) = iter.next().unwrap().unwrap();
        assert_eq!(c21, CELL2);
        assert_eq!(c22, CELL1);

        assert!(iter.next().is_none());
    }

    #[test]
    fn test_typed_row_iterator_wrong_type() {
        let raw_data = Bytes::new();
        let specs = [
            spec("b1", ColumnType::Native(NativeType::Blob)),
            spec("b2", ColumnType::Native(NativeType::Blob)),
        ];
        let iter = RawRowIterator::new(0, &specs, FrameSlice::new(&raw_data));
        assert!(TypedRowIterator::<'_, '_, (i32, i64)>::new(iter).is_err());
    }
}
