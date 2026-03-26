//! Provides types for dealing with query result deserialization.
//!
//! Those yield raw rows, whose deserialization is handled by
//! the `row` module.

use super::row::{BuiltinDeserializationErrorKind, ColumnIterator, DeserializeRow, mk_deser_err};
use super::{DeserializationError, FrameSlice, TypeCheckError};
use crate::frame::response::result::ColumnSpec;
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
