//! Contains the [`RawBatchValues`] and [`RawBatchValuesIterator`] trait and their
//! implementations.

// Note: When editing above doc-comment edit the corresponding comment on
// re-export module in scylla crate too.

use super::batch::{BatchValues, BatchValuesIterator};
use super::row::{RowSerializationContext, SerializedValues};
use super::{RowWriter, SerializationError};

/// Represents a list of sets of values for a batch statement.
///
/// Unlike [`BatchValues`]), it doesn't require type
/// information from the statements of the batch in order to be serialized.
///
/// This is a lower level trait than [`BatchValues`])
/// and is only used for interaction between the code in `scylla` and
/// `scylla-cql` crates. If you are a regular user of the driver, you shouldn't
/// care about this trait at all.
pub trait RawBatchValues {
    /// An `Iterator`-like object over the values from the parent `BatchValues` object.
    // For some unknown reason, this type, when not resolved to a concrete type for a given async function,
    // cannot live across await boundaries while maintaining the corresponding future `Send`, unless `'r: 'static`
    //
    // See <https://github.com/scylladb/scylla-rust-driver/issues/599> for more details
    type RawBatchValuesIter<'r>: RawBatchValuesIterator<'r>
    where
        Self: 'r;

    /// Returns an iterator over the data contained in this object.
    fn batch_values_iter(&self) -> Self::RawBatchValuesIter<'_>;
}

/// An `Iterator`-like object over the values from the parent [`RawBatchValues`] object.
///
/// It's not a true [`Iterator`] because it does not provide direct access to the
/// items being iterated over, instead it allows calling methods of the underlying
/// [`SerializeRow`](super::row::SerializeRow) trait while advancing the iterator.
///
/// Unlike [`BatchValuesIterator`], it doesn't
/// need type information for serialization.
pub trait RawBatchValuesIterator<'a> {
    /// Serializes the next set of values in the sequence and advances the iterator.
    fn serialize_next(&mut self, writer: &mut RowWriter) -> Option<Result<(), SerializationError>>;

    /// Returns whether the next set of values is empty or not and advances the iterator.
    fn is_empty_next(&mut self) -> Option<bool>;

    /// Skips the next set of values.
    fn skip_next(&mut self) -> Option<()>;

    /// Return the number of sets of values, consuming the iterator in the process.
    #[inline]
    fn count(mut self) -> usize
    where
        Self: Sized,
    {
        let mut count = 0;
        while self.skip_next().is_some() {
            count += 1;
        }
        count
    }
}

// An implementation used by `scylla-proxy`
impl RawBatchValues for Vec<SerializedValues> {
    type RawBatchValuesIter<'r> = std::slice::Iter<'r, SerializedValues>
    where
        Self: 'r;

    fn batch_values_iter(&self) -> Self::RawBatchValuesIter<'_> {
        self.iter()
    }
}

impl<'r> RawBatchValuesIterator<'r> for std::slice::Iter<'r, SerializedValues> {
    #[inline]
    fn serialize_next(&mut self, writer: &mut RowWriter) -> Option<Result<(), SerializationError>> {
        self.next().map(|sv| {
            writer.append_serialize_row(sv);
            Ok(())
        })
    }

    fn is_empty_next(&mut self) -> Option<bool> {
        self.next().map(|sv| sv.is_empty())
    }

    #[inline]
    fn skip_next(&mut self) -> Option<()> {
        self.next().map(|_| ())
    }

    #[inline]
    fn count(self) -> usize {
        <_ as Iterator>::count(self)
    }
}

/// Takes `BatchValues` and an iterator over contexts, and turns them into a `RawBatchValues`.
pub struct RawBatchValuesAdapter<BV, CTX> {
    batch_values: BV,
    contexts: CTX,
}

impl<BV, CTX> RawBatchValuesAdapter<BV, CTX> {
    /// Creates a new `RawBatchValuesAdapter` object.
    #[inline]
    pub fn new(batch_values: BV, contexts: CTX) -> Self {
        Self {
            batch_values,
            contexts,
        }
    }
}

impl<'ctx, BV, CTX> RawBatchValues for RawBatchValuesAdapter<BV, CTX>
where
    BV: BatchValues,
    CTX: Iterator<Item = RowSerializationContext<'ctx>> + Clone,
{
    type RawBatchValuesIter<'r> = RawBatchValuesIteratorAdapter<BV::BatchValuesIter<'r>, CTX>
    where
        Self: 'r;

    #[inline]
    fn batch_values_iter(&self) -> Self::RawBatchValuesIter<'_> {
        RawBatchValuesIteratorAdapter {
            batch_values_iterator: self.batch_values.batch_values_iter(),
            contexts: self.contexts.clone(),
        }
    }
}

/// Takes `BatchValuesIterator` and an iterator over contexts, and turns them into a `RawBatchValuesIterator`.
pub struct RawBatchValuesIteratorAdapter<BVI, CTX> {
    batch_values_iterator: BVI,
    contexts: CTX,
}

impl<'bvi, 'ctx, BVI, CTX> RawBatchValuesIterator<'bvi> for RawBatchValuesIteratorAdapter<BVI, CTX>
where
    BVI: BatchValuesIterator<'bvi>,
    CTX: Iterator<Item = RowSerializationContext<'ctx>>,
{
    #[inline]
    fn serialize_next(&mut self, writer: &mut RowWriter) -> Option<Result<(), SerializationError>> {
        let ctx = self.contexts.next()?;
        self.batch_values_iterator.serialize_next(&ctx, writer)
    }

    fn is_empty_next(&mut self) -> Option<bool> {
        self.contexts.next()?;
        let ret = self.batch_values_iterator.is_empty_next()?;
        Some(ret)
    }

    #[inline]
    fn skip_next(&mut self) -> Option<()> {
        self.contexts.next()?;
        self.batch_values_iterator.skip_next()?;
        Some(())
    }
}
