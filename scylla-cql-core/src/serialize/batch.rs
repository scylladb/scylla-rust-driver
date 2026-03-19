//! Contains the [`BatchValues`] and [`BatchValuesIterator`] trait and their
//! implementations.

// Note: When editing above doc-comment edit the corresponding comment on
// re-export module in scylla crate too.

use super::row::{RowSerializationContext, SerializeRow};
use super::{RowWriter, SerializationError};

/// Represents a list of sets of values for a batch statement.
///
/// The data in the object can be consumed with an iterator-like object returned
/// by the [`BatchValues::batch_values_iter`] method.
pub trait BatchValues {
    /// An `Iterator`-like object over the values from the parent `BatchValues` object.
    // For some unknown reason, this type, when not resolved to a concrete type for a given async function,
    // cannot live across await boundaries while maintaining the corresponding future `Send`, unless `'r: 'static`
    //
    // See <https://github.com/scylladb/scylla-rust-driver/issues/599> for more details
    type BatchValuesIter<'r>: BatchValuesIterator<'r>
    where
        Self: 'r;

    /// Returns an iterator over the data contained in this object.
    fn batch_values_iter(&self) -> Self::BatchValuesIter<'_>;
}

/// An `Iterator`-like object over the values from the parent [`BatchValues`] object.
///
/// It's not a true [`Iterator`] because it does not provide direct access to the
/// items being iterated over, instead it allows calling methods of the underlying
/// [`SerializeRow`] trait while advancing the iterator.
pub trait BatchValuesIterator<'bv> {
    /// Serializes the next set of values in the sequence and advances the iterator.
    fn serialize_next(
        &mut self,
        ctx: &RowSerializationContext<'_>,
        writer: &mut RowWriter,
    ) -> Option<Result<(), SerializationError>>;

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

/// Implements `BatchValuesIterator` from an `Iterator` over references to things that implement `SerializeRow`
///
/// Essentially used internally by this lib to provide implementers of `BatchValuesIterator` for cases
/// that always serialize the same concrete `SerializeRow` type
pub struct BatchValuesIteratorFromIterator<IT: Iterator> {
    it: IT,
}

impl<'bv, 'sr: 'bv, IT, SR> BatchValuesIterator<'bv> for BatchValuesIteratorFromIterator<IT>
where
    IT: Iterator<Item = &'sr SR>,
    SR: SerializeRow + 'sr,
{
    #[inline]
    fn serialize_next(
        &mut self,
        ctx: &RowSerializationContext<'_>,
        writer: &mut RowWriter,
    ) -> Option<Result<(), SerializationError>> {
        self.it.next().map(|sr| sr.serialize(ctx, writer))
    }

    #[inline]
    fn is_empty_next(&mut self) -> Option<bool> {
        self.it.next().map(|sr| sr.is_empty())
    }

    #[inline]
    fn skip_next(&mut self) -> Option<()> {
        self.it.next().map(|_| ())
    }

    #[inline]
    fn count(self) -> usize
    where
        Self: Sized,
    {
        self.it.count()
    }
}

impl<IT> From<IT> for BatchValuesIteratorFromIterator<IT>
where
    IT: Iterator,
    IT::Item: SerializeRow,
{
    #[inline]
    fn from(it: IT) -> Self {
        BatchValuesIteratorFromIterator { it }
    }
}

//
// BatchValues impls
//

/// Implements `BatchValues` from an `Iterator` over references to things that implement `SerializeRow`
///
/// This is to avoid requiring allocating a new `Vec` containing all the `SerializeRow`s directly:
/// with this, one can write:
/// `session.batch(&batch, BatchValuesFromIter::from(lines_to_insert.iter().map(|l| &l.value_list)))`
/// where `lines_to_insert` may also contain e.g. data to pick the statement...
///
/// The underlying iterator will always be cloned at least once, once to compute the length if it can't be known
/// in advance, and be re-cloned at every retry.
/// It is consequently expected that the provided iterator is cheap to clone (e.g. `slice.iter().map(...)`).
pub struct BatchValuesFromIterator<'sr, IT> {
    it: IT,

    // Without artificially introducing a lifetime to the struct, I couldn't get
    // impl BatchValues for BatchValuesFromIterator to work. I wish I understood
    // why it's needed.
    _phantom: std::marker::PhantomData<&'sr ()>,
}

impl<'sr, IT, SR> BatchValuesFromIterator<'sr, IT>
where
    IT: Iterator<Item = &'sr SR> + Clone,
    SR: SerializeRow + 'sr,
{
    /// Creates a new `BatchValuesFromIter`` object.
    #[inline]
    pub fn new(into_iter: impl IntoIterator<IntoIter = IT>) -> Self {
        Self {
            it: into_iter.into_iter(),
            _phantom: std::marker::PhantomData,
        }
    }
}

impl<'sr, IT, SR> From<IT> for BatchValuesFromIterator<'sr, IT>
where
    IT: Iterator<Item = &'sr SR> + Clone,
    SR: SerializeRow + 'sr,
{
    #[inline]
    fn from(it: IT) -> Self {
        Self::new(it)
    }
}

impl<'sr, IT, SR> BatchValues for BatchValuesFromIterator<'sr, IT>
where
    IT: Iterator<Item = &'sr SR> + Clone,
    SR: SerializeRow + 'sr,
{
    type BatchValuesIter<'r>
        = BatchValuesIteratorFromIterator<IT>
    where
        Self: 'r;

    #[inline]
    fn batch_values_iter(&self) -> Self::BatchValuesIter<'_> {
        self.it.clone().into()
    }
}

// Implement BatchValues for slices of SerializeRow types
impl<T: SerializeRow> BatchValues for [T] {
    type BatchValuesIter<'r>
        = BatchValuesIteratorFromIterator<std::slice::Iter<'r, T>>
    where
        Self: 'r;

    #[inline]
    fn batch_values_iter(&self) -> Self::BatchValuesIter<'_> {
        self.iter().into()
    }
}

// Implement BatchValues for Vec<SerializeRow>
impl<T: SerializeRow> BatchValues for Vec<T> {
    type BatchValuesIter<'r>
        = BatchValuesIteratorFromIterator<std::slice::Iter<'r, T>>
    where
        Self: 'r;

    #[inline]
    fn batch_values_iter(&self) -> Self::BatchValuesIter<'_> {
        BatchValues::batch_values_iter(self.as_slice())
    }
}

// Here is an example implementation for (T0, )
// Further variants are done using a macro
impl<T0: SerializeRow> BatchValues for (T0,) {
    type BatchValuesIter<'r>
        = BatchValuesIteratorFromIterator<std::iter::Once<&'r T0>>
    where
        Self: 'r;

    #[inline]
    fn batch_values_iter(&self) -> Self::BatchValuesIter<'_> {
        std::iter::once(&self.0).into()
    }
}

/// A [`BatchValuesIterator`] over a tuple.
pub struct TupleValuesIter<'sr, T> {
    tuple: &'sr T,
    idx: usize,
}

macro_rules! impl_batch_values_for_tuple {
    ( $($Ti:ident),* ; $($FieldI:tt),* ; $TupleSize:tt) => {
        impl<$($Ti),+> BatchValues for ($($Ti,)+)
        where
            $($Ti: SerializeRow),+
        {
            type BatchValuesIter<'r> = TupleValuesIter<'r, ($($Ti,)+)> where Self: 'r;

            #[inline]
            fn batch_values_iter(&self) -> Self::BatchValuesIter<'_> {
                TupleValuesIter {
                    tuple: self,
                    idx: 0,
                }
            }
        }

        impl<'bv, $($Ti),+> BatchValuesIterator<'bv> for TupleValuesIter<'bv, ($($Ti,)+)>
        where
            $($Ti: SerializeRow),+
        {
            #[inline]
            fn serialize_next(
                &mut self,
                ctx: &RowSerializationContext<'_>,
                writer: &mut RowWriter,
            ) -> Option<Result<(), SerializationError>> {
                let ret = match self.idx {
                    $(
                        $FieldI => self.tuple.$FieldI.serialize(ctx, writer),
                    )*
                    _ => return None,
                };
                self.idx += 1;
                Some(ret)
            }

            #[inline]
            fn is_empty_next(&mut self) -> Option<bool> {
                let ret = match self.idx {
                    $(
                        $FieldI => self.tuple.$FieldI.is_empty(),
                    )*
                    _ => return None,
                };
                self.idx += 1;
                Some(ret)
            }

            #[inline]
            fn skip_next(&mut self) -> Option<()> {
                if self.idx < $TupleSize {
                    self.idx += 1;
                    Some(())
                } else {
                    None
                }
            }

            #[inline]
            fn count(self) -> usize {
                $TupleSize - self.idx
            }
        }
    }
}

impl_batch_values_for_tuple!(T0, T1; 0, 1; 2);
impl_batch_values_for_tuple!(T0, T1, T2; 0, 1, 2; 3);
impl_batch_values_for_tuple!(T0, T1, T2, T3; 0, 1, 2, 3; 4);
impl_batch_values_for_tuple!(T0, T1, T2, T3, T4; 0, 1, 2, 3, 4; 5);
impl_batch_values_for_tuple!(T0, T1, T2, T3, T4, T5; 0, 1, 2, 3, 4, 5; 6);
impl_batch_values_for_tuple!(T0, T1, T2, T3, T4, T5, T6; 0, 1, 2, 3, 4, 5, 6; 7);
impl_batch_values_for_tuple!(T0, T1, T2, T3, T4, T5, T6, T7; 0, 1, 2, 3, 4, 5, 6, 7; 8);
impl_batch_values_for_tuple!(T0, T1, T2, T3, T4, T5, T6, T7, T8; 0, 1, 2, 3, 4, 5, 6, 7, 8; 9);
impl_batch_values_for_tuple!(T0, T1, T2, T3, T4, T5, T6, T7, T8, T9;
                             0, 1, 2, 3, 4, 5, 6, 7, 8, 9; 10);
impl_batch_values_for_tuple!(T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10;
                             0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10; 11);
impl_batch_values_for_tuple!(T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11;
                             0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11; 12);
impl_batch_values_for_tuple!(T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12;
                             0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12; 13);
impl_batch_values_for_tuple!(T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13;
                             0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13; 14);
impl_batch_values_for_tuple!(T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14;
                             0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14; 15);
impl_batch_values_for_tuple!(T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15;
                             0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15; 16);

// Every &impl BatchValues should also implement BatchValues
impl<T: BatchValues + ?Sized> BatchValues for &T {
    type BatchValuesIter<'r>
        = <T as BatchValues>::BatchValuesIter<'r>
    where
        Self: 'r;

    #[inline]
    fn batch_values_iter(&self) -> Self::BatchValuesIter<'_> {
        <T as BatchValues>::batch_values_iter(*self)
    }
}

#[cfg(test)]
#[path = "batch_tests.rs"]
pub(crate) mod tests;
