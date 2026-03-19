//! Contains the [`BatchValues`] and [`BatchValuesIterator`] trait and their
//! implementations.

// Note: When editing above doc-comment edit the corresponding comment on
// re-export module in scylla crate too.

pub use scylla_cql_core::serialize::batch::{
    BatchValues, BatchValuesFromIterator, BatchValuesIterator, BatchValuesIteratorFromIterator,
    TupleValuesIter,
};

#[cfg(test)]
#[path = "batch_tests.rs"]
pub(crate) mod tests;
