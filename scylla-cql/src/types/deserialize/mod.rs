//! Framework for deserialization of data returned by database queries.
//!
//! Deserialization is based on two traits:
//!
//! - A type that implements `DeserializeCql<'frame>` can be deserialized
//!   from a single _CQL value_ - i.e. an element of a row in the query result,
//! - A type that implements `DeserializeRow<'frame>` can be deserialized
//!   from a single _row_ of a query result.
//!
//! Those traits are quite similar to each other, both in the idea behind them
//! and the interface that they expose.
//!
//! # `type_check` and `deserialize`
//!
//! The deserialization process is divided into two parts: type checking and
//! actual deserialization, represented by `DeserializeCql`/`DeserializeRow`'s
//! methods called `type_check` and `deserialize`.
//!
//! The `deserialize` method can assume that `type_check` was called before, so
//! it doesn't have to verify the type again. This can be a performance gain
//! when deserializing query results with multiple rows: as each row in a result
//! has the same type, it is only necessary to call `type_check` once for the
//! whole result and then `deserialize` for each row.
//!
//! Note that `deserialize` is not an `unsafe` method - although you can be
//! sure that the driver will call `type_check` before `deserialize`, you
//! shouldn't do unsafe things based on this assumption.
//!
//! # Data ownership
//!
//! Some CQL types can be easily consumed while still partially serialized.
//! For example, types like `blob` or `text` can be just represented with
//! `&[u8]` and `&str` that just point to a part of the serialized response.
//! This is more efficient than using `Vec<u8>` or `String` because it avoids
//! an allocation and a copy, however it is less convenient because those types
//! are bound with a lifetime.
//!
//! The framework supports types that refer to the serialized response's memory
//! in three different ways:
//!
//! ## Owned types
//!
//! Some types don't borrow anything and fully own their data, e.g. `i32` or
//! `String`. They aren't constrained by any lifetime and should implement
//! the respective trait for _all_ lifetimes, i.e.:
//!
//! ```rust
//! # use scylla_cql::types::deserialize::{value::DeserializeCql, FrameSlice};
//! # use scylla_cql::frame::response::result::ColumnType;
//! # use scylla_cql::frame::frame_errors::ParseError;
//! struct MyVec(Vec<u8>);
//! impl<'frame> DeserializeCql<'frame> for MyVec {
//!     fn type_check(typ: &ColumnType) -> Result<(), ParseError> {
//!          if let ColumnType::Blob = typ {
//!              return Ok(());
//!          }
//!          Err(ParseError::BadIncomingData("Expected bytes".to_string()))
//!      }
//!
//!      fn deserialize(
//!          _typ: &'frame ColumnType,
//!          v: Option<FrameSlice<'frame>>,
//!      ) -> Result<Self, ParseError> {
//!          v.ok_or_else(|| {
//!              ParseError::BadIncomingData("Expected non-null value".to_string())
//!          })
//!          .map(|v| Self(v.as_slice().to_vec()))
//!      }
//! }
//! ```
//!
//! ## Borrowing types
//!
//! Some types do not fully contain their data but rather will point to some
//! bytes in the serialized response, e.g. `&str` or `&[u8]`. Those types will
//! usually contain a lifetime in their definition. In order to properly
//! implement `DeserializeCql` or `DeserializeRow` for such a type, the `impl`
//! should still have a generic lifetime parameter, but the lifetimes from the
//! type definition should be constrained with the generic lifetime parameter.
//! For example:
//!
//! ```rust
//! # use scylla_cql::types::deserialize::{value::DeserializeCql, FrameSlice};
//! # use scylla_cql::frame::response::result::ColumnType;
//! # use scylla_cql::frame::frame_errors::ParseError;
//! struct MySlice<'a>(&'a [u8]);
//! impl<'a, 'frame> DeserializeCql<'frame> for MySlice<'a>
//! where
//!     'frame: 'a,
//! {
//!     fn type_check(typ: &ColumnType) -> Result<(), ParseError> {
//!          if let ColumnType::Blob = typ {
//!              return Ok(());
//!          }
//!          Err(ParseError::BadIncomingData("Expected bytes".to_string()))
//!      }
//!
//!      fn deserialize(
//!          _typ: &'frame ColumnType,
//!          v: Option<FrameSlice<'frame>>,
//!      ) -> Result<Self, ParseError> {
//!          v.ok_or_else(|| {
//!             ParseError::BadIncomingData("Expected non-null value".to_string())
//!          })
//!          .map(|v| Self(v.as_slice()))
//!      }
//! }
//! ```
//!
//! ## Reference-counted types (`DeserializeCql` only)
//!
//! Internally, the driver uses the `bytes::Bytes` type to keep the contents
//! of the serialized response. It supports creating derived `Bytes` objects
//! which point to a subslice but keep the whole, original `Bytes` object alive.
//!
//! During deserialization, a type can obtain a `Bytes` subslice that points
//! to the serialized value. This approach combines advantages of the previous
//! two approaches - creating a derived `Bytes` object can be cheaper than
//! allocation and a copy (it supports `Arc`-like semantics) and the `Bytes`
//! type is not constrained by a lifetime. However, you should be aware that
//! the subslice will keep the whole `Bytes` object that holds the frame alive.
//! It is not recommended to use this approach for long-living objects because
//! it can introduce space leaks.
//!
//! Example:
//!
//! ```rust
//! # use scylla_cql::types::deserialize::{value::DeserializeCql, FrameSlice};
//! # use scylla_cql::frame::response::result::ColumnType;
//! # use scylla_cql::frame::frame_errors::ParseError;
//! # use bytes::Bytes;
//! struct MyBytes(Bytes);
//! impl<'frame> DeserializeCql<'frame> for MyBytes {
//!     fn type_check(typ: &ColumnType) -> Result<(), ParseError> {
//!          if let ColumnType::Blob = typ {
//!              return Ok(());
//!          }
//!          Err(ParseError::BadIncomingData("Expected bytes".to_string()))
//!      }
//!
//!      fn deserialize(
//!          _typ: &'frame ColumnType,
//!          v: Option<FrameSlice<'frame>>,
//!      ) -> Result<Self, ParseError> {
//!          v.ok_or_else(|| {
//!              ParseError::BadIncomingData("Expected non-null value".to_string())
//!          })
//!          .map(|v| Self(v.to_bytes()))
//!      }
//! }
//! ```

pub mod row;
pub mod value;

use std::marker::PhantomData;

use bytes::Bytes;

use crate::frame::frame_errors::ParseError;
use crate::frame::response::result::ColumnSpec;
use crate::frame::types;

use self::row::{ColumnIterator, DeserializeRow};

/// A reference to a part of the frame.
#[derive(Clone, Copy, Debug)]
pub struct FrameSlice<'frame> {
    // The actual subslice represented by this FrameSlice.
    frame_subslice: &'frame [u8],

    // Reference to the original Bytes object that this FrameSlice is derived
    // from. It is used to convert the `mem` slice into a fully blown Bytes
    // object via Bytes::slice_ref method.
    original_frame: &'frame Bytes,
}

static EMPTY_BYTES: Bytes = Bytes::new();

impl<'frame> FrameSlice<'frame> {
    /// Creates a new FrameSlice from a reference of a Bytes object.
    ///
    /// This method is exposed to allow writing deserialization tests
    /// for custom types.
    #[inline]
    pub fn new(frame: &'frame Bytes) -> Self {
        Self {
            frame_subslice: frame,
            original_frame: frame,
        }
    }

    /// Creates a new FrameSlice that refers to a subslice of a given Bytes object.
    #[inline]
    pub fn new_subslice(mem: &'frame [u8], frame: &'frame Bytes) -> Self {
        Self {
            frame_subslice: mem,
            original_frame: frame,
        }
    }

    /// Creates an empty FrameSlice.
    #[inline]
    pub fn new_empty() -> Self {
        Self {
            frame_subslice: &EMPTY_BYTES,
            original_frame: &EMPTY_BYTES,
        }
    }

    /// Returns a reference to the slice.
    #[inline]
    pub fn as_slice(&self) -> &'frame [u8] {
        self.frame_subslice
    }

    /// Returns `true` if the slice has length of 0.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.frame_subslice.is_empty()
    }

    /// Returns a reference to the Bytes object which encompasses the slice.
    ///
    /// The Bytes object will usually be larger than the slice returned by
    /// [FrameSlice::as_slice]. If you wish to obtain a new Bytes object that
    /// points only to the subslice represented by the FrameSlice object,
    /// see [FrameSlice::to_bytes].
    #[inline]
    pub fn as_bytes_ref(&self) -> &'frame Bytes {
        self.original_frame
    }

    /// Returns a new Bytes object which is a subslice of the original slice
    /// object.
    #[inline]
    pub fn to_bytes(&self) -> Bytes {
        self.original_frame.slice_ref(self.frame_subslice)
    }

    /// Reads and consumes a `[bytes]` item from the beginning of the frame.
    ///
    /// If the operation fails then the slice remains unchanged.
    #[inline]
    fn read_cql_bytes(&mut self) -> Result<Option<FrameSlice<'frame>>, ParseError> {
        match types::read_bytes_opt(&mut self.frame_subslice) {
            Ok(Some(slice)) => Ok(Some(Self::new_subslice(slice, self.original_frame))),
            Ok(None) => Ok(None),
            Err(err) => Err(err),
        }
    }
}

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
    /// - `slice` - a `FrameSlice` that points to the serialized rows data.
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
    type Item = Result<ColumnIterator<'frame>, ParseError>;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        self.remaining = self.remaining.checked_sub(1)?;

        let iter = ColumnIterator::new(self.specs, self.slice);

        // Skip the row here, manually
        for _ in 0..self.specs.len() {
            if let Err(err) = self.slice.read_cql_bytes() {
                return Some(Err(err));
            }
        }

        Some(Ok(iter))
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        (0, Some(self.remaining))
    }
}

/// A typed version of `RowIterator` which deserializes the rows before
/// returning them.
pub struct TypedRowIterator<'frame, R> {
    inner: RowIterator<'frame>,
    _phantom: PhantomData<R>,
}

impl<'frame, R> TypedRowIterator<'frame, R>
where
    R: DeserializeRow<'frame>,
{
    /// Creates a new `TypedRowIterator` from given `RowIterator`.
    ///
    /// Calls `R::type_check` and fails if the type check fails.
    #[inline]
    pub fn new(raw: RowIterator<'frame>) -> Result<Self, ParseError> {
        R::type_check(raw.specs())?;
        Ok(Self {
            inner: raw,
            _phantom: PhantomData,
        })
    }

    /// Returns information about the columns of rows that are iterated over.
    #[inline]
    pub fn specs(&self) -> &'frame [ColumnSpec] {
        self.inner.specs()
    }

    /// Returns the remaining number of rows that this iterator is supposed
    /// to return.
    #[inline]
    pub fn rows_remaining(&self) -> usize {
        self.inner.rows_remaining()
    }
}

impl<'frame, R> Iterator for TypedRowIterator<'frame, R>
where
    R: DeserializeRow<'frame>,
{
    type Item = Result<R, ParseError>;

    fn next(&mut self) -> Option<Self::Item> {
        let raw = match self.inner.next() {
            Some(Ok(raw)) => raw,
            Some(Err(err)) => return Some(Err(err)),
            None => return None,
        };

        Some(R::deserialize(raw))
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.inner.size_hint()
    }
}

#[cfg(test)]
mod tests {
    use crate::frame::{
        response::result::{ColumnType, TableSpec},
        types,
    };

    use super::*;

    use bytes::{Bytes, BytesMut};

    static CELL1: &[u8] = &[1, 2, 3];
    static CELL2: &[u8] = &[4, 5, 6, 7];

    pub(super) fn serialize_cells(
        cells: impl IntoIterator<Item = Option<impl AsRef<[u8]>>>,
    ) -> Bytes {
        let mut bytes = BytesMut::new();
        for cell in cells {
            types::write_bytes_opt(cell, &mut bytes).unwrap();
        }
        bytes.freeze()
    }

    fn spec(name: &str, typ: ColumnType) -> ColumnSpec {
        ColumnSpec {
            name: name.to_owned(),
            typ,
            table_spec: TableSpec {
                ks_name: "ks".to_owned(),
                table_name: "tbl".to_owned(),
            },
        }
    }

    #[test]
    fn test_cql_bytes_consumption() {
        let frame = serialize_cells([Some(CELL1), None, Some(CELL2)]);
        let mut slice = FrameSlice::new(&frame);
        assert!(!slice.is_empty());

        assert_eq!(
            slice.read_cql_bytes().unwrap().map(|s| s.as_slice()),
            Some(CELL1)
        );
        assert!(!slice.is_empty());
        assert!(slice.read_cql_bytes().unwrap().is_none());
        assert!(!slice.is_empty());
        assert_eq!(
            slice.read_cql_bytes().unwrap().map(|s| s.as_slice()),
            Some(CELL2)
        );
        assert!(slice.is_empty());
        slice.read_cql_bytes().unwrap_err();
        assert!(slice.is_empty());
    }

    #[test]
    fn test_cql_bytes_owned() {
        let frame = serialize_cells([Some(CELL1), Some(CELL2)]);
        let mut slice = FrameSlice::new(&frame);

        let subslice1 = slice.read_cql_bytes().unwrap().unwrap();
        let subslice2 = slice.read_cql_bytes().unwrap().unwrap();

        assert_eq!(subslice1.as_slice(), CELL1);
        assert_eq!(subslice2.as_slice(), CELL2);

        assert_eq!(
            subslice1.as_bytes_ref() as *const Bytes,
            &frame as *const Bytes
        );
        assert_eq!(
            subslice2.as_bytes_ref() as *const Bytes,
            &frame as *const Bytes
        );

        let subslice1_bytes = subslice1.to_bytes();
        let subslice2_bytes = subslice2.to_bytes();

        assert_eq!(subslice1.as_slice(), subslice1_bytes.as_ref());
        assert_eq!(subslice2.as_slice(), subslice2_bytes.as_ref());
    }

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

    #[test]
    fn test_typed_row_iterator_basic_parse() {
        let raw_data = serialize_cells([Some(CELL1), Some(CELL2), Some(CELL2), Some(CELL1)]);
        let specs = [spec("b1", ColumnType::Blob), spec("b2", ColumnType::Blob)];
        let iter = RowIterator::new(2, &specs, FrameSlice::new(&raw_data));
        let mut iter = TypedRowIterator::<'_, (&[u8], Vec<u8>)>::new(iter).unwrap();

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
        let specs = [spec("b1", ColumnType::Blob), spec("b2", ColumnType::Blob)];
        let iter = RowIterator::new(0, &specs, FrameSlice::new(&raw_data));
        assert!(TypedRowIterator::<'_, (i32, i64)>::new(iter).is_err());
    }
}
