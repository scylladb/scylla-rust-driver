//! Contains types and traits used for safe serialization of values for a CQL statement.

// Note: When editing above doc-comment edit the corresponding comment on
// re-export module in scylla crate too.

use thiserror::Error;

use super::row::SerializedValues;
use crate::frame::types::unsigned_vint_encode;

/// An interface that facilitates writing values for a CQL query.
pub struct RowWriter<'buf> {
    // Buffer that this value should be serialized to.
    buf: &'buf mut Vec<u8>,

    // Number of values written so far.
    value_count: usize,
}

impl<'buf> RowWriter<'buf> {
    /// Creates a new row writer based on an existing Vec.
    ///
    /// The newly created row writer will append data to the end of the vec.
    #[inline]
    pub fn new(buf: &'buf mut Vec<u8>) -> Self {
        Self {
            buf,
            value_count: 0,
        }
    }

    /// Returns the number of values that were written so far.
    ///
    /// Note that the protocol allows at most u16::MAX to be written into a query,
    /// but the writer's interface allows more to be written.
    #[inline]
    pub fn value_count(&self) -> usize {
        self.value_count
    }

    /// Appends a new value to the sequence and returns an object that allows
    /// to fill it in.
    #[inline]
    pub fn make_cell_writer(&mut self) -> CellWriter<'_> {
        self.value_count += 1;
        CellWriter::new(self.buf)
    }

    /// Appends the values from an existing [`SerializedValues`] object to the
    /// current `RowWriter`.
    #[inline]
    pub fn append_serialize_row(&mut self, sv: &SerializedValues) {
        self.value_count += sv.element_count() as usize;
        self.buf.extend_from_slice(sv.get_contents())
    }
}

/// Represents a handle to a CQL value that needs to be written into.
///
/// The writer can either be transformed into a ready value right away
/// (via [`set_null`](CellWriter::set_null),
/// [`set_unset`](CellWriter::set_unset)
/// or [`set_value`](CellWriter::set_value) or transformed into
/// the [`CellValueBuilder`] in order to gradually initialize
/// the value when the contents are not available straight away.
///
/// After the value is fully initialized, the handle is consumed and
/// a [`WrittenCellProof`] object is returned
/// in its stead. This is a type-level proof that the value was fully initialized
/// and is used in [`SerializeValue::serialize`](`super::value::SerializeValue::serialize`)
/// in order to enforce the implementer to fully initialize the provided handle
/// to CQL value.
///
/// Dropping this type without calling any of its methods will result
/// in nothing being written.
pub struct CellWriter<'buf> {
    buf: &'buf mut Vec<u8>,
    size_prefix: SizePrefix,
}

/// How the size of a value is written before its contents.
#[derive(Clone, Copy)]
enum SizePrefix {
    /// An `[int]`, as for values of a row or elements of a collection.
    Int,
    /// An `[unsigned vint]`, as for variable-length elements of a vector.
    UnsignedVint,
    /// No size, as for fixed-length elements of a vector.
    Omitted,
}

impl<'buf> CellWriter<'buf> {
    /// Creates a new cell writer based on an existing Vec.
    ///
    /// The newly created row writer will append data to the end of the vec.
    #[inline]
    pub fn new(buf: &'buf mut Vec<u8>) -> Self {
        Self {
            buf,
            size_prefix: SizePrefix::Int,
        }
    }

    // Creates a new cell writer based on an existing Vec, without writing size.
    ///
    /// The newly created row writer will append data to the end of the vec.
    #[inline]
    pub fn new_without_size(buf: &'buf mut Vec<u8>) -> Self {
        Self {
            buf,
            size_prefix: SizePrefix::Omitted,
        }
    }

    // Not public: a value with a vint size is only ever an element of a vector,
    // written with `CellValueBuilder::make_sub_writer_with_vint_size`.
    #[inline]
    fn new_with_vint_size(buf: &'buf mut Vec<u8>) -> Self {
        Self {
            buf,
            size_prefix: SizePrefix::UnsignedVint,
        }
    }

    /// Sets this value to be null, consuming this object.
    #[inline]
    pub fn set_null(self) -> WrittenCellProof<'buf> {
        self.buf.extend_from_slice(&(-1i32).to_be_bytes());
        WrittenCellProof::new()
    }

    /// Sets this value to represent an unset value, consuming this object.
    #[inline]
    pub fn set_unset(self) -> WrittenCellProof<'buf> {
        self.buf.extend_from_slice(&(-2i32).to_be_bytes());
        WrittenCellProof::new()
    }

    /// Sets this value to a non-zero, non-unset value with given contents.
    ///
    /// Prefer this to [`into_value_builder`](CellWriter::into_value_builder)
    /// if you have all of the contents of the value ready up front (e.g. for
    /// fixed size types).
    ///
    /// Fails if the contents size overflows the maximum allowed CQL cell size
    /// (which is i32::MAX).
    #[inline]
    pub fn set_value(self, contents: &[u8]) -> Result<WrittenCellProof<'buf>, CellOverflowError> {
        let value_len: i32 = contents.len().try_into().map_err(|_| CellOverflowError)?;
        match self.size_prefix {
            SizePrefix::Int => self.buf.extend_from_slice(&value_len.to_be_bytes()),
            SizePrefix::UnsignedVint => unsigned_vint_encode(contents.len() as u64, self.buf),
            SizePrefix::Omitted => {}
        }
        self.buf.extend_from_slice(contents);
        Ok(WrittenCellProof::new())
    }

    /// Turns this writter into a [`CellValueBuilder`] which can be used
    /// to gradually initialize the CQL value.
    ///
    /// This method should be used if you don't have all of the data
    /// up front, e.g. when serializing compound types such as collections
    /// or UDTs.
    #[inline]
    pub fn into_value_builder(self) -> CellValueBuilder<'buf> {
        CellValueBuilder::new(self.buf, self.size_prefix)
    }
}

/// Allows appending bytes to a non-null, non-unset cell.
///
/// This object needs to be dropped in order for the value to be correctly
/// serialized. Failing to drop this value will result in a payload that will
/// not be parsed by the database correctly, but otherwise should not cause
/// data to be misinterpreted.
pub struct CellValueBuilder<'buf> {
    // Buffer that this value should be serialized to.
    buf: &'buf mut Vec<u8>,

    // Starting position of the value in the buffer.
    starting_pos: usize,

    // How the size of the value is written.
    size_prefix: SizePrefix,
}

impl<'buf> CellValueBuilder<'buf> {
    #[inline]
    fn new(buf: &'buf mut Vec<u8>, size_prefix: SizePrefix) -> Self {
        let starting_pos = buf.len();
        match size_prefix {
            // "Length" of a [bytes] frame can either be a non-negative i32,
            // -1 (null) or -2 (not set). Push an invalid value here. It will be
            // overwritten eventually either by set_null, set_unset or Drop.
            // If the CellSerializer is not dropped as it should, this will trigger
            // an error on the DB side and the serialized data
            // won't be misinterpreted.
            SizePrefix::Int => buf.extend_from_slice(&(-3i32).to_be_bytes()),
            // The size is only known once the value is written. A byte is set
            // aside for it, as most values are shorter than 128 bytes and so
            // have a one-byte size; `finish` moves longer values to make room.
            // Not `push`: see `write_vint_size`.
            SizePrefix::UnsignedVint => buf.extend_from_slice(&[0]),
            SizePrefix::Omitted => {}
        }
        Self {
            buf,
            starting_pos,
            size_prefix,
        }
    }

    /// Appends raw bytes to this cell.
    #[inline]
    pub fn append_bytes(&mut self, bytes: &[u8]) {
        self.buf.extend_from_slice(bytes);
    }

    /// Appends a sub-value to the end of the current contents of the cell
    /// and returns an object that allows to fill it in.
    #[inline]
    pub fn make_sub_writer(&mut self) -> CellWriter<'_> {
        CellWriter::new(self.buf)
    }

    /// Appends a sub-value to the end of the current contents of the cell
    /// and returns an object that allows to fill it in, without writing size.
    #[inline]
    pub fn make_sub_writer_without_size(&mut self) -> CellWriter<'_> {
        CellWriter::new_without_size(self.buf)
    }

    /// Appends a sub-value to the end of the current contents of the cell
    /// and returns an object that allows to fill it in, with its size written
    /// as an unsigned vint. This is how vectors encode elements of
    /// variable-length types.
    #[inline]
    pub fn make_sub_writer_with_vint_size(&mut self) -> CellWriter<'_> {
        CellWriter::new_with_vint_size(self.buf)
    }

    /// Finishes serializing the value.
    ///
    /// Fails if the constructed cell size overflows the maximum allowed
    /// CQL cell size (which is i32::MAX).
    #[inline]
    pub fn finish(self) -> Result<WrittenCellProof<'buf>, CellOverflowError> {
        match self.size_prefix {
            SizePrefix::Int => {
                let value_len: i32 = (self.buf.len() - self.starting_pos - 4)
                    .try_into()
                    .map_err(|_| CellOverflowError)?;
                self.buf[self.starting_pos..self.starting_pos + 4]
                    .copy_from_slice(&value_len.to_be_bytes());
            }
            SizePrefix::UnsignedVint => Self::write_vint_size(self.buf, self.starting_pos)?,
            SizePrefix::Omitted => {}
        }
        Ok(WrittenCellProof::new())
    }

    // Serializers keep the buffer's length in a register between their writes
    // only if the compiler can tell that nothing else holds a pointer to the
    // buffer. Handing the buffer to a function whose body it can't see - like
    // this one, if it were not `#[inline]`, or `Vec::push`'s growth path in
    // the standard library - makes it reload the length after every write.
    // `#[cold]` keeps this out of `finish`, so that `finish` is still inlined.
    #[cold]
    #[inline]
    fn write_vint_size(buf: &mut Vec<u8>, starting_pos: usize) -> Result<(), CellOverflowError> {
        let contents_start = starting_pos + 1;
        let contents_end = buf.len();
        let value_len = contents_end - contents_start;
        i32::try_from(value_len).map_err(|_| CellOverflowError)?;

        // An unsigned vint takes at most 9 bytes.
        let mut size = [0; 9];
        let mut unused = &mut size[..];
        unsigned_vint_encode(value_len as u64, &mut unused);
        let unused_len = unused.len();
        let (first, rest) = size[..size.len() - unused_len].split_at(1);
        buf[starting_pos] = first[0];
        if !rest.is_empty() {
            // Grow the buffer, and move the contents back to make room for the
            // rest of the size. Not `Vec::splice`: like `push`, it hides from
            // the compiler that the buffer isn't kept.
            buf.extend_from_slice(rest);
            buf.copy_within(contents_start..contents_end, contents_start + rest.len());
            buf[contents_start..contents_start + rest.len()].copy_from_slice(rest);
        }
        Ok(())
    }
}

/// An object that indicates a type-level proof that something was written
/// by a [`CellWriter`] or [`CellValueBuilder`] with lifetime parameter `'buf`.
///
/// This type is returned by [`set_null`](CellWriter::set_null),
/// [`set_unset`](CellWriter::set_unset),
/// [`set_value`](CellWriter::set_value)
/// and also [`CellValueBuilder::finish`] - generally speaking, after
/// the value is fully initialized and the `CellWriter` is destroyed.
///
/// The purpose of this type is to enforce the contract of
/// [`SerializeValue::serialize`](super::value::SerializeValue::serialize): either
/// the method succeeds and returns a proof that it serialized itself
/// into the given value, or it fails and returns an error or panics.
#[derive(Debug)]
pub struct WrittenCellProof<'buf> {
    /// Using *mut &'buf () is deliberate and makes WrittenCellProof invariant
    /// on the 'buf lifetime parameter.
    /// Ref: <https://doc.rust-lang.org/reference/subtyping.html>
    _phantom: std::marker::PhantomData<*mut &'buf ()>,
}

impl WrittenCellProof<'_> {
    /// A shorthand for creating the proof.
    ///
    /// Do not make it public! It's important that only the row writer defined
    /// in this module is able to create a proof.
    #[inline]
    fn new() -> Self {
        WrittenCellProof {
            _phantom: std::marker::PhantomData,
        }
    }
}

/// There was an attempt to produce a CQL value over the maximum size limit (i32::MAX)
#[derive(Debug, Clone, Copy, Error)]
#[error("CQL cell overflowed the maximum allowed size of 2^31 - 1")]
pub struct CellOverflowError;

#[cfg(test)]
mod tests {
    use super::{CellWriter, RowWriter};

    #[test]
    fn test_cell_writer() {
        let mut data = Vec::new();
        let writer = CellWriter::new(&mut data);
        let mut sub_writer = writer.into_value_builder();
        sub_writer.make_sub_writer().set_null();
        sub_writer
            .make_sub_writer()
            .set_value(&[1, 2, 3, 4])
            .unwrap();
        sub_writer.make_sub_writer().set_unset();
        sub_writer.finish().unwrap();

        assert_eq!(
            data,
            [
                0, 0, 0, 16, // Length of inner data is 16
                255, 255, 255, 255, // Null (encoded as -1)
                0, 0, 0, 4, 1, 2, 3, 4, // Four byte value
                255, 255, 255, 254, // Unset (encoded as -2)
            ]
        );
    }

    #[test]
    fn test_poisoned_appender() {
        let mut data = Vec::new();
        let writer = CellWriter::new(&mut data);
        let _ = writer.into_value_builder();

        assert_eq!(
            data,
            [
                255, 255, 255, 253, // Invalid value
            ]
        );
    }

    #[test]
    fn test_sub_writer_with_vint_size() {
        let mut data = Vec::new();
        let mut builder = CellWriter::new(&mut data).into_value_builder();
        builder
            .make_sub_writer_with_vint_size()
            .set_value(&[1, 2, 3])
            .unwrap();
        builder
            .make_sub_writer_with_vint_size()
            .set_value(&[4; 200])
            .unwrap();
        let mut sub_builder = builder
            .make_sub_writer_with_vint_size()
            .into_value_builder();
        sub_builder.append_bytes(&[5, 6]);
        sub_builder.finish().unwrap();
        // The size of a value of at least 128 bytes doesn't fit in the byte
        // set aside for it.
        let mut sub_builder = builder
            .make_sub_writer_with_vint_size()
            .into_value_builder();
        sub_builder.append_bytes(&[7; 200]);
        sub_builder.finish().unwrap();
        builder.finish().unwrap();

        let mut expected = 411i32.to_be_bytes().to_vec(); // Length of inner data
        expected.extend_from_slice(&[3, 1, 2, 3]);
        expected.extend_from_slice(&[128, 200]);
        expected.extend_from_slice(&[4; 200]);
        expected.extend_from_slice(&[2, 5, 6]);
        expected.extend_from_slice(&[128, 200]);
        expected.extend_from_slice(&[7; 200]);
        assert_eq!(data, expected);
    }

    #[test]
    fn test_row_writer() {
        let mut data = Vec::new();
        let mut writer = RowWriter::new(&mut data);
        writer.make_cell_writer().set_null();
        writer.make_cell_writer().set_value(&[1, 2, 3, 4]).unwrap();
        writer.make_cell_writer().set_unset();

        assert_eq!(
            data,
            [
                255, 255, 255, 255, // Null (encoded as -1)
                0, 0, 0, 4, 1, 2, 3, 4, // Four byte value
                255, 255, 255, 254, // Unset (encoded as -2)
            ]
        )
    }
}
