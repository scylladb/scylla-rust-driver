//! Contains types and traits used for safe serialization of values for a CQL statement.

// Note: When editing above doc-comment edit the corresponding comment on
// re-export module in scylla crate too.

use thiserror::Error;

use crate::frame::types;

use super::row::SerializedValues;

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
    cell_len: Option<usize>,
    size_as_uvarint: bool,
}

impl<'buf> CellWriter<'buf> {
    /// Creates a new cell writer based on an existing Vec.
    ///
    /// The newly created row writer will append data to the end of the vec.
    #[inline]
    pub fn new(buf: &'buf mut Vec<u8>) -> Self {
        Self {
            buf,
            cell_len: None,
            size_as_uvarint: false,
        }
    }
    /// Creates a new cell writer based on an existing Vec, for fixed-length cells.
    /// This cell writer will serialize each cell directly, without prepending it
    /// with cell length.  
    ///
    /// The newly created row writer will append data to the end of the vec
    #[inline]
    pub fn with_cell_len(
        buf: &'buf mut Vec<u8>,
        cell_len: Option<usize>,
        size_as_uvarint: bool,
    ) -> Self {
        Self {
            buf,
            cell_len,
            size_as_uvarint,
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
        match self.cell_len {
            Some(cell_len) => assert_eq!(cell_len, contents.len()),
            None => {
                if !self.size_as_uvarint {
                    self.buf.extend_from_slice(&value_len.to_be_bytes())
                } else {
                    let mut len = Vec::new();
                    types::unsigned_vint_encode(value_len.try_into().unwrap(), &mut len);
                    self.buf.extend_from_slice(&len);
                }
            }
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
        CellValueBuilder::new(self.buf, self.size_as_uvarint)
    }

    /// Turns this writer into a [`CellValueBuilder`] which can be used
    /// to gradually initialize the CQL value of CQL vector type.
    ///
    /// The length of the value is fixed and known upfront.
    #[inline]
    pub fn into_fixed_len_value_builder(self, len: usize) -> CellValueBuilder<'buf> {
        CellValueBuilder::fixed_len(self.buf, len, self.size_as_uvarint)
    }

    /// Turns this writer into a [`CellValueBuilder`] which can be used
    /// to gradually initialize the CQL value of CQL vector type.
    ///
    ///
    #[inline]
    pub fn into_variable_len_value_builder(self) -> CellValueBuilder<'buf> {
        CellValueBuilder::variable_len(self.buf, self.size_as_uvarint)
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

    // If writing to a fixed length type vector, the type length.
    cell_len: Option<usize>,

    //If serializing a variable length vector cell, the size is encoded as a varint.
    is_variable_length: bool,

    // Buffer for variable length vector cell.
    variable_length_buffer: Option<Vec<u8>>,
}

impl<'buf> CellValueBuilder<'buf> {
    #[inline]
    fn new(buf: &'buf mut Vec<u8>, size_as_uvar_int: bool) -> Self {
        // "Length" of a [bytes] frame can either be a non-negative i32,
        // -1 (null) or -2 (not set). Push an invalid value here. It will be
        // overwritten eventually either by set_null, set_unset or Drop.
        // If the CellSerializer is not dropped as it should, this will trigger
        // an error on the DB side and the serialized data
        // won't be misinterpreted.
        let starting_pos = buf.len();
        if !size_as_uvar_int {
            buf.extend_from_slice(&(-3i32).to_be_bytes());
        }

        Self {
            buf,
            starting_pos,
            cell_len: None,
            is_variable_length: false,
            variable_length_buffer: if size_as_uvar_int {
                Some(Vec::new())
            } else {
                None
            },
        }
    }

    #[inline]
    fn fixed_len(buf: &'buf mut Vec<u8>, cell_len: usize, size_as_uvar_int: bool) -> Self {
        // "Length" of a [bytes] frame can either be a non-negative i32,
        // -1 (null) or -2 (not set). Push an invalid value here. It will be
        // overwritten eventually either by set_null, set_unset or Drop.
        // If the CellSerializer is not dropped as it should, this will trigger
        // an error on the DB side and the serialized data
        // won't be misinterpreted.
        let starting_pos = buf.len();
        if !size_as_uvar_int {
            buf.extend_from_slice(&(-3i32).to_be_bytes());
        }
        Self {
            buf,
            starting_pos,
            cell_len: Some(cell_len),
            is_variable_length: false,
            variable_length_buffer: if size_as_uvar_int {
                Some(Vec::new())
            } else {
                None
            },
        }
    }

    #[inline]
    fn variable_len(buf: &'buf mut Vec<u8>, size_as_uvar_int: bool) -> Self {
        let starting_pos = buf.len();
        if !size_as_uvar_int {
            buf.extend_from_slice(&(-3i32).to_be_bytes());
        }
        Self {
            buf,
            starting_pos,
            cell_len: None,
            is_variable_length: true,
            variable_length_buffer: if size_as_uvar_int {
                Some(Vec::new())
            } else {
                None
            },
        }
    }

    /// Appends raw bytes to this cell.
    #[inline]
    pub fn append_bytes(&mut self, bytes: &[u8]) {
        if let Some(buffer) = &mut self.variable_length_buffer {
            buffer.extend_from_slice(bytes);
        } else {
            self.buf.extend_from_slice(bytes);
        }
    }

    /// Appends a sub-value to the end of the current contents of the cell
    /// and returns an object that allows to fill it in.
    #[inline]
    pub fn make_sub_writer(&mut self) -> CellWriter<'_> {
        if let Some(buffer) = &mut self.variable_length_buffer {
            CellWriter::with_cell_len(buffer, self.cell_len, self.is_variable_length)
        } else {
            CellWriter::with_cell_len(self.buf, self.cell_len, self.is_variable_length)
        }
    }

    /// Finishes serializing the value.
    ///
    /// Fails if the constructed cell size overflows the maximum allowed
    /// CQL cell size (which is i32::MAX).
    #[inline]
    pub fn finish(self) -> Result<WrittenCellProof<'buf>, CellOverflowError> {
        if let Some(buffer) = self.variable_length_buffer {
            let value_len = buffer.len();
            let mut len = Vec::new();
            types::unsigned_vint_encode(value_len as u64, &mut len);
            self.buf.extend_from_slice(&len);
            self.buf.extend_from_slice(&buffer);
        } else {
            let value_len: i32 = (self.buf.len() - self.starting_pos - 4)
                .try_into()
                .map_err(|_| CellOverflowError)?;
            self.buf[self.starting_pos..self.starting_pos + 4]
                .copy_from_slice(&value_len.to_be_bytes());
        }
        Ok(WrittenCellProof::new())
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
