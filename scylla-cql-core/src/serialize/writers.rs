//! Contains types and traits used for safe serialization of values for a CQL statement.

// Note: When editing above doc-comment edit the corresponding comment on
// re-export module in scylla crate too.

use thiserror::Error;

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
    write_size: bool,
}

impl<'buf> CellWriter<'buf> {
    /// Creates a new cell writer based on an existing Vec.
    ///
    /// The newly created row writer will append data to the end of the vec.
    #[inline]
    pub fn new(buf: &'buf mut Vec<u8>) -> Self {
        Self {
            buf,
            write_size: true,
        }
    }

    /// Creates a new cell writer based on an existing Vec, without writing size.
    ///
    /// The newly created row writer will append data to the end of the vec.
    ///
    /// # Correctness
    ///
    /// A size-less cell carries no length header, so the reader recovers the
    /// value boundaries from the CQL type alone. That makes null and unset
    /// unrepresentable, and it makes a payload of the wrong length unreadable.
    /// Neither condition can be reported through
    /// [`set_null`](CellWriter::set_null) or
    /// [`set_unset`](CellWriter::set_unset), which are infallible and consume
    /// the writer.
    ///
    /// So that the caller can still detect both, a size-less writer writes
    /// nothing at all for null and unset instead of the -1 and -2 sentinels a
    /// normal writer would emit. A caller that knows how many bytes the element
    /// type requires, and that number is never zero, can then reject null,
    /// unset and a wrong-length payload with a single length comparison. See
    /// `serialize_next_constant_length_elem` in the `value` module.
    #[inline]
    pub fn new_without_size(buf: &'buf mut Vec<u8>) -> Self {
        Self {
            buf,
            write_size: false,
        }
    }

    /// Sets this value to be null, consuming this object.
    ///
    /// A size-less writer, see [`new_without_size`](CellWriter::new_without_size),
    /// has no encoding for null and writes nothing instead of the -1 sentinel.
    #[inline]
    pub fn set_null(self) -> WrittenCellProof<'buf> {
        if self.write_size {
            self.buf.extend_from_slice(&(-1i32).to_be_bytes());
        }
        WrittenCellProof::new()
    }

    /// Sets this value to represent an unset value, consuming this object.
    ///
    /// A size-less writer, see [`new_without_size`](CellWriter::new_without_size),
    /// has no encoding for unset and writes nothing instead of the -2 sentinel.
    #[inline]
    pub fn set_unset(self) -> WrittenCellProof<'buf> {
        if self.write_size {
            self.buf.extend_from_slice(&(-2i32).to_be_bytes());
        }
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
        if self.write_size {
            self.buf.extend_from_slice(&value_len.to_be_bytes());
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
        CellValueBuilder::new(self.buf, self.write_size)
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

    // Should we write the size of the value?
    write_size: bool,
}

impl<'buf> CellValueBuilder<'buf> {
    #[inline]
    fn new(buf: &'buf mut Vec<u8>, write_size: bool) -> Self {
        // "Length" of a [bytes] frame can either be a non-negative i32,
        // -1 (null) or -2 (not set). Push an invalid value here. It will be
        // overwritten eventually either by set_null, set_unset or Drop.
        // If the CellSerializer is not dropped as it should, this will trigger
        // an error on the DB side and the serialized data
        // won't be misinterpreted.
        let starting_pos = buf.len();
        if write_size {
            buf.extend_from_slice(&(-3i32).to_be_bytes());
        }
        Self {
            buf,
            starting_pos,
            write_size,
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
    ///
    /// See [`CellWriter::new_without_size`] for the correctness obligations
    /// that come with a size-less writer.
    #[inline]
    pub fn make_sub_writer_without_size(&mut self) -> CellWriter<'_> {
        CellWriter::new_without_size(self.buf)
    }

    /// Number of content bytes written into this cell so far, not counting the
    /// length header of the cell itself.
    ///
    /// Bracketing a [`make_sub_writer_without_size`](CellValueBuilder::make_sub_writer_without_size)
    /// call with this is how a caller recovers the length of a size-less
    /// sub-value, which is the only thing that distinguishes a valid payload
    /// from the null and unset that a size-less writer cannot encode.
    #[inline]
    pub(crate) fn content_len(&self) -> usize {
        self.buf.len() - self.starting_pos - if self.write_size { 4 } else { 0 }
    }

    /// Finishes serializing the value.
    ///
    /// Fails if the constructed cell size overflows the maximum allowed
    /// CQL cell size (which is i32::MAX).
    #[inline]
    pub fn finish(self) -> Result<WrittenCellProof<'buf>, CellOverflowError> {
        if self.write_size {
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

/// Why a cell written by a normal [`CellWriter`] has no size-less form.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SizeLessCellError {
    /// The value serialized to null, encoded as a length of -1.
    Null,
    /// The value serialized to unset, encoded as a length of -2.
    Unset,
    /// The header is neither a valid length nor a null/unset sentinel. In
    /// practice this means a [`CellValueBuilder`] was leaked without calling
    /// [`CellValueBuilder::finish`], leaving the -3 poison behind.
    Malformed,
}

/// Splits a cell written by a normal [`CellWriter`] into header and payload,
/// returning the payload only if the cell has a size-less form.
///
/// Used where a length comparison cannot decide the question on its own,
/// namely for a variable-length vector element, whose payload may legitimately
/// be of any length including zero. The header settles it: null and unset are
/// visible as -1 and -2 rather than as an empty payload.
#[inline]
pub(crate) fn cell_payload(cell: &[u8]) -> Result<&[u8], SizeLessCellError> {
    let (header, payload) = cell
        .split_at_checked(4)
        .ok_or(SizeLessCellError::Malformed)?;
    let header = i32::from_be_bytes(
        header
            .try_into()
            .expect("split_at_checked(4) yields 4 bytes"),
    );
    match header {
        -1 => Err(SizeLessCellError::Null),
        -2 => Err(SizeLessCellError::Unset),
        len if len < 0 => Err(SizeLessCellError::Malformed),
        // A successful write always leaves a header that matches the payload,
        // both for `set_value` and for `CellValueBuilder::finish`. Checking it
        // anyway keeps a malformed cell from being re-encoded as a valid one.
        len if u64::from(len.cast_unsigned()) != payload.len() as u64 => {
            Err(SizeLessCellError::Malformed)
        }
        _ => Ok(payload),
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
    fn size_less_writer_does_not_emit_null_or_unset_sentinels() {
        // Regression test for https://github.com/scylladb/scylla-rust-driver/issues/1669.
        // A size-less cell has no length header, so the -1 and -2 sentinels
        // would land in the buffer as payload. Writing nothing instead leaves
        // a length that no fixed-size element type can match.
        let mut data = Vec::new();
        CellWriter::new_without_size(&mut data).set_null();
        assert_eq!(data, [] as [u8; 0]);

        let mut data = Vec::new();
        CellWriter::new_without_size(&mut data).set_unset();
        assert_eq!(data, [] as [u8; 0]);

        // A normal writer is unchanged.
        let mut data = Vec::new();
        CellWriter::new(&mut data).set_null();
        assert_eq!(data, [255, 255, 255, 255]);

        let mut data = Vec::new();
        CellWriter::new(&mut data).set_unset();
        assert_eq!(data, [255, 255, 255, 254]);
    }

    #[test]
    fn size_less_sub_writer_content_len_reports_what_was_written() {
        let mut data = Vec::new();
        let mut builder = CellWriter::new(&mut data).into_value_builder();

        let before = builder.content_len();
        builder
            .make_sub_writer_without_size()
            .set_value(&[1, 2, 3, 4])
            .unwrap();
        assert_eq!(builder.content_len() - before, 4);

        let before = builder.content_len();
        builder.make_sub_writer_without_size().set_null();
        assert_eq!(builder.content_len() - before, 0);

        let before = builder.content_len();
        builder.make_sub_writer_without_size().set_unset();
        assert_eq!(builder.content_len() - before, 0);

        builder.finish().unwrap();
        assert_eq!(data, [0, 0, 0, 4, 1, 2, 3, 4]);
    }

    #[test]
    fn cell_payload_classifies_written_cells() {
        use super::{SizeLessCellError, cell_payload};

        let mut data = Vec::new();
        CellWriter::new(&mut data).set_value(&[7, 7, 7]).unwrap();
        assert_eq!(cell_payload(&data), Ok(&[7, 7, 7][..]));

        let mut data = Vec::new();
        CellWriter::new(&mut data).set_value(&[]).unwrap();
        assert_eq!(cell_payload(&data), Ok(&[][..]));

        let mut data = Vec::new();
        CellWriter::new(&mut data).set_null();
        assert_eq!(cell_payload(&data), Err(SizeLessCellError::Null));

        let mut data = Vec::new();
        CellWriter::new(&mut data).set_unset();
        assert_eq!(cell_payload(&data), Err(SizeLessCellError::Unset));

        // Builder leaked without `finish`, leaving the -3 poison.
        let mut data = Vec::new();
        let _ = CellWriter::new(&mut data).into_value_builder();
        assert_eq!(cell_payload(&data), Err(SizeLessCellError::Malformed));

        assert_eq!(cell_payload(&[0, 0, 0]), Err(SizeLessCellError::Malformed));
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
