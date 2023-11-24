//! Contains types and traits used for safe serialization of values for a CQL statement.

/// An interface that facilitates writing values for a CQL query.
pub trait RowWriter {
    type CellWriter<'a>: CellWriter
    where
        Self: 'a;

    /// Appends a new value to the sequence and returns an object that allows
    /// to fill it in.
    fn make_cell_writer(&mut self) -> Self::CellWriter<'_>;
}

/// Represents a handle to a CQL value that needs to be written into.
///
/// The writer can either be transformed into a ready value right away
/// (via [`set_null`](CellWriter::set_null),
/// [`set_unset`](CellWriter::set_unset)
/// or [`set_value`](CellWriter::set_value) or transformed into
/// the [`CellWriter::ValueBuilder`] in order to gradually initialize
/// the value when the contents are not available straight away.
///
/// After the value is fully initialized, the handle is consumed and
/// a [`WrittenCellProof`](CellWriter::WrittenCellProof) object is returned
/// in its stead. This is a type-level proof that the value was fully initialized
/// and is used in [`SerializeCql::serialize`](`super::value::SerializeCql::serialize`)
/// in order to enforce the implementor to fully initialize the provided handle
/// to CQL value.
///
/// Dropping this type without calling any of its methods will result
/// in nothing being written.
pub trait CellWriter {
    /// The type of the value builder, returned by the [`CellWriter::set_value`]
    /// method.
    type ValueBuilder: CellValueBuilder<WrittenCellProof = Self::WrittenCellProof>;

    /// An object that serves as a proof that the cell was fully initialized.
    ///
    /// This type is returned by [`set_null`](CellWriter::set_null),
    /// [`set_unset`](CellWriter::set_unset),
    /// [`set_value`](CellWriter::set_value)
    /// and also [`CellValueBuilder::finish`] - generally speaking, after
    /// the value is fully initialized and the `CellWriter` is destroyed.
    ///
    /// The purpose of this type is to enforce the contract of
    /// [`SerializeCql::serialize`](super::value::SerializeCql::serialize): either
    /// the method succeeds and returns a proof that it serialized itself
    /// into the given value, or it fails and returns an error or panics.
    /// The exact type of [`WrittenCellProof`](CellWriter::WrittenCellProof)
    /// is not important as the value is not used at all - it's only
    /// a compile-time check.
    type WrittenCellProof;

    /// Sets this value to be null, consuming this object.
    fn set_null(self) -> Self::WrittenCellProof;

    /// Sets this value to represent an unset value, consuming this object.
    fn set_unset(self) -> Self::WrittenCellProof;

    /// Sets this value to a non-zero, non-unset value with given contents.
    ///
    /// Prefer this to [`into_value_builder`](CellWriter::into_value_builder)
    /// if you have all of the contents of the value ready up front (e.g. for
    /// fixed size types).
    fn set_value(self, contents: &[u8]) -> Self::WrittenCellProof;

    /// Turns this writter into a [`CellValueBuilder`] which can be used
    /// to gradually initialize the CQL value.
    ///
    /// This method should be used if you don't have all of the data
    /// up front, e.g. when serializing compound types such as collections
    /// or UDTs.
    fn into_value_builder(self) -> Self::ValueBuilder;
}

/// Allows appending bytes to a non-null, non-unset cell.
///
/// This object needs to be dropped in order for the value to be correctly
/// serialized. Failing to drop this value will result in a payload that will
/// not be parsed by the database correctly, but otherwise should not cause
/// data to be misinterpreted.
pub trait CellValueBuilder {
    type SubCellWriter<'a>: CellWriter
    where
        Self: 'a;

    type WrittenCellProof;

    /// Appends raw bytes to this cell.
    fn append_bytes(&mut self, bytes: &[u8]);

    /// Appends a sub-value to the end of the current contents of the cell
    /// and returns an object that allows to fill it in.
    fn make_sub_writer(&mut self) -> Self::SubCellWriter<'_>;

    /// Finishes serializing the value.
    fn finish(self) -> Self::WrittenCellProof;
}

/// A row writer backed by a buffer (vec).
pub struct BufBackedRowWriter<'buf> {
    // Buffer that this value should be serialized to.
    buf: &'buf mut Vec<u8>,

    // Number of values written so far.
    value_count: u16,
}

impl<'buf> BufBackedRowWriter<'buf> {
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
    #[inline]
    pub fn value_count(&self) -> u16 {
        self.value_count
    }
}

impl<'buf> RowWriter for BufBackedRowWriter<'buf> {
    type CellWriter<'a> = BufBackedCellWriter<'a> where Self: 'a;

    #[inline]
    fn make_cell_writer(&mut self) -> Self::CellWriter<'_> {
        self.value_count = self
            .value_count
            .checked_add(1)
            .expect("tried to serialize too many values for a query (more than u16::MAX)");
        BufBackedCellWriter::new(self.buf)
    }
}

/// A cell writer backed by a buffer (vec).
pub struct BufBackedCellWriter<'buf> {
    buf: &'buf mut Vec<u8>,
}

impl<'buf> BufBackedCellWriter<'buf> {
    /// Creates a new cell writer based on an existing Vec.
    ///
    /// The newly created row writer will append data to the end of the vec.
    #[inline]
    pub fn new(buf: &'buf mut Vec<u8>) -> Self {
        BufBackedCellWriter { buf }
    }
}

impl<'buf> CellWriter for BufBackedCellWriter<'buf> {
    type ValueBuilder = BufBackedCellValueBuilder<'buf>;

    type WrittenCellProof = ();

    #[inline]
    fn set_null(self) {
        self.buf.extend_from_slice(&(-1i32).to_be_bytes());
    }

    #[inline]
    fn set_unset(self) {
        self.buf.extend_from_slice(&(-2i32).to_be_bytes());
    }

    #[inline]
    fn set_value(self, bytes: &[u8]) {
        let value_len: i32 = bytes
            .len()
            .try_into()
            .expect("value is too big to fit into a CQL [bytes] object (larger than i32::MAX)");
        self.buf.extend_from_slice(&value_len.to_be_bytes());
        self.buf.extend_from_slice(bytes);
    }

    #[inline]
    fn into_value_builder(self) -> Self::ValueBuilder {
        BufBackedCellValueBuilder::new(self.buf)
    }
}

/// A cell value builder backed by a buffer (vec).
pub struct BufBackedCellValueBuilder<'buf> {
    // Buffer that this value should be serialized to.
    buf: &'buf mut Vec<u8>,

    // Starting position of the value in the buffer.
    starting_pos: usize,
}

impl<'buf> BufBackedCellValueBuilder<'buf> {
    #[inline]
    fn new(buf: &'buf mut Vec<u8>) -> Self {
        // "Length" of a [bytes] frame can either be a non-negative i32,
        // -1 (null) or -1 (not set). Push an invalid value here. It will be
        // overwritten eventually either by set_null, set_unset or Drop.
        // If the CellSerializer is not dropped as it should, this will trigger
        // an error on the DB side and the serialized data
        // won't be misinterpreted.
        let starting_pos = buf.len();
        buf.extend_from_slice(&(-3i32).to_be_bytes());
        BufBackedCellValueBuilder { buf, starting_pos }
    }
}

impl<'buf> CellValueBuilder for BufBackedCellValueBuilder<'buf> {
    type SubCellWriter<'a> = BufBackedCellWriter<'a>
    where
        Self: 'a;

    type WrittenCellProof = ();

    #[inline]
    fn append_bytes(&mut self, bytes: &[u8]) {
        self.buf.extend_from_slice(bytes);
    }

    #[inline]
    fn make_sub_writer(&mut self) -> Self::SubCellWriter<'_> {
        BufBackedCellWriter::new(self.buf)
    }

    #[inline]
    fn finish(self) {
        // TODO: Should this panic, or should we catch this error earlier?
        // Vec<u8> will panic anyway if we overflow isize, so at least this
        // behavior is consistent with what the stdlib does.
        let value_len: i32 = (self.buf.len() - self.starting_pos - 4)
            .try_into()
            .expect("value is too big to fit into a CQL [bytes] object (larger than i32::MAX)");
        self.buf[self.starting_pos..self.starting_pos + 4]
            .copy_from_slice(&value_len.to_be_bytes());
    }
}

/// A writer that does not actually write anything, just counts the bytes.
///
/// It can serve as a:
///
/// - [`RowWriter`]
/// - [`CellWriter`]
/// - [`CellValueBuilder`]
pub struct CountingWriter<'buf> {
    buf: &'buf mut usize,
}

impl<'buf> CountingWriter<'buf> {
    /// Creates a new writer which increments the counter under given reference
    /// when bytes are appended.
    #[inline]
    fn new(buf: &'buf mut usize) -> Self {
        CountingWriter { buf }
    }
}

impl<'buf> RowWriter for CountingWriter<'buf> {
    type CellWriter<'a> = CountingWriter<'a> where Self: 'a;

    #[inline]
    fn make_cell_writer(&mut self) -> Self::CellWriter<'_> {
        CountingWriter::new(self.buf)
    }
}

impl<'buf> CellWriter for CountingWriter<'buf> {
    type ValueBuilder = CountingWriter<'buf>;

    type WrittenCellProof = ();

    #[inline]
    fn set_null(self) {
        *self.buf += 4;
    }

    #[inline]
    fn set_unset(self) {
        *self.buf += 4;
    }

    #[inline]
    fn set_value(self, contents: &[u8]) {
        *self.buf += 4 + contents.len();
    }

    #[inline]
    fn into_value_builder(self) -> Self::ValueBuilder {
        *self.buf += 4;
        CountingWriter::new(self.buf)
    }
}

impl<'buf> CellValueBuilder for CountingWriter<'buf> {
    type SubCellWriter<'a> = CountingWriter<'a>
    where
        Self: 'a;

    type WrittenCellProof = ();

    #[inline]
    fn append_bytes(&mut self, bytes: &[u8]) {
        *self.buf += bytes.len();
    }

    #[inline]
    fn make_sub_writer(&mut self) -> Self::SubCellWriter<'_> {
        CountingWriter::new(self.buf)
    }

    #[inline]
    fn finish(self) -> Self::WrittenCellProof {}
}

#[cfg(test)]
mod tests {
    use super::{
        BufBackedCellWriter, BufBackedRowWriter, CellValueBuilder, CellWriter, CountingWriter,
        RowWriter,
    };

    // We want to perform the same computation for both buf backed writer
    // and counting writer, but Rust does not support generic closures.
    // This trait comes to the rescue.
    trait CellSerializeCheck {
        fn check<W: CellWriter>(&self, writer: W);
    }

    fn check_cell_serialize<C: CellSerializeCheck>(c: C) -> Vec<u8> {
        let mut data = Vec::new();
        let writer = BufBackedCellWriter::new(&mut data);
        c.check(writer);

        let mut byte_count = 0usize;
        let counting_writer = CountingWriter::new(&mut byte_count);
        c.check(counting_writer);

        assert_eq!(data.len(), byte_count);
        data
    }

    #[test]
    fn test_cell_writer() {
        struct Check;
        impl CellSerializeCheck for Check {
            fn check<W: CellWriter>(&self, writer: W) {
                let mut sub_writer = writer.into_value_builder();
                sub_writer.make_sub_writer().set_null();
                sub_writer.make_sub_writer().set_value(&[1, 2, 3, 4]);
                sub_writer.make_sub_writer().set_unset();
                sub_writer.finish();
            }
        }

        let data = check_cell_serialize(Check);
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
        struct Check;
        impl CellSerializeCheck for Check {
            fn check<W: CellWriter>(&self, writer: W) {
                let _ = writer.into_value_builder();
            }
        }

        let data = check_cell_serialize(Check);
        assert_eq!(
            data,
            [
                255, 255, 255, 253, // Invalid value
            ]
        );
    }

    trait RowSerializeCheck {
        fn check<W: RowWriter>(&self, writer: &mut W);
    }

    fn check_row_serialize<C: RowSerializeCheck>(c: C) -> Vec<u8> {
        let mut data = Vec::new();
        let mut writer = BufBackedRowWriter::new(&mut data);
        c.check(&mut writer);

        let mut byte_count = 0usize;
        let mut counting_writer = CountingWriter::new(&mut byte_count);
        c.check(&mut counting_writer);

        assert_eq!(data.len(), byte_count);
        data
    }

    #[test]
    fn test_row_writer() {
        struct Check;
        impl RowSerializeCheck for Check {
            fn check<W: RowWriter>(&self, writer: &mut W) {
                writer.make_cell_writer().set_null();
                writer.make_cell_writer().set_value(&[1, 2, 3, 4]);
                writer.make_cell_writer().set_unset();
            }
        }

        let data = check_row_serialize(Check);
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
