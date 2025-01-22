use bytes::Bytes;

use crate::frame::frame_errors::LowLevelDeserializationError;
use crate::frame::types;

/// A reference to a part of the frame.
//
// # Design justification
//
// ## Why we need a borrowed type
//
// The reason why we need to store a borrowed slice is that we want a lifetime that is longer than one obtained
// when coercing Bytes to a slice in the body of a function. That is, we want to allow deserializing types
// that borrow from the frame, which resides in QueryResult.
// Consider a function with the signature:
//
// fn fun(b: Bytes) { ... }
//
// This function cannot return a type that borrows from the frame, because any slice created from `b`
// inside `fun` cannot escape `fun`.
// Conversely, if a function has signature:
//
// fn fun(s: &'frame [u8]) { ... }
//
// then it can happily return types with lifetime 'frame.
//
// ## Why we need the full frame
//
// We don't. We only need to be able to return Bytes encompassing our subslice. However, the design choice
// was made to only store a reference to the original Bytes object residing in QueryResult, so that we avoid
// cloning Bytes when performing subslicing on FrameSlice. We delay the Bytes cloning, normally a moderately
// expensive operation involving cloning an Arc, up until it is really needed.
//
// ## Why not different design
//
//      - why not a &'frame [u8] only? Because we want to enable deserializing types containing owned Bytes, too.
//      - why not a Bytes only? Because we need to propagate the 'frame lifetime.
//      - why not a &'frame Bytes only? Because we want to somehow represent subslices, and subslicing
//        &'frame Bytes return Bytes, not &'frame Bytes.
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

    /// Creates an empty FrameSlice.
    #[inline]
    pub fn new_empty() -> Self {
        Self {
            frame_subslice: &EMPTY_BYTES,
            original_frame: &EMPTY_BYTES,
        }
    }

    /// Creates a new FrameSlice from a reference to a slice.
    ///
    /// This method creates a not-fully-valid FrameSlice that does not hold
    /// the valid original frame Bytes. Thus, it is intended to be used in
    /// legacy code that does not operate on Bytes, but rather on borrowed slice only.
    /// For correctness in an unlikely case that someone calls `to_bytes()` on such
    /// a deficient slice, a special treatment is added there that copies
    /// the slice into a new-allocation-based Bytes.
    /// This is pub(crate) for the above reason.
    #[inline]
    pub(crate) fn new_borrowed(frame_subslice: &'frame [u8]) -> Self {
        Self {
            frame_subslice,
            original_frame: &EMPTY_BYTES,
        }
    }

    /// Returns `true` if the slice has length of 0.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.frame_subslice.is_empty()
    }

    /// Returns the subslice.
    #[inline]
    pub fn as_slice(&self) -> &'frame [u8] {
        self.frame_subslice
    }

    /// Returns a mutable reference to the subslice.
    #[inline]
    pub fn as_slice_mut(&mut self) -> &mut &'frame [u8] {
        &mut self.frame_subslice
    }

    /// Returns a reference to the Bytes object which encompasses the whole frame slice.
    ///
    /// The Bytes object will usually be larger than the slice returned by
    /// [FrameSlice::as_slice]. If you wish to obtain a new Bytes object that
    /// points only to the subslice represented by the FrameSlice object,
    /// see [FrameSlice::to_bytes].
    #[inline]
    pub fn as_original_frame_bytes(&self) -> &'frame Bytes {
        self.original_frame
    }

    /// Returns a new Bytes object which is a subslice of the original Bytes
    /// frame slice object.
    #[inline]
    pub fn to_bytes(&self) -> Bytes {
        if self.original_frame.is_empty() {
            // For the borrowed, deficient version of FrameSlice - the one created with
            // FrameSlice::new_borrowed to work properly in case someone calls
            // FrameSlice::to_bytes on it (even though it's not intended for the borrowed version),
            // the special case is introduced that creates new Bytes by copying the slice into
            // a new allocation. Note that it's something unexpected to be ever done.
            return Bytes::copy_from_slice(self.as_slice());
        }

        self.original_frame.slice_ref(self.frame_subslice)
    }

    /// Reads and consumes a `[bytes]` item from the beginning of the frame,
    /// returning a subslice that encompasses that item.
    ///
    /// If the operation fails then the slice remains unchanged.
    #[inline]
    pub fn read_cql_bytes(
        &mut self,
    ) -> Result<Option<FrameSlice<'frame>>, LowLevelDeserializationError> {
        // We copy the slice reference, not to mutate the FrameSlice in case of an error.
        let mut slice = self.frame_subslice;

        let cql_bytes = types::read_bytes_opt(&mut slice)?;

        // `read_bytes_opt` hasn't failed, so now we must update the FrameSlice.
        self.frame_subslice = slice;

        Ok(cql_bytes.map(|slice| Self {
            frame_subslice: slice,
            original_frame: self.original_frame,
        }))
    }

    /// Reads and consumes a fixed number of bytes item from the beginning of the frame,
    /// returning a subslice that encompasses that item.
    ///
    /// If this slice is empty, returns `Ok(None)`.
    /// Otherwise, if the slice does not contain enough data, it returns `Err`.
    /// If the operation fails then the slice remains unchanged.
    #[inline]
    pub fn read_n_bytes(
        &mut self,
        count: usize,
    ) -> Result<Option<FrameSlice<'frame>>, LowLevelDeserializationError> {
        if self.is_empty() {
            return Ok(None);
        }

        // We copy the slice reference, not to mutate the FrameSlice in case of an error.
        let mut slice = self.frame_subslice;

        let cql_bytes = types::read_raw_bytes(count, &mut slice)?;

        // `read_raw_bytes` hasn't failed, so now we must update the FrameSlice.
        self.frame_subslice = slice;

        Ok(Some(Self {
            frame_subslice: cql_bytes,
            original_frame: self.original_frame,
        }))
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use super::super::tests::{serialize_cells, CELL1, CELL2};
    use super::FrameSlice;

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
            subslice1.as_original_frame_bytes() as *const Bytes,
            &frame as *const Bytes
        );
        assert_eq!(
            subslice2.as_original_frame_bytes() as *const Bytes,
            &frame as *const Bytes
        );

        let subslice1_bytes = subslice1.to_bytes();
        let subslice2_bytes = subslice2.to_bytes();

        assert_eq!(subslice1.as_slice(), subslice1_bytes.as_ref());
        assert_eq!(subslice2.as_slice(), subslice2_bytes.as_ref());
    }
}
