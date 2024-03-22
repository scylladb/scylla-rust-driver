pub mod row;
pub mod value;

use bytes::Bytes;

use crate::frame::frame_errors::ParseError;
use crate::frame::types;

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

#[cfg(test)]
mod tests {
    use crate::frame::types;

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
}
