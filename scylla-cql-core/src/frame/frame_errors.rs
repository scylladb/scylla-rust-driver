//! Low-level errors that can occur during CQL frame parsing and serialization.

use std::sync::Arc;

use thiserror::Error;

use crate::frame::TryFromPrimitiveError;

/// A low level deserialization error.
///
/// This type of error is returned when deserialization
/// of some primitive value fails.
///
/// Possible error kinds:
/// - generic io error - reading from buffer failed
/// - out of range integer conversion
/// - conversion errors - e.g. slice-to-array or primitive-to-enum
/// - not enough bytes in the buffer to deserialize a value
#[non_exhaustive]
#[derive(Error, Debug, Clone)]
pub enum LowLevelDeserializationError {
    #[error(transparent)]
    IoError(Arc<std::io::Error>),
    #[error(transparent)]
    TryFromIntError(#[from] std::num::TryFromIntError),
    #[error(transparent)]
    TryFromSliceError(#[from] std::array::TryFromSliceError),
    #[error("Not enough bytes! expected: {expected}, received: {received}")]
    TooFewBytesReceived { expected: usize, received: usize },
    #[error("Invalid value length: {0}")]
    InvalidValueLength(i32),
    #[error("Unknown consistency: {0}")]
    UnknownConsistency(#[from] TryFromPrimitiveError<u16>),
    #[error("Invalid inet bytes length: {0}. Accepted lengths are 4 and 16 bytes.")]
    InvalidInetLength(u8),
    #[error("UTF8 deserialization failed: {0}")]
    UTF8DeserializationError(#[from] std::str::Utf8Error),
}

impl From<std::io::Error> for LowLevelDeserializationError {
    fn from(value: std::io::Error) -> Self {
        Self::IoError(Arc::new(value))
    }
}
