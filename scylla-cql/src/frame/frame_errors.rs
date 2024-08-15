use super::TryFromPrimitiveError;
use crate::cql_to_rust::CqlTypeError;
use crate::frame::value::SerializeValuesError;
use crate::types::deserialize::DeserializationError;
use crate::types::serialize::SerializationError;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum FrameError {
    #[error(transparent)]
    Parse(#[from] ParseError),
    #[error("Frame is compressed, but no compression negotiated for connection.")]
    NoCompressionNegotiated,
    #[error("Received frame marked as coming from a client")]
    FrameFromClient,
    #[error("Received frame marked as coming from the server")]
    FrameFromServer,
    #[error("Received a frame from version {0}, but only 4 is supported")]
    VersionNotSupported(u8),
    #[error("Connection was closed before body was read: missing {0} out of {1}")]
    ConnectionClosed(usize, usize),
    #[error("Frame decompression failed.")]
    FrameDecompression,
    #[error("Frame compression failed.")]
    FrameCompression,
    #[error(transparent)]
    StdIoError(#[from] std::io::Error),
    #[error("Unrecognized opcode{0}")]
    TryFromPrimitiveError(#[from] TryFromPrimitiveError<u8>),
    #[error("Error compressing lz4 data {0}")]
    Lz4CompressError(#[from] lz4_flex::block::CompressError),
    #[error("Error decompressing lz4 data {0}")]
    Lz4DecompressError(#[from] lz4_flex::block::DecompressError),
}

#[derive(Error, Debug)]
pub enum ParseError {
    #[error("Low-level deserialization failed: {0}")]
    LowLevelDeserializationError(#[from] LowLevelDeserializationError),
    #[error("Could not serialize frame: {0}")]
    BadDataToSerialize(String),
    #[error("Could not deserialize frame: {0}")]
    BadIncomingData(String),
    #[error(transparent)]
    DeserializationError(#[from] DeserializationError),
    #[error(transparent)]
    IoError(#[from] std::io::Error),
    #[error("type not yet implemented, id: {0}")]
    TypeNotImplemented(u16),
    #[error("invalid custom type: {0}")]
    InvalidCustomType(String),
    #[error(transparent)]
    SerializeValuesError(#[from] SerializeValuesError),
    #[error(transparent)]
    SerializationError(#[from] SerializationError),
    #[error(transparent)]
    CqlTypeError(#[from] CqlTypeError),
}

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
#[derive(Error, Debug)]
pub enum LowLevelDeserializationError {
    #[error(transparent)]
    IoError(#[from] std::io::Error),
    #[error(transparent)]
    TryFromIntError(#[from] std::num::TryFromIntError),
    #[error("Failed to convert slice into array: {0}")]
    TryFromSliceError(#[from] std::array::TryFromSliceError),
    #[error("Not enough bytes! expected: {expected}, received: {received}")]
    TooFewBytesReceived { expected: usize, received: usize },
    #[error("Invalid value length: {0}")]
    InvalidValueLength(i32),
    #[error("Unknown consistency: {0}")]
    UnknownConsistency(#[from] TryFromPrimitiveError<u16>),
    #[error("Invalid inet bytes length: {0}")]
    InvalidInetLength(u8),
    #[error("UTF8 deserialization failed: {0}")]
    UTF8DeserializationError(#[from] std::str::Utf8Error),
}
