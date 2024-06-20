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
    #[error("Low-level serialization failed: {0}")]
    LowLevelSerializationError(#[from] LowLevelSerializationError),
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
    #[error(transparent)]
    SerializeValuesError(#[from] SerializeValuesError),
    #[error(transparent)]
    SerializationError(#[from] SerializationError),
    #[error(transparent)]
    CqlTypeError(#[from] CqlTypeError),
}

/// A low level serialization error.
///
/// This error is returned when the serialization
/// of some primitive value fails.
///
/// Possible error kinds:
/// - out of range integer conversion
#[non_exhaustive]
#[derive(Error, Debug)]
pub enum LowLevelSerializationError {
    #[error(transparent)]
    TryFromIntError(#[from] std::num::TryFromIntError),
}
