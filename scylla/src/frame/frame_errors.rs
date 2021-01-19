use super::response;
use crate::cql_to_rust::CQLTypeError;
use crate::frame::value::SerializeValuesError;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum FrameError {
    #[error("Type parsing failed")]
    Parse(#[from] ParseError),
    #[error("Frame is compressed, but no compression negotiated for connection.")]
    NoCompressionNegotiated,
    #[error("L4Z body decompression failed")]
    LZ4BodyDecompression,
    #[error("Received frame marked as coming from a client")]
    FrameFromClient,
    #[error("Received a frame from version {0}, but only 4 is supported")]
    VersionNotSupported(u8),
    #[error("Connection was closed before body was read: missing {0} out of {1}")]
    ConnectionClosed(usize, usize),
    #[error("Frame decompression failed.")]
    FrameDecompression,
    #[error("Frame compression failed.")]
    FrameCompression,
    #[error("std io error encountered while processing")]
    StdIOError(#[from] std::io::Error),
    #[error("Unrecognized opcode{0}")]
    TryFromPrimitiveError(#[from] num_enum::TryFromPrimitiveError<response::ResponseOpcode>),
}

#[derive(Error, Debug)]
pub enum ParseError {
    #[error("Bad data - couldn't serialize. Error msg: {0}")]
    BadData(String),
    #[error(transparent)]
    IoError(#[from] std::io::Error),
    #[error("type not yet implemented, id: {0}")]
    TypeNotImplemented(i16),
    #[error(transparent)]
    SerializeValuesError(#[from] SerializeValuesError),
    #[error(transparent)]
    CQLTypeError(#[from] CQLTypeError),
}
