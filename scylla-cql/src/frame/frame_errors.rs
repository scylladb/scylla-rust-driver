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
    #[error(transparent)]
    CqlResultParseError(#[from] CqlResultParseError),
    #[error("Failed to deserialize schema change event: {0}")]
    SchemaChangeEventParseError(#[from] SchemaChangeEventParseError),
    #[error("Low-level serialization failed: {0}")]
    LowLevelSerializationError(#[from] LowLevelSerializationError),
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
    #[error(transparent)]
    SerializeValuesError(#[from] SerializeValuesError),
    #[error(transparent)]
    SerializationError(#[from] SerializationError),
    #[error(transparent)]
    CqlTypeError(#[from] CqlTypeError),
}

/// An error type returned when deserialization of RESULT response fails.
#[non_exhaustive]
#[derive(Error, Debug)]
pub enum CqlResultParseError {
    #[error("Malformed RESULT response id: {0}")]
    ResultIdParseError(LowLevelDeserializationError),
    #[error("Unknown RESULT response id: {0}")]
    UnknownResultId(i32),
    #[error("'Set_keyspace' response deserialization failed: {0}")]
    SetKeyspaceParseError(#[from] SetKeyspaceParseError),
    #[error("'Schema_change' response deserialization failed: {0}")]
    SchemaChangeEventParseError(#[from] SchemaChangeEventParseError),
    #[error("'Prepared' response deserialization failed: {0}")]
    PreparedParseError(#[from] PreparedParseError),
    #[error("'Rows' response deserialization failed: {0}")]
    RowsParseError(#[from] RowsParseError),
}

#[non_exhaustive]
#[derive(Error, Debug)]
pub enum SetKeyspaceParseError {
    #[error("Malformed keyspace name: {0}")]
    StringDeserializationError(#[from] LowLevelDeserializationError),
}

/// An error type returned when deserialization of
/// `RESULT::Schema_change` response fails.
#[non_exhaustive]
#[derive(Error, Debug)]
pub enum SchemaChangeEventParseError {
    #[error("Malformed type of schema change string: {0}")]
    TypeOfChangeParseError(LowLevelDeserializationError),
    #[error("Malformed target of schema change string: {0}")]
    TargetTypeParseError(LowLevelDeserializationError),
    #[error("Malformed name of keyspace affected by schema change: {0}")]
    AffectedKeyspaceParseError(LowLevelDeserializationError),
    #[error("Malformed name of the table affected by schema change: {0}")]
    TableNameParseError(LowLevelDeserializationError),
    #[error("Malformed name of the target affected by schema change: {0}")]
    TargetNameParseError(LowLevelDeserializationError),
    #[error(
        "Malformed number of arguments of the function/aggregate affected by schema change: {0}"
    )]
    ArgumentCountParseError(LowLevelDeserializationError),
    #[error("Malformed argument of the function/aggregate affected by schema change: {0}")]
    FunctionArgumentParseError(LowLevelDeserializationError),
    #[error("Unknown target of schema change: {0}")]
    UnknownTargetOfSchemaChange(String),
}

/// An error type returned when deserialization
/// of `RESULT::`Prepared` response fails.
#[non_exhaustive]
#[derive(Debug, Error)]
pub enum PreparedParseError {
    #[error("Malformed prepared statement's id length: {0}")]
    IdLengthParseError(LowLevelDeserializationError),
    #[error("Invalid result metadata: {0}")]
    ResultMetadataParseError(ResultMetadataParseError),
    #[error("Invalid prepared metadata: {0}")]
    PreparedMetadataParseError(ResultMetadataParseError),
}

/// An error type returned when deserialization
/// of `RESULT::Rows` response fails.
#[non_exhaustive]
#[derive(Debug, Error)]
pub enum RowsParseError {
    #[error("Invalid result metadata: {0}")]
    ResultMetadataParseError(#[from] ResultMetadataParseError),
    #[error("Invalid result metadata, expected {expected_col_count} col specs, received {received_col_count}.")]
    ColumnCountMismatch {
        expected_col_count: usize,
        received_col_count: usize,
    },
    #[error("Malformed rows count: {0}")]
    RowsCountParseError(LowLevelDeserializationError),
    #[error("Data deserialization failed: {0}")]
    DataDeserializationError(#[from] DeserializationError),
}

/// An error type returned when deserialization
/// of `[Result/Prepared]Metadata` failed.
#[non_exhaustive]
#[derive(Error, Debug)]
pub enum ResultMetadataParseError {
    #[error("Malformed metadata flags: {0}")]
    FlagsParseError(LowLevelDeserializationError),
    #[error("Malformed column count: {0}")]
    ColumnCountParseError(LowLevelDeserializationError),
    #[error("Malformed partition key count: {0}")]
    PkCountParseError(LowLevelDeserializationError),
    #[error("Malformed partition key index: {0}")]
    PkIndexParseError(LowLevelDeserializationError),
    #[error("Malformed paging state: {0}")]
    PagingStateParseError(LowLevelDeserializationError),
    #[error("Invalid global table spec: {0}")]
    GlobalTableSpecParseError(#[from] TableSpecParseError),
    #[error("Invalid column specs: {0}")]
    ColumnSpecsParseError(#[from] ColumnSpecsParseError),
}

/// An error type returned when deserialization
/// of table specification fails.
#[non_exhaustive]
#[derive(Error, Debug)]
pub enum TableSpecParseError {
    #[error("Malformed keyspace name: {0}")]
    KeyspaceNameParseError(LowLevelDeserializationError),
    #[error("Malformed table name: {0}")]
    TableNameParseError(LowLevelDeserializationError),
}

/// An error type returned when deserialization
/// of table column specifications fails.
#[non_exhaustive]
#[derive(Error, Debug)]
pub enum ColumnSpecsParseError {
    #[error("Column spec deserialization failed, column index: {column_index}, error: {kind}")]
    ColumnSpecParseError {
        column_index: usize,
        kind: ColumnSpecParseErrorKind,
    },
}

/// The type of error that appeared during deserialization
/// of a column specification.
#[non_exhaustive]
#[derive(Error, Debug)]
pub enum ColumnSpecParseErrorKind {
    #[error("Invalid table spec: {0}")]
    TableSpecParseError(#[from] TableSpecParseError),
    #[error("Malformed column name: {0}")]
    ColumnNameParseError(#[from] LowLevelDeserializationError),
    #[error("Invalid column type: {0}")]
    ColumnTypeParseError(#[from] TypeParseError),
}

/// An error type returned when deserialization of CQL type name fails.
#[non_exhaustive]
#[derive(Error, Debug)]
pub enum TypeParseError {
    #[error("Malformed type id: {0}")]
    TypeIdParseError(LowLevelDeserializationError),
    #[error("Malformed custom type name: {0}")]
    CustomTypeNameParseError(LowLevelDeserializationError),
    #[error("Malformed keyspace name of UDT: {0}")]
    UdtKeyspaceNameParseError(LowLevelDeserializationError),
    #[error("Malformed UDT name: {0}")]
    UdtNameParseError(LowLevelDeserializationError),
    #[error("Malformed UDT fields count: {0}")]
    UdtFieldsCountParseError(LowLevelDeserializationError),
    #[error("Malformed UDT's field name: {0}")]
    UdtFieldNameParseError(LowLevelDeserializationError),
    #[error("Malformed tuple length: {0}")]
    TupleLengthParseError(LowLevelDeserializationError),
    #[error("Type not yet implemented, id: {0}")]
    TypeNotImplemented(u16),
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
