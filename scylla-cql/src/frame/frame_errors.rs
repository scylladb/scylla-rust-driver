//! Low-level errors that can occur during CQL frame parsing and serialization.

use std::error::Error;
use std::sync::Arc;

pub use super::request::{
    auth_response::AuthResponseSerializationError,
    batch::{BatchSerializationError, BatchStatementSerializationError},
    execute::ExecuteSerializationError,
    prepare::PrepareSerializationError,
    query::{QueryParametersSerializationError, QuerySerializationError},
    register::RegisterSerializationError,
    startup::StartupSerializationError,
};

use super::TryFromPrimitiveError;
use super::response::CqlResponseKind;
use crate::utils::parse::ParseErrorCause;
use thiserror::Error;

/// An error returned by `parse_response_body_extensions`.
///
/// It represents an error that occurred during deserialization of
/// frame body extensions. These extensions include tracing id,
/// warnings and custom payload.
///
/// Possible error kinds:
/// - failed to decompress frame body (decompression is required for further deserialization)
/// - failed to deserialize tracing id (body ext.)
/// - failed to deserialize warnings list (body ext.)
/// - failed to deserialize custom payload map (body ext.)
#[derive(Error, Debug, Clone)]
#[non_exhaustive]
pub enum FrameBodyExtensionsParseError {
    /// Frame is compressed, but no compression was negotiated for the connection.
    #[error("Frame is compressed, but no compression negotiated for connection.")]
    NoCompressionNegotiated,

    /// Failed to deserialize frame trace id.
    #[error("Malformed trace id: {0}")]
    TraceIdParse(LowLevelDeserializationError),

    /// Failed to deserialize warnings attached to frame.
    #[error("Malformed warnings list: {0}")]
    WarningsListParse(LowLevelDeserializationError),

    /// Failed to deserialize frame's custom payload.
    #[error("Malformed custom payload map: {0}")]
    CustomPayloadMapParse(LowLevelDeserializationError),

    /// Failed to decompress frame body (snap).
    #[error("Snap decompression error: {0}")]
    SnapDecompressError(Arc<dyn Error + Sync + Send>),

    /// Failed to decompress frame body (lz4).
    #[error("Error decompressing lz4 data {0}")]
    Lz4DecompressError(Arc<dyn Error + Sync + Send>),
}

/// An error that occurred during frame header deserialization.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum FrameHeaderParseError {
    /// Failed to read the frame header from the socket.
    #[error("Failed to read the frame header: {0}")]
    HeaderIoError(std::io::Error),

    /// Received a frame marked as coming from a client.
    #[error("Received frame marked as coming from a client")]
    FrameFromClient,

    /// Received a frame marked as coming from a server, while expecting a frame
    /// coming from a client.
    // FIXME: this should not belong here. User always expects a frame from server.
    // This variant is only used in scylla-proxy - need to investigate it later.
    #[error("Received frame marked as coming from the server")]
    FrameFromServer,

    /// Received a frame with unsupported version.
    #[error("Received a frame from version {0}, but only 4 is supported")]
    VersionNotSupported(u8),

    /// Received unknown response opcode.
    #[error("Unrecognized response opcode {0}")]
    UnknownResponseOpcode(#[from] TryFromPrimitiveError<u8>),

    /// Failed to read frame body from the socket.
    #[error("Failed to read a chunk of response body. Expected {0} more bytes, error: {1}")]
    BodyChunkIoError(usize, std::io::Error),

    /// Connection was closed before whole frame was read.
    #[error("Connection was closed before body was read: missing {0} out of {1}")]
    ConnectionClosed(usize, usize),
}

/// An error that occurred during CQL request serialization.
#[non_exhaustive]
#[derive(Error, Debug, Clone)]
pub enum CqlRequestSerializationError {
    /// Failed to serialize STARTUP request.
    #[error("Failed to serialize STARTUP request: {0}")]
    StartupSerialization(#[from] StartupSerializationError),

    /// Failed to serialize REGISTER request.
    #[error("Failed to serialize REGISTER request: {0}")]
    RegisterSerialization(#[from] RegisterSerializationError),

    /// Failed to serialize AUTH_RESPONSE request.
    #[error("Failed to serialize AUTH_RESPONSE request: {0}")]
    AuthResponseSerialization(#[from] AuthResponseSerializationError),

    /// Failed to serialize BATCH request.
    #[error("Failed to serialize BATCH request: {0}")]
    BatchSerialization(#[from] BatchSerializationError),

    /// Failed to serialize PREPARE request.
    #[error("Failed to serialize PREPARE request: {0}")]
    PrepareSerialization(#[from] PrepareSerializationError),

    /// Failed to serialize EXECUTE request.
    #[error("Failed to serialize EXECUTE request: {0}")]
    ExecuteSerialization(#[from] ExecuteSerializationError),

    /// Failed to serialize QUERY request.
    #[error("Failed to serialize QUERY request: {0}")]
    QuerySerialization(#[from] QuerySerializationError),

    /// Request body compression failed.
    #[error("Snap compression error: {0}")]
    SnapCompressError(Arc<dyn Error + Sync + Send>),
}

/// An error type returned when deserialization of CQL
/// server response fails.
#[non_exhaustive]
#[derive(Error, Debug, Clone)]
// Check triggers because all variants begin with "Cql".
// TODO(2.0): Remove the "Cql" prefix from variants.
#[expect(clippy::enum_variant_names)]
pub enum CqlResponseParseError {
    #[error("Failed to deserialize ERROR response: {0}")]
    CqlErrorParseError(#[from] CqlErrorParseError),
    #[error("Failed to deserialize AUTH_CHALLENGE response: {0}")]
    CqlAuthChallengeParseError(#[from] CqlAuthChallengeParseError),
    #[error("Failed to deserialize AUTH_SUCCESS response: {0}")]
    CqlAuthSuccessParseError(#[from] CqlAuthSuccessParseError),
    #[error("Failed to deserialize AUTHENTICATE response: {0}")]
    CqlAuthenticateParseError(#[from] CqlAuthenticateParseError),
    #[error("Failed to deserialize SUPPORTED response: {0}")]
    CqlSupportedParseError(#[from] CqlSupportedParseError),
    #[error("Failed to deserialize EVENT response: {0}")]
    CqlEventParseError(#[from] CqlEventParseError),
    #[error(transparent)]
    CqlResultParseError(#[from] CqlResultParseError),
}

impl CqlResponseParseError {
    /// Retrieves the kind of CQL response that this error corresponds to.
    pub fn to_response_kind(&self) -> CqlResponseKind {
        match self {
            CqlResponseParseError::CqlErrorParseError(_) => CqlResponseKind::Error,
            CqlResponseParseError::CqlAuthChallengeParseError(_) => CqlResponseKind::AuthChallenge,
            CqlResponseParseError::CqlAuthSuccessParseError(_) => CqlResponseKind::AuthSuccess,
            CqlResponseParseError::CqlAuthenticateParseError(_) => CqlResponseKind::Authenticate,
            CqlResponseParseError::CqlSupportedParseError(_) => CqlResponseKind::Supported,
            CqlResponseParseError::CqlEventParseError(_) => CqlResponseKind::Event,
            CqlResponseParseError::CqlResultParseError(_) => CqlResponseKind::Result,
        }
    }
}

/// An error type returned when deserialization of ERROR response fails.
#[non_exhaustive]
#[derive(Error, Debug, Clone)]
pub enum CqlErrorParseError {
    #[error("Malformed error code: {0}")]
    ErrorCodeParseError(LowLevelDeserializationError),
    #[error("Malformed error reason: {0}")]
    ReasonParseError(LowLevelDeserializationError),
    #[error("Malformed error field {field} of DB error {db_error}: {err}")]
    MalformedErrorField {
        db_error: &'static str,
        field: &'static str,
        err: LowLevelDeserializationError,
    },
}

/// An error type returned when deserialization of AUTH_CHALLENGE response fails.
#[non_exhaustive]
#[derive(Error, Debug, Clone)]
pub enum CqlAuthChallengeParseError {
    #[error("Malformed authenticate message: {0}")]
    AuthMessageParseError(LowLevelDeserializationError),
}

/// An error type returned when deserialization of AUTH_SUCCESS response fails.
#[non_exhaustive]
#[derive(Error, Debug, Clone)]
pub enum CqlAuthSuccessParseError {
    #[error("Malformed success message: {0}")]
    SuccessMessageParseError(LowLevelDeserializationError),
}

/// An error type returned when deserialization of AUTHENTICATE response fails.
#[non_exhaustive]
#[derive(Error, Debug, Clone)]
pub enum CqlAuthenticateParseError {
    #[error("Malformed authenticator name: {0}")]
    AuthNameParseError(LowLevelDeserializationError),
}

/// An error type returned when deserialization of SUPPORTED response fails.
#[non_exhaustive]
#[derive(Error, Debug, Clone)]
pub enum CqlSupportedParseError {
    #[error("Malformed options map: {0}")]
    OptionsMapDeserialization(LowLevelDeserializationError),
}

/// An error type returned when deserialization of RESULT response fails.
#[non_exhaustive]
#[derive(Error, Debug, Clone)]
pub enum CqlResultParseError {
    #[error("Malformed RESULT response id: {0}")]
    ResultIdParseError(LowLevelDeserializationError),
    #[error("Unknown RESULT response id: {0}")]
    UnknownResultId(i32),
    #[error("RESULT:Set_keyspace response deserialization failed: {0}")]
    SetKeyspaceParseError(#[from] SetKeyspaceParseError),
    // This is an error returned during deserialization of
    // `RESULT::Schema_change` response, and not `EVENT` response.
    #[error("RESULT:Schema_change response deserialization failed: {0}")]
    SchemaChangeParseError(#[from] SchemaChangeEventParseError),
    #[error("RESULT:Prepared response deserialization failed: {0}")]
    PreparedParseError(#[from] PreparedParseError),
    #[error("RESULT:Rows response deserialization failed: {0}")]
    RawRowsParseError(#[from] RawRowsAndPagingStateResponseParseError),
    #[error("RESULT:Rows result metadata response deserialization failed: {0}")]
    ResultMetadataParseError(#[from] ResultMetadataAndRowsCountParseError),
}

#[non_exhaustive]
#[derive(Error, Debug, Clone)]
pub enum SetKeyspaceParseError {
    #[error("Malformed keyspace name: {0}")]
    MalformedKeyspaceName(#[from] LowLevelDeserializationError),
}

/// An error type returned when deserialization of
/// `EVENT` response fails.
#[non_exhaustive]
#[derive(Error, Debug, Clone)]
pub enum CqlEventParseError {
    #[error("Malformed event type string: {0}")]
    EventTypeParseError(LowLevelDeserializationError),
    #[error("Unknown event type: {0}")]
    UnknownEventType(String),
    #[error("Failed to deserialize schema change event: {0}")]
    SchemaChangeEventParseError(#[from] SchemaChangeEventParseError),
    #[error("Failed to deserialize topology change event: {0}")]
    TopologyChangeEventParseError(ClusterChangeEventParseError),
    #[error("Failed to deserialize status change event: {0}")]
    StatusChangeEventParseError(ClusterChangeEventParseError),
}

/// An error type returned when deserialization of
/// SchemaChangeEvent fails.
#[non_exhaustive]
#[derive(Error, Debug, Clone)]
pub enum SchemaChangeEventParseError {
    #[error("Malformed schema change type string: {0}")]
    TypeOfChangeParseError(LowLevelDeserializationError),
    #[error("Malformed schema change target string:: {0}")]
    TargetTypeParseError(LowLevelDeserializationError),
    #[error("Malformed name of keyspace affected by schema change: {0}")]
    AffectedKeyspaceParseError(LowLevelDeserializationError),
    #[error("Malformed name of the table affected by schema change: {0}")]
    AffectedTableNameParseError(LowLevelDeserializationError),
    #[error("Malformed name of the target affected by schema change: {0}")]
    AffectedTargetNameParseError(LowLevelDeserializationError),
    #[error(
        "Malformed number of arguments of the function/aggregate affected by schema change: {0}"
    )]
    ArgumentCountParseError(LowLevelDeserializationError),
    #[error("Malformed argument of the function/aggregate affected by schema change: {0}")]
    FunctionArgumentParseError(LowLevelDeserializationError),
    #[error("Unknown target of schema change: {0}")]
    UnknownTargetOfSchemaChange(String),
}

/// An error type returned when deserialization of [Status/Topology]ChangeEvent fails.
#[non_exhaustive]
#[derive(Error, Debug, Clone)]
pub enum ClusterChangeEventParseError {
    #[error("Malformed type of change: {0}")]
    TypeOfChangeParseError(LowLevelDeserializationError),
    #[error("Malformed node address: {0}")]
    NodeAddressParseError(LowLevelDeserializationError),
    #[error("Unknown type of change: {0}")]
    UnknownTypeOfChange(String),
}

/// An error type returned when deserialization
/// of `RESULT::`Prepared` response fails.
#[non_exhaustive]
#[derive(Debug, Error, Clone)]
pub enum PreparedParseError {
    // TODO(2.0): This variant is unused, and should be removed.
    #[error("Malformed prepared statement's id length: {0}")]
    IdLengthParseError(LowLevelDeserializationError),
    #[error("Malformed prepared statement's id: {0}")]
    IdParseError(LowLevelDeserializationError),
    #[error("Invalid result metadata: {0}")]
    ResultMetadataParseError(ResultMetadataParseError),
    #[error("Invalid prepared metadata: {0}")]
    PreparedMetadataParseError(PreparedMetadataParseError),
    #[error("Non-zero paging state in result metadata: {0:?}")]
    NonZeroPagingState(Arc<[u8]>),
}

/// An error that occurred during initial deserialization of
/// `RESULT:Rows` response. Since the deserialization of rows is lazy,
/// we initially only need to deserialize:
/// - result metadata flags
/// - column count (result metadata)
/// - paging state response
#[non_exhaustive]
#[derive(Debug, Error, Clone)]
// Check triggers because all variants end with "ParseError".
// TODO(2.0): Remove the "ParseError" postfix from variants.
#[expect(clippy::enum_variant_names)]
pub enum RawRowsAndPagingStateResponseParseError {
    /// Failed to parse metadata flags.
    #[error("Malformed metadata flags: {0}")]
    FlagsParseError(LowLevelDeserializationError),

    /// Failed to parse column count.
    #[error("Malformed column count: {0}")]
    ColumnCountParseError(LowLevelDeserializationError),

    /// Failed to parse paging state response.
    #[error("Malformed paging state: {0}")]
    PagingStateParseError(LowLevelDeserializationError),
}

/// An error type returned when deserialization
/// of statement's prepared metadata failed.
#[non_exhaustive]
#[derive(Error, Debug, Clone)]
// Check triggers because all variants end with "ParseError".
// TODO(2.0): Remove the "ParseError" postfix from variants.
#[expect(clippy::enum_variant_names)]
pub enum PreparedMetadataParseError {
    /// Failed to parse metadata flags.
    #[error("Malformed metadata flags: {0}")]
    FlagsParseError(LowLevelDeserializationError),

    /// Failed to parse column count.
    #[error("Malformed column count: {0}")]
    ColumnCountParseError(LowLevelDeserializationError),

    /// Failed to parse partition key count.
    #[error("Malformed partition key count: {0}")]
    PkCountParseError(LowLevelDeserializationError),

    /// Failed to parse partition key index.
    #[error("Malformed partition key index: {0}")]
    PkIndexParseError(LowLevelDeserializationError),

    /// Failed to parse global table spec.
    #[error("Invalid global table spec: {0}")]
    GlobalTableSpecParseError(#[from] TableSpecParseError),

    /// Failed to parse column spec.
    #[error("Invalid column spec: {0}")]
    ColumnSpecParseError(#[from] ColumnSpecParseError),
}

/// An error returned when lazy deserialization of
/// result metadata and rows count fails.
#[non_exhaustive]
#[derive(Error, Debug, Clone)]
pub enum ResultMetadataAndRowsCountParseError {
    /// Failed to deserialize result metadata.
    #[error("Failed to lazily deserialize result metadata: {0}")]
    ResultMetadataParseError(#[from] ResultMetadataParseError),

    /// Received malformed rows count from the server.
    #[error("Malformed rows count: {0}")]
    RowsCountParseError(LowLevelDeserializationError),
}

/// An error type returned when deserialization
/// of result metadata failed.
#[non_exhaustive]
#[derive(Error, Debug, Clone)]
// Check triggers because all variants end with "ParseError".
// TODO(2.0): Remove the "ParseError" postfix from variants.
#[expect(clippy::enum_variant_names)]
pub enum ResultMetadataParseError {
    /// Failed to parse metadata flags.
    #[error("Malformed metadata flags: {0}")]
    FlagsParseError(LowLevelDeserializationError),

    /// Failed to parse column count.
    #[error("Malformed column count: {0}")]
    ColumnCountParseError(LowLevelDeserializationError),

    /// Failed to parse paging state response.
    #[error("Malformed paging state: {0}")]
    PagingStateParseError(LowLevelDeserializationError),

    /// Failed to parse global table spec.
    #[error("Invalid global table spec: {0}")]
    GlobalTableSpecParseError(#[from] TableSpecParseError),

    /// Failed to parse column spec.
    #[error("Invalid column spec: {0}")]
    ColumnSpecParseError(#[from] ColumnSpecParseError),
}

/// An error type returned when deserialization
/// of table specification fails.
#[non_exhaustive]
#[derive(Error, Debug, Clone)]
pub enum TableSpecParseError {
    #[error("Malformed keyspace name: {0}")]
    MalformedKeyspaceName(LowLevelDeserializationError),
    #[error("Malformed table name: {0}")]
    MalformedTableName(LowLevelDeserializationError),
}

/// An error type returned when deserialization
/// of table column specifications fails.
#[non_exhaustive]
#[derive(Error, Debug, Clone)]
#[error("Column spec deserialization failed, column index: {column_index}, error: {kind}")]
pub struct ColumnSpecParseError {
    pub column_index: usize,
    pub kind: ColumnSpecParseErrorKind,
}

/// The type of error that appeared during deserialization
/// of a column specification.
#[non_exhaustive]
#[derive(Error, Debug, Clone)]
// Check triggers because all variants end with "ParseError".
// TODO(2.0): Remove the "ParseError" postfix from variants.
#[expect(clippy::enum_variant_names)]
pub enum ColumnSpecParseErrorKind {
    #[error("Invalid table spec: {0}")]
    TableSpecParseError(#[from] TableSpecParseError),
    #[error("Malformed column name: {0}")]
    ColumnNameParseError(#[from] LowLevelDeserializationError),
    #[error("Invalid column type: {0}")]
    ColumnTypeParseError(#[from] CqlTypeParseError),
}

/// An error type returned when deserialization of Custom CQL type name fails.
#[non_exhaustive]
#[derive(Error, Debug, Clone, PartialEq, Eq)]
pub enum CustomTypeParseError {
    #[error("Malformed simple custom type name: {0}")]
    UnknownSimpleCustomTypeName(String),
    #[error("Malformed complex custom type name: {0}")]
    UnknownComplexCustomTypeName(String),
    #[error("Unexpected character encountered: {0}, expected: {1}")]
    UnexpectedCharacter(char, char),
    #[error("Unable to parse an integer: {0}")]
    IntegerParseError(ParseErrorCause),
    #[error("Unexpected end of input")]
    UnexpectedEndOfInput,
    #[error("Bad hex string: {0}")]
    BadHexString(String),
    #[error("Bytes {0:?} do not represent a valid UTF-8 string")]
    InvalidUtf8(Vec<u8>),
    #[error("Wrong number of parameters {actual}, expected: {expected}")]
    InvalidParameterCount { actual: usize, expected: usize },
}

/// An error type returned when deserialization of CQL type name fails.
#[non_exhaustive]
#[derive(Error, Debug, Clone)]
pub enum CqlTypeParseError {
    #[error("Malformed type id: {0}")]
    TypeIdParseError(LowLevelDeserializationError),
    #[error("Malformed custom type name: {0}")]
    CustomTypeNameParseError(LowLevelDeserializationError),
    #[error("Unsupported custom type: {0}")]
    CustomTypeUnsupported(String),
    #[error("Malformed name of UDT keyspace: {0}")]
    UdtKeyspaceNameParseError(LowLevelDeserializationError),
    #[error("Malformed UDT name: {0}")]
    UdtNameParseError(LowLevelDeserializationError),
    #[error("Malformed UDT fields count: {0}")]
    UdtFieldsCountParseError(LowLevelDeserializationError),
    #[error("Malformed UDT's field name: {0}")]
    UdtFieldNameParseError(LowLevelDeserializationError),
    #[error("Malformed tuple length: {0}")]
    TupleLengthParseError(LowLevelDeserializationError),
    #[error("CQL Type not yet implemented, id: {0}")]
    TypeNotImplemented(u16),
    #[error("Failed to parse custom CQL type: {0}")]
    CustomTypeParseError(CustomTypeParseError),
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
