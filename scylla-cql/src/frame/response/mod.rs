//! CQL responses sent by the server.

pub mod authenticate;
mod custom_type_parser;
pub use scylla_cql_core::frame::response::error;
pub mod event;
pub mod result;
pub mod supported;

use std::sync::Arc;

pub use error::Error;
pub use scylla_cql_core::frame::response::CqlResponseKind;
pub use supported::Supported;

use crate::frame::TryFromPrimitiveError;
use crate::frame::protocol_features::ProtocolFeatures;
use crate::frame::response::result::ResultMetadata;

use super::frame_errors::CqlResponseParseError;

/// Opcode of a response, used to identify the response type in a CQL frame.
#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord)]
#[repr(u8)]
pub enum ResponseOpcode {
    /// See [CqlResponseKind::Error].
    Error = 0x00,
    /// See [CqlResponseKind::Ready].
    Ready = 0x02,
    /// See [CqlResponseKind::Authenticate].
    Authenticate = 0x03,
    /// See [CqlResponseKind::Supported].
    Supported = 0x06,
    /// See [CqlResponseKind::Result].
    Result = 0x08,
    /// See [CqlResponseKind::Event].
    Event = 0x0C,
    /// See [CqlResponseKind::AuthChallenge].
    AuthChallenge = 0x0E,
    /// See [CqlResponseKind::AuthSuccess].
    AuthSuccess = 0x10,
}

impl TryFrom<u8> for ResponseOpcode {
    type Error = TryFromPrimitiveError<u8>;

    fn try_from(value: u8) -> Result<Self, TryFromPrimitiveError<u8>> {
        match value {
            0x00 => Ok(Self::Error),
            0x02 => Ok(Self::Ready),
            0x03 => Ok(Self::Authenticate),
            0x06 => Ok(Self::Supported),
            0x08 => Ok(Self::Result),
            0x0C => Ok(Self::Event),
            0x0E => Ok(Self::AuthChallenge),
            0x10 => Ok(Self::AuthSuccess),
            _ => Err(TryFromPrimitiveError::new("ResponseOpcode", value)),
        }
    }
}

/// A CQL response that has been received from the server.
#[derive(Debug)]
#[non_exhaustive]
pub enum ResponseWithDeserializedMetadataV2 {
    /// ERROR response, returned by the server when an error occurs.
    Error(Error),
    /// READY response, indicating that the server is ready to process requests,
    /// typically after a connection is established.
    Ready,
    /// RESULT response, containing the result of a statement execution.
    Result(result::ResultWithDeserializedMetadata),
    /// AUTHENTICATE response, indicating that the server requires authentication.
    Authenticate(authenticate::Authenticate),
    /// AUTH_SUCCESS response, indicating that the authentication was successful.
    AuthSuccess(authenticate::AuthSuccess),
    /// AUTH_CHALLENGE response, indicating that the server requires further authentication.
    AuthChallenge(authenticate::AuthChallenge),
    /// SUPPORTED response, containing the features supported by the server.
    Supported(Supported),
    /// EVENT response, containing an event that occurred on the server.
    Event(event::Event),
}

impl ResponseWithDeserializedMetadataV2 {
    /// Deserializes a response from the given bytes, including the metadata
    /// of a RESULT:Rows response.
    pub fn deserialize(
        features: &ProtocolFeatures,
        opcode: ResponseOpcode,
        buf_bytes: bytes::Bytes,
        cached_metadata: Option<&Arc<ResultMetadata<'static>>>,
    ) -> Result<Self, CqlResponseParseError> {
        let buf = &mut &*buf_bytes;
        let response = match opcode {
            ResponseOpcode::Error => Self::Error(Error::deserialize(features, buf)?),
            ResponseOpcode::Ready => Self::Ready,
            ResponseOpcode::Authenticate => {
                Self::Authenticate(authenticate::Authenticate::deserialize(buf)?)
            }
            ResponseOpcode::Supported => Self::Supported(Supported::deserialize(buf)?),
            ResponseOpcode::Result => {
                Self::Result(result::deserialize(buf_bytes, cached_metadata, features)?)
            }
            ResponseOpcode::Event => Self::Event(event::Event::deserialize(buf)?),
            ResponseOpcode::AuthChallenge => {
                Self::AuthChallenge(authenticate::AuthChallenge::deserialize(buf)?)
            }
            ResponseOpcode::AuthSuccess => {
                Self::AuthSuccess(authenticate::AuthSuccess::deserialize(buf)?)
            }
        };

        Ok(response)
    }

    /// Returns the kind of this response.
    pub fn to_response_kind(&self) -> CqlResponseKind {
        match self {
            Self::Error(_) => CqlResponseKind::Error,
            Self::Ready => CqlResponseKind::Ready,
            Self::Result(_) => CqlResponseKind::Result,
            Self::Authenticate(_) => CqlResponseKind::Authenticate,
            Self::AuthSuccess(_) => CqlResponseKind::AuthSuccess,
            Self::AuthChallenge(_) => CqlResponseKind::AuthChallenge,
            Self::Supported(_) => CqlResponseKind::Supported,
            Self::Event(_) => CqlResponseKind::Event,
        }
    }

    /// Converts this response into a `NonErrorResponseV2`, returning an error if it is an `Error` response.
    pub fn into_non_error_response(
        self,
    ) -> Result<NonErrorResponseWithDeserializedMetadataV2, error::Error> {
        let non_error_response = match self {
            Self::Error(e) => return Err(e),
            Self::Ready => NonErrorResponseWithDeserializedMetadataV2::Ready,
            Self::Result(res) => NonErrorResponseWithDeserializedMetadataV2::Result(res),
            Self::Authenticate(auth) => {
                NonErrorResponseWithDeserializedMetadataV2::Authenticate(auth)
            }
            Self::AuthSuccess(auth_succ) => {
                NonErrorResponseWithDeserializedMetadataV2::AuthSuccess(auth_succ)
            }
            Self::AuthChallenge(auth_chal) => {
                NonErrorResponseWithDeserializedMetadataV2::AuthChallenge(auth_chal)
            }
            Self::Supported(sup) => NonErrorResponseWithDeserializedMetadataV2::Supported(sup),
            Self::Event(eve) => NonErrorResponseWithDeserializedMetadataV2::Event(eve),
        };

        Ok(non_error_response)
    }
}

/// A CQL response that has been received from the server, excluding error responses.
/// This is used to handle responses that are not errors, allowing for easier processing
/// of valid responses without need to handle error case any later.
#[derive(Debug)]
#[non_exhaustive]
pub enum NonErrorResponseWithDeserializedMetadataV2 {
    /// See [`ResponseWithDeserializedMetadataV2::Ready`].
    Ready,
    /// See [`ResponseWithDeserializedMetadataV2::Result`].
    Result(result::ResultWithDeserializedMetadata),
    /// See [`ResponseWithDeserializedMetadataV2::Authenticate`].
    Authenticate(authenticate::Authenticate),
    /// See [`ResponseWithDeserializedMetadataV2::AuthSuccess`].
    AuthSuccess(authenticate::AuthSuccess),
    /// See [`ResponseWithDeserializedMetadataV2::AuthChallenge`].
    AuthChallenge(authenticate::AuthChallenge),
    /// See [`ResponseWithDeserializedMetadataV2::Supported`].
    Supported(Supported),
    /// See [`ResponseWithDeserializedMetadataV2::Event`].
    Event(event::Event),
}

impl NonErrorResponseWithDeserializedMetadataV2 {
    /// Returns the kind of this non-error response.
    pub fn to_response_kind(&self) -> CqlResponseKind {
        match self {
            Self::Ready => CqlResponseKind::Ready,
            Self::Result(_) => CqlResponseKind::Result,
            Self::Authenticate(_) => CqlResponseKind::Authenticate,
            Self::AuthSuccess(_) => CqlResponseKind::AuthSuccess,
            Self::AuthChallenge(_) => CqlResponseKind::AuthChallenge,
            Self::Supported(_) => CqlResponseKind::Supported,
            Self::Event(_) => CqlResponseKind::Event,
        }
    }
}
