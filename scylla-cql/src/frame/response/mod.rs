//! CQL responses sent by the server.

pub mod authenticate;
pub mod custom_type_parser;
pub mod error;
pub mod event;
pub mod result;
pub mod supported;

use std::sync::Arc;

pub use error::Error;
pub use supported::Supported;

use crate::frame::protocol_features::ProtocolFeatures;
use crate::frame::response::result::ResultMetadata;
use crate::frame::TryFromPrimitiveError;

use super::frame_errors::CqlResponseParseError;

/// Possible CQL responses received from the server
#[derive(Debug, Copy, Clone)]
#[non_exhaustive]
pub enum CqlResponseKind {
    Error,
    Ready,
    Authenticate,
    Supported,
    Result,
    Event,
    AuthChallenge,
    AuthSuccess,
}

impl std::fmt::Display for CqlResponseKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let kind_str = match self {
            CqlResponseKind::Error => "ERROR",
            CqlResponseKind::Ready => "READY",
            CqlResponseKind::Authenticate => "AUTHENTICATE",
            CqlResponseKind::Supported => "SUPPORTED",
            CqlResponseKind::Result => "RESULT",
            CqlResponseKind::Event => "EVENT",
            CqlResponseKind::AuthChallenge => "AUTH_CHALLENGE",
            CqlResponseKind::AuthSuccess => "AUTH_SUCCESS",
        };

        f.write_str(kind_str)
    }
}

/// Opcode of a response, used to identify the response type in a CQL frame.
#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord)]
#[repr(u8)]
pub enum ResponseOpcode {
    Error = 0x00,
    Ready = 0x02,
    Authenticate = 0x03,
    Supported = 0x06,
    Result = 0x08,
    Event = 0x0C,
    AuthChallenge = 0x0E,
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
            _ => Err(TryFromPrimitiveError {
                enum_name: "ResponseOpcode",
                primitive: value,
            }),
        }
    }
}

/// A CQL response that has been received from the server.
#[derive(Debug)]
pub enum Response {
    /// ERROR response, returned by the server when an error occurs.
    Error(Error),
    /// READY response, indicating that the server is ready to process requests,
    /// typically after a connection is established.
    Ready,
    /// RESULT response, containing the result of a statement execution.
    Result(result::Result),
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

impl Response {
    /// Returns the kind of this response.
    pub fn to_response_kind(&self) -> CqlResponseKind {
        match self {
            Response::Error(_) => CqlResponseKind::Error,
            Response::Ready => CqlResponseKind::Ready,
            Response::Result(_) => CqlResponseKind::Result,
            Response::Authenticate(_) => CqlResponseKind::Authenticate,
            Response::AuthSuccess(_) => CqlResponseKind::AuthSuccess,
            Response::AuthChallenge(_) => CqlResponseKind::AuthChallenge,
            Response::Supported(_) => CqlResponseKind::Supported,
            Response::Event(_) => CqlResponseKind::Event,
        }
    }

    /// Deserialize a response from the given bytes.
    pub fn deserialize(
        features: &ProtocolFeatures,
        opcode: ResponseOpcode,
        buf_bytes: bytes::Bytes,
        cached_metadata: Option<&Arc<ResultMetadata<'static>>>,
    ) -> Result<Response, CqlResponseParseError> {
        let buf = &mut &*buf_bytes;
        let response = match opcode {
            ResponseOpcode::Error => Response::Error(Error::deserialize(features, buf)?),
            ResponseOpcode::Ready => Response::Ready,
            ResponseOpcode::Authenticate => {
                Response::Authenticate(authenticate::Authenticate::deserialize(buf)?)
            }
            ResponseOpcode::Supported => Response::Supported(Supported::deserialize(buf)?),
            ResponseOpcode::Result => {
                Response::Result(result::deserialize(buf_bytes, cached_metadata)?)
            }
            ResponseOpcode::Event => Response::Event(event::Event::deserialize(buf)?),
            ResponseOpcode::AuthChallenge => {
                Response::AuthChallenge(authenticate::AuthChallenge::deserialize(buf)?)
            }
            ResponseOpcode::AuthSuccess => {
                Response::AuthSuccess(authenticate::AuthSuccess::deserialize(buf)?)
            }
        };

        Ok(response)
    }

    /// Converts this response into a `NonErrorResponse`, returning an error if it is an `Error` response.
    pub fn into_non_error_response(self) -> Result<NonErrorResponse, error::Error> {
        let non_error_response = match self {
            Response::Error(e) => return Err(e),
            Response::Ready => NonErrorResponse::Ready,
            Response::Result(res) => NonErrorResponse::Result(res),
            Response::Authenticate(auth) => NonErrorResponse::Authenticate(auth),
            Response::AuthSuccess(auth_succ) => NonErrorResponse::AuthSuccess(auth_succ),
            Response::AuthChallenge(auth_chal) => NonErrorResponse::AuthChallenge(auth_chal),
            Response::Supported(sup) => NonErrorResponse::Supported(sup),
            Response::Event(eve) => NonErrorResponse::Event(eve),
        };

        Ok(non_error_response)
    }
}

/// A CQL response that has been received from the server, excluding error responses.
/// This is used to handle responses that are not errors, allowing for easier processing
/// of valid responses without need to handle error case any later.
#[derive(Debug)]
pub enum NonErrorResponse {
    /// See [`Response::Ready`].
    Ready,
    /// See [`Response::Result`].
    Result(result::Result),
    /// See [`Response::Authenticate`].
    Authenticate(authenticate::Authenticate),
    /// See [`Response::AuthSuccess`].
    AuthSuccess(authenticate::AuthSuccess),
    /// See [`Response::AuthChallenge`].
    AuthChallenge(authenticate::AuthChallenge),
    /// See [`Response::Supported`].
    Supported(Supported),
    /// See [`Response::Event`].
    Event(event::Event),
}

impl NonErrorResponse {
    /// Returns the kind of this non-error response.
    pub fn to_response_kind(&self) -> CqlResponseKind {
        match self {
            NonErrorResponse::Ready => CqlResponseKind::Ready,
            NonErrorResponse::Result(_) => CqlResponseKind::Result,
            NonErrorResponse::Authenticate(_) => CqlResponseKind::Authenticate,
            NonErrorResponse::AuthSuccess(_) => CqlResponseKind::AuthSuccess,
            NonErrorResponse::AuthChallenge(_) => CqlResponseKind::AuthChallenge,
            NonErrorResponse::Supported(_) => CqlResponseKind::Supported,
            NonErrorResponse::Event(_) => CqlResponseKind::Event,
        }
    }
}
