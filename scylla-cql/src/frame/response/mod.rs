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

#[derive(Debug)]
pub enum Response {
    Error(Error),
    Ready,
    Result(result::Result),
    Authenticate(authenticate::Authenticate),
    AuthSuccess(authenticate::AuthSuccess),
    AuthChallenge(authenticate::AuthChallenge),
    Supported(Supported),
    Event(event::Event),
}

impl Response {
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

// A Response which can not be Response::Error
#[derive(Debug)]
pub enum NonErrorResponse {
    Ready,
    Result(result::Result),
    Authenticate(authenticate::Authenticate),
    AuthSuccess(authenticate::AuthSuccess),
    AuthChallenge(authenticate::AuthChallenge),
    Supported(Supported),
    Event(event::Event),
}

impl NonErrorResponse {
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
