pub mod authenticate;
pub mod cql_to_rust;
pub mod error;
pub mod event;
pub mod result;
pub mod supported;

use crate::{errors::QueryError, frame::frame_errors::ParseError};
use num_enum::TryFromPrimitive;

use crate::frame::protocol_features::ProtocolFeatures;
pub use error::Error;
pub use supported::Supported;

#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord, TryFromPrimitive)]
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
    pub fn deserialize(
        features: &ProtocolFeatures,
        opcode: ResponseOpcode,
        buf: &mut &[u8],
    ) -> Result<Response, ParseError> {
        let response = match opcode {
            ResponseOpcode::Error => Response::Error(Error::deserialize(features, buf)?),
            ResponseOpcode::Ready => Response::Ready,
            ResponseOpcode::Authenticate => {
                Response::Authenticate(authenticate::Authenticate::deserialize(buf)?)
            }
            ResponseOpcode::Supported => Response::Supported(Supported::deserialize(buf)?),
            ResponseOpcode::Result => Response::Result(result::deserialize(buf)?),
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

    pub fn into_non_error_response(self) -> Result<NonErrorResponse, QueryError> {
        Ok(match self {
            Response::Error(err) => return Err(QueryError::from(err)),
            Response::Ready => NonErrorResponse::Ready,
            Response::Result(res) => NonErrorResponse::Result(res),
            Response::Authenticate(auth) => NonErrorResponse::Authenticate(auth),
            Response::AuthSuccess(auth_succ) => NonErrorResponse::AuthSuccess(auth_succ),
            Response::AuthChallenge(auth_chal) => NonErrorResponse::AuthChallenge(auth_chal),
            Response::Supported(sup) => NonErrorResponse::Supported(sup),
            Response::Event(eve) => NonErrorResponse::Event(eve),
        })
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
