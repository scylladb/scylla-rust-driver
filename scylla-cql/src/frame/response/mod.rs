pub mod authenticate;
pub mod cql_to_rust;
pub mod error;
pub mod event;
pub mod result;
pub mod supported;

use crate::frame::frame_errors::ParseError;
use num_enum::TryFromPrimitive;

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
        opcode: ResponseOpcode,
        buf: &mut &[u8],
    ) -> Result<Result<Response, Error>, ParseError> {
        let response = match opcode {
            ResponseOpcode::Error => Err(Error::deserialize(buf)?),
            ResponseOpcode::Ready => Ok(Response::Ready),
            ResponseOpcode::Authenticate => Ok(Response::Authenticate(
                authenticate::Authenticate::deserialize(buf)?,
            )),
            ResponseOpcode::Supported => Ok(Response::Supported(Supported::deserialize(buf)?)),
            ResponseOpcode::Result => Ok(Response::Result(result::deserialize(buf)?)),
            ResponseOpcode::Event => Ok(Response::Event(event::Event::deserialize(buf)?)),
            ResponseOpcode::AuthChallenge => Ok(Response::AuthChallenge(
                authenticate::AuthChallenge::deserialize(buf)?,
            )),
            ResponseOpcode::AuthSuccess => Ok(Response::AuthSuccess(
                authenticate::AuthSuccess::deserialize(buf)?,
            )),
        };

        Ok(response)
    }
}
