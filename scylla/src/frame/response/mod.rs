pub mod error;
pub mod result;
pub mod supported;

use anyhow::Result as AResult;
use num_enum::TryFromPrimitive;

pub use error::Error;
pub use result::Result;
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

pub enum Response {
    Error(Error),
    Ready,
    Result(Result),
    Authenticate,
    Supported(Supported),
}

impl Response {
    pub fn deserialize(opcode: ResponseOpcode, buf: &mut &[u8]) -> AResult<Response> {
        let response = match opcode {
            ResponseOpcode::Error => Response::Error(Error::deserialize(buf)?),
            ResponseOpcode::Ready => Response::Ready,
            ResponseOpcode::Authenticate => unimplemented!(),
            ResponseOpcode::Supported => Response::Supported(Supported::deserialize(buf)?),
            ResponseOpcode::Result => Response::Result(result::deserialize(buf)?),
            ResponseOpcode::Event => unimplemented!(),
            ResponseOpcode::AuthChallenge => unimplemented!(),
            ResponseOpcode::AuthSuccess => unimplemented!(),
        };

        Ok(response)
    }
}
