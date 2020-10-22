pub mod error;
pub mod result;

use anyhow::Result as AResult;
use num_enum::TryFromPrimitive;

pub use error::Error;
pub use result::Result;

#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord, TryFromPrimitive)]
#[repr(u8)]
pub enum ResponseOpcode {
    Error = 0x00,
    Ready = 0x02,
    Result = 0x08,
}

pub enum Response {
    Error(Error),
    Ready,
    Result(Result),
}

impl Response {
    pub fn deserialize(opcode: ResponseOpcode, buf: &mut &[u8]) -> AResult<Response> {
        let response = match opcode {
            ResponseOpcode::Error => Response::Error(Error::deserialize(buf)?),
            ResponseOpcode::Ready => Response::Ready,
            ResponseOpcode::Result => Response::Result(Result::deserialize(buf)?),
        };

        Ok(response)
    }
}
