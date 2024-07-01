use crate::frame::frame_errors::{CqlAuthSuccessParseError, CqlAuthenticateParseError, ParseError};
use crate::frame::types;

// Implements Authenticate message.
#[derive(Debug)]
pub struct Authenticate {
    pub authenticator_name: String,
}

impl Authenticate {
    pub fn deserialize(buf: &mut &[u8]) -> Result<Self, CqlAuthenticateParseError> {
        let authenticator_name = types::read_string(buf)
            .map_err(CqlAuthenticateParseError::AuthNameParseError)?
            .to_string();

        Ok(Authenticate { authenticator_name })
    }
}

#[derive(Debug)]
pub struct AuthSuccess {
    pub success_message: Option<Vec<u8>>,
}

impl AuthSuccess {
    pub fn deserialize(buf: &mut &[u8]) -> Result<Self, CqlAuthSuccessParseError> {
        let success_message = types::read_bytes_opt(buf)
            .map_err(CqlAuthSuccessParseError::SuccessMessageParseError)?
            .map(ToOwned::to_owned);

        Ok(AuthSuccess { success_message })
    }
}

#[derive(Debug)]
pub struct AuthChallenge {
    pub authenticate_message: Option<Vec<u8>>,
}

impl AuthChallenge {
    pub fn deserialize(buf: &mut &[u8]) -> Result<Self, ParseError> {
        let authenticate_message = types::read_bytes_opt(buf)?.map(|b| b.to_owned());

        Ok(AuthChallenge {
            authenticate_message,
        })
    }
}
