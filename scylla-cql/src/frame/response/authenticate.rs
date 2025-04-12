use crate::frame::frame_errors::{
    CqlAuthChallengeParseError, CqlAuthSuccessParseError, CqlAuthenticateParseError,
};
use crate::frame::types;

// Implements Authenticate message.
#[derive(Debug)]
pub struct Authenticate {
    pub authenticator_name: String,
}

impl Authenticate {
    #[inline]
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
    #[inline]
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
    #[inline]
    pub fn deserialize(buf: &mut &[u8]) -> Result<Self, CqlAuthChallengeParseError> {
        let authenticate_message = types::read_bytes_opt(buf)
            .map_err(CqlAuthChallengeParseError::AuthMessageParseError)?
            .map(|b| b.to_owned());

        Ok(AuthChallenge {
            authenticate_message,
        })
    }
}
