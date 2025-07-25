//! CQL protocol-level representation of an `AUTHENTICATE` response.

use crate::frame::frame_errors::{
    CqlAuthChallengeParseError, CqlAuthSuccessParseError, CqlAuthenticateParseError,
};
use crate::frame::types;

/// Represents AUTHENTICATE CQL response.
#[derive(Debug)]
pub struct Authenticate {
    /// The name of the authenticator requested by the server to use.
    pub authenticator_name: String,
}

impl Authenticate {
    /// Deserializes an `AUTHENTICATE` message from the provided buffer.
    pub fn deserialize(buf: &mut &[u8]) -> Result<Self, CqlAuthenticateParseError> {
        let authenticator_name = types::read_string(buf)
            .map_err(CqlAuthenticateParseError::AuthNameParseError)?
            .to_string();

        Ok(Authenticate { authenticator_name })
    }
}

/// Represents AUTH_SUCCESS CQL response.
#[derive(Debug)]
pub struct AuthSuccess {
    /// Optional success message provided by the server.
    pub success_message: Option<Vec<u8>>,
}

impl AuthSuccess {
    /// Deserializes an `AUTH_SUCCESS` message from the provided buffer.
    pub fn deserialize(buf: &mut &[u8]) -> Result<Self, CqlAuthSuccessParseError> {
        let success_message = types::read_bytes_opt(buf)
            .map_err(CqlAuthSuccessParseError::SuccessMessageParseError)?
            .map(ToOwned::to_owned);

        Ok(AuthSuccess { success_message })
    }
}

/// Represents AUTH_CHALLENGE CQL response.
///
/// This message is sent by the server to challenge the client to provide
/// authentication credentials. The client must respond with an `AUTH_RESPONSE`
/// message containing the credentials.
#[derive(Debug)]
pub struct AuthChallenge {
    /// The challenge sent by the server, whose semantics depend on the
    /// authenticator being used.
    pub authenticate_message: Option<Vec<u8>>,
}

impl AuthChallenge {
    /// Deserializes an `AUTH_CHALLENGE` message from the provided buffer.
    pub fn deserialize(buf: &mut &[u8]) -> Result<Self, CqlAuthChallengeParseError> {
        let authenticate_message = types::read_bytes_opt(buf)
            .map_err(CqlAuthChallengeParseError::AuthMessageParseError)?
            .map(|b| b.to_owned());

        Ok(AuthChallenge {
            authenticate_message,
        })
    }
}
