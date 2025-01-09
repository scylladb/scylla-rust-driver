use async_trait::async_trait;
use bytes::{BufMut, BytesMut};

pub use crate::frame::Authenticator;

/// Type to represent an authentication error message.
pub type AuthError = String;

/// Trait used to represent a user-defined custom authentication.
#[async_trait]
pub trait AuthenticatorSession: Send + Sync {
    /// To handle an authentication challenge initiated by the server.
    /// The information contained in the token parameter is authentication protocol specific.
    /// It may be NULL or empty.
    async fn evaluate_challenge(
        &mut self,
        token: Option<&[u8]>,
    ) -> Result<Option<Vec<u8>>, AuthError>;

    /// To handle the success phase of exchange.
    /// The token parameters contain information that may be used to finalize the request.
    async fn success(&mut self, token: Option<&[u8]>) -> Result<(), AuthError>;
}

/// Trait used to represent a factory of [`AuthenticatorSession`] instances.
/// A new [`AuthenticatorSession`] instance will be created for each session.
///
/// The custom authenticator can be set using SessionBuilder::authenticator_provider method.  
///
/// Default: [`PlainTextAuthenticator`] is the default authenticator which requires username and
/// password. It can be set by using SessionBuilder::user(\"user\", \"pass\") method.
#[async_trait]
pub trait AuthenticatorProvider: Sync + Send {
    /// A pair of initial response and boxed [`AuthenticatorSession`]
    /// should be returned if authentication is required by the server.
    async fn start_authentication_session(
        &self,
        authenticator_name: &str,
    ) -> Result<(Option<Vec<u8>>, Box<dyn AuthenticatorSession>), AuthError>;
}

struct PlainTextAuthenticatorSession;

#[async_trait]
impl AuthenticatorSession for PlainTextAuthenticatorSession {
    async fn evaluate_challenge(
        &mut self,
        _token: Option<&[u8]>,
    ) -> Result<Option<Vec<u8>>, AuthError> {
        Err("Challenges are not expected during PlainTextAuthentication".to_string())
    }

    async fn success(&mut self, _token: Option<&[u8]>) -> Result<(), AuthError> {
        Ok(())
    }
}

/// Default authenticator provider that requires username and password if authentication is required.
pub struct PlainTextAuthenticator {
    username: String,
    password: String,
}

impl PlainTextAuthenticator {
    /// Creates new [`PlainTextAuthenticator`] instance with provided username and password.
    pub fn new(username: String, password: String) -> Self {
        PlainTextAuthenticator { username, password }
    }
}

#[async_trait]
impl AuthenticatorProvider for PlainTextAuthenticator {
    async fn start_authentication_session(
        &self,
        _authenticator_name: &str,
    ) -> Result<(Option<Vec<u8>>, Box<dyn AuthenticatorSession>), AuthError> {
        let mut response = BytesMut::new();
        let username_as_bytes = self.username.as_bytes();
        let password_as_bytes = self.password.as_bytes();

        response.put_u8(0);
        response.put_slice(username_as_bytes);
        response.put_u8(0);
        response.put_slice(password_as_bytes);

        Ok((
            Some(response.to_vec()),
            Box::new(PlainTextAuthenticatorSession),
        ))
    }
}
