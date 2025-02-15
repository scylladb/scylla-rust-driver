# Authentication

Driver supports both authentication by username and password and custom authentication defined by a user.
###### Important: The default authentication credentials are sent in plain text to the server. For this reason, it is highly recommended that this be used in conjunction with client-to-node encryption (SSL), or in a trusted network environment.

To use the default authentication, specify credentials using the `user` method in `SessionBuilder`:

```rust
# extern crate scylla;
# extern crate tokio;
# use std::error::Error;
# async fn check_only_compiles() -> Result<(), Box<dyn Error>> {
use scylla::client::session::Session;
use scylla::client::session_builder::SessionBuilder;

let session: Session = SessionBuilder::new()
    .known_node("127.0.0.1:9042")
    .user("myusername", "mypassword")
    .build()
    .await?;

# Ok(())
# }
```

### Custom Authentication

A custom authentication is defined by implementing the `AuthenticatorSession`.
An `AuthenticatorSession` instance is created per session, so it is also necessary to define a `AuthenticatorProvider` for it.
Finally, to make use of the custom authentication, use the `authenticator_provider` method in `SessionBuilder`:

```rust
# extern crate scylla;
# extern crate scylla_cql;
# extern crate tokio;
# extern crate bytes;
# extern crate async_trait;
# use std::error::Error;
# use std::sync::Arc;
use bytes::{BufMut, BytesMut};
use async_trait::async_trait;
use scylla::authentication::{AuthError, AuthenticatorProvider, AuthenticatorSession};

struct CustomAuthenticator;

#[async_trait]
impl AuthenticatorSession for CustomAuthenticator {
    // to handle an authentication challenge initiated by the server.
    // The information contained in the token parameter is authentication protocol specific.
    // It may be NULL or empty. 
    async fn evaluate_challenge(
        &mut self,
        _token: Option<&[u8]>,
    ) -> Result<Option<Vec<u8>>, AuthError> {
        Err("Challenges are not expected".to_string())
    }

    // to handle the success phase of exchange. The token parameters contain information that may be used to finalize the request.
    async fn success(&mut self, _token: Option<&[u8]>) -> Result<(), AuthError> {
        Ok(())
    }
}

struct CustomAuthenticatorProvider;

#[async_trait]
impl AuthenticatorProvider for CustomAuthenticatorProvider {
    async fn start_authentication_session(
        &self,
        _name: &str,
    ) -> Result<(Option<Vec<u8>>, Box<dyn AuthenticatorSession>), AuthError> {
        let mut response = BytesMut::new();
        let cred = "\0cassandra\0cassandra";
        let cred_length = 20;

        response.put_i32(cred_length);
        response.put_slice(cred.as_bytes());

        Ok((Some(response.to_vec()), Box::new(CustomAuthenticator)))
    }
}

async fn authentication_example() -> Result<(), Box<dyn Error>> {
    use scylla::client::session::Session;
    use scylla::client::session_builder::SessionBuilder;

    let _session: Session = SessionBuilder::new()
        .known_node("127.0.0.1:9042")
        .authenticator_provider(Arc::new(CustomAuthenticatorProvider))
        .build()
        .await?;

    Ok(())
}
```
