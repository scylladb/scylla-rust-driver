# Authentication

Driver supports both authentication by username and password and custom authentication defined by a user.
###### Important: The default authentication credentials are sent in plain text to the server. For this reason, it is highly recommended that this be used in conjunction with client-to-node encryption (SSL), or in a trusted network environment.

To use the default authentication, specify credentials using the `user` method in `SessionBuilder`:

```rust
# extern crate scylla;
# extern crate tokio;
# use std::error::Error;
# async fn check_only_compiles() -> Result<(), Box<dyn Error>> {
use scylla::{Session, SessionBuilder};

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
# use std::error::Error;
# use std::sync::Arc;
# use std::pin::Pin;
# use std::future::Future;
use bytes::{BufMut, BytesMut};
use scylla::authentication::{AuthError, AuthenticatorProvider, AuthenticatorSession};

struct CustomAuthenticator;

impl AuthenticatorSession for CustomAuthenticator {
    // to handle an authentication challenge initiated by the server.
    // The information contained in the token parameter is authentication protocol specific.
    // It may be NULL or empty. 
    fn evaluate_challenge<'a>(
        &'a mut self,
        token: Option<&'a [u8]>,
    ) -> Pin<Box<dyn Future<Output = Result<Option<Vec<u8>>, AuthError>> + Send + 'a>> {
        Box::pin(async move {
            Err("Challenges are not expected".to_string())
        })
    }

    // to handle the success phase of exchange. The token parameters contain information that may be used to finalize the request.
    fn success<'a>(
        &'a mut self,
        token: Option<&'a [u8]>
    ) -> Pin<Box<dyn Future<Output = Result<(), AuthError>> + Send + 'a>> {
        Box::pin(async move { Ok(()) })
    }
}

struct CustomAuthenticatorProvider;

impl AuthenticatorProvider for CustomAuthenticatorProvider {
    fn start_authentication_session<'a>(
        &'a self,
        authenticator_name: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<(Option<Vec<u8>>, Box<dyn AuthenticatorSession>), AuthError>> + Send + 'a>> {
        Box::pin(async move {
            let mut response = BytesMut::new();
            let cred = "\0cassandra\0cassandra";
            let cred_length = 20;

            response.put_i32(cred_length);
            response.put_slice(cred.as_bytes());

            Ok((
                Some(response.to_vec()),
                Box::new(CustomAuthenticator) as Box<dyn AuthenticatorSession>
            ))
        })
    }
}

async fn authentication_example() -> Result<(), Box<dyn Error>> {
    use scylla::{Session, SessionBuilder};

    let _session: Session = SessionBuilder::new()
        .known_node("127.0.0.1:9042")
        .authenticator_provider(Arc::new(CustomAuthenticatorProvider))
        .build()
        .await?;

    Ok(())
}
```
