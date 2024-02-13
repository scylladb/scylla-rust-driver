use crate::authentication::{
    AuthError, AuthInitialResponseAndSession, AuthenticatorProvider, AuthenticatorSession,
};
use crate::utils::futures::BoxedFuture;
use crate::utils::test_utils::unique_keyspace_name;
use bytes::{BufMut, BytesMut};
use std::sync::Arc;

#[tokio::test]
#[ignore]
async fn authenticate_superuser() {
    let uri = std::env::var("SCYLLA_URI").unwrap_or_else(|_| "127.0.0.1:9042".to_string());

    println!("Connecting to {} with cassandra superuser ...", uri);

    let session = crate::SessionBuilder::new()
        .known_node(uri)
        .user("cassandra", "cassandra")
        .build()
        .await
        .unwrap();
    let ks = unique_keyspace_name();

    session.query(format!("CREATE KEYSPACE IF NOT EXISTS {} WITH REPLICATION = {{'class' : 'NetworkTopologyStrategy', 'replication_factor' : 1}}", ks), &[]).await.unwrap();
    session.use_keyspace(ks, false).await.unwrap();
    session.query("DROP TABLE IF EXISTS t;", &[]).await.unwrap();

    println!("Ok.");
}

struct CustomAuthenticator;

impl AuthenticatorSession for CustomAuthenticator {
    fn evaluate_challenge<'a>(
        &'a mut self,
        _token: Option<&'a [u8]>,
    ) -> BoxedFuture<'_, Result<Option<Vec<u8>>, AuthError>> {
        Box::pin(async move { Err("Challenges are not expected".to_string()) })
    }

    fn success<'a>(
        &'a mut self,
        _token: Option<&'a [u8]>,
    ) -> BoxedFuture<'_, Result<(), AuthError>> {
        Box::pin(async move { Ok(()) })
    }
}

struct CustomAuthenticatorProvider;

impl AuthenticatorProvider for CustomAuthenticatorProvider {
    fn start_authentication_session<'a>(
        &'a self,
        _authenticator_name: &'a str,
    ) -> BoxedFuture<'_, Result<AuthInitialResponseAndSession, AuthError>> {
        Box::pin(async move {
            let mut response = BytesMut::new();
            let cred = "\0cassandra\0cassandra";

            response.put_slice(cred.as_bytes());

            Ok((
                Some(response.to_vec()),
                Box::new(CustomAuthenticator) as Box<dyn AuthenticatorSession>,
            ))
        })
    }
}

#[tokio::test]
#[ignore]
async fn custom_authentication() {
    let uri = std::env::var("SCYLLA_URI").unwrap_or_else(|_| "127.0.0.1:9042".to_string());

    println!("Connecting to {} with cassandra superuser ...", uri);

    let session = crate::SessionBuilder::new()
        .known_node(uri)
        .authenticator_provider(Arc::new(CustomAuthenticatorProvider))
        .build()
        .await
        .unwrap();
    let ks = unique_keyspace_name();

    session.query(format!("CREATE KEYSPACE IF NOT EXISTS {} WITH REPLICATION = {{'class' : 'NetworkTopologyStrategy', 'replication_factor' : 1}}", ks), &[]).await.unwrap();
    session.use_keyspace(ks, false).await.unwrap();
    session.query("DROP TABLE IF EXISTS t;", &[]).await.unwrap();

    println!("Ok.");
}
