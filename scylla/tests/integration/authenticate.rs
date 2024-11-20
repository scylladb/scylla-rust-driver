use crate::utils::{setup_tracing, unique_keyspace_name};
use async_trait::async_trait;
use bytes::{BufMut, BytesMut};
use scylla::authentication::{AuthError, AuthenticatorProvider, AuthenticatorSession};
use std::sync::Arc;

#[tokio::test]
#[ignore]
async fn authenticate_superuser() {
    setup_tracing();
    let uri = std::env::var("SCYLLA_URI").unwrap_or_else(|_| "127.0.0.1:9042".to_string());

    println!("Connecting to {} with cassandra superuser ...", uri);

    let session = scylla::SessionBuilder::new()
        .known_node(uri)
        .user("cassandra", "cassandra")
        .build()
        .await
        .unwrap();
    let ks = unique_keyspace_name();

    session.query_unpaged(format!("CREATE KEYSPACE IF NOT EXISTS {} WITH REPLICATION = {{'class' : 'NetworkTopologyStrategy', 'replication_factor' : 1}}", ks), &[]).await.unwrap();
    session.use_keyspace(ks, false).await.unwrap();
    session
        .query_unpaged("DROP TABLE IF EXISTS t;", &[])
        .await
        .unwrap();

    println!("Ok.");
}

struct CustomAuthenticator;

#[async_trait]
impl AuthenticatorSession for CustomAuthenticator {
    async fn evaluate_challenge(
        &mut self,
        _token: Option<&[u8]>,
    ) -> Result<Option<Vec<u8>>, AuthError> {
        Err("Challenges are not expected".to_string())
    }

    async fn success(&mut self, _token: Option<&[u8]>) -> Result<(), AuthError> {
        Ok(())
    }
}

struct CustomAuthenticatorProvider;

#[async_trait]
impl AuthenticatorProvider for CustomAuthenticatorProvider {
    async fn start_authentication_session(
        &self,
        _authenticator_name: &str,
    ) -> Result<(Option<Vec<u8>>, Box<dyn AuthenticatorSession>), AuthError> {
        let mut response = BytesMut::new();
        let cred = "\0cassandra\0cassandra";

        response.put_slice(cred.as_bytes());

        Ok((Some(response.to_vec()), Box::new(CustomAuthenticator)))
    }
}

#[tokio::test]
#[ignore]
async fn custom_authentication() {
    setup_tracing();
    let uri = std::env::var("SCYLLA_URI").unwrap_or_else(|_| "127.0.0.1:9042".to_string());

    println!("Connecting to {} with cassandra superuser ...", uri);

    let session = scylla::SessionBuilder::new()
        .known_node(uri)
        .authenticator_provider(Arc::new(CustomAuthenticatorProvider))
        .build()
        .await
        .unwrap();
    let ks = unique_keyspace_name();

    session.query_unpaged(format!("CREATE KEYSPACE IF NOT EXISTS {} WITH REPLICATION = {{'class' : 'NetworkTopologyStrategy', 'replication_factor' : 1}}", ks), &[]).await.unwrap();
    session.use_keyspace(ks, false).await.unwrap();
    session
        .query_unpaged("DROP TABLE IF EXISTS t;", &[])
        .await
        .unwrap();

    println!("Ok.");
}
