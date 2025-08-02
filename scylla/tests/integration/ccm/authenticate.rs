use std::sync::Arc;

use async_trait::async_trait;
use bytes::{BufMut, BytesMut};
use scylla::authentication::{AuthenticatorProvider, AuthenticatorSession};
use scylla::errors::AuthError;
use tokio::sync::Mutex;

use crate::ccm::lib::cluster::{Cluster, ClusterOptions};
use crate::ccm::lib::{run_ccm_test_with_configuration, CLUSTER_VERSION};
use crate::utils::{setup_tracing, unique_keyspace_name, PerformDDL};

fn cluster_1_node() -> ClusterOptions {
    ClusterOptions {
        name: "cluster_auth_1_node".to_string(),
        version: CLUSTER_VERSION.clone(),
        nodes_per_dc: vec![1],
        ..ClusterOptions::default()
    }
}

async fn run_ccm_auth_test_cluster_one_node<T, TFut>(test: T)
where
    T: FnOnce(Arc<Mutex<Cluster>>) -> TFut,
    TFut: std::future::Future<Output = ()>,
{
    run_ccm_test_with_configuration(
        cluster_1_node,
        |mut cluster| async move {
            cluster
                .enable_password_authentication()
                .await
                .expect("Failed to enable password authenticator");
            cluster
        },
        test,
    )
    .await
}

#[tokio::test]
#[cfg_attr(not(ccm_tests), ignore)]
async fn authenticate_superuser_cluster_one_node() {
    setup_tracing();
    async fn test(cluster: Arc<Mutex<Cluster>>) {
        let cluster = cluster.lock().await;

        tracing::info!(
            "Connecting to {:?} with cassandra superuser...",
            cluster.nodes().get_contact_endpoints().await
        );

        let session = cluster
            .make_session_builder()
            .await
            .user("cassandra", "cassandra")
            .build()
            .await
            .unwrap();
        let ks = unique_keyspace_name();

        session.ddl(format!("CREATE KEYSPACE IF NOT EXISTS {ks} WITH REPLICATION = {{'class' : 'NetworkTopologyStrategy', 'replication_factor' : 1}}")).await.unwrap();
        session.use_keyspace(&ks, false).await.unwrap();
        session.ddl("DROP TABLE IF EXISTS t;").await.unwrap();
        session.ddl(format!("DROP KEYSPACE {ks}")).await.unwrap();

        tracing::info!("Ok.");
    }

    run_ccm_auth_test_cluster_one_node(test).await
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
#[cfg_attr(not(ccm_tests), ignore)]
async fn custom_authentication_cluster_one_node() {
    setup_tracing();
    async fn test(cluster: Arc<Mutex<Cluster>>) {
        let cluster = cluster.lock().await;

        tracing::info!(
            "Connecting to {:?} with custom authenticator as cassandra superuser...",
            cluster.nodes().get_contact_endpoints().await
        );

        let session = cluster
            .make_session_builder()
            .await
            .authenticator_provider(Arc::new(CustomAuthenticatorProvider))
            .build()
            .await
            .unwrap();
        let ks = unique_keyspace_name();

        session.ddl(format!("CREATE KEYSPACE IF NOT EXISTS {ks} WITH REPLICATION = {{'class' : 'NetworkTopologyStrategy', 'replication_factor' : 1}}")).await.unwrap();
        session.use_keyspace(&ks, false).await.unwrap();
        session.ddl("DROP TABLE IF EXISTS t;").await.unwrap();
        session.ddl(format!("DROP KEYSPACE {ks}")).await.unwrap();

        tracing::info!("Ok.");
    }

    run_ccm_auth_test_cluster_one_node(test).await
}
