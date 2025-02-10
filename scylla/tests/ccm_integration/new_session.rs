use assert_matches::assert_matches;
use scylla::client::execution_profile::ExecutionProfile;
use scylla::client::session_builder::SessionBuilder;
use scylla::errors::{ConnectionError, ConnectionPoolError, ExecutionError, NewSessionError};
use scylla::policies::retry::DefaultRetryPolicy;
use std::sync::Arc;

use crate::common::retry_policy::NoRetryPolicy;
use crate::common::utils::setup_tracing;

fn get_scylla() -> (String, String, String) {
    let uri1 = std::env::var("SCYLLA_URI1").unwrap_or_else(|_| "127.0.0.1:9042".to_string());
    let uri2 = std::env::var("SCYLLA_URI2").unwrap_or_else(|_| "127.0.0.2:9042".to_string());
    let uri3 = std::env::var("SCYLLA_URI3").unwrap_or_else(|_| "127.0.0.3:9042".to_string());

    (uri1, uri2, uri3)
}

#[cfg(not(scylla_cloud_tests))]
#[tokio::test]
async fn proceed_if_only_some_hostnames_are_invalid() {
    setup_tracing();
    // on purpose left without port
    let uri1 = "scylladbisthefastestdb.invalid".to_owned();
    // correctly provided port, but unknown domain
    let uri2 = "cassandrasuckssomuch.invalid:9042".to_owned();
    let uri3 = std::env::var("SCYLLA_URI3").unwrap_or_else(|_| "127.0.0.3:9042".to_string());

    let session = SessionBuilder::new()
        .known_nodes([uri1, uri2, uri3])
        .build()
        .await
        .unwrap();

    assert!(session
        .query_unpaged("SELECT host_id FROM system.local", &[])
        .await
        .is_ok());
}

#[cfg(not(scylla_cloud_tests))]
#[tokio::test]
async fn all_hostnames_invalid() {
    setup_tracing();
    let uri = "cassandrasuckssomuch.invalid:9042".to_owned();

    assert_matches!(
        SessionBuilder::new().known_node(uri).build().await,
        Err(NewSessionError::FailedToResolveAnyHostname(_))
    );
}

#[cfg(not(scylla_cloud_tests))]
#[tokio::test]
async fn no_nodes_available_reconnection_enabled() {
    setup_tracing();

    let execution_profile = ExecutionProfile::builder()
        .retry_policy(Arc::new(DefaultRetryPolicy))
        .build();

    assert!(SessionBuilder::new()
        .default_execution_profile_handle(execution_profile.into_handle())
        .build()
        .await
        .is_ok());
}

#[cfg(not(scylla_cloud_tests))]
#[tokio::test]
async fn no_nodes_available_reconnection_disabled() {
    setup_tracing();
    // TODO: Replace with CCM
    let (uri1, uri2, uri3) = get_scylla();

    let execution_profile = ExecutionProfile::builder()
        .retry_policy(Arc::new(NoRetryPolicy))
        .build();

    assert_matches!(
        SessionBuilder::new()
            .known_nodes([uri1, uri2, uri3])
            .default_execution_profile_handle(execution_profile.into_handle())
            .build()
            .await,
        Err(NewSessionError::FailedToResolveAnyHostname(_))
    );
}

#[cfg(not(scylla_cloud_tests))]
#[tokio::test]
async fn no_nodes_available_reconnection_enabled_nodes_coming_back() {
    setup_tracing();
    // TODO: Setup CCM

    let execution_profile = ExecutionProfile::builder()
        .retry_policy(Arc::new(DefaultRetryPolicy))
        .build();

    assert!(SessionBuilder::new()
        .default_execution_profile_handle(execution_profile.into_handle())
        .build()
        .await
        .is_ok());
}

#[cfg(not(scylla_cloud_tests))]
#[tokio::test]
async fn session_created_nodes_away_reconnection_enabled() {
    setup_tracing();

    let execution_profile = ExecutionProfile::builder()
        .retry_policy(Arc::new(DefaultRetryPolicy))
        .build();

    let _session = SessionBuilder::new()
        .default_execution_profile_handle(execution_profile.into_handle())
        .build()
        .await
        .unwrap();

    assert!(true);
}

#[cfg(not(scylla_cloud_tests))]
#[tokio::test]
async fn session_created_nodes_away_reconnection_disabled() {
    setup_tracing();

    // TODO: Replace with CCM
    let (uri1, uri2, uri3) = get_scylla();

    let execution_profile = ExecutionProfile::builder()
        .retry_policy(Arc::new(NoRetryPolicy))
        .build();

    let session = SessionBuilder::new()
        .known_nodes([uri1, uri2, uri3])
        .default_execution_profile_handle(execution_profile.into_handle())
        .build()
        .await
        .unwrap();

    // TODO: Everything should be fine
    assert!(session
        .query_unpaged("SELECT host_id FROM system.local", &[])
        .await
        .is_ok());

    // TODO: Stop the nodes

    // TODO: Check the connection -> fails to execute query
    assert_matches!(
        session
            .query_unpaged("SELECT host_id FROM system.local", &[])
            .await,
        Err(ExecutionError::ConnectionPoolError(
            ConnectionPoolError::Broken {
                last_connection_error: ConnectionError::BrokenConnection(_),
            }
        ))
    );

    assert!(true);
}
