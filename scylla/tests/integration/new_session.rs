use crate::utils::setup_tracing;

use assert_matches::assert_matches;
use scylla::errors::NewSessionError;
use scylla::SessionBuilder;

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
    session
        .query_unpaged("SELECT host_id FROM system.local", &[])
        .await
        .unwrap();
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
