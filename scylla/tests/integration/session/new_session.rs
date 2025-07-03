use std::net::Ipv4Addr;

use crate::utils::{create_new_session_builder, find_local_ip_for_destination, setup_tracing};

use assert_matches::assert_matches;
use futures::FutureExt as _;
use scylla::client::session_builder::SessionBuilder;
use scylla::errors::{ConnectionError, ConnectionPoolError, MetadataError, NewSessionError};
use tokio::net::TcpListener;

#[cfg_attr(scylla_cloud_tests, ignore)]
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
        .query_unpaged("SELECT host_id FROM system.local WHERE key='local'", &[])
        .await
        .unwrap();
}

#[cfg_attr(scylla_cloud_tests, ignore)]
#[tokio::test]
async fn all_hostnames_invalid() {
    setup_tracing();
    let uri = "cassandrasuckssomuch.invalid:9042".to_owned();

    assert_matches!(
        SessionBuilder::new().known_node(uri).build().await,
        Err(NewSessionError::FailedToResolveAnyHostname(_))
    );
}

/// This test assumes that there is no interface configured on 1.1.1.1 address.
/// This should be true for most standard setups.
/// It is based on https://github.com/scylladb/cpp-rust-driver/blob/v0.3.0/tests/src/integration/tests/test_control_connection.cpp#L203-L216.
#[tokio::test]
async fn invalid_local_ip_address() {
    setup_tracing();

    let session_builder =
        create_new_session_builder().local_ip_address(Some(Ipv4Addr::new(1, 1, 1, 1)));
    let session_result = session_builder.build().await;

    match session_result {
        Err(NewSessionError::MetadataError(MetadataError::ConnectionPoolError(
            ConnectionPoolError::Broken {
                last_connection_error: ConnectionError::IoError(err),
            },
        ))) => {
            assert_matches!(err.kind(), std::io::ErrorKind::AddrNotAvailable)
        }
        _ => panic!("Expected EADDRNOTAVAIL error"),
    }
}

#[tokio::test]
async fn valid_local_ip_address() {
    setup_tracing();

    // Create a dummy session to retrieve the address of one of the nodes.
    // We could obviously use SCYLLA_URI environment variable, but this would work
    // only for non-cloud cluster in CI.
    let dummy_session = create_new_session_builder().build().await.unwrap();

    let first_node_ip = dummy_session
        .get_cluster_state()
        .get_nodes_info()
        .first()
        .unwrap()
        .address
        .ip();

    let local_ip_address =
        find_local_ip_for_destination(first_node_ip).expect("Failed to find local IP");

    tracing::info!(
        "Found local IP address {} for destination address {}",
        local_ip_address,
        first_node_ip,
    );

    let session_builder = create_new_session_builder().local_ip_address(Some(local_ip_address));
    let session = session_builder.build().await.unwrap();

    session
        .query_unpaged("SELECT host_id FROM system.local WHERE key='local'", &[])
        .await
        .unwrap();
}

/// Make sure that Session::connect fails when the control connection fails to connect.
#[tokio::test]
async fn test_connection_failure() {
    setup_tracing();

    // Create a dummy server which immediately closes the connection.
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let (fut, _handle) = async move {
        loop {
            let _ = listener.accept().await;
        }
    }
    .remote_handle();
    tokio::spawn(fut);

    let res = SessionBuilder::new().known_node_addr(addr).build().await;
    match res {
        Ok(_) => panic!("Unexpected success"),
        Err(err) => println!("Connection error (it was expected): {err:?}"),
    }
}
