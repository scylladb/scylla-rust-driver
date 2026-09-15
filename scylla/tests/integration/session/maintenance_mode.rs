//! Tests of maintenance mode: a session pinned to a single node.

use crate::utils::setup_tracing;
use scylla::client::maintenance_mode::MaintenanceModeEndpoint;
use scylla::client::session::Session;
use scylla::client::session_builder::MaintenanceSessionBuilder;
use std::net::SocketAddr;

fn node_uri() -> SocketAddr {
    std::env::var("SCYLLA_URI")
        .unwrap_or_else(|_| "172.42.0.2:9042".to_string())
        .parse()
        .unwrap()
}

/// Asserts that the session knows of exactly one node, the one it was pinned to.
///
/// The test cluster has three nodes, so "exactly one" is only true if the peer
/// list was never read.
fn assert_pinned_to_one_node(session: &Session, expected: SocketAddr) {
    let cluster_state = session.get_cluster_state();
    let nodes = cluster_state.get_nodes_info();

    assert_eq!(nodes.len(), 1, "expected exactly one node, got {nodes:?}");
    assert_eq!(
        (nodes[0].address.ip(), nodes[0].address.port()),
        (expected.ip(), expected.port())
    );
}

/// Asserts that a maintenance mode session sends every request to the one node it
/// was pinned to, and that refreshing metadata does not discover the others.
#[tokio::test]
async fn maintenance_mode_talks_only_to_its_one_node() {
    setup_tracing();
    let node_addr = node_uri();

    // Note the absence of `.known_node()`: a maintenance mode session takes
    // exactly one endpoint, and takes it up front.
    let session = MaintenanceSessionBuilder::new(MaintenanceModeEndpoint::address(node_addr))
        .build()
        .await
        .unwrap();

    assert_pinned_to_one_node(&session, node_addr);

    // Every request is served by that node. Repeated, because a load balancing
    // policy that had more than one node to choose from would round-robin.
    for _ in 0..10 {
        let result = session
            .query_unpaged("SELECT key FROM system.local WHERE key = 'local'", &())
            .await
            .unwrap();

        assert_eq!(result.request_coordinator().connection_address(), node_addr);
    }

    // A metadata refresh must not discover the rest of the cluster.
    session.refresh_metadata().await.unwrap();
    assert_pinned_to_one_node(&session, node_addr);
}
