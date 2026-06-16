//! CCM test verifying that the control connection still appends a
//! `USING TIMEOUT` directive to its metadata queries even when ScyllaDB is
//! started with `allow_shard_aware_drivers: false`.
//!
//! Rationale: the driver only appends `USING TIMEOUT` (a ScyllaDB-only
//! extension) to control-connection queries when it recognizes the peer as a
//! ScyllaDB node. That recognition (`Connection::is_to_scylladb`) is based on
//! several advertised features, one of which is shard awareness. With
//! `allow_shard_aware_drivers: false`, ScyllaDB stops advertising shard
//! awareness, so this test guards against a regression where the driver would
//! then fail to recognize the node as ScyllaDB and stop sending `USING
//! TIMEOUT` (the other ScyllaDB feature flags should keep `is_to_scylladb`
//! returning `true`).

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use scylla::client::session::Session;
use scylla::client::session_builder::SessionBuilder;
use scylla_proxy::{
    Condition, Node, Proxy, ProxyError, Reaction as _, RequestOpcode, RequestReaction, RequestRule,
    ShardAwareness, WorkerError,
};
use tokio::sync::mpsc;
use tokio::sync::mpsc::error::TryRecvError;
use tracing::info;

use crate::ccm::lib::cluster::{Cluster, ClusterOptions};
use crate::ccm::lib::{CLUSTER_VERSION, run_ccm_test_with_configuration};
use crate::utils::setup_tracing;

/// Custom server-side metadata timeout we force the driver to use, so that the
/// `USING TIMEOUT` clause is deterministic and easy to assert on.
const CUSTOM_TIMEOUT: Duration = Duration::from_millis(2137);

fn cluster_1_node() -> ClusterOptions {
    ClusterOptions {
        name: "using_timeout_no_shard_aware".to_string(),
        version: CLUSTER_VERSION.clone(),
        nodes_per_dc: vec![1],
        ..ClusterOptions::default()
    }
}

fn contains_subslice(slice: &[u8], subslice: &[u8]) -> bool {
    slice
        .windows(subslice.len())
        .any(|window| window == subslice)
}

/// Verifies that the control connection keeps sending `USING TIMEOUT` in its
/// metadata queries when ScyllaDB does not advertise shard awareness.
#[tokio::test]
async fn test_control_connection_using_timeout_without_shard_awareness() {
    setup_tracing();

    async fn test(cluster: &mut Cluster) {
        // Put a proxy in front of the single node so we can inspect the
        // wire-level frames sent on the control connection.
        let node = cluster.nodes().iter().next().expect("No nodes in cluster");
        let real_addr = SocketAddr::new(node.broadcast_rpc_address(), node.native_transport_port());
        let proxy_addr = SocketAddr::new(scylla_proxy::get_exclusive_local_address(), 9042);

        let proxy = Proxy::new([Node::builder()
            .real_address(real_addr)
            .proxy_address(proxy_addr)
            // Faithfully forward whatever the node advertises about shard
            // awareness. With `allow_shard_aware_drivers: false` the node
            // advertises nothing, so QueryNode transparently falls back to
            // shard-unaware behavior.
            .shard_awareness(ShardAwareness::QueryNode)
            .build()]);

        let translation_map = proxy.translation_map();
        let mut running_proxy = proxy.run().await.unwrap();

        // Feedback rule: capture every QUERY/PREPARE issued on the control
        // connection (i.e. the connection that registered for events).
        let (feedback_tx, mut feedback_rx) = mpsc::unbounded_channel();
        running_proxy.running_nodes[0].change_request_rules(Some(vec![RequestRule(
            Condition::and(
                Condition::ConnectionRegisteredAnyEvent,
                Condition::or(
                    Condition::RequestOpcode(RequestOpcode::Query),
                    Condition::RequestOpcode(RequestOpcode::Prepare),
                ),
            ),
            RequestReaction::noop().with_feedback_when_performed(feedback_tx),
        )]));

        // Build the session through the proxy, forcing a known custom
        // server-side metadata timeout. The session build performs the initial
        // metadata fetch, which prepares the metadata queries on the control
        // connection (with the `USING TIMEOUT` clause baked into the PREPARE).
        let session: Session = SessionBuilder::new()
            .known_node(format!("{}", proxy_addr))
            .address_translator(Arc::new(translation_map))
            .metadata_request_serverside_timeout(CUSTOM_TIMEOUT)
            .build()
            .await
            .unwrap();

        // Sanity check: confirm that `allow_shard_aware_drivers: false` took
        // effect, i.e. the driver does NOT see the node as shard-aware. This is
        // the precondition that makes the test meaningful: despite the missing
        // shard awareness, `USING TIMEOUT` must still be sent.
        let sharder = session
            .get_cluster_state()
            .get_nodes_info()
            .first()
            .and_then(|node| node.sharder());
        assert!(
            sharder.is_none(),
            "Expected node to NOT be shard-aware (allow_shard_aware_drivers: false), \
             but a sharder was found: {sharder:?}"
        );

        // Stop the rules so that no further frames race into the channel after
        // we start draining it.
        running_proxy.turn_off_rules();

        // Every metadata query issued on the control connection must carry the
        // custom `USING TIMEOUT` clause.
        let expected = format!("USING TIMEOUT {}ms", CUSTOM_TIMEOUT.as_millis());
        let mut total = 0usize;
        loop {
            match feedback_rx.try_recv() {
                Ok((frame, _shard)) => {
                    total += 1;
                    assert!(
                        contains_subslice(&frame.body, expected.as_bytes()),
                        "Control connection query did not contain `{}`. Frame body: {:?}",
                        expected,
                        frame.body,
                    );
                }
                Err(TryRecvError::Empty) => break,
                Err(TryRecvError::Disconnected) => break,
            }
        }

        assert!(
            total > 0,
            "No control connection QUERY/PREPARE frames were captured — \
             cannot verify that USING TIMEOUT is sent"
        );
        info!(
            "Verified {} control connection metadata queries all contained `{}`",
            total, expected
        );

        match running_proxy.finish().await {
            Ok(()) => (),
            Err(ProxyError::Worker(WorkerError::DriverDisconnected(_))) => (),
            Err(err) => panic!("{}", err),
        }
    }

    run_ccm_test_with_configuration(
        cluster_1_node,
        |mut cluster: Cluster| async move {
            cluster
                .updateconf([("enable_shard_aware_drivers", "false")])
                .await
                .expect("Failed to set allow_shard_aware_drivers: false");
            cluster
        },
        test,
    )
    .await;
}
