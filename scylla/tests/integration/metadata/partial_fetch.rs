//! Tests of partial metadata fetches: a server event announcing a change of one
//! metadata aspect must make the driver re-read that aspect alone, instead of
//! performing a full metadata fetch.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use scylla::client::session_builder::SessionBuilder;
use scylla_proxy::{
    Condition, ProxyError, Reaction, RequestOpcode, RequestReaction, RequestRule, RunningProxy,
    ShardAwareness, WorkerError,
};
use tokio::sync::mpsc;

use crate::utils::{
    inject_status_change_down, inject_status_change_up, inject_topology_change_new_node,
    setup_tracing, test_with_3_node_cluster, wait_until_all_nodes_are_connected,
};

/// The requests a partial topology fetch issues: one for `system.peers` and one
/// for `system.local`. A full fetch would add at least one per schema table
/// (`system_schema.keyspaces`, `.tables`, `.columns`, ...).
const TOPOLOGY_FETCH_REQUESTS: usize = 2;

/// A TOPOLOGY_CHANGE event can only have changed the peer list, so the driver
/// must react with a partial topology fetch: re-read `system.peers` and
/// `system.local`, and nothing else.
#[tokio::test]
async fn topology_change_event_triggers_only_a_topology_fetch() {
    assert_event_triggers_only_a_topology_fetch(inject_topology_change_new_node).await;
}

/// A STATUS_CHANGE UP event must trigger a partial topology fetch too.
///
/// A node's `rpc_address` can change while the node is neither added nor
/// removed - a restart under a new address, for instance - and no
/// TOPOLOGY_CHANGE announces that: the only events it produces are DOWN and UP.
/// Re-reading the peers on status changes is what keeps the driver from
/// addressing such a node by its stale address.
#[tokio::test]
async fn status_change_up_event_triggers_only_a_topology_fetch() {
    assert_event_triggers_only_a_topology_fetch(inject_status_change_up).await;
}

/// A STATUS_CHANGE DOWN event must trigger a partial topology fetch too, for
/// the reason given in [`status_change_up_event_triggers_only_a_topology_fetch`].
#[tokio::test]
async fn status_change_down_event_triggers_only_a_topology_fetch() {
    assert_event_triggers_only_a_topology_fetch(inject_status_change_down).await;
}

/// Injects one event with `inject_event` and requires the driver to react with
/// exactly one partial topology fetch: two metadata requests on the control
/// connection, and a new `ClusterState` that kept the schema metadata of the
/// previous one (the event cannot have affected the schema, and the fetch does
/// not re-read it).
async fn assert_event_triggers_only_a_topology_fetch(
    inject_event: impl FnOnce(&RunningProxy, SocketAddr),
) {
    setup_tracing();

    let res = test_with_3_node_cluster(
        ShardAwareness::QueryNode,
        |proxy_uris, translation_map, mut running_proxy| async move {
            let session = SessionBuilder::new()
                .known_node(proxy_uris[0].as_str())
                .address_translator(Arc::new(translation_map.clone()))
                // Far beyond the test duration: the only fetch on the
                // established control connection is the one the injected event
                // triggers.
                .cluster_metadata_refresh_interval(Duration::from_secs(10_000))
                .build()
                .await
                .unwrap();
            wait_until_all_nodes_are_connected(3, &session).await;

            let state_before_event = session.get_cluster_state();

            // Count the metadata requests of the control connection (only it
            // registers for events, hence the registration condition). The
            // driver prepares its metadata statements, so what identifies a
            // fetch is the number of EXECUTEs, not any query text. Installed
            // only now, so that the session's initial (full) fetch is not
            // counted.
            let (metadata_request_tx, mut metadata_request_rx) = mpsc::unbounded_channel();
            for node in running_proxy.running_nodes.iter_mut() {
                node.prepend_request_rules(vec![RequestRule(
                    Condition::ConnectionRegisteredAnyEvent
                        .and(Condition::RequestOpcode(RequestOpcode::Execute)),
                    RequestReaction::noop()
                        .with_feedback_when_performed(metadata_request_tx.clone()),
                )]);
            }

            // The address named by the event does not matter for the fetch: the
            // driver reacts by re-reading the whole peer list anyway. An address
            // of no known node also keeps the status hints from provoking
            // anything else (a keepalive or a pool refill).
            inject_event(&running_proxy, SocketAddr::from(([127, 0, 0, 1], 9042)));

            metadata_request_rx
                .recv()
                .await
                .expect("proxy feedback channel closed unexpectedly");

            // The fetched topology is published as a new `ClusterState`, which
            // must have kept the schema metadata of the previous one.
            let state_after_event = loop {
                let state = session.get_cluster_state();
                if !Arc::ptr_eq(&state, &state_before_event) {
                    break state;
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            };

            let mut requests = 1;
            while metadata_request_rx.try_recv().is_ok() {
                requests += 1;
            }
            assert_eq!(
                requests, TOPOLOGY_FETCH_REQUESTS,
                "the control connection issued {requests} metadata requests, so the event \
                triggered something other than a partial topology fetch"
            );

            assert_eq!(state_after_event.get_nodes_info().len(), 3);

            running_proxy
        },
    )
    .await;

    match res {
        Ok(()) => (),
        Err(ProxyError::Worker(WorkerError::DriverDisconnected(_))) => (),
        Err(err) => panic!("{}", err),
    }
}
