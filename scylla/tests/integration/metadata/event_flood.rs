//! Regression test: the metadata worker must keep draining server events while
//! a metadata fetch is in flight.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use scylla::client::session_builder::SessionBuilder;
use scylla_proxy::{
    Condition, ProxyError, Reaction, RequestOpcode, RequestReaction, RequestRule, ShardAwareness,
    WorkerError,
};
use tokio::sync::mpsc;

use crate::utils::{
    inject_status_change_down, setup_tracing, test_with_3_node_cluster,
    wait_until_all_nodes_are_connected,
};

/// How long the proxy withholds each metadata query frame. The event flood is
/// injected within this window, i.e. while the fetch is in flight.
const METADATA_QUERY_DELAY: Duration = Duration::from_secs(1);

/// Comfortably more events than the control connection's bounded event channel
/// (32 entries as of writing) can hold.
const FLOODED_EVENTS: usize = 100;

/// Upper bound for the stalled refresh to complete. The refresh performs a
/// handful of [`METADATA_QUERY_DELAY`]-stalled round trips, so a healthy
/// driver stays well under this; only a deadlocked one reaches it.
const REFRESH_TIMEOUT: Duration = Duration::from_secs(30);

/// The control connection delivers server events through a bounded channel,
/// and the connection's reader task blocks once that channel is full - which
/// also stops query responses from being processed. The metadata worker must
/// therefore keep draining events while a metadata fetch is in flight: a
/// worker that awaited the fetch without draining would deadlock with it (the
/// fetch's response is only processed once the events are drained, and the
/// worker only drains events once the fetch completes).
///
/// The test stalls metadata queries on the control connection at the proxy,
/// requests a refresh, floods the control connection with far more events
/// than the channel holds while the fetch is stalled, and requires the
/// refresh to complete.
#[tokio::test]
async fn metadata_fetch_completes_despite_event_flood() {
    setup_tracing();

    let res = test_with_3_node_cluster(
        ShardAwareness::QueryNode,
        |proxy_uris, translation_map, mut running_proxy| async move {
            let session = SessionBuilder::new()
                .known_node(proxy_uris[0].as_str())
                .address_translator(Arc::new(translation_map.clone()))
                .fetch_schema_metadata(false)
                // Far beyond the test duration: the only full fetch on the
                // established control connection is the explicit one below.
                .cluster_metadata_refresh_interval(Duration::from_secs(10_000))
                .build()
                .await
                .unwrap();
            wait_until_all_nodes_are_connected(3, &session).await;

            // Stall every metadata query on the control connection (only the
            // control connection registers for events, hence the condition)
            // and observe the stalled frames. Installed only now, so that the
            // session's initial metadata fetch is unaffected.
            let (fetch_started_tx, mut fetch_started_rx) = mpsc::unbounded_channel();
            let metadata_query_condition = Condition::ConnectionRegisteredAnyEvent.and(
                Condition::RequestOpcode(RequestOpcode::Query)
                    .or(Condition::RequestOpcode(RequestOpcode::Execute)),
            );
            for node in running_proxy.running_nodes.iter_mut() {
                node.prepend_request_rules(vec![RequestRule(
                    metadata_query_condition.clone(),
                    RequestReaction::delay(METADATA_QUERY_DELAY)
                        .with_feedback_when_performed(fetch_started_tx.clone()),
                )]);
            }

            // Request a refresh. It cannot complete before METADATA_QUERY_DELAY
            // elapses, which leaves the flood below plenty of time to land
            // while the fetch is in flight.
            let session = Arc::new(session);
            let refresh = tokio::spawn({
                let session = Arc::clone(&session);
                async move { session.refresh_metadata().await }
            });

            // The first stalled frame proves the fetch is in flight (feedback
            // is emitted before the proxy starts delaying the frame).
            fetch_started_rx
                .recv()
                .await
                .expect("proxy feedback channel closed unexpectedly");

            // Flood the control connection. The address named by the events
            // does not matter: the deadlock under test sits in event
            // *draining*, which precedes any interpretation of the event.
            let flood_addr = SocketAddr::from(([127, 0, 0, 1], 9042));
            for _ in 0..FLOODED_EVENTS {
                inject_status_change_down(&running_proxy, flood_addr);
            }

            // A worker that stops draining events during its fetch never sees
            // the fetch complete; the refresh then never resolves.
            tokio::time::timeout(REFRESH_TIMEOUT, refresh)
                .await
                .expect(
                    "metadata refresh deadlocked: the metadata worker stopped draining \
                    server events while its metadata fetch was in flight",
                )
                .expect("refresh task panicked")
                .expect("metadata refresh failed");

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
