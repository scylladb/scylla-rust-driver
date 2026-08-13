//! Regression tests: metadata fetches must keep draining server events while
//! in flight.
//!
//! The control connection delivers server events through a bounded channel,
//! and the connection's reader task blocks once that channel is full - which
//! also stops query responses from being processed. Whoever awaits a metadata
//! fetch must therefore keep draining events meanwhile: a fetch awaited
//! without draining deadlocks with the reader task (the fetch's response is
//! only processed once the events are drained, and events are only drained
//! once the fetch completes). This holds for the metadata worker's serving
//! loop as much as for the establishment paths (the initial fetch of session
//! build, and the re-establishment of a broken control connection), because
//! a control connection is registered for events from the moment it opens.
//!
//! Each test stalls metadata queries at the proxy, floods the control
//! connection with far more events than the channel holds while a fetch is
//! stalled, and requires the stalled operation to complete.

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

/// Upper bound for the stalled operation to complete. Each operation performs
/// a handful of [`METADATA_QUERY_DELAY`]-stalled round trips, so a healthy
/// driver stays well under this; only a deadlocked one reaches it.
const STALLED_OPERATION_TIMEOUT: Duration = Duration::from_secs(30);

/// Matches the metadata queries of a control connection (only the control
/// connection registers for events, hence the registration condition).
fn cc_metadata_query_condition() -> Condition {
    Condition::ConnectionRegisteredAnyEvent.and(
        Condition::RequestOpcode(RequestOpcode::Query)
            .or(Condition::RequestOpcode(RequestOpcode::Execute)),
    )
}

/// Floods the control connections with forged STATUS_CHANGE events. The
/// address named by the events does not matter: the deadlock under test sits
/// in event *draining*, which precedes any interpretation of the event.
fn flood_with_events(running_proxy: &scylla_proxy::RunningProxy) {
    let flood_addr = SocketAddr::from(([127, 0, 0, 1], 9042));
    for _ in 0..FLOODED_EVENTS {
        inject_status_change_down(running_proxy, flood_addr);
    }
}

/// The metadata worker must keep draining server events while a metadata
/// fetch on its established control connection is in flight.
///
/// The test stalls metadata queries on the control connection at the proxy,
/// requests a refresh, floods the control connection while the fetch is
/// stalled, and requires the refresh to complete.
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

            // Stall every metadata query on the control connection and
            // observe the stalled frames. Installed only now, so that the
            // session's initial metadata fetch is unaffected.
            let (fetch_started_tx, mut fetch_started_rx) = mpsc::unbounded_channel();
            for node in running_proxy.running_nodes.iter_mut() {
                node.prepend_request_rules(vec![RequestRule(
                    cc_metadata_query_condition(),
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

            flood_with_events(&running_proxy);

            // A worker that stops draining events during its fetch never sees
            // the fetch complete; the refresh then never resolves.
            tokio::time::timeout(STALLED_OPERATION_TIMEOUT, refresh)
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

/// Session build must drain server events while the initial metadata fetch is
/// in flight: the fetch runs before the metadata worker is spawned, on a
/// control connection that is already registered for events.
///
/// The test stalls metadata queries from the very start, floods the control
/// connection while the initial fetch is stalled, and requires
/// `SessionBuilder::build` to complete.
#[tokio::test]
async fn session_build_completes_despite_event_flood() {
    setup_tracing();

    let res = test_with_3_node_cluster(
        ShardAwareness::QueryNode,
        |proxy_uris, translation_map, mut running_proxy| async move {
            // Stall every metadata query on the control connection from the
            // very start: the session's initial fetch is the one under test.
            let (fetch_started_tx, mut fetch_started_rx) = mpsc::unbounded_channel();
            for node in running_proxy.running_nodes.iter_mut() {
                node.prepend_request_rules(vec![RequestRule(
                    cc_metadata_query_condition(),
                    RequestReaction::delay(METADATA_QUERY_DELAY)
                        .with_feedback_when_performed(fetch_started_tx.clone()),
                )]);
            }

            let build = tokio::spawn({
                let uri = proxy_uris[0].clone();
                let translation_map = translation_map.clone();
                async move {
                    SessionBuilder::new()
                        .known_node(uri)
                        .address_translator(Arc::new(translation_map))
                        .fetch_schema_metadata(false)
                        .build()
                        .await
                }
            });

            // The first stalled frame proves the initial fetch is in flight.
            fetch_started_rx
                .recv()
                .await
                .expect("proxy feedback channel closed unexpectedly");

            flood_with_events(&running_proxy);

            // A build that stops draining events during the initial fetch
            // never sees the fetch complete, and thus never returns.
            tokio::time::timeout(STALLED_OPERATION_TIMEOUT, build)
                .await
                .expect(
                    "session build deadlocked: the initial metadata fetch stopped draining \
                    server events while in flight",
                )
                .expect("session build task panicked")
                .expect("session build failed");

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

/// Re-establishing a broken control connection must drain server events while
/// the establishment's metadata fetch is in flight, for the same reason as in
/// [`session_build_completes_despite_event_flood`].
///
/// The test breaks the control connection at the proxy (the connection is
/// dropped on the next metadata query, i.e. once the requested refresh starts
/// fetching), which sends the metadata worker into re-establishment. The
/// re-establishment's metadata queries are stalled, and the flood is injected
/// while one is in flight. The refresh request survives the broken connection
/// and is answered from the re-established one, so its completion proves
/// recovery.
#[tokio::test]
async fn cc_reestablishment_completes_despite_event_flood() {
    setup_tracing();

    let res = test_with_3_node_cluster(
        ShardAwareness::QueryNode,
        |proxy_uris, translation_map, mut running_proxy| async move {
            let session = SessionBuilder::new()
                .known_node(proxy_uris[0].as_str())
                .address_translator(Arc::new(translation_map.clone()))
                .fetch_schema_metadata(false)
                // Far beyond the test duration, so that the only fetches are
                // those the test provokes.
                .cluster_metadata_refresh_interval(Duration::from_secs(10_000))
                .build()
                .await
                .unwrap();
            wait_until_all_nodes_are_connected(3, &session).await;

            // Stall every metadata query on the control connection at nodes
            // 1 and 2 and observe the stalled frames. Node 0 - the node
            // hosting the control connection (it is the sole initial contact
            // point) - instead drops the connection on every metadata query:
            // the refresh below thus breaks the control connection, and the
            // re-establishment cannot settle on node 0 either (its fetch is
            // dropped too, failing that candidate), so the new control
            // connection always lands on a node whose queries are stalled.
            // Only stalled frames produce feedback: metadata queries of the
            // broken connection are dropped, never stalled, so the first
            // feedback provably comes from a re-establishment fetch.
            let (fetch_started_tx, mut fetch_started_rx) = mpsc::unbounded_channel();
            for node in running_proxy.running_nodes[1..].iter_mut() {
                node.prepend_request_rules(vec![RequestRule(
                    cc_metadata_query_condition(),
                    RequestReaction::delay(METADATA_QUERY_DELAY)
                        .with_feedback_when_performed(fetch_started_tx.clone()),
                )]);
            }
            running_proxy.running_nodes[0].prepend_request_rules(vec![RequestRule(
                cc_metadata_query_condition(),
                RequestReaction::drop_connection(),
            )]);

            // Request a refresh. Its fetch breaks the control connection; the
            // request survives (the worker retries the refresh while
            // re-establishing), so its completion proves the re-established
            // fetch went through.
            let session = Arc::new(session);
            let refresh = tokio::spawn({
                let session = Arc::clone(&session);
                async move { session.refresh_metadata().await }
            });

            // The first stalled frame proves the re-establishment fetch is in
            // flight, on a fresh control connection registered for events.
            fetch_started_rx
                .recv()
                .await
                .expect("proxy feedback channel closed unexpectedly");

            flood_with_events(&running_proxy);

            // Re-establishment that stops draining events during its fetch
            // never completes, and the refresh request is never answered.
            tokio::time::timeout(STALLED_OPERATION_TIMEOUT, refresh)
                .await
                .expect(
                    "control connection re-establishment deadlocked: its metadata fetch \
                    stopped draining server events while in flight",
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
