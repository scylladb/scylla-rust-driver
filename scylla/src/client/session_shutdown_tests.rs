//! Tests of [`Session::shutdown`], on a dry-mode proxy.

use std::net::SocketAddr;
use std::pin::pin;
use std::sync::Arc;
use std::time::Duration;

use assert_matches::assert_matches;
use futures::StreamExt;
use scylla_proxy::{
    Condition, Proxy, Reaction as _, RequestFrame, RequestOpcode, RequestReaction, RequestRule,
    ResponseFrame, RunningProxy,
};
use tokio::task::JoinHandle;

use super::Session;
use crate::client::session_builder::SessionBuilder;
use crate::cluster::ClusterState;
use crate::errors::{ConnectionPoolError, ExecutionError, NextPageError, NextRowError};
use crate::frame::response::result::{ColumnSpec, ColumnType, NativeType, TableSpec};
use crate::response::query_result::QueryResult;
use crate::test_utils::{dry_mode_handshake_rules, setup_tracing};

/// The proxy delays the response to this query by `SLOW_RESPONSE_DELAY`.
const SLOW_QUERY: &str = "SELECT a FROM ks.slow";
const SLOW_RESPONSE_DELAY: Duration = Duration::from_millis(300);

/// The proxy answers this query with pages that never end.
const PAGED_QUERY: &str = "SELECT a FROM ks.paged";

/// Bounds the waits for the shutdown in tests where the shutdown must not wait.
const TEST_TIMEOUT: Duration = Duration::from_secs(1);

fn rows_response(frame: RequestFrame, paging_state: Option<&[u8]>) -> ResponseFrame {
    let col_specs = [ColumnSpec::borrowed(
        "a",
        ColumnType::Native(NativeType::Int),
        TableSpec::borrowed("ks", "t"),
    )];
    ResponseFrame::forged_rows(frame.params, &col_specs, [(1_i32,)], paging_state).unwrap()
}

fn query_rule(query: &str, reaction: RequestReaction) -> RequestRule {
    RequestRule(
        Condition::RequestOpcode(RequestOpcode::Query).and(Condition::BodyContainsCaseSensitive(
            query.as_bytes().into(),
        )),
        reaction,
    )
}

/// Starts a dry-mode proxy, and connects a session to it.
async fn connect() -> (Arc<Session>, RunningProxy) {
    setup_tracing();
    let proxy_addr = SocketAddr::new(scylla_proxy::get_exclusive_local_address(), 9042);

    let mut rules = dry_mode_handshake_rules();
    rules.extend([
        query_rule(
            SLOW_QUERY,
            RequestReaction::forge_response_with_delay(
                SLOW_RESPONSE_DELAY,
                Arc::new(|frame: RequestFrame| rows_response(frame, None)),
            ),
        ),
        query_rule(
            PAGED_QUERY,
            RequestReaction::forge_response(Arc::new(|frame: RequestFrame| {
                rows_response(frame, Some(b"next page"))
            })),
        ),
        // For example, the metadata queries of the control connection.
        RequestRule(
            Condition::any([
                Condition::RequestOpcode(RequestOpcode::Query),
                Condition::RequestOpcode(RequestOpcode::Prepare),
            ]),
            RequestReaction::forge().server_error(),
        ),
    ]);

    let proxy = Proxy::builder()
        .with_node(
            scylla_proxy::Node::builder()
                .proxy_address(proxy_addr)
                .request_rules(rules)
                .build_dry_mode(),
        )
        .build()
        .run()
        .await
        .unwrap();

    let session = SessionBuilder::new()
        .known_node_addr(proxy_addr)
        .build()
        .await
        .unwrap();

    (Arc::new(session), proxy)
}

/// Starts `SLOW_QUERY` in a new task, and waits until the session counts it as in flight.
async fn start_slow_request(
    session: &Arc<Session>,
) -> JoinHandle<Result<QueryResult, ExecutionError>> {
    let request = tokio::spawn({
        let session = Arc::clone(session);
        async move { session.query_unpaged(SLOW_QUERY, ()).await }
    });
    // Without the timeout, a request that is never admitted hangs the test.
    tokio::time::timeout(TEST_TIMEOUT, async {
        while session.in_flight.count() == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    request
}

/// Checks that no background task is left, so all sockets are closed, and
/// that the pools in `cluster_state` released their connections.
fn assert_closed(session: &Session, cluster_state: &ClusterState) {
    assert_eq!(session.background_tasks.len(), 0);
    assert_pools_closed(cluster_state);
    // The pools also remove their connections from the metrics.
    #[cfg(feature = "metrics")]
    assert_eq!(session.get_metrics().get_total_connections(), 0);
}

fn assert_pools_closed(cluster_state: &ClusterState) {
    for node in cluster_state.get_nodes_info() {
        assert!(!node.is_connected());
        assert_matches!(
            node.get_working_connections().err(),
            Some(ConnectionPoolError::Closed(_))
        );
    }
}

#[tokio::test]
async fn shutdown_waits_for_requests_in_flight() {
    let (session, _proxy) = connect().await;
    let mut slow_request = start_slow_request(&session).await;

    // The first poll closes the session to new requests.
    let mut shutdown = pin!(session.shutdown());
    assert!(futures::poll!(shutdown.as_mut()).is_pending());
    assert_matches!(
        session.query_unpaged(SLOW_QUERY, ()).await,
        Err(ExecutionError::SessionShutDown(_))
    );

    // The admitted request completes normally, before the shutdown does.
    tokio::select! {
        biased;
        result = &mut slow_request => {
            result.unwrap().unwrap();
        }
        () = &mut shutdown => panic!("the shutdown did not wait for a request in flight"),
    }
    tokio::time::timeout(TEST_TIMEOUT, shutdown).await.unwrap();
}

#[tokio::test]
async fn shutdown_completes_when_called_again_after_a_timeout() {
    let (session, _proxy) = connect().await;
    let slow_request = start_slow_request(&session).await;

    // The proxy delays the response for longer than this timeout.
    assert!(
        tokio::time::timeout(Duration::from_millis(10), session.shutdown())
            .await
            .is_err()
    );
    assert_matches!(
        session.query_unpaged(SLOW_QUERY, ()).await,
        Err(ExecutionError::SessionShutDown(_))
    );

    // Nobody waits for the shutdown, so the connections stay open.
    slow_request.await.unwrap().unwrap();
    assert!(session.background_tasks.len() > 0);

    tokio::time::timeout(TEST_TIMEOUT, session.shutdown())
        .await
        .unwrap();
    assert_closed(&session, &session.get_cluster_state());
}

#[tokio::test]
async fn shutdown_timeout_waits_for_requests_that_complete_in_time() {
    let (session, _proxy) = connect().await;
    let slow_request = start_slow_request(&session).await;

    // The timeout is longer than the delay of the proxy.
    assert!(session.shutdown_timeout(TEST_TIMEOUT).await);
    slow_request.await.unwrap().unwrap();
    assert_closed(&session, &session.get_cluster_state());
}

#[tokio::test]
async fn shutdown_timeout_forces_the_shutdown_when_requests_are_slow() {
    let (session, _proxy) = connect().await;
    let slow_request = start_slow_request(&session).await;

    // The timeout is shorter than the delay of the proxy.
    let completed_in_time = tokio::time::timeout(
        SLOW_RESPONSE_DELAY / 2,
        session.shutdown_timeout(SLOW_RESPONSE_DELAY / 6),
    )
    .await
    .unwrap();
    assert!(!completed_in_time);
    assert_closed(&session, &session.get_cluster_state());
    assert!(slow_request.await.unwrap().is_err());
}

#[tokio::test]
async fn force_shutdown_does_not_wait_for_requests_in_flight() {
    let (session, _proxy) = connect().await;
    let slow_request = start_slow_request(&session).await;

    // The proxy delays the response for longer than this timeout.
    tokio::time::timeout(SLOW_RESPONSE_DELAY / 2, session.force_shutdown())
        .await
        .unwrap();
    assert_closed(&session, &session.get_cluster_state());

    // The request in flight lost its connection, so it fails at once. It does
    // not wait for the proxy, or for its request timeout.
    let result = tokio::time::timeout(SLOW_RESPONSE_DELAY / 2, slow_request)
        .await
        .unwrap();
    assert!(result.unwrap().is_err());
    assert_matches!(
        session.query_unpaged(SLOW_QUERY, ()).await,
        Err(ExecutionError::SessionShutDown(_))
    );
}

#[tokio::test]
async fn force_shutdown_ends_a_graceful_shutdown_in_progress() {
    let (session, _proxy) = connect().await;
    let slow_request = start_slow_request(&session).await;

    // The graceful shutdown waits for the slow request.
    let mut graceful = pin!(session.shutdown());
    assert!(futures::poll!(graceful.as_mut()).is_pending());

    // The proxy delays the response for longer than these timeouts.
    tokio::time::timeout(SLOW_RESPONSE_DELAY / 2, session.force_shutdown())
        .await
        .unwrap();
    tokio::time::timeout(SLOW_RESPONSE_DELAY / 2, graceful)
        .await
        .unwrap();
    assert!(slow_request.await.unwrap().is_err());
    assert_closed(&session, &session.get_cluster_state());
}

#[tokio::test]
async fn force_shutdown_can_be_called_more_than_once() {
    let (session, _proxy) = connect().await;

    // Concurrent calls are allowed.
    tokio::time::timeout(TEST_TIMEOUT, async {
        tokio::join!(session.force_shutdown(), session.force_shutdown())
    })
    .await
    .unwrap();
    assert_closed(&session, &session.get_cluster_state());

    // Later calls of both kinds return at once.
    assert!(futures::poll!(pin!(session.force_shutdown())).is_ready());
    assert!(futures::poll!(pin!(session.shutdown())).is_ready());
}

#[tokio::test]
async fn drop_forces_a_shutdown() {
    let (session, _proxy) = connect().await;
    // This keeps the pools alive, as a Node that user code holds does.
    let cluster_state = session.get_cluster_state();
    let background_tasks = session.background_tasks.clone();

    drop(Arc::into_inner(session).unwrap());
    tokio::time::timeout(TEST_TIMEOUT, background_tasks.wait())
        .await
        .unwrap();
    assert_pools_closed(&cluster_state);
}

/// The integration test `test_public_methods_fail_after_shutdown` checks that
/// each public method rejects requests after the shutdown.
#[tokio::test]
async fn shutdown_closes_connections() {
    let (session, _proxy) = connect().await;
    let cluster_state = session.get_cluster_state();
    assert!(cluster_state.get_nodes_info()[0].is_connected());
    assert!(session.background_tasks.len() > 0);

    // Concurrent calls are allowed.
    tokio::time::timeout(TEST_TIMEOUT, async {
        tokio::join!(session.shutdown(), session.shutdown())
    })
    .await
    .unwrap();

    // The pools release their connections, also in an old ClusterState.
    assert_closed(&session, &cluster_state);

    // A later call returns at once.
    assert!(futures::poll!(pin!(session.shutdown())).is_ready());
}

#[tokio::test]
async fn unread_pager_does_not_delay_shutdown() {
    let (session, _proxy) = connect().await;
    let mut rows = session
        .query_iter(PAGED_QUERY, ())
        .await
        .unwrap()
        .rows_stream::<(i32,)>()
        .unwrap();

    tokio::time::timeout(TEST_TIMEOUT, session.shutdown())
        .await
        .unwrap();

    // The rows fetched before the shutdown stay available.
    // Then the pager must fetch a new page, which the session rejects.
    let mut fetched_rows = 0;
    let error = loop {
        match rows.next().await.unwrap() {
            Ok(_) => fetched_rows += 1,
            Err(error) => break error,
        }
    };
    assert!(fetched_rows >= 1);
    assert_matches!(
        error,
        NextRowError::NextPageError(NextPageError::SessionShutDown(_))
    );
}
