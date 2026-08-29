use crate::utils::{setup_tracing, test_with_3_node_cluster};
use scylla::client::session::Session;
use scylla::client::session_builder::SessionBuilder;
use scylla::policies::address_translator::AddressTranslator;
use scylla_cql::frame::request::options;
use scylla_cql::frame::types;
use scylla_proxy::{
    Condition, ProxyError, Reaction, RequestFrame, RequestOpcode, RequestReaction, RequestRule,
    ShardAwareness, WorkerError,
};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use uuid::Uuid;

/// The number of connections a freshly built session opens: one control
/// connection plus, with the default `PoolSize::PerShard(1)`, one pool
/// connection per shard of every node. A non-ScyllaDB node reports no sharding
/// and gets a single pool connection.
fn expected_connection_count(session: &Session) -> usize {
    1 + session
        .get_cluster_state()
        .get_nodes_info()
        .iter()
        .map(|node| {
            node.sharder()
                .map_or(1, |sharder| usize::from(sharder.nr_shards.get()))
        })
        .sum::<usize>()
}

/// Receives exactly `count` STARTUP frames, returning the option map of each.
///
/// Receiving a known count is the barrier that makes these tests deterministic:
/// `Session::connect` only waits for the first connection to each node
/// (`ClusterState::wait_until_all_pools_are_initialized`), so the remaining pool
/// connections are opened asynchronously by the pool refiller and their frames
/// arrive later. Awaiting them is sleep-free; any surplus frame stays queued.
async fn recv_startup_options(
    startup_rx: &mut mpsc::UnboundedReceiver<(RequestFrame, Option<u16>)>,
    count: usize,
    // Frames not belonging to the session under test are not counted; the
    // predicate tells the two apart.
    is_under_test: impl Fn(&HashMap<String, String>) -> bool,
) -> Vec<HashMap<String, String>> {
    let mut received = Vec::new();
    let recv_all = async {
        while received.len() < count {
            let (startup_frame, _shard) = startup_rx.recv().await.unwrap();
            let startup_options = types::read_string_map(&mut &*startup_frame.body).unwrap();
            if is_under_test(&startup_options) {
                received.push(startup_options);
            }
        }
    };

    tokio::time::timeout(Duration::from_secs(30), recv_all)
        .await
        .unwrap_or_else(|_| {
            panic!(
                "received only {} of {count} expected STARTUP frames",
                received.len()
            )
        });

    received
}

/// Receives exactly `count` STARTUP frames reporting `session_id`, asserting
/// that every frame carries a `SESSION_ID` option and that the only other value
/// that may appear is one of `tolerated`.
///
/// Returns the option maps of the received frames.
async fn recv_session_ids(
    startup_rx: &mut mpsc::UnboundedReceiver<(RequestFrame, Option<u16>)>,
    session_id: Uuid,
    count: usize,
    tolerated: &[Uuid],
) -> Vec<HashMap<String, String>> {
    let expected = session_id.to_string();
    recv_startup_options(startup_rx, count, |startup_options| {
        let reported = startup_options
            .get(options::SESSION_ID)
            .expect("STARTUP frame without a SESSION_ID option");

        if *reported == expected {
            true
        } else {
            let reported = reported.parse::<Uuid>().unwrap();
            assert!(
                tolerated.contains(&reported),
                "unexpected SESSION_ID reported in STARTUP: {reported}"
            );
            false
        }
    })
    .await
}

/// Every connection of a session - the control connection and all pool
/// connections alike - must report the same `SESSION_ID`, and it must be the
/// one returned by [`Session::session_id`].
#[tokio::test]
async fn session_id_is_sent_on_every_connection() {
    setup_tracing();

    let res = test_with_3_node_cluster(
        ShardAwareness::QueryNode,
        |proxy_uris, translation_map, mut running_proxy| async move {
            // The proxy informs us (via startup_rx) about every STARTUP frame the driver
            // sends. The rule is installed on all three nodes, so that both the control
            // connection and the per-shard pool connections are observed.
            let (startup_tx, mut startup_rx) = mpsc::unbounded_channel();
            for node in running_proxy.running_nodes.iter_mut() {
                node.change_request_rules(Some(vec![RequestRule(
                    Condition::RequestOpcode(RequestOpcode::Startup),
                    RequestReaction::noop().with_feedback_when_performed(startup_tx.clone()),
                )]));
            }

            let session: Session = SessionBuilder::new()
                .known_node(proxy_uris[0].as_str())
                .address_translator(Arc::new(translation_map))
                .build()
                .await
                .unwrap();

            let expected = expected_connection_count(&session);
            recv_session_ids(&mut startup_rx, session.session_id(), expected, &[]).await;

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

/// The configuration report must be sent by the control connection and by no
/// other connection of the session, and must be a JSON document of the current
/// schema version, small enough that the server accepts it. Disabling it must
/// suppress it on every connection, and must leave `SESSION_ID` alone.
#[tokio::test]
async fn driver_config_is_sent_only_on_the_control_connection_and_can_be_disabled() {
    setup_tracing();

    let res = test_with_3_node_cluster(
        ShardAwareness::QueryNode,
        |proxy_uris, translation_map, mut running_proxy| async move {
            let (startup_tx, mut startup_rx) = mpsc::unbounded_channel();
            for node in running_proxy.running_nodes.iter_mut() {
                node.change_request_rules(Some(vec![RequestRule(
                    Condition::RequestOpcode(RequestOpcode::Startup),
                    RequestReaction::noop().with_feedback_when_performed(startup_tx.clone()),
                )]));
            }

            let translation_map: Arc<dyn AddressTranslator> = Arc::new(translation_map);
            let session: Session = SessionBuilder::new()
                .known_node(proxy_uris[0].as_str())
                .address_translator(Arc::clone(&translation_map))
                .build()
                .await
                .unwrap();

            let expected = expected_connection_count(&session);
            assert!(
                expected > 1,
                "the test proves nothing about 'only the control connection' with a single connection"
            );
            let frames =
                recv_session_ids(&mut startup_rx, session.session_id(), expected, &[]).await;

            let reports: Vec<&String> = frames
                .iter()
                .filter_map(|opts| opts.get(options::DRIVER_CONFIG))
                .collect();
            assert_eq!(
                reports.len(),
                1,
                "expected exactly one of the {expected} connections to report the configuration"
            );

            let report = reports[0];
            // The spec requires drivers to omit an oversized report, so a report
            // that arrived at all must be within the limit.
            assert!(report.len() < 32 * 1024, "report is too large: {report}");
            let parsed: serde_json::Value = serde_json::from_str(report).unwrap();
            assert_eq!(parsed["version"], 1);

            // A session that opted out must not report anything, while still
            // identifying itself with SESSION_ID on every connection. The first
            // session stays alive and may open connections of its own, so its
            // id is tolerated here, and nothing else is.
            let silent_session: Session = SessionBuilder::new()
                .known_node(proxy_uris[0].as_str())
                .address_translator(Arc::clone(&translation_map))
                .driver_config_reporting(false)
                .build()
                .await
                .unwrap();

            let silent_expected = expected_connection_count(&silent_session);
            let silent_frames = recv_session_ids(
                &mut startup_rx,
                silent_session.session_id(),
                silent_expected,
                &[session.session_id()],
            )
            .await;
            assert!(
                silent_frames
                    .iter()
                    .all(|opts| !opts.contains_key(options::DRIVER_CONFIG)),
                "a session with reporting disabled sent a configuration report"
            );

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
