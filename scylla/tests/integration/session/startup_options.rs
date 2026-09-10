use crate::utils::{
    create_new_session_builder, execute_unprepared_statement_on_every_node, setup_tracing,
    test_with_3_node_cluster,
};
use scylla::client::PoolSize;
use scylla::client::session::Session;
use scylla::client::session_builder::SessionBuilder;
use scylla::policies::address_translator::AddressTranslator;
use scylla::statement::Statement;
use scylla_cql::frame::request::options;
use scylla_cql::frame::types;
use scylla_proxy::{
    Condition, ProxyError, Reaction, RequestFrame, RequestOpcode, RequestReaction, RequestRule,
    ShardAwareness, WorkerError,
};
use std::collections::HashMap;
use std::num::NonZeroUsize;
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
            // The spec requires drivers to omit a report *above* the limit, so a
            // report that arrived at all is at most that large.
            assert!(report.len() <= 32 * 1024, "report is too large: {report}");
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

/// The cross-driver schema the report must conform to; the same vendored copy
/// the unit tests validate against, so that what a real server stored is held
/// to the same contract as what the reporter produced.
const SCHEMA: &str = include_str!("../../../src/client/driver-config-schema.json");

/// The whole point of the feature, end to end: the driver sends the options,
/// the server stores them, and an operator reading `system.clients` can find
/// them and correlate them with a session.
///
/// `system.clients` is ScyllaDB-only, hence the Cassandra gate, and node-local:
/// each node reports only the clients connected to it, so the query is run once
/// against every node and the rows unioned. Nothing has to be waited for - the
/// server registers a client while establishing its connection, and
/// `Session::builder().build()` only returns once the control connection has
/// completed its `STARTUP` exchange, so every row asserted on here was written
/// before the session existed to query with.
#[cfg_attr(cassandra_tests, ignore)]
#[tokio::test]
async fn driver_config_is_readable_from_system_clients() {
    setup_tracing();

    let session = create_new_session_builder()
        .fetch_schema_metadata(false)
        .pool_size(PoolSize::PerHost(NonZeroUsize::new(1).unwrap()))
        .build()
        .await
        .unwrap();

    // Capability detection rather than a version check: `client_options` was
    // added to the virtual table at some point and older clusters simply do not
    // have the column.
    let has_column = session
        .query_unpaged(
            "SELECT column_name FROM system_schema.columns \
             WHERE keyspace_name = 'system' AND table_name = 'clients' \
             AND column_name = 'client_options'",
            &[],
        )
        .await
        .unwrap()
        .into_rows_result()
        .unwrap()
        .rows::<(String,)>()
        .unwrap()
        .next()
        .is_some();

    if !has_column {
        tracing::warn!(
            "skipping: this cluster's system.clients has no client_options column, \
             so there is nothing for the driver configuration report to land in"
        );
        return;
    }

    let session_id = session.session_id().to_string();
    let statement = Statement::new("SELECT client_options FROM system.clients");
    let cluster = session.get_cluster_state();
    let results = execute_unprepared_statement_on_every_node(&session, &cluster, &statement, &[])
        .await
        .unwrap();

    let mut own_rows = Vec::new();
    for result in results {
        let rows = result.into_rows_result().unwrap();
        for row in rows
            .rows::<(Option<HashMap<String, String>>,)>()
            .unwrap()
            .map(Result::unwrap)
        {
            let Some(client_options) = row.0 else {
                continue;
            };
            if client_options.get("SESSION_ID") == Some(&session_id) {
                own_rows.push(client_options);
            }
        }
    }

    assert!(
        !own_rows.is_empty(),
        "no system.clients row reports this session's SESSION_ID {session_id}"
    );

    let reports: Vec<&String> = own_rows
        .iter()
        .filter_map(|options| options.get("DRIVER_CONFIG"))
        .collect();
    assert_eq!(
        reports.len(),
        1,
        "exactly one of this session's {} connections must report its configuration",
        own_rows.len()
    );

    let report = reports[0];
    // The spec omits the option only *above* the limit, and
    // `oversized_reports_are_omitted` pins that a report of exactly the limit is
    // still sent, so the bound here has to be inclusive too.
    assert!(report.len() <= 32 * 1024, "report is too large: {report}");

    let report: serde_json::Value = serde_json::from_str(report).unwrap();
    assert_eq!(report["version"], 1);

    let schema: serde_json::Value = serde_json::from_str(SCHEMA).unwrap();
    let validator = jsonschema::validator_for(&schema).unwrap();
    let errors: Vec<String> = validator
        .iter_errors(&report)
        .map(|error| format!("  at {}: {error}", error.instance_path()))
        .collect();
    assert!(
        errors.is_empty(),
        "the configuration stored by the server does not conform to the schema:\n{}\nreport: {report}",
        errors.join("\n")
    );
}
