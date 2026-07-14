//! End-to-end tests for the `TABLETS_ROUTING_V2` protocol extension.
//!
//! Unlike the unit tests in `scylla/src/routing/locator/tablets.rs` (which cover the
//! block encoding and payload parsing in isolation), these tests cross the
//! driver<->server boundary through [`scylla_proxy`]. They validate that:
//!
//! - on a connection that negotiated V2, every `EXECUTE` carries exactly one trailing
//!   tablet-version block byte and the frame stays exactly shaped (no desync);
//! - the block the driver sends actually agrees with the server's encoding, i.e. once the
//!   routing cache is warm the server stops returning `tablets-routing-v2` payloads;
//! - a mixed cluster (one node without the extension) keeps both framings correct on their
//!   respective connections.
//!
//! The extension is experimental: the server only advertises it (on the wire under the name
//! `TABLETS_ROUTING_V2_EXPERIMENTAL`) when started with the experimental feature that gates it
//! (`--experimental-features strongly-consistent-tables`). When the server does not negotiate
//! it, every test here skips, exactly like the other feature-gated integration tests in this
//! suite.

use std::sync::Arc;

use scylla::client::session::Session;
use scylla::client::session_builder::SessionBuilder;

use scylla_cql::frame::parse_response_body_extensions;
use scylla_cql::frame::protocol_features::ProtocolFeatures;
use scylla_cql::frame::request::DeserializableRequest;
use scylla_cql::frame::request::execute::ExecuteV2;
use scylla_cql::frame::response::Supported;
use scylla_cql::frame::types;

use scylla_proxy::{
    Condition, ProxyError, Reaction, RequestFrame, RequestOpcode, RequestReaction, RequestRule,
    ResponseFrame, ResponseOpcode, ResponseReaction, ResponseRule, ShardAwareness, TargetShard,
    WorkerError,
};

use tokio::sync::mpsc;

use crate::utils::{
    PerformDDL, execute_prepared_statement_everywhere, fetch_negotiated_features,
    scylla_supports_tablets, setup_tracing, supports_feature, test_with_3_node_cluster,
    unique_keyspace_name,
};

/// The custom-payload key under which the server returns fresh V2 routing information.
const CUSTOM_PAYLOAD_TABLETS_V2_KEY: &str = "tablets-routing-v2";

/// Creates a keyspace and a single-partition table that use tablets.
///
/// Uses the newer `TABLET_OPTIONS` syntax when the server supports it, falling back to the
/// `initial` syntax otherwise (mirroring the other tablet integration tests).
async fn create_tablet_table(session: &Session, ks: &str) {
    let supports_table_tablet_options = supports_feature(session, "TABLET_OPTIONS").await;
    let (ks_tablet_opts, table_tablet_opts) = if supports_table_tablet_options {
        (
            "AND tablets = { 'enabled': true }".to_string(),
            "WITH tablets = { 'min_tablet_count': 8 }".to_string(),
        )
    } else {
        ("AND tablets = { 'initial': 8 }".to_string(), String::new())
    };

    session
        .ddl(format!(
            "CREATE KEYSPACE IF NOT EXISTS {ks} WITH REPLICATION = \
             {{'class': 'NetworkTopologyStrategy', 'replication_factor': 2}} {ks_tablet_opts}"
        ))
        .await
        .unwrap();
    session
        .ddl(format!(
            "CREATE TABLE IF NOT EXISTS {ks}.t (pk int PRIMARY KEY, v int) {table_tablet_opts}"
        ))
        .await
        .unwrap();
}

/// Decodes a captured `EXECUTE` request frame with the given negotiated `features` and returns
/// its trailing tablet-version block.
///
/// Also asserts that the frame is exactly shaped: after decoding there must be no leftover
/// bytes. A missing or extra byte would leave the buffer non-empty (or make the decode fail),
/// which is precisely the on-wire desync this extension must never cause.
fn decode_execute_block(frame: RequestFrame, features: &ProtocolFeatures) -> Option<u8> {
    let body = parse_response_body_extensions(frame.params.flags, None, frame.body).unwrap();
    let mut buf: &[u8] = &body.body;
    let execute = ExecuteV2::deserialize_with_features(&mut buf, features).unwrap();
    assert!(
        buf.is_empty(),
        "EXECUTE body had {} leftover byte(s) after decoding with the negotiated features; \
         the on-wire frame is desynced",
        buf.len()
    );
    execute.tablet_version_block
}

/// Returns whether a captured RESULT response carries a fresh `tablets-routing-v2` routing
/// payload. The server includes it only when the driver's tablet-version block does not match
/// its own, so its absence over many requests means the driver's cache is in sync.
fn response_carries_v2_payload(frame: ResponseFrame) -> bool {
    let response = parse_response_body_extensions(frame.params.flags, None, frame.body).unwrap();
    match response.custom_payload {
        Some(map) => map.contains_key(CUSTOM_PAYLOAD_TABLETS_V2_KEY),
        None => false,
    }
}

/// On a `TABLETS_ROUTING_V2` connection every `EXECUTE` must carry exactly one trailing
/// tablet-version block byte, and the driver's block encoding must agree with the server's.
///
/// The server returns a `tablets-routing-v2` payload only on a version mismatch. From a cold
/// cache the driver sends a random block, which almost always mismatches, so the server
/// teaches it the current version; once the cache is warm the driver sends a block derived
/// from that version, which matches, so the payloads stop. A wrong block encoding
/// (e.g. a bad bit-shift) would mismatch forever and never converge.
#[tokio::test]
async fn test_tablets_routing_v2_execute_carries_block_and_converges() {
    setup_tracing();

    let features = fetch_negotiated_features(None).await;
    if !features.tablets_v2_supported {
        tracing::warn!(
            "Skipping test because the server did not negotiate TABLETS_ROUTING_V2_EXPERIMENTAL"
        );
        return;
    }

    let res = test_with_3_node_cluster(
        ShardAwareness::QueryNode,
        |proxy_uris, translation_map, mut running_proxy| async move {
            let session: Session = SessionBuilder::new()
                .known_node(proxy_uris[0].as_str())
                .address_translator(Arc::new(translation_map))
                .build()
                .await
                .unwrap();

            if !scylla_supports_tablets(&session).await {
                tracing::warn!("Skipping test because this Scylla version doesn't support tablets");
                return running_proxy;
            }

            let ks = unique_keyspace_name();

            create_tablet_table(&session, &ks).await;

            // A single fixed partition key, so every request targets the same tablet and we
            // observe that one tablet's version converge.
            const PK: i32 = 42;
            session
                .query_unpaged(format!("INSERT INTO {ks}.t (pk, v) VALUES ({PK}, 1)"), &())
                .await
                .unwrap();

            let select = session
                .prepare(format!("SELECT v FROM {ks}.t WHERE pk = ?"))
                .await
                .unwrap();

            // Capture EXECUTE requests and RESULT responses on every node. Rules are installed
            // only now, after schema setup, so only the measured loop below is captured.
            let (tx_exec, mut rx_exec) =
                mpsc::unbounded_channel::<(RequestFrame, Option<TargetShard>)>();
            let (tx_resp, mut rx_resp) =
                mpsc::unbounded_channel::<(ResponseFrame, Option<TargetShard>)>();
            running_proxy.running_nodes.iter_mut().for_each(|node| {
                node.prepend_request_rules(vec![RequestRule(
                    Condition::not(Condition::ConnectionRegisteredAnyEvent)
                        .and(Condition::RequestOpcode(RequestOpcode::Execute)),
                    RequestReaction::noop().with_feedback_when_performed(tx_exec.clone()),
                )]);
                node.prepend_response_rules(vec![ResponseRule(
                    Condition::not(Condition::ConnectionRegisteredAnyEvent)
                        .and(Condition::ResponseOpcode(ResponseOpcode::Result)),
                    ResponseReaction::noop().with_feedback_when_performed(tx_resp.clone()),
                )]);
            });

            // Enough requests to observe the cold-start mismatch and then convergence, while
            // staying cheap even in debug mode.
            const ITERATIONS: usize = 60;
            for _ in 0..ITERATIONS {
                session.execute_unpaged(&select, (PK,)).await.unwrap();
            }

            // Every captured EXECUTE must carry exactly one well-formed trailing block byte.
            let mut executes_seen = 0usize;
            while let Ok((frame, _shard)) = rx_exec.try_recv() {
                assert!(
                    decode_execute_block(frame, &features).is_some(),
                    "EXECUTE on a V2 connection must carry a tablet-version block"
                );
                executes_seen += 1;
            }
            assert!(
                executes_seen >= ITERATIONS,
                "expected to capture at least {ITERATIONS} EXECUTE frames, captured {executes_seen}"
            );

            // From a cold cache the driver sends a randomly chosen block, which almost always
            // mismatches the server's tablet version, so the server returns at least one
            // `tablets-routing-v2` payload; once the cache is warm the block matches and the
            // payloads stop, giving a long run of payload-free responses. (There is a tiny
            // chance -- on the order of one in a few hundred -- that the random cold-start block
            // matches on the very first request and no payload is ever seen; that is rare enough
            // that we accept it here rather than complicate the test.)
            let mut saw_payload = false;
            let mut longest_no_payload_run = 0usize;
            let mut current_run = 0usize;
            while let Ok((frame, _shard)) = rx_resp.try_recv() {
                if response_carries_v2_payload(frame) {
                    saw_payload = true;
                    current_run = 0;
                } else {
                    current_run += 1;
                    longest_no_payload_run = longest_no_payload_run.max(current_run);
                }
            }
            assert!(
                saw_payload,
                "server never returned a tablets-routing-v2 payload; the V2 payload path was \
                 not exercised (is the table really using tablets?)"
            );
            // 15 is an arbitrary threshold: it only needs to be comfortably above 0 and well
            // below ITERATIONS, so that a converged cache (which stops triggering payloads) is
            // clearly distinguished from an encoding that never converges.
            assert!(
                longest_no_payload_run >= 15,
                "the driver's tablet-version cache never converged (longest run of matching \
                 requests was {longest_no_payload_run}); its block encoding likely disagrees \
                 with the server's"
            );

            session
                .ddl(format!("DROP KEYSPACE IF EXISTS {ks}"))
                .await
                .unwrap();
            running_proxy
        },
    )
    .await;

    // `test_with_3_node_cluster` returns the proxy's final status. When the session is dropped
    // as the proxy shuts down, an in-flight request can observe the connection closing and the
    // proxy reports `DriverDisconnected`; that is benign here (the measurement already
    // finished), so we accept it. Any other error is a real failure.
    match res {
        Ok(()) => (),
        Err(ProxyError::Worker(WorkerError::DriverDisconnected(_))) => (),
        Err(err) => panic!("{}", err),
    }
}

/// A mixed cluster where one node does not advertise `TABLETS_ROUTING_V2`.
///
/// `EXECUTE` frames sent to the V2 nodes must carry the trailing tablet-version block, while
/// frames sent to the non-V2 node must carry none. Both framings must stay exactly shaped
/// (each decodes with no leftover bytes) and every request must succeed. This is exactly the
/// invariant a retry relies on when it crosses a V2 and a non-V2 connection: neither frame
/// desyncs.
#[tokio::test]
async fn test_tablets_routing_v2_mixed_feature_connections() {
    setup_tracing();

    let features = fetch_negotiated_features(None).await;
    if !features.tablets_v2_supported {
        tracing::warn!(
            "Skipping test because the server did not negotiate TABLETS_ROUTING_V2_EXPERIMENTAL"
        );
        return;
    }
    // The non-V2 node keeps every other negotiated feature (e.g. the metadata id), so its
    // EXECUTE frames must be decoded with V2 turned off but the rest left on.
    let mut non_v2_features = features;
    non_v2_features.tablets_v2_supported = false;

    let res = test_with_3_node_cluster(
        ShardAwareness::QueryNode,
        |proxy_uris, translation_map, mut running_proxy| async move {
            // Strip TABLETS_ROUTING_V2_EXPERIMENTAL from node 0's SUPPORTED frame so the driver
            // negotiates V2 only with nodes 1 and 2. The rule stays active for the whole test,
            // so every (re)connection to node 0 stays non-V2.
            running_proxy.running_nodes[0].change_response_rules(Some(vec![ResponseRule(
                Condition::ResponseOpcode(ResponseOpcode::Supported),
                ResponseReaction::transform_frame(Arc::new(|mut response: ResponseFrame| {
                    let mut msg = Supported::deserialize(&mut &*response.body).unwrap();
                    msg.options.remove("TABLETS_ROUTING_V2_EXPERIMENTAL");
                    // scylla-cql has no capability to serialize responses, so re-encode the
                    // string multimap by hand.
                    let mut new_body = Vec::new();
                    types::write_string_multimap(&msg.options, &mut new_body).unwrap();
                    response.body = new_body.into();
                    response
                })),
            )]));

            let session: Session = SessionBuilder::new()
                .known_node(proxy_uris[0].as_str())
                .address_translator(Arc::new(translation_map))
                .build()
                .await
                .unwrap();

            if !scylla_supports_tablets(&session).await {
                tracing::warn!("Skipping test because this Scylla version doesn't support tablets");
                return running_proxy;
            }

            let ks = unique_keyspace_name();

            create_tablet_table(&session, &ks).await;
            session
                .query_unpaged(format!("INSERT INTO {ks}.t (pk, v) VALUES (0, 1)"), &())
                .await
                .unwrap();

            let select = session
                .prepare(format!("SELECT v FROM {ks}.t WHERE pk = ?"))
                .await
                .unwrap();

            // Capture EXECUTEs per node so each frame is decoded with that node's features.
            let mut exec_rxs = Vec::new();
            for node in running_proxy.running_nodes.iter_mut() {
                let (tx, rx) = mpsc::unbounded_channel::<(RequestFrame, Option<TargetShard>)>();
                node.prepend_request_rules(vec![RequestRule(
                    Condition::not(Condition::ConnectionRegisteredAnyEvent)
                        .and(Condition::RequestOpcode(RequestOpcode::Execute)),
                    RequestReaction::noop().with_feedback_when_performed(tx),
                )]);
                exec_rxs.push(rx);
            }

            // Force the SELECT onto every node and shard, so the non-V2 node 0 and the V2
            // nodes 1-2 all receive EXECUTEs regardless of replica placement. That every call
            // succeeds already proves neither framing desynced on the wire.
            execute_prepared_statement_everywhere(
                &session,
                session.get_cluster_state().as_ref(),
                &select,
                &(0i32,),
            )
            .await
            .unwrap();

            // Node 0 negotiated non-V2: its EXECUTEs must carry NO trailing block.
            let mut non_v2_frames = 0usize;
            while let Ok((frame, _shard)) = exec_rxs[0].try_recv() {
                assert_eq!(
                    decode_execute_block(frame, &non_v2_features),
                    None,
                    "EXECUTE to the non-V2 node must not carry a tablet-version block"
                );
                non_v2_frames += 1;
            }

            // Nodes 1 and 2 negotiated V2: their EXECUTEs must carry the trailing block.
            let mut v2_frames = 0usize;
            for rx in exec_rxs[1..].iter_mut() {
                while let Ok((frame, _shard)) = rx.try_recv() {
                    assert!(
                        decode_execute_block(frame, &features).is_some(),
                        "EXECUTE to a V2 node must carry a tablet-version block"
                    );
                    v2_frames += 1;
                }
            }

            assert!(non_v2_frames >= 1, "the non-V2 node received no EXECUTEs");
            assert!(v2_frames >= 1, "the V2 nodes received no EXECUTEs");

            session
                .ddl(format!("DROP KEYSPACE IF EXISTS {ks}"))
                .await
                .unwrap();
            running_proxy
        },
    )
    .await;

    // A `DriverDisconnected` error is benign here (the session is dropped as the proxy shuts
    // down). Any other error is a real failure.
    match res {
        Ok(()) => (),
        Err(ProxyError::Worker(WorkerError::DriverDisconnected(_))) => (),
        Err(err) => panic!("{}", err),
    }
}
