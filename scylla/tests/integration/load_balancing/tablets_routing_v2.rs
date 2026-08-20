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
//!   respective connections;
//! - leader-requiring requests to a strongly-consistent keyspace reach the tablet's Raft leader;
//! - V1 still works when V2 is unavailable, since the two are mutually exclusive.
//!
//! The extension is experimental: the server only advertises it (on the wire under the name
//! `TABLETS_ROUTING_V2_EXPERIMENTAL`) when started with the experimental feature that gates it
//! (`--experimental-features strongly-consistent-tables`). When the server does not negotiate
//! it, the V2 tests here skip, exactly like the other feature-gated integration tests in this
//! suite. The V1 test deliberately does not skip: it hides the extension from every node, so it
//! exercises the V1 path either way.
//!
//! A tablet's routing can change under a running test -- it can migrate, and a
//! strongly-consistent tablet can elect a new Raft leader -- which would make correct routing
//! look wrong. Assertions that depend on where a request landed are therefore wrapped in
//! [`with_migration_retry`], following the same approach as the tests in `tablets.rs`, and
//! snapshot [`cached_tablet_routing`] to decide whether a failure was caused by such a change.

use std::cell::Cell;
use std::sync::Arc;

use futures::future::try_join_all;
use scylla::client::session::Session;
use scylla::client::session_builder::SessionBuilder;
use scylla::cluster::metadata::{CollectionType, ColumnType, NativeType};
use scylla::deserialize::FrameSlice;
use scylla::deserialize::value::{DeserializeValue, ListlikeIterator};
use scylla::routing::Shard;
use scylla::serialize::row::SerializeRow;
use scylla::statement::Consistency;
use scylla::statement::prepared::PreparedStatement;
use uuid::Uuid;

use scylla_cql::frame::parse_response_body_extensions;
use scylla_cql::frame::protocol_features::ProtocolFeatures;
use scylla_cql::frame::request::DeserializableRequest;
use scylla_cql::frame::request::execute::ExecuteV2;
use scylla_cql::frame::response::Supported;
use scylla_cql::frame::types;

use scylla_proxy::{
    Condition, ProxyError, Reaction, RequestFrame, RequestOpcode, RequestReaction, RequestRule,
    ResponseFrame, ResponseOpcode, ResponseReaction, ResponseRule, RunningProxy, ShardAwareness,
    TargetShard, WorkerError,
};

use tokio::sync::mpsc;

use crate::utils::{
    PerformDDL, execute_prepared_statement_everywhere, fetch_negotiated_features,
    scylla_supports_tablets, setup_tracing, supports_feature, test_with_3_node_cluster,
    unique_keyspace_name, with_migration_retry,
};

/// The custom-payload key under which the server returns fresh V2 routing information.
const CUSTOM_PAYLOAD_TABLETS_V2_KEY: &str = "tablets-routing-v2";
/// The V1 equivalent, used by the test that checks V1 still works without V2.
const CUSTOM_PAYLOAD_TABLETS_V1_KEY: &str = "tablets-routing-v1";
/// The wire name of the (experimental) V2 extension, as advertised in SUPPORTED.
const TABLETS_ROUTING_V2_EXTENSION: &str = "TABLETS_ROUTING_V2_EXPERIMENTAL";

/// The driver's *cached* routing view of the tablet owning `pk` in `ks.t`: the replica list in
/// the order the driver currently holds it.
///
/// This is what [`with_migration_retry`] snapshots here, and it is deliberately the driver's own
/// view rather than anything read from `system.tablets`. The assertions in these tests compare
/// where a request landed against what the driver believed at the time, so the thing that
/// invalidates a measurement is precisely *the driver's view changing mid-flight* -- and that is
/// what this observes directly.
///
/// It changes whenever the driver learns a new mapping for the tablet, which covers both events
/// that matter and which are largely independent of each other:
///
/// - the tablet migrated, changing its replica set;
/// - a strongly-consistent tablet elected a new Raft leader. A re-election alone is enough: the
///   tablet version stays the same only as long as *both* the replica set and the leader do, so
///   a new leader (very probably) changes the version, and the server then hands the driver a
///   freshly ordered list.
///
/// A snapshot of the replica set read from `system.tablets` would have caught only the first of
/// those -- the leader appears nowhere in that table.
async fn cached_tablet_routing(session: &Session, ks: &str, pk: i32) -> Vec<(Uuid, Shard)> {
    session
        .get_cluster_state()
        .get_endpoints(ks, "t", &(pk,))
        .unwrap()
        .iter()
        .map(|(node, shard)| (node.host_id, *shard))
        .collect()
}

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

/// Returns whether a captured RESULT response carries a fresh tablet-routing payload under
/// `payload_key`.
///
/// For V2 the server includes it only when the driver's tablet-version block does not match its
/// own, so its absence over many requests means the driver's cache is in sync. For V1 it is
/// included whenever the driver contacted a shard that does not own the tablet.
fn response_carries_tablets_payload(frame: ResponseFrame, payload_key: &str) -> bool {
    let response = parse_response_body_extensions(frame.params.flags, None, frame.body).unwrap();
    match response.custom_payload {
        Some(map) => map.contains_key(payload_key),
        None => false,
    }
}

/// The receiving end of a proxy feedback channel carrying captured `EXECUTE` requests.
type ExecuteFeedback = mpsc::UnboundedReceiver<(RequestFrame, Option<TargetShard>)>;
/// The receiving end of a proxy feedback channel carrying captured `RESULT` responses.
type ResultFeedback = mpsc::UnboundedReceiver<(ResponseFrame, Option<TargetShard>)>;

/// The replica list of a captured `tablets-routing-v2` payload, in the order the server sent it,
/// or `None` if the response carries no such payload.
///
/// `replicas[0]` is the tablet's Raft leader. This is decoded straight from the captured frame
/// rather than read back from the driver, on purpose: the leader is what these tests assert
/// about, so taking it from the driver's own cache would make the assertion circular -- a driver
/// that mis-parsed the payload's replica order would agree with itself and the test would pass.
///
/// It is also the only way to learn the leader at all. The server publishes it nowhere else: not
/// in schema, not in `system.tablets`; it only lists it first in this payload, and a Raft
/// re-election can move it at any time.
fn decode_v2_payload_replicas(frame: ResponseFrame) -> Option<Vec<(Uuid, i32)>> {
    type V2Payload<'frame, 'metadata> = (
        i64,
        i64,
        ListlikeIterator<'frame, 'metadata, (Uuid, i32)>,
        i64,
    );

    let response = parse_response_body_extensions(frame.params.flags, None, frame.body).unwrap();
    let payload = response
        .custom_payload?
        .remove(CUSTOM_PAYLOAD_TABLETS_V2_KEY)?;

    // The same shape the driver expects: (first_token, last_token, [(host, shard)], version).
    let typ = ColumnType::Tuple(vec![
        ColumnType::Native(NativeType::BigInt),
        ColumnType::Native(NativeType::BigInt),
        ColumnType::Collection {
            frozen: false,
            typ: CollectionType::List(Box::new(ColumnType::Tuple(vec![
                ColumnType::Native(NativeType::Uuid),
                ColumnType::Native(NativeType::Int),
            ]))),
        },
        ColumnType::Native(NativeType::BigInt),
    ]);

    <V2Payload as DeserializeValue<'_, '_>>::type_check(&typ).unwrap();
    let (_first_token, _last_token, replicas, _version) =
        <V2Payload as DeserializeValue<'_, '_>>::deserialize(&typ, Some(FrameSlice::new(&payload)))
            .unwrap();

    Some(replicas.map(|replica| replica.unwrap()).collect())
}

/// Installs proxy rules capturing every `EXECUTE` request and every `RESULT` response on every
/// node, and returns the receiving ends.
///
/// Rules are prepended, and callers install them only after schema setup, so that just the
/// measured phase of a test is captured.
fn capture_executes_and_results(
    running_proxy: &mut RunningProxy,
) -> (ExecuteFeedback, ResultFeedback) {
    let (tx_exec, rx_exec) = mpsc::unbounded_channel();
    let (tx_resp, rx_resp) = mpsc::unbounded_channel();
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
    (rx_exec, rx_resp)
}

/// A proxy rule that strips `TABLETS_ROUTING_V2_EXPERIMENTAL` from a node's SUPPORTED frame, so
/// the driver never negotiates V2 on any connection to that node.
///
/// Installed for the whole test, so reconnections stay non-V2 too. This is what lets these
/// tests cover the V1 path, and mixed clusters, against a server that does support V2.
fn hide_v2_extension_rule() -> ResponseRule {
    ResponseRule(
        Condition::ResponseOpcode(ResponseOpcode::Supported),
        ResponseReaction::transform_frame(Arc::new(|mut response: ResponseFrame| {
            let mut msg = Supported::deserialize(&mut &*response.body).unwrap();
            msg.options.remove(TABLETS_ROUTING_V2_EXTENSION);
            // scylla-cql has no capability to serialize responses, so re-encode the
            // string multimap by hand.
            let mut new_body = Vec::new();
            types::write_string_multimap(&msg.options, &mut new_body).unwrap();
            response.body = new_body.into();
            response
        })),
    )
}

/// Discards everything currently queued in a proxy feedback channel, so that an assertion only
/// sees frames produced by the phase it is measuring.
fn drain<T>(rx: &mut mpsc::UnboundedReceiver<T>) {
    while rx.try_recv().is_ok() {}
}

/// Executes `statement` `count` times concurrently, returning the coordinator of each execution.
///
/// Concurrency is the point: these are bulk request loops whose only purpose is to put load
/// through the routing path, and running them one at a time makes the tests needlessly slow.
async fn execute_concurrently(
    session: &Session,
    statement: &PreparedStatement,
    values: &(dyn SerializeRow + Sync),
    count: usize,
) -> Result<Vec<Uuid>, String> {
    try_join_all((0..count).map(|_| async move {
        let result = session
            .execute_unpaged(statement, values)
            .await
            .map_err(|e| format!("execution failed: {e}"))?;
        Ok::<_, String>(result.request_coordinator().node().host_id)
    }))
    .await
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
            // only now, after schema setup, so only the phases below are captured.
            let (mut rx_exec, mut rx_resp) = capture_executes_and_results(&mut running_proxy);

            // Phase 1, run once: warm the cache. These are independent of each other, so run
            // them concurrently: each request carries its own randomly chosen block, so every
            // cold one mismatches on its own and the server answers each with routing
            // information. Applying it repeatedly is idempotent, so nothing needs sequencing --
            // only that the batch has finished before the measured phase begins.
            const WARMUP: usize = 12;
            execute_concurrently(&session, &select, &(PK,), WARMUP)
                .await
                .unwrap();

            // From a cold cache the driver sends a randomly chosen block, which almost always
            // mismatches the server's version, so the server teaches it the current one. (With a
            // 1-in-16 chance per request the random block matches and no payload is returned, but
            // over WARMUP requests it is essentially certain that at least one mismatches.)
            let mut saw_payload = false;
            while let Ok((frame, _shard)) = rx_resp.try_recv() {
                saw_payload |=
                    response_carries_tablets_payload(frame, CUSTOM_PAYLOAD_TABLETS_V2_KEY);
            }
            assert!(
                saw_payload,
                "server never returned a tablets-routing-v2 payload during warm-up; the V2 \
                 payload path was not exercised (is the table really using tablets?)"
            );

            // The warm-up EXECUTEs must already carry the block -- the driver sends one from the
            // very first request, cold cache or not. Checked here because the measured phase
            // below drains the channel before running.
            let mut warmup_executes = 0usize;
            while let Ok((frame, _shard)) = rx_exec.try_recv() {
                assert!(
                    decode_execute_block(frame, &features).is_some(),
                    "EXECUTE on a V2 connection must carry a tablet-version block"
                );
                warmup_executes += 1;
            }
            assert!(
                warmup_executes >= WARMUP,
                "expected to capture at least {WARMUP} warm-up EXECUTE frames, captured \
                 {warmup_executes}"
            );

            // Phase 2, retried: with the cache warm, the block the driver sends must agree with
            // the server's encoding, so NOT ONE of these responses may carry a payload. A wrong
            // block encoding (e.g. a bad bit-shift) would mismatch forever.
            //
            // Anything that changes the tablet's version mid-phase (a migration, say) also makes
            // the server send a fresh payload, which would fail this through no fault of the
            // driver -- so snapshot the driver's routing view around the phase and retry if it
            // moved. The warm-up above is deliberately outside the retry: snapshotting a cold
            // cache would register the warm-up itself as a change.
            with_migration_retry(
                async || cached_tablet_routing(&session, &ks, PK).await,
                async |_| {
                    drain(&mut rx_exec);
                    drain(&mut rx_resp);

                    // These are independent, so run them concurrently.
                    const MEASURED: usize = 60;
                    execute_concurrently(&session, &select, &(PK,), MEASURED).await?;

                    let mut payloads = 0usize;
                    while let Ok((frame, _shard)) = rx_resp.try_recv() {
                        if response_carries_tablets_payload(frame, CUSTOM_PAYLOAD_TABLETS_V2_KEY) {
                            payloads += 1;
                        }
                    }
                    if payloads != 0 {
                        return Err(format!(
                            "the driver's tablet-version cache did not converge: {payloads} of \
                             {MEASURED} responses still carried a tablets-routing-v2 payload, so \
                             its block encoding likely disagrees with the server's"
                        ));
                    }

                    // Every measured EXECUTE must carry exactly one well-formed trailing block
                    // byte, just as the warm-up ones did.
                    let mut executes_seen = 0usize;
                    while let Ok((frame, _shard)) = rx_exec.try_recv() {
                        if decode_execute_block(frame, &features).is_none() {
                            return Err(
                                "EXECUTE on a V2 connection must carry a tablet-version block"
                                    .to_owned(),
                            );
                        }
                        executes_seen += 1;
                    }
                    if executes_seen < MEASURED {
                        return Err(format!(
                            "expected to capture at least {MEASURED} EXECUTE frames, captured \
                             {executes_seen}"
                        ));
                    }

                    Ok(())
                },
            )
            .await;

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
            // Hide the extension from node 0 only, so the driver negotiates V2 with nodes 1
            // and 2 but not with node 0.
            running_proxy.running_nodes[0]
                .change_response_rules(Some(vec![hide_v2_extension_rule()]));

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

// -- strongly-consistent (leader-aware) routing -----------------------------

/// Creates a strongly-consistent (Raft-based) keyspace and a single-partition table.
///
/// The `consistency = 'global'` clause is what the driver reads from
/// `system_schema.scylla_keyspaces` and exposes as [`ConsistencyMode::Global`] on
/// [`Keyspace::consistency_mode`], and it is what makes the load balancing policy route the
/// table's requests to the tablet leader.
async fn create_strongly_consistent_tablet_table(session: &Session, ks: &str) {
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
             {{'class': 'NetworkTopologyStrategy', 'replication_factor': 2}} \
             {ks_tablet_opts} AND consistency = 'global'"
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

/// For a table in a strongly-consistent (Raft-based) keyspace, every leader-requiring request
/// (here a `LOCAL_QUORUM` read) must be coordinated by the tablet's Raft leader, saving the extra
/// coordinator->leader hop.
///
/// The leader is taken from the `tablets-routing-v2` payload captured off the wire, not from the
/// driver's cache -- see [`decode_v2_payload_replicas`]. Asking the driver who the leader is would
/// make this circular: a driver that mis-parsed the payload's replica order would agree with
/// itself and the test would pass.
///
/// This also covers, indirectly, that the driver discovered the keyspace's consistency mode from
/// `system_schema.scylla_keyspaces` at all: leader-aware routing only engages for a keyspace the
/// driver believes is strongly consistent, so a mode that failed to be read would show up here as
/// requests spreading across replicas. The mode itself is crate-private, and the mapping from the
/// raw `consistency` column value to it is unit-tested next to the code that does it.
///
/// A read at `ONE`/`LOCAL_ONE` is intentionally *not* pinned to the leader (any single replica
/// satisfies it); that carve-out is covered by the policy unit tests.
#[tokio::test]
async fn test_leader_aware_routing_targets_the_raft_leader() {
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

            let sc_ks = unique_keyspace_name();
            create_strongly_consistent_tablet_table(&session, &sc_ks).await;

            // A single fixed partition key, so every request targets the same tablet.
            const PK: i32 = 2;

            // Writes to a strongly-consistent (Raft) table are rejected unless they use
            // QUORUM/LOCAL_QUORUM, so pin the insert to LOCAL_QUORUM.
            let mut insert = session
                .prepare(format!("INSERT INTO {sc_ks}.t (pk, v) VALUES (?, ?)"))
                .await
                .unwrap();
            insert.set_consistency(Consistency::LocalQuorum);
            session.execute_unpaged(&insert, (PK, 1)).await.unwrap();

            // A strong read (LOCAL_QUORUM) is a leader-requiring request.
            let mut select = session
                .prepare(format!("SELECT v FROM {sc_ks}.t WHERE pk = ?"))
                .await
                .unwrap();
            select.set_consistency(Consistency::LocalQuorum);

            let (mut rx_exec, mut rx_resp) = capture_executes_and_results(&mut running_proxy);

            // Warm the cache, and in doing so learn who leads this tablet. From a cold cache
            // every request sends a random tablet-version block, so the server answers with
            // routing information -- and that payload is the only place it says which replica
            // leads the Raft group.
            const WARMUP: usize = 32;
            execute_concurrently(&session, &select, &(PK,), WARMUP)
                .await
                .unwrap();

            let mut leader = None;
            while let Ok((frame, _shard)) = rx_resp.try_recv() {
                if let Some(replicas) = decode_v2_payload_replicas(frame) {
                    leader = replicas.first().map(|(host, _shard)| *host);
                }
            }
            let leader = Cell::new(leader.expect(
                "server never sent a tablets-routing-v2 payload during warm-up, so the \
                     tablet's leader could not be determined",
            ));

            // A migration or a Raft re-election can move the leader while the batch below is in
            // flight, which would fail the check through no fault of the driver. Either shows up
            // as the server re-sending routing information, so treat that as "retry", adopting
            // the leader it just reported.
            with_migration_retry(
                async || cached_tablet_routing(&session, &sc_ks, PK).await,
                async |_| {
                    drain(&mut rx_exec);
                    drain(&mut rx_resp);

                    const ITERATIONS: usize = 20;
                    let coordinators =
                        execute_concurrently(&session, &select, &(PK,), ITERATIONS).await?;

                    let mut moved = false;
                    while let Ok((frame, _shard)) = rx_resp.try_recv() {
                        if let Some(replicas) = decode_v2_payload_replicas(frame) {
                            if let Some((host, _shard)) = replicas.first() {
                                leader.set(*host);
                            }
                            moved = true;
                        }
                    }
                    if moved {
                        return Err(
                            "the server re-sent routing information mid-measurement, so \
                                    the tablet migrated or re-elected; retrying against the \
                                    leader it just reported"
                                .to_owned(),
                        );
                    }

                    // With the cache warm and the mapping stable, every strong read for this
                    // tablet must be coordinated by the leader.
                    let expected = leader.get();
                    if let Some(other) = coordinators.iter().find(|c| **c != expected) {
                        return Err(format!(
                            "strong read coordinated by {other} but the tablet's Raft leader is \
                             {expected}; leader-aware routing did not target the leader"
                        ));
                    }
                    Ok(())
                },
            )
            .await;

            session
                .ddl(format!("DROP KEYSPACE IF EXISTS {sc_ks}"))
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

/// `TABLETS_ROUTING_V1` must keep working on a server that does not offer V2.
///
/// V2 subsumes V1 and the driver negotiates exactly one of them, so the V1 path would silently
/// stop being exercised the moment a server starts advertising V2. Hiding the V2 extension from
/// every node puts the driver back on V1 against that same cluster, which must still:
///
/// - send no trailing tablet-version block on any `EXECUTE` - that byte belongs to V2 alone, and
///   a V1 server would read it as the start of the next frame;
/// - receive `tablets-routing-v1` payloads, i.e. the V1 feedback path really runs;
/// - converge: once the cache is warm, every request for a key reaches one of that tablet's
///   replicas.
#[tokio::test]
async fn test_tablets_routing_v1_used_when_v2_unavailable() {
    setup_tracing();

    // Deliberately *not* gated on the server supporting V2. On a server that offers V2 the rule
    // below forces the driver back onto V1; on one that does not, the rule is a no-op and the
    // driver is on V1 anyway. Either way this is a V1 test, so it runs in ordinary CI too --
    // which is the point, since V1 is not going away when V2 ships.
    let mut v1_features = fetch_negotiated_features(None).await;
    // With V2 hidden the driver negotiates V1, so its EXECUTE frames must be decoded with V2
    // turned off but every other negotiated feature left on.
    v1_features.tablets_v2_supported = false;

    let res = test_with_3_node_cluster(
        ShardAwareness::QueryNode,
        |proxy_uris, translation_map, mut running_proxy| async move {
            // Hide the extension from every node, so no connection negotiates V2.
            for node in running_proxy.running_nodes.iter_mut() {
                node.change_response_rules(Some(vec![hide_v2_extension_rule()]));
            }

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

            const PK: i32 = 7;
            session
                .query_unpaged(format!("INSERT INTO {ks}.t (pk, v) VALUES ({PK}, 1)"), &())
                .await
                .unwrap();

            let select = session
                .prepare(format!("SELECT v FROM {ks}.t WHERE pk = ?"))
                .await
                .unwrap();

            let (mut rx_exec, mut rx_resp) = capture_executes_and_results(&mut running_proxy);

            // Warm the cache, once. V1 teaches the driver a tablet's replicas whenever it
            // contacts a shard that does not own it, so from a cold cache a few requests suffice
            // to converge. Run them concurrently: they are independent, and applying the same
            // feedback repeatedly is idempotent.
            const WARMUP: usize = 12;
            execute_concurrently(&session, &select, &(PK,), WARMUP)
                .await
                .unwrap();

            let mut saw_v1_payload = false;
            let mut v2_payloads = 0usize;
            while let Ok((frame, _shard)) = rx_resp.try_recv() {
                saw_v1_payload |=
                    response_carries_tablets_payload(frame.clone(), CUSTOM_PAYLOAD_TABLETS_V1_KEY);
                if response_carries_tablets_payload(frame, CUSTOM_PAYLOAD_TABLETS_V2_KEY) {
                    v2_payloads += 1;
                }
            }
            assert!(
                saw_v1_payload,
                "server never returned a tablets-routing-v1 payload; the V1 feedback path was \
                 not exercised"
            );
            // The two extensions are mutually exclusive, so a V1 connection must never see V2
            // routing information. This is the direct check that we really are on V1, rather
            // than inferring it from the V1 payload above.
            assert_eq!(
                v2_payloads, 0,
                "a V1 connection received {v2_payloads} tablets-routing-v2 payload(s); the \
                 driver must not have negotiated V2"
            );

            // The warm-up EXECUTEs must already be free of the block, from the very first
            // request. Checked here because the measured phase below drains the channel.
            let mut warmup_executes = 0usize;
            while let Ok((frame, _shard)) = rx_exec.try_recv() {
                assert_eq!(
                    decode_execute_block(frame, &v1_features),
                    None,
                    "EXECUTE on a V1 connection must not carry a tablet-version block"
                );
                warmup_executes += 1;
            }
            assert!(
                warmup_executes >= WARMUP,
                "expected to capture at least {WARMUP} warm-up EXECUTE frames, captured \
                 {warmup_executes}"
            );

            // A migration mid-batch would move the tablet's replicas out from under the check
            // below, so snapshot the driver's routing view around it and read the expected
            // replica set from that same snapshot.
            with_migration_retry(
                async || cached_tablet_routing(&session, &ks, PK).await,
                async |routing| {
                    drain(&mut rx_exec);

                    // Once warm, every request must reach a replica of the tablet.
                    const ITERATIONS: usize = 20;
                    let coordinators =
                        execute_concurrently(&session, &select, &(PK,), ITERATIONS).await?;
                    let replicas: Vec<Uuid> = routing.iter().map(|(host, _shard)| *host).collect();
                    if let Some(other) = coordinators.iter().find(|c| !replicas.contains(c)) {
                        return Err(format!(
                            "request coordinated by {other}, which is not a replica of the \
                             tablet ({replicas:?}); the V1 routing cache did not converge"
                        ));
                    }

                    // No EXECUTE may carry the V2-only trailing block byte.
                    // `decode_execute_block` also asserts the frame decodes with nothing left
                    // over, so an accidentally appended byte is caught here rather than
                    // corrupting the next frame.
                    let mut executes_seen = 0usize;
                    while let Ok((frame, _shard)) = rx_exec.try_recv() {
                        if decode_execute_block(frame, &v1_features).is_some() {
                            return Err("EXECUTE on a V1 connection must not carry a \
                                        tablet-version block"
                                .to_owned());
                        }
                        executes_seen += 1;
                    }
                    if executes_seen < ITERATIONS {
                        return Err(format!(
                            "expected to capture at least {ITERATIONS} EXECUTE frames, captured \
                             {executes_seen}"
                        ));
                    }

                    Ok(())
                },
            )
            .await;

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
