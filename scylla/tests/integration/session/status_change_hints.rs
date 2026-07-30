use std::net::SocketAddr;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::Duration;

use bytes::{Bytes, BytesMut};
use scylla::client::PoolSize;
use scylla::client::session::Session;
use scylla::client::session_builder::SessionBuilder;
use scylla_proxy::{
    Condition, ProxyError, Reaction, RequestFrame, RequestOpcode, RequestReaction, RequestRule,
    RunningProxy, ShardAwareness, TargetShard, WorkerError,
};
use tokio::sync::mpsc;
use uuid::Uuid;

use crate::utils::{
    calculate_proxy_host_ids, setup_tracing, test_with_3_node_cluster,
    wait_until_all_nodes_are_connected,
};

/// Keepalive interval used by the test session.
///
/// Chosen far beyond any realistic test duration, so that no *periodic*
/// keepalive can possibly fire. Consequently, every `OPTIONS` frame observed
/// after the session became fully connected is attributable to the
/// `STATUS_CHANGE DOWN` hint under test.
const UNREACHABLE_KEEPALIVE_INTERVAL: Duration = Duration::from_secs(10_000);

/// Upper bound for waiting on driver reactions. Generous, because it is only
/// hit when the driver is broken - a healthy driver reacts in microseconds.
const REACTION_TIMEOUT: Duration = Duration::from_secs(10);

/// Exactly one connection per node.
///
/// This makes the test deterministic in two ways: a pool is complete as soon
/// as it holds one connection (so no connection handshake can emit a stray
/// `OPTIONS` once the session is up), and a single hint provokes exactly one
/// keepalive per node - as opposed to the default `PerShard(1)`, where the
/// number of connections, and hence of keepalives, depends on the node's
/// shard count.
const POOL_SIZE: PoolSize = PoolSize::PerHost(NonZeroUsize::new(1).unwrap());

type FeedbackItem = (RequestFrame, Option<TargetShard>);
type FeedbackRx = mpsc::UnboundedReceiver<FeedbackItem>;

/// Builds the body of a `STATUS_CHANGE` / `DOWN` EVENT frame for `addr`:
/// `[string] "STATUS_CHANGE"`, `[string] "DOWN"`, `[inet] addr`.
fn status_change_down_event_body(addr: SocketAddr) -> Bytes {
    use scylla_cql::frame::types::{write_inet, write_string};

    let mut body = BytesMut::new();
    write_string("STATUS_CHANGE", &mut body).unwrap();
    write_string("DOWN", &mut body).unwrap();
    write_inet(addr, &mut body);
    body.freeze()
}

/// Returns the driver-visible address of the node with the given host id.
///
/// This is the address that `ClusterState` compares against the address
/// carried by a `STATUS_CHANGE` event, so it is the address that must be put
/// into the forged event. Depending on whether the node was learnt from
/// metadata or is a contact point, it is either the broadcast (translatable)
/// or the already-translated address - both are handled uniformly here.
fn driver_visible_address(session: &Session, host_id: Uuid) -> SocketAddr {
    let state = session.get_cluster_state();
    let node = state
        .get_node_by_host_id(host_id)
        .unwrap_or_else(|| panic!("node with host id {host_id} unknown to the driver"));
    SocketAddr::new(node.address.ip(), node.address.port())
}

/// Injects a forged `STATUS_CHANGE DOWN` event for `addr` into every proxy
/// node's control connections.
///
/// Only one of the three proxied nodes hosts the driver's control connection,
/// hence the broadcast; at least one injection must have found a registered
/// control connection.
fn inject_status_change_down(running_proxy: &RunningProxy, addr: SocketAddr) {
    let body = status_change_down_event_body(addr);
    let injected = running_proxy
        .running_nodes
        .iter()
        .filter(|node| node.inject_event_to_cc(body.clone()))
        .count();
    assert!(
        injected > 0,
        "no proxy node had a registered control connection to inject the event into"
    );
}

/// Verifies that a `STATUS_CHANGE DOWN` server event is used as a hint for the
/// keepaliver: upon receiving it, the driver immediately sends a CQL keepalive
/// (`OPTIONS`) on the pool connections to exactly the node named by the event,
/// instead of waiting for the next `keepalive_interval` tick.
///
/// This matters because a supposedly-DOWN node's connections are likely
/// defunct; probing them right away either closes them (so the node stops
/// being targeted by the load balancing policy) or proves the node is still
/// alive and the event was stale.
///
/// The session's keepalive interval is [`UNREACHABLE_KEEPALIVE_INTERVAL`]
/// (10_000 s), which cannot elapse while the test runs, and the feedback rules
/// are installed only once all pools are complete - so no periodic keepalive
/// and no connection handshake can account for any observed `OPTIONS`. The
/// only remaining explanation is the DOWN hint.
///
/// Every node is targeted in turn, which proves that the hint is
/// routed to the single node named by the event rather than broadcast to the
/// whole cluster. The event is forged by the proxy, so the test needs no
/// server-side support and runs identically against ScyllaDB and Cassandra.
#[tokio::test]
async fn test_status_change_down_triggers_immediate_keepalive() {
    setup_tracing();

    let res = test_with_3_node_cluster(
        ShardAwareness::QueryNode,
        |proxy_uris, translation_map, mut running_proxy| async move {
            let session = SessionBuilder::new()
                .known_node(proxy_uris[0].as_str())
                .address_translator(Arc::new(translation_map.clone()))
                .keepalive_interval(UNREACHABLE_KEEPALIVE_INTERVAL)
                .pool_size(POOL_SIZE)
                .fetch_schema_metadata(false)
                .cluster_metadata_refresh_interval(UNREACHABLE_KEEPALIVE_INTERVAL)
                .build()
                .await
                .unwrap();

            // The DOWN hint acts on *existing* pool connections, so the test
            // must not start before the pools are complete: the OPTIONS frame
            // of a connection handshake is indistinguishable from a keepalive
            // one. With POOL_SIZE a pool is complete as soon as it holds its
            // single connection, which is what `is_connected()` reports.
            wait_until_all_nodes_are_connected(3, &session).await;

            let host_ids = calculate_proxy_host_ids(&proxy_uris, &translation_map, &session);
            let node_addrs: Vec<SocketAddr> = host_ids
                .iter()
                .map(|&host_id| driver_visible_address(&session, host_id))
                .collect();

            // Installed only now, once all pools are complete: the OPTIONS
            // frames of the connection handshakes are already in the past, so
            // they cannot be mistaken for hint-driven keepalives.
            // `Condition::not(ConnectionRegisteredAnyEvent)` excludes the
            // control connection, leaving only pool connections.
            let keepalive_condition = Condition::RequestOpcode(RequestOpcode::Options)
                .and(Condition::not(Condition::ConnectionRegisteredAnyEvent));
            let mut keepalive_rxs: Vec<FeedbackRx> = Vec::with_capacity(3);
            for node in running_proxy.running_nodes.iter_mut() {
                let (tx, rx) = mpsc::unbounded_channel::<FeedbackItem>();
                node.prepend_request_rules(vec![RequestRule(
                    keepalive_condition.clone(),
                    RequestReaction::noop().with_feedback_when_performed(tx),
                )]);
                keepalive_rxs.push(rx);
            }

            // Each sub-case names a different node, which discriminates a
            // per-node hint from a cluster-wide broadcast. All assertions are
            // exact: with one connection per node, one hint must produce
            // exactly one keepalive, on exactly one node.
            for target in 0..3 {
                // The same node is named twice in a row, and the second
                // keepalive is awaited before any channel is inspected. The
                // second event is injected only after the first keepalive was
                // forwarded by the proxy, hence after the keepaliver consumed
                // the first hint - so a keepalive that the first hint wrongly
                // provoked on another node has necessarily been forwarded by
                // then too. Without that second round trip, the emptiness
                // assertion below could race a broadcast bug and miss it,
                // since each connection's keepaliver runs in its own task.
                for hint in 1..=2 {
                    inject_status_change_down(&running_proxy, node_addrs[target]);

                    tokio::time::timeout(REACTION_TIMEOUT, keepalive_rxs[target].recv())
                        .await
                        .unwrap_or_else(|_| {
                            panic!(
                                "node {target} ({}) received no keepalive OPTIONS within \
                                 {REACTION_TIMEOUT:?} after STATUS_CHANGE DOWN event #{hint} \
                                 naming it",
                                node_addrs[target]
                            )
                        })
                        .expect("keepalive feedback channel closed unexpectedly");
                }

                // Every channel must now be empty: no other node may have been
                // probed, and the target must have been probed exactly twice.
                // Periodic keepalives are 10_000 s away and the pools are
                // complete, so nothing else can emit OPTIONS.
                for (node_idx, rx) in keepalive_rxs.iter_mut().enumerate() {
                    assert!(
                        rx.try_recv().is_err(),
                        "surplus keepalive OPTIONS on node {node_idx} ({}) after two \
                         STATUS_CHANGE DOWN events naming node {target} ({})",
                        node_addrs[node_idx],
                        node_addrs[target]
                    );
                }
            }

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
