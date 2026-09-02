use crate::utils::{setup_tracing, test_with_3_node_cluster};
use scylla::client::session::Session;
use scylla::client::session_builder::SessionBuilder;
use scylla_cql::frame::request::options;
use scylla_cql::frame::types;
use scylla_proxy::{
    Condition, ProxyError, Reaction, RequestFrame, RequestOpcode, RequestReaction, RequestRule,
    ShardAwareness, WorkerError,
};
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

/// Receives exactly `count` STARTUP frames, asserting that each of them carries
/// a `SESSION_ID` option reporting `session_id`.
///
/// Receiving a known count is the barrier that makes this test deterministic:
/// `Session::connect` only waits for the first connection to each node
/// (`ClusterState::wait_until_all_pools_are_initialized`), so the remaining pool
/// connections are opened asynchronously by the pool refiller and their frames
/// arrive later. Awaiting them is sleep-free; any surplus frame stays queued.
async fn recv_session_ids(
    startup_rx: &mut mpsc::UnboundedReceiver<(RequestFrame, Option<u16>)>,
    session_id: Uuid,
    count: usize,
) {
    let expected = session_id.to_string();
    let mut received = 0;
    let recv_all = async {
        while received < count {
            let (startup_frame, _shard) = startup_rx.recv().await.unwrap();
            let startup_options = types::read_string_map(&mut &*startup_frame.body).unwrap();
            let reported = startup_options
                .get(options::SESSION_ID)
                .expect("STARTUP frame without a SESSION_ID option");
            assert_eq!(
                *reported, expected,
                "unexpected SESSION_ID reported in STARTUP"
            );
            received += 1;
        }
    };

    tokio::time::timeout(Duration::from_secs(30), recv_all)
        .await
        .unwrap_or_else(|_| {
            panic!("received only {received} of {count} expected STARTUP frames for {expected}")
        });
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
            recv_session_ids(&mut startup_rx, session.session_id(), expected).await;

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
