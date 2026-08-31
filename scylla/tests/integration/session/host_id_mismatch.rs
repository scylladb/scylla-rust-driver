//! Verifies that the driver reports (at ERROR level) a connection pool whose connections land on a
//! node other than the one the pool serves.
//!
//! The mismatch is provoked with an address translator that redirects every node address the driver
//! discovers - including the local node's own, which `query_peers` reads from `system.local` and
//! marks translatable just like the `system.peers` rows - to node 0's proxy address. All three pools
//! therefore connect to node 0 while the driver still believes it keeps one pool per host ID.
//! Detection relies on the node advertising its host ID under the `SCYLLA_HOST_ID` key of the
//! `SUPPORTED` response, so the test first inspects the real server's `SUPPORTED` frame and returns
//! early on clusters (e.g. Cassandra) lacking it.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::str::FromStr;
use std::sync::Arc;

use scylla::client::PoolSize;
use scylla::client::session_builder::SessionBuilder;
use scylla_cql::frame::request::options::SCYLLA_HOST_ID;
use scylla_cql::frame::response::Supported;
use scylla_proxy::{
    Condition, ProxyError, Reaction, ResponseOpcode, ResponseReaction, ResponseRule,
    ShardAwareness, WorkerError,
};
use uuid::Uuid;

use crate::utils::{
    CapturedLogs, HEALTHCHECK_QUERY, run_capturing_errors, setup_tracing, test_with_3_node_cluster,
    wait_until_all_nodes_are_connected,
};

#[test]
fn mismatched_host_id_is_reported() {
    setup_tracing();
    let captured = CapturedLogs::default();
    let logs_of_run = captured.clone();

    let res = run_capturing_errors(
        &captured,
        test_with_3_node_cluster(
            ShardAwareness::Unaware,
            |proxy_uris, translation_map, mut running_proxy| async move {
                // Installed before any session exists, so that the handshake's SUPPORTED frame is
                // observed. The proxy forwards SUPPORTED unchanged, so this is the real server's
                // advertisement.
                let (supported_tx, mut supported_rx) = tokio::sync::mpsc::unbounded_channel();
                running_proxy.running_nodes[0].change_response_rules(Some(vec![ResponseRule(
                    Condition::ResponseOpcode(ResponseOpcode::Supported),
                    ResponseReaction::noop().with_feedback_when_performed(supported_tx),
                )]));

                // Every node address the driver discovers is translated to node 0's proxy address. The
                // map must cover all three nodes, the local one included: `query_peers` marks it
                // translatable too, so a missing entry would fail its pool with `NoRuleForAddress`.
                // The driver thus keeps three `Node`s with three distinct host IDs, while each of the
                // three pools opens its connections to node 0, which reports its own host ID - a
                // mismatch for two of the three pools.
                let first_proxy_addr = SocketAddr::from_str(&proxy_uris[0]).unwrap();
                let all_to_first: HashMap<SocketAddr, SocketAddr> = translation_map
                    .keys()
                    .map(|&real| (real, first_proxy_addr))
                    .collect();

                let session = SessionBuilder::new()
                    // Contact points are never translated, so the control connection reaches node 0
                    // normally and metadata fetch works.
                    .known_node(proxy_uris[0].as_str())
                    .address_translator(Arc::new(all_to_first))
                    // Exactly one connection, hence exactly one report, per pool.
                    .pool_size(PoolSize::PerHost(1.try_into().unwrap()))
                    .disallow_shard_aware_port(true)
                    .fetch_schema_metadata(false)
                    // Off as well, so that the session does not warn about the two flags disagreeing.
                    .fetch_full_schema_metadata(false)
                    .keyspaces_to_fetch(std::iter::empty::<String>())
                    .build()
                    .await
                    .unwrap();

                let supported_frame = supported_rx.recv().await.unwrap().0;
                let options = Supported::deserialize(&mut &*supported_frame.body)
                    .unwrap()
                    .options;
                if !options.contains_key(SCYLLA_HOST_ID) {
                    // `println!` as well as the log, because the default `EnvFilter` drops WARN and this
                    // notice is the only sign that the test asserted nothing.
                    let msg = format!(
                        "SKIPPING ASSERTIONS: the cluster does not advertise the {SCYLLA_HOST_ID} key \
                     in its SUPPORTED response, so the driver cannot detect host ID mismatches at \
                     all and this test can verify nothing."
                    );
                    println!("{msg}");
                    tracing::warn!("{msg}");
                    return running_proxy;
                }

                // Race-free without any sleep: `verify_host_id` logs the mismatch before
                // `update_shared_conns` publishes the connections, so a pool cannot be seen as
                // connected before its report has reached the capture buffer.
                wait_until_all_nodes_are_connected(3, &session).await;

                // All connections land on node 0, so any query is answered by node 0 and
                // `system.local.host_id` - which the spec guarantees to equal the host ID advertised in
                // SUPPORTED - identifies the node the pools actually reach.
                let (reached_host_id,): (Uuid,) = session
                    .query_unpaged(HEALTHCHECK_QUERY, &[])
                    .await
                    .unwrap()
                    .into_rows_result()
                    .unwrap()
                    .single_row::<(Uuid,)>()
                    .unwrap();

                let logs = logs_of_run.text();
                let reached_str = reached_host_id.to_string();
                let reports: Vec<&str> = logs
                    .lines()
                    .filter(|line| line.contains(&reached_str))
                    .collect();

                let mismatching: Vec<Uuid> = session
                    .get_cluster_state()
                    .get_nodes_info()
                    .iter()
                    .map(|node| node.host_id)
                    .filter(|&host_id| host_id != reached_host_id)
                    .collect();
                assert_eq!(
                    mismatching.len(),
                    2,
                    "expected the 3-node harness to yield 2 host IDs other than the reached one \
                 ({reached_host_id}), got {mismatching:?}"
                );

                assert!(
                    !reports.is_empty(),
                    "no ERROR log mentions the reached host ID {reached_host_id}; captured logs:\n{logs}"
                );

                for host_id in &mismatching {
                    assert!(
                        reports
                            .iter()
                            .any(|line| line.contains(&host_id.to_string())),
                        "no mismatch reported for the pool of host ID {host_id} (reached host ID is \
                     {reached_host_id}); captured logs:\n{logs}"
                    );
                }

                for line in &reports {
                    assert!(
                        mismatching
                            .iter()
                            .any(|host_id| line.contains(&host_id.to_string())),
                        "a mismatch was reported for a pool that reached its own node ({reached_host_id}): \
                     {line}\ncaptured logs:\n{logs}"
                    );
                }

                running_proxy
            },
        ),
    );

    match res {
        Ok(()) | Err(ProxyError::Worker(WorkerError::DriverDisconnected(_))) => (),
        Err(err) => panic!("{err}"),
    }
}
