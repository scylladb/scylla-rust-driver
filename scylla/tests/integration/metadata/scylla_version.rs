use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;

use scylla::client::session_builder::SessionBuilder;
use scylla::policies::address_translator::AddressTranslator;
use scylla_proxy::{
    Condition, ProxyError, Reaction, RequestOpcode, RequestReaction, RequestRule, ShardAwareness,
    WorkerError,
};
use tokio::sync::mpsc;

use crate::utils::{create_new_session_builder, setup_tracing, test_with_3_node_cluster};

/// ScyllaDB nodes report the `system.versions` value; Cassandra nodes report `None`.
/// Runs on both backends.
#[tokio::test]
async fn test_scylla_version_matches_backend() {
    setup_tracing();

    let session = create_new_session_builder().build().await.unwrap();
    let state = session.get_cluster_state();

    // ScyllaDB-only and local; on Cassandra the query errors → no version. All nodes match.
    let expected: Option<String> = match session
        .query_unpaged("SELECT version FROM system.versions", ())
        .await
    {
        Ok(result) => result
            .into_rows_result()
            .ok()
            .and_then(|rows| rows.single_row::<(Option<String>,)>().ok())
            .and_then(|(v,)| v),
        Err(_) => None,
    };

    for node in state.get_nodes_info() {
        if node.sharder().is_some() {
            assert_eq!(
                node.scylla_version(),
                expected.as_deref(),
                "ScyllaDB node {} should match system.versions (and be populated)",
                node.address
            );
            assert!(
                node.scylla_version().is_some(),
                "ScyllaDB node {}",
                node.address
            );
        } else {
            assert_eq!(
                node.scylla_version(),
                None,
                "Cassandra node {} should have no scylla_version",
                node.address
            );
        }
    }
}

/// A `system.versions` failure on one node leaves its version `None`, others populated, and
/// the driver usable. Covers a server error and a dropped response (timeout) in one cluster.
#[tokio::test]
#[cfg_attr(cassandra_tests, ignore)]
async fn test_scylla_version_degrades_on_query_failure() {
    setup_tracing();

    // Connect a fresh session (queries system.versions against the rule active on node 0), then
    // assert node 0 has no version, the others do, and the driver still works.
    async fn assert_node0_degrades(
        proxy_uri: &str,
        translator: Arc<dyn AddressTranslator>,
        node0_real_ip: IpAddr,
        scenario: &str,
    ) {
        let session = SessionBuilder::new()
            .known_node(proxy_uri)
            .address_translator(translator)
            .build()
            .await
            .unwrap();

        for node in session.get_cluster_state().get_nodes_info() {
            if node.address.ip() == node0_real_ip {
                assert_eq!(
                    node.scylla_version(),
                    None,
                    "{scenario}: node {}",
                    node.address
                );
            } else {
                assert!(
                    node.scylla_version().is_some(),
                    "{scenario}: node {}",
                    node.address
                );
            }
        }

        // Driver still usable despite the failure on one node.
        session
            .query_unpaged("SELECT * FROM system.local", ())
            .await
            .unwrap();
    }

    let versions_rule = |reaction: RequestReaction| {
        RequestRule(
            Condition::RequestOpcode(RequestOpcode::Query).and(
                Condition::BodyContainsCaseInsensitive(Box::from(*b"system.versions")),
            ),
            reaction,
        )
    };

    let res = test_with_3_node_cluster(
        ShardAwareness::QueryNode,
        |proxy_uris, translation_map, mut running_proxy| async move {
            // translation_map maps real_addr → proxy_addr. Reverse it to find the real address
            // behind proxy node 0 (where the rule is applied), so we can identify it in the
            // cluster state (which stores real addresses).
            let proxy0_addr: SocketAddr = proxy_uris[0].parse().unwrap();
            let node0_real_ip = translation_map
                .iter()
                .find(|(_, proxy)| **proxy == proxy0_addr)
                .map(|(real, _)| real.ip())
                .expect("proxy0 address not found in translation_map");
            let translator: Arc<dyn AddressTranslator> = Arc::new(translation_map);

            // Server error on system.versions.
            running_proxy.running_nodes[0].change_request_rules(Some(vec![versions_rule(
                RequestReaction::forge().server_error(),
            )]));
            assert_node0_degrades(
                proxy_uris[0].as_str(),
                Arc::clone(&translator),
                node0_real_ip,
                "server error",
            )
            .await;

            // Dropped response — the frame never arrives, so the driver's timeout fires.
            running_proxy.running_nodes[0]
                .change_request_rules(Some(vec![versions_rule(RequestReaction::drop_frame())]));
            assert_node0_degrades(
                proxy_uris[0].as_str(),
                Arc::clone(&translator),
                node0_real_ip,
                "timeout",
            )
            .await;

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

/// A node whose initial `system.versions` query failed is retried on the next refresh.
#[tokio::test]
#[cfg_attr(cassandra_tests, ignore)]
async fn test_scylla_version_repopulated_after_transient_failure() {
    setup_tracing();

    let (feedback_tx, mut feedback_rx) = mpsc::unbounded_channel();

    let error_rule = RequestRule(
        Condition::RequestOpcode(RequestOpcode::Query).and(Condition::BodyContainsCaseInsensitive(
            Box::from(*b"system.versions"),
        )),
        RequestReaction::forge()
            .server_error()
            .with_feedback_when_performed(feedback_tx),
    );

    let res = test_with_3_node_cluster(
        ShardAwareness::QueryNode,
        |proxy_uris, translation_map, mut running_proxy| async move {
            running_proxy.running_nodes[0].change_request_rules(Some(vec![error_rule]));

            let proxy0_addr: SocketAddr = proxy_uris[0].parse().unwrap();
            let node0_real_ip = translation_map
                .iter()
                .find(|(_, proxy)| **proxy == proxy0_addr)
                .map(|(real, _)| real.ip())
                .expect("proxy0 not in translation_map");

            let session = SessionBuilder::new()
                .known_node(proxy_uris[0].as_str())
                .address_translator(Arc::new(translation_map))
                .build()
                .await
                .unwrap();

            while feedback_rx.try_recv().is_ok() {}

            // Node 0 should have None — the query was blocked during connect.
            let node0_version_after_connect = session
                .get_cluster_state()
                .get_nodes_info()
                .iter()
                .find(|n| n.address.ip() == node0_real_ip)
                .unwrap()
                .scylla_version()
                .map(str::to_owned);
            assert_eq!(node0_version_after_connect, None);

            // Remove the block. system.versions on node 0 will now respond normally.
            running_proxy.running_nodes[0].change_request_rules(None);

            // Trigger a refresh. No topology changed, so node 0's Arc is reused.
            session.refresh_metadata().await.unwrap();

            // Node 0 should now have a version — the transient failure is resolved.
            let node0_version_after_refresh = session
                .get_cluster_state()
                .get_nodes_info()
                .iter()
                .find(|n| n.address.ip() == node0_real_ip)
                .unwrap()
                .scylla_version()
                .map(str::to_owned);
            assert!(
                node0_version_after_refresh.is_some(),
                "Node {} still has scylla_version = None after the transient failure was \
                 resolved — system.versions is not retried for unchanged nodes",
                node0_real_ip
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

/// Unchanged nodes are not re-queried across a no-op metadata refresh.
#[tokio::test]
#[cfg_attr(cassandra_tests, ignore)]
async fn test_scylla_version_not_requeried_for_unchanged_nodes() {
    setup_tracing();

    let (feedback_tx, mut feedback_rx) = mpsc::unbounded_channel();

    let count_rule = RequestRule(
        Condition::RequestOpcode(RequestOpcode::Query).and(Condition::BodyContainsCaseInsensitive(
            Box::from(*b"system.versions"),
        )),
        RequestReaction::noop().with_feedback_when_performed(feedback_tx),
    );

    let res = test_with_3_node_cluster(
        ShardAwareness::QueryNode,
        |proxy_uris, translation_map, mut running_proxy| async move {
            for node in &mut running_proxy.running_nodes {
                node.change_request_rules(Some(vec![count_rule.clone()]));
            }

            let session = SessionBuilder::new()
                .known_node(proxy_uris[0].as_str())
                .address_translator(Arc::new(translation_map))
                .build()
                .await
                .unwrap();

            let mut connect_count = 0;
            while feedback_rx.try_recv().is_ok() {
                connect_count += 1;
            }
            assert_eq!(
                connect_count, 3,
                "expected one system.versions query per node on connect"
            );

            let sorted_versions = |state: &scylla::cluster::ClusterState| {
                let mut v: Vec<_> = state
                    .get_nodes_info()
                    .iter()
                    .map(|n| (n.address, n.scylla_version().map(str::to_owned)))
                    .collect();
                v.sort_by_key(|(addr, _)| *addr);
                v
            };
            let versions_before = sorted_versions(&session.get_cluster_state());

            // trigger refresh with no topology changes
            // No new system.versions queries should have been issued for unchanged nodes
            session.refresh_metadata().await.unwrap();
            assert!(
                feedback_rx.try_recv().is_err(),
                "system.versions was re-queried for unchanged nodes"
            );

            // existing values must be preserved not reset to None
            let versions_after = sorted_versions(&session.get_cluster_state());
            assert_eq!(
                versions_before, versions_after,
                "scylla_version changed across a no-op metadata refresh"
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
