use std::sync::Arc;

use assert_matches::assert_matches;
use scylla::client::PoolSize;
use scylla::client::session::Session;
use scylla::client::session_builder::SessionBuilder;
use scylla::errors::{DbError, ExecutionError, RequestAttemptError, SchemaAgreementError};
use scylla::policies::load_balancing::{NodeIdentifier, SingleTargetLoadBalancingPolicy};
use scylla::response::query_result::QueryResult;
use scylla::statement::Statement;
use scylla_proxy::{
    Condition, ProxyError, Reaction, RequestOpcode, RequestReaction, RequestRule, RunningProxy,
    ShardAwareness, WorkerError,
};
use tracing::info;

use crate::utils::{
    calculate_proxy_host_ids, setup_tracing, test_with_3_node_cluster, unique_keyspace_name,
};

async fn run_some_ddl_with_unreachable_node(
    coordinator: NodeIdentifier,
    paused: usize,
    session: &Session,
    running_proxy: &mut RunningProxy,
) -> Result<QueryResult, ExecutionError> {
    // Prevent fetching schema version.
    // It simulates a node that became unreachable after our DDL completed,
    // but the pool in the driver is not yet `Broken`.
    running_proxy.running_nodes[paused].change_request_rules(Some(vec![RequestRule(
        Condition::not(Condition::ConnectionRegisteredAnyEvent)
            .and(Condition::RequestOpcode(RequestOpcode::Query))
            .and(Condition::BodyContainsCaseSensitive(Box::new(
                *b"system.local",
            ))),
        // Simulates driver discovering that node is unreachable.
        RequestReaction::drop_connection(),
    )]));

    let ks = unique_keyspace_name();
    let mut request = Statement::new(format!(
        "CREATE KEYSPACE {ks} WITH REPLICATION = {{'class' : 'NetworkTopologyStrategy', 'replication_factor' : 1}}"
    ));
    request.set_load_balancing_policy(Some(SingleTargetLoadBalancingPolicy::new(
        coordinator,
        None,
    )));

    let result = session.query_unpaged(request, &[]).await;

    // Cleanup
    running_proxy.running_nodes[paused].change_request_rules(Some(vec![]));
    session.await_schema_agreement().await.unwrap();
    session
        .query_unpaged(format!("DROP KEYSPACE {ks}"), &[])
        .await
        .unwrap();

    result
}

// Verifies that auto schema agreement (performed after receiving response of DDL request) works correctly
// when a node is paused.
#[tokio::test]
#[cfg_attr(scylla_cloud_tests, ignore)]
async fn test_schema_await_with_unreachable_node() {
    setup_tracing();

    let res = test_with_3_node_cluster(
        ShardAwareness::QueryNode,
        |proxy_uris, translation_map, mut running_proxy| async move {
            // DB preparation phase
            let session: Session = SessionBuilder::new()
                .known_node(proxy_uris[0].as_str())
                .address_translator(Arc::new(translation_map.clone()))
                .build()
                .await
                .unwrap();

            let host_ids = calculate_proxy_host_ids(&proxy_uris, &translation_map, &session);

            {
                // Case 1: Paused node is a coordinator for DDL.
                // DDL needs to fail.
                let result = run_some_ddl_with_unreachable_node(
                    NodeIdentifier::HostId(host_ids[1]),
                    1,
                    &session,
                    &mut running_proxy,
                )
                .await;
                assert_matches!(
                    result,
                    Err(ExecutionError::SchemaAgreementError(
                        SchemaAgreementError::RequestError(
                            RequestAttemptError::BrokenConnectionError(_)
                        )
                    ))
                )
            }

            {
                // Case 2: Paused node is NOT a coordinator for DDL.
                // DDL should succeed, because auto schema agreement only needs available nodes to agree.
                let result = run_some_ddl_with_unreachable_node(
                    NodeIdentifier::HostId(host_ids[2]),
                    1,
                    &session,
                    &mut running_proxy,
                )
                .await;
                assert_matches!(result, Ok(_))
            }

            {
                // Case 3: Paused node is a coordinator for DDL, and is used by control connection.
                // It is the same as case 1, but paused node is also control connection.
                // DDL needs to fail.
                let result = run_some_ddl_with_unreachable_node(
                    NodeIdentifier::HostId(host_ids[0]),
                    0,
                    &session,
                    &mut running_proxy,
                )
                .await;
                assert_matches!(
                    result,
                    Err(ExecutionError::SchemaAgreementError(
                        SchemaAgreementError::RequestError(
                            RequestAttemptError::BrokenConnectionError(_)
                        )
                    ))
                )
            }

            {
                // Case 4: Paused node is NOT a coordinator for DDL, but is used by control connection.
                // It is the same as case 2, but paused node is also control connection.
                // DDL should succeed, because auto schema agreement only needs available nodes to agree,
                // and control connection is not used for that at all.
                let result = run_some_ddl_with_unreachable_node(
                    NodeIdentifier::HostId(host_ids[1]),
                    0,
                    &session,
                    &mut running_proxy,
                )
                .await;
                assert_matches!(result, Ok(_))
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

// Verifies that schema agreement process works correctly even if the first check fails.
#[tokio::test]
#[cfg_attr(scylla_cloud_tests, ignore)]
async fn test_schema_await_with_transient_failure() {
    setup_tracing();

    let res = test_with_3_node_cluster(
        ShardAwareness::QueryNode,
        |proxy_uris, translation_map, mut running_proxy| async move {
            // DB preparation phase
            let session: Session = SessionBuilder::new()
                .known_node(proxy_uris[0].as_str())
                .address_translator(Arc::new(translation_map.clone()))
                // Important in order to have a predictable amount of connections after session creation.
                // Shard connections are created asynchronously, so it's hard to predict how many will be opened
                // already when we check schema agreement.
                .pool_size(PoolSize::PerHost(1.try_into().unwrap()))
                .build()
                .await
                .unwrap();

            let node_rules = Some(vec![RequestRule(
                Condition::not(Condition::ConnectionRegisteredAnyEvent)
                    .and(Condition::RequestOpcode(RequestOpcode::Query))
                    .and(Condition::BodyContainsCaseSensitive(Box::new(
                        *b"system.local",
                    )))
                    .and(Condition::TrueForLimitedTimes(1)),
                // Use error that would prevent DefaultRetryPolicy from retrying.
                // I don't think it is used for those queries, but it's additional future-proofing
                // for the test.
                RequestReaction::forge_with_error(DbError::SyntaxError),
            )]);

            // First, a sanity check for proxy rules.
            // If for each node first request fails (and subsequent requests succeed),
            // then first schema agreement check should return error, and second should succeed.
            // Note that this is only true because we configured the session with 1-connection-per-node.
            info!("Starting phase 1 - sanity check");
            {
                running_proxy
                    .running_nodes
                    .iter_mut()
                    .for_each(|node| node.change_request_rules(node_rules.clone()));

                // The important check: first call should error out, second one should succeed.
                session.check_schema_agreement().await.unwrap_err();
                session.check_schema_agreement().await.unwrap();

                running_proxy
                    .running_nodes
                    .iter_mut()
                    .for_each(|node| node.change_request_rules(Some(vec![])));
            }

            // Now let's check that awaiting schema agreement doesn't bail on error.
            // I'll use the same proxy rules as before, so first check will error out.
            info!("Starting phase 2 - main test");
            {
                running_proxy
                    .running_nodes
                    .iter_mut()
                    .for_each(|node| node.change_request_rules(node_rules.clone()));

                session.await_schema_agreement().await.unwrap();
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
