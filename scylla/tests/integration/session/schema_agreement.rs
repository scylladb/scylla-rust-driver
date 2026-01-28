use std::sync::Arc;
use std::time::Duration;

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
    util_session: &Session,
) -> Result<QueryResult, ExecutionError> {
    running_proxy.running_nodes.iter_mut().for_each(|node| {
        node.change_request_rules(Some(vec![
            // First check goes without trouble
            RequestRule(
                Condition::not(Condition::ConnectionRegisteredAnyEvent)
                    .and(Condition::RequestOpcode(RequestOpcode::Execute))
                    .and(Condition::TrueForLimitedTimes(1)),
                RequestReaction::noop(),
            ),
            // Second check stalls on non-paused nodes, to time it out and
            // return the error stored from first attempt.
            RequestRule(
                Condition::not(Condition::ConnectionRegisteredAnyEvent)
                    .and(Condition::RequestOpcode(RequestOpcode::Execute)),
                RequestReaction::delay(Duration::from_secs(10)),
            ),
        ]))
    });

    running_proxy.running_nodes[paused].prepend_request_rules(vec![RequestRule(
        Condition::not(Condition::ConnectionRegisteredAnyEvent)
            .and(Condition::RequestOpcode(RequestOpcode::Execute)),
        // Simulates driver discovering that node is unreachable.
        RequestReaction::drop_connection(),
    )]);

    let ks = unique_keyspace_name();
    let mut request = Statement::new(format!(
        "CREATE KEYSPACE {ks}
            WITH REPLICATION = {{'class': 'NetworkTopologyStrategy', 'replication_factor': 1}}"
    ));
    request.set_load_balancing_policy(Some(SingleTargetLoadBalancingPolicy::new(
        coordinator,
        None,
    )));

    let result = session.query_unpaged(request, &[]).await;

    // Cleanup
    running_proxy
        .running_nodes
        .iter_mut()
        .for_each(|node| node.change_request_rules(Some(vec![])));
    util_session
        .query_unpaged(format!("DROP KEYSPACE {ks}"), &[])
        .await
        .unwrap();

    result
}

// Verifies that auto schema agreement (performed after receiving response of DDL request) works correctly
// when a node is paused.
// How does it work?
// There are 4 sub-tests. For each subtest a fresh session is created, because previous subtest may have
// broken some connections, and we need fully connected session.
// Each subtest performs DDL (which triggers agreement wait), and checks the result.
// When coordinator is the same as unreachable node, we expect agreement to fail.
// When they are different, it should succeed.
// Node is simulated as paused by dropping connection on schema request.
// Normally, for coordinator this would cause `RequiredHostAbsent` error because
// all connections to required host would be dropped. We prevent that by not allowing more
// than one agreement check: during second check, other nodes will delay the reponse, triggering
// schema agreement timeout, which will return the error stored during the first check (broken connection).
#[tokio::test]
async fn test_schema_await_with_unreachable_node() {
    setup_tracing();

    let res = test_with_3_node_cluster(
        ShardAwareness::QueryNode,
        |proxy_uris, translation_map, mut running_proxy| async move {
            let normal_timeout = Duration::from_millis(60000);
            let short_timeout = Duration::from_millis(300);

            // DB preparation phase
            let builder = SessionBuilder::new()
                .known_node(proxy_uris[0].as_str())
                .address_translator(Arc::new(translation_map.clone()))
                // Important in order to have a predictable amount of connections after session creation.
                // Shard connections are created asynchronously, so it's hard to predict how many will be opened
                // already when we check schema agreement.
                .pool_size(PoolSize::PerHost(1.try_into().unwrap()))
                // Let's try more often to prevent timeouts.
                .schema_agreement_interval(Duration::from_millis(5));

            // Session used for cleanups (DROP TABLE) and some other actions needed in test.
            let normal_session = builder
                .clone()
                .schema_agreement_timeout(normal_timeout)
                .build()
                .await
                .unwrap();

            let host_ids = calculate_proxy_host_ids(&proxy_uris, &translation_map, &normal_session);

            {
                tracing::info!("================= Sub test 1 =================");

                let session: Session = builder
                    .clone()
                    .schema_agreement_timeout(short_timeout)
                    .build()
                    .await
                    .unwrap();

                // Case 1: Paused node is a coordinator for DDL.
                // DDL needs to fail.
                let result = run_some_ddl_with_unreachable_node(
                    NodeIdentifier::HostId(host_ids[1]),
                    1,
                    &session,
                    &mut running_proxy,
                    &normal_session,
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
                tracing::info!("================= Sub test 2 =================");

                let session: Session = builder
                    .clone()
                    .schema_agreement_timeout(normal_timeout)
                    .build()
                    .await
                    .unwrap();

                // Case 2: Paused node is NOT a coordinator for DDL.
                // DDL should succeed, because auto schema agreement only needs available nodes to agree.
                let result = run_some_ddl_with_unreachable_node(
                    NodeIdentifier::HostId(host_ids[2]),
                    1,
                    &session,
                    &mut running_proxy,
                    &normal_session,
                )
                .await;
                assert_matches!(result, Ok(_))
            }

            {
                tracing::info!("================= Sub test 3 =================");

                let session: Session = builder
                    .clone()
                    .schema_agreement_timeout(short_timeout)
                    .build()
                    .await
                    .unwrap();

                // Case 3: Paused node is a coordinator for DDL, and is used by control connection.
                // It is the same as case 1, but paused node is also control connection.
                // DDL needs to fail.
                let result = run_some_ddl_with_unreachable_node(
                    NodeIdentifier::HostId(host_ids[0]),
                    0,
                    &session,
                    &mut running_proxy,
                    &normal_session,
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
                tracing::info!("================= Sub test 4 =================");

                let session: Session = builder
                    .clone()
                    .schema_agreement_timeout(normal_timeout)
                    .build()
                    .await
                    .unwrap();

                // Case 4: Paused node is NOT a coordinator for DDL, but is used by control connection.
                // It is the same as case 2, but paused node is also control connection.
                // DDL should succeed, because auto schema agreement only needs available nodes to agree,
                // and control connection is not used for that at all.
                let result = run_some_ddl_with_unreachable_node(
                    NodeIdentifier::HostId(host_ids[1]),
                    0,
                    &session,
                    &mut running_proxy,
                    &normal_session,
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
async fn test_schema_await_with_transient_failure() {
    setup_tracing();

    let res = test_with_3_node_cluster(
        ShardAwareness::QueryNode,
        |proxy_uris, translation_map, mut running_proxy| async move {
            // DB preparation phase
            let builder = SessionBuilder::new()
                .known_node(proxy_uris[0].as_str())
                .address_translator(Arc::new(translation_map.clone()))
                // Important in order to have a predictable amount of connections after session creation.
                // Shard connections are created asynchronously, so it's hard to predict how many will be opened
                // already when we check schema agreement.
                .pool_size(PoolSize::PerHost(1.try_into().unwrap()))
                // Let's try more often to prevent timeouts.
                .schema_agreement_interval(Duration::from_millis(30));

            let node_rules = Some(vec![RequestRule(
                Condition::not(Condition::ConnectionRegisteredAnyEvent)
                    .and(Condition::RequestOpcode(RequestOpcode::Execute))
                    .and(Condition::TrueForLimitedTimes(1)),
                RequestReaction::forge_with_error(DbError::Overloaded),
            )]);

            // First, a sanity check for proxy rules.
            // If for each node first request fails (and subsequent requests succeed),
            // then first schema agreement check should return error, and second should succeed.
            // Note that this is only true because we configured the session with 1-connection-per-node.
            info!("Starting phase 1 - sanity check");
            {
                let session: Session = builder.clone().build().await.unwrap();
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
                let session: Session = builder.clone().build().await.unwrap();
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

// Test that produces SchemaAgreementError::RequiredHostAbsent to prove that
// such condition is possible, and handled correctly.
#[tokio::test]
async fn test_schema_await_required_host_absent() {
    setup_tracing();

    let res = test_with_3_node_cluster(
        ShardAwareness::QueryNode,
        |proxy_uris, translation_map, mut running_proxy| async move {
            // DB preparation phase
            let session: Session = SessionBuilder::new()
                .known_node(proxy_uris[0].as_str())
                .address_translator(Arc::new(translation_map.clone()))
                // Needed to have pools filled immediately after session creation.
                .pool_size(PoolSize::PerHost(1.try_into().unwrap()))
                // Schema agreement will only return error after timeout,
                // so without this line the test would take over 60s.
                .schema_agreement_timeout(Duration::from_secs(1))
                .schema_agreement_interval(Duration::from_millis(50))
                .build()
                .await
                .unwrap();

            let host_ids = calculate_proxy_host_ids(&proxy_uris, &translation_map, &session);

            let coordinator = NodeIdentifier::HostId(host_ids[1]);

            running_proxy.running_nodes[1].change_request_rules(Some(vec![
                // This prevents opening new connections to the node
                RequestRule(
                    Condition::RequestOpcode(RequestOpcode::Startup),
                    RequestReaction::drop_connection(),
                ),
                // This prevents schema agreement check on this node, and closes connection.
                // After some attempts, no connections will be left.
                RequestRule(
                    Condition::not(Condition::ConnectionRegisteredAnyEvent)
                        .and(Condition::RequestOpcode(RequestOpcode::Execute)),
                    RequestReaction::drop_connection(),
                ),
            ]));

            let ks = unique_keyspace_name();
            let mut request = Statement::new(format!(
                "CREATE KEYSPACE {ks}
                    WITH REPLICATION = {{'class': 'NetworkTopologyStrategy', 'replication_factor': 1}}"
            ));
            request.set_load_balancing_policy(Some(SingleTargetLoadBalancingPolicy::new(
                coordinator,
                None,
            )));

            let result = session.query_unpaged(request, &[]).await;
            let Err(ExecutionError::SchemaAgreementError(
                SchemaAgreementError::RequiredHostAbsent(host),
            )) = result
            else {
                panic!("Unexpected error type: {:?}", result);
            };

            assert_eq!(host, host_ids[1]);

            // Cleanup
            running_proxy.running_nodes[1].change_request_rules(Some(vec![]));
            session.await_schema_agreement().await.unwrap();
            session
                .query_unpaged(format!("DROP KEYSPACE {ks}"), &[])
                .await
                .unwrap();

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
