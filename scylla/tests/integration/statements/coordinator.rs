use std::net::{IpAddr, SocketAddr};
use std::str::FromStr;
use std::sync::Arc;

use assert_matches::assert_matches;
use futures::StreamExt;
use scylla::client::pager::QueryPager;
use scylla::client::session::Session;
use scylla::errors::ExecutionError;

use scylla::policies::load_balancing::{NodeIdentifier, SingleTargetLoadBalancingPolicy};
use scylla::response::query_result::QueryResult;
use scylla::response::{Coordinator, PagingState};
use scylla::routing::Shard;
use scylla::statement::batch::{Batch, BatchStatement, BatchType};
use scylla::statement::Statement;
use uuid::Uuid;

use crate::utils::{create_new_session_builder, setup_tracing, unique_keyspace_name};

#[tokio::test]
async fn test_enforce_request_coordinator() {
    setup_tracing();
    let session = create_new_session_builder().build().await.unwrap();

    async fn query_system_local_and_verify(
        session: &Session,
        node_identifier: NodeIdentifier,
        expected_node_id: Uuid,
        expected_node_ip: IpAddr,
    ) {
        let mut statement =
            Statement::new("SELECT host_id, rpc_address FROM system.local WHERE key='local'");
        statement.set_load_balancing_policy(Some(SingleTargetLoadBalancingPolicy::new(
            node_identifier,
            None,
        )));

        // Check query_unpaged
        let (actual_node_id, actual_node_ip) = session
            .query_unpaged(statement.clone(), ())
            .await
            .unwrap()
            .into_rows_result()
            .unwrap()
            .single_row::<(uuid::Uuid, IpAddr)>()
            .unwrap();
        assert_eq!(expected_node_id, actual_node_id);
        assert_eq!(expected_node_ip, actual_node_ip);

        // Check query_iter
        let (actual_node_id, actual_node_ip) = session
            .query_iter(statement, ())
            .await
            .unwrap()
            .rows_stream::<(uuid::Uuid, IpAddr)>()
            .unwrap()
            .next()
            .await
            .unwrap()
            .unwrap();
        assert_eq!(expected_node_id, actual_node_id);
        assert_eq!(expected_node_ip, actual_node_ip);
    }

    let cluster_state = session.get_cluster_state();
    for node in cluster_state.get_nodes_info() {
        let (node_id, node_address) = (
            node.host_id,
            SocketAddr::new(node.address.ip(), node.address.port()),
        );

        query_system_local_and_verify(
            &session,
            NodeIdentifier::Node(Arc::clone(node)),
            node_id,
            node_address.ip(),
        )
        .await;

        query_system_local_and_verify(
            &session,
            NodeIdentifier::HostId(node_id),
            node_id,
            node_address.ip(),
        )
        .await;

        query_system_local_and_verify(
            &session,
            NodeIdentifier::NodeAddress(SocketAddr::new(node_address.ip(), node_address.port())),
            node_id,
            node_address.ip(),
        )
        .await;
    }
}

#[tokio::test]
async fn test_enforce_non_existent_request_coordinator() {
    setup_tracing();
    let session = create_new_session_builder().build().await.unwrap();

    let mut statement =
        Statement::new("SELECT host_id, rpc_address FROM system.local WHERE key='local'");
    statement.set_load_balancing_policy(Some(SingleTargetLoadBalancingPolicy::new(
        NodeIdentifier::NodeAddress(SocketAddr::from_str("1.1.1.1:9042").unwrap()),
        None,
    )));

    let result = session.query_unpaged(statement, ()).await;
    assert_matches!(result, Err(ExecutionError::EmptyPlan))
}

/// Checks that if a node is enforced as the coordinator of a request, the [Coordinator] struct
/// exposed on the request result (`QueryResult` and `QueryPager`) contains that `Node`.
#[tokio::test]
async fn test_exposed_request_coordinator() {
    setup_tracing();
    let session = create_new_session_builder().build().await.unwrap();

    // Create schema for batches.
    {
        let ks = unique_keyspace_name();
        session
            .query_unpaged(
                format!(
                    "CREATE KEYSPACE IF NOT EXISTS {ks} WITH replication = {{'class': 'NetworkTopologyStrategy', 'replication_factor': 1}}"
                ),
                (),
            )
            .await
            .unwrap();
        session.use_keyspace(ks, true).await.unwrap();
        session
            .query_unpaged("CREATE TABLE IF NOT EXISTS test (a int PRIMARY KEY)", ())
            .await
            .unwrap();
    }
    let mut batch = Batch::new_with_statements(
        BatchType::Logged,
        vec![BatchStatement::Query(Statement::new(
            "INSERT INTO test (a) VALUES (?)",
        ))],
    );

    let mut statement =
        Statement::new("SELECT host_id, rpc_address FROM system.local WHERE key='local'");
    let mut prepared = session.prepare(statement.clone()).await.unwrap();

    let cluster_state = session.get_cluster_state();
    for node in cluster_state.get_nodes_info() {
        let num_shards = node
            .sharder()
            .map_or(0, |sharder| sharder.nr_shards.get() as Shard);
        for requested_shard in std::iter::once(None).chain((0..num_shards).map(Some)) {
            let policy = Some(SingleTargetLoadBalancingPolicy::new(
                NodeIdentifier::Node(Arc::clone(node)),
                requested_shard,
            ));
            batch.set_load_balancing_policy(policy.clone());
            statement.set_load_balancing_policy(policy.clone());
            prepared.set_load_balancing_policy(policy);

            let expected_node_id = node.host_id;
            let expected_node_ip = node.address.ip();

            let check_coordinator = |coordinator: &Coordinator| {
                assert_eq!(coordinator.node(), node);

                // If we requested no shard, in ScyllaDB we still target some (unspecified) shard, not None.
                // That's why don't expect the Coordinator's shard to be None if requested_shard is None.
                if let Some(shard) = requested_shard {
                    assert_eq!(coordinator.shard(), Some(shard));
                }
            };

            let check_query_results_coordinator = |query_result: QueryResult| {
                let query_rows_result = query_result.into_rows_result().unwrap();

                let (actual_node_id, actual_node_ip) = query_rows_result
                    .single_row::<(uuid::Uuid, IpAddr)>()
                    .unwrap();
                assert_eq!(expected_node_id, actual_node_id);
                assert_eq!(expected_node_ip, actual_node_ip);

                let coordinator = query_rows_result.request_coordinator();
                check_coordinator(coordinator);
            };

            let check_query_pagers_coordinator = |query_pager: QueryPager| async {
                let mut rows_stream = query_pager.rows_stream::<(uuid::Uuid, IpAddr)>().unwrap();

                let (actual_node_id, actual_node_ip) = rows_stream.next().await.unwrap().unwrap();
                assert_eq!(expected_node_id, actual_node_id);
                assert_eq!(expected_node_ip, actual_node_ip);

                let coordinator = rows_stream.request_coordinators().next().unwrap();
                check_coordinator(coordinator);
            };

            // Check {query,execute}_{unpaged,single_page}
            {
                // Check query_unpaged().
                let query_result = session.query_unpaged(statement.clone(), ()).await.unwrap();
                check_query_results_coordinator(query_result);

                // Check execute_unpaged().
                let query_result = session.execute_unpaged(&prepared, ()).await.unwrap();
                check_query_results_coordinator(query_result);

                // Check query_single_page().
                let (query_result, _) = session
                    .query_single_page(statement.clone(), (), PagingState::start())
                    .await
                    .unwrap();
                check_query_results_coordinator(query_result);

                // Check execute_single_page().
                let (query_result, _) = session
                    .execute_single_page(&prepared, (), PagingState::start())
                    .await
                    .unwrap();
                check_query_results_coordinator(query_result);
            }

            // Check {query,execute}_iter
            {
                let query_pager = session.query_iter(statement.clone(), ()).await.unwrap();
                check_query_pagers_coordinator(query_pager).await;

                let query_pager = session.execute_iter(prepared.clone(), ()).await.unwrap();
                check_query_pagers_coordinator(query_pager).await;
            }

            // Check batch()
            {
                // This always inserts the same row, so effectively the row is inserted only once.
                // This is, however, not important for this test. What we care about is just the coordinator.
                let query_result = session.batch(&batch, ((0,),)).await.unwrap();
                let coordinator = query_result.request_coordinator();
                check_coordinator(coordinator);
            }
        }
    }
}
