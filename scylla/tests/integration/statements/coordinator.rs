use std::net::{IpAddr, SocketAddr};
use std::str::FromStr;
use std::sync::Arc;

use assert_matches::assert_matches;
use futures::StreamExt;
use scylla::client::pager::QueryPager;
use scylla::client::session::Session;
use scylla::client::session_builder::SessionBuilder;
use scylla::errors::ExecutionError;
use scylla::policies::load_balancing::{NodeIdentifier, SingleTargetLoadBalancingPolicy};
use scylla::response::query_result::QueryResult;
use scylla::response::{Coordinator, PagingState};
use scylla::routing::Shard;
use scylla::statement::Statement;
use scylla::statement::batch::{Batch, BatchStatement, BatchType};
use scylla_cql::frame::response::Supported;
use scylla_cql::frame::types;
use scylla_proxy::{
    Condition, Node, Proxy, ProxyError, Reaction, ResponseFrame, ResponseOpcode, ResponseReaction,
    ResponseRule, ShardAwareness, WorkerError,
};
use uuid::Uuid;

use crate::utils::{
    PerformDDL, calculate_proxy_host_ids, create_new_session_builder, setup_tracing,
    unique_keyspace_name,
};

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
    let ks = unique_keyspace_name();

    // Create schema for batches.
    {
        session
            .ddl(
                format!(
                    "CREATE KEYSPACE IF NOT EXISTS {ks} WITH replication = {{'class': 'NetworkTopologyStrategy', 'replication_factor': 1}}"
                ),
            )
            .await
            .unwrap();
        session.use_keyspace(&ks, true).await.unwrap();
        session
            .ddl("CREATE TABLE IF NOT EXISTS test (a int PRIMARY KEY)")
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

    session.ddl(format!("DROP KEYSPACE {ks}")).await.unwrap();
}

/// Regression test for the bug where `Coordinator` reported the shard
/// requested by the load-balancing policy instead of the shard that
/// the connection actually belongs to.
///
/// When a connection for the requested shard does not exist (here: because the
/// proxy was configured to make it look like that shard is mapped to a
/// different shard number), the connection pool falls back to a connection
/// on a different shard. The `Coordinator` must reflect the shard of the
/// *actual* connection used, not the shard the LBP asked for.
///
/// Setup:
/// - A single-node proxy sits in front of the first cluster node.
/// - The proxy intercepts every `SUPPORTED` response and rewrites
///   `SCYLLA_SHARD=<shard>` to `SCYLLA_SHARD=0`, so the driver
///   never learns about a connection to shards other than 0.
/// - We then request the last shard of that node via
///   `SingleTargetLoadBalancingPolicy`.
/// - The pool falls back to a shard-0 connection.
/// - We assert `coordinator.shard() == Some(0)`, not `Some(non-0)`.
#[tokio::test]
async fn test_coordinator_shard_fallback() {
    setup_tracing();

    let real_uri = std::env::var("SCYLLA_URI").unwrap_or_else(|_| "172.42.0.2:9042".to_string());
    let proxy_uri = format!("{}:9042", scylla_proxy::get_exclusive_local_address());

    let real_addr: SocketAddr = real_uri.parse().unwrap();
    let proxy_addr: SocketAddr = proxy_uri.parse().unwrap();

    let proxy = Proxy::new([Node::builder()
        .real_address(real_addr)
        .proxy_address(proxy_addr)
        .shard_awareness(ShardAwareness::QueryNode)
        .build()]);

    let translation_map = proxy.translation_map();
    let mut running_proxy = proxy.run().await.unwrap();

    // Intercept SUPPORTED responses and remap other shards to shard 0,
    // so that the driver never populates other shards' connection slots.
    // We do this by changing SCYLLA_SHARD to "0".
    running_proxy.running_nodes[0].change_response_rules(Some(vec![ResponseRule(
        Condition::ResponseOpcode(ResponseOpcode::Supported),
        ResponseReaction::transform_frame(Arc::new(|mut frame: ResponseFrame| {
            let mut msg = Supported::deserialize(&mut &*frame.body).unwrap();

            // If the server advertises SCYLLA_SHARD, we set it to 0, so that the driver
            // never learns about a connection to shards different than 0.
            // This will cause the connection pool to fall back to shard 0 when we request
            // any different shard.
            if msg
                .options
                .get("SCYLLA_SHARD")
                .and_then(|v| v.first())
                .map(|s| s.as_str())
                .is_some()
            {
                msg.options
                    .insert("SCYLLA_SHARD".to_owned(), vec!["0".to_owned()]);

                let mut new_body = Vec::new();
                types::write_string_multimap(&msg.options, &mut new_body).unwrap();
                frame.body = new_body.into();
            }

            frame
        })),
    )]));

    let session = SessionBuilder::new()
        .known_node(&proxy_uri)
        .address_translator(Arc::new(translation_map.clone()))
        .build()
        .await
        .unwrap();

    // Find the node that is behind the proxy (the one with the proxy's
    // address in the translation map).
    let [proxied_node_host_id] =
        calculate_proxy_host_ids(&[proxy_uri], &translation_map, &session)[..]
    else {
        panic!("expected exactly one proxied node");
    };

    let cluster_state = session.get_cluster_state();
    let node = cluster_state
        .get_nodes_info()
        .iter()
        .find(|n| n.host_id == proxied_node_host_id)
        .expect("proxied node not found in cluster state");

    let nr_shards = match node.sharder() {
        Some(s) if s.nr_shards.get() >= 2 => s.nr_shards.get() as Shard,
        // Cassandra or single-shard ScyllaDB – skip.
        _ => {
            let _ = running_proxy.finish().await;
            return;
        }
    };

    // Request the last shard, which has no connection (the proxy erased
    // it), so the pool will fall back to shard 0.
    let last_shard: Shard = nr_shards - 1;
    let policy = SingleTargetLoadBalancingPolicy::new(
        NodeIdentifier::HostId(proxied_node_host_id),
        Some(last_shard),
    );

    let mut stmt = Statement::new("SELECT host_id FROM system.local WHERE key='local'");
    stmt.set_load_balancing_policy(Some(policy));

    // --- query_unpaged path (goes through session.rs) ---
    let result = session.query_unpaged(stmt.clone(), ()).await.unwrap();
    let rows = result.into_rows_result().unwrap();
    let coordinator = rows.request_coordinator();
    assert_eq!(
        coordinator.shard(),
        Some(0),
        "query_unpaged: expected shard 0 (actual connection), \
         got {:?} (LBP requested {})",
        coordinator.shard(),
        last_shard,
    );

    // --- query_iter path (goes through pager.rs) ---
    let mut rows_stream = session
        .query_iter(stmt, ())
        .await
        .unwrap()
        .rows_stream::<(Uuid,)>()
        .unwrap();
    // Consume the single row so that the coordinator is recorded.
    let _ = rows_stream.next().await.unwrap().unwrap();
    let coordinator = rows_stream.request_coordinators().next().unwrap();
    assert_eq!(
        coordinator.shard(),
        Some(0),
        "query_iter: expected shard 0 (actual connection), \
         got {:?} (LBP requested {})",
        coordinator.shard(),
        last_shard,
    );

    match running_proxy.finish().await {
        Ok(()) => (),
        Err(ProxyError::Worker(WorkerError::DriverDisconnected(_))) => (),
        Err(err) => panic!("proxy error: {err}"),
    }
}
