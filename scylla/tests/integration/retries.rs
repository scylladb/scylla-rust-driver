use crate::utils::{setup_tracing, test_with_3_node_cluster, unique_keyspace_name, PerformDDL};
use scylla::client::execution_profile::ExecutionProfile;
use scylla::client::session::Session;
use scylla::client::session_builder::SessionBuilder;
use scylla::policies::retry::FallthroughRetryPolicy;
use scylla::policies::speculative_execution::SimpleSpeculativeExecutionPolicy;
use scylla::query::Query;
use std::sync::Arc;
use std::time::Duration;
use tracing::info;

use scylla_proxy::{
    Condition, ProxyError, Reaction, RequestOpcode, RequestReaction, RequestRule, ShardAwareness,
    WorkerError,
};

#[tokio::test]
#[ntest::timeout(30000)]
#[cfg(not(scylla_cloud_tests))]
async fn retries_occur() {
    setup_tracing();
    let res = test_with_3_node_cluster(ShardAwareness::QueryNode, |proxy_uris, translation_map, mut running_proxy| async move {

        // DB preparation phase
        let session: Session = SessionBuilder::new()
            .known_node(proxy_uris[0].as_str())
            .address_translator(Arc::new(translation_map))
            .build()
            .await
            .unwrap();

        let ks = unique_keyspace_name();
        session.ddl(format!("CREATE KEYSPACE IF NOT EXISTS {} WITH REPLICATION = {{'class' : 'NetworkTopologyStrategy', 'replication_factor' : 3}}", ks)).await.unwrap();
        session.use_keyspace(ks, false).await.unwrap();
        session
            .ddl("CREATE TABLE t (a int primary key)")
            .await
            .unwrap();

        let mut q = Query::from("INSERT INTO t (a) VALUES (?)");
        q.set_is_idempotent(true); // this is to allow retry to fire

        let forge_error_rule = RequestRule(
            Condition::RequestOpcode(RequestOpcode::Prepare)
                .and(Condition::BodyContainsCaseSensitive(Box::new(*b"INTO t"))),
            RequestReaction::forge().server_error(),
        );

        info!("--------------------- BEGINNING main test part ----------------");

        info!("--------------------- first query - no rules  ----------------");
        session.query_unpaged(q.clone(), (3,)).await.unwrap();

        info!("--------------------- second query - 0 and 2 nodes not responding  ----------------");
        running_proxy.running_nodes[0]
            .change_request_rules(Some(vec![forge_error_rule.clone()]));
        running_proxy.running_nodes[2]
            .change_request_rules(Some(vec![forge_error_rule.clone()]));

        session.query_unpaged(q.clone(), (2,)).await.unwrap();

        info!("--------------------- third query - all nodes not responding  ----------------");
        running_proxy.running_nodes[1]
            .change_request_rules(Some(vec![forge_error_rule]));

        session.query_unpaged(q.clone(), (1,)).await.unwrap_err();

        info!("--------------------- fourth query - 0 and 1 nodes not responding  ----------------");
        running_proxy.running_nodes[2]
        .change_request_rules(None);

        session.query_unpaged(q, (1,)).await.unwrap();

        info!("--------------------- FINISHING main test part ----------------");

        running_proxy
    }).await;

    match res {
        Ok(()) => (),
        Err(ProxyError::Worker(WorkerError::DriverDisconnected(_))) => (),
        Err(err) => panic!("{}", err),
    }
}

// See https://github.com/scylladb/scylla-rust-driver/issues/1085
#[tokio::test]
#[ntest::timeout(30000)]
#[cfg(not(scylla_cloud_tests))]
async fn speculative_execution_panic_regression_test() {
    use scylla_proxy::RunningProxy;

    setup_tracing();
    let test = |proxy_uris: [String; 3], translation_map, mut running_proxy: RunningProxy| async move {
        let se = SimpleSpeculativeExecutionPolicy {
            max_retry_count: 2,
            retry_interval: Duration::from_millis(1),
        };
        let profile = ExecutionProfile::builder()
            .speculative_execution_policy(Some(Arc::new(se)))
            .retry_policy(Arc::new(FallthroughRetryPolicy))
            .build();
        // DB preparation phase
        let session: Session = SessionBuilder::new()
            .known_node(proxy_uris[0].as_str())
            .address_translator(Arc::new(translation_map))
            .default_execution_profile_handle(profile.into_handle())
            .build()
            .await
            .unwrap();

        let ks = unique_keyspace_name();
        session.ddl(format!("CREATE KEYSPACE IF NOT EXISTS {} WITH REPLICATION = {{'class' : 'NetworkTopologyStrategy', 'replication_factor' : 3}}", ks)).await.unwrap();
        session.use_keyspace(ks, false).await.unwrap();
        session
            .ddl("CREATE TABLE t (a int primary key)")
            .await
            .unwrap();

        let mut q = session
            .prepare("INSERT INTO t (a) VALUES (?)")
            .await
            .unwrap();
        q.set_is_idempotent(true); // this is to allow speculative execution to fire
        let id: &[u8] = q.get_id();

        let drop_connection_on_execute = RequestRule(
            Condition::RequestOpcode(RequestOpcode::Execute)
                .and(Condition::not(Condition::ConnectionRegisteredAnyEvent))
                .and(Condition::BodyContainsCaseSensitive(id.into())),
            RequestReaction::drop_connection(),
        );

        running_proxy.running_nodes[0]
            .change_request_rules(Some(vec![drop_connection_on_execute.clone()]));
        running_proxy.running_nodes[1]
            .change_request_rules(Some(vec![drop_connection_on_execute.clone()]));
        running_proxy.running_nodes[2]
            .change_request_rules(Some(vec![drop_connection_on_execute.clone()]));

        let _result = session.execute_unpaged(&q, (2,)).await.unwrap_err();

        running_proxy
    };
    let res = test_with_3_node_cluster(ShardAwareness::QueryNode, test).await;

    match res {
        Ok(()) => (),
        Err(ProxyError::Worker(WorkerError::DriverDisconnected(_))) => (),
        Err(err) => panic!("{}", err),
    }
}
