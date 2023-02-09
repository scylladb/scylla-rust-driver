mod utils;

use scylla::retry_policy::FallthroughRetryPolicy;
use scylla::speculative_execution::SimpleSpeculativeExecutionPolicy;
use scylla::transport::session::Session;
use scylla::ExecutionProfile;
use scylla::SessionBuilder;
use scylla::{query::Query, test_utils::unique_keyspace_name};
use std::sync::Arc;
use std::time::Duration;
use tracing::info;
use utils::test_with_3_node_cluster;

use scylla_proxy::{
    Condition, ProxyError, Reaction, RequestOpcode, RequestReaction, RequestRule, ShardAwareness,
    WorkerError,
};

#[tokio::test]
#[ntest::timeout(30000)]
async fn speculative_execution_is_fired() {
    const TIMEOUT_PER_REQUEST: Duration = Duration::from_millis(1000);

    let res = test_with_3_node_cluster(ShardAwareness::QueryNode, |proxy_uris, translation_map, mut running_proxy| async move {
        // DB preparation phase
        let simple_speculative_no_retry_profile = ExecutionProfile::builder().speculative_execution_policy(Some(Arc::new(SimpleSpeculativeExecutionPolicy {
            max_retry_count: 2,
            retry_interval: Duration::from_millis(10),
        }))).retry_policy(Box::new(FallthroughRetryPolicy)).build();
        let session: Session = SessionBuilder::new()
            .known_node(proxy_uris[0].as_str())
            .default_execution_profile_handle(simple_speculative_no_retry_profile.into_handle())
            .address_translator(Arc::new(translation_map))
            .build()
            .await
            .unwrap();

        let ks = unique_keyspace_name();
        session.query(format!("CREATE KEYSPACE IF NOT EXISTS {} WITH REPLICATION = {{'class' : 'SimpleStrategy', 'replication_factor' : 3}}", ks), &[]).await.unwrap();
        session.use_keyspace(ks, false).await.unwrap();
        session
            .query("CREATE TABLE t (a int primary key)", &[])
            .await
            .unwrap();

        let mut q = Query::from("INSERT INTO t (a) VALUES (?)");
        q.set_is_idempotent(true); // this is to allow speculative execution to fire

        let drop_frame_rule = RequestRule(
            Condition::RequestOpcode(RequestOpcode::Query)
                .and(Condition::BodyContainsCaseSensitive(Box::new(*b"t"))),
            RequestReaction::drop_frame(),
        );

        info!("--------------------- BEGINNING main test part ----------------");

        info!("--------------------- first query - no rules  ----------------");
        // first run before any rules
        session.query(q.clone(), (3,)).await.unwrap();

        info!("--------------------- second query - 0 and 2 nodes not responding  ----------------");
        running_proxy.running_nodes[0]
            .change_request_rules(Some(vec![drop_frame_rule.clone()]));
        running_proxy.running_nodes[2]
            .change_request_rules(Some(vec![drop_frame_rule.clone()]));

        session.query(q.clone(), (2,)).await.unwrap();

        info!("--------------------- third query - 0 and 1 nodes not responding  ----------------");
        running_proxy.running_nodes[2]
            .change_request_rules(None);
        running_proxy.running_nodes[1]
            .change_request_rules(Some(vec![drop_frame_rule.clone()]));

        session.query(q.clone(), (1,)).await.unwrap();


        info!("--------------------- fourth query - all nodes not responding  ----------------");
        running_proxy.running_nodes[2]
        .change_request_rules(Some(vec![drop_frame_rule]));

        tokio::select! {
            res = session.query(q, (0,)) => panic!("Rules did not work: received response {:?}", res),
            _ = tokio::time::sleep(TIMEOUT_PER_REQUEST) => (),
        };

        info!("--------------------- FINISHING main test part ----------------");

        running_proxy
    }).await;

    match res {
        Ok(()) => (),
        Err(ProxyError::Worker(WorkerError::DriverDisconnected(_))) => (),
        Err(err) => panic!("{}", err),
    }
}

#[tokio::test]
#[ntest::timeout(30000)]
async fn retries_occur() {
    let res = test_with_3_node_cluster(ShardAwareness::QueryNode, |proxy_uris, translation_map, mut running_proxy| async move {

        // DB preparation phase
        let session: Session = SessionBuilder::new()
            .known_node(proxy_uris[0].as_str())
            .address_translator(Arc::new(translation_map))
            .build()
            .await
            .unwrap();

        let ks = unique_keyspace_name();
        session.query(format!("CREATE KEYSPACE IF NOT EXISTS {} WITH REPLICATION = {{'class' : 'SimpleStrategy', 'replication_factor' : 3}}", ks), &[]).await.unwrap();
        session.use_keyspace(ks, false).await.unwrap();
        session
            .query("CREATE TABLE t (a int primary key)", &[])
            .await
            .unwrap();

        let mut q = Query::from("INSERT INTO t (a) VALUES (?)");
        q.set_is_idempotent(true); // this is to allow retry to fire

        let forge_error_rule = RequestRule(
            Condition::RequestOpcode(RequestOpcode::Query)
                .and(Condition::BodyContainsCaseSensitive(Box::new(*b"INTO t"))),
            RequestReaction::forge().server_error(),
        );

        info!("--------------------- BEGINNING main test part ----------------");

        info!("--------------------- first query - no rules  ----------------");
        session.query(q.clone(), (3,)).await.unwrap();

        info!("--------------------- second query - 0 and 2 nodes not responding  ----------------");
        running_proxy.running_nodes[0]
            .change_request_rules(Some(vec![forge_error_rule.clone()]));
        running_proxy.running_nodes[2]
            .change_request_rules(Some(vec![forge_error_rule.clone()]));

        session.query(q.clone(), (2,)).await.unwrap();

        info!("--------------------- third query - all nodes not responding  ----------------");
        running_proxy.running_nodes[1]
            .change_request_rules(Some(vec![forge_error_rule]));

        session.query(q.clone(), (1,)).await.unwrap_err();

        info!("--------------------- fourth query - 0 and 1 nodes not responding  ----------------");
        running_proxy.running_nodes[2]
        .change_request_rules(None);

        session.query(q, (1,)).await.unwrap();

        info!("--------------------- FINISHING main test part ----------------");

        running_proxy
    }).await;

    match res {
        Ok(()) => (),
        Err(ProxyError::Worker(WorkerError::DriverDisconnected(_))) => (),
        Err(err) => panic!("{}", err),
    }
}
