use crate::utils::{
    PerformDDL, fetch_negotiated_features, setup_tracing, test_with_3_node_cluster,
    unique_keyspace_name,
};
use assert_matches::assert_matches;
use scylla::client::execution_profile::ExecutionProfile;
use scylla::client::session::Session;
use scylla::client::session_builder::SessionBuilder;
use scylla::errors::{DbError, ExecutionError, RequestAttemptError};
use scylla::policies::retry::{DowngradingConsistencyRetryPolicy, FallthroughRetryPolicy};
use scylla::policies::speculative_execution::SimpleSpeculativeExecutionPolicy;
use scylla::statement::unprepared::Statement;
use scylla_cql::Consistency;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tracing::info;

use scylla_proxy::{
    Condition, ProxyError, Reaction, RequestOpcode, RequestReaction, RequestRule, ShardAwareness,
    WorkerError,
};

#[tokio::test]
async fn speculative_execution_is_fired() {
    setup_tracing();
    const TIMEOUT_PER_REQUEST: Duration = Duration::from_millis(1000);

    let res = test_with_3_node_cluster(ShardAwareness::QueryNode, |proxy_uris, translation_map, mut running_proxy| async move {
        // DB preparation phase
        let simple_speculative_no_retry_profile = ExecutionProfile::builder().speculative_execution_policy(Some(Arc::new(SimpleSpeculativeExecutionPolicy {
            max_retry_count: 2,
            retry_interval: Duration::from_millis(10),
        }))).retry_policy(Arc::new(FallthroughRetryPolicy)).build();
        let session: Session = SessionBuilder::new()
            .known_node(proxy_uris[0].as_str())
            .default_execution_profile_handle(simple_speculative_no_retry_profile.into_handle())
            .address_translator(Arc::new(translation_map))
            .build()
            .await
            .unwrap();

        let ks = unique_keyspace_name();
        session.ddl(format!("CREATE KEYSPACE IF NOT EXISTS {ks} WITH REPLICATION = {{'class' : 'NetworkTopologyStrategy', 'replication_factor' : 3}}")).await.unwrap();
        session.use_keyspace(&ks, false).await.unwrap();
        session
            .ddl("CREATE TABLE t (a int primary key)")
            .await
            .unwrap();

        let mut s = Statement::from("INSERT INTO t (a) VALUES (?)");
        s.set_is_idempotent(true); // this is to allow speculative execution to fire

        let drop_frame_rule = RequestRule(
            Condition::RequestOpcode(RequestOpcode::Prepare)
                .and(Condition::BodyContainsCaseSensitive(Box::new(*b"t"))),
            RequestReaction::drop_frame(),
        );

        info!("--------------------- BEGINNING main test part ----------------");

        info!("--------------------- first query - no rules  ----------------");
        // first run before any rules
        session.query_unpaged(s.clone(), (3,)).await.unwrap();

        info!("--------------------- second query - 0 and 2 nodes not responding  ----------------");
        running_proxy.running_nodes[0]
            .change_request_rules(Some(vec![drop_frame_rule.clone()]));
        running_proxy.running_nodes[2]
            .change_request_rules(Some(vec![drop_frame_rule.clone()]));

        session.query_unpaged(s.clone(), (2,)).await.unwrap();

        info!("--------------------- third query - 0 and 1 nodes not responding  ----------------");
        running_proxy.running_nodes[2]
            .change_request_rules(None);
        running_proxy.running_nodes[1]
            .change_request_rules(Some(vec![drop_frame_rule.clone()]));

        session.query_unpaged(s.clone(), (1,)).await.unwrap();


        info!("--------------------- fourth query - all nodes not responding  ----------------");
        running_proxy.running_nodes[2]
        .change_request_rules(Some(vec![drop_frame_rule]));

        tokio::select! {
            res = session.query_unpaged(s, (0,)) => panic!("Rules did not work: received response {:?}", res),
            _ = tokio::time::sleep(TIMEOUT_PER_REQUEST) => (),
        };

        info!("--------------------- FINISHING main test part ----------------");

        running_proxy.running_nodes.iter_mut().for_each(|n| n.change_request_rules(None));
        session.ddl(format!("DROP KEYSPACE {ks}")).await.unwrap();

        running_proxy
    }).await;

    match res {
        Ok(()) => (),
        Err(ProxyError::Worker(WorkerError::DriverDisconnected(_))) => (),
        Err(err) => panic!("{}", err),
    }
}

#[tokio::test]
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
        session.ddl(format!("CREATE KEYSPACE IF NOT EXISTS {ks} WITH REPLICATION = {{'class' : 'NetworkTopologyStrategy', 'replication_factor' : 3}}")).await.unwrap();
        session.use_keyspace(&ks, false).await.unwrap();
        session
            .ddl("CREATE TABLE t (a int primary key)")
            .await
            .unwrap();

        let mut s = Statement::from("INSERT INTO t (a) VALUES (?)");
        s.set_is_idempotent(true); // this is to allow retry to fire

        let forge_error_rule = RequestRule(
            Condition::RequestOpcode(RequestOpcode::Prepare)
                .and(Condition::BodyContainsCaseSensitive(Box::new(*b"INTO t"))),
            RequestReaction::forge().server_error(),
        );

        info!("--------------------- BEGINNING main test part ----------------");

        info!("--------------------- first query - no rules  ----------------");
        session.query_unpaged(s.clone(), (3,)).await.unwrap();

        info!("--------------------- second query - 0 and 2 nodes not responding  ----------------");
        running_proxy.running_nodes[0]
            .change_request_rules(Some(vec![forge_error_rule.clone()]));
        running_proxy.running_nodes[2]
            .change_request_rules(Some(vec![forge_error_rule.clone()]));

        session.query_unpaged(s.clone(), (2,)).await.unwrap();

        info!("--------------------- third query - all nodes not responding  ----------------");
        running_proxy.running_nodes[1]
            .change_request_rules(Some(vec![forge_error_rule]));

        session.query_unpaged(s.clone(), (1,)).await.unwrap_err();

        info!("--------------------- fourth query - 0 and 1 nodes not responding  ----------------");
        running_proxy.running_nodes[2]
        .change_request_rules(None);

        session.query_unpaged(s, (1,)).await.unwrap();

        info!("--------------------- FINISHING main test part ----------------");

        session.ddl(format!("DROP KEYSPACE {ks}")).await.unwrap();

        running_proxy
    }).await;

    match res {
        Ok(()) => (),
        Err(ProxyError::Worker(WorkerError::DriverDisconnected(_))) => (),
        Err(err) => panic!("{}", err),
    }
}

/// Tests that DowngradingConsistencyRetryPolicy retries with a lower consistency level
/// when nodes return an Unavailable error.
///
/// Uses the proxy to forge Unavailable errors only for requests with CL=ALL,
/// so the retried request (with downgraded CL) passes through to the real node.
/// Feedback channels verify exactly which requests were sent and with what CLs.
#[tokio::test]
async fn downgrading_consistency_policy_downgrades_on_unavailable() {
    setup_tracing();
    let features = fetch_negotiated_features(None).await;

    let res = test_with_3_node_cluster(
        ShardAwareness::QueryNode,
        |proxy_uris, translation_map, mut running_proxy| async move {
            let profile = ExecutionProfile::builder()
                .retry_policy(Arc::new(DowngradingConsistencyRetryPolicy::new()))
                .consistency(Consistency::All)
                .build();
            let session: Session = SessionBuilder::new()
                .known_node(proxy_uris[0].as_str())
                .address_translator(Arc::new(translation_map))
                .default_execution_profile_handle(profile.into_handle())
                .build()
                .await
                .unwrap();

            let ks = unique_keyspace_name();
            session.ddl(format!("CREATE KEYSPACE IF NOT EXISTS {ks} WITH REPLICATION = {{'class' : 'NetworkTopologyStrategy', 'replication_factor' : 3}}")).await.unwrap();
            session.use_keyspace(&ks, false).await.unwrap();
            session
                .ddl("CREATE TABLE t (a int primary key)")
                .await
                .unwrap();

            let execute_not_control = Condition::RequestOpcode(RequestOpcode::Execute)
                .and(Condition::not(Condition::ConnectionRegisteredAnyEvent));

            // Cases 1 & 2: one node returns Unavailable(alive=2) for CL=ALL.
            // The policy should downgrade CL from ALL to TWO and retry.
            // Unavailable errors are always retried regardless of idempotency,
            // so both cases expect the same behavior.
            let test_unavailable_downgrade =
                |running_proxy: &mut scylla_proxy::RunningProxy,
                 is_idempotent: bool,
                 value: i32| {
                    let (error_tx, mut error_rx) = mpsc::unbounded_channel();
                    let (pass_tx, mut pass_rx) = mpsc::unbounded_channel();

                    let unavailable_error = DbError::Unavailable {
                        consistency: Consistency::All,
                        required: 3,
                        alive: 2,
                    };

                    // Rule 1: Execute with CL=ALL → forge Unavailable, capture in error_tx
                    // Rule 2: Execute (any other CL) → pass through, capture in pass_tx
                    for node in running_proxy.running_nodes.iter_mut() {
                        node.change_request_rules(Some(vec![
                            RequestRule(
                                execute_not_control
                                    .clone()
                                    .and(Condition::RequestConsistency(
                                        Consistency::All,
                                        features,
                                    )),
                                RequestReaction::forge_with_error(unavailable_error.clone())
                                    .with_feedback_when_performed(error_tx.clone()),
                            ),
                            RequestRule(
                                execute_not_control.clone(),
                                RequestReaction::noop()
                                    .with_feedback_when_performed(pass_tx.clone()),
                            ),
                        ]));
                    }

                    let session = &session;
                    async move {
                        let mut stmt = Statement::from("INSERT INTO t (a) VALUES (?)");
                        stmt.set_is_idempotent(is_idempotent);
                        session.query_unpaged(stmt, (value,)).await.unwrap();

                        // Verify: exactly 1 request rejected (CL=ALL), exactly 1 retry passed (CL=TWO).
                        let (rejected_frame, _) = error_rx.recv().await.unwrap();
                        assert!(
                            error_rx.try_recv().is_err(),
                            "expected exactly 1 rejected request"
                        );
                        assert_eq!(
                            rejected_frame
                                .deserialize(&features)
                                .unwrap()
                                .get_consistency()
                                .unwrap(),
                            Consistency::All
                        );

                        let (passed_frame, _) = pass_rx.recv().await.unwrap();
                        assert!(
                            pass_rx.try_recv().is_err(),
                            "expected exactly 1 passed request"
                        );
                        assert_eq!(
                            passed_frame
                                .deserialize(&features)
                                .unwrap()
                                .get_consistency()
                                .unwrap(),
                            Consistency::Two
                        );
                    }
                };

            info!("--- Case 1: idempotent query, Unavailable(alive=2) ---");
            test_unavailable_downgrade(&mut running_proxy, true, 1).await;

            info!("--- Case 2: non-idempotent query, Unavailable(alive=2) ---");
            test_unavailable_downgrade(&mut running_proxy, false, 2).await;

            // --- Case 3: Unavailable with alive=0, policy cannot downgrade ---
            // With alive=0 and CL != EachQuorum, the policy returns DontRetry.
            // Only 1 request should be sent, and the query should fail.
            info!("--- Case 3: Unavailable(alive=0), no retry possible ---");
            {
                let (feedback_tx, mut feedback_rx) = mpsc::unbounded_channel();

                let no_alive_error = DbError::Unavailable {
                    consistency: Consistency::All,
                    required: 3,
                    alive: 0,
                };

                for node in running_proxy.running_nodes.iter_mut() {
                    node.change_request_rules(Some(vec![RequestRule(
                        execute_not_control.clone(),
                        RequestReaction::forge_with_error(no_alive_error.clone())
                            .with_feedback_when_performed(feedback_tx.clone()),
                    )]));
                }

                let mut stmt = Statement::from("INSERT INTO t (a) VALUES (?)");
                stmt.set_is_idempotent(true);
                let err = session.query_unpaged(stmt, (3,)).await.unwrap_err();

                // Verify the error propagated to the user is Unavailable.
                assert_matches!(err, ExecutionError::LastAttemptError(RequestAttemptError::DbError(DbError::Unavailable {
                    consistency: Consistency::All,
                    required: 3,
                    alive: 0,
                }, _)));

                // Verify exactly 1 request was sent (no retry).
                let (frame, _) = feedback_rx.recv().await.unwrap();
                assert!(feedback_rx.try_recv().is_err(), "expected exactly 1 request (no retry)");
                assert_eq!(
                    frame.deserialize(&features).unwrap().get_consistency().unwrap(),
                    Consistency::All
                );
            }

            info!("--- cleanup ---");
            running_proxy
                .running_nodes
                .iter_mut()
                .for_each(|n| n.change_request_rules(None));
            session.ddl(format!("DROP KEYSPACE {ks}")).await.unwrap();

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

// See https://github.com/scylladb/scylla-rust-driver/issues/1085
#[tokio::test]
async fn speculative_execution_panic_regression_test() {
    use scylla_proxy::RunningProxy;

    setup_tracing();
    let test = |proxy_uris: [String; 3],
                translation_map: HashMap<SocketAddr, SocketAddr>,
                mut running_proxy: RunningProxy| async move {
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
            .address_translator(Arc::new(translation_map.clone()))
            .default_execution_profile_handle(profile.into_handle())
            .build()
            .await
            .unwrap();

        let ks = unique_keyspace_name();
        session.ddl(format!("CREATE KEYSPACE IF NOT EXISTS {ks} WITH REPLICATION = {{'class' : 'NetworkTopologyStrategy', 'replication_factor' : 3}}")).await.unwrap();
        session.use_keyspace(&ks, false).await.unwrap();
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

        // Need to recreate session - old one has connections dropped and fails the request.
        running_proxy.turn_off_rules();
        let session: Session = SessionBuilder::new()
            .known_node(proxy_uris[0].as_str())
            .address_translator(Arc::new(translation_map))
            .build()
            .await
            .unwrap();
        session.ddl(format!("DROP KEYSPACE {ks}")).await.unwrap();

        running_proxy
    };
    let res = test_with_3_node_cluster(ShardAwareness::QueryNode, test).await;

    match res {
        Ok(()) => (),
        Err(ProxyError::Worker(WorkerError::DriverDisconnected(_))) => (),
        Err(err) => panic!("{}", err),
    }
}
