use std::collections::BTreeSet;
use std::sync::Arc;

use scylla::client::caching_session::{CachingSession, CachingSessionBuilder};
use scylla::client::execution_profile::ExecutionProfile;
use scylla::client::session_builder::SessionBuilder;
use scylla::policies::load_balancing::{NodeIdentifier, SingleTargetLoadBalancingPolicy};
use scylla::statement::batch::{Batch, BatchStatement, BatchType};
use scylla::statement::prepared::PreparedStatement;
use scylla_cql::frame::request::RequestV2;
use scylla_cql::frame::request::execute::ExecuteV2;
use scylla_proxy::Condition;
use scylla_proxy::ProxyError;
use scylla_proxy::Reaction;
use scylla_proxy::RequestFrame;
use scylla_proxy::RequestOpcode;
use scylla_proxy::RequestReaction;
use scylla_proxy::RequestRule;
use scylla_proxy::WorkerError;
use tokio::sync::mpsc;

use crate::utils::{
    PerformDDL, calculate_proxy_host_ids, create_new_session_builder, fetch_negotiated_features,
    setup_tracing, test_with_3_node_cluster, unique_keyspace_name,
    wait_until_all_shards_are_connected,
};

#[tokio::test]
async fn test_caching_session_metadata_cache() {
    setup_tracing();
    let features = fetch_negotiated_features(None).await;
    let has_metadata_extension = features.scylla_metadata_id_supported;
    let res = test_with_3_node_cluster(
        scylla_proxy::ShardAwareness::QueryNode,
        |proxy_uris, translation_map, mut running_proxy| async move {
            let (feedback_tx, mut feedback_rx) = mpsc::unbounded_channel();
            let prepared_request_feedback_rule = RequestRule(
                Condition::and(
                    Condition::not(Condition::ConnectionRegisteredAnyEvent),
                    Condition::RequestOpcode(RequestOpcode::Execute),
                ),
                RequestReaction::noop().with_feedback_when_performed(feedback_tx),
            );
            for node in running_proxy.running_nodes.iter_mut() {
                node.change_request_rules(Some(vec![prepared_request_feedback_rule.clone()]));
            }

            let verify_statement_metadata = async |session: &CachingSession,
                                                   statement: &str,
                                                   should_have_metadata: bool,
                                                   feedback: &mut mpsc::UnboundedReceiver<(
                RequestFrame,
                Option<u16>,
            )>| {
                let should_have_metadata = should_have_metadata && !has_metadata_extension;
                let _result = session.execute_unpaged(statement, ()).await.unwrap();
                let (req_frame, _) = feedback.recv().await.unwrap();
                let _ = feedback.try_recv().unwrap_err(); // There should be only one frame.
                let request = req_frame.deserialize(&features).unwrap();
                let RequestV2::Execute(ExecuteV2 { parameters, .. }) = request else {
                    panic!("Unexpected request type");
                };
                let has_metadata = !parameters.skip_metadata;
                assert_eq!(has_metadata, should_have_metadata);
            };

            const REQUEST: &str = "SELECT * FROM system.local WHERE key = 'local'";

            let session = Arc::new(
                SessionBuilder::new()
                    .known_node(proxy_uris[0].as_str())
                    .address_translator(Arc::new(translation_map.clone()))
                    .build()
                    .await
                    .unwrap(),
            );
            let caching_session: CachingSession =
                CachingSessionBuilder::new_shared(Arc::clone(&session))
                    .use_cached_result_metadata(false) // Default, set just to be more explicit
                    .build();

            // Skipping metadata was not set, so metadata should be present
            verify_statement_metadata(&caching_session, REQUEST, true, &mut feedback_rx).await;

            // It should also be present when executing statement already in cache
            verify_statement_metadata(&caching_session, REQUEST, true, &mut feedback_rx).await;

            let caching_session: CachingSession =
                CachingSessionBuilder::new_shared(Arc::clone(&session))
                    .use_cached_result_metadata(true)
                    .build();

            // Now we set skip_metadata to true, so metadata should not be present for a new query
            verify_statement_metadata(&caching_session, REQUEST, false, &mut feedback_rx).await;

            // It should also not be present when executing statement already in cache
            verify_statement_metadata(&caching_session, REQUEST, false, &mut feedback_rx).await;

            // Test also without setting it explicitly, to verify that it is false by default.
            let caching_session: CachingSession =
                CachingSessionBuilder::new_shared(Arc::clone(&session)).build();

            // Skipping metadata was not set, so metadata should be present
            verify_statement_metadata(&caching_session, REQUEST, true, &mut feedback_rx).await;

            // It should also be present when executing statement already in cache
            verify_statement_metadata(&caching_session, REQUEST, true, &mut feedback_rx).await;

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

async fn assert_test_batch_table_rows_contain(sess: &CachingSession, expected_rows: &[(i32, i32)]) {
    let selected_rows: BTreeSet<(i32, i32)> = sess
        .execute_unpaged("SELECT a, b FROM test_batch_table", ())
        .await
        .unwrap()
        .into_rows_result()
        .unwrap()
        .rows::<(i32, i32)>()
        .unwrap()
        .map(|r| r.unwrap())
        .collect();
    for expected_row in expected_rows.iter() {
        if !selected_rows.contains(expected_row) {
            panic!("Expected {selected_rows:?} to contain row: {expected_row:?}, but they didn't");
        }
    }
}

#[tokio::test]
async fn test_batch() {
    setup_tracing();
    let session = create_new_session_builder().build().await.unwrap();
    let ks = unique_keyspace_name();
    session
        .ddl(format!(
            "CREATE KEYSPACE IF NOT EXISTS {ks} WITH REPLICATION = {{'class' : 'NetworkTopologyStrategy', 'replication_factor' : 1}}"
        ))
        .await
        .unwrap();
    session.use_keyspace(&ks, false).await.unwrap();
    session
        .ddl("CREATE TABLE IF NOT EXISTS test_batch_table (a int, b int, primary key (a, b))")
        .await
        .unwrap();
    let session: CachingSession = CachingSession::from(session, 2);

    let unprepared_insert_a_b: &str = "insert into test_batch_table (a, b) values (?, ?)";
    let unprepared_insert_a_7: &str = "insert into test_batch_table (a, b) values (?, 7)";
    let unprepared_insert_8_b: &str = "insert into test_batch_table (a, b) values (8, ?)";
    let prepared_insert_a_b: PreparedStatement = session
        .add_prepared_statement(&unprepared_insert_a_b.into())
        .await
        .unwrap();
    let prepared_insert_a_7: PreparedStatement = session
        .add_prepared_statement(&unprepared_insert_a_7.into())
        .await
        .unwrap();
    let prepared_insert_8_b: PreparedStatement = session
        .add_prepared_statement(&unprepared_insert_8_b.into())
        .await
        .unwrap();

    let assert_batch_prepared = |b: &Batch| {
        for stmt in &b.statements {
            match stmt {
                BatchStatement::PreparedStatement(_) => {}
                _ => panic!("Unprepared statement in prepared batch!"),
            }
        }
    };

    {
        let mut unprepared_batch: Batch = Default::default();
        unprepared_batch.append_statement(unprepared_insert_a_b);
        unprepared_batch.append_statement(unprepared_insert_a_7);
        unprepared_batch.append_statement(unprepared_insert_8_b);

        session
            .batch(&unprepared_batch, ((10, 20), (10,), (20,)))
            .await
            .unwrap();
        assert_test_batch_table_rows_contain(&session, &[(10, 20), (10, 7), (8, 20)]).await;

        let prepared_batch: Batch = session.prepare_batch(&unprepared_batch).await.unwrap();
        assert_batch_prepared(&prepared_batch);

        session
            .batch(&prepared_batch, ((15, 25), (15,), (25,)))
            .await
            .unwrap();
        assert_test_batch_table_rows_contain(&session, &[(15, 25), (15, 7), (8, 25)]).await;
    }

    {
        let mut partially_prepared_batch: Batch = Default::default();
        partially_prepared_batch.append_statement(unprepared_insert_a_b);
        partially_prepared_batch.append_statement(prepared_insert_a_7.clone());
        partially_prepared_batch.append_statement(unprepared_insert_8_b);

        session
            .batch(&partially_prepared_batch, ((30, 40), (30,), (40,)))
            .await
            .unwrap();
        assert_test_batch_table_rows_contain(&session, &[(30, 40), (30, 7), (8, 40)]).await;

        let prepared_batch: Batch = session
            .prepare_batch(&partially_prepared_batch)
            .await
            .unwrap();
        assert_batch_prepared(&prepared_batch);

        session
            .batch(&prepared_batch, ((35, 45), (35,), (45,)))
            .await
            .unwrap();
        assert_test_batch_table_rows_contain(&session, &[(35, 45), (35, 7), (8, 45)]).await;
    }

    {
        let mut fully_prepared_batch: Batch = Default::default();
        fully_prepared_batch.append_statement(prepared_insert_a_b);
        fully_prepared_batch.append_statement(prepared_insert_a_7);
        fully_prepared_batch.append_statement(prepared_insert_8_b);

        session
            .batch(&fully_prepared_batch, ((50, 60), (50,), (60,)))
            .await
            .unwrap();
        assert_test_batch_table_rows_contain(&session, &[(50, 60), (50, 7), (8, 60)]).await;

        let prepared_batch: Batch = session.prepare_batch(&fully_prepared_batch).await.unwrap();
        assert_batch_prepared(&prepared_batch);

        session
            .batch(&prepared_batch, ((55, 65), (55,), (65,)))
            .await
            .unwrap();

        assert_test_batch_table_rows_contain(&session, &[(55, 65), (55, 7), (8, 65)]).await;
    }

    {
        let mut bad_batch: Batch = Default::default();
        bad_batch.append_statement(unprepared_insert_a_b);
        bad_batch.append_statement("This isnt even CQL");
        bad_batch.append_statement(unprepared_insert_8_b);

        assert!(session.batch(&bad_batch, ((1, 2), (), (2,))).await.is_err());
        assert!(session.prepare_batch(&bad_batch).await.is_err());
    }

    session.ddl(format!("DROP KEYSPACE {ks}")).await.unwrap();
}

/// Takes all the feedback frames that are in `rx` now, and returns their number.
/// It does not wait for more frames.
fn consume_current_feedbacks(
    rx: &mut mpsc::UnboundedReceiver<(RequestFrame, Option<u16>)>,
) -> usize {
    std::iter::from_fn(|| rx.try_recv().ok()).count()
}

/// A `CachingSession` prepares a statement that it did not see before. After
/// that, it uses the cached prepared statement. Thus, it must not prepare the
/// same statement again.
///
/// The test checks this on the wire. On each node, a proxy rule sends feedback
/// for every PREPARE frame, and the test counts these frames for each node.
/// `Session::prepare` sends a statement to one connection per node. Thus, one
/// statement costs one PREPARE on each node.
#[tokio::test]
async fn test_caching_session_statement_cache() {
    setup_tracing();
    let res = test_with_3_node_cluster(
        scylla_proxy::ShardAwareness::QueryNode,
        |proxy_uris, translation_map, mut running_proxy| async move {
            const CLUSTER_SIZE: usize = 3;

            let session = SessionBuilder::new()
                .known_node(proxy_uris[0].as_str())
                .address_translator(Arc::new(translation_map.clone()))
                .build()
                .await
                .unwrap();
            // The feedback channels below are in the order of `proxy_uris`. So
            // are these host IDs.
            let host_ids = calculate_proxy_host_ids(&proxy_uris, &translation_map, &session);

            // The test sends the batch to each shard. Thus, it needs a connection
            // to every shard.
            wait_until_all_shards_are_connected(&session).await;

            let (feedback_txs, mut feedback_rxs): (Vec<_>, Vec<_>) =
                (0..CLUSTER_SIZE).map(|_| mpsc::unbounded_channel()).unzip();
            for (node, tx) in running_proxy.running_nodes.iter_mut().zip(feedback_txs) {
                node.change_request_rules(Some(vec![RequestRule(
                    Condition::and(
                        Condition::RequestOpcode(RequestOpcode::Prepare),
                        Condition::not(Condition::ConnectionRegisteredAnyEvent),
                    ),
                    RequestReaction::noop().with_feedback_when_performed(tx),
                )]));
            }
            let mut prepares_per_node = || -> Vec<usize> {
                feedback_rxs
                    .iter_mut()
                    .map(consume_current_feedbacks)
                    .collect()
            };

            let ks = unique_keyspace_name();
            session
                .ddl(format!(
                    "CREATE KEYSPACE {ks} WITH REPLICATION = \
                     {{'class' : 'NetworkTopologyStrategy', 'replication_factor' : 1}}"
                ))
                .await
                .unwrap();
            session.use_keyspace(&ks, false).await.unwrap();
            session
                .ddl("CREATE TABLE tab (a int, b int, c int, primary key (a, b, c))")
                .await
                .unwrap();

            let caching_session: CachingSession = CachingSession::from(session, 100);

            const BATCH_SIZE: usize = 4;
            let mut batch = Batch::new(BatchType::Logged);
            for i in 1..=BATCH_SIZE {
                batch.append_statement(
                    format!("INSERT INTO tab (a, b, c) VALUES ({i}, ?, ?)").as_str(),
                );
            }
            let batch_values: Vec<(i32, i32)> = (1..=BATCH_SIZE as i32).map(|i| (i, i)).collect();

            // The cache does not contain any statement of the batch yet. Thus,
            // the `CachingSession` prepares each statement once per node.
            // `Session::prepare` sends it to one connection per node, not to
            // every shard.
            caching_session
                .batch(&batch, batch_values.clone())
                .await
                .unwrap();
            assert_eq!(prepares_per_node(), [BATCH_SIZE; CLUSTER_SIZE]);

            // Now the cache contains all the statements of the batch. Thus, the
            // next runs of the same batch must not prepare anything.
            for _ in 0..4 {
                caching_session
                    .batch(&batch, batch_values.clone())
                    .await
                    .unwrap();
                assert_eq!(prepares_per_node(), [0; CLUSTER_SIZE]);
            }

            // The cache does not contain this statement. Thus, the
            // `CachingSession` prepares it once per node.
            let mut rows: Vec<(i32, i32, i32)> = caching_session
                .execute_unpaged("SELECT a, b, c FROM tab", &[])
                .await
                .unwrap()
                .into_rows_result()
                .unwrap()
                .rows()
                .unwrap()
                .collect::<Result<_, _>>()
                .unwrap();
            assert_eq!(prepares_per_node(), [1; CLUSTER_SIZE]);

            // The batches wrote the given values.
            rows.sort_unstable();
            let expected: Vec<(i32, i32, i32)> =
                (1..=BATCH_SIZE as i32).map(|i| (i, i, i)).collect();
            assert_eq!(rows, expected);

            // Rename the columns back and forth. After that, the table is the
            // same as before. But the server invalidates the prepared statements
            // of the table. Thus, the driver must prepare each statement again at
            // its next execution.
            for alter in [
                "ALTER TABLE tab RENAME c to tmp",
                "ALTER TABLE tab RENAME b to c",
                "ALTER TABLE tab RENAME tmp to b",
            ] {
                caching_session.get_session().ddl(alter).await.unwrap();
            }

            // Send the batch to each shard of each node in turn. A node updates
            // its cache at the first mismatch. Thus, on each node, only the shard
            // that the test contacts first answers `Unprepared` and causes a
            // repreparation. When the test contacts the other shards, the node
            // already agrees with the driver.
            for node in caching_session
                .get_session()
                .get_cluster_state()
                .get_nodes_info()
                .iter()
            {
                let node_index = host_ids
                    .iter()
                    .position(|&host_id| host_id == node.host_id)
                    .unwrap();
                let sharder = node.sharder();
                let nr_shards = sharder
                    .as_ref()
                    .map_or(1, |sharder| sharder.nr_shards.get());
                for shard in 0..nr_shards {
                    let profile = ExecutionProfile::builder()
                        .load_balancing_policy(SingleTargetLoadBalancingPolicy::new(
                            NodeIdentifier::Node(Arc::clone(node)),
                            Some(shard.into()),
                        ))
                        .build();
                    batch.set_execution_profile_handle(Some(profile.into_handle()));

                    let result = caching_session
                        .batch(&batch, batch_values.clone())
                        .await
                        .unwrap();

                    // If the batch goes to a different shard, the count below
                    // does not check the target shard.
                    let coordinator = result.request_coordinator();
                    assert_eq!(coordinator.node().host_id, node.host_id);
                    // A node without a sharder, for example Cassandra, reports
                    // no shard in the coordinator.
                    if sharder.is_some() {
                        assert_eq!(coordinator.shard(), Some(shard.into()));
                    }

                    let mut expected = [0; CLUSTER_SIZE];
                    if shard == 0 {
                        expected[node_index] = BATCH_SIZE;
                    }
                    assert_eq!(
                        prepares_per_node(),
                        expected,
                        "Unexpected number of prepares on node {node:?}, shard {shard}"
                    );
                }
            }

            caching_session
                .get_session()
                .ddl(format!("DROP KEYSPACE {ks}"))
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
