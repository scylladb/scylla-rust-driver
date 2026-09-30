use std::sync::Arc;

use scylla::client::caching_session::{CachingSession, CachingSessionBuilder};
use scylla::client::execution_profile::ExecutionProfile;
use scylla::client::session_builder::SessionBuilder;
use scylla::policies::load_balancing::{NodeIdentifier, SingleTargetLoadBalancingPolicy};
use scylla::statement::batch::{Batch, BatchType};
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
    PerformDDL as _, fetch_negotiated_features, setup_tracing, test_with_3_node_cluster,
    unique_keyspace_name,
};

#[tokio::test]
async fn test_caching_session_metadata_cache() {
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

/// Takes every feedback frame that has arrived so far, without waiting for more,
/// and returns how many there were.
fn consume_current_feedbacks(
    rx: &mut mpsc::UnboundedReceiver<(RequestFrame, Option<u16>)>,
) -> usize {
    std::iter::from_fn(|| rx.try_recv().ok()).count()
}

/// A `CachingSession` prepares a statement it has not seen before and reuses
/// the prepared statement afterwards, so a statement executed again must not be
/// prepared again.
///
/// The check is made on the wire: a proxy rule feeds back every PREPARE frame
/// reaching any node, and the test counts them. `Session::prepare` goes to one
/// connection per node, so a statement costs one PREPARE per node, and the
/// counts below are in terms of the cluster's size.
#[tokio::test]
async fn test_caching_session_statement_cache() {
    setup_tracing();
    let res = test_with_3_node_cluster(
        scylla_proxy::ShardAwareness::QueryNode,
        |proxy_uris, translation_map, mut running_proxy| async move {
            const CLUSTER_SIZE: usize = 3;

            let session = SessionBuilder::new()
                .known_node(proxy_uris[0].as_str())
                .address_translator(Arc::new(translation_map))
                .build()
                .await
                .unwrap();

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
            let mut prepares_so_far =
                || -> usize { feedback_rxs.iter_mut().map(consume_current_feedbacks).sum() };

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

            // All the nodes are assumed to have the same number of shards.
            let nr_shards = session
                .get_cluster_state()
                .get_nodes_info()
                .first()
                .expect("No nodes information available")
                .sharder()
                .map_or(1, |sharder| sharder.nr_shards.get() as usize);

            // Setting up the schema prepared statements of its own; they are not
            // what is being counted.
            prepares_so_far();

            let caching_session = CachingSession::from(session, 100);

            const BATCH_SIZE: usize = 4;
            let mut batch = Batch::new(BatchType::Logged);
            for i in 1..=BATCH_SIZE {
                batch.append_statement(
                    format!("INSERT INTO tab (a, b, c) VALUES ({i}, ?, ?)").as_str(),
                );
            }
            let batch_values: Vec<(i32, i32)> = (1..=BATCH_SIZE as i32).map(|i| (i, i)).collect();

            // None of the batch's statements has been seen before, so each is
            // prepared - once per node, as `Session::prepare` goes to one
            // connection per node rather than to every shard.
            caching_session
                .batch(&batch, batch_values.clone())
                .await
                .unwrap();
            assert_eq!(prepares_so_far(), BATCH_SIZE * CLUSTER_SIZE);

            // They are all in the cache now, so running the same batch again
            // must not prepare anything.
            for _ in 0..4 {
                caching_session
                    .batch(&batch, batch_values.clone())
                    .await
                    .unwrap();
                assert_eq!(prepares_so_far(), 0);
            }

            // A statement the cache has not seen is prepared, once per node.
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
            assert_eq!(prepares_so_far(), CLUSTER_SIZE);

            // The batches did write what they were given.
            rows.sort_unstable();
            let expected: Vec<(i32, i32, i32)> =
                (1..=BATCH_SIZE as i32).map(|i| (i, i, i)).collect();
            assert_eq!(rows, expected);

            // Renaming the columns back and forth leaves the table as it was but
            // invalidates the statements the server holds, so the next execution
            // of each has to be reprepared.
            for alter in [
                "ALTER TABLE tab RENAME c to tmp",
                "ALTER TABLE tab RENAME b to c",
                "ALTER TABLE tab RENAME tmp to b",
            ] {
                caching_session.ddl(alter).await.unwrap();
            }
            prepares_so_far();

            // Send the batch to each shard of each node in turn. A node updates
            // its cache upon the first mismatch, so on every node it is only the
            // shard contacted first that answers `Unprepared` and causes a
            // repreparation; by the time the other shards are asked, the node
            // already agrees with the driver.
            for node in caching_session
                .get_session()
                .get_cluster_state()
                .get_nodes_info()
                .iter()
            {
                for shard in 0..nr_shards {
                    let profile = ExecutionProfile::builder()
                        .load_balancing_policy(SingleTargetLoadBalancingPolicy::new(
                            NodeIdentifier::Node(Arc::clone(node)),
                            Some(shard as u32),
                        ))
                        .build();
                    batch.set_execution_profile_handle(Some(profile.into_handle()));

                    caching_session
                        .batch(&batch, batch_values.clone())
                        .await
                        .unwrap();

                    let expected = if shard == 0 { BATCH_SIZE } else { 0 };
                    assert_eq!(
                        prepares_so_far(),
                        expected,
                        "Unexpected number of prepares on node {node:?}, shard {shard}"
                    );
                }
            }

            caching_session
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
