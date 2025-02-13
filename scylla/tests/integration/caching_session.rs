use std::sync::Arc;

use crate::utils::test_with_3_node_cluster;
use crate::utils::{setup_tracing, unique_keyspace_name, PerformDDL};
use scylla::batch::Batch;
use scylla::batch::BatchType;
use scylla::client::caching_session::CachingSession;
use scylla_proxy::RequestOpcode;
use scylla_proxy::RequestReaction;
use scylla_proxy::RequestRule;
use scylla_proxy::ShardAwareness;
use scylla_proxy::{Condition, ProxyError, Reaction, RequestFrame, TargetShard, WorkerError};
use tokio::sync::mpsc;

fn consume_current_feedbacks(
    rx: &mut mpsc::UnboundedReceiver<(RequestFrame, Option<TargetShard>)>,
) -> usize {
    std::iter::from_fn(|| rx.try_recv().ok()).count()
}

#[tokio::test]
#[cfg(not(scylla_cloud_tests))]
async fn ensure_cache_is_used() {
    use scylla::client::execution_profile::ExecutionProfile;

    use crate::utils::SingleTargetLBP;

    setup_tracing();
    let res = test_with_3_node_cluster(
        ShardAwareness::QueryNode,
        |proxy_uris, translation_map, mut running_proxy| async move {
            let session = scylla::client::session_builder::SessionBuilder::new()
                .known_node(proxy_uris[0].as_str())
                .address_translator(Arc::new(translation_map))
                .build()
                .await
                .unwrap();

            let cluster_size: usize = 3;
            let (feedback_txs, mut feedback_rxs): (Vec<_>, Vec<_>) = (0..cluster_size)
                .map(|_| mpsc::unbounded_channel::<(RequestFrame, Option<TargetShard>)>())
                .unzip();
            for (i, tx) in feedback_txs.iter().cloned().enumerate() {
                running_proxy.running_nodes[i].change_request_rules(Some(vec![RequestRule(
                    Condition::and(
                        Condition::RequestOpcode(RequestOpcode::Prepare),
                        Condition::not(Condition::ConnectionRegisteredAnyEvent),
                    ),
                    RequestReaction::noop().with_feedback_when_performed(tx),
                )]));
            }

            let ks = unique_keyspace_name();
            let rs = "{'class' : 'NetworkTopologyStrategy', 'replication_factor' : 1}";
            session
                .ddl(format!(
                    "CREATE KEYSPACE IF NOT EXISTS {} WITH REPLICATION = {}",
                    ks, rs
                ))
                .await
                .unwrap();
            session.use_keyspace(ks, false).await.unwrap();
            session
                .ddl("CREATE TABLE IF NOT EXISTS tab (a int, b int, c int, primary key (a, b, c))")
                .await
                .unwrap();
            // Assumption: all nodes have the same number of shards
            let nr_shards = session
                .get_cluster_state()
                .get_nodes_info()
                .first()
                .expect("No nodes information available")
                .sharder()
                .map(|sharder| sharder.nr_shards.get() as usize)
                .unwrap_or_else(|| 1); // If there is no sharder, assume 1 shard.

            // Consume all feedbacks so far to ensure we will not count something unrelated.
            let _feedbacks = feedback_rxs
                .iter_mut()
                .map(consume_current_feedbacks)
                .sum::<usize>();

            let caching_session: CachingSession = CachingSession::from(session, 100);

            let batch_size: usize = 4;
            let mut batch = Batch::new(BatchType::Logged);
            for i in 1..=batch_size {
                let insert_b_c = format!("INSERT INTO tab (a, b, c) VALUES ({}, ?, ?)", i);
                batch.append_statement(insert_b_c.as_str());
            }
            let batch_values: Vec<(i32, i32)> = (1..=batch_size as i32).map(|i| (i, i)).collect();

            // First batch that should generate prepares for each shard.
            caching_session
                .batch(&batch, batch_values.clone())
                .await
                .unwrap();
            let feedbacks: usize = feedback_rxs.iter_mut().map(consume_current_feedbacks).sum();
            assert_eq!(feedbacks, batch_size * nr_shards * cluster_size);

            // Few extra runs. Those batches should not result in any prepares being sent.
            for _ in 0..4 {
                caching_session
                    .batch(&batch, batch_values.clone())
                    .await
                    .unwrap();
                let feedbacks: usize = feedback_rxs.iter_mut().map(consume_current_feedbacks).sum();
                assert_eq!(feedbacks, 0);
            }

            let prepared_batch_res_rows: Vec<(i32, i32, i32)> = caching_session
                .execute_unpaged("SELECT * FROM tab", &[])
                .await
                .unwrap()
                .into_rows_result()
                .unwrap()
                .rows()
                .unwrap()
                .collect::<Result<_, _>>()
                .unwrap();

            // Select should have been prepared on all shards
            let feedbacks: usize = feedback_rxs.iter_mut().map(consume_current_feedbacks).sum();
            assert_eq!(feedbacks, nr_shards * cluster_size);

            // Verify the data from inserts
            let mut prepared_batch_res_rows = prepared_batch_res_rows;
            prepared_batch_res_rows.sort();
            let expected_rows: Vec<(i32, i32, i32)> =
                (1..=batch_size as i32).map(|i| (i, i, i)).collect();
            assert_eq!(prepared_batch_res_rows, expected_rows);

            // Run some alters to invalidate the server side cache, similarly to scylla/src/session_test.rs
            caching_session
                .ddl("ALTER TABLE tab RENAME c to tmp")
                .await
                .unwrap();
            caching_session
                .ddl("ALTER TABLE tab RENAME b to c")
                .await
                .unwrap();
            caching_session
                .ddl("ALTER TABLE tab RENAME tmp to b")
                .await
                .unwrap();

            // execute_unpageds caused by alters likely resulted in some prepares being sent.
            // Consume those frames.
            feedback_rxs
                .iter_mut()
                .map(consume_current_feedbacks)
                .sum::<usize>();

            // Run batch for each shard. The server cache should be updated on the first mismatch,
            // therefore only first contacted shard will request reprepare due to mismatch.
            for node_info in caching_session
                .get_session()
                .get_cluster_state()
                .get_nodes_info()
                .iter()
            {
                for shard_id in 0..nr_shards {
                    let policy = SingleTargetLBP {
                        target: (node_info.clone(), Some(shard_id as u32)),
                    };
                    let execution_profile = ExecutionProfile::builder()
                        .load_balancing_policy(Arc::new(policy))
                        .build();
                    batch.set_execution_profile_handle(Some(execution_profile.into_handle()));
                    caching_session
                        .batch(&batch, batch_values.clone())
                        .await
                        .unwrap();
                    let feedbacks: usize =
                        feedback_rxs.iter_mut().map(consume_current_feedbacks).sum();
                    let expected_feedbacks = if shard_id == 0 { batch_size } else { 0 };
                    assert_eq!(
                        feedbacks, expected_feedbacks,
                        "Mismatch in feedbacks on execution for node: {:?}, shard: {}",
                        node_info, shard_id
                    );
                }
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
