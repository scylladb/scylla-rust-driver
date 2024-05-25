use crate::utils::test_with_3_node_cluster;
use futures::prelude::*;
use futures_batch::ChunksTimeoutStreamExt;
use scylla::retry_policy::FallthroughRetryPolicy;
use scylla::routing::Shard;
use scylla::serialize::row::SerializedValues;
use scylla::test_utils::unique_keyspace_name;
use scylla::transport::session::Session;
use scylla::{ExecutionProfile, SessionBuilder};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;

use scylla_proxy::{
    Condition, ProxyError, Reaction, RequestOpcode, RequestReaction, RequestRule, RunningProxy,
    ShardAwareness, WorkerError,
};

#[tokio::test]
#[ntest::timeout(20000)]
#[cfg(not(scylla_cloud_tests))]
async fn shard_aware_batching_pattern_routes_to_proper_shard() {
    let res = test_with_3_node_cluster(ShardAwareness::QueryNode, run_test).await;

    match res {
        Ok(()) => (),
        Err(ProxyError::Worker(WorkerError::DriverDisconnected(_))) => (),
        Err(err) => panic!("{}", err),
    }
}

async fn run_test(
    proxy_uris: [String; 3],
    translation_map: HashMap<std::net::SocketAddr, std::net::SocketAddr>,
    mut running_proxy: RunningProxy,
) -> RunningProxy {
    // This is just to increase the likelihood that only intended prepared statements (which contain this mark) are captured by the proxy.
    const MAGIC_MARK: i32 = 123;

    // We set up proxy, so that it passes us information about which node was queried (via prepared_rx).

    let prepared_rule = |tx| {
        RequestRule(
            Condition::and(
                Condition::RequestOpcode(RequestOpcode::Batch),
                Condition::BodyContainsCaseSensitive(Box::new(MAGIC_MARK.to_be_bytes())),
            ),
            RequestReaction::noop().with_feedback_when_performed(tx),
        )
    };

    let mut prepared_rxs = [0, 1, 2].map(|i| {
        let (prepared_tx, prepared_rx) = mpsc::unbounded_channel();
        running_proxy.running_nodes[i].change_request_rules(Some(vec![prepared_rule(prepared_tx)]));
        prepared_rx
    });
    let shards_for_nodes_test_check: Arc<tokio::sync::Mutex<HashMap<uuid::Uuid, Vec<Shard>>>> =
        Default::default();

    let handle = ExecutionProfile::builder()
        .retry_policy(Box::new(FallthroughRetryPolicy))
        .build()
        .into_handle();

    // DB preparation phase
    let session: Arc<Session> = Arc::new(
        SessionBuilder::new()
            .known_node(proxy_uris[0].as_str())
            .default_execution_profile_handle(handle)
            .address_translator(Arc::new(translation_map))
            .build()
            .await
            .unwrap(),
    );

    // Create schema
    let ks = unique_keyspace_name();
    session.query(format!("CREATE KEYSPACE IF NOT EXISTS {} WITH REPLICATION = {{'class' : 'NetworkTopologyStrategy', 'replication_factor' : 3}}", ks), &[]).await.unwrap();
    session.use_keyspace(ks, false).await.unwrap();

    session
        .query("CREATE TABLE t (a int primary key, b int)", &[])
        .await
        .unwrap();

    // We will check which nodes where queries, for both LWT and non-LWT prepared statements.
    let prepared_statement = session
        .prepare("INSERT INTO t (a, b) VALUES (?, ?)")
        .await
        .unwrap();

    assert!(prepared_statement.is_token_aware());

    // Build the shard-aware batching system

    #[derive(Clone, Copy, PartialEq, Eq, Hash)]
    struct DestinationShard {
        node_id: uuid::Uuid,
        shard_id_on_node: u32,
    }
    let mut channels_for_shards: HashMap<
        DestinationShard,
        tokio::sync::mpsc::Sender<SerializedValues>,
    > = HashMap::new();
    let mut batching_tasks: Vec<tokio::task::JoinHandle<()>> = Vec::new(); // To make sure nothing panicked
    for i in 0..150 {
        let values = (i, MAGIC_MARK);

        let serialized_values = prepared_statement
            .serialize_values(&values)
            .expect("Failed to serialize values");

        let (node, shard_id_on_node) = session
            .shard_for_statement(&prepared_statement, &serialized_values)
            .expect("Error when getting shard for statement")
            .expect("Query is not shard-aware");
        let destination_shard = DestinationShard {
            node_id: node.host_id,
            shard_id_on_node,
        };

        // Typically if lines may come from different places, the `shards` `HashMap` would be behind
        // a mutex, but for this example we keep it simple.
        // Create the task that constitutes and sends the batches for this shard if it doesn't already exist

        let sender = channels_for_shards
            .entry(destination_shard)
            .or_insert_with(|| {
                let (sender, receiver) = tokio::sync::mpsc::channel(10000);
                let prepared_statement = prepared_statement.clone();
                let session = session.clone();

                let mut scylla_batch =
                    scylla::batch::Batch::new(scylla::batch::BatchType::Unlogged);
                scylla_batch.enforce_target_node(&node, shard_id_on_node, &session);

                let shards_for_nodes_test_check_clone = Arc::clone(&shards_for_nodes_test_check);
                batching_tasks.push(tokio::spawn(async move {
                    let mut batches = ReceiverStream::new(receiver)
                        .chunks_timeout(10, Duration::from_millis(100));

                    while let Some(batch) = batches.next().await {
                        // Obviously if the actual prepared statement depends on each element of the batch
                        // this requires adjustment
                        scylla_batch.statements.resize_with(batch.len(), || {
                            scylla::batch::BatchStatement::PreparedStatement(
                                prepared_statement.clone(),
                            )
                        });

                        // Take a global lock to make test deterministic
                        // (and because we need to push stuff in there to test that shard-awareness is respected)
                        let mut shards_for_nodes_test_check =
                            shards_for_nodes_test_check_clone.lock().await;

                        session
                            .batch(&scylla_batch, &batch)
                            .await
                            .expect("Query to send batch failed");

                        shards_for_nodes_test_check
                            .entry(destination_shard.node_id)
                            .or_default()
                            .push(destination_shard.shard_id_on_node);
                    }
                }));
                sender
            });

        sender
            .send(serialized_values)
            .await
            .expect("Failed to send serialized values to dedicated channel");
    }

    // Let's drop the senders, which will ensure that all batches are sent immediately,
    // then wait for all the tasks to finish, and ensure that there were no errors
    // In a production setting these dynamically instantiated tasks may be monitored more easily
    // by using e.g. `tokio_tasks_shutdown`
    std::mem::drop(channels_for_shards);
    for task in batching_tasks {
        task.await.unwrap();
    }

    // finally check that batching was indeed shard-aware.

    let mut expected: Vec<Vec<Shard>> = Arc::try_unwrap(shards_for_nodes_test_check)
        .expect("All batching tasks have finished")
        .into_inner()
        .into_values()
        .collect();

    let mut nodes_shards_calls: Vec<Vec<Shard>> = Vec::new();
    for rx in prepared_rxs.iter_mut() {
        let mut shards_calls = Vec::new();
        shards_calls.push(
            rx.recv()
                .await
                .expect("Each node should have received at least one message")
                .1
                .unwrap_or({
                    // Cassandra case (non-scylla)
                    0
                })
                .into(),
        );
        while let Ok((_, call_shard)) = rx.try_recv() {
            shards_calls.push(
                call_shard
                    .unwrap_or({
                        // Cassandra case (non-scylla)
                        0
                    })
                    .into(),
            )
        }
        nodes_shards_calls.push(shards_calls);
    }

    // Don't know which node is which
    // but at least once we don't care about which node is which they should agree about what was sent to what shard
    dbg!(&expected, &nodes_shards_calls);
    expected.sort_unstable();
    nodes_shards_calls.sort_unstable();
    assert_eq!(expected, nodes_shards_calls);

    running_proxy
}
