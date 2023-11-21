use std::sync::Arc;

use crate::utils::test_with_3_node_cluster;
use scylla::{test_utils::unique_keyspace_name, SessionBuilder};
use tokio::sync::mpsc;

use scylla_proxy::TargetShard;
use scylla_proxy::{
    Condition, Reaction, RequestOpcode, RequestReaction, RequestRule, ShardAwareness,
};
use scylla_proxy::{ProxyError, RequestFrame, WorkerError};

#[tokio::test]
#[ntest::timeout(30000)]
#[cfg(not(scylla_cloud_tests))]
async fn test_consistent_shard_awareness() {
    use std::collections::HashSet;

    let res = test_with_3_node_cluster(ShardAwareness::QueryNode, |proxy_uris, translation_map, mut running_proxy| async move {

        let (feedback_txs, mut feedback_rxs): (Vec<_>, Vec<_>) = (0..3).map(|_| {
            mpsc::unbounded_channel::<(RequestFrame, Option<TargetShard>)>()
        }).unzip();
        for (i, tx) in feedback_txs.iter().cloned().enumerate() {
            running_proxy.running_nodes[i].change_request_rules(Some(vec![
                RequestRule(Condition::RequestOpcode(RequestOpcode::Execute).and(Condition::not(Condition::ConnectionRegisteredAnyEvent)), RequestReaction::noop().with_feedback_when_performed(tx))
            ]));
        }

        let session = SessionBuilder::new()
            .known_node(proxy_uris[0].as_str())
            .address_translator(Arc::new(translation_map))
            .build()
            .await
            .unwrap();
        let ks = unique_keyspace_name();

        /* Prepare schema */
        session.query(format!("CREATE KEYSPACE IF NOT EXISTS {} WITH REPLICATION = {{'class' : 'NetworkTopologyStrategy', 'replication_factor' : 3}}", ks), &[]).await.unwrap();
        session
            .query(
                format!(
                    "CREATE TABLE IF NOT EXISTS {}.t (a int, b int, c text, primary key (a, b))",
                    ks
                ),
                &[],
            )
            .await
            .unwrap();

        let prepared = session.prepare(format!("INSERT INTO {}.t (a, b, c) VALUES (?, ?, 'abc')", ks)).await.unwrap();

        let value_lists = [
            (4, 2),
            (2, 1),
            (3, 7),
        ];

        fn assert_one_shard_queried(rx: &mut mpsc::UnboundedReceiver<(RequestFrame, Option<TargetShard>)>) {
            let shards = std::iter::from_fn(|| rx.try_recv().ok().map(|(_frame, shard)| shard)).collect::<HashSet<_>>();
            if !shards.is_empty() {
                assert_eq!(shards.len(), 1);
            }
        }

        for values in value_lists {
            for _ in 0..10 {
                session.execute(&prepared, values).await.unwrap();
            }
            for rx in feedback_rxs.iter_mut() {
                assert_one_shard_queried(rx);
            }
        }

        running_proxy
    }).await;
    match res {
        Ok(()) => (),
        Err(ProxyError::Worker(WorkerError::DriverDisconnected(_))) => (),
        Err(err) => panic!("{}", err),
    }
}
