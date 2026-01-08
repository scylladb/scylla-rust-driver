use std::collections::HashSet;
use std::sync::Arc;

use crate::utils::{
    PerformDDL, create_new_session_builder, scylla_supports_tablets, setup_tracing,
    test_with_3_node_cluster, unique_keyspace_name,
};
use scylla::client::execution_profile::ExecutionProfile;
use scylla::client::session_builder::SessionBuilder;
use scylla::cluster::{ClusterState, NodeRef};
use scylla::policies::load_balancing::{FallbackPlan, LoadBalancingPolicy, RoutingInfo};
use scylla::routing::Shard;
use tokio::sync::mpsc;

use scylla_proxy::TargetShard;
use scylla_proxy::{
    Condition, Reaction, RequestOpcode, RequestReaction, RequestRule, ShardAwareness,
};
use scylla_proxy::{ProxyError, RequestFrame, WorkerError};

#[tokio::test]
#[ntest::timeout(30000)]
async fn test_consistent_shard_awareness() {
    setup_tracing();

    let res = test_with_3_node_cluster(ShardAwareness::QueryNode, |proxy_uris, translation_map, mut running_proxy| async move {

        let session = SessionBuilder::new()
            .known_node(proxy_uris[0].as_str())
            .address_translator(Arc::new(translation_map))
            .build()
            .await
            .unwrap();
        let ks = unique_keyspace_name();

        /* Prepare schema */
        let mut create_ks = format!("CREATE KEYSPACE IF NOT EXISTS {ks} WITH REPLICATION = {{'class' : 'NetworkTopologyStrategy', 'replication_factor' : 3}}");
        if scylla_supports_tablets(&session).await {
            create_ks += " and TABLETS = { 'enabled': false}";
        }
        session.ddl(create_ks).await.unwrap();
        session
            .ddl(
                format!(
                    "CREATE TABLE IF NOT EXISTS {ks}.t (a int, b int, c text, primary key (a, b))"
                ),
            )
            .await
            .unwrap();

        let (feedback_txs, mut feedback_rxs): (Vec<_>, Vec<_>) = (0..3).map(|_| {
            mpsc::unbounded_channel::<(RequestFrame, Option<TargetShard>)>()
        }).unzip();
        for (i, tx) in feedback_txs.iter().cloned().enumerate() {
            running_proxy.running_nodes[i].change_request_rules(Some(vec![
                RequestRule(Condition::RequestOpcode(RequestOpcode::Execute).and(Condition::not(Condition::ConnectionRegisteredAnyEvent)), RequestReaction::noop().with_feedback_when_performed(tx))
            ]));
        }

        let prepared = session.prepare(format!("INSERT INTO {ks}.t (a, b, c) VALUES (?, ?, 'abc')")).await.unwrap();

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
                session.execute_unpaged(&prepared, values).await.unwrap();
            }
            for rx in feedback_rxs.iter_mut() {
                assert_one_shard_queried(rx);
            }
        }

        running_proxy.turn_off_rules();
        session.ddl(format!("DROP KEYSPACE {ks}")).await.unwrap();

        running_proxy
    }).await;
    match res {
        Ok(()) => (),
        Err(ProxyError::Worker(WorkerError::DriverDisconnected(_))) => (),
        Err(err) => panic!("{}", err),
    }
}

/// Regression test for panic that appeared if LBP picked a
/// shard that is out of range for the node.
#[tokio::test]
async fn test_shard_out_of_range() {
    setup_tracing();

    #[derive(Debug)]
    struct ShardOutOfRangeLBP;
    impl LoadBalancingPolicy for ShardOutOfRangeLBP {
        fn pick<'a>(
            &'a self,
            _query: &'a RoutingInfo,
            cluster: &'a ClusterState,
        ) -> Option<(NodeRef<'a>, Option<Shard>)> {
            let node = &cluster.get_nodes_info()[0];
            match node.sharder() {
                Some(sharder) => Some((node, Some(sharder.nr_shards.get() as u32))),
                // For Cassandra let's pick some crazy shard number - it should be ignored anyway.
                None => Some((node, Some(u16::MAX as u32))),
            }
        }

        fn fallback<'a>(
            &'a self,
            _query: &'a RoutingInfo,
            _cluster: &'a ClusterState,
        ) -> FallbackPlan<'a> {
            Box::new(std::iter::empty())
        }

        fn name(&self) -> String {
            "ShardOutOfRangeLBP".into()
        }
    }

    let handle = ExecutionProfile::builder()
        .load_balancing_policy(Arc::new(ShardOutOfRangeLBP))
        .build()
        .into_handle();
    let session = create_new_session_builder()
        .default_execution_profile_handle(handle)
        .build()
        .await
        .unwrap();

    let _ = session
        .query_unpaged("SELECT * FROM system.local WHERE key='local'", ())
        .await
        .unwrap();
}
