use std::sync::Arc;

use scylla::client::execution_profile::ExecutionProfile;
use scylla::cluster::{ClusterState, NodeRef};
use scylla::policies::load_balancing::{
    DefaultPolicy, FallbackPlan, LatencyAwarenessBuilder, LoadBalancingPolicy, RoutingInfo,
};
use scylla::routing::Shard;

use crate::utils::{create_new_session_builder, setup_tracing};

// This is a regression test for #696.
#[tokio::test]
#[ntest::timeout(1000)]
async fn latency_aware_query_completes() {
    setup_tracing();
    let policy = DefaultPolicy::builder()
        .latency_awareness(LatencyAwarenessBuilder::default())
        .build();
    let handle = ExecutionProfile::builder()
        .load_balancing_policy(policy)
        .build()
        .into_handle();

    let session = create_new_session_builder()
        .default_execution_profile_handle(handle)
        .build()
        .await
        .unwrap();

    session.query_unpaged("whatever", ()).await.unwrap_err();
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
