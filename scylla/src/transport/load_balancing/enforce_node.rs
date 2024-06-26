use super::{DefaultPolicy, FallbackPlan, LoadBalancingPolicy, NodeRef, RoutingInfo};
use crate::{
    routing::Shard,
    transport::{cluster::ClusterData, Node},
};
use std::sync::Arc;
use uuid::Uuid;

/// This policy will always return the same node, unless it is not available anymore, in which case it will
/// fallback to the provided policy.
///
/// This is meant to be used for shard-aware batching.
#[derive(Debug)]
pub struct EnforceTargetShardPolicy {
    target_node: Uuid,
    shard: Shard,
    fallback: Arc<dyn LoadBalancingPolicy>,
}

impl EnforceTargetShardPolicy {
    pub fn new(
        target_node: &Arc<Node>,
        shard: Shard,
        fallback: Arc<dyn LoadBalancingPolicy>,
    ) -> Self {
        Self {
            target_node: target_node.host_id,
            shard,
            fallback,
        }
    }
}
impl LoadBalancingPolicy for EnforceTargetShardPolicy {
    fn pick<'a>(
        &'a self,
        query: &'a RoutingInfo,
        cluster: &'a ClusterData,
    ) -> Option<(NodeRef<'a>, Option<Shard>)> {
        cluster
            .known_peers
            .get(&self.target_node)
            .map(|node| (node, Some(self.shard)))
            .filter(|&(node, shard)| DefaultPolicy::is_alive(node, shard))
            .or_else(|| self.fallback.pick(query, cluster))
    }

    fn fallback<'a>(
        &'a self,
        query: &'a RoutingInfo,
        cluster: &'a ClusterData,
    ) -> FallbackPlan<'a> {
        self.fallback.fallback(query, cluster)
    }

    fn name(&self) -> String {
        format!(
            "Enforce target shard Load balancing policy - Node: {} - fallback: {}",
            self.target_node,
            self.fallback.name()
        )
    }
}
