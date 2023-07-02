use super::{FallbackPlan, LoadBalancingPolicy, NodeRef, RoutingInfo};
use crate::transport::{cluster::ClusterData, Node};
use std::sync::Arc;

#[derive(Debug)]
pub struct EnforceTargetNodePolicy {
    target_node: uuid::Uuid,
    fallback: Arc<dyn LoadBalancingPolicy>,
}

impl EnforceTargetNodePolicy {
    pub fn new(target_node: &Arc<Node>, fallback: Arc<dyn LoadBalancingPolicy>) -> Self {
        Self {
            target_node: target_node.host_id,
            fallback,
        }
    }
}
impl LoadBalancingPolicy for EnforceTargetNodePolicy {
    fn pick<'a>(&'a self, query: &'a RoutingInfo, cluster: &'a ClusterData) -> Option<NodeRef<'a>> {
        cluster
            .known_peers
            .get(&self.target_node)
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
            "Enforce target node Load balancing policy - Node: {} - fallback: {}",
            self.target_node,
            self.fallback.name()
        )
    }
}
