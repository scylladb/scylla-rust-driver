use super::{FallbackPlan, LoadBalancingPolicy, NodeRef, RoutingInfo};
use crate::transport::cluster::ClusterData;

#[derive(Debug, Default)]
pub struct DefaultPolicy {}

impl DefaultPolicy {
    pub fn new() -> Self {
        Self {}
    }
}

impl LoadBalancingPolicy for DefaultPolicy {
    fn pick<'a>(&'a self, info: &'a RoutingInfo, cluster: &'a ClusterData) -> Option<NodeRef<'a>> {
        self.fallback(info, cluster).next()
    }

    fn fallback<'a>(
        &'a self,
        _info: &'a RoutingInfo,
        cluster: &'a ClusterData,
    ) -> FallbackPlan<'a> {
        Box::new(
            cluster
                .replica_locator()
                .unique_nodes_in_global_ring()
                .iter(),
        )
    }

    fn name(&self) -> String {
        "DefaultPolicy".to_string()
    }
}
