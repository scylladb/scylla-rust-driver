use super::{ChildLoadBalancingPolicy, LoadBalancingPolicy, Plan, Statement};
use crate::transport::{cluster::ClusterData, node::Node};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};
use tracing::trace;

/// A Round-robin load balancing policy.
#[derive(Debug)]
pub struct RoundRobinPolicy {
    index: AtomicUsize,
}

impl RoundRobinPolicy {
    pub fn new() -> Self {
        Self {
            index: AtomicUsize::new(0),
        }
    }
}

impl Default for RoundRobinPolicy {
    fn default() -> Self {
        Self::new()
    }
}

const ORDER_TYPE: Ordering = Ordering::Relaxed;

impl LoadBalancingPolicy for RoundRobinPolicy {
    fn plan<'a>(&self, _statement: &Statement, cluster: &'a ClusterData) -> Plan<'a> {
        let index = self.index.fetch_add(1, ORDER_TYPE);

        let nodes_count = cluster
            .replica_locator()
            .unique_nodes_in_global_ring()
            .len();
        let rotation = super::compute_rotation(index, nodes_count);
        let rotated_nodes = super::slice_rotated_left(
            cluster.replica_locator().unique_nodes_in_global_ring(),
            rotation,
        )
        .cloned();
        trace!(
            nodes = rotated_nodes
                .clone()
                .map(|node| node.address.to_string())
                .collect::<Vec<String>>()
                .join(",")
                .as_str(),
            "RoundRobin"
        );

        Box::new(rotated_nodes)
    }

    fn name(&self) -> String {
        "RoundRobinPolicy".to_string()
    }
}

impl ChildLoadBalancingPolicy for RoundRobinPolicy {
    fn apply_child_policy(
        &self,
        mut plan: Vec<Arc<Node>>,
    ) -> Box<dyn Iterator<Item = Arc<Node>> + Send + Sync> {
        let index = self.index.fetch_add(1, ORDER_TYPE);

        let len = plan.len(); // borrow checker forces making such a variable

        plan.rotate_left(super::compute_rotation(index, len));
        Box::new(plan.into_iter())
    }
}
