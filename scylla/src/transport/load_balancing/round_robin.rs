use super::{ChildLoadBalancingPolicy, LoadBalancingPolicy, Statement};
use crate::transport::{cluster::ClusterData, node::Node};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};
use tracing::trace;

/// A Round-robin load balancing policy.
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
    fn plan<'a>(
        &self,
        _statement: &Statement,
        cluster: &'a ClusterData,
    ) -> Box<dyn Iterator<Item = Arc<Node>> + Send + Sync + 'a> {
        let index = self.index.fetch_add(1, ORDER_TYPE);

        let nodes_count = cluster.all_nodes.len();
        let rotation = super::compute_rotation(index, nodes_count);
        let rotated_nodes = super::slice_rotated_left(&cluster.all_nodes, rotation).cloned();
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

#[cfg(test)]
mod tests {
    use super::*;

    use crate::transport::load_balancing::tests;

    // ConnectionKeeper (which lives in Node) requires context of Tokio runtime
    #[tokio::test]
    async fn test_round_robin_policy() {
        let cluster = tests::mock_cluster_data_for_round_robin_tests();

        let policy = RoundRobinPolicy::new();

        let plans = (0..6)
            .map(|_| {
                tests::get_plan_and_collect_node_identifiers(
                    &policy,
                    &tests::EMPTY_STATEMENT,
                    &cluster,
                )
            })
            .collect::<Vec<_>>();

        let expected_plans = vec![
            vec![1, 2, 3, 4, 5],
            vec![2, 3, 4, 5, 1],
            vec![3, 4, 5, 1, 2],
            vec![4, 5, 1, 2, 3],
            vec![5, 1, 2, 3, 4],
            vec![1, 2, 3, 4, 5],
        ];

        assert_eq!(plans, expected_plans);
    }
}
