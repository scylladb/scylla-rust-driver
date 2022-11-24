use super::{ChildLoadBalancingPolicy, LoadBalancingPolicy, Plan, Statement};
use crate::transport::{cluster::ClusterData, node::Node};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};
use tracing::trace;

/// A data-center aware Round-robin load balancing policy.
#[derive(Debug)]
pub struct DcAwareRoundRobinPolicy {
    index: AtomicUsize,
    local_dc: String,
    include_remote_nodes: bool,
}

impl DcAwareRoundRobinPolicy {
    pub fn new(local_dc: String) -> Self {
        Self {
            index: AtomicUsize::new(0),
            local_dc,
            include_remote_nodes: true,
        }
    }

    pub fn set_include_remote_nodes(&mut self, val: bool) {
        self.include_remote_nodes = val;
    }

    fn is_local_node(node: &Node, local_dc: &str) -> bool {
        node.datacenter.as_deref() == Some(local_dc)
    }

    fn retrieve_local_nodes<'a>(&self, cluster: &'a ClusterData) -> &'a [Arc<Node>] {
        cluster
            .replica_locator()
            .unique_nodes_in_datacenter_ring(&self.local_dc)
            .unwrap_or(EMPTY_NODE_LIST)
    }

    fn retrieve_remote_nodes<'a>(
        &self,
        cluster: &'a ClusterData,
    ) -> impl Iterator<Item = Arc<Node>> + Clone + 'a {
        // local_dc is moved into filter closure so clone is needed
        let local_dc = self.local_dc.clone();

        cluster
            .replica_locator()
            .unique_nodes_in_global_ring()
            .iter()
            .cloned()
            .filter(move |node| !DcAwareRoundRobinPolicy::is_local_node(node, &local_dc))
    }
}

const EMPTY_NODE_LIST: &Vec<Arc<Node>> = &vec![];
const ORDER_TYPE: Ordering = Ordering::Relaxed;

impl LoadBalancingPolicy for DcAwareRoundRobinPolicy {
    fn plan<'a>(&self, _statement: &Statement, cluster: &'a ClusterData) -> Plan<'a> {
        let index = self.index.fetch_add(1, ORDER_TYPE);

        let local_nodes = self.retrieve_local_nodes(cluster);
        let local_nodes_rotation = super::compute_rotation(index, local_nodes.len());
        let rotated_local_nodes =
            super::slice_rotated_left(local_nodes, local_nodes_rotation).cloned();

        if self.include_remote_nodes {
            let remote_nodes = self.retrieve_remote_nodes(cluster);
            let remote_nodes_count = cluster
                .replica_locator()
                .unique_nodes_in_global_ring()
                .len()
                - local_nodes.len();
            let remote_nodes_rotation = super::compute_rotation(index, remote_nodes_count);
            let rotated_remote_nodes =
                super::iter_rotated_left(remote_nodes, remote_nodes_rotation);
            trace!(
                local_nodes = rotated_local_nodes
                    .clone()
                    .map(|node| node.address.to_string())
                    .collect::<Vec<String>>()
                    .join(",")
                    .as_str(),
                remote_nodes = rotated_remote_nodes
                    .clone()
                    .map(|node| node.address.to_string())
                    .collect::<Vec<String>>()
                    .join(",")
                    .as_str(),
                "DC Aware"
            );
            Box::new(rotated_local_nodes.chain(rotated_remote_nodes))
        } else {
            trace!(
                local_nodes = rotated_local_nodes
                    .clone()
                    .map(|node| node.address.to_string())
                    .collect::<Vec<String>>()
                    .join(",")
                    .as_str(),
                "DC Aware"
            );
            Box::new(rotated_local_nodes)
        }
    }

    fn name(&self) -> String {
        "DcAwareRoundRobinPolicy".to_string()
    }
}

impl ChildLoadBalancingPolicy for DcAwareRoundRobinPolicy {
    fn apply_child_policy(
        &self,
        plan: Vec<Arc<Node>>,
    ) -> Box<dyn Iterator<Item = Arc<Node>> + Send + Sync> {
        let index = self.index.fetch_add(1, ORDER_TYPE);

        let (local_nodes, remote_nodes): (Vec<_>, Vec<_>) = plan
            .into_iter()
            .partition(|node| DcAwareRoundRobinPolicy::is_local_node(node, &self.local_dc));

        let local_nodes_rotation = super::compute_rotation(index, local_nodes.len());
        let rotated_local_nodes = super::slice_rotated_left(&local_nodes, local_nodes_rotation);

        let plan = if self.include_remote_nodes {
            let remote_nodes_rotation = super::compute_rotation(index, remote_nodes.len());
            let rotated_remote_nodes =
                super::slice_rotated_left(&remote_nodes, remote_nodes_rotation);

            rotated_local_nodes
                .chain(rotated_remote_nodes)
                .cloned()
                .collect::<Vec<_>>()
        } else {
            rotated_local_nodes.cloned().collect()
        };
        Box::new(plan.into_iter())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::transport::load_balancing::tests;
    use std::collections::HashSet;

    async fn test_dc_aware_round_robin_policy(
        policy: DcAwareRoundRobinPolicy,
        expected_plans: HashSet<Vec<u16>>,
    ) {
        let cluster = tests::mock_cluster_data_for_round_robin_and_latency_aware_tests().await;

        let plans = (0..32)
            .map(|_| {
                tests::get_plan_and_collect_node_identifiers(
                    &policy,
                    &tests::EMPTY_STATEMENT,
                    &cluster,
                )
            })
            .collect::<HashSet<_>>();

        assert_eq!(expected_plans, plans);
    }

    #[tokio::test]
    async fn test_dc_aware_round_robin_policy_with_remote_nodes() {
        let local_dc = "eu".to_string();
        let policy = DcAwareRoundRobinPolicy::new(local_dc);

        let expected_plans = vec![
            vec![1, 2, 3, 4, 5],
            vec![1, 2, 3, 5, 4],
            vec![2, 3, 1, 5, 4],
            vec![2, 3, 1, 4, 5],
            vec![3, 1, 2, 4, 5],
            vec![3, 1, 2, 5, 4],
        ]
        .into_iter()
        .collect::<HashSet<_>>();

        test_dc_aware_round_robin_policy(policy, expected_plans).await;
    }

    #[tokio::test]
    async fn test_dc_aware_round_robin_policy_without_remote_nodes() {
        let local_dc = "eu".to_string();
        let mut policy = DcAwareRoundRobinPolicy::new(local_dc);
        policy.set_include_remote_nodes(false);

        let expected_plans = vec![vec![1, 2, 3], vec![2, 3, 1], vec![3, 1, 2]]
            .into_iter()
            .collect::<HashSet<_>>();

        test_dc_aware_round_robin_policy(policy, expected_plans).await;
    }
}
