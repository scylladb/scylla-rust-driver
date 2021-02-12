use super::{cluster::ClusterData, node::Node};
use crate::routing::Token;

use std::{
    collections::VecDeque,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
};

/// Represents info about statement that can be used by load balancing policies.
pub struct Statement {
    pub token: Option<Token>,
    pub keyspace: Option<String>,
}

/// Policy that decides which nodes to contact for each query
pub trait LoadBalancingPolicy: Send + Sync {
    /// It is used for each query to find which nodes to query first
    fn plan<'a>(
        &self,
        statement: &Statement,
        cluster: &'a ClusterData,
    ) -> Box<dyn Iterator<Item = Arc<Node>> + 'a>;

    /// Returns name of load balancing policy
    fn name(&self) -> String;
}

/// This trait is used to apply policy to plan made by parent policy.
///
/// For example, this enables RoundRobinPolicy to process plan made by TokenAwarePolicy.
pub trait ChildLoadBalancingPolicy: LoadBalancingPolicy {
    fn apply_child_policy(
        &self,
        plan: &mut dyn Iterator<Item = Arc<Node>>,
    ) -> Box<dyn Iterator<Item = Arc<Node>>>;
}

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
    ) -> Box<dyn Iterator<Item = Arc<Node>> + 'a> {
        let index = self.index.fetch_add(1, ORDER_TYPE);

        let nodes_count = cluster.all_nodes.len();
        let rotation = compute_rotation(index, nodes_count);
        let rotated_nodes = slice_rotated_left(&cluster.all_nodes, rotation).cloned();

        Box::new(rotated_nodes)
    }

    fn name(&self) -> String {
        "RoundRobinPolicy".to_string()
    }
}

impl ChildLoadBalancingPolicy for RoundRobinPolicy {
    fn apply_child_policy(
        &self,
        plan: &mut dyn Iterator<Item = Arc<Node>>,
    ) -> Box<dyn Iterator<Item = Arc<Node>>> {
        let index = self.index.fetch_add(1, ORDER_TYPE);

        // `plan` iterator is not cloneable, because of this we can't
        // call iter_rotated_left()
        let mut vec: VecDeque<Arc<Node>> = plan.collect();
        vec.rotate_left(compute_rotation(index, vec.len()));

        Box::new(vec.into_iter())
    }
}

/// A wrapper load balancing policy that adds token awareness to a child policy.
pub struct TokenAwarePolicy {
    child_policy: Box<dyn ChildLoadBalancingPolicy>,
}

impl TokenAwarePolicy {
    pub fn new(child_policy: Box<dyn ChildLoadBalancingPolicy>) -> Self {
        Self { child_policy }
    }
}

impl LoadBalancingPolicy for TokenAwarePolicy {
    fn plan<'a>(
        &self,
        statement: &Statement,
        cluster: &'a ClusterData,
    ) -> Box<dyn Iterator<Item = Arc<Node>> + 'a> {
        match statement.token {
            Some(token) => {
                // TODO: we try only the owner of the range (vnode) that the token lies in
                // we should calculate the *set* of replicas for this token, using the replication strategy
                // of the table being queried.
                let get_first_node = || cluster.ring.values().next().unwrap().clone();

                let owner: Arc<Node> = cluster
                    .ring
                    .range(token..)
                    .next()
                    .map(|(_token, node)| node.clone())
                    .unwrap_or_else(get_first_node);

                let mut plan = std::iter::once(owner);
                self.child_policy.apply_child_policy(&mut plan)
            }
            // fallback to child policy
            None => self.child_policy.plan(statement, cluster),
        }
    }

    fn name(&self) -> String {
        format!(
            "TokenAwarePolicy{{child_policy: {}}}",
            self.child_policy.name()
        )
    }
}

/// A data-center aware Round-robin load balancing policy.
pub struct DCAwareRoundRobinPolicy {
    index: AtomicUsize,
    local_dc: String,
}

impl DCAwareRoundRobinPolicy {
    pub fn new(local_dc: String) -> Self {
        Self {
            index: AtomicUsize::new(0),
            local_dc,
        }
    }

    fn is_local_node(node: &Node, local_dc: &str) -> bool {
        node.datacenter.as_deref() == Some(local_dc)
    }

    fn retrieve_local_nodes<'a>(&self, cluster: &'a ClusterData) -> &'a [Arc<Node>] {
        cluster
            .datacenters
            .get(&self.local_dc)
            .unwrap_or(EMPTY_NODE_LIST)
    }

    fn retrieve_remote_nodes<'a>(
        &self,
        cluster: &'a ClusterData,
    ) -> impl Iterator<Item = Arc<Node>> + Clone + 'a {
        // local_dc is moved into filter closure so clone is needed
        let local_dc = self.local_dc.clone();

        cluster
            .all_nodes
            .iter()
            .cloned()
            .filter(move |node| !DCAwareRoundRobinPolicy::is_local_node(node, &local_dc))
    }
}

const EMPTY_NODE_LIST: &Vec<Arc<Node>> = &vec![];

impl LoadBalancingPolicy for DCAwareRoundRobinPolicy {
    fn plan<'a>(
        &self,
        _statement: &Statement,
        cluster: &'a ClusterData,
    ) -> Box<dyn Iterator<Item = Arc<Node>> + 'a> {
        let index = self.index.fetch_add(1, ORDER_TYPE);

        let local_nodes = self.retrieve_local_nodes(cluster);
        let local_nodes_rotation = compute_rotation(index, local_nodes.len());
        let rotated_local_nodes = slice_rotated_left(local_nodes, local_nodes_rotation).cloned();

        let remote_nodes = self.retrieve_remote_nodes(cluster);
        let remote_nodes_count = cluster.all_nodes.len() - local_nodes.len();
        let remote_nodes_rotation = compute_rotation(index, remote_nodes_count);
        let rotated_remote_nodes = iter_rotated_left(remote_nodes, remote_nodes_rotation);

        let plan = rotated_local_nodes.chain(rotated_remote_nodes);
        Box::new(plan)
    }

    fn name(&self) -> String {
        "DCAwareRoundRobinPolicy".to_string()
    }
}

impl ChildLoadBalancingPolicy for DCAwareRoundRobinPolicy {
    fn apply_child_policy(
        &self,
        plan: &mut dyn Iterator<Item = Arc<Node>>,
    ) -> Box<dyn Iterator<Item = Arc<Node>>> {
        let index = self.index.fetch_add(1, ORDER_TYPE);

        let (local_nodes, remote_nodes): (Vec<_>, Vec<_>) =
            plan.partition(|node| DCAwareRoundRobinPolicy::is_local_node(node, &self.local_dc));

        let local_nodes_rotation = compute_rotation(index, local_nodes.len());
        let rotated_local_nodes = slice_rotated_left(&local_nodes, local_nodes_rotation);

        let remote_nodes_rotation = compute_rotation(index, remote_nodes.len());
        let rotated_remote_nodes = slice_rotated_left(&remote_nodes, remote_nodes_rotation);

        let plan = rotated_local_nodes
            .chain(rotated_remote_nodes)
            .cloned()
            .collect::<Vec<_>>()
            .into_iter();
        Box::new(plan)
    }
}

// Does safe modulo
fn compute_rotation(index: usize, count: usize) -> usize {
    if count != 0 {
        index % count
    } else {
        0
    }
}

// similar to slice::rotate_left, but works on iterators
fn iter_rotated_left<'a, T>(
    iter: impl Iterator<Item = T> + Clone + 'a,
    mid: usize,
) -> impl Iterator<Item = T> + Clone + 'a {
    let begin = iter.clone().skip(mid);
    let end = iter.take(mid);
    begin.chain(end)
}

// similar to slice::rotate_left, but it returns an iterator, doesn't mutate input
fn slice_rotated_left<'a, T>(slice: &'a [T], mid: usize) -> impl Iterator<Item = &T> + 'a {
    let begin = &slice[mid..];
    let end = &slice[..mid];
    begin.iter().chain(end.iter())
}

#[cfg(test)]
mod tests {
    use crate::transport::connection::ConnectionConfig;
    use std::collections::{BTreeMap, HashMap};
    use std::net::SocketAddr;

    use super::*;

    #[test]
    fn test_slice_rotation() {
        let a = [1, 2, 3, 4, 5];
        let a_rotated = slice_rotated_left(&a, 2).cloned().collect::<Vec<i32>>();

        assert_eq!(vec![3, 4, 5, 1, 2], a_rotated);
    }

    #[test]
    fn test_iter_rotation() {
        let a = [1, 2, 3, 4, 5];
        let a_iter = a.iter().cloned();
        let a_rotated = iter_rotated_left(a_iter, 2).collect::<Vec<i32>>();

        assert_eq!(vec![3, 4, 5, 1, 2], a_rotated);
    }

    fn create_node_with_failing_connection(id: u16, datacenter: Option<String>) -> Arc<Node> {
        let node = Node::new(
            SocketAddr::from(([255, 255, 255, 255], id)),
            ConnectionConfig {
                compression: None,
                tcp_nodelay: false,
            },
            datacenter,
            None,
            None,
        );

        Arc::new(node)
    }

    fn mock_cluster_data(nodes_recipe: &[(&str, u16)]) -> ClusterData {
        let all_nodes = nodes_recipe
            .iter()
            .map(|(dc, id)| create_node_with_failing_connection(*id, Some(dc.to_string())))
            .collect::<Vec<_>>();

        let known_peers = all_nodes
            .iter()
            .cloned()
            .map(|node| (node.address, node))
            .collect::<HashMap<_, _>>();

        let mut datacenters: HashMap<String, Vec<Arc<Node>>> = HashMap::new();

        for node in &all_nodes {
            if let Some(dc) = &node.datacenter {
                match datacenters.get_mut(dc) {
                    Some(v) => v.push(node.clone()),
                    None => {
                        let v = vec![node.clone()];
                        datacenters.insert(dc.clone(), v);
                    }
                }
            }
        }

        ClusterData {
            known_peers,
            ring: BTreeMap::new(),
            keyspaces: HashMap::new(),
            all_nodes,
            datacenters,
        }
    }

    const EMPTY_STATEMENT: Statement = Statement {
        token: None,
        keyspace: None,
    };

    fn get_plan_and_collect_node_identifiers<L: LoadBalancingPolicy>(
        policy: &L,
        statement: &Statement,
        cluster: &ClusterData,
    ) -> Vec<u16> {
        let plan = policy.plan(statement, &cluster);
        plan.map(|node| node.address.port()).collect::<Vec<_>>()
    }

    // ConnectionKeeper (which lives in Node) requires context of Tokio runtime
    #[tokio::test]
    async fn test_round_robin_policy() {
        let nodes_recipe = [("eu", 1), ("eu", 2), ("us", 3), ("us", 4)];

        let cluster = mock_cluster_data(&nodes_recipe);
        let policy = RoundRobinPolicy::new();

        let plans = (0..5)
            .map(|_| get_plan_and_collect_node_identifiers(&policy, &EMPTY_STATEMENT, &cluster))
            .collect::<Vec<_>>();

        let expected_plans = vec![
            vec![1, 2, 3, 4],
            vec![2, 3, 4, 1],
            vec![3, 4, 1, 2],
            vec![4, 1, 2, 3],
            vec![1, 2, 3, 4],
        ];

        assert_eq!(plans, expected_plans);
    }

    #[tokio::test]
    async fn test_dc_aware_round_robin_policy() {
        let nodes_recipe = [("eu", 1), ("eu", 2), ("eu", 3), ("us", 4), ("us", 5)];

        let cluster = mock_cluster_data(&nodes_recipe);
        let local_dc = "eu".to_string();
        let policy = DCAwareRoundRobinPolicy::new(local_dc);

        let plans = (0..4)
            .map(|_| get_plan_and_collect_node_identifiers(&policy, &EMPTY_STATEMENT, &cluster))
            .collect::<Vec<_>>();

        let expected_plans = vec![
            vec![1, 2, 3, 4, 5],
            vec![2, 3, 1, 5, 4],
            vec![3, 1, 2, 4, 5],
            vec![1, 2, 3, 5, 4],
        ];

        assert_eq!(plans, expected_plans);
    }

    #[tokio::test]
    async fn test_token_aware_fallback_policy() {
        let nodes_recipe = [("eu", 1), ("eu", 2), ("eu", 3), ("us", 4), ("us", 5)];

        let cluster = mock_cluster_data(&nodes_recipe);
        let local_dc = "eu".to_string();
        let policy = TokenAwarePolicy::new(Box::new(DCAwareRoundRobinPolicy::new(local_dc)));

        let plans = (0..4)
            .map(|_| get_plan_and_collect_node_identifiers(&policy, &EMPTY_STATEMENT, &cluster))
            .collect::<Vec<_>>();

        let expected_plans = vec![
            vec![1, 2, 3, 4, 5],
            vec![2, 3, 1, 5, 4],
            vec![3, 1, 2, 4, 5],
            vec![1, 2, 3, 5, 4],
        ];

        assert_eq!(plans, expected_plans);
    }

    #[test]
    fn test_names() {
        let local_dc = "eu".to_string();
        let policy = TokenAwarePolicy::new(Box::new(DCAwareRoundRobinPolicy::new(local_dc)));

        assert_eq!(
            policy.name(),
            "TokenAwarePolicy{child_policy: DCAwareRoundRobinPolicy}".to_string()
        );
    }
}
