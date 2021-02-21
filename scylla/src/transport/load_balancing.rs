use super::{cluster::ClusterData, node::Node};
use crate::routing::Token;
use crate::transport::topology::Strategy;

use itertools::Itertools;
use std::{
    collections::HashMap,
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
    fn apply_child_policy(&self, plan: Vec<Arc<Node>>) -> Box<dyn Iterator<Item = Arc<Node>>>;
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
    fn apply_child_policy(&self, mut plan: Vec<Arc<Node>>) -> Box<dyn Iterator<Item = Arc<Node>>> {
        let index = self.index.fetch_add(1, ORDER_TYPE);

        let len = plan.len(); // borrow checker forces making such a variable

        plan.rotate_left(compute_rotation(index, len));
        Box::new(plan.into_iter())
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

    fn simple_strategy_replicas<'a>(
        cluster: &'a ClusterData,
        token: &Token,
        replication_factor: usize,
    ) -> Vec<Arc<Node>> {
        cluster
            .ring_range(&token)
            .unique()
            .take(replication_factor)
            .collect()
    }

    fn network_topology_strategy_replicas<'a>(
        cluster: &'a ClusterData,
        token: &Token,
        datacenter_repfactors: &HashMap<String, usize>,
    ) -> Vec<Arc<Node>> {
        let mut acceptable_repeats = datacenter_repfactors
            .iter()
            .map(|(dc_name, repfactor)| {
                let rack_count = cluster
                    .datacenters
                    .get(dc_name)
                    .map(|dc| dc.rack_count)
                    .unwrap_or(0);

                (dc_name.clone(), repfactor - rack_count)
            })
            .collect::<HashMap<String, usize>>();

        let desired_result_len: usize = datacenter_repfactors.values().sum();

        let mut result: Vec<Arc<Node>> = Vec::with_capacity(desired_result_len);
        for node in cluster.ring_range(&token).unique() {
            let current_node_dc = match &node.datacenter {
                None => continue,
                Some(dc) => dc,
            };

            let repfactor = match datacenter_repfactors.get(current_node_dc) {
                None => continue,
                Some(r) => r,
            };

            let picked_nodes_from_current_dc = || {
                result
                    .iter()
                    .filter(|node| node.datacenter.as_ref() == Some(current_node_dc))
            };

            if *repfactor == picked_nodes_from_current_dc().count() {
                // found enough nodes in this datacenter
                continue;
            }

            let current_node_rack = node.rack.as_ref();
            let current_node_rack_count = picked_nodes_from_current_dc()
                .filter(|node| node.rack.as_ref() == current_node_rack)
                .count();

            if current_node_rack_count == 0 {
                // new rack
                result.push(node.clone());
            } else {
                // we’ve already found a node in this rack

                // unwrap, because we already know repfactor
                let repeats = acceptable_repeats.get_mut(current_node_dc).unwrap();
                if *repeats > 0 {
                    // we must pick multiple nodes in the same rack
                    *repeats -= 1;
                    result.push(node.clone());
                }
            }

            if result.len() == desired_result_len {
                break;
            }
        }

        result
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
                let keyspace = statement
                    .keyspace
                    .as_ref()
                    .map(|k| cluster.keyspaces.get(k))
                    .flatten();

                let strategy = keyspace
                    .map(|k| &k.strategy)
                    // default to simple strategy
                    .unwrap_or(&Strategy::SimpleStrategy {
                        replication_factor: 1,
                    });

                match strategy {
                    Strategy::SimpleStrategy { replication_factor } => {
                        let replicas =
                            Self::simple_strategy_replicas(cluster, &token, *replication_factor);
                        self.child_policy.apply_child_policy(replicas)
                    }
                    Strategy::NetworkTopologyStrategy {
                        datacenter_repfactors,
                    } => {
                        let replicas = Self::network_topology_strategy_replicas(
                            cluster,
                            &token,
                            datacenter_repfactors,
                        );
                        self.child_policy.apply_child_policy(replicas)
                    }
                    _ => {
                        // default to simple strategy with replication factor = 1
                        let replication_factor = 1;
                        let replica =
                            Self::simple_strategy_replicas(cluster, &token, replication_factor);
                        self.child_policy.apply_child_policy(replica)
                    }
                }
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
            .map(|dc| &dc.nodes)
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
    fn apply_child_policy(&self, plan: Vec<Arc<Node>>) -> Box<dyn Iterator<Item = Arc<Node>>> {
        let index = self.index.fetch_add(1, ORDER_TYPE);

        let (local_nodes, remote_nodes): (Vec<_>, Vec<_>) = plan
            .into_iter()
            .partition(|node| DCAwareRoundRobinPolicy::is_local_node(node, &self.local_dc));

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
    use crate::transport::cluster::Datacenter;
    use crate::transport::connection::ConnectionConfig;
    use crate::transport::topology::Keyspace;
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

    fn create_node_with_failing_connection(
        id: u16,
        datacenter: Option<String>,
        rack: Option<String>,
    ) -> Arc<Node> {
        let node = Node::new(
            SocketAddr::from(([255, 255, 255, 255], id)),
            ConnectionConfig {
                compression: None,
                tcp_nodelay: false,
            },
            datacenter,
            rack,
            None,
        );

        Arc::new(node)
    }

    struct NodeMockRecipe {
        datacenter: Option<String>,
        rack: Option<String>,
        id: u16,
        tokens: Vec<Token>,
    }

    fn mock_cluster_data(nodes_recipe: &[NodeMockRecipe]) -> ClusterData {
        let mut ring = BTreeMap::new();
        let mut all_nodes = Vec::new();
        let mut datacenters: HashMap<String, Datacenter> = HashMap::new();

        for recipe in nodes_recipe {
            let node = create_node_with_failing_connection(
                recipe.id,
                recipe.datacenter.clone(),
                recipe.rack.clone(),
            );

            for vnode in &recipe.tokens {
                ring.insert(*vnode, node.clone());
            }

            if let Some(dc) = &node.datacenter {
                match datacenters.get_mut(dc) {
                    Some(v) => v.nodes.push(node.clone()),
                    None => {
                        let v = vec![node.clone()];
                        datacenters.insert(
                            dc.clone(),
                            Datacenter {
                                nodes: v,
                                rack_count: 0,
                            },
                        );
                    }
                }
            }

            all_nodes.push(node);
        }

        let known_peers = all_nodes
            .iter()
            .cloned()
            .map(|node| (node.address, node))
            .collect::<HashMap<_, _>>();

        let keyspaces = [
            (
                "keyspace_with_simple_strategy_replication_factor_2".into(),
                Keyspace {
                    strategy: Strategy::SimpleStrategy {
                        replication_factor: 2,
                    },
                },
            ),
            (
                "keyspace_with_simple_strategy_replication_factor_3".into(),
                Keyspace {
                    strategy: Strategy::SimpleStrategy {
                        replication_factor: 3,
                    },
                },
            ),
            (
                "keyspace_with_nts".into(),
                Keyspace {
                    strategy: Strategy::NetworkTopologyStrategy {
                        datacenter_repfactors: [("waw".to_string(), 2), ("her".to_string(), 3)]
                            .iter()
                            .cloned()
                            .collect::<HashMap<_, _>>(),
                    },
                },
            ),
        ]
        .iter()
        .cloned()
        .collect();

        let mut cluster = ClusterData {
            known_peers,
            ring,
            keyspaces,
            all_nodes,
            datacenters,
        };
        cluster.update_rack_count();

        cluster
    }

    // creates ClusterData with info about 5 nodes living in 2 different datacenters
    // ring field is empty
    fn mock_cluster_data_for_round_robin_tests() -> ClusterData {
        let nodes_recipe = [("eu", 1), ("eu", 2), ("eu", 3), ("us", 4), ("us", 5)]
            .iter()
            .map(|(dc, id)| NodeMockRecipe {
                datacenter: Some(dc.to_string()),
                rack: None,
                id: *id,
                tokens: Vec::new(),
            })
            .collect::<Vec<_>>();

        mock_cluster_data(&nodes_recipe)
    }

    // creates ClusterData with info about 3 nodes living in the same datacenter
    // ring field is populated as follows:
    // ring tokens:            50 100 150 200 250 300 400 500
    // corresponding node ids: 2  1   2   3   1   2   3   1
    fn mock_cluster_data_for_token_aware_tests() -> ClusterData {
        let nodes_recipe = [
            NodeMockRecipe {
                datacenter: Some("eu".into()),
                rack: None,
                id: 1,
                tokens: vec![
                    Token { value: 100 },
                    Token { value: 250 },
                    Token { value: 500 },
                ],
            },
            NodeMockRecipe {
                datacenter: Some("eu".into()),
                rack: None,
                id: 2,
                tokens: vec![
                    Token { value: 50 },
                    Token { value: 150 },
                    Token { value: 300 },
                ],
            },
            NodeMockRecipe {
                datacenter: Some("us".into()),
                rack: None,
                id: 3,
                tokens: vec![Token { value: 200 }, Token { value: 400 }],
            },
        ];

        mock_cluster_data(&nodes_recipe)
    }

    // creates ClusterData with info about 8 nodes living in two different datacenters
    //
    // ring field is populated as follows:
    // ring tokens:            50 100 150 200 250 300 400 500 510
    // corresponding node ids: 1  5   2   1   6   4   8   7   3
    //
    // datacenter:       waw
    // nodes in rack r1: 1 2
    // nodes in rack r2: 3 4
    //
    // datacenter:       her
    // nodes in rack r3: 5 6
    // nodes in rack r4: 7 8
    fn mock_cluster_data_for_nts_token_aware_tests() -> ClusterData {
        let nodes_recipe = [
            NodeMockRecipe {
                datacenter: Some("waw".into()),
                rack: Some("r1".into()),
                id: 1,
                tokens: vec![Token { value: 50 }, Token { value: 200 }],
            },
            NodeMockRecipe {
                datacenter: Some("waw".into()),
                rack: Some("r1".into()),
                id: 2,
                tokens: vec![Token { value: 150 }],
            },
            NodeMockRecipe {
                datacenter: Some("waw".into()),
                rack: Some("r2".into()),
                id: 3,
                tokens: vec![Token { value: 510 }],
            },
            NodeMockRecipe {
                datacenter: Some("waw".into()),
                rack: Some("r2".into()),
                id: 4,
                tokens: vec![Token { value: 300 }],
            },
            NodeMockRecipe {
                datacenter: Some("her".into()),
                rack: Some("r3".into()),
                id: 5,
                tokens: vec![Token { value: 100 }],
            },
            NodeMockRecipe {
                datacenter: Some("her".into()),
                rack: Some("r3".into()),
                id: 6,
                tokens: vec![Token { value: 250 }],
            },
            NodeMockRecipe {
                datacenter: Some("her".into()),
                rack: Some("r4".into()),
                id: 7,
                tokens: vec![Token { value: 500 }],
            },
            NodeMockRecipe {
                datacenter: Some("her".into()),
                rack: Some("r4".into()),
                id: 8,
                tokens: vec![Token { value: 400 }],
            },
        ];

        mock_cluster_data(&nodes_recipe)
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
        let cluster = mock_cluster_data_for_round_robin_tests();

        let policy = RoundRobinPolicy::new();

        let plans = (0..6)
            .map(|_| get_plan_and_collect_node_identifiers(&policy, &EMPTY_STATEMENT, &cluster))
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

    #[tokio::test]
    async fn test_dc_aware_round_robin_policy() {
        let cluster = mock_cluster_data_for_round_robin_tests();

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
        let cluster = mock_cluster_data_for_round_robin_tests();

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

    #[tokio::test]
    async fn test_token_aware_policy_with_simple_strategy() {
        let cluster = mock_cluster_data_for_token_aware_tests();

        struct Test {
            statement: Statement,
            expected_plans: Vec<Vec<u16>>,
        };

        let tests = [
            Test {
                statement: Statement {
                    token: Some(Token { value: 160 }),
                    keyspace: Some("keyspace_with_simple_strategy_replication_factor_2".into()),
                },
                expected_plans: vec![vec![3, 1], vec![1, 3]],
            },
            Test {
                statement: Statement {
                    token: Some(Token { value: 60 }),
                    keyspace: Some("keyspace_with_simple_strategy_replication_factor_3".into()),
                },
                expected_plans: vec![vec![1, 2, 3], vec![2, 3, 1], vec![3, 1, 2]],
            },
            Test {
                statement: Statement {
                    token: Some(Token { value: 500 }),
                    keyspace: Some("keyspace_with_simple_strategy_replication_factor_3".into()),
                },
                expected_plans: vec![vec![1, 2, 3], vec![2, 3, 1], vec![3, 1, 2]],
            },
            Test {
                statement: Statement {
                    token: Some(Token { value: 60 }),
                    keyspace: Some("invalid".into()),
                },
                expected_plans: vec![vec![1]],
            },
            Test {
                statement: Statement {
                    token: Some(Token { value: 60 }),
                    keyspace: None,
                },
                expected_plans: vec![vec![1]],
            },
        ];

        for test in &tests {
            let policy = TokenAwarePolicy::new(Box::new(RoundRobinPolicy::new()));

            let plan_count = test.expected_plans.len();
            let plans = (0..plan_count)
                .map(|_| get_plan_and_collect_node_identifiers(&policy, &test.statement, &cluster))
                .collect::<Vec<_>>();

            assert_eq!(plans, test.expected_plans);
        }
    }

    #[tokio::test]
    async fn test_token_aware_policy_with_nts() {
        let cluster = mock_cluster_data_for_nts_token_aware_tests();

        let policy = TokenAwarePolicy::new(Box::new(RoundRobinPolicy::new()));

        let statement = Statement {
            token: Some(Token { value: 0 }),
            keyspace: Some("keyspace_with_nts".into()),
        };

        let plan = get_plan_and_collect_node_identifiers(&policy, &statement, &cluster);
        let expected_plan = vec![1, 5, 6, 4, 8];

        assert_eq!(plan, expected_plan);
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
