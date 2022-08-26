//! Load balancing configurations\
//! `Session` can use any load balancing policy which implements the `LoadBalancingPolicy` trait\
//! Policies which implement the `ChildLoadBalancingPolicy` can be wrapped in some other policies\
//! See [the book](https://rust-driver.docs.scylladb.com/stable/load-balancing/load-balancing.html) for more information

use super::{cluster::ClusterData, node::Node};
use crate::routing::Token;

use std::{collections::hash_map::DefaultHasher, hash::Hasher, sync::Arc};

mod dc_aware_round_robin;
mod round_robin;
mod token_aware;

pub use dc_aware_round_robin::DcAwareRoundRobinPolicy;
pub use round_robin::RoundRobinPolicy;
pub use token_aware::TokenAwarePolicy;

mod latency_aware;

pub use latency_aware::LatencyAwarePolicy;

/// Represents info about statement that can be used by load balancing policies.
#[derive(Default)]
pub struct Statement<'a> {
    pub token: Option<Token>,
    pub keyspace: Option<&'a str>,
}

impl<'a> Statement<'a> {
    fn empty() -> Self {
        Self {
            token: None,
            keyspace: None,
        }
    }
}

pub type Plan<'a> = Box<dyn Iterator<Item = Arc<Node>> + Send + Sync + 'a>;

/// Policy that decides which nodes to contact for each query
pub trait LoadBalancingPolicy: Send + Sync + std::fmt::Debug {
    /// It is used for each query to find which nodes to query first
    fn plan<'a>(&self, statement: &Statement, cluster: &'a ClusterData) -> Plan<'a>;

    /// Returns name of load balancing policy
    fn name(&self) -> String;

    /// Informs whether latency measurements should be done when the policy is active.
    fn requires_latency_measurements(&self) -> bool {
        false
    }

    fn update_cluster_data(&self, _cluster_data: &ClusterData) {}
}

/// This trait is used to apply policy to plan made by parent policy.
///
/// For example, this enables RoundRobinPolicy to process plan made by TokenAwarePolicy.
pub trait ChildLoadBalancingPolicy: LoadBalancingPolicy {
    fn apply_child_policy(
        &self,
        plan: Vec<Arc<Node>>,
    ) -> Box<dyn Iterator<Item = Arc<Node>> + Send + Sync>;
}

// Hashing round robin's index is a mitigation to problems that occur when a
// `RoundRobin::apply_child_policy()` is called twice by a parent policy.
fn round_robin_index_hash(index: usize) -> u64 {
    let mut hasher = DefaultHasher::new();
    hasher.write_usize(index);

    hasher.finish()
}

// Does safe modulo and additionally hashes the index
fn compute_rotation(round_robin_index: usize, sequence_length: usize) -> usize {
    if sequence_length > 1 {
        let hash = round_robin_index_hash(round_robin_index);

        (hash % sequence_length as u64) as usize
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
fn slice_rotated_left<T>(slice: &[T], mid: usize) -> impl Iterator<Item = &T> + Clone {
    let begin = &slice[mid..];
    let end = &slice[..mid];
    begin.iter().chain(end.iter())
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::transport::node::TimestampedAverage;
    use crate::transport::topology::Keyspace;
    use crate::transport::topology::Metadata;
    use crate::transport::topology::Peer;
    use crate::transport::topology::Strategy;
    use std::collections::HashMap;
    use std::net::SocketAddr;

    // Used as child policy for load balancing policy tests
    // Forwards plan passed to it in apply_child_policy() method
    #[derive(Debug)]
    pub struct DumbPolicy {}

    impl LoadBalancingPolicy for DumbPolicy {
        fn plan<'a>(&self, _: &Statement, _: &'a ClusterData) -> Plan<'a> {
            let empty_node_list: Vec<Arc<Node>> = Vec::new();

            Box::new(empty_node_list.into_iter())
        }

        fn name(&self) -> String {
            "".into()
        }
    }

    impl ChildLoadBalancingPolicy for DumbPolicy {
        fn apply_child_policy(
            &self,
            plan: Vec<Arc<Node>>,
        ) -> Box<dyn Iterator<Item = Arc<Node>> + Send + Sync> {
            Box::new(plan.into_iter())
        }
    }

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

    #[test]
    fn test_names() {
        let local_dc = "eu".to_string();
        let policy = TokenAwarePolicy::new(Box::new(DcAwareRoundRobinPolicy::new(local_dc)));

        assert_eq!(
            policy.name(),
            "TokenAwarePolicy{child_policy: DcAwareRoundRobinPolicy}".to_string()
        );
    }

    pub fn id_to_invalid_addr(id: u16) -> SocketAddr {
        SocketAddr::from(([255, 255, 255, 255], id))
    }

    // creates ClusterData with info about 5 nodes living in 2 different datacenters
    // ring field is empty
    pub fn mock_cluster_data_for_round_robin_and_latency_aware_tests() -> ClusterData {
        let peers = [("eu", 1), ("eu", 2), ("eu", 3), ("us", 4), ("us", 5)]
            .iter()
            .map(|(dc, id)| Peer {
                datacenter: Some(dc.to_string()),
                rack: None,
                address: tests::id_to_invalid_addr(*id),
                tokens: Vec::new(),
                untranslated_address: Some(tests::id_to_invalid_addr(*id)),
            })
            .collect::<Vec<_>>();

        let info = Metadata {
            peers,
            keyspaces: HashMap::new(),
        };

        ClusterData::new(info, &Default::default(), &HashMap::new(), &None, None)
    }

    pub const EMPTY_STATEMENT: Statement = Statement {
        token: None,
        keyspace: None,
    };

    pub fn get_plan_and_collect_node_identifiers<L: LoadBalancingPolicy>(
        policy: &L,
        statement: &Statement,
        cluster: &ClusterData,
    ) -> Vec<u16> {
        let plan = policy.plan(statement, cluster);
        plan.map(|node| node.address.port()).collect::<Vec<_>>()
    }

    pub fn set_nodes_latency_stats(
        cluster: &mut ClusterData,
        averages: &[(u16, Option<TimestampedAverage>)],
    ) {
        for (id, average) in averages {
            *cluster
                .known_peers
                .get_mut(&tests::id_to_invalid_addr(*id))
                .unwrap()
                .average_latency
                .write()
                .unwrap() = *average;
        }
    }

    // creates ClusterData with info about 3 nodes living in the same datacenter
    // ring field is populated as follows:
    // ring tokens:            50 100 150 200 250 300 400 500
    // corresponding node ids: 2  1   2   3   1   2   3   1
    pub fn mock_cluster_data_for_token_aware_tests() -> ClusterData {
        let peers = [
            Peer {
                datacenter: Some("eu".into()),
                rack: None,
                address: tests::id_to_invalid_addr(1),
                tokens: vec![
                    Token { value: 100 },
                    Token { value: 250 },
                    Token { value: 500 },
                ],
                untranslated_address: None,
            },
            Peer {
                datacenter: Some("eu".into()),
                rack: None,
                address: tests::id_to_invalid_addr(2),
                tokens: vec![
                    Token { value: 50 },
                    Token { value: 150 },
                    Token { value: 300 },
                ],
                untranslated_address: None,
            },
            Peer {
                datacenter: Some("us".into()),
                rack: None,
                address: tests::id_to_invalid_addr(3),
                tokens: vec![Token { value: 200 }, Token { value: 400 }],
                untranslated_address: None,
            },
        ];

        let keyspaces = [
            (
                "keyspace_with_simple_strategy_replication_factor_2".into(),
                Keyspace {
                    strategy: Strategy::SimpleStrategy {
                        replication_factor: 2,
                    },
                    tables: HashMap::new(),
                    views: HashMap::new(),
                    user_defined_types: HashMap::new(),
                },
            ),
            (
                "keyspace_with_simple_strategy_replication_factor_3".into(),
                Keyspace {
                    strategy: Strategy::SimpleStrategy {
                        replication_factor: 3,
                    },
                    tables: HashMap::new(),
                    views: HashMap::new(),
                    user_defined_types: HashMap::new(),
                },
            ),
        ]
        .iter()
        .cloned()
        .collect();

        let info = Metadata {
            peers: Vec::from(peers),
            keyspaces,
        };

        ClusterData::new(info, &Default::default(), &HashMap::new(), &None, None)
    }
}
