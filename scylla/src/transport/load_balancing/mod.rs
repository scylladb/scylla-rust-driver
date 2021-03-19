use super::{cluster::ClusterData, node::Node};
use crate::routing::Token;

use std::sync::Arc;

mod dc_aware_round_robin;
mod round_robin;
mod token_aware;

pub use dc_aware_round_robin::DCAwareRoundRobinPolicy;
pub use round_robin::RoundRobinPolicy;
pub use token_aware::TokenAwarePolicy;

/// Represents info about statement that can be used by load balancing policies.
pub struct Statement<'a> {
    pub token: Option<Token>,
    pub keyspace: Option<&'a str>,
}

/// Policy that decides which nodes to contact for each query
pub trait LoadBalancingPolicy: Send + Sync {
    /// It is used for each query to find which nodes to query first
    fn plan<'a>(
        &self,
        statement: &Statement,
        cluster: &'a ClusterData,
    ) -> Box<dyn Iterator<Item = Arc<Node>> + Send + Sync + 'a>;

    /// Returns name of load balancing policy
    fn name(&self) -> String;
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

impl Default for Statement<'_> {
    fn default() -> Self {
        Statement {
            token: None,
            keyspace: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::transport::topology::Peer;
    use crate::transport::topology::TopologyInfo;
    use std::collections::HashMap;
    use std::net::SocketAddr;

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
        let policy = TokenAwarePolicy::new(Box::new(DCAwareRoundRobinPolicy::new(local_dc)));

        assert_eq!(
            policy.name(),
            "TokenAwarePolicy{child_policy: DCAwareRoundRobinPolicy}".to_string()
        );
    }

    pub fn id_to_invalid_addr(id: u16) -> SocketAddr {
        SocketAddr::from(([255, 255, 255, 255], id))
    }

    // creates ClusterData with info about 5 nodes living in 2 different datacenters
    // ring field is empty
    pub fn mock_cluster_data_for_round_robin_tests() -> ClusterData {
        let peers = [("eu", 1), ("eu", 2), ("eu", 3), ("us", 4), ("us", 5)]
            .iter()
            .map(|(dc, id)| Peer {
                datacenter: Some(dc.to_string()),
                rack: None,
                address: tests::id_to_invalid_addr(*id),
                tokens: Vec::new(),
            })
            .collect::<Vec<_>>();

        let info = TopologyInfo {
            peers,
            keyspaces: HashMap::new(),
        };

        ClusterData::new(info, &Default::default(), &HashMap::new(), &None)
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
        let plan = policy.plan(statement, &cluster);
        plan.map(|node| node.address.port()).collect::<Vec<_>>()
    }
}
