use super::{cluster::ClusterData, node::Node};
use crate::routing::Token;

use core::ops::Bound::{Included, Unbounded};
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
        vec.rotate_right(compute_rotation(index, vec.len()));

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
                let mut owner = cluster
                    .ring
                    .range((Included(token), Unbounded))
                    .map(|(_token, node)| node.clone())
                    .take(1)
                    .chain(cluster.ring.values().cloned())
                    .take(1);

                self.child_policy.apply_child_policy(&mut owner)
            }
            // fallback to child policy
            None => Box::new(self.child_policy.plan(statement, cluster)),
        }
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

    fn is_local_node(node: &Node, local_dc: &String) -> bool {
        match &node.datacenter {
            Some(dc) => (dc == local_dc),
            None => false,
        }
    }

    fn retrieve_local_nodes<'a>(&self, cluster: &'a ClusterData) -> &'a Vec<Arc<Node>> {
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
}
