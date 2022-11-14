//! Load balancing configurations\
//! `Session` can use any load balancing policy which implements the `LoadBalancingPolicy` trait\
//! Policies which implement the `ChildLoadBalancingPolicy` can be wrapped in some other policies\
//! See [the book](https://rust-driver.docs.scylladb.com/stable/load-balancing/load-balancing.html) for more information

use super::{cluster::ClusterData, node::Node};
use crate::routing::Token;

use std::{collections::hash_map::DefaultHasher, hash::Hasher, sync::Arc};

mod dc_aware_round_robin;
mod default;
mod round_robin;
mod token_aware;

pub use dc_aware_round_robin::DcAwareRoundRobinPolicy;
pub use default::DefaultPolicy;
pub use round_robin::RoundRobinPolicy;
pub use token_aware::TokenAwarePolicy;

/// Represents info about statement that can be used by load balancing policies.
#[derive(Default)]
pub struct Statement<'a> {
    pub token: Option<Token>,
    pub keyspace: Option<&'a str>,

    /// If, while preparing, we received from the cluster information that the statement is an LWT,
    /// then we can use this information for routing optimisation. Namely, an optimisation
    /// can be performed: the query should be routed to the replicas in a predefined order
    /// (i. e. always try first to contact replica A, then B if it fails, then C, etc.).
    /// If false, the query should be routed normally.
    /// Note: this a Scylla-specific optimisation. Therefore, the flag will be always false for Cassandra.
    pub is_confirmed_lwt: bool,
}

impl<'a> Statement<'a> {
    fn empty() -> Self {
        Self {
            token: None,
            keyspace: None,
            is_confirmed_lwt: false,
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
