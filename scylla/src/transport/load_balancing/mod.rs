//! Load balancing configurations\
//! `Session` can use any load balancing policy which implements the `LoadBalancingPolicy` trait\
//! See [the book](https://rust-driver.docs.scylladb.com/stable/load-balancing/load-balancing.html) for more information

use super::{cluster::ClusterData, node::Node};
use crate::routing::Token;

use std::sync::Arc;

mod default;
pub use default::DefaultPolicy;

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

pub type Plan<'a> = Box<dyn Iterator<Item = Arc<Node>> + Send + Sync + 'a>;

/// Policy that decides which nodes to contact for each query
pub trait LoadBalancingPolicy: Send + Sync + std::fmt::Debug {
    /// It is used for each query to find which nodes to query first
    fn plan<'a>(&self, statement: &Statement, cluster: &'a ClusterData) -> Plan<'a>;

    /// Returns name of load balancing policy
    fn name(&self) -> String;
}
