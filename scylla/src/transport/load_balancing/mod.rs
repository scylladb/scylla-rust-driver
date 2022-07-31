//! Load balancing configurations\
//! `Session` can use any load balancing policy which implements the `LoadBalancingPolicy` trait\
//! Policies which implement the `ChildLoadBalancingPolicy` can be wrapped in some other policies\
//! See [the book](https://rust-driver.docs.scylladb.com/stable/load-balancing/load-balancing.html) for more information

use super::{cluster::ClusterData, node::Node};
use crate::routing::Token;

use std::sync::Arc;

mod dumb;

pub use dumb::DumbPolicy;

/// Represents info about statement that can be used by load balancing policies.
#[derive(Default)]
pub struct StatementInfo<'a> {
    pub token: Option<Token>,
    pub keyspace: Option<&'a str>,
}

pub type Plan<'a> = Box<dyn Iterator<Item = Arc<Node>> + Send + Sync + 'a>;

/// Policy that decides which nodes to contact for each query
pub trait LoadBalancingPolicy: Send + Sync {
    /// It is used for each query to find which nodes to query first
    fn plan<'a>(&self, statement_info: &StatementInfo, cluster: &'a ClusterData) -> Plan<'a>;

    /// Returns name of load balancing policy
    fn name(&self) -> String;
}
