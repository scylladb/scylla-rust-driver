//! Load balancing configurations\
//! `Session` can use any load balancing policy which implements the `LoadBalancingPolicy` trait\
//! Policies which implement the `ChildLoadBalancingPolicy` can be wrapped in some other policies\
//! See [the book](https://rust-driver.docs.scylladb.com/stable/load-balancing/load-balancing.html) for more information

use super::{
    cluster::{ClusterData, EventConsumer},
    node::Node,
};
use crate::routing::Token;

use std::sync::Arc;

mod default;

pub use crate::frame::response::event::Event;
pub use default::DefaultPolicy;

/// Represents info about a query that can be used by load balancing policies.
#[derive(Default, Debug)]
pub struct QueryInfo<'a> {
    pub token: Option<Token>,
    pub keyspace: Option<&'a str>,
}

pub type Plan<'a> = Box<dyn Iterator<Item = Arc<Node>> + Send + Sync + 'a>;

/// Policy that decides which nodes to contact for each query
pub trait LoadBalancingPolicy: EventConsumer {
    /// It is used for each query to find which nodes to query first
    fn plan<'a>(&self, info: &QueryInfo, cluster: &'a ClusterData) -> Plan<'a>;
}
