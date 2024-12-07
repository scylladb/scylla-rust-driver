//! Load balancing configurations\
//! `Session` can use any load balancing policy which implements the `LoadBalancingPolicy` trait\
//! See [the book](https://rust-driver.docs.scylladb.com/stable/load-balancing/load-balancing.html) for more information

use crate::cluster::{ClusterState, NodeRef};
use crate::{
    routing::{Shard, Token},
    transport::errors::QueryError,
};
use scylla_cql::frame::{response::result::TableSpec, types};

use std::time::Duration;

mod default;
mod plan;
pub use default::{DefaultPolicy, DefaultPolicyBuilder, LatencyAwarenessBuilder};
pub use plan::Plan;

/// Represents info about statement that can be used by load balancing policies.
#[derive(Default, Clone, Debug)]
pub struct RoutingInfo<'a> {
    /// Requested consistency information allows to route queries to the appropriate
    /// datacenters. E.g. queries with a LOCAL_ONE consistency should be routed to the same
    /// datacenter.
    pub consistency: types::Consistency,
    pub serial_consistency: Option<types::SerialConsistency>,

    /// Information that are the basis of token-aware routing:
    /// - token, keyspace for vnodes-based routing;
    /// - token, keyspace, table for tablets-based routing.
    pub token: Option<Token>,
    pub table: Option<&'a TableSpec<'a>>,

    /// If, while preparing, we received from the cluster information that the statement is an LWT,
    /// then we can use this information for routing optimisation. Namely, an optimisation
    /// can be performed: the query should be routed to the replicas in a predefined order
    /// (i. e. always try first to contact replica A, then B if it fails, then C, etc.).
    /// If false, the query should be routed normally.
    /// Note: this a Scylla-specific optimisation. Therefore, the flag will be always false for Cassandra.
    pub is_confirmed_lwt: bool,
}

/// The fallback list of nodes in the query plan.
///
/// It is computed on-demand, only if querying the most preferred node fails
/// (or when speculative execution is triggered).
pub type FallbackPlan<'a> =
    Box<dyn Iterator<Item = (NodeRef<'a>, Option<Shard>)> + Send + Sync + 'a>;

/// Policy that decides which nodes and shards to contact for each query.
///
/// When a query is prepared to be sent to ScyllaDB/Cassandra, a `LoadBalancingPolicy`
/// implementation constructs a load balancing plan. That plan is a list of
/// targets (target is a node + an optional shard) to which
/// the driver will try to send the query. The first elements of the plan are the targets which are
/// the best to contact (e.g. they might have the lowest latency).
///
/// Most queries are sent on the first try, so the query execution layer rarely needs to know more
/// than one target from plan. To better optimize that case, `LoadBalancingPolicy` has two methods:
/// `pick` and `fallback`. `pick` returns the first target to contact for a given query, `fallback`
/// returns the rest of the load balancing plan.
///
/// `fallback` is called not only if a send to `pick`ed node failed (or when executing
/// speculatively), but also if `pick` returns `None`.
///
/// Usually the driver needs only the first node from load balancing plan (most queries are send
/// successfully, and there is no need to retry).
///
/// This trait is used to produce an iterator of nodes to contact for a given query.
pub trait LoadBalancingPolicy: Send + Sync + std::fmt::Debug {
    /// Returns the first node to contact for a given query.
    fn pick<'a>(
        &'a self,
        query: &'a RoutingInfo,
        cluster: &'a ClusterState,
    ) -> Option<(NodeRef<'a>, Option<Shard>)>;

    /// Returns all contact-appropriate nodes for a given query.
    fn fallback<'a>(
        &'a self,
        query: &'a RoutingInfo,
        cluster: &'a ClusterState,
    ) -> FallbackPlan<'a>;

    /// Invoked each time a query succeeds.
    fn on_query_success(&self, _query: &RoutingInfo, _latency: Duration, _node: NodeRef<'_>) {}

    /// Invoked each time a query fails.
    fn on_query_failure(
        &self,
        _query: &RoutingInfo,
        _latency: Duration,
        _node: NodeRef<'_>,
        _error: &QueryError,
    ) {
    }

    /// Returns the name of load balancing policy.
    fn name(&self) -> String;
}
