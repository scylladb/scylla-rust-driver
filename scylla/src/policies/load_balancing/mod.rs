//! Load balancing configurations\
//! `Session` can use any load balancing policy which implements the `LoadBalancingPolicy` trait\
//! See [the book](https://rust-driver.docs.scylladb.com/stable/load-balancing/load-balancing.html) for more information

use crate::cluster::{ClusterState, NodeRef};
use crate::{
    errors::RequestAttemptError,
    routing::{Shard, Token},
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
    /// Requested consistency information allows to route requests to the appropriate
    /// datacenters. E.g. requests with a LOCAL_ONE consistency should be routed to the same
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
    /// can be performed: the request should be routed to the replicas in a predefined order
    /// (i. e. always try first to contact replica A, then B if it fails, then C, etc.).
    /// If false, the request should be routed normally.
    /// Note: this a Scylla-specific optimisation. Therefore, the flag will be always false for Cassandra.
    pub is_confirmed_lwt: bool,
}

/// The fallback list of nodes in the request plan.
///
/// It is computed on-demand, only if querying the most preferred node fails
/// (or when speculative execution is triggered).
pub type FallbackPlan<'a> =
    Box<dyn Iterator<Item = (NodeRef<'a>, Option<Shard>)> + Send + Sync + 'a>;

/// Policy that decides which nodes and shards to contact for each request.
///
/// When a request is prepared to be sent to ScyllaDB/Cassandra, a `LoadBalancingPolicy`
/// implementation constructs a load balancing plan. That plan is a list of
/// targets (target is a node + an optional shard) to which
/// the driver will try to send the request. The first elements of the plan are the targets which are
/// the best to contact (e.g. they might have the lowest latency).
///
/// Most requests are sent on the first try, so the request execution layer rarely needs to know more
/// than one target from plan. To better optimize that case, `LoadBalancingPolicy` has two methods:
/// `pick` and `fallback`. `pick` returns the first target to contact for a given request, `fallback`
/// returns the rest of the load balancing plan.
///
/// `fallback` is called not only if a send to `pick`ed node failed (or when executing
/// speculatively), but also if `pick` returns `None`.
///
/// Usually the driver needs only the first node from load balancing plan (most requests are send
/// successfully, and there is no need to retry).
///
/// This trait is used to produce an iterator of nodes to contact for a given request.
pub trait LoadBalancingPolicy: Send + Sync + std::fmt::Debug {
    /// Returns the first node to contact for a given request.
    fn pick<'a>(
        &'a self,
        request: &'a RoutingInfo,
        cluster: &'a ClusterState,
    ) -> Option<(NodeRef<'a>, Option<Shard>)>;

    /// Returns all contact-appropriate nodes for a given request.
    fn fallback<'a>(
        &'a self,
        request: &'a RoutingInfo,
        cluster: &'a ClusterState,
    ) -> FallbackPlan<'a>;

    /// Invoked each time a request succeeds.
    fn on_request_success(&self, _request: &RoutingInfo, _latency: Duration, _node: NodeRef<'_>) {}

    /// Invoked each time a request fails.
    fn on_request_failure(
        &self,
        _request: &RoutingInfo,
        _latency: Duration,
        _node: NodeRef<'_>,
        _error: &RequestAttemptError,
    ) {
    }

    /// Returns the name of load balancing policy.
    fn name(&self) -> String;
}
