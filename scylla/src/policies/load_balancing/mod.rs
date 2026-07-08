//! Load balancing configurations\
//! `Session` can use any load balancing policy which implements the `LoadBalancingPolicy` trait\
//! See [the book](https://rust-driver.docs.scylladb.com/stable/load-balancing/load-balancing.html) for more information

use crate::cluster::{ClusterState, NodeRef};
use crate::frame::response::result::TableSpec;
use crate::frame::types;
use crate::routing::NodeLocationPreference;
use crate::{
    errors::RequestAttemptError,
    routing::{Shard, Token},
};

use std::time::Duration;

mod default;
mod plan;
mod single_target;
pub use default::{DefaultPolicy, DefaultPolicyBuilder, LatencyAwarenessBuilder};
pub use plan::Plan;
pub use single_target::{NodeIdentifier, SingleTargetLoadBalancingPolicy};

/// Represents info about statement that can be used by load balancing policies.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct RoutingInfo<'a> {
    /// Consistency level for the request.
    pub consistency: types::Consistency,

    /// Serial consistency level to be used for serial part of the request, if set.
    pub serial_consistency: Option<types::SerialConsistency>,

    /// Information that are the basis of token-aware routing:
    /// - token, keyspace for vnodes-based routing;
    /// - token, keyspace, table for tablets-based routing.
    pub token: Option<Token>,

    /// Keyspace and table that the request is being executed against.
    pub table: Option<&'a TableSpec<'a>>,

    /// If, while preparing, we received from the cluster information that the statement is an LWT,
    /// then we can use this information for routing optimisation. Namely, an optimisation
    /// can be performed: the request should be routed to the replicas in a predefined order
    /// (i. e. always try first to contact replica A, then B if it fails, then C, etc.).
    /// If false, the request should be routed normally.
    /// Note: this a ScyllaDB-specific optimisation. Therefore, the flag will be always false for Cassandra.
    ///
    /// <div class="warning">
    ///
    /// This flag alone is not sufficient to determine whether a request should be
    /// routed as LWT. A statement can also be executed with [`Consistency::Serial`] or
    /// [`Consistency::LocalSerial`] as its consistency level, which makes the server treat it
    /// as a Paxos (LWT) request — this is the only way to execute a `SELECT` as LWT,
    /// since `SELECT` statements never contain an `IF` clause.
    /// Use [`RoutingInfo::should_route_as_lwt()`] to correctly account for both cases.
    ///
    /// </div>
    ///
    /// [`Consistency::Serial`]: types::Consistency::Serial
    /// [`Consistency::LocalSerial`]: types::Consistency::LocalSerial
    pub is_confirmed_lwt: bool,

    /// The session-level node location preference to pass to load balancing policies.
    pub node_location_preference: &'a NodeLocationPreference,
}

impl Default for RoutingInfo<'_> {
    fn default() -> Self {
        Self {
            consistency: types::Consistency::default(),
            serial_consistency: None,
            token: None,
            table: None,
            is_confirmed_lwt: false,
            node_location_preference: &NodeLocationPreference::Any,
        }
    }
}

impl RoutingInfo<'_> {
    /// Returns `true` if the request should be routed using LWT optimisation
    /// (i.e. deterministic replica ordering to reduce Paxos contention).
    ///
    /// This takes into account both the [`is_confirmed_lwt`](RoutingInfo::is_confirmed_lwt) flag
    /// (set by ScyllaDB during prepare for statements containing `IF` clauses) and the
    /// [`consistency`](RoutingInfo::consistency) level — a request executed with
    /// [`Consistency::Serial`](types::Consistency::Serial) or
    /// [`Consistency::LocalSerial`](types::Consistency::LocalSerial) is always treated
    /// as LWT by the server, even if `is_confirmed_lwt` is `false` (e.g. for `SELECT`
    /// statements, which never contain an `IF` clause).
    pub fn should_route_as_lwt(&self) -> bool {
        self.is_confirmed_lwt
            || matches!(
                self.consistency,
                types::Consistency::Serial | types::Consistency::LocalSerial
            )
    }
}

impl<'a> RoutingInfo<'a> {
    /// Creates a new `RoutingInfo` instance with the specified parameters.
    ///
    /// This constructor should be used only by the Python RS Driver to construct
    /// routing metadata, enabling built-in driver components—such as the
    /// default load balancing policy to be exposed to Python.
    #[cfg(all(scylla_unstable, feature = "unstable-python-rs"))]
    pub fn new(
        consistency: types::Consistency,
        serial_consistency: Option<types::SerialConsistency>,
        token: Option<Token>,
        table: Option<&'a TableSpec<'a>>,
        is_confirmed_lwt: bool,
        node_location_preference: &'a NodeLocationPreference,
    ) -> Self {
        Self {
            consistency,
            serial_consistency,
            token,
            table,
            is_confirmed_lwt,
            node_location_preference,
        }
    }
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
/// Note: The chosen shard serves as a hint for the driver. The driver may ignore the chosen
/// shard and fallback to some random shard for the node in two cases:
/// 1. Picked shard is out of bounds (i.e. greater or equal to `nr_shards` specified by the node).
/// 2. The connection pool to the picked shard is empty.
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
