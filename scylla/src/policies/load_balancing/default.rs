use self::latency_awareness::LatencyAwareness;
pub use self::latency_awareness::LatencyAwarenessBuilder;

use super::{FallbackPlan, LoadBalancingPolicy, NodeRef, RoutingInfo};
use crate::cluster::ClusterState;
use crate::{
    cluster::metadata::Strategy,
    cluster::node::Node,
    errors::RequestAttemptError,
    routing::locator::ReplicaSet,
    routing::{Shard, Token},
};
use itertools::{Either, Itertools};
use rand::{prelude::SliceRandom, rng, Rng};
use rand_pcg::Pcg32;
use scylla_cql::frame::response::result::TableSpec;
use std::hash::{Hash, Hasher};
use std::{fmt, sync::Arc, time::Duration};
use tracing::{debug, warn};
use uuid::Uuid;

#[derive(Clone, Copy)]
enum NodeLocationCriteria<'a> {
    Any,
    Datacenter(&'a str),
    DatacenterAndRack(&'a str, &'a str),
}

impl<'a> NodeLocationCriteria<'a> {
    fn datacenter(&self) -> Option<&'a str> {
        match self {
            Self::Any => None,
            Self::Datacenter(dc) | Self::DatacenterAndRack(dc, _) => Some(dc),
        }
    }
}

#[derive(Debug, Clone)]
enum NodeLocationPreference {
    Any,
    Datacenter(String),
    DatacenterAndRack(String, String),
}

impl NodeLocationPreference {
    fn datacenter(&self) -> Option<&str> {
        match self {
            Self::Any => None,
            Self::Datacenter(dc) | Self::DatacenterAndRack(dc, _) => Some(dc),
        }
    }

    #[expect(unused)]
    fn rack(&self) -> Option<&str> {
        match self {
            Self::Any | Self::Datacenter(_) => None,
            Self::DatacenterAndRack(_, rack) => Some(rack),
        }
    }
}

/// An ordering requirement for replicas.
#[derive(Clone, Copy)]
enum ReplicaOrder {
    /// No requirement. Replicas can be returned in arbitrary order.
    Arbitrary,

    /// A requirement for the order to be deterministic, not only across statement executions
    /// but also across drivers. This is used for LWT optimisation, to avoid Paxos conflicts.
    Deterministic,
}

/// Statement kind, used to enable specific load balancing patterns for certain cases.
///
/// Currently, there is a distinguished case of LWT statements, which should always be routed
/// to replicas in a deterministic order to avoid Paxos conflicts. Other statements
/// are routed to random replicas to balance the load.
#[derive(Clone, Copy)]
enum StatementType {
    /// The statement is a confirmed LWT. It's to be routed specifically.
    Lwt,

    /// The statement is not a confirmed LWT. It's to be routed in a default way.
    NonLwt,
}

/// A result of `pick_replica`.
enum PickedReplica<'a> {
    /// A replica that could be computed cheaply.
    Computed((NodeRef<'a>, Shard)),

    /// A replica that could not be computed cheaply. `pick` should therefore return None
    /// and `fallback` will then return that replica as the first in the iterator.
    ToBeComputedInFallback,
}

/// The default load balancing policy.
///
/// It can be configured to be datacenter-aware, rack-aware and token-aware.
/// Datacenter failover (sending query to a node from a remote datacenter)
/// for queries with non local consistency mode is also supported.
///
/// Latency awareness is available, although **not recommended**:
/// the penalisation mechanism it involves may interact badly with other
/// mechanisms, such as LWT optimisation. Also, the very tactics of penalising
/// nodes for recently measures latencies is believed to not be very stable
/// and beneficial. The number of in-flight requests, for instance, seems
/// to be a better metric showing how (over)loaded a target node/shard is.
/// For now, however, we don't have an implementation of the
/// in-flight-requests-aware policy.
#[expect(clippy::type_complexity)]
pub struct DefaultPolicy {
    /// Preferences regarding node location. One of: rack and DC, DC, or no preference.
    preferences: NodeLocationPreference,

    /// Configures whether the policy takes token into consideration when creating plans.
    /// If this is set to `true` AND token, keyspace and table are available,
    /// then policy prefers replicas and puts them earlier in the query plan.
    is_token_aware: bool,

    /// Whether to permit remote nodes (those not located in the preferred DC) in plans.
    /// If no preferred DC is set, this has no effect.
    permit_dc_failover: bool,

    /// A predicate that a target (node + shard) must satisfy in order to be picked.
    /// This was introduced to make latency awareness cleaner.
    /// - if latency awareness is disabled, then `pick_predicate` is just `Self::is_alive()`;
    /// - if latency awareness is enabled, then it is `Self::is_alive() && latency_predicate()`,
    ///   which checks that the target is not penalised due to high latencies.
    pick_predicate: Box<dyn Fn(NodeRef<'_>, Option<Shard>) -> bool + Send + Sync>,

    /// Additional layer that penalises targets that are too slow compared to others
    /// in terms of latency. It works in the following way:
    /// - for `pick`, it uses `latency_predicate` to filter out penalised nodes,
    ///   so that a penalised node will never be `pick`ed;
    /// - for `fallback`, it wraps the returned iterator, moving all penalised nodes
    ///   to the end, in a stable way.
    ///
    /// Penalisation is done based on collected and updated latencies.
    latency_awareness: Option<LatencyAwareness>,

    /// The policy chooses (in `pick`) and shuffles (in `fallback`) replicas and nodes
    /// based on random number generator. For sake of deterministic testing,
    /// a fixed seed can be used.
    fixed_seed: Option<u64>,
}

impl fmt::Debug for DefaultPolicy {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DefaultPolicy")
            .field("preferences", &self.preferences)
            .field("is_token_aware", &self.is_token_aware)
            .field("permit_dc_failover", &self.permit_dc_failover)
            .field("latency_awareness", &self.latency_awareness)
            .field("fixed_seed", &self.fixed_seed)
            .finish_non_exhaustive()
    }
}

impl LoadBalancingPolicy for DefaultPolicy {
    fn pick<'a>(
        &'a self,
        query: &'a RoutingInfo,
        cluster: &'a ClusterState,
    ) -> Option<(NodeRef<'a>, Option<Shard>)> {
        /* For prepared statements, token-aware logic is available, we know what are the replicas
         * for the statement, so that we can pick one of them. */
        let routing_info = self.routing_info(query, cluster);

        if let Some(ref token_with_strategy) = routing_info.token_with_strategy {
            if self.preferences.datacenter().is_some()
                && !self.permit_dc_failover
                && matches!(
                    token_with_strategy.strategy,
                    Strategy::SimpleStrategy { .. }
                )
            {
                warn!("\
Combining SimpleStrategy with preferred_datacenter set to Some and disabled datacenter failover may lead to empty query plans for some tokens.\
It is better to give up using one of them: either operate in a keyspace with NetworkTopologyStrategy, which explicitly states\
how many replicas there are in each datacenter (you probably want at least 1 to avoid empty plans while preferring that datacenter), \
or refrain from preferring datacenters (which may ban all other datacenters, if datacenter failover happens to be not possible)."
                );
            }
        }

        /* LWT statements need to be routed differently: always to the same replica, to avoid Paxos contention. */
        let statement_type = if query.is_confirmed_lwt {
            StatementType::Lwt
        } else {
            StatementType::NonLwt
        };

        /* Token-aware logic - if routing info is available, we know what are the replicas
         * for the statement. Try to pick one of them. */
        if let (Some(ts), Some(table_spec)) = (&routing_info.token_with_strategy, query.table) {
            if let NodeLocationPreference::DatacenterAndRack(dc, rack) = &self.preferences {
                // Try to pick some alive local rack random replica.
                let local_rack_picked = self.pick_replica(
                    ts,
                    NodeLocationCriteria::DatacenterAndRack(dc, rack),
                    |node, shard| (self.pick_predicate)(node, Some(shard)),
                    cluster,
                    statement_type,
                    table_spec,
                );

                if let Some(picked) = local_rack_picked {
                    return match picked {
                        PickedReplica::Computed((alive_local_rack_replica, shard)) => {
                            Some((alive_local_rack_replica, Some(shard)))
                        }
                        // Let call to fallback() compute the replica, because it requires allocation.
                        PickedReplica::ToBeComputedInFallback => None,
                    };
                }
            }

            if let NodeLocationPreference::DatacenterAndRack(dc, _)
            | NodeLocationPreference::Datacenter(dc) = &self.preferences
            {
                // Try to pick some alive local random replica.
                let picked = self.pick_replica(
                    ts,
                    NodeLocationCriteria::Datacenter(dc),
                    |node, shard| (self.pick_predicate)(node, Some(shard)),
                    cluster,
                    statement_type,
                    table_spec,
                );

                if let Some(picked) = picked {
                    return match picked {
                        PickedReplica::Computed((alive_local_replica, shard)) => {
                            Some((alive_local_replica, Some(shard)))
                        }
                        // Let call to fallback() compute the replica, because it requires allocation.
                        PickedReplica::ToBeComputedInFallback => None,
                    };
                }
            }

            // If preferred datacenter is not specified, or if datacenter failover is possible, loosen restriction about locality.
            if self.preferences.datacenter().is_none() || self.is_datacenter_failover_possible() {
                // Try to pick some alive random replica.
                let picked = self.pick_replica(
                    ts,
                    NodeLocationCriteria::Any,
                    |node, shard| (self.pick_predicate)(node, Some(shard)),
                    cluster,
                    statement_type,
                    table_spec,
                );
                if let Some(picked) = picked {
                    return match picked {
                        PickedReplica::Computed((alive_remote_replica, shard)) => {
                            Some((alive_remote_replica, Some(shard)))
                        }
                        // Let call to fallback() compute the replica, because it requires allocation.
                        PickedReplica::ToBeComputedInFallback => None,
                    };
                }
            }
        };

        /* Token-unaware logic - if routing info is not available (e.g. for unprepared statements),
         * or no replica was suitable for targeting it (e.g. disabled or down), try to choose
         * a random node, not necessarily a replica. */

        /* We start having not alive nodes filtered out. This is done by `pick_predicate`,
         * which always contains `Self::is_alive()`. */

        // Let's start with local nodes, i.e. those in the preferred datacenter.
        // If there was no preferred datacenter specified, all nodes are treated as local.
        let local_nodes = self.preferred_node_set(cluster);

        if let NodeLocationPreference::DatacenterAndRack(dc, rack) = &self.preferences {
            // Try to pick some alive random local rack node.
            let rack_predicate = Self::make_rack_predicate(
                |node| (self.pick_predicate)(node, None),
                NodeLocationCriteria::DatacenterAndRack(dc, rack),
            );
            let local_rack_node_picked = self.pick_node(local_nodes, rack_predicate);

            if let Some(alive_local_rack_node) = local_rack_node_picked {
                return Some((alive_local_rack_node, None));
            }
        }

        // Try to pick some alive random local node.
        let local_node_picked =
            self.pick_node(local_nodes, |node| (self.pick_predicate)(node, None));
        if let Some(alive_local_node) = local_node_picked {
            return Some((alive_local_node, None));
        }

        let all_nodes = cluster.replica_locator().unique_nodes_in_global_ring();
        // If a datacenter failover is possible, loosen restriction about locality.
        if self.is_datacenter_failover_possible() {
            let maybe_remote_node_picked =
                self.pick_node(all_nodes, |node| (self.pick_predicate)(node, None));
            if let Some(alive_maybe_remote_node) = maybe_remote_node_picked {
                return Some((alive_maybe_remote_node, None));
            }
        }

        /* As we are here, we failed to pick any alive node. Now let's consider even down nodes. */

        // Previous checks imply that every node we could have selected is down.
        // Let's try to return a down node that wasn't disabled.
        let maybe_down_local_node_picked = self.pick_node(local_nodes, |node| node.is_enabled());
        if let Some(down_but_enabled_local_node) = maybe_down_local_node_picked {
            return Some((down_but_enabled_local_node, None));
        }

        // If a datacenter failover is possible, loosen restriction about locality.
        if self.is_datacenter_failover_possible() {
            let maybe_down_maybe_remote_node_picked =
                self.pick_node(all_nodes, |node| node.is_enabled());
            if let Some(down_but_enabled_maybe_remote_node) = maybe_down_maybe_remote_node_picked {
                return Some((down_but_enabled_maybe_remote_node, None));
            }
        }

        // Every node is disabled. This could be due to a bad host filter - configuration error.
        // It makes no sense to return disabled nodes (there are no open connections to them anyway),
        // so let's return None. `fallback()` will return empty iterator, and so the whole plan
        // will be empty.
        None
    }

    fn fallback<'a>(
        &'a self,
        query: &'a RoutingInfo,
        cluster: &'a ClusterState,
    ) -> FallbackPlan<'a> {
        /* For prepared statements, token-aware logic is available, we know what are the replicas
         * for the statement, so that we can pick one of them. */
        let routing_info = self.routing_info(query, cluster);

        /* LWT statements need to be routed differently: always to the same replica, to avoid Paxos contention. */
        let statement_type = if query.is_confirmed_lwt {
            StatementType::Lwt
        } else {
            StatementType::NonLwt
        };

        /* Token-aware logic - if routing info is available, we know what are the replicas for the statement.
         * Get a list of alive replicas:
         * - shuffled list in case of non-LWTs,
         * - deterministically ordered in case of LWTs. */
        let maybe_replicas = if let (Some(ts), Some(table_spec)) =
            (&routing_info.token_with_strategy, query.table)
        {
            // Iterator over alive local rack replicas (shuffled or deterministically ordered,
            // depending on the statement being LWT or not).
            let maybe_local_rack_replicas =
                if let NodeLocationPreference::DatacenterAndRack(dc, rack) = &self.preferences {
                    let local_rack_replicas = self.maybe_shuffled_replicas(
                        ts,
                        NodeLocationCriteria::DatacenterAndRack(dc, rack),
                        |node, shard| Self::is_alive(node, Some(shard)),
                        cluster,
                        statement_type,
                        table_spec,
                    );
                    Either::Left(local_rack_replicas)
                } else {
                    Either::Right(std::iter::empty())
                };

            // Iterator over alive local datacenter replicas (shuffled or deterministically ordered,
            // depending on the statement being LWT or not).
            let maybe_local_replicas = if let NodeLocationPreference::DatacenterAndRack(dc, _)
            | NodeLocationPreference::Datacenter(dc) =
                &self.preferences
            {
                let local_replicas = self.maybe_shuffled_replicas(
                    ts,
                    NodeLocationCriteria::Datacenter(dc),
                    |node, shard| Self::is_alive(node, Some(shard)),
                    cluster,
                    statement_type,
                    table_spec,
                );
                Either::Left(local_replicas)
            } else {
                Either::Right(std::iter::empty())
            };

            // If no datacenter is preferred, or datacenter failover is possible, loosen restriction about locality.
            let maybe_remote_replicas = if self.preferences.datacenter().is_none()
                || self.is_datacenter_failover_possible()
            {
                // Iterator over alive replicas (shuffled or deterministically ordered,
                // depending on the statement being LWT or not).
                let remote_replicas = self.maybe_shuffled_replicas(
                    ts,
                    NodeLocationCriteria::Any,
                    |node, shard| Self::is_alive(node, Some(shard)),
                    cluster,
                    statement_type,
                    table_spec,
                );
                Either::Left(remote_replicas)
            } else {
                Either::Right(std::iter::empty())
            };

            // Produce an iterator, prioritizing local replicas.
            // If preferred datacenter is not specified, every replica is treated as a remote one.
            Either::Left(
                maybe_local_rack_replicas
                    .chain(maybe_local_replicas)
                    .chain(maybe_remote_replicas)
                    .map(|(node, shard)| (node, Some(shard))),
            )
        } else {
            Either::Right(std::iter::empty::<(NodeRef<'a>, Option<Shard>)>())
        };

        /* Token-unaware logic - if routing info is not available (e.g. for unprepared statements),
         * or no replica is suitable for targeting it (e.g. disabled or down), try targetting nodes
         * that are not necessarily replicas. */

        /* We start having not alive nodes filtered out. */

        // All nodes in the local datacenter (if one is given).
        let local_nodes = self.preferred_node_set(cluster);

        let robinned_local_rack_nodes =
            if let NodeLocationPreference::DatacenterAndRack(dc, rack) = &self.preferences {
                let rack_predicate = Self::make_rack_predicate(
                    |node| Self::is_alive(node, None),
                    NodeLocationCriteria::DatacenterAndRack(dc, rack),
                );
                Either::Left(
                    self.round_robin_nodes(local_nodes, rack_predicate)
                        .map(|node| (node, None)),
                )
            } else {
                Either::Right(std::iter::empty::<(NodeRef<'a>, Option<Shard>)>())
            };

        let robinned_local_nodes = self
            .round_robin_nodes(local_nodes, |node| Self::is_alive(node, None))
            .map(|node| (node, None));

        // All nodes in the cluster.
        let all_nodes = cluster.replica_locator().unique_nodes_in_global_ring();

        // If a datacenter failover is possible, loosen restriction about locality.
        let maybe_remote_nodes = if self.is_datacenter_failover_possible() {
            let robinned_all_nodes =
                self.round_robin_nodes(all_nodes, |node| Self::is_alive(node, None));

            Either::Left(robinned_all_nodes.map(|node| (node, None)))
        } else {
            Either::Right(std::iter::empty::<(NodeRef<'a>, Option<Shard>)>())
        };

        // Even if we consider some enabled nodes to be down, we should try contacting them in the last resort.
        let maybe_down_local_nodes = local_nodes
            .iter()
            .filter(|node| node.is_enabled())
            .map(|node| (node, None));

        // If a datacenter failover is possible, loosen restriction about locality.
        let maybe_down_nodes = if self.is_datacenter_failover_possible() {
            Either::Left(
                all_nodes
                    .iter()
                    .filter(|node| node.is_enabled())
                    .map(|node| (node, None)),
            )
        } else {
            Either::Right(std::iter::empty())
        };

        /// *Plan* should return unique elements. It is not however obvious what it means,
        /// because some nodes in the plan may have shards and some may not.
        ///
        /// This helper structure defines equality of plan elements.
        /// How the comparison works:
        /// - If at least one of elements is shard-less, then compare just nodes.
        /// - If both elements have shards, then compare both nodes and shards.
        ///
        /// Why is it implemented this way?
        /// Driver should not attempt to send a request to the same destination twice.
        /// If a plan element doesn't have shard specified, then a random shard will be
        /// chosen by the driver. If the plan also contains the same node but with
        /// a shard present, and we randomly choose the same shard for the shard-less element,
        /// then we have duplication.
        ///
        /// Example: plan is `[(Node1, Some(1)), (Node1, None)]` - if the driver uses
        /// the second element and randomly chooses shard 1, then we have duplication.
        ///
        /// On the other hand, if a plan has a duplicate node, but with different shards,
        /// then we want to use both elements - so we can't just make the list unique by node,
        /// and so this struct was created.
        struct DefaultPolicyTargetComparator {
            host_id: Uuid,
            shard: Option<Shard>,
        }

        impl PartialEq for DefaultPolicyTargetComparator {
            fn eq(&self, other: &Self) -> bool {
                match (self.shard, other.shard) {
                    (_, None) | (None, _) => self.host_id.eq(&other.host_id),
                    (Some(shard_left), Some(shard_right)) => {
                        self.host_id.eq(&other.host_id) && shard_left.eq(&shard_right)
                    }
                }
            }
        }

        impl Eq for DefaultPolicyTargetComparator {}

        impl Hash for DefaultPolicyTargetComparator {
            fn hash<H: Hasher>(&self, state: &mut H) {
                self.host_id.hash(state);
            }
        }

        // Construct a fallback plan as a composition of:
        // - local rack alive replicas,
        // - local datacenter alive replicas (or all alive replicas is no DC is preferred),
        // - remote alive replicas (if DC failover is enabled),
        // - local rack alive nodes,
        // - local datacenter alive nodes (or all alive nodes is no DC is preferred),
        // - remote alive nodes (if DC failover is enabled),
        // - local datacenter nodes,
        // - remote nodes (if DC failover is enabled).
        let plan = maybe_replicas
            .chain(robinned_local_rack_nodes)
            .chain(robinned_local_nodes)
            .chain(maybe_remote_nodes)
            .chain(maybe_down_local_nodes)
            .chain(maybe_down_nodes)
            .unique_by(|(node, shard)| DefaultPolicyTargetComparator {
                host_id: node.host_id,
                shard: *shard,
            });

        // If latency awareness is enabled, wrap the plan by applying latency penalisation:
        // all penalised nodes are moved behind non-penalised nodes, in a stable fashion.
        if let Some(latency_awareness) = self.latency_awareness.as_ref() {
            Box::new(latency_awareness.wrap(plan))
        } else {
            Box::new(plan)
        }
    }

    fn name(&self) -> String {
        "DefaultPolicy".to_string()
    }

    fn on_request_success(
        &self,
        _routing_info: &RoutingInfo,
        latency: Duration,
        node: NodeRef<'_>,
    ) {
        if let Some(latency_awareness) = self.latency_awareness.as_ref() {
            latency_awareness.report_request(node, latency);
        }
    }

    fn on_request_failure(
        &self,
        _routing_info: &RoutingInfo,
        latency: Duration,
        node: NodeRef<'_>,
        error: &RequestAttemptError,
    ) {
        if let Some(latency_awareness) = self.latency_awareness.as_ref() {
            if LatencyAwareness::reliable_latency_measure(error) {
                latency_awareness.report_request(node, latency);
            }
        }
    }
}

impl DefaultPolicy {
    /// Creates a builder used to customise configuration of a new DefaultPolicy.
    pub fn builder() -> DefaultPolicyBuilder {
        DefaultPolicyBuilder::new()
    }

    /// Returns the given routing info processed based on given cluster state.
    fn routing_info<'a>(
        &'a self,
        query: &'a RoutingInfo,
        cluster: &'a ClusterState,
    ) -> ProcessedRoutingInfo<'a> {
        let mut routing_info = ProcessedRoutingInfo::new(query, cluster);

        if !self.is_token_aware {
            routing_info.token_with_strategy = None;
        }

        routing_info
    }

    /// Returns all nodes in the local datacenter if one is given,
    /// or else all nodes in the cluster.
    fn preferred_node_set<'a>(&'a self, cluster: &'a ClusterState) -> &'a [Arc<Node>] {
        if let Some(preferred_datacenter) = self.preferences.datacenter() {
            if let Some(nodes) = cluster
                .replica_locator()
                .unique_nodes_in_datacenter_ring(preferred_datacenter)
            {
                nodes
            } else {
                tracing::warn!(
                    "Datacenter specified as the preferred one ({}) does not exist!",
                    preferred_datacenter
                );
                // We won't guess any DC, as it could lead to possible violation of dc failover ban.
                &[]
            }
        } else {
            cluster.replica_locator().unique_nodes_in_global_ring()
        }
    }

    /// Returns a full replica set for given datacenter (if given, else for all DCs),
    /// cluster state and table spec.
    fn nonfiltered_replica_set<'a>(
        &'a self,
        ts: &TokenWithStrategy<'a>,
        replica_location: NodeLocationCriteria<'a>,
        cluster: &'a ClusterState,
        table_spec: &TableSpec,
    ) -> ReplicaSet<'a> {
        let datacenter = replica_location.datacenter();

        cluster
            .replica_locator()
            .replicas_for_token(ts.token, ts.strategy, datacenter, table_spec)
    }

    /// Wraps the provided predicate, adding the requirement for rack to match.
    fn make_rack_predicate<'a>(
        predicate: impl Fn(NodeRef<'a>) -> bool + 'a,
        replica_location: NodeLocationCriteria<'a>,
    ) -> impl Fn(NodeRef<'a>) -> bool {
        move |node| match replica_location {
            NodeLocationCriteria::Any | NodeLocationCriteria::Datacenter(_) => predicate(node),
            NodeLocationCriteria::DatacenterAndRack(_, rack) => {
                predicate(node) && node.rack.as_deref() == Some(rack)
            }
        }
    }

    /// Wraps the provided predicate, adding the requirement for rack to match.
    fn make_sharded_rack_predicate<'a>(
        predicate: impl Fn(NodeRef<'a>, Shard) -> bool + 'a,
        replica_location: NodeLocationCriteria<'a>,
    ) -> impl Fn(NodeRef<'a>, Shard) -> bool {
        move |node, shard| match replica_location {
            NodeLocationCriteria::Any | NodeLocationCriteria::Datacenter(_) => {
                predicate(node, shard)
            }
            NodeLocationCriteria::DatacenterAndRack(_, rack) => {
                predicate(node, shard) && node.rack.as_deref() == Some(rack)
            }
        }
    }

    /// Returns iterator over replicas for given token and table spec, filtered
    /// by provided location criteria and predicate.
    /// Respects requested replica order, i.e. if requested, returns replicas ordered
    /// deterministically (i.e. by token ring order or by tablet definition order),
    /// else returns replicas in arbitrary order.
    fn filtered_replicas<'a>(
        &'a self,
        ts: &TokenWithStrategy<'a>,
        replica_location: NodeLocationCriteria<'a>,
        predicate: impl Fn(NodeRef<'a>, Shard) -> bool + 'a,
        cluster: &'a ClusterState,
        order: ReplicaOrder,
        table_spec: &TableSpec,
    ) -> impl Iterator<Item = (NodeRef<'a>, Shard)> {
        let predicate = Self::make_sharded_rack_predicate(predicate, replica_location);

        let replica_iter = match order {
            ReplicaOrder::Arbitrary => Either::Left(
                self.nonfiltered_replica_set(ts, replica_location, cluster, table_spec)
                    .into_iter(),
            ),
            ReplicaOrder::Deterministic => Either::Right(
                self.nonfiltered_replica_set(ts, replica_location, cluster, table_spec)
                    .into_replicas_ordered()
                    .into_iter(),
            ),
        };
        replica_iter.filter(move |(node, shard): &(NodeRef<'a>, Shard)| predicate(node, *shard))
    }

    /// Picks a replica for given token and table spec which meets the provided location criteria
    /// and the predicate.
    /// The replica is chosen randomly over all candidates that meet the criteria
    /// unless the query is LWT; if so, the first replica meeting the criteria is chosen
    /// to avoid Paxos contention.
    fn pick_replica<'a>(
        &'a self,
        ts: &TokenWithStrategy<'a>,
        replica_location: NodeLocationCriteria<'a>,
        predicate: impl Fn(NodeRef<'a>, Shard) -> bool + 'a,
        cluster: &'a ClusterState,
        statement_type: StatementType,
        table_spec: &TableSpec,
    ) -> Option<PickedReplica<'a>> {
        match statement_type {
            StatementType::Lwt => {
                self.pick_first_replica(ts, replica_location, predicate, cluster, table_spec)
            }
            StatementType::NonLwt => self
                .pick_random_replica(ts, replica_location, predicate, cluster, table_spec)
                .map(PickedReplica::Computed),
        }
    }

    /// Picks the first (wrt the deterministic order imposed on the keyspace, see comment below)
    /// replica for given token and table spec which meets the provided location criteria
    /// and the predicate.
    // This is to be used for LWT optimisation: in order to reduce contention
    // caused by Paxos conflicts, we always try to query replicas in the same,
    // deterministic order:
    // - ring order for token ring keyspaces,
    // - tablet definition order for tablet keyspaces.
    //
    // If preferred rack and DC are set, then the first (encountered on the ring) replica
    // that resides in that rack in that DC **and** satisfies the `predicate` is returned.
    //
    // If preferred DC is set, then the first (encountered on the ring) replica
    // that resides in that DC **and** satisfies the `predicate` is returned.
    //
    // If no DC/rack preferences are set, then the only possible replica to be returned
    // (due to expensive computation of the others, and we avoid expensive computation in `pick()`)
    // is the primary replica. If it exists, Some is returned, with either Computed(primary_replica)
    // **iff** it satisfies the predicate or ToBeComputedInFallback otherwise.
    fn pick_first_replica<'a>(
        &'a self,
        ts: &TokenWithStrategy<'a>,
        replica_location: NodeLocationCriteria<'a>,
        predicate: impl Fn(NodeRef<'a>, Shard) -> bool + 'a,
        cluster: &'a ClusterState,
        table_spec: &TableSpec,
    ) -> Option<PickedReplica<'a>> {
        match replica_location {
            NodeLocationCriteria::Any => {
                // ReplicaSet returned by ReplicaLocator for this case:
                // 1) can be precomputed and later used cheaply,
                // 2) returns replicas in the **non-ring order** (this because ReplicaSet chains
                //    ring-ordered replicas sequences from different DCs, thus not preserving
                //    the global ring order).
                // Because of 2), we can't use a precomputed ReplicaSet, but instead we need ReplicasOrdered.
                // As ReplicasOrdered can compute cheaply only the primary global replica
                // (computation of the remaining ones is expensive), in case that the primary replica
                // does not satisfy the `predicate`, ToBeComputedInFallback is returned.
                // All expensive computation is to be done only when `fallback()` is called.
                self.nonfiltered_replica_set(ts, replica_location, cluster, table_spec)
                    .into_replicas_ordered()
                    .into_iter()
                    .next()
                    .map(|(primary_replica, shard)| {
                        if predicate(primary_replica, shard) {
                            PickedReplica::Computed((primary_replica, shard))
                        } else {
                            PickedReplica::ToBeComputedInFallback
                        }
                    })
            }
            NodeLocationCriteria::Datacenter(_) | NodeLocationCriteria::DatacenterAndRack(_, _) => {
                // ReplicaSet returned by ReplicaLocator for this case:
                // 1) can be precomputed and later used cheaply,
                // 2) returns replicas in the ring order (this is not true for the case
                //    when multiple DCs are allowed, because ReplicaSet chains replicas sequences
                //    from different DCs, thus not preserving the global ring order)
                self.filtered_replicas(
                    ts,
                    replica_location,
                    predicate,
                    cluster,
                    ReplicaOrder::Deterministic,
                    table_spec,
                )
                .next()
                .map(PickedReplica::Computed)
            }
        }
    }

    /// Picks a random replica for given token and table spec which meets the provided
    /// location criteria and the predicate.
    fn pick_random_replica<'a>(
        &'a self,
        ts: &TokenWithStrategy<'a>,
        replica_location: NodeLocationCriteria<'a>,
        predicate: impl Fn(NodeRef<'a>, Shard) -> bool + 'a,
        cluster: &'a ClusterState,
        table_spec: &TableSpec,
    ) -> Option<(NodeRef<'a>, Shard)> {
        let predicate = Self::make_sharded_rack_predicate(predicate, replica_location);

        let replica_set = self.nonfiltered_replica_set(ts, replica_location, cluster, table_spec);

        if let Some(fixed) = self.fixed_seed {
            let mut generator = Pcg32::new(fixed, 0);
            replica_set.choose_filtered(&mut generator, |(node, shard)| predicate(node, *shard))
        } else {
            replica_set.choose_filtered(&mut rng(), |(node, shard)| predicate(node, *shard))
        }
    }

    /// Returns iterator over replicas for given token and table spec, filtered
    /// by provided location criteria and predicate.
    /// By default, the replicas are shuffled.
    /// For LWTs, though, the replicas are instead returned in a deterministic order.
    fn maybe_shuffled_replicas<'a>(
        &'a self,
        ts: &TokenWithStrategy<'a>,
        replica_location: NodeLocationCriteria<'a>,
        predicate: impl Fn(NodeRef<'a>, Shard) -> bool + 'a,
        cluster: &'a ClusterState,
        statement_type: StatementType,
        table_spec: &TableSpec,
    ) -> impl Iterator<Item = (NodeRef<'a>, Shard)> {
        let order = match statement_type {
            StatementType::Lwt => ReplicaOrder::Deterministic,
            StatementType::NonLwt => ReplicaOrder::Arbitrary,
        };

        let replicas =
            self.filtered_replicas(ts, replica_location, predicate, cluster, order, table_spec);

        match statement_type {
            // As an LWT optimisation: in order to reduce contention caused by Paxos conflicts,
            // we always try to query replicas in the same order.
            StatementType::Lwt => Either::Left(replicas),
            StatementType::NonLwt => Either::Right(self.shuffle(replicas)),
        }
    }

    /// Returns an iterator over the given slice of nodes, rotated by a random shift.
    fn randomly_rotated_nodes(nodes: &[Arc<Node>]) -> impl Iterator<Item = NodeRef<'_>> {
        // Create a randomly rotated slice view
        let nodes_len = nodes.len();
        if nodes_len > 0 {
            let index = rng().random_range(0..nodes_len); // gen_range() panics when range is empty!
            Either::Left(
                nodes[index..]
                    .iter()
                    .chain(nodes[..index].iter())
                    .take(nodes.len()),
            )
        } else {
            Either::Right(std::iter::empty())
        }
    }

    /// Picks a random node from the slice of nodes. The node must satisfy the given predicate.
    fn pick_node<'a>(
        &'a self,
        nodes: &'a [Arc<Node>],
        predicate: impl Fn(NodeRef<'a>) -> bool,
    ) -> Option<NodeRef<'a>> {
        // Select the first node that matches the predicate
        Self::randomly_rotated_nodes(nodes).find(|&node| predicate(node))
    }

    /// Returns an iterator over the given slice of nodes, rotated by a random shift
    /// and filtered by given predicate.
    fn round_robin_nodes<'a>(
        &'a self,
        nodes: &'a [Arc<Node>],
        predicate: impl Fn(NodeRef<'a>) -> bool,
    ) -> impl Iterator<Item = NodeRef<'a>> {
        Self::randomly_rotated_nodes(nodes).filter(move |node| predicate(node))
    }

    /// Wraps a given iterator by shuffling its contents.
    fn shuffle<'a>(
        &self,
        iter: impl Iterator<Item = (NodeRef<'a>, Shard)>,
    ) -> impl Iterator<Item = (NodeRef<'a>, Shard)> {
        let mut vec: Vec<(NodeRef<'_>, Shard)> = iter.collect();

        if let Some(fixed) = self.fixed_seed {
            let mut generator = Pcg32::new(fixed, 0);
            vec.shuffle(&mut generator);
        } else {
            vec.shuffle(&mut rng());
        }

        vec.into_iter()
    }

    /// Returns true iff the node should be considered to be alive.
    fn is_alive(node: NodeRef, _shard: Option<Shard>) -> bool {
        // For now we ignore the shard.
        // We could theoretically only return true if we have a connection open to given shard, but:
        //  - There is no public API to check that, and I don't want DefaultPolicy to use private APIs.
        //  - Shards returned from policy are only a hint anyway, so it probably makes no sense to throw out the whole host.
        node.is_connected()
    }

    /// Returns true iff the datacenter failover is permitted for the statement being executed.
    fn is_datacenter_failover_possible(&self) -> bool {
        self.preferences.datacenter().is_some() && self.permit_dc_failover
    }
}

impl Default for DefaultPolicy {
    fn default() -> Self {
        Self {
            preferences: NodeLocationPreference::Any,
            is_token_aware: true,
            permit_dc_failover: false,
            pick_predicate: Box::new(Self::is_alive),
            latency_awareness: None,
            fixed_seed: None,
        }
    }
}

/// The intended way to instantiate the DefaultPolicy.
///
/// # Example
/// ```
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// use scylla::policies::load_balancing::DefaultPolicy;
///
/// let default_policy = DefaultPolicy::builder()
///     .prefer_datacenter("dc1".to_string())
///     .token_aware(true)
///     .permit_dc_failover(true)
///     .build();
/// # Ok(())
/// # }
#[derive(Clone, Debug)]
pub struct DefaultPolicyBuilder {
    preferences: NodeLocationPreference,
    is_token_aware: bool,
    permit_dc_failover: bool,
    latency_awareness: Option<LatencyAwarenessBuilder>,
    enable_replica_shuffle: bool,
}

impl DefaultPolicyBuilder {
    /// Creates a builder used to customise configuration of a new DefaultPolicy.
    pub fn new() -> Self {
        Self {
            preferences: NodeLocationPreference::Any,
            is_token_aware: true,
            permit_dc_failover: false,
            latency_awareness: None,
            enable_replica_shuffle: true,
        }
    }

    /// Builds a new DefaultPolicy with the previously set configuration.
    pub fn build(self) -> Arc<dyn LoadBalancingPolicy> {
        let latency_awareness = self.latency_awareness.map(|builder| builder.build());
        let pick_predicate = if let Some(ref latency_awareness) = latency_awareness {
            let latency_predicate = latency_awareness.generate_predicate();
            Box::new(move |node: NodeRef<'_>, shard| {
                DefaultPolicy::is_alive(node, shard) && latency_predicate(node)
            })
                as Box<dyn Fn(NodeRef<'_>, Option<Shard>) -> bool + Send + Sync + 'static>
        } else {
            Box::new(DefaultPolicy::is_alive)
        };

        Arc::new(DefaultPolicy {
            preferences: self.preferences,
            is_token_aware: self.is_token_aware,
            permit_dc_failover: self.permit_dc_failover,
            pick_predicate,
            latency_awareness,
            fixed_seed: (!self.enable_replica_shuffle).then(|| {
                let seed = rand::random();
                debug!("DefaultPolicy: setting fixed seed to {}", seed);
                seed
            }),
        })
    }

    /// Sets the datacenter to be preferred by this policy.
    ///
    /// Allows the load balancing policy to prioritize nodes based on their location.
    /// When a preferred datacenter is set, the policy will treat nodes in that
    /// datacenter as "local" nodes, and nodes in other datacenters as "remote" nodes.
    /// This affects the order in which nodes are returned by the policy when
    /// selecting replicas for read or write operations. If no preferred datacenter
    /// is specified, the policy will treat all nodes as local nodes.
    ///
    /// When datacenter failover is disabled (`permit_dc_failover` is set to false),
    /// the default policy will only include local nodes in load balancing plans.
    /// Remote nodes will be excluded, even if they are alive and available
    /// to serve requests.
    pub fn prefer_datacenter(mut self, datacenter_name: String) -> Self {
        self.preferences = NodeLocationPreference::Datacenter(datacenter_name);
        self
    }

    /// Sets the datacenter and rack to be preferred by this policy.
    ///
    /// Allows the load balancing policy to prioritize nodes based on their location
    /// as well as their availability zones in the preferred datacenter.
    /// When a preferred datacenter is set, the policy will treat nodes in that
    /// datacenter as "local" nodes, and nodes in other datacenters as "remote" nodes.
    /// This affects the order in which nodes are returned by the policy when
    /// selecting replicas for read or write operations. If no preferred datacenter
    /// is specified, the policy will treat all nodes as local nodes.
    ///
    /// When datacenter failover is disabled (`permit_dc_failover` is set to false),
    /// the default policy will only include local nodes in load balancing plans.
    /// Remote nodes will be excluded, even if they are alive and available
    /// to serve requests.
    ///
    /// When a preferred rack is set, the policy will first return replicas in the local rack
    /// in the preferred datacenter, and then the other replicas in the datacenter.
    pub fn prefer_datacenter_and_rack(
        mut self,
        datacenter_name: String,
        rack_name: String,
    ) -> Self {
        self.preferences = NodeLocationPreference::DatacenterAndRack(datacenter_name, rack_name);
        self
    }

    /// Sets whether this policy is token-aware (balances load more consciously) or not.
    ///
    /// Token awareness refers to a mechanism by which the driver is aware
    /// of the token range assigned to each node in the cluster. Tokens
    /// are assigned to nodes to partition the data and distribute it
    /// across the cluster.
    ///
    /// When a user wants to read or write data, the driver can use token awareness
    /// to route the request to the correct node based on the token range of the data
    /// being accessed. This can help to minimize network traffic and improve
    /// performance by ensuring that the data is accessed locally as much as possible.
    ///
    /// In the case of `DefaultPolicy`, token awareness is enabled by default,
    /// meaning that the policy will prefer to return alive local replicas
    /// if the token is available. This means that if the client is requesting data
    /// that falls within the token range of a particular node, the policy will try
    /// to route the request to that node first, assuming it is alive and responsive.
    ///
    /// Token awareness can significantly improve the performance and scalability
    /// of applications built on Scylla. By using token awareness, users can ensure
    /// that data is accessed locally as much as possible, reducing network overhead
    /// and improving throughput.
    pub fn token_aware(mut self, is_token_aware: bool) -> Self {
        self.is_token_aware = is_token_aware;
        self
    }

    /// Sets whether this policy permits datacenter failover, i.e. ever attempts
    /// to send requests to nodes from a non-preferred datacenter.
    ///
    /// In the event of a datacenter outage or network failure, the nodes
    /// in that datacenter may become unavailable, and clients may no longer
    /// be able to access data stored on those nodes. To address this,
    /// the `DefaultPolicy` supports datacenter failover, which allows routing
    /// requests to nodes in other datacenters if the local nodes are unavailable.
    ///
    /// Datacenter failover can be enabled in `DefaultPolicy` setting this flag.
    /// When it is set, the policy will prefer to return alive remote replicas
    /// if datacenter failover is permitted and possible due to consistency
    /// constraints.
    pub fn permit_dc_failover(mut self, permit: bool) -> Self {
        self.permit_dc_failover = permit;
        self
    }

    /// Latency awareness is a mechanism that penalises nodes whose measured
    /// recent average latency classifies it as falling behind the others.
    ///
    /// Every `update_rate` the global minimum average latency is computed,
    /// and all nodes whose average latency is worse than `exclusion_threshold`
    /// times the global minimum average latency become penalised for
    /// `retry_period`. Penalisation involves putting those nodes at the very end
    /// of the query plan. As it is often not truly beneficial to prefer
    /// faster non-replica than replicas lagging behind the non-replicas,
    /// this mechanism may as well worsen latencies and/or throughput.
    ///
    /// ATTENTION: using latency awareness is NOT recommended, unless prior
    /// benchmarks prove its beneficial impact on the specific workload's
    /// performance. Use with caution.
    pub fn latency_awareness(mut self, latency_awareness_builder: LatencyAwarenessBuilder) -> Self {
        self.latency_awareness = Some(latency_awareness_builder);
        self
    }

    /// Sets whether this policy should shuffle replicas when token-awareness
    /// is enabled. Shuffling can help distribute the load over replicas, but
    /// can reduce the effectiveness of caching on the database side (e.g.
    /// for reads).
    ///
    /// This option is enabled by default. If disabled, replicas will be chosen
    /// in some random order that is chosen when the load balancing policy
    /// is created and will not change over its lifetime.
    pub fn enable_shuffling_replicas(mut self, enable: bool) -> Self {
        self.enable_replica_shuffle = enable;
        self
    }
}

impl Default for DefaultPolicyBuilder {
    fn default() -> Self {
        Self::new()
    }
}

struct ProcessedRoutingInfo<'a> {
    token_with_strategy: Option<TokenWithStrategy<'a>>,
}

impl<'a> ProcessedRoutingInfo<'a> {
    fn new(query: &'a RoutingInfo, cluster: &'a ClusterState) -> ProcessedRoutingInfo<'a> {
        Self {
            token_with_strategy: TokenWithStrategy::new(query, cluster),
        }
    }
}

struct TokenWithStrategy<'a> {
    strategy: &'a Strategy,
    token: Token,
}

impl<'a> TokenWithStrategy<'a> {
    fn new(query: &'a RoutingInfo, cluster: &'a ClusterState) -> Option<TokenWithStrategy<'a>> {
        let token = query.token?;
        let keyspace_name = query.table?.ks_name();
        let keyspace = cluster.get_keyspace(keyspace_name)?;
        let strategy = &keyspace.strategy;
        Some(TokenWithStrategy { strategy, token })
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use scylla_cql::{frame::types::SerialConsistency, Consistency};
    use tracing::info;

    use self::framework::{
        get_plan_and_collect_node_identifiers, mock_cluster_state_for_token_unaware_tests,
        ExpectedGroups, ExpectedGroupsBuilder,
    };
    use crate::policies::host_filter::HostFilter;
    use crate::routing::locator::tablets::TabletsInfo;
    use crate::routing::locator::test::{
        id_to_invalid_addr, mock_metadata_for_token_aware_tests, TABLE_NTS_RF_2, TABLE_NTS_RF_3,
        TABLE_SS_RF_2,
    };
    use crate::{
        cluster::ClusterState,
        policies::load_balancing::{
            default::tests::framework::mock_cluster_state_for_token_aware_tests, Plan, RoutingInfo,
        },
        routing::Token,
        test_utils::setup_tracing,
    };

    use super::{DefaultPolicy, NodeLocationPreference};

    pub(crate) mod framework {
        use crate::routing::locator::test::{
            id_to_invalid_addr, mock_metadata_for_token_aware_tests,
        };
        use std::collections::{HashMap, HashSet};

        use uuid::Uuid;

        use crate::{
            cluster::{
                metadata::{Metadata, Peer},
                ClusterState,
            },
            policies::load_balancing::{LoadBalancingPolicy, Plan, RoutingInfo},
            routing::Token,
            test_utils::setup_tracing,
        };

        use super::TabletsInfo;

        #[derive(Debug)]
        enum ExpectedGroup {
            NonDeterministic(HashSet<u16>),
            Deterministic(HashSet<u16>),
            Ordered(Vec<u16>),
        }

        impl ExpectedGroup {
            fn len(&self) -> usize {
                match self {
                    Self::NonDeterministic(s) => s.len(),
                    Self::Deterministic(s) => s.len(),
                    Self::Ordered(v) => v.len(),
                }
            }
        }

        pub(crate) struct ExpectedGroupsBuilder {
            groups: Vec<ExpectedGroup>,
        }

        impl ExpectedGroupsBuilder {
            pub(crate) fn new() -> Self {
                Self { groups: Vec::new() }
            }
            /// Expects that the next group in the plan will have a set of nodes
            /// that is equal to the provided one. The groups are assumed to be
            /// non deterministic, i.e. the policy is expected to shuffle
            /// the nodes within that group.
            pub(crate) fn group(mut self, group: impl IntoIterator<Item = u16>) -> Self {
                self.groups
                    .push(ExpectedGroup::NonDeterministic(group.into_iter().collect()));
                self
            }
            /// Expects that the next group in the plan will have a set of nodes
            /// that is equal to the provided one, but the order of nodes in
            /// that group must be stable over multiple plans.
            pub(crate) fn deterministic(mut self, group: impl IntoIterator<Item = u16>) -> Self {
                self.groups
                    .push(ExpectedGroup::Deterministic(group.into_iter().collect()));
                self
            }
            /// Expects that the next group in the plan will have a sequence of nodes
            /// that is equal to the provided one, including order.
            pub(crate) fn ordered(mut self, group: impl IntoIterator<Item = u16>) -> Self {
                self.groups
                    .push(ExpectedGroup::Ordered(group.into_iter().collect()));
                self
            }
            pub(crate) fn build(self) -> ExpectedGroups {
                ExpectedGroups {
                    groups: self.groups,
                }
            }
        }

        #[derive(Debug)]
        pub(crate) struct ExpectedGroups {
            groups: Vec<ExpectedGroup>,
        }

        impl ExpectedGroups {
            pub(crate) fn assert_proper_grouping_in_plans(&self, gots: &[Vec<u16>]) {
                // For simplicity, assume that there is at least one plan
                // in `gots`
                assert!(!gots.is_empty());

                // Each plan is assumed to have the same number of groups.
                // For group index `i`, the code below will go over all plans
                // and will collect their `i`-th groups and put them under
                // index `i` in `sets_of_groups`.
                let mut sets_of_groups: Vec<HashSet<Vec<u16>>> =
                    vec![HashSet::new(); self.groups.len()];

                for got in gots {
                    // First, make sure that `got` has the right number of items,
                    // equal to the sum of sizes of all expected groups
                    let combined_groups_len: usize = self.groups.iter().map(|s| s.len()).sum();
                    assert_eq!(
                        got.len(),
                        combined_groups_len,
                        "Plan length different than expected. Got plan {:?}, expected groups {:?}",
                        got,
                        self.groups,
                    );

                    // Now, split `got` into groups of expected sizes
                    // and just `assert_eq` them
                    let mut got = got.iter();
                    for (group_id, expected) in self.groups.iter().enumerate() {
                        // Collect the nodes that constitute the group
                        // in the actual plan
                        let got_group: Vec<_> = (&mut got).take(expected.len()).copied().collect();

                        match expected {
                            ExpectedGroup::NonDeterministic(expected_set)
                            | ExpectedGroup::Deterministic(expected_set) => {
                                // Verify that the group has the same nodes as the
                                // expected one
                                let got_set: HashSet<_> = got_group.iter().copied().collect();
                                assert_eq!(&got_set, expected_set, "Unordered group mismatch");
                            }
                            ExpectedGroup::Ordered(sequence) => {
                                assert_eq!(&got_group, sequence, "Ordered group mismatch");
                            }
                        }

                        // Put the group into sets_of_groups
                        sets_of_groups[group_id].insert(got_group);
                    }
                }

                // Verify that the groups are either deterministic
                // or non-deterministic
                for (sets, expected) in sets_of_groups.iter().zip(self.groups.iter()) {
                    match expected {
                        ExpectedGroup::NonDeterministic(s) => {
                            // The group is supposed to have non-deterministic
                            // ordering. If the group size is larger than one,
                            // then expect there to be more than one group
                            // in the set.
                            if gots.len() > 1 && s.len() > 1 {
                                assert!(
                                    sets.len() > 1,
                                    "Group {expected:?} is expected to be nondeterministic, but it appears to be deterministic"
                                );
                            }
                        }
                        ExpectedGroup::Deterministic(_) | ExpectedGroup::Ordered(_) => {
                            // The group is supposed to be deterministic,
                            // i.e. a given instance of the default policy
                            // must always return the nodes within it using
                            // the same order.
                            // There will only be one, unique ordering shared
                            // by all plans - check this
                            assert_eq!(
                                sets.len(),
                                1,
                                "Group {expected:?} is expected to be deterministic, but it appears to be nondeterministic"
                            );
                        }
                    }
                }
            }
        }

        #[test]
        fn test_assert_proper_grouping_in_plan_good() {
            setup_tracing();
            let got = vec![1u16, 2, 3, 4, 5];
            let expected_groups = ExpectedGroupsBuilder::new()
                .group([1])
                .group([3, 2, 4])
                .group([5])
                .build();

            expected_groups.assert_proper_grouping_in_plans(&[got]);
        }

        #[test]
        #[should_panic]
        fn test_assert_proper_grouping_in_plan_too_many_nodes_in_the_end() {
            setup_tracing();
            let got = vec![1u16, 2, 3, 4, 5, 6];
            let expected_groups = ExpectedGroupsBuilder::new()
                .group([1])
                .group([3, 2, 4])
                .group([5])
                .build();

            expected_groups.assert_proper_grouping_in_plans(&[got]);
        }

        #[test]
        #[should_panic]
        fn test_assert_proper_grouping_in_plan_too_many_nodes_in_the_middle() {
            setup_tracing();
            let got = vec![1u16, 2, 6, 3, 4, 5];
            let expected_groups = ExpectedGroupsBuilder::new()
                .group([1])
                .group([3, 2, 4])
                .group([5])
                .build();

            expected_groups.assert_proper_grouping_in_plans(&[got]);
        }

        #[test]
        #[should_panic]
        fn test_assert_proper_grouping_in_plan_missing_node() {
            setup_tracing();
            let got = vec![1u16, 2, 3, 4];
            let expected_groups = ExpectedGroupsBuilder::new()
                .group([1])
                .group([3, 2, 4])
                .group([5])
                .build();

            expected_groups.assert_proper_grouping_in_plans(&[got]);
        }

        // based on locator mock cluster
        pub(crate) async fn mock_cluster_state_for_token_aware_tests() -> ClusterState {
            let metadata = mock_metadata_for_token_aware_tests();
            let state = ClusterState::new(
                metadata,
                &Default::default(),
                &HashMap::new(),
                &None,
                None,
                TabletsInfo::new(),
                &HashMap::new(),
                #[cfg(feature = "metrics")]
                &Default::default(),
            )
            .await;

            for node in state.get_nodes_info() {
                node.use_enabled_as_connected();
            }

            state
        }

        // creates ClusterState with info about 5 nodes living in 2 different datacenters
        // ring field is minimal, not intended to influence the tests
        pub(crate) async fn mock_cluster_state_for_token_unaware_tests() -> ClusterState {
            let peers = [("eu", 1), ("eu", 2), ("eu", 3), ("us", 4), ("us", 5)]
                .iter()
                .map(|(dc, id)| Peer {
                    datacenter: Some(dc.to_string()),
                    rack: None,
                    address: id_to_invalid_addr(*id),
                    tokens: vec![Token::new(*id as i64 * 100)],
                    host_id: Uuid::new_v4(),
                })
                .collect::<Vec<_>>();

            let info = Metadata {
                peers,
                keyspaces: HashMap::new(),
            };

            let state = ClusterState::new(
                info,
                &Default::default(),
                &HashMap::new(),
                &None,
                None,
                TabletsInfo::new(),
                &HashMap::new(),
                #[cfg(feature = "metrics")]
                &Default::default(),
            )
            .await;

            for node in state.get_nodes_info() {
                node.use_enabled_as_connected();
            }

            state
        }

        pub(crate) fn get_plan_and_collect_node_identifiers(
            policy: &impl LoadBalancingPolicy,
            query_info: &RoutingInfo,
            cluster: &ClusterState,
        ) -> Vec<u16> {
            let plan = Plan::new(policy, query_info, cluster);
            plan.map(|(node, _shard)| node.address.port())
                .collect::<Vec<_>>()
        }
    }

    pub(crate) const EMPTY_ROUTING_INFO: RoutingInfo = RoutingInfo {
        token: None,
        table: None,
        is_confirmed_lwt: false,
        consistency: Consistency::Quorum,
        serial_consistency: Some(SerialConsistency::Serial),
    };

    pub(super) fn test_default_policy_with_given_cluster_and_routing_info(
        policy: &DefaultPolicy,
        cluster: &ClusterState,
        routing_info: &RoutingInfo<'_>,
        expected_groups: &ExpectedGroups,
    ) {
        let mut plans = Vec::new();
        for _ in 0..256 {
            let plan = get_plan_and_collect_node_identifiers(policy, routing_info, cluster);
            plans.push(plan);
        }
        let example_plan = Plan::new(policy, routing_info, cluster);
        info!("Example plan from policy:",);
        for (node, shard) in example_plan {
            info!(
                "Node port: {}, shard: {}, dc: {:?}, rack: {:?}",
                node.address.port(),
                shard,
                node.datacenter,
                node.rack,
            );
        }

        expected_groups.assert_proper_grouping_in_plans(&plans);
    }

    async fn test_given_default_policy_with_token_unaware_statements(
        policy: DefaultPolicy,
        expected_groups: &ExpectedGroups,
    ) {
        let cluster = mock_cluster_state_for_token_unaware_tests().await;

        test_default_policy_with_given_cluster_and_routing_info(
            &policy,
            &cluster,
            &EMPTY_ROUTING_INFO,
            expected_groups,
        );
    }

    #[tokio::test]
    async fn test_default_policy_with_token_unaware_statements() {
        setup_tracing();
        let local_dc = "eu".to_string();
        let policy_with_disabled_dc_failover = DefaultPolicy {
            preferences: NodeLocationPreference::Datacenter(local_dc.clone()),
            permit_dc_failover: false,
            ..Default::default()
        };
        let expected_groups = ExpectedGroupsBuilder::new()
            .group([1, 2, 3]) // pick + fallback local nodes
            .build();
        test_given_default_policy_with_token_unaware_statements(
            policy_with_disabled_dc_failover,
            &expected_groups,
        )
        .await;

        let policy_with_enabled_dc_failover = DefaultPolicy {
            preferences: NodeLocationPreference::Datacenter(local_dc.clone()),
            permit_dc_failover: true,
            ..Default::default()
        };
        let expected_groups = ExpectedGroupsBuilder::new()
            .group([1, 2, 3]) // pick + fallback local nodes
            .group([4, 5]) // fallback remote nodes
            .build();
        test_given_default_policy_with_token_unaware_statements(
            policy_with_enabled_dc_failover,
            &expected_groups,
        )
        .await;
    }

    #[tokio::test]
    async fn test_default_policy_with_token_aware_statements() {
        setup_tracing();

        use crate::routing::locator::test::{A, B, C, D, E, F, G};
        let cluster = mock_cluster_state_for_token_aware_tests().await;

        #[derive(Debug)]
        struct Test<'a> {
            policy: DefaultPolicy,
            routing_info: RoutingInfo<'a>,
            expected_groups: ExpectedGroups,
        }

        let tests = [
            // Keyspace NTS with RF=2 with enabled DC failover
            Test {
                policy: DefaultPolicy {
                    preferences: NodeLocationPreference::Datacenter("eu".to_owned()),
                    is_token_aware: true,
                    permit_dc_failover: true,
                    ..Default::default()
                },
                routing_info: RoutingInfo {
                    token: Some(Token::new(160)),
                    table: Some(TABLE_NTS_RF_2),
                    consistency: Consistency::Two,
                    ..Default::default()
                },
                // going through the ring, we get order: F , A , C , D , G , B , E
                //                                      us  eu  eu  us  eu  eu  us
                //                                      r2  r1  r1  r1  r2  r1  r1
                expected_groups: ExpectedGroupsBuilder::new()
                    .group([A, G]) // pick + fallback local replicas
                    .group([F, D]) // remote replicas
                    .group([C, B]) // local nodes
                    .group([E]) // remote nodes
                    .build(),
            },
            // Keyspace NTS with RF=2 with enabled DC failover, shuffling replicas disabled
            Test {
                policy: DefaultPolicy {
                    preferences: NodeLocationPreference::Datacenter("eu".to_owned()),
                    is_token_aware: true,
                    permit_dc_failover: true,
                    fixed_seed: Some(123),
                    ..Default::default()
                },
                routing_info: RoutingInfo {
                    token: Some(Token::new(160)),
                    table: Some(TABLE_NTS_RF_2),
                    consistency: Consistency::Two,
                    ..Default::default()
                },
                // going through the ring, we get order: F , A , C , D , G , B , E
                //                                      us  eu  eu  us  eu  eu  us
                //                                      r2  r1  r1  r1  r2  r1  r1
                expected_groups: ExpectedGroupsBuilder::new()
                    .deterministic([A, G]) // pick + fallback local replicas
                    .deterministic([F, D]) // remote replicas
                    .group([C, B]) // local nodes
                    .group([E]) // remote nodes
                    .build(),
            },
            // Keyspace NTS with RF=2 with DC with local Consistency. DC failover should still work.
            Test {
                policy: DefaultPolicy {
                    preferences: NodeLocationPreference::Datacenter("eu".to_owned()),
                    is_token_aware: true,
                    permit_dc_failover: true,
                    fixed_seed: Some(123),
                    ..Default::default()
                },
                routing_info: RoutingInfo {
                    token: Some(Token::new(160)),
                    table: Some(TABLE_NTS_RF_2),
                    consistency: Consistency::LocalOne,
                    ..Default::default()
                },
                // going through the ring, we get order: F , A , C , D , G , B , E
                //                                      us  eu  eu  us  eu  eu  us
                //                                      r2  r1  r1  r1  r2  r1  r1
                expected_groups: ExpectedGroupsBuilder::new()
                    .deterministic([A, G]) // pick + fallback local replicas
                    .deterministic([F, D]) // remote replicas
                    .group([C, B]) // local nodes
                    .group([E]) // remote nodes
                    .build(),
            },
            // Keyspace NTS with RF=2 with explicitly disabled DC failover
            Test {
                policy: DefaultPolicy {
                    preferences: NodeLocationPreference::Datacenter("eu".to_owned()),
                    is_token_aware: true,
                    permit_dc_failover: false,
                    ..Default::default()
                },
                routing_info: RoutingInfo {
                    token: Some(Token::new(160)),
                    table: Some(TABLE_NTS_RF_2),
                    consistency: Consistency::One,
                    ..Default::default()
                },
                // going through the ring, we get order: F , A , C , D , G , B , E
                //                                      us  eu  eu  us  eu  eu  us
                //                                      r2  r1  r1  r1  r2  r1  r1
                expected_groups: ExpectedGroupsBuilder::new()
                    .group([A, G]) // pick + fallback local replicas
                    .group([C, B]) // local nodes
                    .build(), // failover is explicitly forbidden
            },
            // Keyspace NTS with RF=3 with enabled DC failover
            Test {
                policy: DefaultPolicy {
                    preferences: NodeLocationPreference::Datacenter("eu".to_owned()),
                    is_token_aware: true,
                    permit_dc_failover: true,
                    ..Default::default()
                },
                routing_info: RoutingInfo {
                    token: Some(Token::new(160)),
                    table: Some(TABLE_NTS_RF_3),
                    consistency: Consistency::Quorum,
                    ..Default::default()
                },
                // going through the ring, we get order: F , A , C , D , G , B , E
                //                                      us  eu  eu  us  eu  eu  us
                //                                      r2  r1  r1  r1  r2  r1  r1
                expected_groups: ExpectedGroupsBuilder::new()
                    .group([A, C, G]) // pick + fallback local replicas
                    .group([F, D, E]) // remote replicas
                    .group([B]) // local nodes
                    .group([]) // remote nodes
                    .build(),
            },
            // Keyspace NTS with RF=3 with enabled DC failover, shuffling replicas disabled
            Test {
                policy: DefaultPolicy {
                    preferences: NodeLocationPreference::Datacenter("eu".to_owned()),
                    is_token_aware: true,
                    permit_dc_failover: true,
                    fixed_seed: Some(123),
                    ..Default::default()
                },
                routing_info: RoutingInfo {
                    token: Some(Token::new(160)),
                    table: Some(TABLE_NTS_RF_3),
                    consistency: Consistency::Quorum,
                    ..Default::default()
                },
                // going through the ring, we get order: F , A , C , D , G , B , E
                //                                      us  eu  eu  us  eu  eu  us
                //                                      r2  r1  r1  r1  r2  r1  r1
                expected_groups: ExpectedGroupsBuilder::new()
                    .deterministic([A, C, G]) // pick + fallback local replicas
                    .deterministic([F, D, E]) // remote replicas
                    .group([B]) // local nodes
                    .group([]) // remote nodes
                    .build(),
            },
            // Keyspace NTS with RF=3 with disabled DC failover
            Test {
                policy: DefaultPolicy {
                    preferences: NodeLocationPreference::Datacenter("eu".to_owned()),
                    is_token_aware: true,
                    permit_dc_failover: false,
                    ..Default::default()
                },
                routing_info: RoutingInfo {
                    token: Some(Token::new(160)),
                    table: Some(TABLE_NTS_RF_3),
                    consistency: Consistency::Quorum,
                    ..Default::default()
                },
                // going through the ring, we get order: F , A , C , D , G , B , E
                //                                      us  eu  eu  us  eu  eu  us
                //                                      r2  r1  r1  r1  r2  r1  r1
                expected_groups: ExpectedGroupsBuilder::new()
                    .group([A, C, G]) // pick + fallback local replicas
                    .group([B]) // local nodes
                    .build(), // failover explicitly forbidden
            },
            // Keyspace SS with RF=2 with enabled DC failover
            Test {
                policy: DefaultPolicy {
                    preferences: NodeLocationPreference::Datacenter("eu".to_owned()),
                    is_token_aware: true,
                    permit_dc_failover: true,
                    ..Default::default()
                },
                routing_info: RoutingInfo {
                    token: Some(Token::new(160)),
                    table: Some(TABLE_SS_RF_2),
                    consistency: Consistency::Two,
                    ..Default::default()
                },
                // going through the ring, we get order: F , A , C , D , G , B , E
                //                                      us  eu  eu  us  eu  eu  us
                //                                      r2  r1  r1  r1  r2  r1  r1
                expected_groups: ExpectedGroupsBuilder::new()
                    .group([A]) // pick + fallback local replicas
                    .group([F]) // remote replicas
                    .group([C, G, B]) // local nodes
                    .group([D, E]) // remote nodes
                    .build(),
            },
            // Keyspace SS with RF=2 with DC failover and local Consistency. DC failover should still work.
            Test {
                policy: DefaultPolicy {
                    preferences: NodeLocationPreference::Datacenter("eu".to_owned()),
                    is_token_aware: true,
                    permit_dc_failover: true,
                    ..Default::default()
                },
                routing_info: RoutingInfo {
                    token: Some(Token::new(160)),
                    table: Some(TABLE_SS_RF_2),
                    consistency: Consistency::LocalOne,
                    ..Default::default()
                },
                // going through the ring, we get order: F , A , C , D , G , B , E
                //                                      us  eu  eu  us  eu  eu  us
                //                                      r2  r1  r1  r1  r2  r1  r1
                // going through the ring, we get order: F , A , C , D , G , B , E
                //                                      us  eu  eu  us  eu  eu  us
                //                                      r2  r1  r1  r1  r2  r1  r1
                expected_groups: ExpectedGroupsBuilder::new()
                    .group([A]) // pick + fallback local replicas
                    .group([F]) // remote replicas
                    .group([C, G, B]) // local nodes
                    .group([D, E]) // remote nodes
                    .build(),
            },
            // No token implies no token awareness
            Test {
                policy: DefaultPolicy {
                    preferences: NodeLocationPreference::Datacenter("eu".to_owned()),
                    is_token_aware: true,
                    permit_dc_failover: true,
                    ..Default::default()
                },
                routing_info: RoutingInfo {
                    token: None, // no token
                    table: Some(TABLE_NTS_RF_3),
                    consistency: Consistency::Quorum,
                    ..Default::default()
                },
                expected_groups: ExpectedGroupsBuilder::new()
                    .group([A, B, C, G]) // local nodes
                    .group([D, E, F]) // remote nodes
                    .build(),
            },
            // No keyspace implies no token awareness
            Test {
                policy: DefaultPolicy {
                    preferences: NodeLocationPreference::Datacenter("eu".to_owned()),
                    is_token_aware: true,
                    permit_dc_failover: true,
                    ..Default::default()
                },
                routing_info: RoutingInfo {
                    token: Some(Token::new(160)),
                    table: None, // no keyspace
                    consistency: Consistency::Quorum,
                    ..Default::default()
                },
                expected_groups: ExpectedGroupsBuilder::new()
                    .group([A, B, C, G]) // local nodes
                    .group([D, E, F]) // remote nodes
                    .build(),
            },
            // Unknown preferred DC, failover permitted
            Test {
                policy: DefaultPolicy {
                    preferences: NodeLocationPreference::Datacenter("au".to_owned()),
                    is_token_aware: true,
                    permit_dc_failover: true,
                    ..Default::default()
                },
                routing_info: RoutingInfo {
                    token: Some(Token::new(160)),
                    table: Some(TABLE_NTS_RF_2),
                    consistency: Consistency::Quorum,
                    ..Default::default()
                },
                // going through the ring, we get order: F , A , C , D , G , B , E
                //                                      us  eu  eu  us  eu  eu  us
                //                                      r2  r1  r1  r1  r2  r1  r1
                expected_groups: ExpectedGroupsBuilder::new()
                    .group([A, D, F, G]) // remote replicas
                    .group([B, C, E]) // remote nodes
                    .build(),
            },
            // Unknown preferred DC, failover forbidden
            Test {
                policy: DefaultPolicy {
                    preferences: NodeLocationPreference::Datacenter("au".to_owned()),
                    is_token_aware: true,
                    permit_dc_failover: false,
                    ..Default::default()
                },
                routing_info: RoutingInfo {
                    token: Some(Token::new(160)),
                    table: Some(TABLE_NTS_RF_2),
                    consistency: Consistency::Quorum,
                    ..Default::default()
                },
                // going through the ring, we get order: F , A , C , D , G , B , E
                //                                      us  eu  eu  us  eu  eu  us
                //                                      r2  r1  r1  r1  r2  r1  r1
                expected_groups: ExpectedGroupsBuilder::new().build(), // empty plan, because all nodes are remote and failover is forbidden
            },
            // No preferred DC, failover permitted
            Test {
                policy: DefaultPolicy {
                    preferences: NodeLocationPreference::Any,
                    is_token_aware: true,
                    permit_dc_failover: true,
                    ..Default::default()
                },
                routing_info: RoutingInfo {
                    token: Some(Token::new(160)),
                    table: Some(TABLE_NTS_RF_2),
                    consistency: Consistency::Quorum,
                    ..Default::default()
                },
                // going through the ring, we get order: F , A , C , D , G , B , E
                //                                      us  eu  eu  us  eu  eu  us
                //                                      r2  r1  r1  r1  r2  r1  r1
                expected_groups: ExpectedGroupsBuilder::new()
                    .group([A, D, F, G]) // remote replicas
                    .group([B, C, E]) // remote nodes
                    .build(),
            },
            // No preferred DC, failover forbidden
            Test {
                policy: DefaultPolicy {
                    preferences: NodeLocationPreference::Any,
                    is_token_aware: true,
                    permit_dc_failover: false,
                    ..Default::default()
                },
                routing_info: RoutingInfo {
                    token: Some(Token::new(160)),
                    table: Some(TABLE_NTS_RF_2),
                    consistency: Consistency::Quorum,
                    ..Default::default()
                },
                // going through the ring, we get order: F , A , C , D , G , B , E
                //                                      us  eu  eu  us  eu  eu  us
                //                                      r2  r1  r1  r1  r2  r1  r1
                expected_groups: ExpectedGroupsBuilder::new()
                    .group([A, D, F, G]) // remote replicas
                    .group([B, C, E]) // remote nodes
                    .build(),
            },
            // Keyspace NTS with RF=3 with enabled DC failover and rack-awareness
            Test {
                policy: DefaultPolicy {
                    preferences: NodeLocationPreference::DatacenterAndRack(
                        "eu".to_owned(),
                        "r1".to_owned(),
                    ),
                    is_token_aware: true,
                    permit_dc_failover: true,
                    ..Default::default()
                },
                routing_info: RoutingInfo {
                    token: Some(Token::new(160)),
                    table: Some(TABLE_NTS_RF_3),
                    consistency: Consistency::One,
                    ..Default::default()
                },
                // going through the ring, we get order: F , A , C , D , G , B , E
                //                                      us  eu  eu  us  eu  eu  us
                //                                      r2  r1  r1  r1  r2  r1  r1
                expected_groups: ExpectedGroupsBuilder::new()
                    .group([A, C]) // pick local rack replicas
                    .group([G]) // local DC replicas
                    .group([F, D, E]) // remote replicas
                    .group([B]) // local nodes
                    .build(),
            },
            // Keyspace SS with RF=2 with enabled rack-awareness, shuffling replicas disabled
            Test {
                policy: DefaultPolicy {
                    preferences: NodeLocationPreference::DatacenterAndRack(
                        "eu".to_owned(),
                        "r1".to_owned(),
                    ),
                    is_token_aware: true,
                    permit_dc_failover: false,
                    fixed_seed: Some(123),
                    ..Default::default()
                },
                routing_info: RoutingInfo {
                    token: Some(Token::new(560)),
                    table: Some(TABLE_SS_RF_2),
                    consistency: Consistency::Two,
                    ..Default::default()
                },
                // going through the ring, we get order: B , C , E , G , A , F , D
                //                                      eu  eu  us  eu  eu  us  us
                //                                      r1  r1  r1  r2  r1  r2  r1
                expected_groups: ExpectedGroupsBuilder::new()
                    .deterministic([B]) // pick local rack replicas
                    .deterministic([C]) // fallback replicas
                    .group([A]) // local rack nodes
                    .group([G]) // local DC nodes
                    .build(),
            },
            // Keyspace SS with RF=2 with enabled rack-awareness and no local-rack replica
            Test {
                policy: DefaultPolicy {
                    preferences: NodeLocationPreference::DatacenterAndRack(
                        "eu".to_owned(),
                        "r2".to_owned(),
                    ),
                    is_token_aware: true,
                    permit_dc_failover: false,
                    ..Default::default()
                },
                routing_info: RoutingInfo {
                    token: Some(Token::new(160)),
                    table: Some(TABLE_SS_RF_2),
                    consistency: Consistency::One,
                    ..Default::default()
                },
                // going through the ring, we get order: F , A , C , D , G , B , E
                //                                      us  eu  eu  us  eu  eu  us
                //                                      r2  r1  r1  r1  r2  r1  r1
                expected_groups: ExpectedGroupsBuilder::new()
                    .group([A]) // pick local DC
                    .group([G]) // local rack nodes
                    .group([C, B]) // local DC nodes
                    .build(),
            },
            // Keyspace NTS with RF=3 with enabled DC failover and rack-awareness, no token provided
            Test {
                policy: DefaultPolicy {
                    preferences: NodeLocationPreference::DatacenterAndRack(
                        "eu".to_owned(),
                        "r1".to_owned(),
                    ),
                    is_token_aware: true,
                    permit_dc_failover: true,
                    ..Default::default()
                },
                routing_info: RoutingInfo {
                    token: None,
                    table: Some(TABLE_NTS_RF_3),
                    consistency: Consistency::One,
                    ..Default::default()
                },
                // going through the ring, we get order: F , A , C , D , G , B , E
                //                                      us  eu  eu  us  eu  eu  us
                //                                      r2  r1  r1  r1  r2  r1  r1
                expected_groups: ExpectedGroupsBuilder::new()
                    .group([A, C, B]) // local rack nodes
                    .group([G]) // local DC nodes
                    .group([F, D, E]) // remote nodes
                    .build(),
            },
        ];

        for test in tests {
            info!("Test: {:?}", test);
            let Test {
                policy,
                routing_info,
                expected_groups,
            } = test;
            test_default_policy_with_given_cluster_and_routing_info(
                &policy,
                &cluster,
                &routing_info,
                &expected_groups,
            );
        }
    }

    #[tokio::test]
    async fn test_default_policy_with_lwt_statements() {
        setup_tracing();
        use crate::routing::locator::test::{A, B, C, D, E, F, G};

        let cluster = mock_cluster_state_for_token_aware_tests().await;
        struct Test<'a> {
            policy: DefaultPolicy,
            routing_info: RoutingInfo<'a>,
            expected_groups: ExpectedGroups,
        }

        let tests = [
            // Keyspace NTS with RF=2 with enabled DC failover
            Test {
                policy: DefaultPolicy {
                    preferences: NodeLocationPreference::Datacenter("eu".to_owned()),
                    is_token_aware: true,
                    permit_dc_failover: true,
                    ..Default::default()
                },
                routing_info: RoutingInfo {
                    token: Some(Token::new(160)),
                    table: Some(TABLE_NTS_RF_2),
                    consistency: Consistency::Two,
                    is_confirmed_lwt: true,
                    ..Default::default()
                },
                // going through the ring, we get order: F , A , C , D , G , B , E
                //                                      us  eu  eu  us  eu  eu  us
                //                                      r2  r1  r1  r1  r2  r1  r1
                expected_groups: ExpectedGroupsBuilder::new()
                    .ordered([A, G]) // pick + fallback local replicas
                    .ordered([F, D]) // remote replicas
                    .group([C, B]) // local nodes
                    .group([E]) // remote nodes
                    .build(),
            },
            // Keyspace NTS with RF=2 with enabled DC failover, shuffling replicas disabled
            Test {
                policy: DefaultPolicy {
                    preferences: NodeLocationPreference::Datacenter("eu".to_owned()),
                    is_token_aware: true,
                    permit_dc_failover: true,
                    fixed_seed: Some(123),
                    ..Default::default()
                },
                routing_info: RoutingInfo {
                    token: Some(Token::new(160)),
                    table: Some(TABLE_NTS_RF_2),
                    consistency: Consistency::Two,
                    is_confirmed_lwt: true,
                    ..Default::default()
                },
                // going through the ring, we get order: F , A , C , D , G , B , E
                //                                      us  eu  eu  us  eu  eu  us
                //                                      r2  r1  r1  r1  r2  r1  r1
                expected_groups: ExpectedGroupsBuilder::new()
                    .ordered([A, G]) // pick + fallback local replicas
                    .ordered([F, D]) // remote replicas
                    .group([C, B]) // local nodes
                    .group([E]) // remote nodes
                    .build(),
            },
            // Keyspace NTS with RF=2 with DC failover and local Consistency. DC failover should still work.
            Test {
                policy: DefaultPolicy {
                    preferences: NodeLocationPreference::Datacenter("eu".to_owned()),
                    is_token_aware: true,
                    permit_dc_failover: true,
                    ..Default::default()
                },
                routing_info: RoutingInfo {
                    token: Some(Token::new(160)),
                    table: Some(TABLE_NTS_RF_2),
                    consistency: Consistency::LocalOne,
                    is_confirmed_lwt: true,
                    ..Default::default()
                },
                // going through the ring, we get order: F , A , C , D , G , B , E
                //                                      us  eu  eu  us  eu  eu  us
                //                                      r2  r1  r1  r1  r2  r1  r1
                expected_groups: ExpectedGroupsBuilder::new()
                    .ordered([A, G]) // pick + fallback local replicas
                    .ordered([F, D]) // remote replicas
                    .group([C, B]) // local nodes
                    .group([E]) // remote nodes
                    .build(),
            },
            // Keyspace NTS with RF=2 with explicitly disabled DC failover
            Test {
                policy: DefaultPolicy {
                    preferences: NodeLocationPreference::Datacenter("eu".to_owned()),
                    is_token_aware: true,
                    permit_dc_failover: false,
                    ..Default::default()
                },
                routing_info: RoutingInfo {
                    token: Some(Token::new(160)),
                    table: Some(TABLE_NTS_RF_2),
                    consistency: Consistency::One,
                    is_confirmed_lwt: true,
                    ..Default::default()
                },
                // going through the ring, we get order: F , A , C , D , G , B , E
                //                                      us  eu  eu  us  eu  eu  us
                //                                      r2  r1  r1  r1  r2  r1  r1
                expected_groups: ExpectedGroupsBuilder::new()
                    .ordered([A, G]) // pick + fallback local replicas
                    .group([C, B]) // local nodes
                    .build(), // failover is explicitly forbidden
            },
            // Keyspace NTS with RF=3 with enabled DC failover
            Test {
                policy: DefaultPolicy {
                    preferences: NodeLocationPreference::Datacenter("eu".to_owned()),
                    is_token_aware: true,
                    permit_dc_failover: true,
                    ..Default::default()
                },
                routing_info: RoutingInfo {
                    token: Some(Token::new(160)),
                    table: Some(TABLE_NTS_RF_3),
                    consistency: Consistency::Quorum,
                    is_confirmed_lwt: true,
                    ..Default::default()
                },
                // going through the ring, we get order: F , A , C , D , G , B , E
                //                                      us  eu  eu  us  eu  eu  us
                //                                      r2  r1  r1  r1  r2  r1  r1
                expected_groups: ExpectedGroupsBuilder::new()
                    .ordered([A, C, G]) // pick + fallback local replicas
                    .ordered([F, D, E]) // remote replicas
                    .group([B]) // local nodes
                    .group([]) // remote nodes
                    .build(),
            },
            // Keyspace NTS with RF=3 with enabled DC failover, shuffling replicas disabled
            Test {
                policy: DefaultPolicy {
                    preferences: NodeLocationPreference::Datacenter("eu".to_owned()),
                    is_token_aware: true,
                    permit_dc_failover: true,
                    fixed_seed: Some(123),
                    ..Default::default()
                },
                routing_info: RoutingInfo {
                    token: Some(Token::new(160)),
                    table: Some(TABLE_NTS_RF_3),
                    consistency: Consistency::Quorum,
                    is_confirmed_lwt: true,
                    ..Default::default()
                },
                // going through the ring, we get order: F , A , C , D , G , B , E
                //                                      us  eu  eu  us  eu  eu  us
                //                                      r2  r1  r1  r1  r2  r1  r1
                expected_groups: ExpectedGroupsBuilder::new()
                    .ordered([A, C, G]) // pick + fallback local replicas
                    .ordered([F, D, E]) // remote replicas
                    .group([B]) // local nodes
                    .group([]) // remote nodes
                    .build(),
            },
            // Keyspace NTS with RF=3 with disabled DC failover
            Test {
                policy: DefaultPolicy {
                    preferences: NodeLocationPreference::Datacenter("eu".to_owned()),
                    is_token_aware: true,
                    permit_dc_failover: false,
                    ..Default::default()
                },
                routing_info: RoutingInfo {
                    token: Some(Token::new(160)),
                    table: Some(TABLE_NTS_RF_3),
                    consistency: Consistency::Quorum,
                    is_confirmed_lwt: true,
                    ..Default::default()
                },
                // going through the ring, we get order: F , A , C , D , G , B , E
                //                                      us  eu  eu  us  eu  eu  us
                //                                      r2  r1  r1  r1  r2  r1  r1
                expected_groups: ExpectedGroupsBuilder::new()
                    .ordered([A, C, G]) // pick + fallback local replicas
                    .group([B]) // local nodes
                    .build(), // failover explicitly forbidden
            },
            // Keyspace SS with RF=2 with enabled DC failover
            Test {
                policy: DefaultPolicy {
                    preferences: NodeLocationPreference::Datacenter("eu".to_owned()),
                    is_token_aware: true,
                    permit_dc_failover: true,
                    ..Default::default()
                },
                routing_info: RoutingInfo {
                    token: Some(Token::new(160)),
                    table: Some(TABLE_SS_RF_2),
                    consistency: Consistency::Two,
                    is_confirmed_lwt: true,
                    ..Default::default()
                },
                // going through the ring, we get order: F , A , C , D , G , B , E
                //                                      us  eu  eu  us  eu  eu  us
                //                                      r2  r1  r1  r1  r2  r1  r1
                expected_groups: ExpectedGroupsBuilder::new()
                    .ordered([A]) // pick + fallback local replicas
                    .ordered([F]) // remote replicas
                    .group([C, G, B]) // local nodes
                    .group([D, E]) // remote nodes
                    .build(),
            },
            // Keyspace SS with RF=2 with DC failover and local Consistency. DC failover should still work.
            Test {
                policy: DefaultPolicy {
                    preferences: NodeLocationPreference::Datacenter("eu".to_owned()),
                    is_token_aware: true,
                    permit_dc_failover: true,
                    ..Default::default()
                },
                routing_info: RoutingInfo {
                    token: Some(Token::new(160)),
                    table: Some(TABLE_SS_RF_2),
                    consistency: Consistency::LocalOne,
                    is_confirmed_lwt: true,
                    ..Default::default()
                },
                // going through the ring, we get order: F , A , C , D , G , B , E
                //                                      us  eu  eu  us  eu  eu  us
                //                                      r2  r1  r1  r1  r2  r1  r1
                expected_groups: ExpectedGroupsBuilder::new()
                    .ordered([A]) // pick + fallback local replicas
                    .ordered([F]) // remote replicas
                    .group([C, G, B]) // local nodes
                    .group([D, E]) // remote nodes
                    .build(),
            },
            // No token implies no token awareness
            Test {
                policy: DefaultPolicy {
                    preferences: NodeLocationPreference::Datacenter("eu".to_owned()),
                    is_token_aware: true,
                    permit_dc_failover: true,
                    ..Default::default()
                },
                routing_info: RoutingInfo {
                    token: None, // no token
                    table: Some(TABLE_NTS_RF_3),
                    consistency: Consistency::Quorum,
                    is_confirmed_lwt: true,
                    ..Default::default()
                },
                expected_groups: ExpectedGroupsBuilder::new()
                    .group([A, B, C, G]) // local nodes
                    .group([D, E, F]) // remote nodes
                    .build(),
            },
            // No keyspace implies no token awareness
            Test {
                policy: DefaultPolicy {
                    preferences: NodeLocationPreference::Datacenter("eu".to_owned()),
                    is_token_aware: true,
                    permit_dc_failover: true,
                    ..Default::default()
                },
                routing_info: RoutingInfo {
                    token: Some(Token::new(160)),
                    table: None, // no keyspace
                    consistency: Consistency::Quorum,
                    is_confirmed_lwt: true,
                    ..Default::default()
                },
                expected_groups: ExpectedGroupsBuilder::new()
                    .group([A, B, C, G]) // local nodes
                    .group([D, E, F]) // remote nodes
                    .build(),
            },
            // Unknown preferred DC, failover permitted
            Test {
                policy: DefaultPolicy {
                    preferences: NodeLocationPreference::Datacenter("au".to_owned()),
                    is_token_aware: true,
                    permit_dc_failover: true,
                    ..Default::default()
                },
                routing_info: RoutingInfo {
                    token: Some(Token::new(160)),
                    table: Some(TABLE_NTS_RF_2),
                    consistency: Consistency::Quorum,
                    is_confirmed_lwt: true,
                    ..Default::default()
                },
                // going through the ring, we get order: F , A , C , D , G , B , E
                //                                      us  eu  eu  us  eu  eu  us
                //                                      r2  r1  r1  r1  r2  r1  r1
                expected_groups: ExpectedGroupsBuilder::new()
                    .ordered([F, A, D, G]) // remote replicas
                    .group([B, C, E]) // remote nodes
                    .build(),
            },
            // Unknown preferred DC, failover forbidden
            Test {
                policy: DefaultPolicy {
                    preferences: NodeLocationPreference::Datacenter("au".to_owned()),
                    is_token_aware: true,
                    permit_dc_failover: false,
                    ..Default::default()
                },
                routing_info: RoutingInfo {
                    token: Some(Token::new(160)),
                    table: Some(TABLE_NTS_RF_2),
                    consistency: Consistency::Quorum,
                    is_confirmed_lwt: true,
                    ..Default::default()
                },
                // going through the ring, we get order: F , A , C , D , G , B , E
                //                                      us  eu  eu  us  eu  eu  us
                //                                      r2  r1  r1  r1  r2  r1  r1
                expected_groups: ExpectedGroupsBuilder::new().build(), // empty plan, because all nodes are remote and failover is forbidden
            },
            // No preferred DC, failover permitted
            Test {
                policy: DefaultPolicy {
                    preferences: NodeLocationPreference::Any,
                    is_token_aware: true,
                    permit_dc_failover: true,
                    ..Default::default()
                },
                routing_info: RoutingInfo {
                    token: Some(Token::new(160)),
                    table: Some(TABLE_NTS_RF_2),
                    consistency: Consistency::Quorum,
                    is_confirmed_lwt: true,
                    ..Default::default()
                },
                // going through the ring, we get order: F , A , C , D , G , B , E
                //                                      us  eu  eu  us  eu  eu  us
                //                                      r2  r1  r1  r1  r2  r1  r1
                expected_groups: ExpectedGroupsBuilder::new()
                    .ordered([F, A, D, G]) // remote replicas
                    .group([B, C, E]) // remote nodes
                    .build(),
            },
            // No preferred DC, failover forbidden
            Test {
                policy: DefaultPolicy {
                    preferences: NodeLocationPreference::Any,
                    is_token_aware: true,
                    permit_dc_failover: false,
                    ..Default::default()
                },
                routing_info: RoutingInfo {
                    token: Some(Token::new(160)),
                    table: Some(TABLE_NTS_RF_2),
                    consistency: Consistency::Quorum,
                    is_confirmed_lwt: true,
                    ..Default::default()
                },
                // going through the ring, we get order: F , A , C , D , G , B , E
                //                                      us  eu  eu  us  eu  eu  us
                //                                      r2  r1  r1  r1  r2  r1  r1
                expected_groups: ExpectedGroupsBuilder::new()
                    .ordered([F, A, D, G]) // remote replicas
                    .group([B, C, E]) // remote nodes
                    .build(),
            },
            // Keyspace NTS with RF=3 with enabled DC failover and rack-awareness
            Test {
                policy: DefaultPolicy {
                    preferences: NodeLocationPreference::DatacenterAndRack(
                        "eu".to_owned(),
                        "r1".to_owned(),
                    ),
                    is_token_aware: true,
                    permit_dc_failover: true,
                    ..Default::default()
                },
                routing_info: RoutingInfo {
                    token: Some(Token::new(160)),
                    table: Some(TABLE_NTS_RF_3),
                    consistency: Consistency::One,
                    is_confirmed_lwt: true,
                    ..Default::default()
                },
                // going through the ring, we get order: F , A , C , D , G , B , E
                //                                      us  eu  eu  us  eu  eu  us
                //                                      r2  r1  r1  r1  r2  r1  r1
                expected_groups: ExpectedGroupsBuilder::new()
                    .ordered([A, C]) // pick local rack replicas
                    .ordered([G]) // local DC replicas
                    .ordered([F, D, E]) // remote replicas
                    .group([B]) // local nodes
                    .build(),
            },
            // Keyspace SS with RF=2 with enabled rack-awareness, shuffling replicas disabled
            Test {
                policy: DefaultPolicy {
                    preferences: NodeLocationPreference::DatacenterAndRack(
                        "eu".to_owned(),
                        "r1".to_owned(),
                    ),
                    is_token_aware: true,
                    permit_dc_failover: false,
                    fixed_seed: Some(123),
                    ..Default::default()
                },
                routing_info: RoutingInfo {
                    token: Some(Token::new(760)),
                    table: Some(TABLE_SS_RF_2),
                    consistency: Consistency::Two,
                    is_confirmed_lwt: true,
                    ..Default::default()
                },
                // going through the ring, we get order: G , B , A , E , F , C , D
                //                                      eu  eu  eu  us  us  eu  us
                //                                      r2  r1  r1  r1  r2  r1  r1
                expected_groups: ExpectedGroupsBuilder::new()
                    .ordered([B]) // pick local rack replicas
                    .ordered([G]) // local DC replicas
                    .group([A, C]) // local nodes
                    .build(),
            },
            // Keyspace SS with RF=2 with enabled rack-awareness and no local-rack replica
            Test {
                policy: DefaultPolicy {
                    preferences: NodeLocationPreference::DatacenterAndRack(
                        "eu".to_owned(),
                        "r2".to_owned(),
                    ),
                    is_token_aware: true,
                    permit_dc_failover: false,
                    ..Default::default()
                },
                routing_info: RoutingInfo {
                    token: Some(Token::new(160)),
                    table: Some(TABLE_SS_RF_2),
                    consistency: Consistency::One,
                    is_confirmed_lwt: true,
                    ..Default::default()
                },
                // going through the ring, we get order: F , A , C , D , G , B , E
                //                                      us  eu  eu  us  eu  eu  us
                //                                      r2  r1  r1  r1  r2  r1  r1
                expected_groups: ExpectedGroupsBuilder::new()
                    .group([A]) // pick local DC
                    .group([C, G, B]) // local nodes
                    .build(),
            },
        ];

        for Test {
            policy,
            routing_info,
            expected_groups,
        } in tests
        {
            test_default_policy_with_given_cluster_and_routing_info(
                &policy,
                &cluster,
                &routing_info,
                &expected_groups,
            );
        }

        let cluster_with_disabled_node_f = ClusterState::new(
            mock_metadata_for_token_aware_tests(),
            &Default::default(),
            &HashMap::new(),
            &None,
            {
                struct FHostFilter;
                impl HostFilter for FHostFilter {
                    fn accept(&self, peer: &crate::cluster::metadata::Peer) -> bool {
                        peer.address != id_to_invalid_addr(F)
                    }
                }

                Some(&FHostFilter)
            },
            TabletsInfo::new(),
            &HashMap::new(),
            #[cfg(feature = "metrics")]
            &Default::default(),
        )
        .await;

        for node in cluster_with_disabled_node_f.get_nodes_info() {
            node.use_enabled_as_connected();
        }

        let tests_with_disabled_node_f = [
            // Keyspace NTS with RF=3 without preferred DC.
            // The primary replica does not satisfy the predicate (being disabled by HostFilter),
            // so pick() should return None and fallback should return A first.
            //
            // This is a regression test after a bug was fixed.
            Test {
                policy: DefaultPolicy {
                    preferences: NodeLocationPreference::Any,
                    is_token_aware: true,
                    permit_dc_failover: true,
                    pick_predicate: Box::new(|node, _shard| node.address != id_to_invalid_addr(F)),
                    ..Default::default()
                },
                routing_info: RoutingInfo {
                    token: Some(Token::new(160)),
                    table: Some(TABLE_NTS_RF_3),
                    consistency: Consistency::One,
                    is_confirmed_lwt: true,
                    ..Default::default()
                },
                // going through the ring, we get order: F , A , C , D , G , B , E
                //                                      us  eu  eu  us  eu  eu  us
                //                                      r2  r1  r1  r1  r2  r1  r1
                expected_groups: ExpectedGroupsBuilder::new()
                    // pick is empty, because the primary replica does not satisfy pick predicate,
                    // and with LWT we cannot compute other replicas for NTS without allocations.
                    .ordered([A, C, D, G, E]) // replicas
                    .group([B]) // nodes
                    .build(),
            },
        ];

        for Test {
            policy,
            routing_info,
            expected_groups,
        } in tests_with_disabled_node_f
        {
            test_default_policy_with_given_cluster_and_routing_info(
                &policy,
                &cluster_with_disabled_node_f,
                &routing_info,
                &expected_groups,
            );
        }
    }
}

mod latency_awareness {
    use futures::{future::RemoteHandle, FutureExt};
    use itertools::Either;
    use tokio::time::{Duration, Instant};
    use tracing::{trace, warn};
    use uuid::Uuid;

    use crate::cluster::node::Node;
    use crate::errors::{DbError, RequestAttemptError};
    use crate::policies::load_balancing::NodeRef;
    use crate::routing::Shard;
    use std::{
        collections::HashMap,
        ops::Deref,
        sync::{
            atomic::{AtomicU64, Ordering},
            Arc, RwLock,
        },
    };

    #[derive(Debug)]
    struct AtomicDuration(AtomicU64);

    impl AtomicDuration {
        fn new() -> Self {
            Self(AtomicU64::new(u64::MAX))
        }

        fn store(&self, duration: Duration) {
            self.0.store(duration.as_micros() as u64, Ordering::Relaxed)
        }

        fn load(&self) -> Option<Duration> {
            let micros = self.0.load(Ordering::Relaxed);
            if micros == u64::MAX {
                None
            } else {
                Some(Duration::from_micros(micros))
            }
        }
    }

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub(super) struct TimestampedAverage {
        pub(super) timestamp: Instant,
        pub(super) average: Duration,
        pub(super) num_measures: usize,
    }

    impl TimestampedAverage {
        pub(crate) fn compute_next(
            previous: Option<Self>,
            last_latency: Duration,
            scale_secs: f64,
        ) -> Option<Self> {
            let now = Instant::now();
            match previous {
                prev if last_latency.is_zero() => prev,
                None => Some(Self {
                    num_measures: 1,
                    average: last_latency,
                    timestamp: now,
                }),
                Some(prev_avg) => Some({
                    let delay = now
                        .saturating_duration_since(prev_avg.timestamp)
                        .as_secs_f64();
                    let scaled_delay = delay / scale_secs;
                    let prev_weight = if scaled_delay <= 0. {
                        1.
                    } else {
                        (scaled_delay + 1.).ln() / scaled_delay
                    };

                    let last_latency_secs = last_latency.as_secs_f64();
                    let prev_avg_secs = prev_avg.average.as_secs_f64();
                    let average = match Duration::try_from_secs_f64(
                        (1. - prev_weight) * last_latency_secs + prev_weight * prev_avg_secs,
                    ) {
                        Ok(ts) => ts,
                        Err(e) => {
                            warn!(
                                "Error while calculating average: {e}. \
                                prev_avg_secs: {prev_avg_secs}, \
                                last_latency_secs: {last_latency_secs}, \
                                prev_weight: {prev_weight}, \
                                scaled_delay: {scaled_delay}, \
                                delay: {delay}, \
                                prev_avg.timestamp: {:?}, \
                                now: {now:?}",
                                prev_avg.timestamp
                            );

                            // Not sure when we could enter this branch,
                            // so I have no idea what would be a sensible value to return here,
                            // this does not seem like a very bad choice.
                            prev_avg.average
                        }
                    };
                    Self {
                        num_measures: prev_avg.num_measures + 1,
                        timestamp: now,
                        average,
                    }
                }),
            }
        }
    }

    /// A latency-aware load balancing policy module, which enables penalising nodes that are too slow.
    #[derive(Debug)]
    pub(super) struct LatencyAwareness {
        pub(super) exclusion_threshold: f64,
        pub(super) retry_period: Duration,
        pub(super) _update_rate: Duration,
        pub(super) minimum_measurements: usize,
        pub(super) scale_secs: f64,

        /// Last minimum average latency that was noted among the nodes. It is updated every
        /// [update_rate](Self::_update_rate).
        last_min_latency: Arc<AtomicDuration>,

        node_avgs: Arc<RwLock<HashMap<Uuid, RwLock<Option<TimestampedAverage>>>>>,

        // This is Some iff there is an associated updater running on a separate Tokio task
        // For some tests, not to rely on timing, this is None. The updater is then tick'ed
        // explicitly from outside this struct.
        _updater_handle: Option<RemoteHandle<()>>,
    }

    impl LatencyAwareness {
        pub(super) fn builder() -> LatencyAwarenessBuilder {
            LatencyAwarenessBuilder::new()
        }

        fn new_for_test(
            exclusion_threshold: f64,
            retry_period: Duration,
            update_rate: Duration,
            minimum_measurements: usize,
            scale: Duration,
        ) -> (Self, MinAvgUpdater) {
            let min_latency = Arc::new(AtomicDuration::new());

            let min_latency_clone = min_latency.clone();
            let node_avgs = Arc::new(RwLock::new(HashMap::new()));
            let node_avgs_clone = node_avgs.clone();

            let updater = MinAvgUpdater {
                node_avgs,
                min_latency,
                minimum_measurements,
            };

            (
                Self {
                    exclusion_threshold,
                    retry_period,
                    _update_rate: update_rate,
                    minimum_measurements,
                    scale_secs: scale.as_secs_f64(),
                    last_min_latency: min_latency_clone,
                    node_avgs: node_avgs_clone,
                    _updater_handle: None,
                },
                updater,
            )
        }

        fn new(
            exclusion_threshold: f64,
            retry_period: Duration,
            update_rate: Duration,
            minimum_measurements: usize,
            scale: Duration,
        ) -> Self {
            let (self_, updater) = Self::new_for_test(
                exclusion_threshold,
                retry_period,
                update_rate,
                minimum_measurements,
                scale,
            );

            let (updater_fut, updater_handle) = async move {
                let mut update_scheduler = tokio::time::interval(update_rate);
                loop {
                    update_scheduler.tick().await;
                    updater.tick().await;
                }
            }
            .remote_handle();
            tokio::task::spawn(updater_fut);

            Self {
                _updater_handle: Some(updater_handle),
                ..self_
            }
        }

        pub(super) fn generate_predicate(&self) -> impl Fn(&Node) -> bool {
            let last_min_latency = self.last_min_latency.clone();
            let node_avgs = self.node_avgs.clone();
            let exclusion_threshold = self.exclusion_threshold;
            let minimum_measurements = self.minimum_measurements;
            let retry_period = self.retry_period;

            move |node| {
                last_min_latency.load().map(|min_avg| match fast_enough(&node_avgs.read().unwrap(), node.host_id, exclusion_threshold, retry_period, minimum_measurements, min_avg) {
                    FastEnough::Yes => true,
                    FastEnough::No { average } => {
                        trace!("Latency awareness: Penalising node {{address={}, datacenter={:?}, rack={:?}}} for being on average at least {} times slower (latency: {}ms) than the fastest ({}ms).",
                                node.address, node.datacenter, node.rack, exclusion_threshold, average.as_millis(), min_avg.as_millis());
                        false
                    }
                }).unwrap_or(true)
            }
        }

        pub(super) fn wrap<'a>(
            &self,
            fallback: impl Iterator<Item = (NodeRef<'a>, Option<Shard>)>,
        ) -> impl Iterator<Item = (NodeRef<'a>, Option<Shard>)> {
            let min_avg_latency = match self.last_min_latency.load() {
                Some(min_avg) => min_avg,
                None => return Either::Left(fallback), // noop, as no latency data has been collected yet
            };

            let average_latencies = self.node_avgs.read().unwrap();
            let targets = fallback;

            let (fast_targets, penalised_targets): (Vec<_>, Vec<_>) = targets.partition(|(node, _shard)|{
                match fast_enough(
                    average_latencies.deref(),
                    node.host_id,
                    self.exclusion_threshold,
                    self.retry_period,
                    self.minimum_measurements,
                    min_avg_latency,
                ) {
                    FastEnough::Yes => true,
                    FastEnough::No { average } => {
                        trace!("Latency awareness: Penalising node {{address={}, datacenter={:?}, rack={:?}}} for being on average at least {} times slower (latency: {}ms) than the fastest ({}ms).",
                                node.address, node.datacenter, node.rack, self.exclusion_threshold, average.as_millis(), min_avg_latency.as_millis());
                        false
                    }
                }
            });

            let skipping_penalised_targets_iterator =
                fast_targets.into_iter().chain(penalised_targets);

            Either::Right(skipping_penalised_targets_iterator)
        }

        pub(super) fn report_request(&self, node: &Node, latency: Duration) {
            let node_avgs_guard = self.node_avgs.read().unwrap();
            if let Some(previous_node_avg) = node_avgs_guard.get(&node.host_id) {
                // The usual path, the node has been already noticed.
                let mut node_avg_guard = previous_node_avg.write().unwrap();
                let previous_node_avg = *node_avg_guard;
                *node_avg_guard =
                    TimestampedAverage::compute_next(previous_node_avg, latency, self.scale_secs);
            } else {
                // We drop the read lock not to deadlock while taking write lock.
                std::mem::drop(node_avgs_guard);
                let mut node_avgs_guard = self.node_avgs.write().unwrap();

                // We have to read this again, as other threads may race with us.
                let previous_node_avg = node_avgs_guard
                    .get(&node.host_id)
                    .and_then(|rwlock| *rwlock.read().unwrap());

                // We most probably need to add the node to the map.
                // (this will be Some only in an unlikely case that another thread raced with us and won)
                node_avgs_guard.insert(
                    node.host_id,
                    RwLock::new(TimestampedAverage::compute_next(
                        previous_node_avg,
                        latency,
                        self.scale_secs,
                    )),
                );
            }
        }

        pub(crate) fn reliable_latency_measure(error: &RequestAttemptError) -> bool {
            match error {
                // "fast" errors, i.e. ones that are returned quickly after the query begins
                RequestAttemptError::CqlRequestSerialization(_)
                | RequestAttemptError::BrokenConnectionError(_)
                | RequestAttemptError::UnableToAllocStreamId
                | RequestAttemptError::DbError(DbError::IsBootstrapping, _)
                | RequestAttemptError::DbError(DbError::Unavailable { .. }, _)
                | RequestAttemptError::DbError(DbError::Unprepared { .. }, _)
                | RequestAttemptError::DbError(DbError::Overloaded, _)
                | RequestAttemptError::DbError(DbError::RateLimitReached { .. }, _)
                | RequestAttemptError::SerializationError(_) => false,

                // "slow" errors, i.e. ones that are returned after considerable time of query being run
                RequestAttemptError::DbError(_, _)
                | RequestAttemptError::CqlResultParseError(_)
                | RequestAttemptError::CqlErrorParseError(_)
                | RequestAttemptError::BodyExtensionsParseError(_)
                | RequestAttemptError::RepreparedIdChanged { .. }
                | RequestAttemptError::RepreparedIdMissingInBatch
                | RequestAttemptError::UnexpectedResponse(_)
                | RequestAttemptError::NonfinishedPagingState => true,
            }
        }
    }

    impl Default for LatencyAwareness {
        fn default() -> Self {
            Self::builder().build()
        }
    }

    /// Updates minimum average latency upon request each request to `tick()`.
    /// The said average is a crucial criterium for penalising "too slow" nodes.
    struct MinAvgUpdater {
        node_avgs: Arc<RwLock<HashMap<Uuid, RwLock<Option<TimestampedAverage>>>>>,
        min_latency: Arc<AtomicDuration>,
        minimum_measurements: usize,
    }

    impl MinAvgUpdater {
        async fn tick(&self) {
            let averages: &HashMap<Uuid, RwLock<Option<TimestampedAverage>>> =
                &self.node_avgs.read().unwrap();
            if averages.is_empty() {
                return; // No nodes queries registered to LAP performed yet.
            }

            let min_avg = averages
                .values()
                .filter_map(|avg| {
                    avg.read().unwrap().and_then(|timestamped_average| {
                        (timestamped_average.num_measures >= self.minimum_measurements)
                            .then_some(timestamped_average.average)
                    })
                })
                .min();
            if let Some(min_avg) = min_avg {
                self.min_latency.store(min_avg);
                trace!(
                    "Latency awareness: updated min average latency to {} ms",
                    min_avg.as_secs_f64() * 1000.
                );
            }
        }
    }

    /// The builder of LatencyAwareness module of DefaultPolicy.
    ///
    /// (For more information about latency awareness, see [DefaultPolicyBuilder::latency_awareness()](super::DefaultPolicyBuilder::latency_awareness)).
    /// It is intended to be created and configured by the user and then
    /// passed to DefaultPolicyBuilder, like this:
    ///
    /// # Example
    /// ```
    /// # fn example() {
    /// use scylla::policies::load_balancing::{
    ///     LatencyAwarenessBuilder, DefaultPolicy
    /// };
    ///
    /// let latency_awareness_builder = LatencyAwarenessBuilder::new()
    ///     .exclusion_threshold(3.)
    ///     .minimum_measurements(200);
    ///
    /// let policy = DefaultPolicy::builder()
    ///     .latency_awareness(latency_awareness_builder)
    ///     .build();
    /// # }
    #[derive(Debug, Clone)]
    pub struct LatencyAwarenessBuilder {
        exclusion_threshold: f64,
        retry_period: Duration,
        update_rate: Duration,
        minimum_measurements: usize,
        scale: Duration,
    }

    impl LatencyAwarenessBuilder {
        /// Creates a builder of LatencyAwareness module of DefaultPolicy.
        pub fn new() -> Self {
            Self {
                exclusion_threshold: 2_f64,
                retry_period: Duration::from_secs(10),
                update_rate: Duration::from_millis(100),
                minimum_measurements: 50,
                scale: Duration::from_millis(100),
            }
        }

        /// Sets minimum measurements for latency awareness (if there have been fewer measurements taken for a node,
        /// the node will not be penalised).
        ///
        /// Penalising nodes is based on an average of their recently measured average latency.
        /// This average is only meaningful if a minimum of measurements have been collected.
        /// This is what this option controls. If fewer than [minimum_measurements](Self::minimum_measurements)
        /// data points have been collected for a given host, the policy will never penalise that host.
        /// Note that the number of collected measurements for a given host is reset if the node
        /// is restarted.
        /// The default for this option is **50**.
        pub fn minimum_measurements(self, minimum_measurements: usize) -> Self {
            Self {
                minimum_measurements,
                ..self
            }
        }

        /// Sets retry period for latency awareness (max time that a node is being penalised).
        ///
        /// The retry period defines how long a node may be penalised by the policy before it is given
        /// a 2nd chance. More precisely, a node is excluded from query plans if both his calculated
        /// average latency is [exclusion_threshold](Self::exclusion_threshold) times slower than
        /// the fastest node average latency (at the time the query plan is computed) **and** his
        /// calculated average latency has been updated since less than [retry_period](Self::retry_period).
        /// Since penalised nodes will likely not see their latency updated, this is basically how long
        /// the policy will exclude a node.
        pub fn retry_period(self, retry_period: Duration) -> Self {
            Self {
                retry_period,
                ..self
            }
        }

        /// Sets exclusion threshold for latency awareness (a threshold for a node to be penalised).
        ///
        /// The exclusion threshold controls how much worse the average latency of a node must be
        /// compared to the fastest performing node for it to be penalised by the policy.
        /// For example, if set to 2, the resulting policy excludes nodes that are more than twice
        /// slower than the fastest node.
        pub fn exclusion_threshold(self, exclusion_threshold: f64) -> Self {
            Self {
                exclusion_threshold,
                ..self
            }
        }

        /// Sets update rate for latency awareness (how often is the global minimal average latency updated).
        ///
        /// The update rate defines how often the minimum average latency is recomputed. While the
        /// average latency score of each node is computed iteratively (updated each time a new latency
        /// is collected), the minimum score needs to be recomputed from scratch every time, which is
        /// slightly more costly. For this reason, the minimum is only re-calculated at the given fixed
        /// rate and cached between re-calculation.
        /// The default update rate if **100 milliseconds**, which should be appropriate for most
        /// applications. In particular, note that while we want to avoid to recompute the minimum for
        /// every query, that computation is not particularly intensive either and there is no reason to
        /// use a very slow rate (more than second is probably unnecessarily slow for instance).
        pub fn update_rate(self, update_rate: Duration) -> Self {
            Self {
                update_rate,
                ..self
            }
        }

        /// Sets the scale to use for the resulting latency aware policy.
        ///
        /// The `scale` provides control on how the weight given to older latencies decreases
        /// over time. For a given host, if a new latency `l` is received at time `t`, and
        /// the previously calculated average is `prev` calculated at time `t'`, then the
        /// newly calculated average `avg` for that host is calculated thusly:
        ///
        /// ```text
        /// d = (t - t') / scale
        /// alpha = 1 - (ln(d+1) / d)
        /// avg = alpha * l + (1 - alpha) * prev
        /// ```
        ///
        /// Typically, with a `scale` of 100 milliseconds (the default), if a new latency is
        /// measured and the previous measure is 10 millisecond old (so `d=0.1`), then `alpha`
        /// will be around `0.05`. In other words, the new latency will weight 5% of the
        /// updated average. A bigger scale will get less weight to new measurements (compared to
        /// previous ones), a smaller one will give them more weight.
        ///
        /// The default scale (if this method is not used) is of **100 milliseconds**. If unsure,
        /// try this default scale first and experiment only if it doesn't provide acceptable results
        /// (hosts are excluded too quickly or not fast enough and tuning the exclusion threshold doesn't
        /// help).
        pub fn scale(self, scale: Duration) -> Self {
            Self { scale, ..self }
        }

        pub(super) fn build(self) -> LatencyAwareness {
            let Self {
                exclusion_threshold,
                retry_period,
                update_rate,
                minimum_measurements,
                scale,
            } = self;
            LatencyAwareness::new(
                exclusion_threshold,
                retry_period,
                update_rate,
                minimum_measurements,
                scale,
            )
        }

        #[cfg(test)]
        fn build_for_test(self) -> (LatencyAwareness, MinAvgUpdater) {
            let Self {
                exclusion_threshold,
                retry_period,
                update_rate,
                minimum_measurements,
                scale,
            } = self;
            LatencyAwareness::new_for_test(
                exclusion_threshold,
                retry_period,
                update_rate,
                minimum_measurements,
                scale,
            )
        }
    }

    impl Default for LatencyAwarenessBuilder {
        fn default() -> Self {
            Self::new()
        }
    }

    pub(super) enum FastEnough {
        Yes,
        No { average: Duration },
    }

    pub(super) fn fast_enough(
        average_latencies: &HashMap<Uuid, RwLock<Option<TimestampedAverage>>>,
        node: Uuid,
        exclusion_threshold: f64,
        retry_period: Duration,
        minimum_measurements: usize,
        min_avg: Duration,
    ) -> FastEnough {
        let avg = match average_latencies
            .get(&node)
            .and_then(|avgs| *avgs.read().unwrap())
        {
            Some(avg) => avg,
            None => return FastEnough::Yes,
        };
        if avg.num_measures >= minimum_measurements
            && avg.timestamp.elapsed() < retry_period
            && avg.average.as_micros() as f64 > exclusion_threshold * min_avg.as_micros() as f64
        {
            FastEnough::No {
                average: avg.average,
            }
        } else {
            FastEnough::Yes
        }
    }

    #[cfg(test)]
    mod tests {
        use scylla_cql::Consistency;

        use super::{
            super::tests::{framework::*, EMPTY_ROUTING_INFO},
            super::DefaultPolicy,
            *,
        };

        use crate::{
            cluster::ClusterState,
            cluster::NodeAddr,
            policies::load_balancing::default::NodeLocationPreference,
            policies::load_balancing::{
                default::tests::test_default_policy_with_given_cluster_and_routing_info,
                RoutingInfo,
            },
            routing::locator::test::{id_to_invalid_addr, A, B, C, D, E, F, G},
            routing::locator::test::{TABLE_INVALID, TABLE_NTS_RF_2, TABLE_NTS_RF_3},
            routing::Shard,
            routing::Token,
            test_utils::setup_tracing,
        };
        use tokio::time::Instant;

        trait DefaultPolicyTestExt {
            fn set_nodes_latency_stats(
                &self,
                cluster: &ClusterState,
                averages: &[(u16, Option<TimestampedAverage>)],
            );
        }

        impl DefaultPolicyTestExt for DefaultPolicy {
            fn set_nodes_latency_stats(
                &self,
                cluster: &ClusterState,
                averages: &[(u16, Option<TimestampedAverage>)],
            ) {
                let addr_to_host_id: HashMap<NodeAddr, Uuid> = cluster
                    .known_peers
                    .values()
                    .map(|node| (node.address, node.host_id))
                    .collect();

                for (id, average) in averages.iter().copied() {
                    let host_id = *addr_to_host_id.get(&id_to_invalid_addr(id)).unwrap();
                    let mut node_latencies = self
                        .latency_awareness
                        .as_ref()
                        .unwrap()
                        .node_avgs
                        .write()
                        .unwrap();
                    let mut node_latency =
                        node_latencies.entry(host_id).or_default().write().unwrap();
                    println!("Set latency: node {id}, latency {average:?}.");
                    *node_latency = average;
                }
                println!("Set node latency stats.")
            }
        }

        fn default_policy_with_given_latency_awareness(
            latency_awareness: LatencyAwareness,
        ) -> DefaultPolicy {
            let pick_predicate = {
                let latency_predicate = latency_awareness.generate_predicate();
                Box::new(move |node: NodeRef<'_>, shard| {
                    DefaultPolicy::is_alive(node, shard) && latency_predicate(node)
                })
                    as Box<dyn Fn(NodeRef<'_>, Option<Shard>) -> bool + Send + Sync + 'static>
            };

            DefaultPolicy {
                preferences: NodeLocationPreference::Datacenter("eu".to_owned()),
                permit_dc_failover: true,
                is_token_aware: true,
                pick_predicate,
                latency_awareness: Some(latency_awareness),
                fixed_seed: None,
            }
        }

        fn latency_aware_default_policy_customised(
            configurer: impl FnOnce(LatencyAwarenessBuilder) -> LatencyAwarenessBuilder,
        ) -> DefaultPolicy {
            let latency_awareness = configurer(LatencyAwareness::builder()).build();
            default_policy_with_given_latency_awareness(latency_awareness)
        }

        fn latency_aware_default_policy() -> DefaultPolicy {
            latency_aware_default_policy_customised(|b| b)
        }

        fn latency_aware_policy_with_explicit_updater_customised(
            configurer: impl FnOnce(LatencyAwarenessBuilder) -> LatencyAwarenessBuilder,
        ) -> (DefaultPolicy, MinAvgUpdater) {
            let (latency_awareness, updater) =
                configurer(LatencyAwareness::builder()).build_for_test();
            (
                default_policy_with_given_latency_awareness(latency_awareness),
                updater,
            )
        }

        fn latency_aware_default_policy_with_explicit_updater() -> (DefaultPolicy, MinAvgUpdater) {
            latency_aware_policy_with_explicit_updater_customised(|b| b)
        }

        #[tokio::test]
        async fn latency_aware_default_policy_does_not_penalise_if_no_latency_info_available_yet() {
            setup_tracing();
            let policy = latency_aware_default_policy();
            let cluster = tests::mock_cluster_state_for_token_unaware_tests().await;

            let expected_groups = ExpectedGroupsBuilder::new()
                .group([1, 2, 3]) // pick + fallback local nodes
                .group([4, 5]) // fallback remote nodes
                .build();

            test_default_policy_with_given_cluster_and_routing_info(
                &policy,
                &cluster,
                &EMPTY_ROUTING_INFO,
                &expected_groups,
            );
        }

        #[tokio::test]
        async fn latency_aware_default_policy_does_not_penalise_if_not_enough_measurements() {
            setup_tracing();
            let policy = latency_aware_default_policy();
            let cluster = tests::mock_cluster_state_for_token_unaware_tests().await;

            let min_avg = Duration::from_millis(10);

            policy.set_nodes_latency_stats(
                &cluster,
                &[
                    (
                        1,
                        Some(TimestampedAverage {
                            timestamp: Instant::now(),
                            average: Duration::from_secs_f64(
                                policy
                                    .latency_awareness
                                    .as_ref()
                                    .unwrap()
                                    .exclusion_threshold
                                    * 1.5
                                    * min_avg.as_secs_f64(),
                            ),
                            num_measures: policy
                                .latency_awareness
                                .as_ref()
                                .unwrap()
                                .minimum_measurements
                                - 1,
                        }),
                    ),
                    (
                        3,
                        Some(TimestampedAverage {
                            timestamp: Instant::now(),
                            average: min_avg,
                            num_measures: policy
                                .latency_awareness
                                .as_ref()
                                .unwrap()
                                .minimum_measurements,
                        }),
                    ),
                ],
            );

            let expected_groups = ExpectedGroupsBuilder::new()
                .group([1, 2, 3]) // pick + fallback local nodes
                .group([4, 5]) // fallback remote nodes
                .build();

            test_default_policy_with_given_cluster_and_routing_info(
                &policy,
                &cluster,
                &EMPTY_ROUTING_INFO,
                &expected_groups,
            );
        }

        #[tokio::test]
        async fn latency_aware_default_policy_does_not_penalise_if_exclusion_threshold_not_crossed()
        {
            setup_tracing();
            let policy = latency_aware_default_policy();
            let cluster = tests::mock_cluster_state_for_token_unaware_tests().await;

            let min_avg = Duration::from_millis(10);

            policy.set_nodes_latency_stats(
                &cluster,
                &[
                    (
                        1,
                        Some(TimestampedAverage {
                            timestamp: Instant::now(),
                            average: Duration::from_secs_f64(
                                policy
                                    .latency_awareness
                                    .as_ref()
                                    .unwrap()
                                    .exclusion_threshold
                                    * 0.95
                                    * min_avg.as_secs_f64(),
                            ),
                            num_measures: policy
                                .latency_awareness
                                .as_ref()
                                .unwrap()
                                .minimum_measurements,
                        }),
                    ),
                    (
                        3,
                        Some(TimestampedAverage {
                            timestamp: Instant::now(),
                            average: min_avg,
                            num_measures: policy
                                .latency_awareness
                                .as_ref()
                                .unwrap()
                                .minimum_measurements,
                        }),
                    ),
                ],
            );

            let expected_groups = ExpectedGroupsBuilder::new()
                .group([1, 2, 3]) // pick + fallback local nodes
                .group([4, 5]) // fallback remote nodes
                .build();

            test_default_policy_with_given_cluster_and_routing_info(
                &policy,
                &cluster,
                &EMPTY_ROUTING_INFO,
                &expected_groups,
            );
        }

        #[tokio::test]
        async fn latency_aware_default_policy_does_not_penalise_if_retry_period_expired() {
            setup_tracing();
            let policy = latency_aware_default_policy_customised(|b| {
                b.retry_period(Duration::from_millis(10))
            });

            let cluster = tests::mock_cluster_state_for_token_unaware_tests().await;

            let min_avg = Duration::from_millis(10);

            policy.set_nodes_latency_stats(
                &cluster,
                &[
                    (
                        1,
                        Some(TimestampedAverage {
                            timestamp: Instant::now(),
                            average: Duration::from_secs_f64(
                                policy
                                    .latency_awareness
                                    .as_ref()
                                    .unwrap()
                                    .exclusion_threshold
                                    * 1.5
                                    * min_avg.as_secs_f64(),
                            ),
                            num_measures: policy
                                .latency_awareness
                                .as_ref()
                                .unwrap()
                                .minimum_measurements,
                        }),
                    ),
                    (
                        3,
                        Some(TimestampedAverage {
                            timestamp: Instant::now(),
                            average: min_avg,
                            num_measures: policy
                                .latency_awareness
                                .as_ref()
                                .unwrap()
                                .minimum_measurements,
                        }),
                    ),
                ],
            );

            tokio::time::sleep(2 * policy.latency_awareness.as_ref().unwrap().retry_period).await;

            let expected_groups = ExpectedGroupsBuilder::new()
                .group([1, 2, 3]) // pick + fallback local nodes
                .group([4, 5]) // fallback remote nodes
                .build();

            test_default_policy_with_given_cluster_and_routing_info(
                &policy,
                &cluster,
                &EMPTY_ROUTING_INFO,
                &expected_groups,
            );
        }

        #[tokio::test]
        async fn latency_aware_default_policy_penalises_if_conditions_met() {
            setup_tracing();
            let (policy, updater) = latency_aware_default_policy_with_explicit_updater();
            let cluster = tests::mock_cluster_state_for_token_unaware_tests().await;

            let min_avg = Duration::from_millis(10);

            policy.set_nodes_latency_stats(
                &cluster,
                &[
                    // 3 is fast enough to make 1 and 4 penalised.
                    (
                        1,
                        Some(TimestampedAverage {
                            timestamp: Instant::now(),
                            average: Duration::from_secs_f64(
                                policy
                                    .latency_awareness
                                    .as_ref()
                                    .unwrap()
                                    .exclusion_threshold
                                    * 1.05
                                    * min_avg.as_secs_f64(),
                            ),
                            num_measures: policy
                                .latency_awareness
                                .as_ref()
                                .unwrap()
                                .minimum_measurements,
                        }),
                    ),
                    (
                        3,
                        Some(TimestampedAverage {
                            timestamp: Instant::now(),
                            average: min_avg,
                            num_measures: policy
                                .latency_awareness
                                .as_ref()
                                .unwrap()
                                .minimum_measurements,
                        }),
                    ),
                    (
                        4,
                        Some(TimestampedAverage {
                            timestamp: Instant::now(),
                            average: Duration::from_secs_f64(
                                policy
                                    .latency_awareness
                                    .as_ref()
                                    .unwrap()
                                    .exclusion_threshold
                                    * 1.05
                                    * min_avg.as_secs_f64(),
                            ),
                            num_measures: policy
                                .latency_awareness
                                .as_ref()
                                .unwrap()
                                .minimum_measurements,
                        }),
                    ),
                ],
            );

            // Await last min average updater.
            updater.tick().await;

            let expected_groups = ExpectedGroupsBuilder::new()
                .group([2, 3]) // pick + fallback local nodes
                .group([5]) // fallback remote nodes
                .group([1]) // local node that was penalised due to high latency
                .group([4]) // remote node that was penalised due to high latency
                .build();

            test_default_policy_with_given_cluster_and_routing_info(
                &policy,
                &cluster,
                &EMPTY_ROUTING_INFO,
                &expected_groups,
            );
        }

        #[tokio::test]
        async fn latency_aware_default_policy_stops_penalising_after_min_average_increases_enough_only_after_update_rate_elapses(
        ) {
            setup_tracing();

            let (policy, updater) = latency_aware_default_policy_with_explicit_updater();

            let cluster = tests::mock_cluster_state_for_token_unaware_tests().await;

            let min_avg = Duration::from_millis(10);

            policy.set_nodes_latency_stats(
                &cluster,
                &[
                    (
                        1,
                        Some(TimestampedAverage {
                            timestamp: Instant::now(),
                            average: Duration::from_secs_f64(
                                policy
                                    .latency_awareness
                                    .as_ref()
                                    .unwrap()
                                    .exclusion_threshold
                                    * 1.05
                                    * min_avg.as_secs_f64(),
                            ),
                            num_measures: policy
                                .latency_awareness
                                .as_ref()
                                .unwrap()
                                .minimum_measurements,
                        }),
                    ),
                    (
                        3,
                        Some(TimestampedAverage {
                            timestamp: Instant::now(),
                            average: min_avg,
                            num_measures: policy
                                .latency_awareness
                                .as_ref()
                                .unwrap()
                                .minimum_measurements,
                        }),
                    ),
                ],
            );

            // Await last min average updater.
            updater.tick().await;
            {
                // min_avg is low enough to penalise node 1
                let expected_groups = ExpectedGroupsBuilder::new()
                    .group([2, 3]) // pick + fallback local nodes
                    .group([4, 5]) // fallback remote nodes
                    .group([1]) // local node that was penalised due to high latency
                    .build();

                test_default_policy_with_given_cluster_and_routing_info(
                    &policy,
                    &cluster,
                    &EMPTY_ROUTING_INFO,
                    &expected_groups,
                );
            }

            // node 3 becomes as slow as node 1
            policy.set_nodes_latency_stats(
                &cluster,
                &[(
                    3,
                    Some(TimestampedAverage {
                        timestamp: Instant::now(),
                        average: Duration::from_secs_f64(
                            policy
                                .latency_awareness
                                .as_ref()
                                .unwrap()
                                .exclusion_threshold
                                * min_avg.as_secs_f64(),
                        ),
                        num_measures: policy
                            .latency_awareness
                            .as_ref()
                            .unwrap()
                            .minimum_measurements,
                    }),
                )],
            );
            {
                // min_avg has not yet been updated and so node 1 is still being penalised
                let expected_groups = ExpectedGroupsBuilder::new()
                    .group([2, 3]) // pick + fallback local nodes
                    .group([4, 5]) // fallback remote nodes
                    .group([1]) // local node that was penalised due to high latency
                    .build();

                test_default_policy_with_given_cluster_and_routing_info(
                    &policy,
                    &cluster,
                    &EMPTY_ROUTING_INFO,
                    &expected_groups,
                );
            }

            updater.tick().await;
            {
                // min_avg has been updated and is already high enough to stop penalising node 1
                let expected_groups = ExpectedGroupsBuilder::new()
                    .group([1, 2, 3]) // pick + fallback local nodes
                    .group([4, 5]) // fallback remote nodes
                    .build();

                test_default_policy_with_given_cluster_and_routing_info(
                    &policy,
                    &cluster,
                    &EMPTY_ROUTING_INFO,
                    &expected_groups,
                );
            }
        }

        #[tokio::test]
        async fn latency_aware_default_policy_is_correctly_token_aware() {
            setup_tracing();

            struct Test<'a, 'b> {
                // If Some, then the provided value is set as a min_avg.
                // Else, the min_avg is updated based on values provided to set_latency_stats().
                preset_min_avg: Option<Duration>,
                latency_stats: &'b [(u16, Option<TimestampedAverage>)],
                routing_info: RoutingInfo<'a>,
                expected_groups: ExpectedGroups,
            }

            let cluster = tests::mock_cluster_state_for_token_aware_tests().await;
            let latency_awareness_defaults =
                latency_aware_default_policy().latency_awareness.unwrap();
            let min_avg = Duration::from_millis(10);

            let fast_leader = || {
                Some(TimestampedAverage {
                    timestamp: Instant::now(),
                    average: min_avg,
                    num_measures: latency_awareness_defaults.minimum_measurements,
                })
            };

            let fast_enough = || {
                Some(TimestampedAverage {
                    timestamp: Instant::now(),
                    average: Duration::from_secs_f64(
                        latency_awareness_defaults.exclusion_threshold
                            * 0.95
                            * min_avg.as_secs_f64(),
                    ),
                    num_measures: latency_awareness_defaults.minimum_measurements,
                })
            };

            let slow_penalised = || {
                Some(TimestampedAverage {
                    timestamp: Instant::now(),
                    average: Duration::from_secs_f64(
                        latency_awareness_defaults.exclusion_threshold
                            * 1.05
                            * min_avg.as_secs_f64(),
                    ),
                    num_measures: latency_awareness_defaults.minimum_measurements,
                })
            };

            let too_few_measurements_slow = || {
                Some(TimestampedAverage {
                    timestamp: Instant::now(),
                    average: Duration::from_secs_f64(
                        latency_awareness_defaults.exclusion_threshold
                            * 1.05
                            * min_avg.as_secs_f64(),
                    ),
                    num_measures: latency_awareness_defaults.minimum_measurements - 1,
                })
            };

            let too_few_measurements_fast_leader = || {
                Some(TimestampedAverage {
                    timestamp: Instant::now(),
                    average: min_avg,
                    num_measures: 1,
                })
            };

            let tests = [
                Test {
                    // Latency-awareness penalisation fires up and moves C and D to the end.
                    preset_min_avg: None,
                    latency_stats: &[
                        (A, fast_leader()),
                        (C, slow_penalised()),
                        (D, slow_penalised()),
                        (E, too_few_measurements_slow()),
                    ],
                    routing_info: RoutingInfo {
                        token: Some(Token::new(160)),
                        table: Some(TABLE_NTS_RF_3),
                        consistency: Consistency::Quorum,
                        ..Default::default()
                    },
                    // going through the ring, we get order: F , A , C , D , G , B , E
                    //                                      us  eu  eu  us  eu  eu  us
                    //                                      r2  r1  r1  r1  r2  r1  r1
                    expected_groups: ExpectedGroupsBuilder::new()
                        .group([A, G]) // fast enough local replicas
                        .group([F, E]) // fast enough remote replicas
                        .group([B]) // fast enough local nodes
                        .group([C]) // penalised local replica
                        .group([D]) // penalised remote replica
                        .build(),
                },
                Test {
                    // Latency-awareness has old minimum average cached, so does not fire.
                    preset_min_avg: Some(100 * min_avg),
                    routing_info: RoutingInfo {
                        token: Some(Token::new(160)),
                        table: Some(TABLE_NTS_RF_3),
                        consistency: Consistency::Quorum,
                        ..Default::default()
                    },
                    latency_stats: &[
                        (A, fast_leader()),
                        (B, fast_enough()),
                        (C, slow_penalised()),
                        (D, slow_penalised()),
                    ],
                    // going through the ring, we get order: F , A , C , D , G , B , E
                    //                                      us  eu  eu  us  eu  eu  us
                    //                                      r2  r1  r1  r1  r2  r1  r1
                    expected_groups: ExpectedGroupsBuilder::new()
                        .group([A, C, G]) // fast enough local replicas
                        .group([F, D, E]) // fast enough remote replicas
                        .group([B]) // fast enough local nodes
                        .build(),
                },
                Test {
                    // Both A and B are slower than C, but only B has enough measurements collected.
                    preset_min_avg: None,
                    latency_stats: &[
                        (A, slow_penalised()), // not really penalised, because no fast leader here
                        (B, slow_penalised()), // ditto
                        (C, too_few_measurements_fast_leader()),
                    ],
                    routing_info: RoutingInfo {
                        token: Some(Token::new(160)),
                        table: Some(TABLE_NTS_RF_2),
                        consistency: Consistency::Quorum,
                        ..Default::default()
                    },
                    // going through the ring, we get order: F , A , C , D , G , B , E
                    //                                      us  eu  eu  us  eu  eu  us
                    //                                      r2  r1  r1  r1  r2  r1  r1
                    expected_groups: ExpectedGroupsBuilder::new()
                        .group([A, G]) // pick + fallback local replicas
                        .group([F, D]) // remote replicas
                        .group([C, B]) // local nodes
                        .group([E]) // remote nodes
                        .build(),
                },
                Test {
                    // No latency stats, so latency-awareness is a no-op.
                    preset_min_avg: None,
                    routing_info: RoutingInfo {
                        token: Some(Token::new(160)),
                        table: Some(TABLE_INVALID),
                        consistency: Consistency::Quorum,
                        ..Default::default()
                    },
                    latency_stats: &[],
                    expected_groups: ExpectedGroupsBuilder::new()
                        .group([A, B, C, G]) // local nodes
                        .group([D, E, F]) // remote nodes
                        .build(),
                },
            ];

            for test in &tests {
                let (policy, updater) = latency_aware_default_policy_with_explicit_updater();

                if let Some(preset_min_avg) = test.preset_min_avg {
                    policy.set_nodes_latency_stats(
                        &cluster,
                        &[(
                            1,
                            Some(TimestampedAverage {
                                timestamp: Instant::now(),
                                average: preset_min_avg,
                                num_measures: latency_awareness_defaults.minimum_measurements,
                            }),
                        )],
                    );
                    // Await last min average updater for update with a forged min_avg.
                    updater.tick().await;
                    policy.set_nodes_latency_stats(&cluster, &[(1, None)]);
                }
                policy.set_nodes_latency_stats(&cluster, test.latency_stats);

                if test.preset_min_avg.is_none() {
                    // Await last min average updater for update with None min_avg.
                    updater.tick().await;
                }

                test_default_policy_with_given_cluster_and_routing_info(
                    &policy,
                    &cluster,
                    &test.routing_info,
                    &test.expected_groups,
                );
            }
        }

        #[tokio::test(start_paused = true)]
        async fn timestamped_average_works_when_clock_stops() {
            setup_tracing();
            let avg = Some(TimestampedAverage {
                timestamp: Instant::now(),
                average: Duration::from_secs(123),
                num_measures: 1,
            });
            let new_avg = TimestampedAverage::compute_next(avg, Duration::from_secs(456), 10.0);
            assert_eq!(
                new_avg,
                Some(TimestampedAverage {
                    timestamp: Instant::now(),
                    average: Duration::from_secs(123),
                    num_measures: 2,
                }),
            );
        }
    }
}
