use self::latency_awareness::LatencyAwareness;
pub use self::latency_awareness::LatencyAwarenessBuilder;

use super::{FallbackPlan, LoadBalancingPolicy, NodeRef, RoutingInfo};
use crate::{
    routing::Token,
    transport::{cluster::ClusterData, locator::ReplicaSet, node::Node, topology::Strategy},
};
use itertools::{Either, Itertools};
use rand::{prelude::SliceRandom, thread_rng, Rng};
use rand_pcg::Pcg32;
use scylla_cql::{errors::QueryError, frame::types::SerialConsistency, Consistency};
use std::{fmt, sync::Arc, time::Duration};
use tracing::warn;

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

    #[allow(unused)]
    fn rack(&self) -> Option<&str> {
        match self {
            Self::Any | Self::Datacenter(_) => None,
            Self::DatacenterAndRack(_, rack) => Some(rack),
        }
    }
}

#[derive(Clone, Copy)]
enum ReplicaOrder {
    Arbitrary,
    RingOrder,
}

#[derive(Clone, Copy)]
enum StatementType {
    Lwt,
    NonLwt,
}

/// The default load balancing policy.
///
/// It can be configured to be datacenter-aware and token-aware.
/// Datacenter failover for queries with non local consistency mode is also supported.
/// Latency awareness is available, althrough not recommended.
pub struct DefaultPolicy {
    preferences: NodeLocationPreference,
    is_token_aware: bool,
    permit_dc_failover: bool,
    pick_predicate: Box<dyn Fn(&NodeRef) -> bool + Send + Sync>,
    latency_awareness: Option<LatencyAwareness>,
    fixed_shuffle_seed: Option<u64>,
}

impl fmt::Debug for DefaultPolicy {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DefaultPolicy")
            .field("preferences", &self.preferences)
            .field("is_token_aware", &self.is_token_aware)
            .field("permit_dc_failover", &self.permit_dc_failover)
            .field("latency_awareness", &self.latency_awareness)
            .field("fixed_shuffle_seed", &self.fixed_shuffle_seed)
            .finish_non_exhaustive()
    }
}

impl LoadBalancingPolicy for DefaultPolicy {
    fn pick<'a>(&'a self, query: &'a RoutingInfo, cluster: &'a ClusterData) -> Option<NodeRef<'a>> {
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
        let statement_type = if query.is_confirmed_lwt {
            StatementType::Lwt
        } else {
            StatementType::NonLwt
        };
        if let Some(ts) = &routing_info.token_with_strategy {
            if let NodeLocationPreference::DatacenterAndRack(dc, rack) = &self.preferences {
                // Try to pick some alive local rack random replica.
                let local_rack_picked = self.pick_replica(
                    ts,
                    NodeLocationCriteria::DatacenterAndRack(dc, rack),
                    &self.pick_predicate,
                    cluster,
                    statement_type,
                );

                if let Some(alive_local_rack_replica) = local_rack_picked {
                    return Some(alive_local_rack_replica);
                }
            }

            if let NodeLocationPreference::DatacenterAndRack(dc, _)
            | NodeLocationPreference::Datacenter(dc) = &self.preferences
            {
                // Try to pick some alive local random replica.
                let picked = self.pick_replica(
                    ts,
                    NodeLocationCriteria::Datacenter(dc),
                    &self.pick_predicate,
                    cluster,
                    statement_type,
                );

                if let Some(alive_local_replica) = picked {
                    return Some(alive_local_replica);
                }
            }

            // If preferred datacenter is not specified, or if datacenter failover is possible, loosen restriction about locality.
            if self.preferences.datacenter().is_none()
                || self.is_datacenter_failover_possible(&routing_info)
            {
                // Try to pick some alive random replica.
                let picked = self.pick_replica(
                    ts,
                    NodeLocationCriteria::Any,
                    &self.pick_predicate,
                    cluster,
                    statement_type,
                );
                if let Some(alive_remote_replica) = picked {
                    return Some(alive_remote_replica);
                }
            }
        };

        // If no token was available (or all the replicas for that token are down), try to pick
        // some alive local node.
        // If there was no preferred datacenter specified, all nodes are treated as local.
        let nodes = self.preferred_node_set(cluster);

        if let NodeLocationPreference::DatacenterAndRack(dc, rack) = &self.preferences {
            // Try to pick some alive local rack random node.
            let rack_predicate = Self::make_rack_predicate(
                &self.pick_predicate,
                NodeLocationCriteria::DatacenterAndRack(dc, rack),
            );
            let local_rack_picked = Self::pick_node(nodes, rack_predicate);

            if let Some(alive_local_rack) = local_rack_picked {
                return Some(alive_local_rack);
            }
        }

        // Try to pick some alive local random node.
        if let Some(alive_local) = Self::pick_node(nodes, &self.pick_predicate) {
            return Some(alive_local);
        }

        let all_nodes = cluster.replica_locator().unique_nodes_in_global_ring();
        // If a datacenter failover is possible, loosen restriction about locality.
        if self.is_datacenter_failover_possible(&routing_info) {
            let picked = Self::pick_node(all_nodes, &self.pick_predicate);
            if let Some(alive_maybe_remote) = picked {
                return Some(alive_maybe_remote);
            }
        }

        // Previous checks imply that every node we could have selected is down.
        // Let's try to return a down node that wasn't disabled.
        let picked = Self::pick_node(nodes, |node| node.is_enabled());
        if let Some(down_but_enabled_local_node) = picked {
            return Some(down_but_enabled_local_node);
        }

        // If a datacenter failover is possible, loosen restriction about locality.
        if self.is_datacenter_failover_possible(&routing_info) {
            let picked = Self::pick_node(all_nodes, |node| node.is_enabled());
            if let Some(down_but_enabled_maybe_remote_node) = picked {
                return Some(down_but_enabled_maybe_remote_node);
            }
        }

        // Every node is disabled. This could be due to a bad host filter - configuration error.
        nodes.first()
    }

    fn fallback<'a>(
        &'a self,
        query: &'a RoutingInfo,
        cluster: &'a ClusterData,
    ) -> FallbackPlan<'a> {
        let routing_info = self.routing_info(query, cluster);
        let statement_type = if query.is_confirmed_lwt {
            StatementType::Lwt
        } else {
            StatementType::NonLwt
        };

        // If token is available, get a shuffled list of alive replicas.
        let maybe_replicas = if let Some(ts) = &routing_info.token_with_strategy {
            let maybe_local_rack_replicas =
                if let NodeLocationPreference::DatacenterAndRack(dc, rack) = &self.preferences {
                    let local_rack_replicas = self.fallback_replicas(
                        ts,
                        NodeLocationCriteria::DatacenterAndRack(dc, rack),
                        Self::is_alive,
                        cluster,
                        statement_type,
                    );
                    Either::Left(local_rack_replicas)
                } else {
                    Either::Right(std::iter::empty())
                };

            let maybe_local_replicas = if let NodeLocationPreference::DatacenterAndRack(dc, _)
            | NodeLocationPreference::Datacenter(dc) =
                &self.preferences
            {
                let local_replicas = self.fallback_replicas(
                    ts,
                    NodeLocationCriteria::Datacenter(dc),
                    Self::is_alive,
                    cluster,
                    statement_type,
                );
                Either::Left(local_replicas)
            } else {
                Either::Right(std::iter::empty())
            };

            // If no datacenter is preferred, or datacenter failover is possible, loosen restriction about locality.
            let maybe_remote_replicas = if self.preferences.datacenter().is_none()
                || self.is_datacenter_failover_possible(&routing_info)
            {
                let remote_replicas = self.fallback_replicas(
                    ts,
                    NodeLocationCriteria::Any,
                    Self::is_alive,
                    cluster,
                    statement_type,
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
                    .chain(maybe_remote_replicas),
            )
        } else {
            Either::Right(std::iter::empty::<NodeRef<'a>>())
        };

        // Get a list of all local alive nodes, and apply a round robin to it
        let local_nodes = self.preferred_node_set(cluster);

        let maybe_local_rack_nodes =
            if let NodeLocationPreference::DatacenterAndRack(dc, rack) = &self.preferences {
                let rack_predicate = Self::make_rack_predicate(
                    &self.pick_predicate,
                    NodeLocationCriteria::DatacenterAndRack(dc, rack),
                );
                Either::Left(Self::round_robin_nodes(local_nodes, rack_predicate))
            } else {
                Either::Right(std::iter::empty::<NodeRef<'a>>())
            };
        let robined_local_nodes = Self::round_robin_nodes(local_nodes, Self::is_alive);

        let all_nodes = cluster.replica_locator().unique_nodes_in_global_ring();

        // If a datacenter failover is possible, loosen restriction about locality.
        let maybe_remote_nodes = if self.is_datacenter_failover_possible(&routing_info) {
            let robined_all_nodes = Self::round_robin_nodes(all_nodes, Self::is_alive);

            Either::Left(robined_all_nodes)
        } else {
            Either::Right(std::iter::empty::<NodeRef<'a>>())
        };

        // Even if we consider some enabled nodes to be down, we should try contacting them in the last resort.
        let maybe_down_local_nodes = local_nodes.iter().filter(|node| node.is_enabled());

        // If a datacenter failover is possible, loosen restriction about locality.
        let maybe_down_nodes = if self.is_datacenter_failover_possible(&routing_info) {
            Either::Left(all_nodes.iter().filter(|node| node.is_enabled()))
        } else {
            Either::Right(std::iter::empty())
        };

        // Construct a fallback plan as a composition of replicas, local nodes and remote nodes.
        let plan = maybe_replicas
            .chain(maybe_local_rack_nodes)
            .chain(robined_local_nodes)
            .chain(maybe_remote_nodes)
            .chain(maybe_down_local_nodes)
            .chain(maybe_down_nodes)
            .unique();

        if let Some(latency_awareness) = self.latency_awareness.as_ref() {
            Box::new(latency_awareness.wrap(plan))
        } else {
            Box::new(plan)
        }
    }

    fn name(&self) -> String {
        "DefaultPolicy".to_string()
    }

    fn on_query_success(&self, _routing_info: &RoutingInfo, latency: Duration, node: NodeRef<'_>) {
        if let Some(latency_awareness) = self.latency_awareness.as_ref() {
            latency_awareness.report_query(node, latency);
        }
    }

    fn on_query_failure(
        &self,
        _routing_info: &RoutingInfo,
        latency: Duration,
        node: NodeRef<'_>,
        error: &QueryError,
    ) {
        if let Some(latency_awareness) = self.latency_awareness.as_ref() {
            if LatencyAwareness::reliable_latency_measure(error) {
                latency_awareness.report_query(node, latency);
            }
        }
    }
}

impl DefaultPolicy {
    /// Creates a builder used to customise configuration of a new DefaultPolicy.
    pub fn builder() -> DefaultPolicyBuilder {
        DefaultPolicyBuilder::new()
    }

    fn routing_info<'a>(
        &'a self,
        query: &'a RoutingInfo,
        cluster: &'a ClusterData,
    ) -> ProcessedRoutingInfo<'a> {
        let mut routing_info = ProcessedRoutingInfo::new(query, cluster);

        if !self.is_token_aware {
            routing_info.token_with_strategy = None;
        }

        routing_info
    }

    fn preferred_node_set<'a>(&'a self, cluster: &'a ClusterData) -> &'a [Arc<Node>] {
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

    fn nonfiltered_replica_set<'a>(
        &'a self,
        ts: &TokenWithStrategy<'a>,
        replica_location: NodeLocationCriteria<'a>,
        cluster: &'a ClusterData,
    ) -> ReplicaSet<'a> {
        let datacenter = replica_location.datacenter();

        cluster
            .replica_locator()
            .replicas_for_token(ts.token, ts.strategy, datacenter)
    }

    /// Wraps the provided predicate, adding the requirement for rack to match.
    fn make_rack_predicate<'a>(
        predicate: impl Fn(&NodeRef<'a>) -> bool + 'a,
        replica_location: NodeLocationCriteria<'a>,
    ) -> impl Fn(&NodeRef<'a>) -> bool {
        move |node| match replica_location {
            NodeLocationCriteria::Any | NodeLocationCriteria::Datacenter(_) => predicate(node),
            NodeLocationCriteria::DatacenterAndRack(_, rack) => {
                predicate(node) && node.rack.as_deref() == Some(rack)
            }
        }
    }

    fn replicas<'a>(
        &'a self,
        ts: &TokenWithStrategy<'a>,
        replica_location: NodeLocationCriteria<'a>,
        predicate: impl Fn(&NodeRef<'a>) -> bool + 'a,
        cluster: &'a ClusterData,
        order: ReplicaOrder,
    ) -> impl Iterator<Item = NodeRef<'a>> {
        let predicate = Self::make_rack_predicate(predicate, replica_location);

        let replica_iter = match order {
            ReplicaOrder::Arbitrary => Either::Left(
                self.nonfiltered_replica_set(ts, replica_location, cluster)
                    .into_iter(),
            ),
            ReplicaOrder::RingOrder => Either::Right(
                self.nonfiltered_replica_set(ts, replica_location, cluster)
                    .into_replicas_ordered()
                    .into_iter(),
            ),
        };
        replica_iter.filter(move |node: &NodeRef<'a>| predicate(node))
    }

    fn pick_replica<'a>(
        &'a self,
        ts: &TokenWithStrategy<'a>,
        replica_location: NodeLocationCriteria<'a>,
        predicate: &'a impl Fn(&NodeRef<'a>) -> bool,
        cluster: &'a ClusterData,
        statement_type: StatementType,
    ) -> Option<NodeRef<'a>> {
        match statement_type {
            StatementType::Lwt => self.pick_first_replica(ts, replica_location, predicate, cluster),
            StatementType::NonLwt => {
                self.pick_random_replica(ts, replica_location, predicate, cluster)
            }
        }
    }

    // This is to be used for LWT optimisation: in order to reduce contention
    // caused by Paxos conflicts, we always try to query replicas in the same, ring order.
    //
    // If preferred rack and DC are set, then the first (encountered on the ring) replica
    // that resides in that rack in that DC **and** satisfies the `predicate`  is returned.
    //
    // If preferred DC is set, then the first (encountered on the ring) replica
    // that resides in that DC **and** satisfies the `predicate` is returned.
    //
    // If no DC/rack preferences are set, then the only possible replica to be returned
    // (due to expensive computation of the others, and we avoid expensive computation in `pick()`)
    // is the primary replica. It is returned **iff** it satisfies the predicate, else None.
    fn pick_first_replica<'a>(
        &'a self,
        ts: &TokenWithStrategy<'a>,
        replica_location: NodeLocationCriteria<'a>,
        predicate: &'a impl Fn(&NodeRef<'a>) -> bool,
        cluster: &'a ClusterData,
    ) -> Option<NodeRef<'a>> {
        match replica_location {
            NodeLocationCriteria::Any => {
                // ReplicaSet returned by ReplicaLocator for this case:
                // 1) can be precomputed and lated used cheaply,
                // 2) returns replicas in the **non-ring order** (this because ReplicaSet chains
                //    ring-ordered replicas sequences from different DCs, thus not preserving
                //    the global ring order).
                // Because of 2), we can't use a precomputed ReplicaSet, but instead we need ReplicasOrdered.
                // As ReplicasOrdered can compute cheaply only the primary global replica
                // (computation of the remaining ones is expensive), in case that the primary replica
                // does not satisfy the `predicate`, None is returned. All expensive computation
                // is to be done only when `fallback()` is called.
                self.nonfiltered_replica_set(ts, replica_location, cluster)
                    .into_replicas_ordered()
                    .into_iter()
                    .next()
                    .and_then(|primary_replica| {
                        predicate(&primary_replica).then_some(primary_replica)
                    })
            }
            NodeLocationCriteria::Datacenter(_) | NodeLocationCriteria::DatacenterAndRack(_, _) => {
                // ReplicaSet returned by ReplicaLocator for this case:
                // 1) can be precomputed and lated used cheaply,
                // 2) returns replicas in the ring order (this is not true for the case
                //    when multiple DCs are allowed, because ReplicaSet chains replicas sequences
                //    from different DCs, thus not preserving the global ring order)
                self.replicas(
                    ts,
                    replica_location,
                    move |node| predicate(node),
                    cluster,
                    ReplicaOrder::RingOrder,
                )
                .next()
            }
        }
    }

    fn pick_random_replica<'a>(
        &'a self,
        ts: &TokenWithStrategy<'a>,
        replica_location: NodeLocationCriteria<'a>,
        predicate: &'a impl Fn(&NodeRef<'a>) -> bool,
        cluster: &'a ClusterData,
    ) -> Option<NodeRef<'a>> {
        let predicate = Self::make_rack_predicate(predicate, replica_location);

        let replica_set = self.nonfiltered_replica_set(ts, replica_location, cluster);

        if let Some(fixed) = self.fixed_shuffle_seed {
            let mut gen = Pcg32::new(fixed, 0);
            replica_set.choose_filtered(&mut gen, predicate)
        } else {
            replica_set.choose_filtered(&mut thread_rng(), predicate)
        }
    }

    fn fallback_replicas<'a>(
        &'a self,
        ts: &TokenWithStrategy<'a>,
        replica_location: NodeLocationCriteria<'a>,
        predicate: impl Fn(&NodeRef<'a>) -> bool + 'a,
        cluster: &'a ClusterData,
        statement_type: StatementType,
    ) -> impl Iterator<Item = NodeRef<'a>> {
        let order = match statement_type {
            StatementType::Lwt => ReplicaOrder::RingOrder,
            StatementType::NonLwt => ReplicaOrder::Arbitrary,
        };

        let replicas = self.replicas(ts, replica_location, predicate, cluster, order);

        match statement_type {
            // As an LWT optimisation: in order to reduce contention caused by Paxos conflicts,
            //  we always try to query replicas in the same order.
            StatementType::Lwt => Either::Left(replicas),
            StatementType::NonLwt => Either::Right(self.shuffle(replicas)),
        }
    }

    fn randomly_rotated_nodes(nodes: &[Arc<Node>]) -> impl Iterator<Item = NodeRef<'_>> {
        // Create a randomly rotated slice view
        let nodes_len = nodes.len();
        if nodes_len > 0 {
            let index = rand::thread_rng().gen_range(0..nodes_len); // gen_range() panics when range is empty!
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

    fn pick_node<'a>(
        nodes: &'a [Arc<Node>],
        predicate: impl Fn(&NodeRef<'a>) -> bool,
    ) -> Option<NodeRef<'a>> {
        // Select the first node that matches the predicate
        Self::randomly_rotated_nodes(nodes).find(predicate)
    }

    fn round_robin_nodes<'a>(
        nodes: &'a [Arc<Node>],
        predicate: impl Fn(&NodeRef<'a>) -> bool,
    ) -> impl Iterator<Item = NodeRef<'a>> {
        Self::randomly_rotated_nodes(nodes).filter(predicate)
    }

    fn shuffle<'a>(
        &self,
        iter: impl Iterator<Item = NodeRef<'a>>,
    ) -> impl Iterator<Item = NodeRef<'a>> {
        let mut vec: Vec<NodeRef<'a>> = iter.collect();

        if let Some(fixed) = self.fixed_shuffle_seed {
            let mut gen = Pcg32::new(fixed, 0);
            vec.shuffle(&mut gen);
        } else {
            vec.shuffle(&mut thread_rng());
        }

        vec.into_iter()
    }

    fn is_alive(node: &NodeRef<'_>) -> bool {
        // For now, we leave this as stub, until we have time to improve node events.
        // node.is_enabled() && !node.is_down()
        node.is_enabled()
    }

    fn is_datacenter_failover_possible(&self, routing_info: &ProcessedRoutingInfo) -> bool {
        self.preferences.datacenter().is_some()
            && self.permit_dc_failover
            && !routing_info.local_consistency
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
            fixed_shuffle_seed: None,
        }
    }
}

/// The intended way to instantiate the DefaultPolicy.
///
/// # Example
/// ```
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// use scylla::load_balancing::DefaultPolicy;
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
            Box::new(move |node: &NodeRef| DefaultPolicy::is_alive(node) && latency_predicate(node))
                as Box<dyn Fn(&NodeRef) -> bool + Send + Sync + 'static>
        } else {
            Box::new(DefaultPolicy::is_alive)
        };

        Arc::new(DefaultPolicy {
            preferences: self.preferences,
            is_token_aware: self.is_token_aware,
            permit_dc_failover: self.permit_dc_failover,
            pick_predicate,
            latency_awareness,
            fixed_shuffle_seed: (!self.enable_replica_shuffle).then(rand::random),
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

    // True if one of LOCAL_ONE, LOCAL_QUORUM, LOCAL_SERIAL was requested
    local_consistency: bool,
}

impl<'a> ProcessedRoutingInfo<'a> {
    fn new(query: &'a RoutingInfo, cluster: &'a ClusterData) -> ProcessedRoutingInfo<'a> {
        let local_consistency = matches!(
            (query.consistency, query.serial_consistency),
            (Consistency::LocalQuorum, _)
                | (Consistency::LocalOne, _)
                | (_, Some(SerialConsistency::LocalSerial))
        );

        Self {
            token_with_strategy: TokenWithStrategy::new(query, cluster),
            local_consistency,
        }
    }
}

struct TokenWithStrategy<'a> {
    strategy: &'a Strategy,
    token: Token,
}

impl<'a> TokenWithStrategy<'a> {
    fn new(query: &'a RoutingInfo, cluster: &'a ClusterData) -> Option<TokenWithStrategy<'a>> {
        let token = query.token?;
        let keyspace_name = query.keyspace?;
        let keyspace = cluster.get_keyspace_info().get(keyspace_name)?;
        let strategy = &keyspace.strategy;
        Some(TokenWithStrategy { strategy, token })
    }
}

#[cfg(test)]
mod tests {
    use scylla_cql::{frame::types::SerialConsistency, Consistency};

    use self::framework::{
        get_plan_and_collect_node_identifiers, mock_cluster_data_for_token_unaware_tests,
        ExpectedGroups, ExpectedGroupsBuilder,
    };
    use crate::{
        load_balancing::{
            default::tests::framework::mock_cluster_data_for_token_aware_tests, RoutingInfo,
        },
        routing::Token,
        transport::{
            locator::test::{KEYSPACE_NTS_RF_2, KEYSPACE_NTS_RF_3, KEYSPACE_SS_RF_2},
            ClusterData,
        },
    };

    use super::{DefaultPolicy, NodeLocationPreference};

    pub(crate) mod framework {
        use std::collections::{HashMap, HashSet};

        use uuid::Uuid;

        use crate::{
            load_balancing::{LoadBalancingPolicy, Plan, RoutingInfo},
            routing::Token,
            transport::{
                locator::test::{id_to_invalid_addr, mock_metadata_for_token_aware_tests},
                topology::{Metadata, Peer},
                ClusterData,
            },
        };

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
                        "Plan length different than expected"
                    );

                    // Now, split `got` into groups of expected sizes
                    // and just `assert_eq` them
                    let mut got = got.iter();
                    for (group_id, expected) in self.groups.iter().enumerate() {
                        // Collect the nodes that consistute the group
                        // in the actual plan
                        let got_group: Vec<_> = (&mut got).take(expected.len()).copied().collect();

                        match expected {
                            ExpectedGroup::NonDeterministic(expected_set)
                            | ExpectedGroup::Deterministic(expected_set) => {
                                // Verify that the group has the same nodes as the
                                // expected one
                                let got_set: HashSet<_> = got_group.iter().copied().collect();
                                assert_eq!(&got_set, expected_set);
                            }
                            ExpectedGroup::Ordered(sequence) => {
                                assert_eq!(&got_group, sequence);
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
                                assert!(sets.len() > 1);
                            }
                        }
                        ExpectedGroup::Deterministic(_) | ExpectedGroup::Ordered(_) => {
                            // The group is supposed to be deterministic,
                            // i.e. a given instance of the default policy
                            // must always return the nodes within it using
                            // the same order.
                            // There will only be one, unique ordering shared
                            // by all plans - check this
                            assert_eq!(sets.len(), 1);
                        }
                    }
                }
            }
        }

        #[test]
        fn test_assert_proper_grouping_in_plan_good() {
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
            let got = vec![1u16, 2, 3, 4];
            let expected_groups = ExpectedGroupsBuilder::new()
                .group([1])
                .group([3, 2, 4])
                .group([5])
                .build();

            expected_groups.assert_proper_grouping_in_plans(&[got]);
        }

        // based on locator mock cluster
        pub(crate) async fn mock_cluster_data_for_token_aware_tests() -> ClusterData {
            let metadata = mock_metadata_for_token_aware_tests();
            ClusterData::new(metadata, &Default::default(), &HashMap::new(), &None, None).await
        }

        // creates ClusterData with info about 5 nodes living in 2 different datacenters
        // ring field is minimal, not intended to influence the tests
        pub(crate) async fn mock_cluster_data_for_token_unaware_tests() -> ClusterData {
            let peers = [("eu", 1), ("eu", 2), ("eu", 3), ("us", 4), ("us", 5)]
                .iter()
                .map(|(dc, id)| Peer {
                    datacenter: Some(dc.to_string()),
                    rack: None,
                    address: id_to_invalid_addr(*id),
                    tokens: vec![Token {
                        value: *id as i64 * 100,
                    }],
                    host_id: Uuid::new_v4(),
                })
                .collect::<Vec<_>>();

            let info = Metadata {
                peers,
                keyspaces: HashMap::new(),
            };

            ClusterData::new(info, &Default::default(), &HashMap::new(), &None, None).await
        }

        pub(crate) fn get_plan_and_collect_node_identifiers(
            policy: &impl LoadBalancingPolicy,
            query_info: &RoutingInfo,
            cluster: &ClusterData,
        ) -> Vec<u16> {
            let plan = Plan::new(policy, query_info, cluster);
            plan.map(|node| node.address.port()).collect::<Vec<_>>()
        }
    }

    pub(crate) const EMPTY_ROUTING_INFO: RoutingInfo = RoutingInfo {
        token: None,
        keyspace: None,
        is_confirmed_lwt: false,
        consistency: Consistency::Quorum,
        serial_consistency: Some(SerialConsistency::Serial),
    };

    pub(super) async fn test_default_policy_with_given_cluster_and_routing_info(
        policy: &DefaultPolicy,
        cluster: &ClusterData,
        routing_info: &RoutingInfo<'_>,
        expected_groups: &ExpectedGroups,
    ) {
        let mut plans = Vec::new();
        for _ in 0..256 {
            let plan = get_plan_and_collect_node_identifiers(policy, routing_info, cluster);
            plans.push(plan);
        }
        expected_groups.assert_proper_grouping_in_plans(&plans);
    }

    async fn test_given_default_policy_with_token_unaware_statements(
        policy: DefaultPolicy,
        expected_groups: &ExpectedGroups,
    ) {
        let cluster = mock_cluster_data_for_token_unaware_tests().await;

        test_default_policy_with_given_cluster_and_routing_info(
            &policy,
            &cluster,
            &EMPTY_ROUTING_INFO,
            expected_groups,
        )
        .await;
    }

    #[tokio::test]
    async fn test_default_policy_with_token_unaware_statements() {
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
        use crate::transport::locator::test::{A, B, C, D, E, F, G};

        let cluster = mock_cluster_data_for_token_aware_tests().await;
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
                    token: Some(Token { value: 160 }),
                    keyspace: Some(KEYSPACE_NTS_RF_2),
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
                    fixed_shuffle_seed: Some(123),
                    ..Default::default()
                },
                routing_info: RoutingInfo {
                    token: Some(Token { value: 160 }),
                    keyspace: Some(KEYSPACE_NTS_RF_2),
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
            // Keyspace NTS with RF=2 with DC failover forbidden by local Consistency
            Test {
                policy: DefaultPolicy {
                    preferences: NodeLocationPreference::Datacenter("eu".to_owned()),
                    is_token_aware: true,
                    permit_dc_failover: true,
                    ..Default::default()
                },
                routing_info: RoutingInfo {
                    token: Some(Token { value: 160 }),
                    keyspace: Some(KEYSPACE_NTS_RF_2),
                    consistency: Consistency::LocalOne, // local Consistency forbids datacenter failover
                    ..Default::default()
                },
                // going through the ring, we get order: F , A , C , D , G , B , E
                //                                      us  eu  eu  us  eu  eu  us
                //                                      r2  r1  r1  r1  r2  r1  r1
                expected_groups: ExpectedGroupsBuilder::new()
                    .group([A, G]) // pick + fallback local replicas
                    .group([C, B]) // local nodes
                    .build(), // failover is forbidden by local Consistency
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
                    token: Some(Token { value: 160 }),
                    keyspace: Some(KEYSPACE_NTS_RF_2),
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
                    token: Some(Token { value: 160 }),
                    keyspace: Some(KEYSPACE_NTS_RF_3),
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
                    fixed_shuffle_seed: Some(123),
                    ..Default::default()
                },
                routing_info: RoutingInfo {
                    token: Some(Token { value: 160 }),
                    keyspace: Some(KEYSPACE_NTS_RF_3),
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
                    token: Some(Token { value: 160 }),
                    keyspace: Some(KEYSPACE_NTS_RF_3),
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
                    token: Some(Token { value: 160 }),
                    keyspace: Some(KEYSPACE_SS_RF_2),
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
            // Keyspace SS with RF=2 with DC failover forbidden by local Consistency
            Test {
                policy: DefaultPolicy {
                    preferences: NodeLocationPreference::Datacenter("eu".to_owned()),
                    is_token_aware: true,
                    permit_dc_failover: true,
                    ..Default::default()
                },
                routing_info: RoutingInfo {
                    token: Some(Token { value: 160 }),
                    keyspace: Some(KEYSPACE_SS_RF_2),
                    consistency: Consistency::LocalOne, // local Consistency forbids datacenter failover
                    ..Default::default()
                },
                // going through the ring, we get order: F , A , C , D , G , B , E
                //                                      us  eu  eu  us  eu  eu  us
                //                                      r2  r1  r1  r1  r2  r1  r1
                expected_groups: ExpectedGroupsBuilder::new()
                    .group([A]) // pick + fallback local replicas
                    .group([C, G, B]) // local nodes
                    .build(), // failover is forbidden by local Consistency
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
                    keyspace: Some(KEYSPACE_NTS_RF_3),
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
                    token: Some(Token { value: 160 }),
                    keyspace: None, // no keyspace
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
                    token: Some(Token { value: 160 }),
                    keyspace: Some(KEYSPACE_NTS_RF_2),
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
                    token: Some(Token { value: 160 }),
                    keyspace: Some(KEYSPACE_NTS_RF_2),
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
                    token: Some(Token { value: 160 }),
                    keyspace: Some(KEYSPACE_NTS_RF_2),
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
                    token: Some(Token { value: 160 }),
                    keyspace: Some(KEYSPACE_NTS_RF_2),
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
                    token: Some(Token { value: 160 }),
                    keyspace: Some(KEYSPACE_NTS_RF_3),
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
                    fixed_shuffle_seed: Some(123),
                    ..Default::default()
                },
                routing_info: RoutingInfo {
                    token: Some(Token { value: 560 }),
                    keyspace: Some(KEYSPACE_SS_RF_2),
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
                    token: Some(Token { value: 160 }),
                    keyspace: Some(KEYSPACE_SS_RF_2),
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
                    keyspace: Some(KEYSPACE_NTS_RF_3),
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
            )
            .await;
        }
    }

    #[tokio::test]
    async fn test_default_policy_with_lwt_statements() {
        use crate::transport::locator::test::{A, B, C, D, E, F, G};

        let cluster = mock_cluster_data_for_token_aware_tests().await;
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
                    token: Some(Token { value: 160 }),
                    keyspace: Some(KEYSPACE_NTS_RF_2),
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
                    fixed_shuffle_seed: Some(123),
                    ..Default::default()
                },
                routing_info: RoutingInfo {
                    token: Some(Token { value: 160 }),
                    keyspace: Some(KEYSPACE_NTS_RF_2),
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
            // Keyspace NTS with RF=2 with DC failover forbidden by local Consistency
            Test {
                policy: DefaultPolicy {
                    preferences: NodeLocationPreference::Datacenter("eu".to_owned()),
                    is_token_aware: true,
                    permit_dc_failover: true,
                    ..Default::default()
                },
                routing_info: RoutingInfo {
                    token: Some(Token { value: 160 }),
                    keyspace: Some(KEYSPACE_NTS_RF_2),
                    consistency: Consistency::LocalOne, // local Consistency forbids datacenter failover
                    is_confirmed_lwt: true,
                    ..Default::default()
                },
                // going through the ring, we get order: F , A , C , D , G , B , E
                //                                      us  eu  eu  us  eu  eu  us
                //                                      r2  r1  r1  r1  r2  r1  r1
                expected_groups: ExpectedGroupsBuilder::new()
                    .ordered([A, G]) // pick + fallback local replicas
                    .group([C, B]) // local nodes
                    .build(), // failover is forbidden by local Consistency
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
                    token: Some(Token { value: 160 }),
                    keyspace: Some(KEYSPACE_NTS_RF_2),
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
                    token: Some(Token { value: 160 }),
                    keyspace: Some(KEYSPACE_NTS_RF_3),
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
                    fixed_shuffle_seed: Some(123),
                    ..Default::default()
                },
                routing_info: RoutingInfo {
                    token: Some(Token { value: 160 }),
                    keyspace: Some(KEYSPACE_NTS_RF_3),
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
                    token: Some(Token { value: 160 }),
                    keyspace: Some(KEYSPACE_NTS_RF_3),
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
                    token: Some(Token { value: 160 }),
                    keyspace: Some(KEYSPACE_SS_RF_2),
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
            // Keyspace SS with RF=2 with DC failover forbidden by local Consistency
            Test {
                policy: DefaultPolicy {
                    preferences: NodeLocationPreference::Datacenter("eu".to_owned()),
                    is_token_aware: true,
                    permit_dc_failover: true,
                    ..Default::default()
                },
                routing_info: RoutingInfo {
                    token: Some(Token { value: 160 }),
                    keyspace: Some(KEYSPACE_SS_RF_2),
                    consistency: Consistency::LocalOne, // local Consistency forbids datacenter failover
                    is_confirmed_lwt: true,
                    ..Default::default()
                },
                // going through the ring, we get order: F , A , C , D , G , B , E
                //                                      us  eu  eu  us  eu  eu  us
                //                                      r2  r1  r1  r1  r2  r1  r1
                expected_groups: ExpectedGroupsBuilder::new()
                    .ordered([A]) // pick + fallback local replicas
                    .group([C, G, B]) // local nodes
                    .build(), // failover is forbidden by local Consistency
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
                    keyspace: Some(KEYSPACE_NTS_RF_3),
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
                    token: Some(Token { value: 160 }),
                    keyspace: None, // no keyspace
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
                    token: Some(Token { value: 160 }),
                    keyspace: Some(KEYSPACE_NTS_RF_2),
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
                    token: Some(Token { value: 160 }),
                    keyspace: Some(KEYSPACE_NTS_RF_2),
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
                    token: Some(Token { value: 160 }),
                    keyspace: Some(KEYSPACE_NTS_RF_2),
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
                    token: Some(Token { value: 160 }),
                    keyspace: Some(KEYSPACE_NTS_RF_2),
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
                    token: Some(Token { value: 160 }),
                    keyspace: Some(KEYSPACE_NTS_RF_3),
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
                    fixed_shuffle_seed: Some(123),
                    ..Default::default()
                },
                routing_info: RoutingInfo {
                    token: Some(Token { value: 760 }),
                    keyspace: Some(KEYSPACE_SS_RF_2),
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
                    token: Some(Token { value: 160 }),
                    keyspace: Some(KEYSPACE_SS_RF_2),
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
            )
            .await;
        }
    }
}

mod latency_awareness {
    use futures::{future::RemoteHandle, FutureExt};
    use itertools::Either;
    use scylla_cql::errors::{DbError, QueryError};
    use tokio::time::{Duration, Instant};
    use tracing::{instrument::WithSubscriber, trace, warn};
    use uuid::Uuid;

    use crate::{load_balancing::NodeRef, transport::node::Node};
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
            tokio::task::spawn(updater_fut.with_current_subscriber());

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
            fallback: impl Iterator<Item = NodeRef<'a>>,
        ) -> impl Iterator<Item = NodeRef<'a>> {
            let min_avg_latency = match self.last_min_latency.load() {
                Some(min_avg) => min_avg,
                None => return Either::Left(fallback), // noop, as no latency data has been collected yet
            };

            Either::Right(IteratorWithSkippedNodes::new(
                self.node_avgs.read().unwrap().deref(),
                fallback,
                self.exclusion_threshold,
                self.retry_period,
                self.minimum_measurements,
                min_avg_latency,
            ))
        }

        pub(super) fn report_query(&self, node: &Node, latency: Duration) {
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

        pub(crate) fn reliable_latency_measure(error: &QueryError) -> bool {
            match error {
                // "fast" errors, i.e. ones that are returned quickly after the query begins
                QueryError::BadQuery(_)
                | QueryError::TooManyOrphanedStreamIds(_)
                | QueryError::UnableToAllocStreamId
                | QueryError::DbError(DbError::IsBootstrapping, _)
                | QueryError::DbError(DbError::Unavailable { .. }, _)
                | QueryError::DbError(DbError::Unprepared { .. }, _)
                | QueryError::TranslationError(_)
                | QueryError::DbError(DbError::Overloaded { .. }, _)
                | QueryError::DbError(DbError::RateLimitReached { .. }, _) => false,

                // "slow" errors, i.e. ones that are returned after considerable time of query being run
                QueryError::DbError(_, _)
                | QueryError::InvalidMessage(_)
                | QueryError::IoError(_)
                | QueryError::ProtocolError(_)
                | QueryError::TimeoutError
                | QueryError::EmptyQueryPlan
                | QueryError::RequestTimeout(_) => true,
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
    /// use scylla::load_balancing::{
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

    struct IteratorWithSkippedNodes<'a, Fast, Penalised>
    where
        Fast: Iterator<Item = NodeRef<'a>>,
        Penalised: Iterator<Item = NodeRef<'a>>,
    {
        fast_nodes: Fast,
        penalised_nodes: Penalised,
    }

    impl<'a>
        IteratorWithSkippedNodes<
            'a,
            std::vec::IntoIter<NodeRef<'a>>,
            std::vec::IntoIter<NodeRef<'a>>,
        >
    {
        fn new(
            average_latencies: &HashMap<Uuid, RwLock<Option<TimestampedAverage>>>,
            nodes: impl Iterator<Item = NodeRef<'a>>,
            exclusion_threshold: f64,
            retry_period: Duration,
            minimum_measurements: usize,
            min_avg: Duration,
        ) -> Self {
            let mut fast_nodes = vec![];
            let mut penalised_nodes = vec![];

            for node in nodes {
                match fast_enough(
                    average_latencies,
                    node.host_id,
                    exclusion_threshold,
                    retry_period,
                    minimum_measurements,
                    min_avg,
                ) {
                    FastEnough::Yes => fast_nodes.push(node),
                    FastEnough::No { average } => {
                        trace!("Latency awareness: Penalising node {{address={}, datacenter={:?}, rack={:?}}} for being on average at least {} times slower (latency: {}ms) than the fastest ({}ms).",
                                node.address, node.datacenter, node.rack, exclusion_threshold, average.as_millis(), min_avg.as_millis());
                        penalised_nodes.push(node);
                    }
                }
            }

            Self {
                fast_nodes: fast_nodes.into_iter(),
                penalised_nodes: penalised_nodes.into_iter(),
            }
        }
    }

    impl<'a, Fast, Penalised> Iterator for IteratorWithSkippedNodes<'a, Fast, Penalised>
    where
        Fast: Iterator<Item = NodeRef<'a>>,
        Penalised: Iterator<Item = NodeRef<'a>>,
    {
        type Item = &'a Arc<Node>;

        fn next(&mut self) -> Option<Self::Item> {
            self.fast_nodes
                .next()
                .or_else(|| self.penalised_nodes.next())
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
            load_balancing::default::NodeLocationPreference, test_utils::create_new_session_builder,
        };
        use crate::{
            load_balancing::{
                default::tests::test_default_policy_with_given_cluster_and_routing_info,
                RoutingInfo,
            },
            routing::Token,
            transport::{
                locator::test::{
                    id_to_invalid_addr, A, B, C, D, E, F, G, KEYSPACE_NTS_RF_2, KEYSPACE_NTS_RF_3,
                },
                ClusterData, NodeAddr,
            },
            ExecutionProfile,
        };
        use tokio::time::Instant;

        trait DefaultPolicyTestExt {
            fn set_nodes_latency_stats(
                &self,
                cluster: &ClusterData,
                averages: &[(u16, Option<TimestampedAverage>)],
            );
        }

        impl DefaultPolicyTestExt for DefaultPolicy {
            fn set_nodes_latency_stats(
                &self,
                cluster: &ClusterData,
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
                    println!("Set latency: node {}, latency {:?}.", id, average);
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
                Box::new(move |node: &NodeRef| {
                    DefaultPolicy::is_alive(node) && latency_predicate(node)
                }) as Box<dyn Fn(&NodeRef) -> bool + Send + Sync + 'static>
            };

            DefaultPolicy {
                preferences: NodeLocationPreference::Datacenter("eu".to_owned()),
                permit_dc_failover: true,
                is_token_aware: true,
                pick_predicate,
                latency_awareness: Some(latency_awareness),
                fixed_shuffle_seed: None,
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
            let policy = latency_aware_default_policy();
            let cluster = tests::mock_cluster_data_for_token_unaware_tests().await;

            let expected_groups = ExpectedGroupsBuilder::new()
                .group([1, 2, 3]) // pick + fallback local nodes
                .group([4, 5]) // fallback remote nodes
                .build();

            test_default_policy_with_given_cluster_and_routing_info(
                &policy,
                &cluster,
                &EMPTY_ROUTING_INFO,
                &expected_groups,
            )
            .await;
        }

        #[tokio::test]
        async fn latency_aware_default_policy_does_not_penalise_if_not_enough_measurements() {
            let policy = latency_aware_default_policy();
            let cluster = tests::mock_cluster_data_for_token_unaware_tests().await;

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
            )
            .await;
        }

        #[tokio::test]
        async fn latency_aware_default_policy_does_not_penalise_if_exclusion_threshold_not_crossed()
        {
            let policy = latency_aware_default_policy();
            let cluster = tests::mock_cluster_data_for_token_unaware_tests().await;

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
            )
            .await;
        }

        #[tokio::test]
        async fn latency_aware_default_policy_does_not_penalise_if_retry_period_expired() {
            let policy = latency_aware_default_policy_customised(|b| {
                b.retry_period(Duration::from_millis(10))
            });

            let cluster = tests::mock_cluster_data_for_token_unaware_tests().await;

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
            )
            .await;
        }

        #[tokio::test]
        async fn latency_aware_default_policy_penalises_if_conditions_met() {
            let _ = tracing_subscriber::fmt::fmt()
                .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
                .without_time()
                .try_init();
            let (policy, updater) = latency_aware_default_policy_with_explicit_updater();
            let cluster = tests::mock_cluster_data_for_token_unaware_tests().await;

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
            )
            .await;
        }

        #[tokio::test]
        async fn latency_aware_default_policy_stops_penalising_after_min_average_increases_enough_only_after_update_rate_elapses(
        ) {
            let (policy, updater) = latency_aware_default_policy_with_explicit_updater();

            let cluster = tests::mock_cluster_data_for_token_unaware_tests().await;

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
                )
                .await;
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
                )
                .await;
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
                )
                .await;
            }
        }

        #[tokio::test]
        async fn latency_aware_default_policy_is_correctly_token_aware() {
            let _ = tracing_subscriber::fmt::try_init();

            struct Test<'a, 'b> {
                // If Some, then the provided value is set as a min_avg.
                // Else, the min_avg is updated based on values provided to set_latency_stats().
                preset_min_avg: Option<Duration>,
                latency_stats: &'b [(u16, Option<TimestampedAverage>)],
                routing_info: RoutingInfo<'a>,
                expected_groups: ExpectedGroups,
            }

            let cluster = tests::mock_cluster_data_for_token_aware_tests().await;
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
                        token: Some(Token { value: 160 }),
                        keyspace: Some(KEYSPACE_NTS_RF_3),
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
                        token: Some(Token { value: 160 }),
                        keyspace: Some(KEYSPACE_NTS_RF_3),
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
                        token: Some(Token { value: 160 }),
                        keyspace: Some(KEYSPACE_NTS_RF_2),
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
                        token: Some(Token { value: 160 }),
                        keyspace: Some("invalid"),
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
                )
                .await;
            }
        }

        // This is a regression test for #696.
        #[tokio::test]
        #[ntest::timeout(1000)]
        async fn latency_aware_query_completes() {
            let policy = DefaultPolicy::builder()
                .latency_awareness(LatencyAwarenessBuilder::default())
                .build();
            let handle = ExecutionProfile::builder()
                .load_balancing_policy(policy)
                .build()
                .into_handle();

            let session = create_new_session_builder()
                .default_execution_profile_handle(handle)
                .build()
                .await
                .unwrap();

            session.query("whatever", ()).await.unwrap_err();
        }

        #[tokio::test]
        async fn timestamped_average_works_when_clock_stops() {
            tokio::time::pause();
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
