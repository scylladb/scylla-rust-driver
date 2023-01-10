use super::{FallbackPlan, LoadBalancingPolicy, NodeRef, RoutingInfo};
use crate::{
    routing::Token,
    transport::{cluster::ClusterData, locator::ReplicaSet, node::Node, topology::Strategy},
};
use itertools::{Either, Itertools};
use rand::{prelude::SliceRandom, thread_rng, Rng};
use scylla_cql::{frame::types::SerialConsistency, Consistency};
use std::sync::Arc;
use tracing::warn;

// TODO: LWT optimisation
/// The default load balancing policy.
///
/// It can be configured to be datacenter-aware and token-aware.
/// Datacenter failover for queries with non local consistency mode is also supported.
#[derive(Debug)]
pub struct DefaultPolicy {
    preferred_datacenter: Option<String>,
    is_token_aware: bool,
    permit_dc_failover: bool,
}

impl LoadBalancingPolicy for DefaultPolicy {
    fn pick<'a>(&'a self, query: &'a RoutingInfo, cluster: &'a ClusterData) -> Option<NodeRef<'a>> {
        let routing_info = self.routing_info(query, cluster);
        if let Some(ref token_with_strategy) = routing_info.token_with_strategy {
            if self.preferred_datacenter.is_some()
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
        if let Some(ts) = &routing_info.token_with_strategy {
            // Try to pick some alive local random replica.
            // If preferred datacenter is not specified, all replicas are treated as local.
            let picked = self.pick_replica(ts, true, Self::is_alive, cluster);
            if let Some(alive_local_replica) = picked {
                return Some(alive_local_replica);
            }

            // If datacenter failover is possible, loosen restriction about locality.
            if self.is_datacenter_failover_possible(&routing_info) {
                let picked = self.pick_replica(ts, false, Self::is_alive, cluster);
                if let Some(alive_remote_replica) = picked {
                    return Some(alive_remote_replica);
                }
            }
        };

        // If no token was available (or all the replicas for that token are down), try to pick
        // some alive local node.
        // If there was no preferred datacenter specified, all nodes are treated as local.
        let nodes = self.preferred_node_set(cluster);
        let picked = Self::pick_node(nodes, Self::is_alive);
        if let Some(alive_local) = picked {
            return Some(alive_local);
        }

        let all_nodes = cluster.replica_locator().unique_nodes_in_global_ring();
        // If a datacenter failover is possible, loosen restriction about locality.
        if self.is_datacenter_failover_possible(&routing_info) {
            let picked = Self::pick_node(all_nodes, Self::is_alive);
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

        // If token is available, get a shuffled list of alive replicas.
        let maybe_replicas = if let Some(ts) = &routing_info.token_with_strategy {
            let local_replicas = self.shuffled_replicas(ts, true, Self::is_alive, cluster);

            // If a datacenter failover is possible, loosen restriction about locality.
            let maybe_remote_replicas = if self.is_datacenter_failover_possible(&routing_info) {
                let remote_replicas = self.shuffled_replicas(ts, false, Self::is_alive, cluster);
                Either::Left(remote_replicas)
            } else {
                Either::Right(std::iter::empty())
            };

            // Produce an iterator, prioritizes local replicas.
            // If preferred datacenter is not specified, every replica is treated as a local one.
            Either::Left(local_replicas.chain(maybe_remote_replicas))
        } else {
            Either::Right(std::iter::empty::<NodeRef<'a>>())
        };

        // Get a list of all local alive nodes, and apply a round robin to it
        let local_nodes = self.preferred_node_set(cluster);
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
            .chain(robined_local_nodes)
            .chain(maybe_remote_nodes)
            .chain(maybe_down_local_nodes)
            .chain(maybe_down_nodes)
            .unique();

        Box::new(plan)
    }

    fn name(&self) -> String {
        "DefaultPolicy".to_string()
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
        if let Some(preferred_datacenter) = &self.preferred_datacenter {
            if let Some(nodes) = cluster
                .replica_locator()
                .unique_nodes_in_datacenter_ring(preferred_datacenter.as_str())
            {
                return nodes;
            } else {
                tracing::warn!(
                    "Datacenter specified as the preferred one ({}) does not exist!",
                    preferred_datacenter
                );
                // We won't guess any DC, as it could lead to possible violation dc failover ban.
                return &[];
            }
        }

        cluster.replica_locator().unique_nodes_in_global_ring()
    }

    fn nonfiltered_replica_set<'a>(
        &'a self,
        ts: &TokenWithStrategy<'a>,
        should_be_local: bool,
        cluster: &'a ClusterData,
    ) -> ReplicaSet<'a> {
        let datacenter = should_be_local
            .then_some(self.preferred_datacenter.as_deref())
            .flatten();

        cluster
            .replica_locator()
            .replicas_for_token(ts.token, ts.strategy, datacenter)
    }

    fn replicas<'a>(
        &'a self,
        ts: &TokenWithStrategy<'a>,
        should_be_local: bool,
        predicate: impl Fn(&NodeRef<'a>) -> bool,
        cluster: &'a ClusterData,
    ) -> impl Iterator<Item = NodeRef<'a>> {
        self.nonfiltered_replica_set(ts, should_be_local, cluster)
            .into_iter()
            .filter(predicate)
    }

    fn pick_replica<'a>(
        &'a self,
        ts: &TokenWithStrategy<'a>,
        should_be_local: bool,
        predicate: impl Fn(&NodeRef<'a>) -> bool,
        cluster: &'a ClusterData,
    ) -> Option<NodeRef<'a>> {
        self.nonfiltered_replica_set(ts, should_be_local, cluster)
            .choose_filtered(&mut thread_rng(), |node| predicate(&node))
    }

    fn shuffled_replicas<'a>(
        &'a self,
        ts: &TokenWithStrategy<'a>,
        should_be_local: bool,
        predicate: impl Fn(&NodeRef<'a>) -> bool,
        cluster: &'a ClusterData,
    ) -> impl Iterator<Item = NodeRef<'a>> {
        let replicas = self.replicas(ts, should_be_local, predicate, cluster);

        Self::shuffle(replicas)
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

    fn shuffle<'a>(iter: impl Iterator<Item = NodeRef<'a>>) -> impl Iterator<Item = NodeRef<'a>> {
        let mut vec: Vec<NodeRef<'a>> = iter.collect();

        let mut rng = thread_rng();
        vec.shuffle(&mut rng);

        vec.into_iter()
    }

    fn is_alive(node: &NodeRef<'_>) -> bool {
        // For now, we leave this as stub, until we have time to improve node events.
        // node.is_enabled() && !node.is_down()
        node.is_enabled()
    }

    fn is_datacenter_failover_possible(&self, routing_info: &ProcessedRoutingInfo) -> bool {
        self.preferred_datacenter.is_some()
            && self.permit_dc_failover
            && !routing_info.local_consistency
    }
}

impl Default for DefaultPolicy {
    fn default() -> Self {
        Self {
            preferred_datacenter: None,
            is_token_aware: true,
            permit_dc_failover: false,
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
    preferred_datacenter: Option<String>,
    is_token_aware: bool,
    permit_dc_failover: bool,
}

impl DefaultPolicyBuilder {
    /// Creates a builder used to customise configuration of a new DefaultPolicy.
    pub fn new() -> Self {
        Self {
            preferred_datacenter: None,
            is_token_aware: true,
            permit_dc_failover: false,
        }
    }

    /// Builds a new DefaultPolicy with the previously set configuration.
    pub fn build(self) -> Arc<dyn LoadBalancingPolicy> {
        Arc::new(DefaultPolicy {
            preferred_datacenter: self.preferred_datacenter,
            is_token_aware: self.is_token_aware,
            permit_dc_failover: self.permit_dc_failover,
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
        self.preferred_datacenter = Some(datacenter_name);
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
    /// that falls within the token range of a particular node, the policy will try\
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
