use super::{FallbackPlan, LoadBalancingPolicy, NodeRef, RoutingInfo};
use crate::{
    routing::Token,
    transport::{cluster::ClusterData, locator::ReplicaSet, node::Node, topology::Strategy},
};
use itertools::{Either, Itertools};
use rand::{prelude::SliceRandom, thread_rng, Rng};
use scylla_cql::{errors::QueryError, frame::types::SerialConsistency, Consistency};
use std::{fmt, sync::Arc, time::Duration};
use tracing::warn;

// TODO: LWT optimisation
/// The default load balancing policy.
///
/// It can be configured to be datacenter-aware and token-aware.
/// Datacenter failover for queries with non local consistency mode is also supported.
pub struct DefaultPolicy {
    preferred_datacenter: Option<String>,
    is_token_aware: bool,
    permit_dc_failover: bool,
    pick_predicate: Box<dyn Fn(&NodeRef) -> bool + Send + Sync>,
}

impl fmt::Debug for DefaultPolicy {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DefaultPolicy")
            .field("preferred_datacenter", &self.preferred_datacenter)
            .field("is_token_aware", &self.is_token_aware)
            .field("permit_dc_failover", &self.permit_dc_failover)
            .finish_non_exhaustive()
    }
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
            let picked = self.pick_replica(ts, true, &self.pick_predicate, cluster);

            if let Some(alive_local_replica) = picked {
                return Some(alive_local_replica);
            }

            // If datacenter failover is possible, loosen restriction about locality.
            if self.is_datacenter_failover_possible(&routing_info) {
                let picked = self.pick_replica(ts, false, &self.pick_predicate, cluster);
                if let Some(alive_remote_replica) = picked {
                    return Some(alive_remote_replica);
                }
            }
        };

        // If no token was available (or all the replicas for that token are down), try to pick
        // some alive local node.
        // If there was no preferred datacenter specified, all nodes are treated as local.
        let nodes = self.preferred_node_set(cluster);
        let picked = Self::pick_node(nodes, &self.pick_predicate);
        if let Some(alive_local) = picked {
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

    fn on_query_success(
        &self,
        _routing_info: &RoutingInfo,
        _latency: Duration,
        _node: NodeRef<'_>,
    ) {
    }

    fn on_query_failure(
        &self,
        _routing_info: &RoutingInfo,
        _latency: Duration,
        _node: NodeRef<'_>,
        _error: &QueryError,
    ) {
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
        predicate: &impl Fn(&NodeRef<'a>) -> bool,
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
            pick_predicate: Box::new(Self::is_alive),
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
        let pick_predicate = Box::new(DefaultPolicy::is_alive);

        Arc::new(DefaultPolicy {
            preferred_datacenter: self.preferred_datacenter,
            is_token_aware: self.is_token_aware,
            permit_dc_failover: self.permit_dc_failover,
            pick_predicate,
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

#[cfg(test)]
mod tests {
    use scylla_cql::{frame::types::SerialConsistency, Consistency};

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
    use std::collections::HashSet;

    use self::framework::{
        assert_proper_grouping_in_plan, get_plan_and_collect_node_identifiers,
        mock_cluster_data_for_token_unaware_tests, ExpectedGroupsBuilder,
    };

    use super::DefaultPolicy;

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
        pub(crate) struct ExpectedGroupsBuilder {
            groups: Vec<HashSet<u16>>,
        }

        impl ExpectedGroupsBuilder {
            pub(crate) fn new() -> Self {
                Self { groups: Vec::new() }
            }
            pub(crate) fn group(mut self, group: impl IntoIterator<Item = u16>) -> Self {
                self.groups.push(group.into_iter().collect());
                self
            }
            pub(crate) fn build(self) -> Vec<HashSet<u16>> {
                self.groups
            }
        }

        pub(crate) fn assert_proper_grouping_in_plan(
            got: &Vec<u16>,
            expected_groups: &Vec<HashSet<u16>>,
        ) {
            // First, make sure that `got` has the right number of items,
            // equal to the sum of sizes of all expected groups
            let combined_groups_len = expected_groups.iter().map(|s| s.len()).sum();
            assert_eq!(got.len(), combined_groups_len);

            // Now, split `got` into groups of expected sizes
            // and just `assert_eq` them
            let mut got = got.iter();
            let got_groups = expected_groups
                .iter()
                .map(|s| (&mut got).take(s.len()).copied().collect::<HashSet<u16>>())
                .collect::<Vec<_>>();

            assert_eq!(&got_groups, expected_groups);
        }

        #[test]
        fn test_assert_proper_grouping_in_plan_good() {
            let got = vec![1u16, 2, 3, 4, 5];
            let expected_groups = ExpectedGroupsBuilder::new()
                .group([1])
                .group([3, 2, 4])
                .group([5])
                .build();

            assert_proper_grouping_in_plan(&got, &expected_groups);
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

            assert_proper_grouping_in_plan(&got, &expected_groups);
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

            assert_proper_grouping_in_plan(&got, &expected_groups);
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

            assert_proper_grouping_in_plan(&got, &expected_groups);
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
        expected_groups: &Vec<HashSet<u16>>,
    ) {
        for _ in 0..16 {
            let plan = get_plan_and_collect_node_identifiers(policy, routing_info, cluster);
            assert_proper_grouping_in_plan(&plan, expected_groups);
        }
    }

    async fn test_given_default_policy_with_token_unaware_statements(
        policy: DefaultPolicy,
        expected_groups: &Vec<HashSet<u16>>,
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
            preferred_datacenter: Some(local_dc.clone()),
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
            preferred_datacenter: Some(local_dc),
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
        let _ = tracing_subscriber::fmt::try_init();
        use crate::transport::locator::test::{A, B, C, D, E, F, G};

        let cluster = mock_cluster_data_for_token_aware_tests().await;
        struct Test<'a> {
            policy: DefaultPolicy,
            routing_info: RoutingInfo<'a>,
            expected_groups: Vec<HashSet<u16>>,
        }

        let tests = [
            // Keyspace NTS with RF=2 with enabled DC failover
            Test {
                policy: DefaultPolicy {
                    preferred_datacenter: Some("eu".to_owned()),
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
                // going though the ring, we get order: F , A , C , D , G , B , E
                //                                      us  eu  eu  us  eu  eu  us
                //                                      r2  r1  r1  r1  r2  r1  r1
                expected_groups: ExpectedGroupsBuilder::new()
                    .group([A, G]) // pick + fallback local replicas
                    .group([F, D]) // remote replicas
                    .group([C, B]) // local nodes
                    .group([E]) // remote nodes
                    .build(),
            },
            // Keyspace NTS with RF=2 with DC failover forbidden by local Consistency
            Test {
                policy: DefaultPolicy {
                    preferred_datacenter: Some("eu".to_owned()),
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
                // going though the ring, we get order: F , A , C , D , G , B , E
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
                    preferred_datacenter: Some("eu".to_owned()),
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
                // going though the ring, we get order: F , A , C , D , G , B , E
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
                    preferred_datacenter: Some("eu".to_owned()),
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
                // going though the ring, we get order: F , A , C , D , G , B , E
                //                                      us  eu  eu  us  eu  eu  us
                //                                      r2  r1  r1  r1  r2  r1  r1
                expected_groups: ExpectedGroupsBuilder::new()
                    .group([A, C, G]) // pick + fallback local replicas
                    .group([F, D, E]) // remote replicas
                    .group([B]) // local nodes
                    .group([]) // remote nodes
                    .build(),
            },
            // Keyspace NTS with RF=3 with disabled DC failover
            Test {
                policy: DefaultPolicy {
                    preferred_datacenter: Some("eu".to_owned()),
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
                // going though the ring, we get order: F , A , C , D , G , B , E
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
                    preferred_datacenter: Some("eu".to_owned()),
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
                // going though the ring, we get order: F , A , C , D , G , B , E
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
                    preferred_datacenter: Some("eu".to_owned()),
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
                // going though the ring, we get order: F , A , C , D , G , B , E
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
                    preferred_datacenter: Some("eu".to_owned()),
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
                    preferred_datacenter: Some("eu".to_owned()),
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
                    preferred_datacenter: Some("au".to_owned()),
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
                // going though the ring, we get order: F , A , C , D , G , B , E
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
                    preferred_datacenter: Some("au".to_owned()),
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
                // going though the ring, we get order: F , A , C , D , G , B , E
                //                                      us  eu  eu  us  eu  eu  us
                //                                      r2  r1  r1  r1  r2  r1  r1
                expected_groups: ExpectedGroupsBuilder::new().build(), // empty plan, because all nodes are remote and failover is forbidden
            },
            // No preferred DC, failover permitted
            Test {
                policy: DefaultPolicy {
                    preferred_datacenter: None,
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
                // going though the ring, we get order: F , A , C , D , G , B , E
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
                    preferred_datacenter: None,
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
                // going though the ring, we get order: F , A , C , D , G , B , E
                //                                      us  eu  eu  us  eu  eu  us
                //                                      r2  r1  r1  r1  r2  r1  r1
                expected_groups: ExpectedGroupsBuilder::new()
                    .group([A, D, F, G]) // remote replicas
                    .group([B, C, E]) // remote nodes
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
