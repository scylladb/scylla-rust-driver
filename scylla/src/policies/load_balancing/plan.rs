use rand::{Rng, rng};
use tracing::error;

use super::{FallbackPlan, LoadBalancingPolicy, NodeRef, RoutingInfo};
use crate::cluster::ClusterState;
use crate::routing::Shard;

enum PlanState<'a> {
    Created,
    PickedNone, // This always means an abnormal situation: it means that no nodes satisfied locality/node filter requirements.
    Picked((NodeRef<'a>, Option<Shard>)),
    Fallback {
        iter: FallbackPlan<'a>,
        target_to_filter_out: (NodeRef<'a>, Option<Shard>),
    },
}

/// The list of targets constituting the query plan. Target here is a pair `(NodeRef<'a>, Shard)`.
///
/// The plan is partly lazily computed, with the first target computed
/// eagerly in the first place and the remaining targets computed on-demand
/// (all at once).
/// This significantly reduces the allocation overhead on "the happy path"
/// (when the first target successfully handles the request).
///
/// `Plan` implements `Iterator<Item=(NodeRef<'a>, Shard)>` but LoadBalancingPolicy
/// returns `Option<Shard>` instead of `Shard` both in `pick` and in `fallback`.
/// `Plan` handles the `None` case by using random shard for a given node.
/// There is currently no way to configure RNG used by `Plan`.
/// If you don't want `Plan` to do randomize shards or you want to control the RNG,
/// use custom LBP that will always return non-`None` shards.
/// Example of LBP that always uses shard 0, preventing `Plan` from using random numbers:
///
/// ```
/// # use std::sync::Arc;
/// # use scylla::cluster::NodeRef;
/// # use scylla::cluster::ClusterState;
/// # use scylla::policies::load_balancing::FallbackPlan;
/// # use scylla::policies::load_balancing::LoadBalancingPolicy;
/// # use scylla::policies::load_balancing::RoutingInfo;
/// # use scylla::routing::Shard;
///
/// #[derive(Debug)]
/// struct NonRandomLBP {
///     inner: Arc<dyn LoadBalancingPolicy>,
/// }
/// impl LoadBalancingPolicy for NonRandomLBP {
///     fn pick<'a>(
///         &'a self,
///         info: &'a RoutingInfo,
///         cluster: &'a ClusterState,
///     ) -> Option<(NodeRef<'a>, Option<Shard>)> {
///         self.inner
///             .pick(info, cluster)
///             .map(|(node, shard)| (node, shard.or(Some(0))))
///     }
///
///     fn fallback<'a>(&'a self, info: &'a RoutingInfo, cluster: &'a ClusterState) -> FallbackPlan<'a> {
///         Box::new(self.inner
///             .fallback(info, cluster)
///             .map(|(node, shard)| (node, shard.or(Some(0)))))
///     }
///
///     fn name(&self) -> String {
///         "NonRandomLBP".to_string()
///     }
/// }
/// ```
// TODO(2.0): Unpub this.
pub struct Plan<'a> {
    policy: &'a dyn LoadBalancingPolicy,
    routing_info: &'a RoutingInfo<'a>,
    cluster: &'a ClusterState,

    state: PlanState<'a>,
}

impl<'a> Plan<'a> {
    /// Asks the given [LoadBalancingPolicy] to compute a load balancing plan for the given `RoutingInfo`.
    // TODO(2.0): Unpub this.
    pub fn new(
        policy: &'a dyn LoadBalancingPolicy,
        routing_info: &'a RoutingInfo<'a>,
        cluster: &'a ClusterState,
    ) -> Self {
        Self {
            policy,
            routing_info,
            cluster,
            state: PlanState::Created,
        }
    }

    fn with_random_shard_if_unknown(
        (node, shard): (NodeRef<'_>, Option<Shard>),
    ) -> (NodeRef<'_>, Shard) {
        (
            node,
            shard.unwrap_or_else(|| {
                let nr_shards = node
                    .sharder()
                    .map(|sharder| sharder.nr_shards.get())
                    .unwrap_or(1);
                rng().random_range(0..nr_shards).into()
            }),
        )
    }
}

impl<'a> Iterator for Plan<'a> {
    type Item = (NodeRef<'a>, Shard);

    fn next(&mut self) -> Option<Self::Item> {
        match &mut self.state {
            PlanState::Created => {
                let picked = self.policy.pick(self.routing_info, self.cluster);
                if let Some(picked) = picked {
                    self.state = PlanState::Picked(picked);
                    Some(Self::with_random_shard_if_unknown(picked))
                } else {
                    // `pick()` returned None, which semantically means that a first node cannot be computed _cheaply_.
                    // This, however, does not imply that fallback would return an empty plan, too.
                    // For instance, as a side effect of LWT optimisation in Default Policy, pick() may return None
                    // when the primary replica is down. `fallback()` will nevertheless return the remaining replicas,
                    // if there are such.
                    let mut iter = self.policy.fallback(self.routing_info, self.cluster);
                    let first_fallback_node = iter.next();
                    if let Some(node) = first_fallback_node {
                        self.state = PlanState::Fallback {
                            iter,
                            target_to_filter_out: node,
                        };
                        Some(Self::with_random_shard_if_unknown(node))
                    } else {
                        error!(
                            "Load balancing policy returned an empty plan! The query cannot be executed. Routing info: {:?}",
                            self.routing_info
                        );
                        self.state = PlanState::PickedNone;
                        None
                    }
                }
            }
            PlanState::Picked(node) => {
                self.state = PlanState::Fallback {
                    iter: self.policy.fallback(self.routing_info, self.cluster),
                    target_to_filter_out: *node,
                };

                self.next()
            }
            PlanState::Fallback {
                iter,
                target_to_filter_out: node_to_filter_out,
            } => {
                for node in iter {
                    if node == *node_to_filter_out {
                        continue;
                    } else {
                        return Some(Self::with_random_shard_if_unknown(node));
                    }
                }

                None
            }
            PlanState::PickedNone => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{net::SocketAddr, str::FromStr, sync::Arc};

    use crate::{
        cluster::{Node, NodeAddr},
        routing::locator::test::{create_locator, mock_metadata_for_token_aware_tests},
        test_utils::setup_tracing,
    };

    use super::*;

    fn expected_nodes() -> Vec<(Arc<Node>, Shard)> {
        vec![(
            Arc::new(Node::new_for_test(
                None,
                Some(NodeAddr::Translatable(
                    SocketAddr::from_str("127.0.0.1:9042").unwrap(),
                )),
                None,
                None,
            )),
            42,
        )]
    }

    #[derive(Debug)]
    struct PickingNonePolicy {
        expected_nodes: Vec<(Arc<Node>, Shard)>,
    }
    impl LoadBalancingPolicy for PickingNonePolicy {
        fn pick<'a>(
            &'a self,
            _query: &'a RoutingInfo,
            _cluster: &'a ClusterState,
        ) -> Option<(NodeRef<'a>, Option<Shard>)> {
            None
        }

        fn fallback<'a>(
            &'a self,
            _query: &'a RoutingInfo,
            _cluster: &'a ClusterState,
        ) -> FallbackPlan<'a> {
            Box::new(
                self.expected_nodes
                    .iter()
                    .map(|(node_ref, shard)| (node_ref, Some(*shard))),
            )
        }

        fn name(&self) -> String {
            "PickingNone".into()
        }
    }

    #[tokio::test]
    async fn plan_calls_fallback_even_if_pick_returned_none() {
        setup_tracing();
        let policy = PickingNonePolicy {
            expected_nodes: expected_nodes(),
        };
        let locator = create_locator(&mock_metadata_for_token_aware_tests());
        let cluster_state = ClusterState {
            known_peers: Default::default(),
            all_nodes: Default::default(),
            keyspaces: Default::default(),
            locator,
        };
        let routing_info = RoutingInfo::default();
        let plan = Plan::new(&policy, &routing_info, &cluster_state);
        assert_eq!(
            Vec::from_iter(plan.map(|(node, shard)| (node.clone(), shard))),
            policy.expected_nodes
        );
    }
}
