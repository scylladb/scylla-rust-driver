use tracing::error;

use super::{FallbackPlan, LoadBalancingPolicy, NodeRef, RoutingInfo};
use crate::{routing::Shard, transport::ClusterData};

enum PlanState<'a> {
    Created,
    PickedNone, // This always means an abnormal situation: it means that no nodes satisfied locality/node filter requirements.
    Picked((NodeRef<'a>, Shard)),
    Fallback {
        iter: FallbackPlan<'a>,
        node_to_filter_out: (NodeRef<'a>, Shard),
    },
}

/// The list of nodes constituting the query plan.
///
/// The plan is partly lazily computed, with the first node computed
/// eagerly in the first place and the remaining nodes computed on-demand
/// (all at once).
/// This significantly reduces the allocation overhead on "the happy path"
/// (when the first node successfully handles the request),
pub struct Plan<'a> {
    policy: &'a dyn LoadBalancingPolicy,
    routing_info: &'a RoutingInfo<'a>,
    cluster: &'a ClusterData,

    state: PlanState<'a>,
}

impl<'a> Plan<'a> {
    pub fn new(
        policy: &'a dyn LoadBalancingPolicy,
        routing_info: &'a RoutingInfo<'a>,
        cluster: &'a ClusterData,
    ) -> Self {
        Self {
            policy,
            routing_info,
            cluster,
            state: PlanState::Created,
        }
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
                    Some(picked)
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
                            node_to_filter_out: node,
                        };
                        Some(node)
                    } else {
                        error!("Load balancing policy returned an empty plan! The query cannot be executed. Routing info: {:?}", self.routing_info);
                        self.state = PlanState::PickedNone;
                        None
                    }
                }
            }
            PlanState::Picked(node) => {
                self.state = PlanState::Fallback {
                    iter: self.policy.fallback(self.routing_info, self.cluster),
                    node_to_filter_out: *node,
                };

                self.next()
            }
            PlanState::Fallback {
                iter,
                node_to_filter_out,
            } => {
                for node in iter {
                    if node == *node_to_filter_out {
                        continue;
                    } else {
                        return Some(node);
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
        test_utils::setup_tracing,
        transport::{
            locator::test::{create_locator, mock_metadata_for_token_aware_tests},
            Node, NodeAddr,
        },
    };

    use super::*;

    fn expected_nodes() -> Vec<(Arc<Node>, Shard)> {
        vec![(
            Arc::new(Node::new_for_test(
                NodeAddr::Translatable(SocketAddr::from_str("127.0.0.1:9042").unwrap()),
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
            _cluster: &'a ClusterData,
        ) -> Option<(NodeRef<'a>, Shard)> {
            None
        }

        fn fallback<'a>(
            &'a self,
            _query: &'a RoutingInfo,
            _cluster: &'a ClusterData,
        ) -> FallbackPlan<'a> {
            Box::new(
                self.expected_nodes
                    .iter()
                    .map(|(node_ref, shard)| (node_ref, *shard)),
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
        let cluster_data = ClusterData {
            known_peers: Default::default(),
            keyspaces: Default::default(),
            locator,
        };
        let routing_info = RoutingInfo::default();
        let plan = Plan::new(&policy, &routing_info, &cluster_data);
        assert_eq!(
            Vec::from_iter(plan.map(|(node, shard)| (node.clone(), shard))),
            policy.expected_nodes
        );
    }
}
