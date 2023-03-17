use tracing::error;

use super::{FallbackPlan, LoadBalancingPolicy, NodeRef, RoutingInfo};
use crate::transport::ClusterData;

enum PlanState<'a> {
    Created,
    PickedNone, // This always means an abnormal situation: it means that no nodes satisfied locality/node filter requirements.
    Picked(NodeRef<'a>),
    Fallback {
        iter: FallbackPlan<'a>,
        node_to_filter_out: NodeRef<'a>,
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
    type Item = NodeRef<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        match &mut self.state {
            PlanState::Created => {
                let picked = self.policy.pick(self.routing_info, self.cluster);
                if let Some(picked) = picked {
                    self.state = PlanState::Picked(picked);
                    Some(picked)
                } else {
                    error!("Load balancing policy returned an empty plan! The query cannot be executed. Routing info: {:?}", self.routing_info);
                    self.state = PlanState::PickedNone;
                    None
                }
            }
            PlanState::Picked(node) => {
                self.state = PlanState::Fallback {
                    iter: self.policy.fallback(self.routing_info, self.cluster),
                    node_to_filter_out: node,
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
