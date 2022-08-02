use std::sync::Arc;

use crate::{
    routing::Token,
    transport::{ClusterData, Node},
};

use super::{LoadBalancingPlan, RandomOrderPlan, TriedNodesSet};

pub enum NoTokenNetworkStrategyPlan<'a> {
    LocalRandomTokenReplicas {
        local_dc: &'a str,
        local_replicas_plan: RandomOrderPlan<'a>,
        tried_local_replicas: TriedNodesSet,
        cluster_data: &'a ClusterData,
    },
    LocalOthers {
        local_dc: &'a str,
        local_others_plan: RandomOrderPlan<'a>,
        cluster_data: &'a ClusterData,
    },
    OtherNodes {
        others_plan: RandomOrderPlan<'a>,
    },
}

impl<'a> NoTokenNetworkStrategyPlan<'a> {
    pub fn new(local_dc: &'a str, cluster_data: &'a ClusterData) -> NoTokenNetworkStrategyPlan<'a> {
        let local_replicas_iter =
            cluster_data.get_network_strategy_replicas(Token::random(), local_dc, 3);
        let local_replicas_plan = RandomOrderPlan::from_iter(local_replicas_iter);
        NoTokenNetworkStrategyPlan::LocalRandomTokenReplicas {
            local_dc,
            local_replicas_plan,
            tried_local_replicas: TriedNodesSet::new(),
            cluster_data,
        }
    }
}

impl<'a> LoadBalancingPlan<'a> for NoTokenNetworkStrategyPlan<'a> {
    fn next(&mut self) -> Option<&'a Arc<Node>> {
        match self {
            NoTokenNetworkStrategyPlan::LocalRandomTokenReplicas {
                local_dc,
                local_replicas_plan,
                tried_local_replicas,
                cluster_data,
            } => {
                if let Some(next_replica) = local_replicas_plan.next() {
                    tried_local_replicas.insert(next_replica);
                    return Some(next_replica);
                }

                let local_nodes: &[Arc<Node>] = cluster_data
                    .get_datacenters_info()
                    .get(*local_dc)
                    .map(|dc| dc.nodes.as_slice())
                    .unwrap_or_default();
                let local_others_iter = local_nodes
                    .iter()
                    .filter(|n| !tried_local_replicas.contains(n));
                let local_others_plan = RandomOrderPlan::from_iter(local_others_iter);
                *self = NoTokenNetworkStrategyPlan::LocalOthers {
                    local_dc,
                    local_others_plan,
                    cluster_data,
                };
                self.next()
            }
            NoTokenNetworkStrategyPlan::LocalOthers {
                local_dc,
                local_others_plan,
                cluster_data,
            } => {
                if let Some(next_local_other) = local_others_plan.next() {
                    return Some(next_local_other);
                }

                let others_iter = cluster_data
                    .get_nodes_info()
                    .iter()
                    .filter(|n| n.datacenter.as_deref() != Some(local_dc));
                let others_plan = RandomOrderPlan::from_iter(others_iter);
                *self = NoTokenNetworkStrategyPlan::OtherNodes { others_plan };
                self.next()
            }
            NoTokenNetworkStrategyPlan::OtherNodes { others_plan } => others_plan.next(),
        }
    }
}
