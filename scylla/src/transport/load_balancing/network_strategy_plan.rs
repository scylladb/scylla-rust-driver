use std::{collections::HashMap, sync::Arc};

use crate::{
    routing::Token,
    transport::{ClusterData, Node},
};

use super::{LoadBalancingPlan, RandomOrderIter, RandomOrderPlan, TriedNodesSet};

pub enum NetworkStrategyPlan<'a> {
    LocalReplicas {
        local_replicas_plan: RandomOrderPlan<'a>,
        tried_nodes: TriedNodesSet,
        token: Token,
        local_dc: &'a str,
        datacenter_repfactors: &'a HashMap<String, usize>,
        cluster_data: &'a ClusterData,
    },
    RemoteReplicas {
        token: Token,
        local_dc: &'a str,
        dcs_to_try: RandomOrderIter<(&'a str, usize)>,
        cur_dc_replicas_plan: RandomOrderPlan<'a>,
        tried_nodes: TriedNodesSet,
        cluster_data: &'a ClusterData,
    },
    LocalOtherNodes {
        cluster_data: &'a ClusterData,
        tried_nodes: TriedNodesSet,
        local_other_plan: RandomOrderPlan<'a>,
    },
    RemoteOtherNodes {
        remote_other_plan: RandomOrderPlan<'a>,
    },
}

impl<'a> NetworkStrategyPlan<'a> {
    pub fn new(
        token: Token,
        datacenter_repfactors: &'a HashMap<String, usize>,
        local_dc: &'a str,
        cluster_data: &'a ClusterData,
    ) -> NetworkStrategyPlan<'a> {
        let local_dc_repfactor: usize = datacenter_repfactors.get(local_dc).copied().unwrap_or(0);
        let local_replicas_iter =
            cluster_data.get_network_strategy_replicas(token, local_dc, local_dc_repfactor);
        let local_replicas_plan = RandomOrderPlan::from_iter(local_replicas_iter);
        NetworkStrategyPlan::LocalReplicas {
            local_replicas_plan,
            tried_nodes: TriedNodesSet::new(),
            token,
            local_dc,
            datacenter_repfactors,
            cluster_data,
        }
    }
}

impl<'a> LoadBalancingPlan<'a> for NetworkStrategyPlan<'a> {
    fn next(&mut self) -> Option<&'a Arc<Node>> {
        match self {
            NetworkStrategyPlan::LocalReplicas {
                local_replicas_plan,
                tried_nodes,
                token,
                local_dc,
                datacenter_repfactors,
                cluster_data,
            } => {
                if let Some(next_node) = local_replicas_plan.next() {
                    tried_nodes.insert(next_node);
                    return Some(next_node);
                }

                let other_dcs_iter = datacenter_repfactors
                    .iter()
                    .filter(|(dc_name, _)| dc_name != local_dc)
                    .map(|(dc_name, dc_repfactor)| (dc_name.as_str(), *dc_repfactor));
                let dcs_to_try = RandomOrderIter::from_iter(other_dcs_iter);
                *self = NetworkStrategyPlan::RemoteReplicas {
                    token: *token,
                    local_dc,
                    tried_nodes: std::mem::take(tried_nodes),
                    cur_dc_replicas_plan: RandomOrderPlan::from_iter([].iter()),
                    dcs_to_try,
                    cluster_data,
                };
                self.next()
            }
            NetworkStrategyPlan::RemoteReplicas {
                token,
                local_dc,
                tried_nodes,
                cur_dc_replicas_plan,
                dcs_to_try,
                cluster_data,
            } => {
                if let Some(next_node) = cur_dc_replicas_plan.next() {
                    tried_nodes.insert(next_node);
                    return Some(next_node);
                }

                if let Some((next_dc_name, next_dc_repfactor)) = dcs_to_try.next() {
                    let next_dc_replicas_iter = cluster_data.get_network_strategy_replicas(
                        token,
                        next_dc_name,
                        next_dc_repfactor,
                    );
                    let next_dc_replicas_plan = RandomOrderPlan::from_iter(next_dc_replicas_iter);
                    *cur_dc_replicas_plan = next_dc_replicas_plan;
                    return self.next();
                }

                let local_nodes = cluster_data
                    .datacenters
                    .get(*local_dc)
                    .map(|dc| dc.nodes.as_slice())
                    .unwrap_or(&[])
                    .iter();
                let local_other_nodes = local_nodes.filter(|n| !tried_nodes.contains(n));
                let local_other_plan = RandomOrderPlan::from_iter(local_other_nodes);
                *self = NetworkStrategyPlan::LocalOtherNodes {
                    tried_nodes: std::mem::take(tried_nodes),
                    local_other_plan,
                    cluster_data,
                };
                self.next()
            }
            NetworkStrategyPlan::LocalOtherNodes {
                tried_nodes,
                local_other_plan,
                cluster_data,
            } => {
                if let Some(next_node) = local_other_plan.next() {
                    tried_nodes.insert(next_node);
                    return Some(next_node);
                }

                let other_nodes_iter = cluster_data
                    .all_nodes
                    .iter()
                    .filter(|n| !tried_nodes.contains(n));
                let remote_other_plan = RandomOrderPlan::from_iter(other_nodes_iter);

                *self = NetworkStrategyPlan::RemoteOtherNodes { remote_other_plan };
                self.next()
            }
            NetworkStrategyPlan::RemoteOtherNodes { remote_other_plan } => remote_other_plan.next(),
        }
    }
}
