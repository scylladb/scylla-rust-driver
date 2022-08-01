use super::LoadBalancingData;
use super::TokenRing;
use crate::routing::Token;
use crate::transport::node::Node;
use crate::transport::topology::Strategy;
use std::borrow::Borrow;
use std::collections::HashMap;
use std::sync::Arc;

pub type Replicas = Vec<Arc<Node>>;

#[derive(Clone)]
pub struct PrecomputedReplicas {
    global_replicas: PrecomputedReplicasRing,
    datacenter_replicas: HashMap<String, PrecomputedReplicasRing>,
}

#[derive(Clone)]
struct PrecomputedReplicasRing {
    pub replicas_for_token: TokenRing<Replicas>,
    pub max_rep_factor: usize,
}

impl PrecomputedReplicas {
    pub fn compute<'a>(
        lb_data: &LoadBalancingData,
        keyspace_strategies: impl Iterator<Item = &'a Strategy>,
    ) -> PrecomputedReplicas {
        // Each ring will precompute for at least this RF
        let min_precomputed_rep_factor: usize = 3;

        let mut max_global_repfactor: usize = min_precomputed_rep_factor;
        let mut max_datacener_repfactors: HashMap<&'a str, usize> = HashMap::new();

        for strategy in keyspace_strategies {
            match strategy {
                Strategy::SimpleStrategy { replication_factor } => {
                    max_global_repfactor = std::cmp::max(max_global_repfactor, *replication_factor)
                }
                Strategy::NetworkTopologyStrategy {
                    datacenter_repfactors,
                } => {
                    for (dc_name, dc_repfactor) in datacenter_repfactors {
                        let max_repfactor: &mut usize = max_datacener_repfactors
                            .entry(dc_name)
                            .or_insert(min_precomputed_rep_factor);
                        *max_repfactor = std::cmp::max(*max_repfactor, *dc_repfactor);
                    }
                }
                Strategy::LocalStrategy => {} // RF=1
                Strategy::Other { .. } => {}  // Can't precompute for custom strategies
            }
        }

        let global_replicas_iter = lb_data.global_ring.iter().map(|(token, _)| {
            let cur_replicas: Replicas = lb_data
                .get_global_replicas_for_token(token, max_global_repfactor)
                .cloned()
                .collect();
            (*token, cur_replicas)
        });
        let global_replicas = PrecomputedReplicasRing {
            replicas_for_token: TokenRing::new(global_replicas_iter),
            max_rep_factor: max_global_repfactor,
        };

        let mut datacenter_replicas: HashMap<String, PrecomputedReplicasRing> = HashMap::new();
        for (dc_name, max_dc_repfactor) in max_datacener_repfactors {
            let dc_lb_data = match lb_data.datacenters.get(dc_name) {
                Some(dc_lb_data) => dc_lb_data,
                None => continue,
            };

            let dc_replicas_iter = dc_lb_data.dc_ring.iter().map(|(token, _)| {
                let cur_replicas: Replicas = lb_data
                    .get_datacenter_replicas_for_token(token, dc_name, max_dc_repfactor)
                    .cloned()
                    .collect();
                (*token, cur_replicas)
            });
            let dc_replicas = PrecomputedReplicasRing {
                replicas_for_token: TokenRing::new(dc_replicas_iter),
                max_rep_factor: max_dc_repfactor,
            };
            datacenter_replicas.insert(dc_name.to_string(), dc_replicas);
        }

        PrecomputedReplicas {
            global_replicas,
            datacenter_replicas,
        }
    }

    pub fn get_precomputed_simple_strategy_replicas(
        &self,
        token: impl Borrow<Token>,
        replication_factor: usize,
    ) -> Option<&[Arc<Node>]> {
        if replication_factor > self.global_replicas.max_rep_factor {
            return None;
        }

        if let Some(precomputed_replicas) = self
            .global_replicas
            .replicas_for_token
            .get_elem_for_token(token)
        {
            let result_len: usize = std::cmp::min(precomputed_replicas.len(), replication_factor);
            return Some(&precomputed_replicas[..result_len]);
        }

        None
    }

    pub fn get_precomputed_network_strategy_replicas(
        &self,
        token: impl Borrow<Token>,
        dc_name: &str,
        dc_replication_factor: usize,
    ) -> Option<&[Arc<Node>]> {
        if let Some(precomputed_dc_replicas) = self.datacenter_replicas.get(dc_name) {
            if dc_replication_factor > precomputed_dc_replicas.max_rep_factor {
                return None;
            }

            if let Some(precomputed_replicas) = precomputed_dc_replicas
                .replicas_for_token
                .get_elem_for_token(token)
            {
                let result_len: usize =
                    std::cmp::min(precomputed_replicas.len(), dc_replication_factor);
                return Some(&precomputed_replicas[..result_len]);
            }
        }

        None
    }
}
