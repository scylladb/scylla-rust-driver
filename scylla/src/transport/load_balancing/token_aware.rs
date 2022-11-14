use super::{ChildLoadBalancingPolicy, LoadBalancingPolicy, Plan, Statement};
use crate::routing::Token;
use crate::transport::node::NodeAddr;
use crate::transport::topology::Strategy;
use crate::transport::{cluster::ClusterData, node::Node};
use itertools::Itertools;
use std::collections::HashSet;
use std::{collections::HashMap, sync::Arc};
use tracing::trace;

/// A wrapper load balancing policy that adds token awareness to a child policy.
#[derive(Debug)]
pub struct TokenAwarePolicy {
    child_policy: Box<dyn ChildLoadBalancingPolicy>,
}

impl TokenAwarePolicy {
    pub fn new(child_policy: Box<dyn ChildLoadBalancingPolicy>) -> Self {
        Self { child_policy }
    }

    fn simple_strategy_replicas(
        cluster: &ClusterData,
        token: &Token,
        replication_factor: usize,
    ) -> Vec<Arc<Node>> {
        cluster
            .ring_range(token)
            .unique()
            .take(replication_factor)
            .collect()
    }

    fn rack_count_in_dc(dc_name: &str, cluster: &ClusterData) -> Option<usize> {
        let nodes_in_that_dc = cluster
            .replica_locator()
            .unique_nodes_in_datacenter_ring(dc_name)?;

        let count = nodes_in_that_dc
            .iter()
            .map(|node| &node.rack)
            .unique()
            .count();

        Some(count)
    }

    fn network_topology_strategy_replicas(
        cluster: &ClusterData,
        token: &Token,
        datacenter_repfactors: &HashMap<String, usize>,
    ) -> Vec<Arc<Node>> {
        let mut acceptable_repeats = datacenter_repfactors
            .iter()
            .map(|(dc_name, repfactor)| {
                let rack_count = Self::rack_count_in_dc(dc_name, cluster).unwrap_or(0);

                (dc_name.as_str(), repfactor.saturating_sub(rack_count))
            })
            .collect::<HashMap<&str, usize>>();

        let desired_result_len: usize = datacenter_repfactors.values().sum();

        let mut result: Vec<Arc<Node>> = Vec::with_capacity(desired_result_len);
        for node in cluster.ring_range(token).unique() {
            let current_node_dc = match &node.datacenter {
                None => continue,
                Some(dc) => dc,
            };

            let repfactor = match datacenter_repfactors.get(current_node_dc) {
                None => continue,
                Some(r) => r,
            };

            let picked_nodes_from_current_dc = || {
                result
                    .iter()
                    .filter(|node| node.datacenter.as_ref() == Some(current_node_dc))
            };

            if *repfactor == picked_nodes_from_current_dc().count() {
                // found enough nodes in this datacenter
                continue;
            }

            let current_node_rack = node.rack.as_ref();
            let current_node_rack_count = picked_nodes_from_current_dc()
                .filter(|node| node.rack.as_ref() == current_node_rack)
                .count();

            if current_node_rack_count == 0 {
                // new rack
                result.push(node.clone());
            } else {
                // we’ve already found a node in this rack

                // unwrap, because we already know repfactor
                let repeats = acceptable_repeats
                    .get_mut(current_node_dc.as_str())
                    .unwrap();
                if *repeats > 0 {
                    // we must pick multiple nodes in the same rack
                    *repeats -= 1;
                    result.push(node.clone());
                }
            }

            if result.len() == desired_result_len {
                break;
            }
        }

        result
    }

    pub(crate) fn replicas_for_token(
        cluster: &ClusterData,
        token: &Token,
        keyspace_name: Option<&str>,
    ) -> Vec<Arc<Node>> {
        let keyspace = keyspace_name.and_then(|k| cluster.keyspaces.get(k));

        let strategy = keyspace.map(|k| &k.strategy);

        match strategy {
            Some(Strategy::SimpleStrategy { replication_factor }) => {
                Self::simple_strategy_replicas(cluster, token, *replication_factor)
            }
            Some(Strategy::NetworkTopologyStrategy {
                datacenter_repfactors,
            }) => Self::network_topology_strategy_replicas(cluster, token, datacenter_repfactors),
            _ => {
                // default to simple strategy with replication factor = 1
                let replication_factor = 1;
                Self::simple_strategy_replicas(cluster, token, replication_factor)
            }
        }
    }
}

impl LoadBalancingPolicy for TokenAwarePolicy {
    fn plan<'a>(&self, statement: &Statement, cluster: &'a ClusterData) -> Plan<'a> {
        match statement.token {
            Some(token) => {
                let replicas = Self::replicas_for_token(cluster, &token, statement.keyspace);
                trace!(
                    token = token.value,
                    replicas = replicas
                        .iter()
                        .map(|node| node.address.to_string())
                        .collect::<Vec<String>>()
                        .join(",")
                        .as_str(),
                    "TokenAware"
                );

                let fallback_plan = {
                    let replicas_set: HashSet<NodeAddr> =
                        replicas.iter().map(|node| node.address).collect();

                    self.child_policy
                        .plan(&Statement::empty(), cluster)
                        .filter(move |node| !replicas_set.contains(&node.address))
                };

                if statement.is_confirmed_lwt {
                    // As optimisation, in order to reduce contention caused by Paxos conflicts, we always try
                    // to query replicas in the same order. Therefore, we bypass child load balancing policy.
                    Box::new(replicas.into_iter().chain(fallback_plan))
                } else {
                    Box::new(
                        self.child_policy
                            .apply_child_policy(replicas)
                            .chain(fallback_plan),
                    )
                }
            }
            // fallback to child policy
            None => {
                trace!("TokenAware: falling back to child policy, no token present");
                self.child_policy.plan(statement, cluster)
            }
        }
    }

    fn name(&self) -> String {
        format!(
            "TokenAwarePolicy{{child_policy: {}}}",
            self.child_policy.name()
        )
    }
}
