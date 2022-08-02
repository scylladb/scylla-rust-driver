use super::{LoadBalancingPlan, RandomOrderPlan, TriedNodesSet};
use crate::{
    routing::Token,
    transport::{ClusterData, Node},
};
use std::sync::Arc;

pub enum SimpleStrategyPlan<'a> {
    Replicas {
        replicas_plan: RandomOrderPlan<'a>,
        tried_nodes: TriedNodesSet,
        all_nodes: &'a [Arc<Node>],
    },
    OtherNodes {
        other_nodes_plan: RandomOrderPlan<'a>,
    },
}

impl<'a> SimpleStrategyPlan<'a> {
    pub fn new(
        token_opt: &Option<Token>,
        replication_factor: usize,
        cluster_data: &'a ClusterData,
    ) -> SimpleStrategyPlan<'a> {
        let token: Token = token_opt.as_ref().copied().unwrap_or_else(Token::random);
        let replicas_iter = cluster_data.get_simple_strategy_replicas(token, replication_factor);
        let replicas_plan = RandomOrderPlan::from_iter(replicas_iter);
        SimpleStrategyPlan::Replicas {
            replicas_plan,
            tried_nodes: TriedNodesSet::new(),
            all_nodes: &cluster_data.all_nodes,
        }
    }
}

impl<'a> LoadBalancingPlan<'a> for SimpleStrategyPlan<'a> {
    fn next(&mut self) -> Option<&'a Arc<Node>> {
        match self {
            SimpleStrategyPlan::Replicas {
                replicas_plan,
                tried_nodes,
                all_nodes,
            } => {
                if let Some(next_node) = replicas_plan.next() {
                    tried_nodes.insert(next_node);
                    return Some(next_node);
                }

                // No more replica nodes to try, try other nodes
                let other_nodes_plan = RandomOrderPlan::from_iter(
                    all_nodes.iter().filter(|n| !tried_nodes.contains(n)),
                );
                *self = SimpleStrategyPlan::OtherNodes { other_nodes_plan };
                self.next()
            }
            SimpleStrategyPlan::OtherNodes { other_nodes_plan } => other_nodes_plan.next(),
        }
    }
}
