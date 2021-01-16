use super::{cluster::ClusterData, node::Node};
use crate::routing::Token;

use core::ops::Bound::{Included, Unbounded};
use std::{
    collections::VecDeque,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
};

const ORDER_TYPE: Ordering = Ordering::Relaxed;

pub struct Statement {
    pub token: Option<Token>,
    pub keyspace: Option<String>,
}

pub trait LoadBalancingPolicy: Send + Sync {
    fn plan<'a>(
        &self,
        statement: &Statement,
        cluster: &'a ClusterData,
    ) -> Box<dyn Iterator<Item = Arc<Node>> + 'a>;
}

pub trait InternalLoadBalancingPolicy: LoadBalancingPolicy {
    fn apply_policy_for_plan(
        &self,
        plan: &mut dyn Iterator<Item = Arc<Node>>,
    ) -> Box<dyn Iterator<Item = Arc<Node>>>;
}

pub struct RoundRobin {
    index: AtomicUsize,
}

impl RoundRobin {
    pub fn new() -> Self {
        RoundRobin {
            index: AtomicUsize::new(0),
        }
    }
}

impl LoadBalancingPolicy for RoundRobin {
    fn plan<'a>(
        &self,
        _statement: &Statement,
        cluster: &'a ClusterData,
    ) -> Box<dyn Iterator<Item = Arc<Node>> + 'a> {
        let index = self.index.fetch_add(1, ORDER_TYPE);

        let number_of_nodes = cluster.known_peers.len();

        let iter = cluster
            .known_peers
            .values()
            .cloned()
            .cycle()
            .skip(index % number_of_nodes)
            .take(number_of_nodes);

        Box::new(iter)
    }
}

impl InternalLoadBalancingPolicy for RoundRobin {
    fn apply_policy_for_plan(
        &self,
        plan: &mut dyn Iterator<Item = Arc<Node>>,
    ) -> Box<dyn Iterator<Item = Arc<Node>>> {
        let index = self.index.fetch_add(1, ORDER_TYPE);

        // `plan` iterator is not cloneable, because of this we can't
        // call plan.cycle(), and do a cyclic shift
        let mut vec: VecDeque<Arc<Node>> = plan.collect();
        vec.rotate_right(index % vec.len());

        Box::new(vec.into_iter())
    }
}

pub struct TokenAware {
    internal_policy: Box<dyn InternalLoadBalancingPolicy>,
}

impl TokenAware {
    pub fn new(internal_policy: Box<dyn InternalLoadBalancingPolicy>) -> Self {
        Self { internal_policy }
    }
}

impl LoadBalancingPolicy for TokenAware {
    fn plan<'a>(
        &self,
        statement: &Statement,
        cluster: &'a ClusterData,
    ) -> Box<dyn Iterator<Item = Arc<Node>> + 'a> {
        match statement.token {
            Some(token) => {
                // FIXME add replica calculation
                let mut owner = cluster
                    .ring
                    .range((Included(token), Unbounded))
                    .map(|(_token, node)| node.clone())
                    .take(1)
                    .chain(cluster.ring.values().cloned())
                    .take(1);

                self.internal_policy.apply_policy_for_plan(&mut owner)
            }
            // fallback to internal policy
            None => Box::new(self.internal_policy.plan(statement, cluster)),
        }
    }
}
