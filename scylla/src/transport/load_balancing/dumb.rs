use super::{LBPlan, LoadBalancingPlan, LoadBalancingPolicy, StatementInfo};
use crate::transport::{cluster::ClusterData, node::Node};
use std::borrow::Cow;
use std::sync::Arc;

pub struct DumbPolicy;

impl LoadBalancingPolicy for DumbPolicy {
    fn plan<'a>(
        &'a self,
        _statement_info: &'a StatementInfo,
        cluster: &'a ClusterData,
    ) -> LBPlan<'a> {
        LBPlan::Dumb(DumbPlan {
            nodes_to_try: &cluster.all_nodes,
            next_index_to_try: 0,
        })
    }

    fn name(&self) -> Cow<'_, str> {
        Cow::Borrowed("DumbPolicy")
    }
}

pub struct DumbPlan<'a> {
    nodes_to_try: &'a [Arc<Node>],
    next_index_to_try: usize,
}

impl<'a> LoadBalancingPlan<'a> for DumbPlan<'a> {
    fn next(&mut self) -> Option<&'a Arc<Node>> {
        if self.next_index_to_try >= self.nodes_to_try.len() {
            return None;
        }

        let result: &Arc<Node> = &self.nodes_to_try[self.next_index_to_try];
        self.next_index_to_try += 1;
        Some(result)
    }
}
