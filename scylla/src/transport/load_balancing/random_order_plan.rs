use super::{random_order_iter::RandomOrderIter, LoadBalancingPlan};
use crate::transport::node::Node;
use std::sync::Arc;

// TODO: Consider using a Feistel Network to generate permutations in O(1) space and time
// Example crate: https://github.com/asimihsan/permutation-iterator-rs

pub struct RandomOrderPlan<'a> {
    nodes_to_try: RandomOrderIter<&'a Arc<Node>>,
}

impl<'a> std::iter::FromIterator<&'a Arc<Node>> for RandomOrderPlan<'a> {
    fn from_iter<I>(nodes_to_try_iter: I) -> RandomOrderPlan<'a>
    where
        I: IntoIterator<Item = &'a Arc<Node>>,
    {
        RandomOrderPlan {
            nodes_to_try: RandomOrderIter::from_iter(nodes_to_try_iter),
        }
    }
}

impl<'a> LoadBalancingPlan<'a> for RandomOrderPlan<'a> {
    fn next(&mut self) -> Option<&'a Arc<Node>> {
        self.nodes_to_try.next()
    }
}
