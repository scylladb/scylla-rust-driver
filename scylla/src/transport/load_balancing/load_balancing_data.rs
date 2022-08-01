use itertools::Itertools;

use super::TokenRing;
use crate::routing::Token;
use crate::transport::node::Node;
use std::borrow::Borrow;
use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;

#[derive(Clone)]
pub struct DatacenterLBData {
    pub dc_ring: TokenRing<Arc<Node>>,
    pub unique_nodes_in_ring: usize,
    pub rack_count: usize,
}

#[derive(Clone)]
pub struct LoadBalancingData {
    pub global_ring: TokenRing<Arc<Node>>,
    pub unique_nodes_in_global_ring: usize,
    pub datacenters: HashMap<String, DatacenterLBData>,
    empty_dc_data: DatacenterLBData,
}

impl LoadBalancingData {
    pub fn new(ring_iter: impl Iterator<Item = (Token, Arc<Node>)>) -> LoadBalancingData {
        let global_ring: TokenRing<Arc<Node>> = TokenRing::new(ring_iter);

        let unique_nodes_in_global_ring: usize =
            global_ring.iter().map(|(_t, n)| n).unique().count();

        let mut datacenter_nodes: HashMap<String, Vec<(Token, Arc<Node>)>> = HashMap::new();
        for (token, node) in global_ring.iter() {
            if let Some(datacenter_name) = &node.datacenter {
                match datacenter_nodes.get_mut(datacenter_name) {
                    Some(this_datacenter_nodes) => {
                        this_datacenter_nodes.push((*token, node.clone()))
                    }
                    None => {
                        datacenter_nodes
                            .insert(datacenter_name.clone(), vec![(*token, node.clone())]);
                    }
                }
            }
        }

        let mut datacenters: HashMap<String, DatacenterLBData> = HashMap::new();
        for (datacenter_name, this_datacenter_nodes) in datacenter_nodes {
            let dc_ring = TokenRing::new(this_datacenter_nodes.into_iter());
            let unique_nodes_in_ring: usize = dc_ring.iter().map(|(_t, n)| n).unique().count();
            // When counting racks consider None as a separate rack
            let rack_count: usize = dc_ring
                .iter()
                .map(|(_t, n)| n.rack.as_ref())
                .unique()
                .count();
            datacenters.insert(
                datacenter_name,
                DatacenterLBData {
                    dc_ring,
                    unique_nodes_in_ring,
                    rack_count,
                },
            );
        }

        let empty_dc_data = DatacenterLBData {
            dc_ring: TokenRing::new([].into_iter()),
            unique_nodes_in_ring: 0,
            rack_count: 0,
        };

        LoadBalancingData {
            global_ring,
            unique_nodes_in_global_ring,
            datacenters,
            empty_dc_data,
        }
    }

    pub fn get_global_replicas_for_token(
        &self,
        token: impl Borrow<Token>,
        mut replication_factor: usize,
    ) -> impl Iterator<Item = &Arc<Node>> {
        if replication_factor > self.unique_nodes_in_global_ring {
            replication_factor = self.unique_nodes_in_global_ring;
        }

        self.global_ring
            .ring_range(*token.borrow())
            .unique()
            .take(replication_factor)
    }

    pub fn get_datacenter_replicas_for_token<'a>(
        &'a self,
        token: impl Borrow<Token>,
        dc_name: &str,
        mut replication_factor: usize,
    ) -> impl Iterator<Item = &'a Arc<Node>> {
        let dc_lb_data: &DatacenterLBData = match self.datacenters.get(dc_name) {
            Some(dc_lb_data) => dc_lb_data,
            None => &self.empty_dc_data,
        };

        if replication_factor > dc_lb_data.unique_nodes_in_ring {
            replication_factor = dc_lb_data.unique_nodes_in_ring;
        }

        let unique_dc_nodes = || dc_lb_data.dc_ring.ring_range(*token.borrow()).unique();
        let double_dc_nodes = unique_dc_nodes().chain(unique_dc_nodes());

        DcReplicasIterator {
            replicas_left_to_find: replication_factor,
            unique_nodes_iter: double_dc_nodes,
            used_racks: BTreeSet::new(),
            rack_count: dc_lb_data.rack_count,
        }
    }
}

struct DcReplicasIterator<'a, I>
where
    I: Iterator<Item = &'a Arc<Node>>,
{
    replicas_left_to_find: usize,
    unique_nodes_iter: I,
    used_racks: BTreeSet<Option<&'a str>>,
    rack_count: usize,
}

impl<'a, I> Iterator for DcReplicasIterator<'a, I>
where
    I: Iterator<Item = &'a Arc<Node>>,
{
    type Item = &'a Arc<Node>;

    fn next(&mut self) -> Option<&'a Arc<Node>> {
        if self.replicas_left_to_find == 0 {
            return None;
        }

        let next_node: &Arc<Node> = match self.unique_nodes_iter.next() {
            Some(next_node) => next_node,
            None => return None,
        };

        let cur_rack: Option<&str> = next_node.rack.as_deref();
        if self.used_racks.len() < self.rack_count && self.used_racks.contains(&cur_rack) {
            return None;
        }

        self.used_racks.insert(cur_rack);
        self.replicas_left_to_find -= 1;
        Some(next_node)
    }
}
