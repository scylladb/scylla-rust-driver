use itertools::Itertools;

use super::TokenRing;
use crate::cluster::node::Node;
use crate::routing::Token;

use std::cmp;
use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;

/// DatacenterNodes nodes holds a token ring in which all nodes belong to one datacenter.
#[derive(Debug, Clone)]
pub(crate) struct DatacenterNodes {
    dc_ring: TokenRing<Arc<Node>>,
    unique_nodes_in_dc_ring: Vec<Arc<Node>>,
    rack_count: usize,
}

impl DatacenterNodes {
    const fn new_empty() -> Self {
        Self {
            dc_ring: TokenRing::new_empty(),
            unique_nodes_in_dc_ring: Vec::new(),
            rack_count: 0,
        }
    }

    pub(crate) fn get_dc_ring(&self) -> &TokenRing<Arc<Node>> {
        &self.dc_ring
    }

    pub(crate) fn get_rack_count(&self) -> usize {
        self.rack_count
    }
}

/// ReplicationInfo keeps information about the token ring
/// and provides methods to calculate replica lists for SimpleStrategy and NetworkTopologyStrategy.
#[derive(Debug, Clone)]
pub(crate) struct ReplicationInfo {
    /// Global ring is used to calculate SimpleStrategy replicas.
    global_ring: TokenRing<Arc<Node>>,
    unique_nodes_in_global_ring: Vec<Arc<Node>>,

    /// We keep a separate token ring for each datacenter.
    /// Each datacenter's ring contains only nodes from this datacenter.
    /// It makes it simpler and more efficient to calculate the NetworkTopologyStrategy replicas.
    datacenters: HashMap<String, DatacenterNodes>,
}

static EMPTY_DATACENTER_NODES: DatacenterNodes = DatacenterNodes::new_empty();

impl ReplicationInfo {
    pub(crate) fn get_datacenters(&self) -> &HashMap<String, DatacenterNodes> {
        &self.datacenters
    }

    pub(crate) fn get_global_ring(&self) -> &TokenRing<Arc<Node>> {
        &self.global_ring
    }

    pub(crate) fn new(ring_iter: impl Iterator<Item = (Token, Arc<Node>)>) -> ReplicationInfo {
        let global_ring: TokenRing<Arc<Node>> = TokenRing::new(ring_iter);

        let unique_nodes_in_global_ring = global_ring
            .iter()
            .map(|(_t, n)| n)
            .unique()
            .cloned()
            .collect();

        let mut datacenter_nodes: HashMap<&str, Vec<(Token, Arc<Node>)>> = HashMap::new();
        for (token, node) in global_ring.iter() {
            if let Some(datacenter_name) = node.datacenter.as_deref() {
                datacenter_nodes
                    .entry(datacenter_name)
                    .or_default()
                    .push((*token, node.clone()));
            }
        }

        let mut datacenters: HashMap<String, DatacenterNodes> = HashMap::new();
        for (datacenter_name, this_datacenter_nodes) in datacenter_nodes {
            let dc_ring = TokenRing::new(this_datacenter_nodes.into_iter());
            let unique_nodes_in_dc_ring =
                dc_ring.iter().map(|(_t, n)| n).unique().cloned().collect();
            // When counting racks consider None as a separate rack
            let rack_count: usize = dc_ring
                .iter()
                .map(|(_t, n)| n.rack.as_ref())
                .unique()
                .count();
            datacenters.insert(
                datacenter_name.to_owned(),
                DatacenterNodes {
                    dc_ring,
                    unique_nodes_in_dc_ring,
                    rack_count,
                },
            );
        }

        ReplicationInfo {
            global_ring,
            unique_nodes_in_global_ring,
            datacenters,
        }
    }

    /// Creates an iterator over SimpleStrategy replicas for the given token and replication factor.
    /// The iterator computes consecutive replicas lazily as needed.
    pub(crate) fn simple_strategy_replicas(
        &self,
        token: Token,
        replication_factor: usize,
    ) -> impl Iterator<Item = &Arc<Node>> {
        let num_to_take = cmp::min(replication_factor, self.unique_nodes_in_global_ring.len());

        self.global_ring
            .ring_range(token)
            .unique()
            .take(num_to_take)
    }

    /// Creates an iterator over network topology strategy replicas for the given datacenter.
    /// The iterator computes consecutive replicas lazily as needed.
    pub(crate) fn nts_replicas_in_datacenter<'a>(
        &'a self,
        token: Token,
        datacenter_name: &str,
        replication_factor: usize,
    ) -> impl Iterator<Item = &'a Arc<Node>> + use<'a> {
        let dc_lb_data: &DatacenterNodes = self
            .datacenters
            .get(datacenter_name)
            .unwrap_or(&EMPTY_DATACENTER_NODES);

        let num_to_take = cmp::min(replication_factor, dc_lb_data.unique_nodes_in_dc_ring.len());
        let unique_dc_nodes = dc_lb_data.dc_ring.ring_range(token).unique();

        NtsReplicasInDatacenterIterator {
            replicas_left_to_find: num_to_take,
            unique_dc_ring_nodes_iter: unique_dc_nodes,
            used_racks: BTreeSet::new(),
            acceptable_repeats: replication_factor.saturating_sub(dc_lb_data.rack_count),
        }
    }

    pub(crate) fn unique_nodes_in_global_ring(&self) -> &[Arc<Node>] {
        self.unique_nodes_in_global_ring.as_slice()
    }

    pub(crate) fn unique_nodes_in_datacenter_ring<'a>(
        &'a self,
        datacenter_name: &str,
    ) -> Option<&'a [Arc<Node>]> {
        self.datacenters
            .get(datacenter_name)
            .map(|dc| dc.unique_nodes_in_dc_ring.as_slice())
    }
}

struct NtsReplicasInDatacenterIterator<'a, I>
where
    I: Iterator<Item = &'a Arc<Node>>,
{
    replicas_left_to_find: usize,
    unique_dc_ring_nodes_iter: I,
    used_racks: BTreeSet<Option<&'a str>>,
    acceptable_repeats: usize,
}

impl<'a, I> Iterator for NtsReplicasInDatacenterIterator<'a, I>
where
    I: Iterator<Item = &'a Arc<Node>>,
{
    type Item = &'a Arc<Node>;

    fn next(&mut self) -> Option<&'a Arc<Node>> {
        if self.replicas_left_to_find == 0 {
            return None;
        }

        // Move forward over unique nodes on this datacenter's token ring
        for next_node in &mut self.unique_dc_ring_nodes_iter {
            let cur_rack: Option<&str> = next_node.rack.as_deref();
            if !self.used_racks.contains(&cur_rack) {
                // We haven't used this rack yet, we can use the node and mark the rack as used.
                self.used_racks.insert(cur_rack);
                self.replicas_left_to_find -= 1;
                return Some(next_node);
            } else if self.acceptable_repeats > 0 {
                // We have already seen this rack but we have acceptable repeats left.
                // Use this node.
                self.acceptable_repeats -= 1;
                self.replicas_left_to_find -= 1;
                return Some(next_node);
            }
        }

        None
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        routing::Token,
        routing::locator::test::{
            A, B, C, D, E, F, G, create_ring, mock_metadata_for_token_aware_tests,
        },
        test_utils::setup_tracing,
    };

    use super::ReplicationInfo;

    #[tokio::test]
    async fn test_simple_strategy() {
        setup_tracing();
        let ring = create_ring(&mock_metadata_for_token_aware_tests());
        let replication_info = ReplicationInfo::new(ring);

        let check = |token, replication_factor, expected_node_ids| {
            let replicas =
                replication_info.simple_strategy_replicas(Token::new(token), replication_factor);
            let ids: Vec<u16> = replicas.map(|node| node.address.port()).collect();

            assert_eq!(ids, expected_node_ids);
        };

        check(160, 0, vec![]);
        check(160, 2, vec![F, A]);

        check(200, 1, vec![F]);
        check(200, 2, vec![F, A]);
        check(200, 3, vec![F, A, C]);
        check(200, 4, vec![F, A, C, D]);
        check(200, 5, vec![F, A, C, D, G]);
        check(200, 6, vec![F, A, C, D, G, B]);
        check(200, 7, vec![F, A, C, D, G, B, E]);

        check(701, 1, vec![E]);
        check(701, 2, vec![E, G]);
        check(701, 3, vec![E, G, B]);
        check(701, 4, vec![E, G, B, A]);
        check(701, 5, vec![E, G, B, A, F]);
        check(701, 6, vec![E, G, B, A, F, C]);
        check(701, 7, vec![E, G, B, A, F, C, D]);
        check(701, 8, vec![E, G, B, A, F, C, D]);
    }

    #[tokio::test]
    async fn test_network_topology_strategy() {
        setup_tracing();
        let ring = create_ring(&mock_metadata_for_token_aware_tests());
        let replication_info = ReplicationInfo::new(ring);

        let check = |token, dc, rf, expected| {
            let replicas = replication_info.nts_replicas_in_datacenter(Token::new(token), dc, rf);
            let ids: Vec<u16> = replicas.map(|node| node.address.port()).collect();

            assert_eq!(ids, expected);
        };

        check(160, "eu", 0, vec![]);
        check(160, "eu", 1, vec![A]);
        check(160, "eu", 2, vec![A, G]);
        check(160, "eu", 3, vec![A, C, G]);
        check(160, "eu", 4, vec![A, C, G, B]);
        check(160, "eu", 5, vec![A, C, G, B]);

        check(160, "us", 0, vec![]);
        check(160, "us", 1, vec![F]);
        check(160, "us", 2, vec![F, D]);
        check(160, "us", 3, vec![F, D, E]);
        check(160, "us", 4, vec![F, D, E]);
    }
}
