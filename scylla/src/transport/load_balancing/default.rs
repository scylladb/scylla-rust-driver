use std::sync::Arc;

use arc_swap::ArcSwap;
use tracing::warn;

use super::{Event, LoadBalancingPolicy, Plan, QueryInfo};
use crate::transport::{
    cluster::{ClusterData, EventConsumer},
    Node,
};

pub struct DefaultPolicy {
    local_dc: Option<String>,
    nodes: ArcSwap<NodesDistribution>,
}

enum NodesDistribution {
    Uniform(Vec<Arc<Node>>),
    DCAware {
        local: Vec<Arc<Node>>,
        remote: Vec<Arc<Node>>,
    },
}

impl DefaultPolicy {
    pub fn new() -> Self {
        Self {
            local_dc: None,
            nodes: ArcSwap::new(Arc::new(NodesDistribution::Uniform(Vec::new()))),
        }
    }

    pub fn dc_aware(mut self, local_dc_name: String) -> Self {
        self.local_dc = Some(local_dc_name);
        self
    }

    fn update_state(&self, cluster: &ClusterData) {
        let distribution = match &self.local_dc {
            Some(dc) => {
                let (local, remote) = cluster
                    .known_peers
                    .values()
                    .cloned()
                    .partition(|node| node.datacenter.as_ref() == Some(dc));

                NodesDistribution::DCAware { local, remote }
            }
            None => {
                let nodes = cluster.known_peers.values().cloned().collect();

                NodesDistribution::Uniform(nodes)
            }
        };

        self.nodes.store(Arc::new(distribution));
    }
}

impl LoadBalancingPolicy for DefaultPolicy {
    fn plan<'a>(&self, info: &QueryInfo, cluster: &'a ClusterData) -> Plan<'a> {
        if let QueryInfo {
            token: Some(token),
            keyspace: Some(keyspace),
        } = info
        {
            match cluster.replicas_for_token(token, keyspace) {
                Ok(replicas) => return Box::new(replicas.into_iter()),
                Err(e) => warn!("Failed to get replicas for {:?}: {:?}", info, e),
            }
        }

        let nodes = self.nodes.load_full();
        Box::new(NoTokenPlan { nodes })
    }
}

impl EventConsumer for DefaultPolicy {
    fn consume(&self, events: &[Event], cluster: &ClusterData) {
        if is_there_topology_update(events) {
            self.update_state(cluster);
        }
    }
}

struct NoTokenPlan {
    nodes: Arc<NodesDistribution>,
}

impl Iterator for NoTokenPlan {
    type Item = Arc<Node>;

    fn next(&mut self) -> Option<Self::Item> {
        match &*self.nodes {
            NodesDistribution::Uniform(nodes) => nodes.iter().cloned().next(),
            NodesDistribution::DCAware { .. } => todo!(),
        }
    }
}

fn is_there_topology_update(events: &[Event]) -> bool {
    for event in events {
        match event {
            Event::TopologyChange(_) => return true,
            _ => {}
        }
    }

    false
}
