use std::net::SocketAddr;
use std::sync::Arc;

use uuid::Uuid;

use crate::cluster::{ClusterState, Node, NodeRef};
use crate::observability::warn_rate_limited;
use crate::routing::Shard;

use super::{LoadBalancingPolicy, RoutingInfo};

/// Node identifier used by [`SingleTargetLoadBalancingPolicy`].
///
/// If `NodeIdentifier::Node` is used, it will be returned immediately without
/// the lookup in the cluster metadata.
///
/// If `NodeIdentifier::HostId` or `NodeIdentifier::NodeAddress` is used,
/// the node will be looked up in the cluster metadata based on its host_id/address.
///
/// Note: `NodeIdentifier::NodeAddress` assumes that provided address is **untranslated**.
/// In other words, it should be the address of form `<rpc_address>:<cql_port>`, where
/// `<rpc_address>` is the address of the node stored in the system tables, as well as
/// in the [`Node`] struct.
#[derive(Debug, Clone)] // <- Cheaply clonable
#[non_exhaustive]
pub enum NodeIdentifier {
    Node(Arc<Node>),
    HostId(Uuid),
    NodeAddress(SocketAddr),
}

/// Load balancing policy that enforces a single target.
///
/// It may be useful for queries to node-local system tables.
#[derive(Debug)]
pub struct SingleTargetLoadBalancingPolicy {
    node_identifier: NodeIdentifier,
    shard: Option<Shard>,
}

impl SingleTargetLoadBalancingPolicy {
    /// Creates a new instance of [`SingleTargetLoadBalancingPolicy`].
    #[expect(clippy::new_ret_no_self)]
    pub fn new(
        node_identifier: NodeIdentifier,
        shard: Option<Shard>,
    ) -> Arc<dyn LoadBalancingPolicy> {
        Arc::new(Self {
            node_identifier,
            shard,
        })
    }
}

impl LoadBalancingPolicy for SingleTargetLoadBalancingPolicy {
    fn pick<'a>(
        &'a self,
        _request: &'a RoutingInfo,
        cluster: &'a ClusterState,
    ) -> Option<(NodeRef<'a>, Option<Shard>)> {
        let node = match &self.node_identifier {
            NodeIdentifier::Node(node) => Some(node),
            NodeIdentifier::HostId(host_id) => cluster.known_peers.get(host_id),
            NodeIdentifier::NodeAddress(addr) => cluster
                .all_nodes
                .iter()
                .find(|node| SocketAddr::new(node.address.ip(), node.address.port()) == *addr),
        };

        match node {
            Some(node) => Some((node, self.shard)),
            None => {
                warn_rate_limited!(
                    std::time::Duration::from_secs(1),
                    "SingleTargetLoadBalancingPolicy failed to find requested node {:?} in cluster metadata.",
                    self.node_identifier
                );
                None
            }
        }
    }

    fn fallback<'a>(
        &'a self,
        _request: &'a RoutingInfo,
        _cluster: &'a ClusterState,
    ) -> super::FallbackPlan<'a> {
        Box::new(std::iter::empty())
    }

    fn name(&self) -> String {
        "SingleTargetLoadBalancingPolicy".to_string()
    }
}
