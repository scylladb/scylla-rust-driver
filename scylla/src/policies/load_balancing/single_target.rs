use std::net::SocketAddr;
use std::sync::Arc;

use uuid::Uuid;

use crate::cluster::{ClusterState, Node, NodeRef};
use crate::routing::Shard;

use super::{LoadBalancingPolicy, RoutingInfo};

/// Node identifier used by [`SingleTargetLoadBalancingPolicy`].
///
/// Used to specify which node the policy should target for routing requests.
#[derive(Debug, Clone)] // <- Cheaply clonable
#[non_exhaustive]
pub enum NodeIdentifier {
    /// Identifies a node by its [`Node`] reference.
    /// If this variant is used, the node will be returned immediately, without
    /// lookup in the cluster metadata.
    Node(Arc<Node>),
    /// Identifies a node by its host ID.
    /// The node will be looked up in the cluster metadata based on it.
    HostId(Uuid),
    /// Identifies a node by its address.
    /// The node will be looked up in the cluster metadata based on it.
    ///
    /// **Warning: This variant may be a footgun if used with address translation.**
    ///
    /// Background:
    /// [Address translation](crate::policies::address_translator::AddressTranslator)
    /// is a feature that allows the driver to translate addresses of nodes in the cluster
    /// metadata to addresses that are reachable from the client. In other words, different
    /// addresses are broadcast by the nodes in the cluster than the ones that the client
    /// must use to connect to the nodes.
    /// The problem is, [`Node`] holds the [`NodeAddr`](crate::cluster::NodeAddr), which may be
    /// an untranslated or translated address, depending on whether the node that the control
    /// connection is opened to is that `Node` or not, respectively. As the control connection
    /// may be opened to different nodes in the cluster in different periods of time (e.g., when
    /// the control connection breaks, it may be recreated with another node), any `Node`
    /// in a cluster with address translation enabled may have untranslated or translated address
    /// held in the `NodeAddr` field.
    ///
    /// Therefore, this variant is unreliable in the context of address translation,
    /// so it is recommended to not use it if address translation is enabled.
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
                tracing::warn!(
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
