use std::{net::SocketAddr, sync::Arc};

use crate::{
    cluster::{Node, NodeRef},
    network::Connection,
    routing::Shard,
};

/// The coordinator of a CQL request, i.e., the node+shard that receives
/// and processes the request, and hopefully eventually sends a response.
#[derive(Debug, Clone)]
pub struct Coordinator {
    /// Translated address, i.e., one that the connection is opened against.
    connection_address: SocketAddr,
    /// The node that served as coordinator.
    node: Arc<Node>,
    /// Number of the shard, if applicable (present for ScyllaDB nodes, absent for Cassandra).
    shard: Option<Shard>,
}

impl Coordinator {
    pub(crate) fn new(node: NodeRef, shard: Option<Shard>, connection: &Connection) -> Self {
        Self {
            connection_address: connection.get_connect_address(),
            node: Arc::clone(node),
            shard,
        }
    }

    /// Translated address, i.e., one that the connection is opened against.
    #[inline]
    pub fn connection_address(&self) -> SocketAddr {
        self.connection_address
    }

    /// The node that served as coordinator of the request.
    #[inline]
    pub fn node(&self) -> NodeRef<'_> {
        &self.node
    }

    /// Number of the shard, if applicable (present for ScyllaDB nodes, absent for Cassandra).
    #[inline]
    pub fn shard(&self) -> Option<Shard> {
        self.shard
    }
}
