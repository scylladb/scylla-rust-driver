//! This module holds entities that represent the cluster as a whole,
//! nodes in the cluster (together with a pool of connections),
//! the cluster's state, and logic for ruling out specific nodes.
//!
//! This includes:
//! - node's representation (Node),
//! - metadata representation, fetching and management, including:
//!     - topology metadata,
//!     - schema metadata,
//!     - tablet metadata,
//! - ClusterData, which is a snapshot of the cluster's state.
//!   - ClusterData is replaced atomically upon a metadata refresh,
//!     preventing any issues arising from mutability, including races.
//! - HostFilter, which is a way to filter out some nodes and thus
//!   not contact them at all on any condition.
//!

#[allow(clippy::module_inception)]
mod cluster;
pub mod host_filter;
pub mod metadata;
pub(crate) mod node;

pub use cluster::ClusterData;
pub(crate) use cluster::{use_keyspace_result, Cluster, ClusterNeatDebug};

pub use node::{KnownNode, Node, NodeAddr, NodeRef};
