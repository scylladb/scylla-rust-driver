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
//!

// The purpose of cluster::cluster module is to not have any complex logic in mod.rs.
// No code external to this module will ever see this awkward path, because the inner
// cluster module is pub(self), and its items are only accessible through the below
// re-exports.
#[allow(clippy::module_inception)]
mod cluster;
pub use cluster::ClusterData;
pub(crate) use cluster::{use_keyspace_result, Cluster, ClusterNeatDebug};

pub(crate) mod node;
pub use node::{KnownNode, Node, NodeAddr, NodeRef};

pub mod metadata;
