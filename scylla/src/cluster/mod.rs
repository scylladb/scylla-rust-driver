//! This module holds entities that represent the cluster as a whole,
//! nodes in the cluster (together with a pool of connections),
//! the cluster's state, and logic for ruling out specific nodes.
//!
//! This includes:
//! - node's representation ([Node]),
//! - [metadata] representation, fetching and management, including:
//!   - topology metadata,
//!   - schema metadata,
//    - tablet metadata,
//! - [ClusterState], which is a snapshot of the cluster's state.
//!   - [ClusterState] is replaced atomically upon a metadata refresh,
//!     preventing any issues arising from mutability, including races.
//  - [ControlConnection](control_connection::ControlConnection), which
//    is the single connection used to fetch metadata and receive events
//    from the cluster.

mod worker;
pub(crate) use worker::{Cluster, ClusterNeatDebug, use_keyspace_result};

mod state;
pub use state::ClusterState;

pub(crate) mod node;
pub use node::{KnownNode, Node, NodeAddr, NodeRef};

mod control_connection;

pub mod metadata;
