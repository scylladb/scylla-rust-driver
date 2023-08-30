// The purpose of cluster::cluster module is to not have any complex logic in mod.rs.
// No code external to this module will ever see this awkward path, because the inner
// cluster module is pub(self), and its items are only accessible through the below
// re-exports.
#[allow(clippy::module_inception)]
mod cluster;
pub use cluster::ClusterData;
pub(crate) use cluster::{use_keyspace_result, Cluster, ClusterNeatDebug};

pub mod metadata;
