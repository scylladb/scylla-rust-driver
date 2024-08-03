pub(crate) mod caching_session;
mod cluster;
pub(crate) mod connection;
mod connection_pool;
pub mod downgrading_consistency_retry_policy;
pub mod execution_profile;
pub mod host_filter;
pub mod iterator;
pub mod load_balancing;
pub mod locator;
pub(crate) mod metrics;
mod node;
pub mod partitioner;
pub mod query_result;
pub mod retry_policy;
pub mod session;
pub mod session_builder;
pub mod speculative_execution;
pub mod topology;

pub use crate::frame::{Authenticator, Compression};
pub use connection::SelfIdentity;
pub use execution_profile::ExecutionProfile;
pub use scylla_cql::errors;

#[cfg(test)]
mod authenticate_test;
#[cfg(test)]
mod cql_collections_test;
#[cfg(test)]
mod session_test;
#[cfg(test)]
mod silent_prepare_batch_test;

#[cfg(test)]
mod cql_types_test;
#[cfg(test)]
mod cql_value_test;
#[cfg(test)]
mod large_batch_statements_test;

pub use cluster::ClusterData;
pub use node::{KnownNode, Node, NodeAddr, NodeRef};
