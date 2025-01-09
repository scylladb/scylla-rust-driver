pub mod caching_session;

mod self_identity;
pub use self_identity::SelfIdentity;

pub mod session;

pub mod session_builder;

#[cfg(test)]
mod session_test;

pub use scylla_cql::frame::Compression;

pub use crate::transport::connection_pool::PoolSize;
