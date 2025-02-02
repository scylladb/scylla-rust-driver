//! This module holds entities that represent connections to the cluster
//! and management over those connections (connection pooling).
//! This includes two main abstractions:
//! - Connection - a single, possibly encrypted, connection to a Scylla node over CQL protocol,
//! - NodeConnectionPool - a manager that keeps a desired number of connections opened to each shard.

mod connection;
pub(crate) use connection::{Connection, ConnectionConfig, VerifiedKeyspaceName};

mod connection_pool;

pub use connection_pool::PoolSize;
pub(crate) use connection_pool::{NodeConnectionPool, PoolConfig};

#[cfg(feature = "__tls")]
pub(crate) mod tls;

// #[cfg(feature = "unstable-cloud")]
// pub(crate) use tls::TlsConfig;
// #[cfg(feature = "__tls")]
// pub use tls::TlsError;
// #[cfg(feature = "__tls")]
// pub(crate) use tls::TlsProvider;
