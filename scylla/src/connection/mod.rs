//! This module holds entities that represent connections to the cluster
//! and management over those connections (connection pooling).
//! This includes two main abstractions:
//! - Connection - a single, possibly encrypted, connection to a Scylla node over CQL protocol,
//! - NodeConnectionPool - a manager that keeps a desired number of connections opened to each shard.

#[allow(clippy::module_inception)]
mod connection;
mod connection_pool;

pub use crate::execution::errors::TranslationError;
use connection::ErrorReceiver;
#[cfg(feature = "ssl")]
pub(crate) use connection::SslConfig;
pub(crate) use connection::{
    open_connection, Connection, ConnectionConfig, NonErrorQueryResponse, QueryResponse,
    VerifiedKeyspaceName,
};
pub use connection::{AddressTranslator, SelfIdentity};

pub use connection_pool::PoolSize;
pub(crate) use connection_pool::{NodeConnectionPool, PoolConfig};

pub use scylla_cql::frame::Compression;
