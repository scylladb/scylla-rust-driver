mod connection;
mod connection_pool;

use connection::ErrorReceiver;

pub use scylla_cql::frame::Compression;

#[cfg(feature = "ssl")]
pub(crate) use connection::SslConfig;
pub use connection::{AddressTranslator, ConnectionConfig};
pub(crate) use connection::{Connection, VerifiedKeyspaceName};

pub use connection_pool::PoolSize;
pub(crate) use connection_pool::{NodeConnectionPool, PoolConfig};
