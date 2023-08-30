mod connection;
mod connection_pool;

pub use crate::transport::errors::TranslationError;
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
