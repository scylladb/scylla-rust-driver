mod connection;
#[cfg(feature = "ssl")]
pub(crate) use connection::SslConfig;
pub(crate) use connection::{
    Connection, ConnectionConfig, NonErrorQueryResponse, QueryResponse, VerifiedKeyspaceName,
};

mod connection_pool;

pub use connection_pool::PoolSize;
pub(crate) use connection_pool::{NodeConnectionPool, PoolConfig};
