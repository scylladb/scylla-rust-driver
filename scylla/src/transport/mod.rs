mod cluster;
pub(crate) mod connection;
mod connection_keeper;
pub mod load_balancing;
mod node;
pub mod query_result;
pub mod retry_policy;
pub mod session;
pub mod session_builder;
pub mod speculative_execution;
mod topology;

pub mod errors;
pub mod iterator;
pub(crate) mod metrics;

mod authenticate_test;
#[cfg(test)]
mod session_test;

// All of the Authenticators supported by Scylla
#[derive(Debug, PartialEq)]
pub enum Authenticator {
    AllowAllAuthenticator,
    PasswordAuthenticator,
    CassandraPasswordAuthenticator,
    CassandraAllowAllAuthenticator,
    ScyllaTransitionalAuthenticator,
}

/// The wire protocol compression algorithm.
#[derive(Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Debug)]
pub enum Compression {
    /// LZ4 compression algorithm.
    Lz4,
    /// Snappy compression algorithm.
    Snappy,
}

impl ToString for Compression {
    fn to_string(&self) -> String {
        match self {
            Compression::Lz4 => "lz4".to_owned(),
            Compression::Snappy => "snappy".to_owned(),
        }
    }
}
