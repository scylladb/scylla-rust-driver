mod cluster;
pub mod connection;
mod connection_keeper;
pub mod load_balancing;
mod node;
pub mod session;
pub mod session_builder;
mod topology;

pub mod errors;
pub mod iterator;
mod metrics;

#[cfg(test)]
mod session_test;

/// The wire protocol compression algorithm.
#[derive(Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Debug)]
pub enum Compression {
    /// LZ4 compression algorithm.
    LZ4,
    /// Snappy compression algorithm.
    Snappy,
}

impl ToString for Compression {
    fn to_string(&self) -> String {
        match self {
            Compression::LZ4 => "lz4".to_owned(),
            Compression::Snappy => "snappy".to_owned(),
        }
    }
}
