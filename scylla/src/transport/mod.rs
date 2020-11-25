pub mod connection;
pub mod iterator;
mod metrics;
pub mod session;
mod topology;

/// The wire protocol compression algorithm.
#[derive(Copy, Clone)]
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

#[cfg(test)]
mod session_test;
