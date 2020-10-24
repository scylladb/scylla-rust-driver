pub mod connection;
pub mod iterator;
pub mod session;

/// The wire protocol compression algorithm.
#[derive(Copy, Clone)]
pub enum Compression {
    /// LZ4 compression algorithm.
    LZ4,
    /// Snappy compression algorithm.
    Snappy,
}

#[cfg(test)]
mod connection_test;
