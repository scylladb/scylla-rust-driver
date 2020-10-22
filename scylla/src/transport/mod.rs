pub mod connection;
pub mod session;

/// The wire protocol compression algorithm.
#[derive(Copy, Clone)]
pub enum Compression {
    /// LZ4 compression algorithm.
    LZ4
}

#[cfg(test)]
mod connection_test;
