pub mod response;
pub mod types;

use std::fmt::Display;
use std::str::FromStr;

use thiserror::Error;

/// An error type for parsing an enum value from a primitive.
#[derive(Error, Debug, Clone, PartialEq, Eq)]
#[error("No discrimant in enum `{enum_name}` matches the value `{primitive:?}`")]
pub struct TryFromPrimitiveError<T: Copy + std::fmt::Debug> {
    enum_name: &'static str,
    primitive: T,
}

impl<T: Copy + std::fmt::Debug> TryFromPrimitiveError<T> {
    /// Creates a new `TryFromPrimitiveError` with the given enum name and primitive value.
    pub fn new(enum_name: &'static str, primitive: T) -> Self {
        Self {
            enum_name,
            primitive,
        }
    }
}

/// All of the Authenticators supported by ScyllaDB
#[derive(Debug, PartialEq, Eq, Clone)]
// Check triggers because all variants end with "Authenticator".
// TODO(2.0): Remove the "Authenticator" postfix from variants.
#[expect(clippy::enum_variant_names)]
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

impl Compression {
    /// Returns the string representation of the compression algorithm.
    pub fn as_str(&self) -> &'static str {
        match self {
            Compression::Lz4 => "lz4",
            Compression::Snappy => "snappy",
        }
    }
}

/// Unknown compression.
#[derive(Error, Debug, Clone)]
#[error("Unknown compression: {name}")]
pub struct CompressionFromStrError {
    name: String,
}

impl FromStr for Compression {
    type Err = CompressionFromStrError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "lz4" => Ok(Self::Lz4),
            "snappy" => Ok(Self::Snappy),
            other => Err(Self::Err {
                name: other.to_owned(),
            }),
        }
    }
}

impl Display for Compression {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}
