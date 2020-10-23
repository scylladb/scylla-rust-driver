//! Async CQL driver for Rust, optimized for Scylla.

#[macro_use]
extern crate anyhow;

#[macro_use]
pub mod macros;

pub mod frame;
pub mod routing;
pub mod statement;
pub mod transport;

pub use macros::*;
pub use statement::prepared_statement;
pub use statement::query;
