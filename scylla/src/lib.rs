//! Async CQL driver for Rust, optimized for Scylla.

#[macro_use]
pub mod macros;

pub mod frame;
pub mod routing;
pub mod statement;
pub mod tracing;
pub mod transport;

pub use macros::*;
pub use statement::batch;
pub use statement::prepared_statement;
pub use statement::query;

pub use frame::response::cql_to_rust;

pub use transport::connection::{BatchResult, QueryResult};
pub use transport::session::{IntoTypedRows, Session, SessionConfig};
pub use transport::session_builder::SessionBuilder;
