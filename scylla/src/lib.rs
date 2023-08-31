//! Async Rust driver for the [Scylla](https://scylladb.com) database written in Rust.
//! Although optimized for Scylla, the driver is also compatible with [Apache Cassandra®](https://cassandra.apache.org/).
//!
//! # Documentation book
//! The best source to learn about this driver is the [documentation book](https://rust-driver.docs.scylladb.com/).\
//! This page contains mainly API documentation
//!
//! # Other documentation
//! * [Documentation book](https://rust-driver.docs.scylladb.com/)
//! * [Examples](https://github.com/scylladb/scylla-rust-driver/tree/main/examples)
//! * [Scylla documentation](https://docs.scylladb.com)
//! * [Cassandra® documentation](https://cassandra.apache.org/doc/latest/)
//!
//! # Driver overview
//! ### Connecting
//! All driver activity revolves around the [Session](crate::Session)\
//! `Session` is created by specifying a few known nodes and connecting to them:
//!
//! ```rust,no_run
//! use scylla::{Session, SessionBuilder};
//! use std::error::Error;
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn Error>> {
//!    let session: Session = SessionBuilder::new()
//!         .known_node("127.0.0.1:9042")
//!         .known_node("1.2.3.4:9876")
//!         .build()
//!         .await?;
//!
//!    Ok(())
//! }
//! ```
//! `Session` is usually created using the [SessionBuilder](crate::SessionBuilder).\
//! All configuration options for a `Session` can be specified while building.
//!
//! ### Making queries
//! After successfully connecting to the cluster we can make queries.\
//! The driver supports multiple query types:
//! * [Simple](crate::Session::query)
//! * [Simple paged](crate::Session::query_iter)
//! * [Prepare](crate::Session::execute) (need to be [prepared](crate::Session::prepare) before use)
//! * [Prepared paged](crate::Session::execute_iter)
//! * [Batch](crate::Session::batch)
//!
//! To specify options for a single query create the query object and configure it:
//! * For simple: [Query](crate::query::Query)
//! * For prepared: [PreparedStatement](crate::prepared_statement::PreparedStatement)
//! * For batch: [Batch](crate::batch::Batch)
//!
//! The easiest way to specify bound values in a query is using a tuple:
//! ```rust
//! # use scylla::Session;
//! # use std::error::Error;
//! # async fn check_only_compiles(session: &Session) -> Result<(), Box<dyn Error>> {
//! // Insert an int and text into the table
//! session
//!     .query(
//!         "INSERT INTO ks.tab (a, b) VALUES(?, ?)",
//!         (2_i32, "some text")
//!     )
//!     .await?;
//! # Ok(())
//! # }
//! ```
//! But the driver will accept anything implementing the trait [ValueList](crate::cql::ValueList)
//!
//! ### Receiving results
//! The easiest way to read rows returned by a query is to cast each row to a tuple of values:
//!
//! ```rust
//! # use scylla::Session;
//! # use std::error::Error;
//! # async fn check_only_compiles(session: &Session) -> Result<(), Box<dyn Error>> {
//! use scylla::IntoTypedRows;
//!
//! // Read rows containing an int and text
//! let rows_opt = session
//! .query("SELECT a, b FROM ks.tab", &[])
//!     .await?
//!     .rows;
//!
//! if let Some(rows) = rows_opt {
//!     for row in rows.into_typed::<(i32, String)>() {
//!         // Parse row as int and text \
//!         let (int_val, text_val): (i32, String) = row?;
//!     }
//! }
//! # Ok(())
//! # }
//! ```
//! See the [book](https://rust-driver.docs.scylladb.com/stable/queries/result.html) for more receiving methods

#![cfg_attr(docsrs, feature(doc_auto_cfg))]

#[doc(hidden)]
pub mod _macro_internal {
    pub use scylla_cql::_macro_internal::*;
}

pub use scylla_cql::frame;
pub use scylla_cql::macros::{self, *};

pub mod authentication;
#[cfg(feature = "cloud")]
pub mod cloud;

pub mod cluster;
pub mod connection;
pub mod cql;
pub mod execution;
pub mod response;
pub mod routing;
pub mod session;
pub mod statement;

#[cfg(test)]
mod tests;

pub(crate) mod utils;

/// This module is NOT part of the public API (it is `pub` only for internal use of integration tests).
/// Future minor releases are free to introduce breaking API changes inside it.
#[doc(hidden)]
pub use utils::test_utils;

pub use statement::batch;
pub use statement::prepared_statement;
pub use statement::query;

pub use cql::cql_to_rust;
pub use cql::cql_to_rust::FromRow;

pub use execution::ExecutionProfile;
pub use response::{IntoTypedRows, QueryResult};
pub use session::CachingSession;
pub use session::SessionBuilder;
pub use session::{Session, SessionConfig};

#[cfg(feature = "cloud")]
pub use session::CloudSessionBuilder;

pub use cluster::host_filter;
pub use execution::{load_balancing, speculative_execution};
