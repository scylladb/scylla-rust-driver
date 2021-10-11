//! Async Rust driver for the [Scylla](https://scylladb.com) database written in Rust.
//! Although optimized for Scylla, the driver is also compatible with [Apache Cassandra®](https://cassandra.apache.org/).
//!
//! # Documentation book
//! The best source to learn about this driver is the [documentation book](https://cvybhu.github.io/scyllabook/index.html).  
//! This page contains mainly API documentation
//!
//! # Other documentation
//! * [Documentation book](https://cvybhu.github.io/scyllabook/index.html)
//! * [Examples](https://github.com/scylladb/scylla-rust-driver/tree/main/examples)
//! * [Scylla documentation](https://docs.scylladb.com)
//! * [Cassandra® documentation](https://cassandra.apache.org/doc/latest/)
//!
//! # Driver overview
//! ### Connecting
//! All driver activity revolves around the [Session](crate::Session)  
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
//! `Session` is usually created using the [SessionBuilder](crate::SessionBuilder).  
//! All configuration options for a `Session` can be specified while building.
//!
//! ### Making queries
//! After succesfully connecting to the cluster we can make queries.  
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
//! But the driver will accept anything implementing the trait [ValueList](crate::frame::value::ValueList)
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
//!         // Parse row as int and text   
//!         let (int_val, text_val): (i32, String) = row?;
//!     }
//! }
//! # Ok(())
//! # }
//! ```
//! See the [book](https://cvybhu.github.io/scyllabook/queries/result.html) for more receiving methods

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
pub use frame::response::cql_to_rust::FromRow;

pub use transport::connection::{BatchResult, QueryResult};
pub use transport::session::{IntoTypedRows, Session, SessionConfig};
pub use transport::session_builder::SessionBuilder;

pub use transport::load_balancing;
pub use transport::retry_policy;
pub use transport::speculative_execution;

pub use transport::metrics::Metrics;
