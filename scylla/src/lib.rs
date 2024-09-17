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
//! All driver activity revolves around the [Session]\
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
//! `Session` is usually created using the [SessionBuilder].\
//! All configuration options for a `Session` can be specified while building.
//!
//! ### Making queries
//! After successfully connecting to the cluster we can make queries.\
//! The driver supports multiple query types:
//! * [Simple](crate::Session::query_unpaged)
//! * [Simple paged](crate::Session::query_iter)
//! * [Prepared](crate::Session::execute_unpaged) (need to be [prepared](crate::Session::prepare) before use)
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
//!     .query_unpaged(
//!         "INSERT INTO ks.tab (a, b) VALUES(?, ?)",
//!         (2_i32, "some text")
//!     )
//!     .await?;
//! # Ok(())
//! # }
//! ```
//! But the driver will accept anything implementing the trait [SerializeRow]
//! (crate::serialize::row::SerializeRow)
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
//! // Keep in mind that all results come in one response (no paging is done!),
//! // so the memory footprint and latency may be huge!
//! // To prevent that, use `Session::query_iter` or `Session::query_single_page`.
//! let rows_opt = session
//!     .query_unpaged("SELECT a, b FROM ks.tab", &[])
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

pub mod macros;
#[doc(inline)]
pub use macros::*;

pub mod frame {
    pub use scylla_cql::frame::{frame_errors, value, Authenticator, Compression};
    pub(crate) use scylla_cql::frame::{
        parse_response_body_extensions, protocol_features, read_response_frame, request,
        server_event_type, FrameParams, SerializedRequest,
    };

    pub mod types {
        pub use scylla_cql::frame::types::{Consistency, SerialConsistency};
    }

    pub mod response {
        pub use scylla_cql::frame::response::cql_to_rust;
        pub(crate) use scylla_cql::frame::response::*;

        pub mod result {
            pub(crate) use scylla_cql::frame::response::result::*;
            pub use scylla_cql::frame::response::result::{
                ColumnSpec, ColumnType, CqlValue, PartitionKeyIndex, Row, TableSpec,
            };
        }
    }
}

/// Serializing bound values of a query to be sent to the DB.
pub mod serialize {
    pub use scylla_cql::types::serialize::*;
}

/// Deserializing DB response containing CQL query results.
pub mod deserialize {
    pub use scylla_cql::types::deserialize::{
        DeserializationError, DeserializeRow, DeserializeValue, FrameSlice, TypeCheckError,
    };

    /// Deserializing the whole query result contents.
    pub mod result {
        pub use scylla_cql::types::deserialize::result::{RowIterator, TypedRowIterator};
    }

    /// Deserializing a row of the query result.
    pub mod row {
        pub use scylla_cql::types::deserialize::row::{
            BuiltinDeserializationError, BuiltinDeserializationErrorKind, BuiltinTypeCheckError,
            BuiltinTypeCheckErrorKind, ColumnIterator, RawColumn,
        };
    }

    /// Deserializing a single CQL value from a column of the query result row.
    pub mod value {
        pub use scylla_cql::types::deserialize::value::{
            BuiltinDeserializationError, BuiltinDeserializationErrorKind, BuiltinTypeCheckError,
            BuiltinTypeCheckErrorKind, Emptiable, ListlikeIterator, MapDeserializationErrorKind,
            MapIterator, MapTypeCheckErrorKind, MaybeEmpty, SetOrListDeserializationErrorKind,
            SetOrListTypeCheckErrorKind, TupleDeserializationErrorKind, TupleTypeCheckErrorKind,
            UdtIterator, UdtTypeCheckErrorKind,
        };
    }
}

pub mod authentication;
#[cfg(feature = "cloud")]
pub mod cloud;

pub mod errors;
pub mod history;
pub mod routing;
pub mod statement;
pub mod tracing;
pub mod transport;

pub(crate) mod utils;

/// This module is NOT part of the public API (it is `pub` only for internal use of integration tests).
/// Future minor releases are free to introduce breaking API changes inside it.
#[doc(hidden)]
pub use utils::test_utils;

pub use statement::batch;
pub use statement::prepared_statement;
pub use statement::query;

pub use frame::response::cql_to_rust;
pub use frame::response::cql_to_rust::FromRow;

pub use transport::caching_session::CachingSession;
pub use transport::execution_profile::ExecutionProfile;
pub use transport::query_result::QueryResult;
pub use transport::session::{IntoTypedRows, Session, SessionConfig};
pub use transport::session_builder::SessionBuilder;

#[cfg(feature = "cloud")]
pub use transport::session_builder::CloudSessionBuilder;

pub use transport::execution_profile;
pub use transport::host_filter;
pub use transport::load_balancing;
pub use transport::retry_policy;
pub use transport::speculative_execution;

pub use transport::metrics::Metrics;
