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
//! But the driver will accept anything implementing the trait [SerializeRow].
//!
//! ### Receiving results
//! The easiest way to read rows returned by a query is to cast each row to a tuple of values:
//!
//! ```rust
//! # use scylla::Session;
//! # use std::error::Error;
//! # async fn check_only_compiles(session: &Session) -> Result<(), Box<dyn Error>> {
//!
//! // Read rows containing an int and text
//! // Keep in mind that all results come in one response (no paging is done!),
//! // so the memory footprint and latency may be huge!
//! // To prevent that, use `Session::query_iter` or `Session::query_single_page`.
//! let query_rows = session
//!     .query_unpaged("SELECT a, b FROM ks.tab", &[])
//!     .await?
//!     .into_rows_result()?;
//!     
//! for row in query_rows.rows()? {
//!     // Parse row as int and text \
//!     let (int_val, text_val): (i32, &str) = row?;
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
            #[cfg(cpp_rust_unstable)]
            pub use scylla_cql::frame::response::result::DeserializedMetadataAndRawRows;

            pub(crate) use scylla_cql::frame::response::result::*;
            pub use scylla_cql::frame::response::result::{
                ColumnSpec, ColumnType, CqlValue, PartitionKeyIndex, Row, TableSpec,
            };
        }
    }
}

/// Serializing bound values of a query to be sent to the DB.
// Note: When editing comment on submodules here edit corresponding comments
// on scylla-cql modules too.
pub mod serialize {
    pub use scylla_cql::types::serialize::SerializationError;
    /// Contains the [BatchValues][batch::BatchValues] and [BatchValuesIterator][batch::BatchValuesIterator] trait and their
    /// implementations.
    pub mod batch {
        // Main types
        pub use scylla_cql::types::serialize::batch::{
            BatchValues, BatchValuesFromIterator, BatchValuesIterator,
            BatchValuesIteratorFromIterator, TupleValuesIter,
        };

        // Legacy migration types - to be removed when removing legacy framework
        #[allow(deprecated)]
        pub use scylla_cql::types::serialize::batch::{
            LegacyBatchValuesAdapter, LegacyBatchValuesIteratorAdapter,
        };
    }

    /// Contains the [SerializeRow][row::SerializeRow] trait and its implementations.
    pub mod row {
        // Main types
        pub use scylla_cql::types::serialize::row::{RowSerializationContext, SerializeRow};

        // Errors
        pub use scylla_cql::types::serialize::row::{
            BuiltinSerializationError, BuiltinSerializationErrorKind, BuiltinTypeCheckError,
            BuiltinTypeCheckErrorKind,
        };

        // Legacy migration types - to be removed when removing legacy framework
        #[allow(deprecated)]
        pub use scylla_cql::types::serialize::row::{
            // Legacy migration types - to be removed when removing legacy framework
            serialize_legacy_row,
            ValueListAdapter,
            ValueListToSerializeRowAdapterError,
        };

        // Not part of the old framework, but something that we should
        // still aim to remove from public API.
        pub use scylla_cql::types::serialize::row::{SerializedValues, SerializedValuesIterator};
    }

    /// Contains the [SerializeValue][value::SerializeValue] trait and its implementations.
    pub mod value {
        // Main types
        pub use scylla_cql::types::serialize::value::SerializeValue;

        // Errors
        pub use scylla_cql::types::serialize::value::{
            BuiltinSerializationError, BuiltinSerializationErrorKind, BuiltinTypeCheckError,
            BuiltinTypeCheckErrorKind, MapSerializationErrorKind, MapTypeCheckErrorKind,
            SetOrListSerializationErrorKind, SetOrListTypeCheckErrorKind,
            TupleSerializationErrorKind, TupleTypeCheckErrorKind, UdtSerializationErrorKind,
            UdtTypeCheckErrorKind,
        };

        // Legacy migration types - to be removed when removing legacy framework
        #[allow(deprecated)]
        pub use scylla_cql::types::serialize::value::{
            serialize_legacy_value, ValueAdapter, ValueToSerializeValueAdapterError,
        };
    }

    /// Contains types and traits used for safe serialization of values for a CQL statement.
    pub mod writers {
        pub use scylla_cql::types::serialize::writers::{
            CellOverflowError, CellValueBuilder, CellWriter, RowWriter, WrittenCellProof,
        };
    }
}

/// Deserializing DB response containing CQL query results.
pub mod deserialize {
    pub use scylla_cql::types::deserialize::{
        DeserializationError, DeserializeRow, DeserializeValue, FrameSlice, TypeCheckError,
    };

    /// Deserializing the whole query result contents.
    pub mod result {
        pub use scylla_cql::types::deserialize::result::TypedRowIterator;
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

    // Shorthands for better readability.
    #[cfg_attr(not(test), allow(unused))]
    pub(crate) trait DeserializeOwnedValue:
        for<'frame, 'metadata> DeserializeValue<'frame, 'metadata>
    {
    }
    impl<T> DeserializeOwnedValue for T where
        T: for<'frame, 'metadata> DeserializeValue<'frame, 'metadata>
    {
    }
    pub(crate) trait DeserializeOwnedRow:
        for<'frame, 'metadata> DeserializeRow<'frame, 'metadata>
    {
    }
    impl<T> DeserializeOwnedRow for T where T: for<'frame, 'metadata> DeserializeRow<'frame, 'metadata> {}
}

pub mod authentication;
#[cfg(feature = "cloud")]
pub mod cloud;

pub mod cluster;
pub mod connection;
pub mod execution;
pub mod response;
pub mod routing;
pub mod session;
pub mod statement;
pub mod transport;

pub(crate) mod utils;

#[cfg(test)]
pub(crate) use utils::test_utils;

pub use statement::batch;
pub use statement::prepared_statement;
pub use statement::query;

#[allow(deprecated)]
pub use frame::response::cql_to_rust::{self, FromRow};

pub use execution::execution_profile::ExecutionProfile;
#[allow(deprecated)]
pub use response::legacy_query_result::LegacyQueryResult;
pub use response::query_result::{QueryResult, QueryRowsResult};
#[allow(deprecated)]
pub use session::{
    CachingSession, GenericCachingSession, IntoTypedRows, LegacyCachingSession, LegacySession,
    Session, SessionBuilder, SessionConfig,
};

#[cfg(feature = "cloud")]
pub use session::CloudSessionBuilder;

pub use cluster::host_filter;
pub use execution::execution_profile;
pub use execution::load_balancing;
pub use execution::speculative_execution;

pub use execution::metrics::{Metrics, MetricsError};
