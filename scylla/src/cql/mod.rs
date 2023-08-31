//! This module re-exports entities from scylla-cql crate in a structured,
//! controlled way, so that only appropriate items are exposed for users.

pub mod value {
    pub use scylla_cql::frame::response::result::CqlValue;
    pub use scylla_cql::frame::value::{
        Counter, CqlDuration, Date, MaybeUnset, SerializeValuesError, SerializedValues, Time,
        Timestamp, Unset, Value, ValueList,
    };
}

pub use value::{Value, ValueList};

pub mod frame {
    pub mod response {
        pub mod result {
            pub use scylla_cql::frame::response::result::{
                ColumnSpec, ColumnType, PartitionKeyIndex, PreparedMetadata, Row, TableSpec,
            };
        }
    }
}

pub mod cql_to_rust {
    pub use scylla_cql::cql_to_rust::{
        CqlTypeError, FromCqlVal, FromCqlValError, FromRow, FromRowError,
    };
}
