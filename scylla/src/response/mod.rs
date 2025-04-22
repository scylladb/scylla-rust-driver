//! This module holds entities that represent responses to requests
//! sent to the cluster by the driver.
//! The following abstractions are involved:
//  - [QueryResponse] - a response to any kind of a CQL request.
//  - [NonErrorQueryResponse] - a non-error response to any kind of a CQL request.
//! - [QueryResult](query_result::QueryResult) - a result of a CQL QUERY/EXECUTE/BATCH request.
//! - [QueryRowsResult](query_result::QueryRowsResult) - a result of CQL QUERY/EXECUTE/BATCH
//!   request that contains some rows, which can be deserialized by the user.

pub mod query_result;
mod request_response;

pub(crate) use request_response::{
    NonErrorAuthResponse, NonErrorQueryResponse, NonErrorStartupResponse, QueryResponse,
    RawPreparedStatement,
};
pub use scylla_cql::frame::request::query::{PagingState, PagingStateResponse};
