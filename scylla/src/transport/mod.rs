pub mod legacy_query_result;
pub mod query_result;

pub use crate::frame::Authenticator;
pub use scylla_cql::frame::request::query::{PagingState, PagingStateResponse};
