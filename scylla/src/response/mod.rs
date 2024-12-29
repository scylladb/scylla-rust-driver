pub mod legacy_query_result;
pub mod query_result;
mod request_response;

pub(crate) use request_response::{
    NonErrorAuthResponse, NonErrorQueryResponse, NonErrorStartupResponse, QueryResponse,
};
