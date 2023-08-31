use scylla_cql::frame::response::{result, NonErrorResponse, Response};
use uuid::Uuid;

use crate::{execution::errors::QueryError, QueryResult};

pub(crate) struct QueryResponse {
    pub(crate) response: Response,
    pub(crate) tracing_id: Option<Uuid>,
    pub(crate) warnings: Vec<String>,
}

// A QueryResponse in which response can not be Response::Error
pub(crate) struct NonErrorQueryResponse {
    pub(crate) response: NonErrorResponse,
    pub(crate) tracing_id: Option<Uuid>,
    pub(crate) warnings: Vec<String>,
}

impl QueryResponse {
    pub(crate) fn into_non_error_query_response(self) -> Result<NonErrorQueryResponse, QueryError> {
        Ok(NonErrorQueryResponse {
            response: self.response.into_non_error_response()?,
            tracing_id: self.tracing_id,
            warnings: self.warnings,
        })
    }

    pub(crate) fn into_query_result(self) -> Result<QueryResult, QueryError> {
        self.into_non_error_query_response()?.into_query_result()
    }
}

impl NonErrorQueryResponse {
    pub(crate) fn as_set_keyspace(&self) -> Option<&result::SetKeyspace> {
        match &self.response {
            NonErrorResponse::Result(result::Result::SetKeyspace(sk)) => Some(sk),
            _ => None,
        }
    }

    pub(crate) fn as_schema_change(&self) -> Option<&result::SchemaChange> {
        match &self.response {
            NonErrorResponse::Result(result::Result::SchemaChange(sc)) => Some(sc),
            _ => None,
        }
    }

    pub(crate) fn into_query_result(self) -> Result<QueryResult, QueryError> {
        let (rows, paging_state, col_specs, serialized_size) = match self.response {
            NonErrorResponse::Result(result::Result::Rows(rs)) => (
                Some(rs.rows),
                rs.metadata.paging_state,
                rs.metadata.col_specs,
                rs.serialized_size,
            ),
            NonErrorResponse::Result(_) => (None, None, vec![], 0),
            _ => {
                return Err(QueryError::ProtocolError(
                    "Unexpected server response, expected Result or Error",
                ))
            }
        };

        Ok(QueryResult {
            rows,
            warnings: self.warnings,
            tracing_id: self.tracing_id,
            paging_state,
            col_specs,
            serialized_size,
        })
    }
}
