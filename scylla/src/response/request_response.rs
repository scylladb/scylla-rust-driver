use std::collections::HashMap;
use std::sync::Arc;

use bytes::Bytes;
use scylla_cql::frame::request::query::PagingStateResponse;
use scylla_cql::frame::response::{NonErrorResponse, Response};
use tracing::error;
use uuid::Uuid;

use crate::errors::RequestAttemptError;
use crate::frame::response::{self, result};
use crate::response::query_result::QueryResult;
use crate::statement::prepared::PreparedStatement;
use crate::statement::Statement;

pub(crate) struct QueryResponse {
    pub(crate) response: Response,
    pub(crate) tracing_id: Option<Uuid>,
    pub(crate) warnings: Vec<String>,
    #[allow(dead_code)] // This is not exposed to user (yet?)
    pub(crate) custom_payload: Option<HashMap<String, Bytes>>,
}

// A QueryResponse in which response can not be Response::Error
pub(crate) struct NonErrorQueryResponse {
    pub(crate) response: NonErrorResponse,
    pub(crate) tracing_id: Option<Uuid>,
    pub(crate) warnings: Vec<String>,
}

impl QueryResponse {
    pub(crate) fn into_non_error_query_response(
        self,
    ) -> Result<NonErrorQueryResponse, RequestAttemptError> {
        Ok(NonErrorQueryResponse {
            response: self.response.into_non_error_response()?,
            tracing_id: self.tracing_id,
            warnings: self.warnings,
        })
    }

    pub(crate) fn into_query_result(self) -> Result<QueryResult, RequestAttemptError> {
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

    pub(crate) fn into_query_result_and_paging_state(
        self,
    ) -> Result<(QueryResult, PagingStateResponse), RequestAttemptError> {
        let (raw_rows, paging_state_response) = match self.response {
            NonErrorResponse::Result(result::Result::Rows((rs, paging_state_response))) => {
                (Some(rs), paging_state_response)
            }
            NonErrorResponse::Result(_) => (None, PagingStateResponse::NoMorePages),
            _ => {
                return Err(RequestAttemptError::UnexpectedResponse(
                    self.response.to_response_kind(),
                ))
            }
        };

        Ok((
            QueryResult::new(raw_rows, self.tracing_id, self.warnings),
            paging_state_response,
        ))
    }

    pub(crate) fn into_query_result(self) -> Result<QueryResult, RequestAttemptError> {
        let (result, paging_state) = self.into_query_result_and_paging_state()?;

        if !paging_state.finished() {
            error!(
                "Internal driver API misuse or a server bug: nonfinished paging state\
                would be discarded by `NonErrorQueryResponse::into_query_result`"
            );
            return Err(RequestAttemptError::NonfinishedPagingState);
        }

        Ok(result)
    }
}

pub(crate) enum NonErrorStartupResponse {
    Ready,
    Authenticate(response::authenticate::Authenticate),
}

pub(crate) enum NonErrorAuthResponse {
    AuthChallenge(response::authenticate::AuthChallenge),
    AuthSuccess(response::authenticate::AuthSuccess),
}

/// Parts which are needed to construct [PreparedStatement].
///
/// Kept separate for performance reasons, because constructing
/// [PreparedStatement] involves allocations.
pub(crate) struct RawPreparedStatement<'statement> {
    pub(crate) statement: &'statement Statement,
    pub(crate) prepared_response: result::Prepared,
    pub(crate) is_lwt: bool,
    pub(crate) tracing_id: Option<Uuid>,
}

impl<'statement> RawPreparedStatement<'statement> {
    pub(crate) fn new(
        statement: &'statement Statement,
        prepared_response: result::Prepared,
        is_lwt: bool,
        tracing_id: Option<Uuid>,
    ) -> Self {
        Self {
            statement,
            prepared_response,
            is_lwt,
            tracing_id,
        }
    }
}

/// Constructs the fully-fledged [PreparedStatement].
///
/// This involves allocations.
impl RawPreparedStatement<'_> {
    pub(crate) fn into_prepared_statement(self) -> PreparedStatement {
        let Self {
            statement,
            prepared_response,
            is_lwt,
            tracing_id,
        } = self;
        let mut prepared_statement = PreparedStatement::new(
            prepared_response.id,
            is_lwt,
            prepared_response.prepared_metadata,
            Arc::new(prepared_response.result_metadata),
            statement.contents.clone(),
            statement.get_validated_page_size(),
            statement.config.clone(),
        );

        if let Some(tracing_id) = tracing_id {
            prepared_statement.prepare_tracing_ids.push(tracing_id);
        }

        prepared_statement
    }
}
