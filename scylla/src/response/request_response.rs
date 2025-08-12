use std::collections::HashMap;
use std::sync::Arc;

use bytes::Bytes;
use scylla_cql::frame::request::query::PagingStateResponse;
use scylla_cql::frame::response::{NonErrorResponse, Response};
use tracing::error;
use uuid::Uuid;

use crate::errors::RequestAttemptError;
use crate::frame::response::{self, result};
use crate::response::Coordinator;
use crate::response::query_result::QueryResult;
use crate::statement::Statement;
use crate::statement::prepared::PreparedStatement;

pub(crate) struct QueryResponse {
    pub(crate) response: Response,
    pub(crate) tracing_id: Option<Uuid>,
    pub(crate) warnings: Vec<String>,
    // This is not exposed to user (yet?)
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

    fn into_query_result_and_paging_state_with_maybe_unknown_coordinator(
        self,
        request_coordinator: Option<Coordinator>,
    ) -> Result<(QueryResult, PagingStateResponse), RequestAttemptError> {
        let Self {
            response,
            tracing_id,
            warnings,
        } = self;
        let (raw_rows, paging_state_response) = match response {
            NonErrorResponse::Result(result::Result::Rows((rs, paging_state_response))) => {
                (Some(rs), paging_state_response)
            }
            NonErrorResponse::Result(_) => (None, PagingStateResponse::NoMorePages),
            _ => {
                return Err(RequestAttemptError::UnexpectedResponse(
                    response.to_response_kind(),
                ));
            }
        };

        Ok((
            match request_coordinator {
                Some(coordinator) => QueryResult::new(coordinator, raw_rows, tracing_id, warnings),
                None => QueryResult::new_with_unknown_coordinator(raw_rows, tracing_id, warnings),
            },
            paging_state_response,
        ))
    }

    /// Converts [NonErrorQueryResponse] into [QueryResult] and the associated [PagingStateResponse].
    pub(crate) fn into_query_result_and_paging_state(
        self,
        request_coordinator: Coordinator,
    ) -> Result<(QueryResult, PagingStateResponse), RequestAttemptError> {
        self.into_query_result_and_paging_state_with_maybe_unknown_coordinator(Some(
            request_coordinator,
        ))
    }

    fn into_query_result_with_maybe_unknown_coordinator(
        self,
        request_coordinator: Option<Coordinator>,
    ) -> Result<QueryResult, RequestAttemptError> {
        let (result, paging_state) = self
            .into_query_result_and_paging_state_with_maybe_unknown_coordinator(
                request_coordinator,
            )?;

        if !paging_state.finished() {
            error!(
                "Internal driver API misuse or a server bug: nonfinished paging state\
                would be discarded by `NonErrorQueryResponse::into_query_result`"
            );
            return Err(RequestAttemptError::NonfinishedPagingState);
        }

        Ok(result)
    }

    /// Converts [NonErrorQueryResponse] into [QueryResult]. Because it's intended to be used together with unpaged queries,
    /// it asserts that the associated [PagingStateResponse] is <finished> (says that there are no more pages left).
    pub(crate) fn into_query_result(
        self,
        request_coordinator: Coordinator,
    ) -> Result<QueryResult, RequestAttemptError> {
        self.into_query_result_with_maybe_unknown_coordinator(Some(request_coordinator))
    }

    /// The same as [Self::into_query_result()], but not omitting the [Coordinator].
    /// HACK: This is the way to create a [QueryResult] with `request_coordinator` set to [None].
    ///
    /// See [QueryResult::new_with_unknown_coordinator]
    pub(crate) fn into_query_result_with_unknown_coordinator(
        self,
    ) -> Result<QueryResult, RequestAttemptError> {
        self.into_query_result_with_maybe_unknown_coordinator(None)
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
