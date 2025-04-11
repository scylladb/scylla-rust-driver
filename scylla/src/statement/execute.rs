use scylla_cql::{
    frame::request::query::{PagingState, PagingStateResponse},
    serialize::row::SerializeRow,
};
use tracing::error;

use crate::{
    client::session::Session,
    errors::{ExecutionError, RequestAttemptError},
    response::query_result::QueryResult,
};

use super::{batch::BoundBatch, bound::BoundStatement, Statement};

// seals the trait to foreign implementations
mod private {
    #[allow(unnameable_types)]
    pub trait Sealed {}
}

/// A type that can be executed on a [`Session`] without any additional values
///
/// In practice this means that the statement(s) all already had their values bound.
pub trait Execute: private::Sealed {
    /// Executes on the session
    fn execute(
        &self,
        session: &Session,
    ) -> impl std::future::Future<Output = Result<QueryResult, ExecutionError>>;
}

/// A type that can be executed on a [`Session`] and is aware of pagination
pub trait ExecutePageable {
    /// Executes a command with the `paging_state` determining where the results should start
    ///
    /// If SINGLE_PAGE is set to true then a single page is returned. If SINGLE_PAGE is set to
    /// false, then all pages (starting at `paging_state`) are returned
    fn execute_pageable<const SINGLE_PAGE: bool>(
        &self,
        session: &Session,
        paging_state: PagingState,
    ) -> impl std::future::Future<Output = Result<(QueryResult, PagingStateResponse), ExecutionError>>;
}

impl<T: ExecutePageable + private::Sealed> Execute for T {
    /// Executes the pageable type but getting all pages from the start
    async fn execute(&self, session: &Session) -> Result<QueryResult, ExecutionError> {
        let (result, paging_state) = self
            .execute_pageable::<false>(session, PagingState::start())
            .await?;

        if !paging_state.finished() {
            error!("Unpaged query returned a non-empty paging state! This is a driver-side or server-side bug.");
            return Err(ExecutionError::LastAttemptError(
                RequestAttemptError::NonfinishedPagingState,
            ));
        }

        Ok(result)
    }
}

impl private::Sealed for BoundBatch {}

impl Execute for BoundBatch {
    async fn execute(&self, session: &Session) -> Result<QueryResult, ExecutionError> {
        session.do_batch(self).await
    }
}

impl private::Sealed for BoundStatement {}

impl ExecutePageable for BoundStatement {
    async fn execute_pageable<const SINGLE_PAGE: bool>(
        &self,
        session: &Session,
        paging_state: PagingState,
    ) -> Result<(QueryResult, PagingStateResponse), ExecutionError> {
        let page_size = if SINGLE_PAGE {
            Some(self.prepared.get_validated_page_size())
        } else {
            None
        };
        session
            .execute_bound_statement(self, page_size, paging_state)
            .await
    }
}

impl<V: SerializeRow> private::Sealed for (&Statement, V) {}

impl<V: SerializeRow> ExecutePageable for (&Statement, V) {
    async fn execute_pageable<const SINGLE_PAGE: bool>(
        &self,
        session: &Session,
        paging_state: PagingState,
    ) -> Result<(QueryResult, PagingStateResponse), ExecutionError> {
        let page_size = if SINGLE_PAGE {
            Some(self.0.get_validated_page_size())
        } else {
            None
        };

        session
            .query(self.0, &self.1, page_size, paging_state)
            .await
    }
}
