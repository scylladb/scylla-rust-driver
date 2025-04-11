//! Defines the [`Execute`] and [`ExecutePageable`] sealed traits which are implemented for types
//! that can be executed on a given session

use scylla_cql::frame::request::query::{PagingState, PagingStateResponse};
use tracing::error;

use crate::{
    client::session::Session,
    errors::{ExecutionError, RequestAttemptError},
    response::query_result::QueryResult,
};

// seals the trait to foreign implementations
mod private {
    use scylla_cql::serialize::row::SerializeRow;

    use crate::statement::{Statement, batch::BoundBatch, bound::BoundStatement};

    #[allow(unnameable_types)]
    pub trait Sealed {}

    impl Sealed for BoundBatch {}
    impl Sealed for BoundStatement<'_> {}
    impl<V: SerializeRow> Sealed for (&Statement, V) {}
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

/// A type that can be executed on a [`Session`], optionally conitnuing froma saved point
///
/// We believe that paging is such an important concept that we require users to make a conscious
/// decision to use paging or not. For that, we expose three different ways to execute pageable
/// requests:
///
/// * `Execute::execute`: unpaged and from the start
/// * `ExecutePageable::execute_pageable::<true>`: paginating from a saved point
/// * `ExecutePageable::execute_pageable::<false>`: no pagination from a saved point
pub trait ExecutePageable {
    /// Sends a request to the database, optionally continuing from a saved point.
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
            error!(
                "Unpaged prepared query returned a non-empty paging state! This is a driver-side or server-side bug."
            );
            return Err(ExecutionError::LastAttemptError(
                RequestAttemptError::NonfinishedPagingState,
            ));
        }

        Ok(result)
    }
}
