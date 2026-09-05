//! The ways to send a single statement, as values that a test can loop over.
//!
//! A statement is sent through one of six `Session` methods.
//! Two independent axes select the method:
//! - Is the statement prepared or unprepared?
//! - How is the response fetched? In one shot, iterated, or one page at a time?
//!
//! [`PagingMode`] holds the second axis. It makes each of the six calls in
//! exactly one place. [`EntryPoint`] adds the first axis. Use it in tests that
//! have only the statement text.
//!
//! The six methods do not share an error type:
//! - The iterating methods return a [`PagerExecutionError`].
//! - The other methods return an [`ExecutionError`].
//!
//! [`SendError`] holds the failure of any of them. It also knows where each
//! kind of failure is expected to occur.

use scylla::client::session::Session;
use scylla::errors::{ExecutionError, PagerExecutionError, SchemaAgreementError};
use scylla::response::PagingState;
use scylla::serialize::row::SerializeRow;
use scylla::statement::Statement;
use scylla::statement::prepared::PreparedStatement;

/// How the response to a statement is fetched: in one shot, iterated, or one
/// page at a time.
///
/// This does not depend on whether the statement is prepared. Thus, the caller
/// builds and configures the statement before it passes it in.
#[derive(Clone, Copy, Debug)]
pub(crate) enum PagingMode {
    Unpaged,
    Iter,
    SinglePage,
}

/// One of the six ways to send a single statement: a [`PagingMode`],
/// prepared or unprepared.
#[derive(Clone, Copy, Debug)]
pub(crate) enum EntryPoint {
    QueryUnpaged,
    QueryIter,
    QuerySinglePage,
    ExecuteUnpaged,
    ExecuteIter,
    ExecuteSinglePage,
}

/// The error that a send failed with.
///
/// The iterating entry points return a [`PagerExecutionError`]. The other entry
/// points return an [`ExecutionError`]. Thus, one error type cannot hold every
/// failure.
#[derive(Debug)]
pub(crate) enum SendError {
    Execution(ExecutionError),
    Pager(PagerExecutionError),
}

impl PagingMode {
    /// Sends an unprepared statement. The caller configures it first.
    // `ExecutionError` is a large enum. A box here makes a match on `SendError`
    // harder to read.
    #[allow(clippy::result_large_err)]
    pub(crate) async fn send_unprepared(
        self,
        session: &Session,
        stmt: impl Into<Statement>,
        values: impl SerializeRow,
    ) -> Result<(), SendError> {
        match self {
            PagingMode::Unpaged => session
                .query_unpaged(stmt, values)
                .await
                .map(|_| ())
                .map_err(SendError::Execution),
            PagingMode::Iter => session
                .query_iter(stmt, values)
                .await
                .map(|_| ())
                .map_err(SendError::Pager),
            PagingMode::SinglePage => session
                .query_single_page(stmt, values, PagingState::start())
                .await
                .map(|_| ())
                .map_err(SendError::Execution),
        }
    }

    /// Sends a prepared statement. The caller configures it first.
    #[allow(clippy::result_large_err)]
    pub(crate) async fn send_prepared(
        self,
        session: &Session,
        stmt: &PreparedStatement,
        values: impl SerializeRow,
    ) -> Result<(), SendError> {
        match self {
            PagingMode::Unpaged => session
                .execute_unpaged(stmt, values)
                .await
                .map(|_| ())
                .map_err(SendError::Execution),
            // `execute_iter` takes the statement by value. The clone only copies
            // a few `Arc`s.
            PagingMode::Iter => session
                .execute_iter(stmt.clone(), values)
                .await
                .map(|_| ())
                .map_err(SendError::Pager),
            PagingMode::SinglePage => session
                .execute_single_page(stmt, values, PagingState::start())
                .await
                .map(|_| ())
                .map_err(SendError::Execution),
        }
    }
}

impl EntryPoint {
    pub(crate) const ALL: [EntryPoint; 6] = [
        EntryPoint::QueryUnpaged,
        EntryPoint::QueryIter,
        EntryPoint::QuerySinglePage,
        EntryPoint::ExecuteUnpaged,
        EntryPoint::ExecuteIter,
        EntryPoint::ExecuteSinglePage,
    ];

    /// The name of the `Session` method this entry point calls.
    pub(crate) fn name(self) -> &'static str {
        match self {
            EntryPoint::QueryUnpaged => "query_unpaged",
            EntryPoint::QueryIter => "query_iter",
            EntryPoint::QuerySinglePage => "query_single_page",
            EntryPoint::ExecuteUnpaged => "execute_unpaged",
            EntryPoint::ExecuteIter => "execute_iter",
            EntryPoint::ExecuteSinglePage => "execute_single_page",
        }
    }

    fn paging_mode(self) -> PagingMode {
        match self {
            EntryPoint::QueryUnpaged | EntryPoint::ExecuteUnpaged => PagingMode::Unpaged,
            EntryPoint::QueryIter | EntryPoint::ExecuteIter => PagingMode::Iter,
            EntryPoint::QuerySinglePage | EntryPoint::ExecuteSinglePage => PagingMode::SinglePage,
        }
    }

    /// Whether the statement is prepared before it is sent.
    ///
    /// This decides when the values are serialized. Thus, it also decides
    /// how a bad value list is reported.
    pub(crate) fn is_prepared(self) -> bool {
        match self {
            EntryPoint::QueryUnpaged | EntryPoint::QueryIter | EntryPoint::QuerySinglePage => false,
            EntryPoint::ExecuteUnpaged
            | EntryPoint::ExecuteIter
            | EntryPoint::ExecuteSinglePage => true,
        }
    }

    /// Sends `stmt` with `values` and discards the response.
    ///
    /// The prepared entry points prepare `stmt` first. The prepared statement
    /// inherits the configuration of `stmt`. Thus, the caller configures only
    /// the statement that it passes in.
    #[allow(clippy::result_large_err)]
    pub(crate) async fn send(
        self,
        session: &Session,
        stmt: impl Into<Statement>,
        values: impl SerializeRow,
    ) -> Result<(), SendError> {
        let stmt = stmt.into();
        if self.is_prepared() {
            let prepared = match session.prepare(stmt).await {
                Ok(prepared) => prepared,
                Err(err) => {
                    return Err(match self.paging_mode() {
                        PagingMode::Iter => SendError::Pager(err.into()),
                        PagingMode::Unpaged | PagingMode::SinglePage => {
                            SendError::Execution(err.into())
                        }
                    });
                }
            };
            self.paging_mode()
                .send_prepared(session, &prepared, values)
                .await
        } else {
            self.paging_mode()
                .send_unprepared(session, stmt, values)
                .await
        }
    }
}

impl SendError {
    /// The schema agreement error this failure carries. Both entry point
    /// families wrap one, in `ExecutionError::SchemaAgreementError` and
    /// `PagerExecutionError::SchemaAgreementError` respectively.
    pub(crate) fn into_schema_agreement_error(
        self,
        entry_point: EntryPoint,
    ) -> SchemaAgreementError {
        match self {
            SendError::Execution(ExecutionError::SchemaAgreementError(err))
            | SendError::Pager(PagerExecutionError::SchemaAgreementError(err)) => err,
            other => panic!(
                "{} failed with an unexpected error: {other:?}",
                entry_point.name()
            ),
        }
    }
}
