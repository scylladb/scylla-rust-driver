//! The ways a single statement can be sent, as values a test can loop over.
//!
//! A statement is sent through one of six `Session` methods: it is either
//! unprepared or prepared, and its response is fetched in one shot, iterated,
//! or one page at a time. Those two axes are independent, so [`PagingMode`]
//! holds the second one and makes the six calls exactly once, and
//! [`EntryPoint`] pairs it with the first for tests that have nothing but the
//! statement text.
//!
//! The six do not share an error type - the iterating ones return a
//! [`PagerExecutionError`] and the rest an [`ExecutionError`] - so [`SendError`]
//! carries the failure of any of them, and knows where each kind of failure is
//! expected to surface.

use assert_matches::assert_matches;
use scylla::client::session::Session;
use scylla::errors::{
    BadQuery, DbError, ExecutionError, NextPageError, PagerExecutionError, RequestAttemptError,
    RequestError, SchemaAgreementError,
};
use scylla::response::PagingState;
use scylla::serialize::SerializationError;
use scylla::serialize::row::SerializeRow;
use scylla::statement::Statement;
use scylla::statement::prepared::PreparedStatement;

/// How a statement's response is fetched: in one shot, iterated, or one page at
/// a time. Independent of whether the statement is prepared, which is why the
/// caller passes the statement in already built and configured.
#[derive(Clone, Copy, Debug)]
pub(crate) enum PagingMode {
    Unpaged,
    Iter,
    SinglePage,
}

/// One of the six ways a single statement can be sent: a [`PagingMode`],
/// prepared or not.
#[derive(Clone, Copy, Debug)]
pub(crate) enum EntryPoint {
    QueryUnpaged,
    QueryIter,
    QuerySinglePage,
    ExecuteUnpaged,
    ExecuteIter,
    ExecuteSinglePage,
}

/// The error a send failed with. The iterating entry points return a
/// [`PagerExecutionError`] and the rest an [`ExecutionError`], so a failure
/// cannot be reported in one type.
#[derive(Debug)]
pub(crate) enum SendError {
    Execution(ExecutionError),
    Pager(PagerExecutionError),
}

impl PagingMode {
    /// Sends an unprepared statement, already configured by the caller.
    // `ExecutionError` is a large enum, and boxing it here would only make
    // matching on `SendError` harder to read.
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

    /// Sends a prepared statement, already configured by the caller.
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
            // `execute_iter` takes the statement by value; the clone is a
            // handful of `Arc`s.
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

    /// Whether the statement is prepared before being sent. It decides when the
    /// values are serialized, and so how a bad value list is reported.
    pub(crate) fn is_prepared(self) -> bool {
        match self {
            EntryPoint::QueryUnpaged | EntryPoint::QueryIter | EntryPoint::QuerySinglePage => false,
            EntryPoint::ExecuteUnpaged
            | EntryPoint::ExecuteIter
            | EntryPoint::ExecuteSinglePage => true,
        }
    }

    /// Sends `stmt` with `values`, discarding the response. The prepared entry
    /// points prepare it first, and the prepared statement inherits its
    /// configuration, so the caller only ever configures the one it passes in.
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
    /// The serialization error this failure carries, asserting on the way that
    /// it surfaced where it is supposed to.
    ///
    /// A prepared statement knows its bind markers before the request is sent,
    /// so it rejects the values up front, as a `BadQuery`. An unprepared one
    /// learns them only from the response to its own request, so it fails per
    /// attempt, as a `LastAttemptError`. Either way an iterating entry point
    /// reports it as a `PagerExecutionError` of its own.
    pub(crate) fn into_serialization_error(self, entry_point: EntryPoint) -> SerializationError {
        match self {
            SendError::Pager(PagerExecutionError::SerializationError(err)) => err,
            SendError::Execution(ExecutionError::BadQuery(BadQuery::SerializationError(err)))
                if entry_point.is_prepared() =>
            {
                err
            }
            SendError::Execution(ExecutionError::LastAttemptError(
                RequestAttemptError::SerializationError(err),
            )) if !entry_point.is_prepared() => err,
            other => panic!(
                "{} failed with an unexpected error: {other:?}",
                entry_point.name()
            ),
        }
    }

    /// Asserts that this failure is the database rejecting the request with
    /// `Invalid`, and that its message mentions `substring`.
    pub(crate) fn assert_is_invalid_db_error(self, entry_point: EntryPoint, substring: &str) {
        let db_error = match self {
            SendError::Execution(ExecutionError::LastAttemptError(
                RequestAttemptError::DbError(err, message),
            )) => (err, message),
            SendError::Pager(PagerExecutionError::NextPageError(
                NextPageError::RequestFailure(RequestError::LastAttemptError(
                    RequestAttemptError::DbError(err, message),
                )),
            )) => (err, message),
            other => panic!(
                "{} failed with an unexpected error: {other:?}",
                entry_point.name()
            ),
        };
        assert_matches!(&db_error, (DbError::Invalid, message) if message.contains(substring));
    }

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
