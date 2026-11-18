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

use assert_matches::assert_matches;
use futures::TryStreamExt as _;
use scylla::client::pager::QueryPager;
use scylla::client::session::Session;
use scylla::deserialize::row::DeserializeRow;
use scylla::errors::{
    BadQuery, DbError, ExecutionError, NextPageError, PagerExecutionError, RequestAttemptError,
    RequestError, SchemaAgreementError,
};
use scylla::response::query_result::QueryResult;
use scylla::response::{PagingState, PagingStateResponse};
use scylla::serialize::SerializationError;
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

/// The response to a statement, as the entry point returned it.
///
/// Each [`PagingMode`] returns a different type. Thus, this enum holds
/// whichever of them the call returned.
pub(crate) enum Response {
    Unpaged(QueryResult),
    Iter(QueryPager),
    SinglePage(QueryResult, PagingStateResponse),
}

/// The rows that a caller gets from a [`Response`].
pub(crate) struct Rows<R> {
    /// All rows for `Unpaged` and `Iter`. Only the rows of the first page for
    /// `SinglePage`.
    pub(crate) rows: Vec<R>,
    /// Whether the database has more pages after these rows. This is always
    /// `false` for `Unpaged` and `Iter`, because they fetch all the pages.
    pub(crate) has_more_pages: bool,
}

impl Response {
    /// Deserializes the rows of the response. Panics on a failure, because
    /// only a test calls this.
    ///
    /// An `Iter` response is drained, so this fetches all the pages.
    pub(crate) async fn into_rows<R>(self) -> Rows<R>
    where
        R: for<'frame, 'metadata> DeserializeRow<'frame, 'metadata>,
    {
        fn rows_of<R>(result: QueryResult) -> Vec<R>
        where
            R: for<'frame, 'metadata> DeserializeRow<'frame, 'metadata>,
        {
            result
                .into_rows_result()
                .unwrap()
                .rows::<R>()
                .unwrap()
                .collect::<Result<Vec<_>, _>>()
                .unwrap()
        }

        match self {
            Response::Unpaged(result) => Rows {
                rows: rows_of(result),
                has_more_pages: false,
            },
            Response::Iter(pager) => Rows {
                rows: pager
                    .rows_stream::<R>()
                    .unwrap()
                    .try_collect()
                    .await
                    .unwrap(),
                has_more_pages: false,
            },
            Response::SinglePage(result, paging_state_response) => Rows {
                rows: rows_of(result),
                has_more_pages: !paging_state_response.finished(),
            },
        }
    }
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
    /// Sends an unprepared statement and returns the response. The caller
    /// configures the statement first.
    // `ExecutionError` is a large enum. A box here makes a match on `SendError`
    // harder to read.
    #[allow(clippy::result_large_err)]
    pub(crate) async fn fetch_unprepared(
        self,
        session: &Session,
        stmt: impl Into<Statement>,
        values: impl SerializeRow,
    ) -> Result<Response, SendError> {
        match self {
            PagingMode::Unpaged => session
                .query_unpaged(stmt, values)
                .await
                .map(Response::Unpaged)
                .map_err(SendError::Execution),
            PagingMode::Iter => session
                .query_iter(stmt, values)
                .await
                .map(Response::Iter)
                .map_err(SendError::Pager),
            PagingMode::SinglePage => session
                .query_single_page(stmt, values, PagingState::start())
                .await
                .map(|(result, paging_state)| Response::SinglePage(result, paging_state))
                .map_err(SendError::Execution),
        }
    }

    /// Sends a prepared statement and returns the response. The caller
    /// configures the statement first.
    #[allow(clippy::result_large_err)]
    pub(crate) async fn fetch_prepared(
        self,
        session: &Session,
        stmt: &PreparedStatement,
        values: impl SerializeRow,
    ) -> Result<Response, SendError> {
        match self {
            PagingMode::Unpaged => session
                .execute_unpaged(stmt, values)
                .await
                .map(Response::Unpaged)
                .map_err(SendError::Execution),
            // `execute_iter` takes the statement by value. The clone only copies
            // a few `Arc`s.
            PagingMode::Iter => session
                .execute_iter(stmt.clone(), values)
                .await
                .map(Response::Iter)
                .map_err(SendError::Pager),
            PagingMode::SinglePage => session
                .execute_single_page(stmt, values, PagingState::start())
                .await
                .map(|(result, paging_state)| Response::SinglePage(result, paging_state))
                .map_err(SendError::Execution),
        }
    }

    /// Sends an unprepared statement and discards the response. The caller
    /// configures the statement first.
    #[allow(clippy::result_large_err)]
    pub(crate) async fn send_unprepared(
        self,
        session: &Session,
        stmt: impl Into<Statement>,
        values: impl SerializeRow,
    ) -> Result<(), SendError> {
        self.fetch_unprepared(session, stmt, values)
            .await
            .map(|_| ())
    }

    /// Sends a prepared statement and discards the response. The caller
    /// configures the statement first.
    #[allow(clippy::result_large_err)]
    pub(crate) async fn send_prepared(
        self,
        session: &Session,
        stmt: &PreparedStatement,
        values: impl SerializeRow,
    ) -> Result<(), SendError> {
        self.fetch_prepared(session, stmt, values).await.map(|_| ())
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

    /// Whether the call returns the rows of all the pages. The single page
    /// entry points return only the first page.
    pub(crate) fn fetches_all_pages(self) -> bool {
        !matches!(self.paging_mode(), PagingMode::SinglePage)
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

    /// Sends `stmt` with `values` and returns the response.
    ///
    /// The prepared entry points prepare `stmt` first. The prepared statement
    /// inherits the configuration of `stmt`. Thus, the caller configures only
    /// the statement that it passes in.
    #[allow(clippy::result_large_err)]
    pub(crate) async fn fetch(
        self,
        session: &Session,
        stmt: impl Into<Statement>,
        values: impl SerializeRow,
    ) -> Result<Response, SendError> {
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
                .fetch_prepared(session, &prepared, values)
                .await
        } else {
            self.paging_mode()
                .fetch_unprepared(session, stmt, values)
                .await
        }
    }

    /// Sends `stmt` with `values` and discards the response. See
    /// [`EntryPoint::fetch`].
    #[allow(clippy::result_large_err)]
    pub(crate) async fn send(
        self,
        session: &Session,
        stmt: impl Into<Statement>,
        values: impl SerializeRow,
    ) -> Result<(), SendError> {
        self.fetch(session, stmt, values).await.map(|_| ())
    }
}

impl SendError {
    /// Returns the serialization error in this failure. Also asserts that the
    /// error occurred in the expected place.
    ///
    /// The expected place depends on the entry point:
    /// - A prepared statement knows its bind markers before it sends the
    ///   request. Thus, it rejects the values before the send, as a `BadQuery`.
    /// - An unprepared statement learns its bind markers only from the response
    ///   to its request. Thus, each attempt fails, and the error is a
    ///   `LastAttemptError`.
    /// - An iterating entry point wraps either error in its own
    ///   `PagerExecutionError`.
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

    /// Asserts two things:
    /// - The database rejected the request with `Invalid`.
    /// - The error message contains `substring`.
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

    /// Returns the schema agreement error in this failure.
    ///
    /// Both entry point families wrap one:
    /// - `ExecutionError::SchemaAgreementError`,
    /// - `PagerExecutionError::SchemaAgreementError`.
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
