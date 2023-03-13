use bytes::Bytes;
use thiserror::Error;
use uuid::Uuid;

use scylla_cql::frame::frame_errors::ParseError;
use scylla_cql::frame::response::result::{ColumnSpec, RawRows, Row};
use scylla_cql::types::deserialize::row::DeserializeRow;
use scylla_cql::types::deserialize::TypedRowIterator;

use super::legacy_query_result::Legacy08QueryResult;

/// Raw results of a single query.
///
/// More comprehensive description TODO
#[derive(Default, Debug)]
pub struct QueryResult {
    raw_rows: Option<RawRows>,
    tracing_id: Option<Uuid>,
    warnings: Vec<String>,
}

impl QueryResult {
    pub(crate) fn new(
        raw_rows: Option<RawRows>,
        tracing_id: Option<Uuid>,
        warnings: Vec<String>,
    ) -> Self {
        Self {
            raw_rows,
            tracing_id,
            warnings,
        }
    }

    /// Returns the number of received rows, or `None` if the response wasn't of Rows type.
    pub fn rows_num(&self) -> Option<usize> {
        Some(self.raw_rows.as_ref()?.rows_count())
    }

    /// Returns a bool indicating the current response is of Rows type.
    pub fn is_rows(&self) -> bool {
        self.raw_rows.is_some()
    }

    /// Returns the received rows when present.
    ///
    /// Returns an error if the original query didn't return a Rows response (e.g. it was an `INSERT`),
    /// or the response is of incorrect type.
    pub fn rows<'s, R: DeserializeRow<'s>>(&'s self) -> Result<TypedRowIterator<'s, R>, RowsError> {
        Ok(self
            .raw_rows
            .as_ref()
            .ok_or(RowsError::NotRowsResponse)?
            .rows_iter()?)
    }

    /// Returns the received rows when present, or None.
    ///
    /// Returns an error if the rows are present but are of incorrect type.
    pub fn maybe_rows<'s, R: DeserializeRow<'s>>(
        &'s self,
    ) -> Result<Option<TypedRowIterator<'s, R>>, ParseError> {
        match &self.raw_rows {
            Some(rows) => Ok(Some(rows.rows_iter()?)),
            None => Ok(None),
        }
    }

    /// Returns `Ok` for a result of a query that shouldn't contain any rows.\
    /// Will return `Ok` for `INSERT` result, but a `SELECT` result, even an empty one, will cause an error.\
    /// Opposite of [`rows()`](QueryResult::rows).
    pub fn result_not_rows(&self) -> Result<(), ResultNotRowsError> {
        match &self.raw_rows {
            Some(_) => Err(ResultNotRowsError),
            None => Ok(()),
        }
    }

    /// Returns first row from the received rows.\
    /// When the first row is not available, returns an error.
    pub fn first_row<'s, R: DeserializeRow<'s>>(&'s self) -> Result<R, FirstRowError> {
        match self.maybe_first_row::<R>() {
            Ok(Some(row)) => Ok(row),
            Ok(None) => Err(FirstRowError::RowsEmpty),
            Err(RowsError::NotRowsResponse) => Err(FirstRowError::NotRowsResponse),
            Err(RowsError::TypeCheckFailed(err)) => Err(FirstRowError::TypeCheckFailed(err)),
        }
    }

    /// Returns `Option<R>` containing the first of a result.\
    /// Fails when the query isn't of a type that could return rows, same as [`rows()`](QueryResult::rows).
    pub fn maybe_first_row<'s, R: DeserializeRow<'s>>(&'s self) -> Result<Option<R>, RowsError> {
        Ok(self.rows::<R>()?.next().transpose()?)
    }

    /// Returns the only received row.\
    /// Fails if the result is anything else than a single row.\
    pub fn single_row<'s, R: DeserializeRow<'s>>(&'s self) -> Result<R, SingleRowError> {
        match self.rows::<R>() {
            Ok(mut rows) => match rows.next() {
                Some(Ok(row)) => {
                    if rows.rows_remaining() != 0 {
                        return Err(SingleRowError::UnexpectedRowCount(
                            rows.rows_remaining() + 1,
                        ));
                    }
                    Ok(row)
                }
                Some(Err(err)) => Err(err.into()),
                None => Err(SingleRowError::UnexpectedRowCount(0)),
            },
            Err(RowsError::NotRowsResponse) => Err(SingleRowError::NotRowsResponse),
            Err(RowsError::TypeCheckFailed(err)) => Err(SingleRowError::TypeCheckFailed(err)),
        }
    }

    /// Returns a column specification for a column with given name, or None if not found
    pub fn get_column_spec<'a>(&'a self, name: &str) -> Option<(usize, &'a ColumnSpec)> {
        self.raw_rows
            .as_ref()?
            .metadata()
            .col_specs
            .iter()
            .enumerate()
            .find(|(_id, spec)| spec.name == name)
    }

    pub fn column_specs(&self) -> Option<&[ColumnSpec]> {
        Some(self.raw_rows.as_ref()?.metadata().col_specs.as_slice())
    }

    pub fn warnings(&self) -> impl Iterator<Item = &str> {
        self.warnings.iter().map(String::as_str)
    }

    pub fn paging_state(&self) -> Option<Bytes> {
        self.raw_rows.as_ref()?.metadata().paging_state.clone()
    }

    pub fn tracing_id(&self) -> Option<Uuid> {
        self.tracing_id
    }

    pub fn into_legacy_result(self) -> Result<Legacy08QueryResult, ParseError> {
        if let Some(raw_rows) = self.raw_rows {
            let deserialized_rows = raw_rows
                .rows_iter::<Row>()?
                .collect::<Result<Vec<_>, ParseError>>()?;
            let serialized_size = raw_rows.rows_size();
            let metadata = raw_rows.into_metadata();
            Ok(Legacy08QueryResult {
                rows: Some(deserialized_rows),
                warnings: self.warnings,
                tracing_id: self.tracing_id,
                paging_state: metadata.paging_state,
                col_specs: metadata.col_specs,
                serialized_size,
            })
        } else {
            Ok(Legacy08QueryResult {
                rows: None,
                warnings: self.warnings,
                tracing_id: self.tracing_id,
                paging_state: None,
                col_specs: Vec::new(),
                serialized_size: 0,
            })
        }
    }
}

/// An error returned by [`QueryResult::rows`] or [`QueryResult::maybe_first_row`].
#[derive(Debug, Error)]
pub enum RowsError {
    /// The query response was not a Rows response
    #[error("The query response was not a Rows response")]
    NotRowsResponse,

    /// Type check failed
    #[error("Type check failed: {0}")]
    TypeCheckFailed(#[from] ParseError),
}

/// An error returned by [`QueryResult::first_row`].
#[derive(Debug, Error)]
pub enum FirstRowError {
    /// The query response was not a Rows response
    #[error("The query response was not a Rows response")]
    NotRowsResponse,

    /// The query response was of Rows type, but no rows were returned
    #[error("The query response was of Rows type, but no rows were returned")]
    RowsEmpty,

    /// Type check failed
    #[error("Type check failed: {0}")]
    TypeCheckFailed(#[from] ParseError),
}

/// An error returned by [`QueryResult::single_row`].
#[derive(Debug, Error)]
pub enum SingleRowError {
    /// The query response was not a Rows response
    #[error("The query response was not a Rows response")]
    NotRowsResponse,

    /// Expected one row, but got a different count
    #[error("Expected a single row, but got {0}")]
    UnexpectedRowCount(usize),

    /// Type check failed
    #[error("Type check failed: {0}")]
    TypeCheckFailed(#[from] ParseError),
}

/// An error returned by [`QueryResult::result_not_rows`].
///
/// It indicates that response to the query was, unexpectedly, of Rows kind.
#[derive(Debug, Error)]
#[error("The query response was, unexpectedly, of Rows kind")]
pub struct ResultNotRowsError;

// TODO: Tests
