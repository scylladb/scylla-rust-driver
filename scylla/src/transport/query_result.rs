use crate::frame::response::cql_to_rust::{FromRow, FromRowError};
use crate::frame::response::result::ColumnSpec;
use crate::frame::response::result::Row;
use crate::transport::session::{IntoTypedRows, TypedRowIter};
use bytes::Bytes;
use thiserror::Error;
use uuid::Uuid;

/// Result of a single query  
/// Contains all rows returned by the database and some more information
#[derive(Default, Debug)]
pub struct QueryResult {
    /// Rows returned by the database.  
    /// Queries like `SELECT` will have `Some(Vec)`, while queries like `INSERT` will have `None`.  
    /// Can contain an empty Vec.
    pub rows: Option<Vec<Row>>,
    /// Warnings returned by the database
    pub warnings: Vec<String>,
    /// CQL Tracing uuid - can only be Some if tracing is enabled for this query
    pub tracing_id: Option<Uuid>,
    /// Paging state returned from the server
    pub paging_state: Option<Bytes>,
    /// Column specification returned from the server
    pub col_specs: Vec<ColumnSpec>,
}

impl QueryResult {
    /// Returns the number of received rows.  
    /// Fails when the query isn't of a type that could return rows, same as [`rows()`](QueryResult::rows).
    pub fn rows_num(&self) -> Result<usize, RowsExpectedError> {
        match &self.rows {
            Some(rows) => Ok(rows.len()),
            None => Err(RowsExpectedError),
        }
    }

    /// Returns the received rows when present.  
    /// If `QueryResult.rows` is `None`, which means that this query is not supposed to return rows (e.g `INSERT`), returns an error.  
    /// Can return an empty `Vec`.
    pub fn rows(self) -> Result<Vec<Row>, RowsExpectedError> {
        match self.rows {
            Some(rows) => Ok(rows),
            None => Err(RowsExpectedError),
        }
    }

    /// Returns the received rows parsed as the given type.  
    /// Equal to `rows()?.into_typed()`.  
    /// Fails when the query isn't of a type that could return rows, same as [`rows()`](QueryResult::rows).
    pub fn rows_typed<RowT: FromRow>(self) -> Result<TypedRowIter<RowT>, RowsExpectedError> {
        Ok(self.rows()?.into_typed())
    }

    /// Returns `Ok` for a result of a query that shouldn't contain any rows.  
    /// Will return `Ok` for `INSERT` result, but a `SELECT` result, even an empty one, will cause an error.  
    /// Opposite of [`rows()`](QueryResult::rows).
    pub fn result_not_rows(&self) -> Result<(), RowsNotExpectedError> {
        match self.rows {
            Some(_) => Err(RowsNotExpectedError),
            None => Ok(()),
        }
    }

    /// Returns rows when `QueryResult.rows` is `Some`, otherwise an empty Vec.  
    /// Equal to `rows().unwrap_or_default()`.
    pub fn rows_or_empty(self) -> Vec<Row> {
        self.rows.unwrap_or_default()
    }

    /// Returns rows parsed as the given type.  
    /// When `QueryResult.rows` is `None`, returns 0 rows.  
    /// Equal to `rows_or_empty().into_typed::<RowT>()`.
    pub fn rows_typed_or_empty<RowT: FromRow>(self) -> TypedRowIter<RowT> {
        self.rows_or_empty().into_typed::<RowT>()
    }

    /// Returns first row from the received rows.  
    /// When the first row is not available, returns an error.
    pub fn first_row(self) -> Result<Row, FirstRowError> {
        match self.maybe_first_row()? {
            Some(row) => Ok(row),
            None => Err(FirstRowError::RowsEmpty),
        }
    }

    /// Returns first row from the received rows parsed as the given type.  
    /// When the first row is not available, returns an error.
    pub fn first_row_typed<RowT: FromRow>(self) -> Result<RowT, FirstRowTypedError> {
        Ok(self.first_row()?.into_typed()?)
    }

    /// Returns `Option<RowT>` containing the first of a result.  
    /// Fails when the query isn't of a type that could return rows, same as [`rows()`](QueryResult::rows).
    pub fn maybe_first_row(self) -> Result<Option<Row>, RowsExpectedError> {
        Ok(self.rows()?.into_iter().next())
    }

    /// Returns `Option<RowT>` containing the first of a result.  
    /// Fails when the query isn't of a type that could return rows, same as [`rows()`](QueryResult::rows).
    pub fn maybe_first_row_typed<RowT: FromRow>(
        self,
    ) -> Result<Option<RowT>, MaybeFirstRowTypedError> {
        match self.maybe_first_row()? {
            Some(row) => Ok(Some(row.into_typed::<RowT>()?)),
            None => Ok(None),
        }
    }

    /// Returns the only received row.  
    /// Fails if the result is anything else than a single row.  
    pub fn single_row(self) -> Result<Row, SingleRowError> {
        let rows: Vec<Row> = self.rows()?;

        if rows.len() != 1 {
            return Err(SingleRowError::BadNumberOfRows(rows.len()));
        }

        Ok(rows.into_iter().next().unwrap())
    }

    /// Returns the only received row parsed as the given type.  
    /// Fails if the result is anything else than a single row.  
    pub fn single_row_typed<RowT: FromRow>(self) -> Result<RowT, SingleRowTypedError> {
        Ok(self.single_row()?.into_typed::<RowT>()?)
    }

    /// Returns a column specification for a column with given name, or None if not found
    pub fn get_column_spec<'a>(&'a self, name: &str) -> Option<(usize, &'a ColumnSpec)> {
        self.col_specs
            .iter()
            .enumerate()
            .find(|(_id, spec)| spec.name == name)
    }
}

/// [`QueryResult::rows()`](QueryResult::rows) or a similar function called on a bad QueryResult.  
/// Expected `QueryResult.rows` to be `Some`, but it was `None`.  
/// `QueryResult.rows` is `Some` for queries that can return rows (e.g `SELECT`).  
/// It is `None` for queries that can't return rows (e.g `INSERT`).
#[derive(Debug, Error, PartialEq, Eq)]
#[error(
    "QueryResult::rows() or similar function called on a bad QueryResult.
         Expected QueryResult.rows to be Some, but it was None.
         QueryResult.rows is Some for queries that can return rows (e.g SELECT).
         It is None for queries that can't return rows (e.g INSERT)."
)]
pub struct RowsExpectedError;

/// [`QueryResult::result_not_rows()`](QueryResult::result_not_rows) called on a bad QueryResult.  
/// Expected `QueryResult.rows` to be `None`, but it was `Some`.  
/// `QueryResult.rows` is `Some` for queries that can return rows (e.g `SELECT`).  
/// It is `None` for queries that can't return rows (e.g `INSERT`).
#[derive(Debug, Error, PartialEq, Eq)]
#[error(
    "QueryResult::result_not_rows() called on a bad QueryResult.
         Expected QueryResult.rows to be None, but it was Some.
         QueryResult.rows is Some for queries that can return rows (e.g SELECT).
         It is None for queries that can't return rows (e.g INSERT)."
)]
pub struct RowsNotExpectedError;

#[derive(Debug, Error, PartialEq, Eq)]
pub enum FirstRowError {
    /// [`QueryResult::first_row()`](QueryResult::first_row) called on a bad QueryResult.  
    /// Expected `QueryResult.rows` to be `Some`, but it was `None`.  
    /// `QueryResult.rows` is `Some` for queries that can return rows (e.g `SELECT`).  
    /// It is `None` for queries that can't return rows (e.g `INSERT`).
    #[error(transparent)]
    RowsExpected(#[from] RowsExpectedError),

    /// Rows in `QueryResult` are empty
    #[error("Rows in QueryResult are empty")]
    RowsEmpty,
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum FirstRowTypedError {
    /// [`QueryResult::first_row_typed()`](QueryResult::first_row_typed) called on a bad QueryResult.  
    /// Expected `QueryResult.rows` to be `Some`, but it was `None`.  
    /// `QueryResult.rows` is `Some` for queries that can return rows (e.g `SELECT`).  
    /// It is `None` for queries that can't return rows (e.g `INSERT`).
    #[error(transparent)]
    RowsExpected(#[from] RowsExpectedError),

    /// Rows in `QueryResult` are empty
    #[error("Rows in QueryResult are empty")]
    RowsEmpty,

    /// Parsing row as the given type failed
    #[error(transparent)]
    FromRowError(#[from] FromRowError),
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum MaybeFirstRowTypedError {
    /// [`QueryResult::maybe_first_row_typed()`](QueryResult::maybe_first_row_typed) called on a bad QueryResult.  
    /// Expected `QueryResult.rows` to be `Some`, but it was `None`.
    /// `QueryResult.rows` is `Some` for queries that can return rows (e.g `SELECT`).  
    /// It is `None` for queries that can't return rows (e.g `INSERT`).
    #[error(transparent)]
    RowsExpected(#[from] RowsExpectedError),

    /// Parsing row as the given type failed
    #[error(transparent)]
    FromRowError(#[from] FromRowError),
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum SingleRowError {
    /// [`QueryResult::single_row()`](QueryResult::single_row) called on a bad QueryResult.  
    /// Expected `QueryResult.rows` to be `Some`, but it was `None`.  
    /// `QueryResult.rows` is `Some` for queries that can return rows (e.g `SELECT`).  
    /// It is `None` for queries that can't return rows (e.g `INSERT`).
    #[error(transparent)]
    RowsExpected(#[from] RowsExpectedError),

    /// Expected a single row, found other number of rows
    #[error("Expected a single row, found {0} rows")]
    BadNumberOfRows(usize),
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum SingleRowTypedError {
    /// [`QueryResult::single_row_typed()`](QueryResult::single_row_typed) called on a bad QueryResult.  
    /// Expected `QueryResult.rows` to be `Some`, but it was `None`.  
    /// `QueryResult.rows` is `Some` for queries that can return rows (e.g `SELECT`).  
    /// It is `None` for queries that can't return rows (e.g `INSERT`).
    #[error(transparent)]
    RowsExpected(#[from] RowsExpectedError),

    /// Expected a single row, found other number of rows
    #[error("Expected a single row, found {0} rows")]
    BadNumberOfRows(usize),

    /// Parsing row as the given type failed
    #[error(transparent)]
    FromRowError(#[from] FromRowError),
}

impl From<FirstRowError> for FirstRowTypedError {
    fn from(err: FirstRowError) -> FirstRowTypedError {
        match err {
            FirstRowError::RowsExpected(e) => FirstRowTypedError::RowsExpected(e),
            FirstRowError::RowsEmpty => FirstRowTypedError::RowsEmpty,
        }
    }
}

impl From<SingleRowError> for SingleRowTypedError {
    fn from(err: SingleRowError) -> SingleRowTypedError {
        match err {
            SingleRowError::RowsExpected(e) => SingleRowTypedError::RowsExpected(e),
            SingleRowError::BadNumberOfRows(r) => SingleRowTypedError::BadNumberOfRows(r),
        }
    }
}
