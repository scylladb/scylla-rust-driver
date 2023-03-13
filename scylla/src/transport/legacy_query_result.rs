use crate::frame::response::cql_to_rust::{FromRow, FromRowError};
use crate::frame::response::result::ColumnSpec;
use crate::frame::response::result::Row;
use crate::transport::session::{IntoTypedRows, TypedRowIter};
use scylla_cql::frame::response::result::ResultMetadataHolder;
use thiserror::Error;
use uuid::Uuid;

/// Result of a single query\
/// Contains all rows returned by the database and some more information
#[derive(Debug)]
pub struct LegacyQueryResult {
    /// Rows returned by the database.\
    /// Queries like `SELECT` will have `Some(Vec)`, while queries like `INSERT` will have `None`.\
    /// Can contain an empty Vec.
    pub rows: Option<Vec<Row>>,
    /// Warnings returned by the database
    pub warnings: Vec<String>,
    /// CQL Tracing uuid - can only be Some if tracing is enabled for this query
    pub tracing_id: Option<Uuid>,
    /// Metadata returned along with this response.
    pub(crate) metadata: Option<ResultMetadataHolder>,
    /// The original size of the serialized rows in request
    pub serialized_size: usize,
}

impl LegacyQueryResult {
    /// Returns the number of received rows.\
    /// Fails when the query isn't of a type that could return rows, same as [`rows()`](LegacyQueryResult::rows).
    pub fn rows_num(&self) -> Result<usize, RowsExpectedError> {
        match &self.rows {
            Some(rows) => Ok(rows.len()),
            None => Err(RowsExpectedError),
        }
    }

    /// Returns the received rows when present.\
    /// If `LegacyQueryResult.rows` is `None`, which means that this query is not supposed to return rows (e.g `INSERT`), returns an error.\
    /// Can return an empty `Vec`.
    pub fn rows(self) -> Result<Vec<Row>, RowsExpectedError> {
        match self.rows {
            Some(rows) => Ok(rows),
            None => Err(RowsExpectedError),
        }
    }

    /// Returns the received rows parsed as the given type.\
    /// Equal to `rows()?.into_typed()`.\
    /// Fails when the query isn't of a type that could return rows, same as [`rows()`](LegacyQueryResult::rows).
    pub fn rows_typed<RowT: FromRow>(self) -> Result<TypedRowIter<RowT>, RowsExpectedError> {
        Ok(self.rows()?.into_typed())
    }

    /// Returns `Ok` for a result of a query that shouldn't contain any rows.\
    /// Will return `Ok` for `INSERT` result, but a `SELECT` result, even an empty one, will cause an error.\
    /// Opposite of [`rows()`](LegacyQueryResult::rows).
    pub fn result_not_rows(&self) -> Result<(), RowsNotExpectedError> {
        match self.rows {
            Some(_) => Err(RowsNotExpectedError),
            None => Ok(()),
        }
    }

    /// Returns rows when `LegacyQueryResult.rows` is `Some`, otherwise an empty Vec.\
    /// Equal to `rows().unwrap_or_default()`.
    pub fn rows_or_empty(self) -> Vec<Row> {
        self.rows.unwrap_or_default()
    }

    /// Returns rows parsed as the given type.\
    /// When `LegacyQueryResult.rows` is `None`, returns 0 rows.\
    /// Equal to `rows_or_empty().into_typed::<RowT>()`.
    pub fn rows_typed_or_empty<RowT: FromRow>(self) -> TypedRowIter<RowT> {
        self.rows_or_empty().into_typed::<RowT>()
    }

    /// Returns first row from the received rows.\
    /// When the first row is not available, returns an error.
    pub fn first_row(self) -> Result<Row, FirstRowError> {
        match self.maybe_first_row()? {
            Some(row) => Ok(row),
            None => Err(FirstRowError::RowsEmpty),
        }
    }

    /// Returns first row from the received rows parsed as the given type.\
    /// When the first row is not available, returns an error.
    pub fn first_row_typed<RowT: FromRow>(self) -> Result<RowT, FirstRowTypedError> {
        Ok(self.first_row()?.into_typed()?)
    }

    /// Returns `Option<RowT>` containing the first of a result.\
    /// Fails when the query isn't of a type that could return rows, same as [`rows()`](LegacyQueryResult::rows).
    pub fn maybe_first_row(self) -> Result<Option<Row>, RowsExpectedError> {
        Ok(self.rows()?.into_iter().next())
    }

    /// Returns `Option<RowT>` containing the first of a result.\
    /// Fails when the query isn't of a type that could return rows, same as [`rows()`](LegacyQueryResult::rows).
    pub fn maybe_first_row_typed<RowT: FromRow>(
        self,
    ) -> Result<Option<RowT>, MaybeFirstRowTypedError> {
        match self.maybe_first_row()? {
            Some(row) => Ok(Some(row.into_typed::<RowT>()?)),
            None => Ok(None),
        }
    }

    /// Returns the only received row.\
    /// Fails if the result is anything else than a single row.\
    pub fn single_row(self) -> Result<Row, SingleRowError> {
        let rows: Vec<Row> = self.rows()?;

        if rows.len() != 1 {
            return Err(SingleRowError::BadNumberOfRows(rows.len()));
        }

        Ok(rows.into_iter().next().unwrap())
    }

    /// Returns the only received row parsed as the given type.\
    /// Fails if the result is anything else than a single row.\
    pub fn single_row_typed<RowT: FromRow>(self) -> Result<RowT, SingleRowTypedError> {
        Ok(self.single_row()?.into_typed::<RowT>()?)
    }

    /// Returns column specifications.
    #[inline]
    pub fn col_specs(&self) -> &[ColumnSpec<'_>] {
        self.metadata
            .as_ref()
            .map(|metadata| metadata.inner().col_specs())
            .unwrap_or_default()
    }

    /// Returns a column specification for a column with given name, or None if not found
    #[inline]
    pub fn get_column_spec<'a>(&'a self, name: &str) -> Option<(usize, &'a ColumnSpec<'_>)> {
        self.col_specs()
            .iter()
            .enumerate()
            .find(|(_id, spec)| spec.name() == name)
    }
}

/// [`LegacyQueryResult::rows()`](LegacyQueryResult::rows) or a similar function called on a bad LegacyQueryResult.\
/// Expected `LegacyQueryResult.rows` to be `Some`, but it was `None`.\
/// `LegacyQueryResult.rows` is `Some` for queries that can return rows (e.g `SELECT`).\
/// It is `None` for queries that can't return rows (e.g `INSERT`).
#[derive(Debug, Clone, Error, PartialEq, Eq)]
#[error(
    "LegacyQueryResult::rows() or similar function called on a bad LegacyQueryResult.
         Expected LegacyQueryResult.rows to be Some, but it was None.
         LegacyQueryResult.rows is Some for queries that can return rows (e.g SELECT).
         It is None for queries that can't return rows (e.g INSERT)."
)]
pub struct RowsExpectedError;

/// [`LegacyQueryResult::result_not_rows()`](LegacyQueryResult::result_not_rows) called on a bad LegacyQueryResult.\
/// Expected `LegacyQueryResult.rows` to be `None`, but it was `Some`.\
/// `LegacyQueryResult.rows` is `Some` for queries that can return rows (e.g `SELECT`).\
/// It is `None` for queries that can't return rows (e.g `INSERT`).
#[derive(Debug, Clone, Error, PartialEq, Eq)]
#[error(
    "LegacyQueryResult::result_not_rows() called on a bad LegacyQueryResult.
         Expected LegacyQueryResult.rows to be None, but it was Some.
         LegacyQueryResult.rows is Some for queries that can return rows (e.g SELECT).
         It is None for queries that can't return rows (e.g INSERT)."
)]
pub struct RowsNotExpectedError;

#[derive(Debug, Clone, Error, PartialEq, Eq)]
pub enum FirstRowError {
    /// [`LegacyQueryResult::first_row()`](LegacyQueryResult::first_row) called on a bad LegacyQueryResult.\
    /// Expected `LegacyQueryResult.rows` to be `Some`, but it was `None`.\
    /// `LegacyQueryResult.rows` is `Some` for queries that can return rows (e.g `SELECT`).\
    /// It is `None` for queries that can't return rows (e.g `INSERT`).
    #[error(transparent)]
    RowsExpected(#[from] RowsExpectedError),

    /// Rows in `LegacyQueryResult` are empty
    #[error("Rows in LegacyQueryResult are empty")]
    RowsEmpty,
}

#[derive(Debug, Clone, Error, PartialEq, Eq)]
pub enum FirstRowTypedError {
    /// [`LegacyQueryResult::first_row_typed()`](LegacyQueryResult::first_row_typed) called on a bad LegacyQueryResult.\
    /// Expected `LegacyQueryResult.rows` to be `Some`, but it was `None`.\
    /// `LegacyQueryResult.rows` is `Some` for queries that can return rows (e.g `SELECT`).\
    /// It is `None` for queries that can't return rows (e.g `INSERT`).
    #[error(transparent)]
    RowsExpected(#[from] RowsExpectedError),

    /// Rows in `LegacyQueryResult` are empty
    #[error("Rows in LegacyQueryResult are empty")]
    RowsEmpty,

    /// Parsing row as the given type failed
    #[error(transparent)]
    FromRowError(#[from] FromRowError),
}

#[derive(Debug, Clone, Error, PartialEq, Eq)]
pub enum MaybeFirstRowTypedError {
    /// [`LegacyQueryResult::maybe_first_row_typed()`](LegacyQueryResult::maybe_first_row_typed) called on a bad LegacyQueryResult.\
    /// Expected `LegacyQueryResult.rows` to be `Some`, but it was `None`.
    /// `LegacyQueryResult.rows` is `Some` for queries that can return rows (e.g `SELECT`).\
    /// It is `None` for queries that can't return rows (e.g `INSERT`).
    #[error(transparent)]
    RowsExpected(#[from] RowsExpectedError),

    /// Parsing row as the given type failed
    #[error(transparent)]
    FromRowError(#[from] FromRowError),
}

#[derive(Debug, Clone, Error, PartialEq, Eq)]
pub enum SingleRowError {
    /// [`LegacyQueryResult::single_row()`](LegacyQueryResult::single_row) called on a bad LegacyQueryResult.\
    /// Expected `LegacyQueryResult.rows` to be `Some`, but it was `None`.\
    /// `LegacyQueryResult.rows` is `Some` for queries that can return rows (e.g `SELECT`).\
    /// It is `None` for queries that can't return rows (e.g `INSERT`).
    #[error(transparent)]
    RowsExpected(#[from] RowsExpectedError),

    /// Expected a single row, found other number of rows
    #[error("Expected a single row, found {0} rows")]
    BadNumberOfRows(usize),
}

#[derive(Debug, Clone, Error, PartialEq, Eq)]
pub enum SingleRowTypedError {
    /// [`LegacyQueryResult::single_row_typed()`](LegacyQueryResult::single_row_typed) called on a bad LegacyQueryResult.\
    /// Expected `LegacyQueryResult.rows` to be `Some`, but it was `None`.\
    /// `LegacyQueryResult.rows` is `Some` for queries that can return rows (e.g `SELECT`).\
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        frame::response::result::{CqlValue, Row},
        test_utils::setup_tracing,
    };
    use std::convert::TryInto;
    use std::sync::Arc;

    use assert_matches::assert_matches;
    use scylla_cql::frame::response::result::{ColumnType, ResultMetadata, TableSpec};

    // Returns specified number of rows, each one containing one int32 value.
    // Values are 0, 1, 2, 3, 4, ...
    fn make_rows(rows_num: usize) -> Vec<Row> {
        let mut rows: Vec<Row> = Vec::with_capacity(rows_num);
        for cur_value in 0..rows_num {
            let int_val: i32 = cur_value.try_into().unwrap();
            rows.push(Row {
                columns: vec![Some(CqlValue::Int(int_val))],
            });
        }
        rows
    }

    // Just like make_rows, but each column has one String value
    // values are "val0", "val1", "val2", ...
    fn make_string_rows(rows_num: usize) -> Vec<Row> {
        let mut rows: Vec<Row> = Vec::with_capacity(rows_num);
        for cur_value in 0..rows_num {
            rows.push(Row {
                columns: vec![Some(CqlValue::Text(format!("val{}", cur_value)))],
            });
        }
        rows
    }

    fn make_test_metadata() -> ResultMetadata<'static> {
        let table_spec = TableSpec::borrowed("some_keyspace", "some_table");

        let column_spec = ColumnSpec::borrowed("column0", ColumnType::Int, table_spec);

        ResultMetadata::new_for_test(1, vec![column_spec])
    }

    fn make_not_rows_query_result() -> LegacyQueryResult {
        LegacyQueryResult {
            rows: None,
            warnings: vec![],
            tracing_id: None,
            metadata: None,
            serialized_size: 0,
        }
    }

    fn make_rows_query_result(rows_num: usize) -> LegacyQueryResult {
        let mut res = make_not_rows_query_result();
        res.rows = Some(make_rows(rows_num));
        res.metadata = Some(ResultMetadataHolder::SharedCached(Arc::new(
            make_test_metadata(),
        )));
        res
    }

    fn make_string_rows_query_result(rows_num: usize) -> LegacyQueryResult {
        let mut res = make_not_rows_query_result();
        res.rows = Some(make_string_rows(rows_num));
        res.metadata = Some(ResultMetadataHolder::SharedCached(Arc::new(
            make_test_metadata(),
        )));
        res
    }

    #[test]
    fn rows_num_test() {
        setup_tracing();
        assert_eq!(
            make_not_rows_query_result().rows_num(),
            Err(RowsExpectedError)
        );
        assert_eq!(make_rows_query_result(0).rows_num(), Ok(0));
        assert_eq!(make_rows_query_result(1).rows_num(), Ok(1));
        assert_eq!(make_rows_query_result(2).rows_num(), Ok(2));
        assert_eq!(make_rows_query_result(3).rows_num(), Ok(3));
    }

    #[test]
    fn rows_test() {
        setup_tracing();
        assert_eq!(make_not_rows_query_result().rows(), Err(RowsExpectedError));
        assert_eq!(make_rows_query_result(0).rows(), Ok(vec![]));
        assert_eq!(make_rows_query_result(1).rows(), Ok(make_rows(1)));
        assert_eq!(make_rows_query_result(2).rows(), Ok(make_rows(2)));
    }

    #[test]
    fn rows_typed_test() {
        setup_tracing();
        assert!(make_not_rows_query_result().rows_typed::<(i32,)>().is_err());

        let rows0: Vec<(i32,)> = make_rows_query_result(0)
            .rows_typed::<(i32,)>()
            .unwrap()
            .map(|r| r.unwrap())
            .collect();

        assert_eq!(rows0, vec![]);

        let rows1: Vec<(i32,)> = make_rows_query_result(1)
            .rows_typed::<(i32,)>()
            .unwrap()
            .map(|r| r.unwrap())
            .collect();

        assert_eq!(rows1, vec![(0,)]);

        let rows2: Vec<(i32,)> = make_rows_query_result(2)
            .rows_typed::<(i32,)>()
            .unwrap()
            .map(|r| r.unwrap())
            .collect();

        assert_eq!(rows2, vec![(0,), (1,)]);
    }

    #[test]
    fn result_not_rows_test() {
        setup_tracing();
        assert_eq!(make_not_rows_query_result().result_not_rows(), Ok(()));
        assert_eq!(
            make_rows_query_result(0).result_not_rows(),
            Err(RowsNotExpectedError)
        );
        assert_eq!(
            make_rows_query_result(1).result_not_rows(),
            Err(RowsNotExpectedError)
        );
        assert_eq!(
            make_rows_query_result(2).result_not_rows(),
            Err(RowsNotExpectedError)
        );
    }

    #[test]
    fn rows_or_empty_test() {
        setup_tracing();
        assert_eq!(make_not_rows_query_result().rows_or_empty(), vec![]);
        assert_eq!(make_rows_query_result(0).rows_or_empty(), make_rows(0));
        assert_eq!(make_rows_query_result(1).rows_or_empty(), make_rows(1));
        assert_eq!(make_rows_query_result(2).rows_or_empty(), make_rows(2));
    }

    #[test]
    fn rows_typed_or_empty() {
        setup_tracing();
        let rows_empty: Vec<(i32,)> = make_not_rows_query_result()
            .rows_typed_or_empty::<(i32,)>()
            .map(|r| r.unwrap())
            .collect();

        assert_eq!(rows_empty, vec![]);

        let rows0: Vec<(i32,)> = make_rows_query_result(0)
            .rows_typed_or_empty::<(i32,)>()
            .map(|r| r.unwrap())
            .collect();

        assert_eq!(rows0, vec![]);

        let rows1: Vec<(i32,)> = make_rows_query_result(1)
            .rows_typed_or_empty::<(i32,)>()
            .map(|r| r.unwrap())
            .collect();

        assert_eq!(rows1, vec![(0,)]);

        let rows2: Vec<(i32,)> = make_rows_query_result(2)
            .rows_typed_or_empty::<(i32,)>()
            .map(|r| r.unwrap())
            .collect();

        assert_eq!(rows2, vec![(0,), (1,)]);
    }

    #[test]
    fn first_row_test() {
        setup_tracing();
        assert_eq!(
            make_not_rows_query_result().first_row(),
            Err(FirstRowError::RowsExpected(RowsExpectedError))
        );
        assert_eq!(
            make_rows_query_result(0).first_row(),
            Err(FirstRowError::RowsEmpty)
        );
        assert_eq!(
            make_rows_query_result(1).first_row(),
            Ok(make_rows(1).into_iter().next().unwrap())
        );
        assert_eq!(
            make_rows_query_result(2).first_row(),
            Ok(make_rows(2).into_iter().next().unwrap())
        );
        assert_eq!(
            make_rows_query_result(3).first_row(),
            Ok(make_rows(3).into_iter().next().unwrap())
        );
    }

    #[test]
    fn first_row_typed_test() {
        setup_tracing();
        assert_eq!(
            make_not_rows_query_result().first_row_typed::<(i32,)>(),
            Err(FirstRowTypedError::RowsExpected(RowsExpectedError))
        );
        assert_eq!(
            make_rows_query_result(0).first_row_typed::<(i32,)>(),
            Err(FirstRowTypedError::RowsEmpty)
        );
        assert_eq!(
            make_rows_query_result(1).first_row_typed::<(i32,)>(),
            Ok((0,))
        );
        assert_eq!(
            make_rows_query_result(2).first_row_typed::<(i32,)>(),
            Ok((0,))
        );
        assert_eq!(
            make_rows_query_result(3).first_row_typed::<(i32,)>(),
            Ok((0,))
        );

        assert_matches!(
            make_string_rows_query_result(2).first_row_typed::<(i32,)>(),
            Err(FirstRowTypedError::FromRowError(_))
        );
    }

    #[test]
    fn maybe_first_row_test() {
        setup_tracing();
        assert_eq!(
            make_not_rows_query_result().maybe_first_row(),
            Err(RowsExpectedError)
        );
        assert_eq!(make_rows_query_result(0).maybe_first_row(), Ok(None));
        assert_eq!(
            make_rows_query_result(1).maybe_first_row(),
            Ok(Some(make_rows(1).into_iter().next().unwrap()))
        );
        assert_eq!(
            make_rows_query_result(2).maybe_first_row(),
            Ok(Some(make_rows(2).into_iter().next().unwrap()))
        );
        assert_eq!(
            make_rows_query_result(3).maybe_first_row(),
            Ok(Some(make_rows(3).into_iter().next().unwrap()))
        );
    }

    #[test]
    fn maybe_first_row_typed_test() {
        setup_tracing();
        assert_eq!(
            make_not_rows_query_result().maybe_first_row_typed::<(i32,)>(),
            Err(MaybeFirstRowTypedError::RowsExpected(RowsExpectedError))
        );

        assert_eq!(
            make_rows_query_result(0).maybe_first_row_typed::<(i32,)>(),
            Ok(None)
        );

        assert_eq!(
            make_rows_query_result(1).maybe_first_row_typed::<(i32,)>(),
            Ok(Some((0,)))
        );

        assert_eq!(
            make_rows_query_result(2).maybe_first_row_typed::<(i32,)>(),
            Ok(Some((0,)))
        );

        assert_eq!(
            make_rows_query_result(3).maybe_first_row_typed::<(i32,)>(),
            Ok(Some((0,)))
        );

        assert_matches!(
            make_string_rows_query_result(1).maybe_first_row_typed::<(i32,)>(),
            Err(MaybeFirstRowTypedError::FromRowError(_))
        )
    }

    #[test]
    fn single_row_test() {
        setup_tracing();
        assert_eq!(
            make_not_rows_query_result().single_row(),
            Err(SingleRowError::RowsExpected(RowsExpectedError))
        );
        assert_eq!(
            make_rows_query_result(0).single_row(),
            Err(SingleRowError::BadNumberOfRows(0))
        );
        assert_eq!(
            make_rows_query_result(1).single_row(),
            Ok(make_rows(1).into_iter().next().unwrap())
        );
        assert_eq!(
            make_rows_query_result(2).single_row(),
            Err(SingleRowError::BadNumberOfRows(2))
        );
        assert_eq!(
            make_rows_query_result(3).single_row(),
            Err(SingleRowError::BadNumberOfRows(3))
        );
    }

    #[test]
    fn single_row_typed_test() {
        setup_tracing();
        assert_eq!(
            make_not_rows_query_result().single_row_typed::<(i32,)>(),
            Err(SingleRowTypedError::RowsExpected(RowsExpectedError))
        );
        assert_eq!(
            make_rows_query_result(0).single_row_typed::<(i32,)>(),
            Err(SingleRowTypedError::BadNumberOfRows(0))
        );
        assert_eq!(
            make_rows_query_result(1).single_row_typed::<(i32,)>(),
            Ok((0,))
        );
        assert_eq!(
            make_rows_query_result(2).single_row_typed::<(i32,)>(),
            Err(SingleRowTypedError::BadNumberOfRows(2))
        );
        assert_eq!(
            make_rows_query_result(3).single_row_typed::<(i32,)>(),
            Err(SingleRowTypedError::BadNumberOfRows(3))
        );

        assert_matches!(
            make_string_rows_query_result(1).single_row_typed::<(i32,)>(),
            Err(SingleRowTypedError::FromRowError(_))
        );
    }
}
