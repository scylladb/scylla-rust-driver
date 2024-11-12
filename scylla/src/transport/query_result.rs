use std::fmt::Debug;

use thiserror::Error;
use uuid::Uuid;

use scylla_cql::frame::frame_errors::RowsParseError;
use scylla_cql::frame::response::result::{
    ColumnSpec, ColumnType, DeserializedMetadataAndRawRows, RawMetadataAndRawRows, Row, TableSpec,
};
use scylla_cql::types::deserialize::result::TypedRowIterator;
use scylla_cql::types::deserialize::row::DeserializeRow;
use scylla_cql::types::deserialize::{DeserializationError, TypeCheckError};

use super::legacy_query_result::LegacyQueryResult;

/// A view over specification of a table in the database.
#[derive(Debug, Clone, Copy)]
#[cfg_attr(test, derive(PartialEq, Eq))]
pub struct TableSpecView<'res> {
    table_name: &'res str,
    ks_name: &'res str,
}

impl<'res> TableSpecView<'res> {
    pub(crate) fn new_from_table_spec(spec: &'res TableSpec) -> Self {
        Self {
            table_name: spec.table_name(),
            ks_name: spec.ks_name(),
        }
    }

    /// The name of the table.
    #[inline]
    pub fn table_name(&self) -> &'res str {
        self.table_name
    }

    /// The name of the keyspace the table resides in.
    #[inline]
    pub fn ks_name(&self) -> &'res str {
        self.ks_name
    }
}

/// A view over specification of a column returned by the database.
#[derive(Debug, Clone, Copy)]
#[cfg_attr(test, derive(PartialEq, Eq))]
pub struct ColumnSpecView<'res> {
    table_spec: TableSpecView<'res>,
    name: &'res str,
    typ: &'res ColumnType<'res>,
}

impl<'res> ColumnSpecView<'res> {
    pub(crate) fn new_from_column_spec(spec: &'res ColumnSpec) -> Self {
        Self {
            table_spec: TableSpecView::new_from_table_spec(spec.table_spec()),
            name: spec.name(),
            typ: spec.typ(),
        }
    }

    /// Returns a view over specification of the table the column is part of.
    #[inline]
    pub fn table_spec(&self) -> TableSpecView<'res> {
        self.table_spec
    }

    /// The column's name.
    #[inline]
    pub fn name(&self) -> &'res str {
        self.name
    }

    /// The column's CQL type.
    #[inline]
    pub fn typ(&self) -> &'res ColumnType {
        self.typ
    }
}

/// A view over specification of columns returned by the database.
#[derive(Debug, Clone, Copy)]
pub struct ColumnSpecs<'res> {
    specs: &'res [ColumnSpec<'res>],
}

impl<'res> ColumnSpecs<'res> {
    pub(crate) fn new(specs: &'res [ColumnSpec<'res>]) -> Self {
        Self { specs }
    }

    pub(crate) fn inner(&self) -> &'res [ColumnSpec<'res>] {
        self.specs
    }

    /// Returns number of columns.
    #[allow(clippy::len_without_is_empty)]
    #[inline]
    pub fn len(&self) -> usize {
        self.specs.len()
    }

    /// Returns specification of k-th column returned from the database.
    #[inline]
    pub fn get_by_index(&self, k: usize) -> Option<ColumnSpecView<'res>> {
        self.specs.get(k).map(ColumnSpecView::new_from_column_spec)
    }

    /// Returns specification of the column with given name returned from the database.
    #[inline]
    pub fn get_by_name(&self, name: &str) -> Option<(usize, ColumnSpecView<'res>)> {
        self.specs
            .iter()
            .enumerate()
            .find(|(_idx, spec)| spec.name() == name)
            .map(|(idx, spec)| (idx, ColumnSpecView::new_from_column_spec(spec)))
    }

    /// Returns iterator over specification of columns returned from the database,
    /// ordered by column order in the response.
    #[inline]
    pub fn iter(&self) -> impl Iterator<Item = ColumnSpecView<'res>> {
        self.specs.iter().map(ColumnSpecView::new_from_column_spec)
    }
}

/// Result of a single request to the database. It represents any kind of Result frame.
///
/// The received rows and metadata, which are present if the frame is of Result:Rows kind,
/// are kept in a raw binary form. To deserialize and access them, transform `QueryResult`
/// to [QueryRowsResult] by calling [QueryResult::into_rows_result].
///
/// NOTE: this is a result of a single CQL request. If you use paging for your query,
/// this will contain exactly one page.
#[derive(Debug)]
pub struct QueryResult {
    raw_metadata_and_rows: Option<RawMetadataAndRawRows>,
    tracing_id: Option<Uuid>,
    warnings: Vec<String>,
}

impl QueryResult {
    pub(crate) fn new(
        raw_rows: Option<RawMetadataAndRawRows>,
        tracing_id: Option<Uuid>,
        warnings: Vec<String>,
    ) -> Self {
        Self {
            raw_metadata_and_rows: raw_rows,
            tracing_id,
            warnings,
        }
    }

    // Preferred to implementing Default, because users shouldn't be able to create
    // an empty QueryResult.
    //
    // For now unused, but it will be used once Session's API is migrated
    // to the new QueryResult.
    #[allow(dead_code)]
    pub(crate) fn mock_empty() -> Self {
        Self {
            raw_metadata_and_rows: None,
            tracing_id: None,
            warnings: Vec::new(),
        }
    }

    pub(crate) fn raw_metadata_and_rows(&self) -> Option<&RawMetadataAndRawRows> {
        self.raw_metadata_and_rows.as_ref()
    }

    /// Warnings emitted by the database.
    #[inline]
    pub fn warnings(&self) -> impl Iterator<Item = &str> {
        self.warnings.iter().map(String::as_str)
    }

    /// Tracing ID associated with this CQL request.
    #[inline]
    pub fn tracing_id(&self) -> Option<Uuid> {
        self.tracing_id
    }

    /// Returns a bool indicating the current response is of Rows type.
    #[inline]
    pub fn is_rows(&self) -> bool {
        self.raw_metadata_and_rows.is_some()
    }

    /// Returns `Ok` for a request's result that shouldn't contain any rows.\
    /// Will return `Ok` for `INSERT` result, but a `SELECT` result, even an empty one, will cause an error.\
    /// Opposite of [QueryResult::into_rows_result].
    #[inline]
    pub fn result_not_rows(&self) -> Result<(), ResultNotRowsError> {
        match &self.raw_metadata_and_rows {
            Some(_) => Err(ResultNotRowsError),
            None => Ok(()),
        }
    }

    /// Transforms itself into the Rows result type to enable deserializing rows.
    /// Deserializes result metadata and allocates it.
    ///
    /// Returns `None` if the response is not of Rows kind.
    ///
    /// ```rust
    /// # use scylla::transport::query_result::{QueryResult, QueryRowsResult};
    /// # fn example(query_result: QueryResult) -> Result<(), Box<dyn std::error::Error>> {
    /// let maybe_rows_result = query_result.into_rows_result()?;
    /// if let Some(rows_result) = maybe_rows_result {
    ///     let mut rows_iter = rows_result.rows::<(i32, &str)>()?;
    ///     while let Some((num, text)) = rows_iter.next().transpose()? {
    ///         // do something with `num` and `text``
    ///     }
    /// } else {
    ///     // Response was not Result:Rows, but some other kind of Result.
    /// }
    ///
    /// Ok(())
    /// # }
    ///
    /// ```
    pub fn into_rows_result(self) -> Result<Option<QueryRowsResult>, RowsParseError> {
        let QueryResult {
            raw_metadata_and_rows,
            tracing_id,
            warnings,
        } = self;
        raw_metadata_and_rows
            .map(|raw_rows| {
                let raw_rows_with_metadata = raw_rows.deserialize_metadata()?;
                Ok(QueryRowsResult {
                    raw_rows_with_metadata,
                    warnings,
                    tracing_id,
                })
            })
            .transpose()
    }

    /// Transforms itself into the legacy result type, by eagerly deserializing rows
    /// into the Row type. This is inefficient, and should only be used during transition
    /// period to the new API.
    pub fn into_legacy_result(self) -> Result<LegacyQueryResult, RowsParseError> {
        if let Some(raw_rows) = self.raw_metadata_and_rows {
            let raw_rows_with_metadata = raw_rows.deserialize_metadata()?;

            let deserialized_rows = raw_rows_with_metadata
                .rows_iter::<Row>()?
                .collect::<Result<Vec<_>, DeserializationError>>()?;
            let serialized_size = raw_rows_with_metadata.rows_bytes_size();
            let metadata = raw_rows_with_metadata.into_metadata();

            Ok(LegacyQueryResult {
                rows: Some(deserialized_rows),
                warnings: self.warnings,
                tracing_id: self.tracing_id,
                metadata: Some(metadata),
                serialized_size,
            })
        } else {
            Ok(LegacyQueryResult {
                rows: None,
                warnings: self.warnings,
                tracing_id: self.tracing_id,
                metadata: None,
                serialized_size: 0,
            })
        }
    }
}

/// Enables deserialization of rows received from the database in a [`QueryResult`].
///
/// Upon creation, it deserializes result metadata and allocates it.
///
/// This struct provides generic methods which enable typed access to the data,
/// by deserializing rows on the fly to the type provided as a type parameter.
/// Those methods are:
/// - [rows()](QueryRowsResult::rows) - for iterating through rows,
/// - [first_row()](QueryRowsResult::first_row) and
///   [maybe_first_row()](QueryRowsResult::maybe_first_row) -
///   for accessing the first row,
/// - [single_row()](QueryRowsResult::single_row) - for accessing the first row,
///   additionally asserting that it's the only one in the response.
///
/// ```rust
/// # use scylla::transport::query_result::QueryResult;
/// # fn example(query_result: QueryResult) -> Result<(), Box<dyn std::error::Error>> {
/// let maybe_rows_result = query_result.into_rows_result()?;
/// if let Some(rows_result) = maybe_rows_result {
///     let mut rows_iter = rows_result.rows::<(i32, &str)>()?;
///     while let Some((num, text)) = rows_iter.next().transpose()? {
///         // do something with `num` and `text``
///     }
/// } else {
///     // Response was not Result:Rows, but some other kind of Result.
/// }
///
/// Ok(())
/// # }
///
/// ```
#[derive(Debug)]
pub struct QueryRowsResult {
    raw_rows_with_metadata: DeserializedMetadataAndRawRows,
    tracing_id: Option<Uuid>,
    warnings: Vec<String>,
}

impl QueryRowsResult {
    /// Warnings emitted by the database.
    #[inline]
    pub fn warnings(&self) -> impl Iterator<Item = &str> {
        self.warnings.iter().map(String::as_str)
    }

    /// Tracing ID associated with this CQL request.
    #[inline]
    pub fn tracing_id(&self) -> Option<Uuid> {
        self.tracing_id
    }

    /// Returns the number of received rows.
    #[inline]
    pub fn rows_num(&self) -> usize {
        self.raw_rows_with_metadata.rows_count()
    }

    /// Returns the size of the serialized rows.
    #[inline]
    pub fn rows_bytes_size(&self) -> usize {
        self.raw_rows_with_metadata.rows_bytes_size()
    }

    /// Returns column specifications.
    #[inline]
    pub fn column_specs(&self) -> ColumnSpecs {
        ColumnSpecs::new(self.raw_rows_with_metadata.metadata().col_specs())
    }

    /// Returns an iterator over the received rows.
    ///
    /// Returns an error if the rows in the response are of incorrect type.
    #[inline]
    pub fn rows<'frame, R: DeserializeRow<'frame, 'frame>>(
        &'frame self,
    ) -> Result<TypedRowIterator<'frame, 'frame, R>, RowsError> {
        self.raw_rows_with_metadata
            .rows_iter()
            .map_err(RowsError::TypeCheckFailed)
    }

    /// Returns `Option<R>` containing the first row of the result.
    ///
    /// Fails when the the rows in the response are of incorrect type,
    /// or when the deserialization fails.
    pub fn maybe_first_row<'frame, R: DeserializeRow<'frame, 'frame>>(
        &'frame self,
    ) -> Result<Option<R>, MaybeFirstRowError> {
        self.rows::<R>()
            .map_err(|err| match err {
                RowsError::TypeCheckFailed(typck_err) => {
                    MaybeFirstRowError::TypeCheckFailed(typck_err)
                }
            })?
            .next()
            .transpose()
            .map_err(MaybeFirstRowError::DeserializationFailed)
    }

    /// Returns the first row of the received result.
    ///
    /// When the first row is not available, returns an error.
    /// Fails when the the rows in the response are of incorrect type,
    /// or when the deserialization fails.
    pub fn first_row<'frame, R: DeserializeRow<'frame, 'frame>>(
        &'frame self,
    ) -> Result<R, FirstRowError> {
        match self.maybe_first_row::<R>() {
            Ok(Some(row)) => Ok(row),
            Ok(None) => Err(FirstRowError::RowsEmpty),
            Err(MaybeFirstRowError::TypeCheckFailed(err)) => {
                Err(FirstRowError::TypeCheckFailed(err))
            }
            Err(MaybeFirstRowError::DeserializationFailed(err)) => {
                Err(FirstRowError::DeserializationFailed(err))
            }
        }
    }

    /// Returns the only received row.
    ///
    /// Fails if the result is anything else than a single row.
    /// Fails when the the rows in the response are of incorrect type,
    /// or when the deserialization fails.
    pub fn single_row<'frame, R: DeserializeRow<'frame, 'frame>>(
        &'frame self,
    ) -> Result<R, SingleRowError> {
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
                Some(Err(err)) => Err(SingleRowError::DeserializationFailed(err)),
                None => Err(SingleRowError::UnexpectedRowCount(0)),
            },
            Err(RowsError::TypeCheckFailed(err)) => Err(SingleRowError::TypeCheckFailed(err)),
        }
    }
}

/// An error returned by [`QueryRowsResult::rows`].
#[derive(Debug, Error)]
pub enum RowsError {
    /// Type check failed
    #[error("Type check failed: {0}")]
    TypeCheckFailed(#[from] TypeCheckError),
}

/// An error returned by [`QueryRowsResult::maybe_first_row`].
#[derive(Debug, Error)]
pub enum MaybeFirstRowError {
    /// Type check failed
    #[error("Type check failed: {0}")]
    TypeCheckFailed(#[from] TypeCheckError),

    /// Deserialization failed
    #[error("Deserialization failed: {0}")]
    DeserializationFailed(#[from] DeserializationError),
}

/// An error returned by [`QueryRowsResult::first_row`].
#[derive(Debug, Error)]
pub enum FirstRowError {
    /// The request response was of Rows type, but no rows were returned
    #[error("The request response was of Rows type, but no rows were returned")]
    RowsEmpty,

    /// Type check failed
    #[error("Type check failed: {0}")]
    TypeCheckFailed(#[from] TypeCheckError),

    /// Deserialization failed
    #[error("Deserialization failed: {0}")]
    DeserializationFailed(#[from] DeserializationError),
}

/// An error returned by [`QueryRowsResult::single_row`].
#[derive(Debug, Error, Clone)]
pub enum SingleRowError {
    /// Expected one row, but got a different count
    #[error("Expected a single row, but got {0} rows")]
    UnexpectedRowCount(usize),

    /// Type check failed
    #[error("Type check failed: {0}")]
    TypeCheckFailed(#[from] TypeCheckError),

    /// Deserialization failed
    #[error("Deserialization failed: {0}")]
    DeserializationFailed(#[from] DeserializationError),
}

/// An error returned by [`QueryResult::result_not_rows`].
///
/// It indicates that response to the request was, unexpectedly, of Rows kind.
#[derive(Debug, Error)]
#[error("The request response was, unexpectedly, of Rows kind")]
pub struct ResultNotRowsError;

#[cfg(test)]
mod tests {
    use assert_matches::assert_matches;
    use bytes::{Bytes, BytesMut};
    use itertools::Itertools as _;
    use scylla_cql::frame::response::result::ResultMetadata;
    use scylla_cql::frame::types;

    use super::*;

    const TABLE_SPEC: TableSpec<'static> = TableSpec::borrowed("ks", "tbl");

    fn column_spec_infinite_iter() -> impl Iterator<Item = ColumnSpec<'static>> {
        (0..).map(|k| {
            ColumnSpec::owned(
                format!("col_{}", k),
                match k % 3 {
                    0 => ColumnType::Ascii,
                    1 => ColumnType::Boolean,
                    2 => ColumnType::Float,
                    _ => unreachable!(),
                },
                TABLE_SPEC,
            )
        })
    }

    #[test]
    fn test_query_result() {
        fn serialize_cells(cells: impl IntoIterator<Item = Option<impl AsRef<[u8]>>>) -> Bytes {
            let mut bytes = BytesMut::new();
            for cell in cells {
                types::write_bytes_opt(cell, &mut bytes).unwrap();
            }
            bytes.freeze()
        }

        fn sample_result_metadata(cols: usize) -> ResultMetadata<'static> {
            ResultMetadata::new_for_test(cols, column_spec_infinite_iter().take(cols).collect())
        }

        fn sample_raw_rows(cols: usize, rows: usize) -> RawMetadataAndRawRows {
            let metadata = sample_result_metadata(cols);

            static STRING: &[u8] = "MOCK".as_bytes();
            static BOOLEAN: &[u8] = &(true as i8).to_be_bytes();
            static FLOAT: &[u8] = &12341_i32.to_be_bytes();
            let cells = metadata.col_specs().iter().map(|spec| match spec.typ() {
                ColumnType::Ascii => STRING,
                ColumnType::Boolean => BOOLEAN,
                ColumnType::Float => FLOAT,
                _ => unreachable!(),
            });
            let bytes = serialize_cells(cells.map(Some));
            RawMetadataAndRawRows::new_for_test(None, Some(metadata), false, rows, &bytes).unwrap()
        }

        // Used to trigger DeserializationError.
        fn sample_raw_rows_invalid_bytes(cols: usize, rows: usize) -> RawMetadataAndRawRows {
            let metadata = sample_result_metadata(cols);

            RawMetadataAndRawRows::new_for_test(None, Some(metadata), false, rows, &[]).unwrap()
        }

        // Check tracing ID
        for tracing_id in [None, Some(Uuid::from_u128(0x_feed_dead))] {
            for raw_rows in [None, Some(sample_raw_rows(7, 6))] {
                let qr = QueryResult::new(raw_rows, tracing_id, vec![]);
                assert_eq!(qr.tracing_id(), tracing_id);
            }
        }

        // Check warnings
        for raw_rows in [None, Some(sample_raw_rows(7, 6))] {
            let warnings = &["Ooops", "Meltdown..."];
            let qr = QueryResult::new(
                raw_rows,
                None,
                warnings.iter().copied().map(String::from).collect(),
            );
            assert_eq!(qr.warnings().collect_vec(), warnings);
        }

        // Check col specs
        {
            // Not RESULT::Rows response -> no column specs
            {
                let rqr = QueryResult::new(None, None, Vec::new());
                let qr = rqr.into_rows_result().unwrap();
                assert_matches!(qr, None);
            }

            // RESULT::Rows response -> some column specs
            {
                let n = 5;
                let metadata = sample_result_metadata(n);
                let rr = RawMetadataAndRawRows::new_for_test(None, Some(metadata), false, 0, &[])
                    .unwrap();
                let rqr = QueryResult::new(Some(rr), None, Vec::new());
                let qr = rqr.into_rows_result().unwrap().unwrap();
                let column_specs = qr.column_specs();
                assert_eq!(column_specs.len(), n);

                // By index
                {
                    for (i, expected_col_spec) in column_spec_infinite_iter().enumerate().take(n) {
                        let expected_view =
                            ColumnSpecView::new_from_column_spec(&expected_col_spec);
                        assert_eq!(column_specs.get_by_index(i), Some(expected_view));
                    }

                    assert_matches!(column_specs.get_by_index(n), None);
                }

                // By name
                {
                    for (idx, expected_col_spec) in column_spec_infinite_iter().enumerate().take(n)
                    {
                        let name = expected_col_spec.name();
                        let expected_view =
                            ColumnSpecView::new_from_column_spec(&expected_col_spec);
                        assert_eq!(column_specs.get_by_name(name), Some((idx, expected_view)));
                    }

                    assert_matches!(column_specs.get_by_name("ala ma kota"), None);
                }

                // By iter
                {
                    for (got_view, expected_col_spec) in
                        column_specs.iter().zip(column_spec_infinite_iter())
                    {
                        let expected_view =
                            ColumnSpecView::new_from_column_spec(&expected_col_spec);
                        assert_eq!(got_view, expected_view);
                    }
                }
            }
        }

        // rows(), maybe_rows(), result_not_rows(), first_row(), maybe_first_row(), single_row()
        // All errors are checked.
        {
            // Not RESULT::Rows
            {
                let rqr = QueryResult::new(None, None, Vec::new());
                let qr = rqr.into_rows_result().unwrap();
                assert_matches!(qr, None);
            }

            // RESULT::Rows with 0 rows
            {
                let rr = sample_raw_rows(1, 0);
                let rqr = QueryResult::new(Some(rr), None, Vec::new());
                assert_matches!(rqr.result_not_rows(), Err(ResultNotRowsError));

                let qr = rqr.into_rows_result().unwrap().unwrap();

                // Type check error
                {
                    assert_matches!(qr.rows::<(i32,)>(), Err(RowsError::TypeCheckFailed(_)));

                    assert_matches!(
                        qr.first_row::<(i32,)>(),
                        Err(FirstRowError::TypeCheckFailed(_))
                    );
                    assert_matches!(
                        qr.maybe_first_row::<(i32,)>(),
                        Err(MaybeFirstRowError::TypeCheckFailed(_))
                    );

                    assert_matches!(
                        qr.single_row::<(i32,)>(),
                        Err(SingleRowError::TypeCheckFailed(_))
                    );
                }

                // Correct type
                {
                    assert_matches!(qr.rows::<(&str,)>(), Ok(_));

                    assert_matches!(qr.first_row::<(&str,)>(), Err(FirstRowError::RowsEmpty));
                    assert_matches!(qr.maybe_first_row::<(&str,)>(), Ok(None));

                    assert_matches!(
                        qr.single_row::<(&str,)>(),
                        Err(SingleRowError::UnexpectedRowCount(0))
                    );
                }
            }

            // RESULT::Rows with 1 row
            {
                let rr_good_data = sample_raw_rows(2, 1);
                let rr_bad_data = sample_raw_rows_invalid_bytes(2, 1);
                let rqr_good_data = QueryResult::new(Some(rr_good_data), None, Vec::new());
                let rqr_bad_data = QueryResult::new(Some(rr_bad_data), None, Vec::new());

                for rqr in [&rqr_good_data, &rqr_bad_data] {
                    assert_matches!(rqr.result_not_rows(), Err(ResultNotRowsError));
                }

                let qr_good_data = rqr_good_data.into_rows_result().unwrap().unwrap();
                let qr_bad_data = rqr_bad_data.into_rows_result().unwrap().unwrap();

                for qr in [&qr_good_data, &qr_bad_data] {
                    // Type check error
                    {
                        assert_matches!(
                            qr.rows::<(i32, i32)>(),
                            Err(RowsError::TypeCheckFailed(_))
                        );

                        assert_matches!(
                            qr.first_row::<(i32, i32)>(),
                            Err(FirstRowError::TypeCheckFailed(_))
                        );
                        assert_matches!(
                            qr.maybe_first_row::<(i32, i32)>(),
                            Err(MaybeFirstRowError::TypeCheckFailed(_))
                        );

                        assert_matches!(
                            qr.single_row::<(i32, i32)>(),
                            Err(SingleRowError::TypeCheckFailed(_))
                        );
                    }
                }

                // Correct type
                {
                    assert_matches!(qr_good_data.rows::<(&str, bool)>(), Ok(_));
                    assert_matches!(qr_bad_data.rows::<(&str, bool)>(), Ok(_));

                    assert_matches!(qr_good_data.first_row::<(&str, bool)>(), Ok(_));
                    assert_matches!(
                        qr_bad_data.first_row::<(&str, bool)>(),
                        Err(FirstRowError::DeserializationFailed(_))
                    );
                    assert_matches!(qr_good_data.maybe_first_row::<(&str, bool)>(), Ok(_));
                    assert_matches!(
                        qr_bad_data.maybe_first_row::<(&str, bool)>(),
                        Err(MaybeFirstRowError::DeserializationFailed(_))
                    );

                    assert_matches!(qr_good_data.single_row::<(&str, bool)>(), Ok(_));
                    assert_matches!(
                        qr_bad_data.single_row::<(&str, bool)>(),
                        Err(SingleRowError::DeserializationFailed(_))
                    );
                }
            }

            // RESULT::Rows with 2 rows
            {
                let rr = sample_raw_rows(2, 2);
                let rqr = QueryResult::new(Some(rr), None, Vec::new());
                assert_matches!(rqr.result_not_rows(), Err(ResultNotRowsError));

                let qr = rqr.into_rows_result().unwrap().unwrap();

                // Type check error
                {
                    assert_matches!(qr.rows::<(i32, i32)>(), Err(RowsError::TypeCheckFailed(_)));

                    assert_matches!(
                        qr.first_row::<(i32, i32)>(),
                        Err(FirstRowError::TypeCheckFailed(_))
                    );
                    assert_matches!(
                        qr.maybe_first_row::<(i32, i32)>(),
                        Err(MaybeFirstRowError::TypeCheckFailed(_))
                    );

                    assert_matches!(
                        qr.single_row::<(i32, i32)>(),
                        Err(SingleRowError::TypeCheckFailed(_))
                    );
                }

                // Correct type
                {
                    assert_matches!(qr.rows::<(&str, bool)>(), Ok(_));

                    assert_matches!(qr.first_row::<(&str, bool)>(), Ok(_));
                    assert_matches!(qr.maybe_first_row::<(&str, bool)>(), Ok(_));

                    assert_matches!(
                        qr.single_row::<(&str, bool)>(),
                        Err(SingleRowError::UnexpectedRowCount(2))
                    );
                }
            }
        }
    }
}
