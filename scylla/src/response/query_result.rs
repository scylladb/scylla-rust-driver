//! Types for representing results of CQL queries and iterating
//! over them.

use std::fmt::Debug;

use thiserror::Error;
use uuid::Uuid;

use scylla_cql::deserialize::result::TypedRowIterator;
use scylla_cql::deserialize::row::DeserializeRow;
use scylla_cql::deserialize::{DeserializationError, TypeCheckError};
use scylla_cql::frame::frame_errors::ResultMetadataAndRowsCountParseError;
use scylla_cql::frame::response::result::{ColumnSpec, DeserializedMetadataAndRawRows};

use crate::response::Coordinator;

/// A view over specification of columns returned by the database.
#[derive(Debug, Clone, Copy)]
pub struct ColumnSpecs<'slice, 'spec> {
    specs: &'slice [ColumnSpec<'spec>],
}

impl<'slice, 'spec> ColumnSpecs<'slice, 'spec> {
    /// Creates new [`ColumnSpecs`] wrapper from a slice.
    pub fn new(specs: &'slice [ColumnSpec<'spec>]) -> Self {
        Self { specs }
    }

    /// Returns a slice of col specs encompassed by this struct.
    pub fn as_slice(&self) -> &'slice [ColumnSpec<'spec>] {
        self.specs
    }

    /// Returns number of columns.
    #[expect(clippy::len_without_is_empty)]
    #[inline]
    pub fn len(&self) -> usize {
        self.specs.len()
    }

    /// Returns specification of k-th column returned from the database.
    #[inline]
    pub fn get_by_index(&self, k: usize) -> Option<&'slice ColumnSpec<'spec>> {
        self.specs.get(k)
    }

    /// Returns specification of the column with given name returned from the database.
    #[inline]
    pub fn get_by_name(&self, name: &str) -> Option<(usize, &'slice ColumnSpec<'spec>)> {
        self.specs
            .iter()
            .enumerate()
            .find(|(_idx, spec)| spec.name() == name)
    }

    /// Returns iterator over specification of columns returned from the database,
    /// ordered by column order in the response.
    #[inline]
    pub fn iter(&self) -> impl Iterator<Item = &'slice ColumnSpec<'spec>> + use<'slice, 'spec> {
        self.specs.iter()
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
#[derive(Debug, Clone)]
pub struct QueryResult {
    /// May be `None` only for results of driver's internal requests.
    /// If user gets a `QueryResult` with `request_coordinator` set to `None`,
    /// this is a bug.
    request_coordinator: Option<Coordinator>,
    deserialized_metadata_and_rows: Option<DeserializedMetadataAndRawRows>,
    tracing_id: Option<Uuid>,
    warnings: Vec<String>,
}

impl QueryResult {
    pub(crate) fn new(
        request_coordinator: Coordinator,
        raw_rows: Option<DeserializedMetadataAndRawRows>,
        tracing_id: Option<Uuid>,
        warnings: Vec<String>,
    ) -> Self {
        Self {
            request_coordinator: Some(request_coordinator),
            deserialized_metadata_and_rows: raw_rows,
            tracing_id,
            warnings,
        }
    }

    /// HACK: This is the only way to create a [QueryResult] with `request_coordinator` set to [None].
    ///
    /// Rationale: driver uses [QueryResult] internally even if it does not have a [Node](crate::cluster::Node)
    /// corresponding to the connection that it executes requests on. This happens in
    /// non-[Session](crate::client::session::Session) APIs (`MetadataReader::query_metadata()`,
    /// `Connection::query_iter()`, etc.), most notably for a control connection upon initial metadata fetch.
    /// However, [QueryResult::request_coordinator] panics if [Coordinator] stored is [None].
    /// Therefore, extra care must be taken not to leak such [QueryResult] to the user.
    pub(crate) fn new_with_unknown_coordinator(
        raw_rows: Option<DeserializedMetadataAndRawRows>,
        tracing_id: Option<Uuid>,
        warnings: Vec<String>,
    ) -> Self {
        Self {
            request_coordinator: None,
            deserialized_metadata_and_rows: raw_rows,
            tracing_id,
            warnings,
        }
    }

    // Preferred to implementing Default, because users shouldn't be able to create
    // an empty QueryResult.
    pub(crate) fn mock_empty(request_coordinator: Coordinator) -> Self {
        Self {
            request_coordinator: Some(request_coordinator),
            deserialized_metadata_and_rows: None,
            tracing_id: None,
            warnings: Vec::new(),
        }
    }

    pub(crate) fn deserialized_metadata_and_rows(&self) -> Option<&DeserializedMetadataAndRawRows> {
        self.deserialized_metadata_and_rows.as_ref()
    }

    /// The node+shard that served the request.
    #[inline]
    pub fn request_coordinator(&self) -> &Coordinator {
        self.request_coordinator
            .as_ref()
            .expect("BUG: Driver leaked a QueryResult with an unknown Coordinator, even though such results are driver-internal.")
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
        self.deserialized_metadata_and_rows.is_some()
    }

    /// Returns `Ok` for a request's result that shouldn't contain any rows.\
    /// Will return `Ok` for `INSERT` result, but a `SELECT` result, even an empty one, will cause an error.\
    /// Opposite of [QueryResult::into_rows_result].
    #[inline]
    pub fn result_not_rows(&self) -> Result<(), ResultNotRowsError> {
        match &self.deserialized_metadata_and_rows {
            Some(_) => Err(ResultNotRowsError),
            None => Ok(()),
        }
    }

    /// Transforms itself into the Rows result type to enable deserializing rows.
    /// Deserializes result metadata and allocates it.
    ///
    /// Returns an error if the response is not of Rows kind or metadata deserialization failed.
    ///
    /// ```rust
    /// # use scylla::response::query_result::{QueryResult, QueryRowsResult};
    /// # fn example(query_result: QueryResult) -> Result<(), Box<dyn std::error::Error>> {
    /// let rows_result = query_result.into_rows_result()?;
    ///
    /// let mut rows_iter = rows_result.rows::<(i32, &str)>()?;
    /// while let Some((num, text)) = rows_iter.next().transpose()? {
    ///     // do something with `num` and `text``
    /// }
    ///
    /// Ok(())
    /// # }
    ///
    /// ```
    ///
    /// If the response is not of Rows kind, the original [`QueryResult`] (self) is
    /// returned back to the user in the error type. See [`IntoRowsResultError`] documentation.
    ///
    /// ```rust
    /// # use scylla::response::query_result::{QueryResult, QueryRowsResult, IntoRowsResultError};
    /// # fn example(non_rows_query_result: QueryResult) -> Result<(), Box<dyn std::error::Error>> {
    /// let err = non_rows_query_result.into_rows_result().unwrap_err();
    ///
    /// match err {
    ///     IntoRowsResultError::ResultNotRows(query_result) => {
    ///         // do something with original `query_result`
    ///     }
    ///     _ => {
    ///         // deserialization failed - query result is not recovered
    ///     }
    /// }
    ///
    /// Ok(())
    /// # }
    /// ```
    // `allow(clippy::result_large_err)` is fine. `QueryRowsResult` is larger than `IntoRowsResultError`.
    #[allow(clippy::result_large_err)]
    pub fn into_rows_result(self) -> Result<QueryRowsResult, IntoRowsResultError> {
        let Some(raw_rows_with_metadata) = self.deserialized_metadata_and_rows else {
            return Err(IntoRowsResultError::ResultNotRows(self));
        };
        let tracing_id = self.tracing_id;
        let warnings = self.warnings;
        let request_coordinator = self.request_coordinator;

        Ok(QueryRowsResult {
            request_coordinator,
            raw_rows_with_metadata,
            warnings,
            tracing_id,
        })
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
/// # use scylla::response::query_result::QueryResult;
/// # fn example(query_result: QueryResult) -> Result<(), Box<dyn std::error::Error>> {
/// let rows_result = query_result.into_rows_result()?;
///
/// let mut rows_iter = rows_result.rows::<(i32, &str)>()?;
/// while let Some((num, text)) = rows_iter.next().transpose()? {
///     // do something with `num` and `text``
/// }
///
/// Ok(())
/// # }
///
/// ```
#[derive(Debug)]
pub struct QueryRowsResult {
    /// May be `None` only for results of driver's internal requests.
    /// If user gets a `QueryResult` with `request_coordinator` set to `None`,
    /// this is a bug.
    request_coordinator: Option<Coordinator>,
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

    /// The node+shard that served the request.
    #[inline]
    pub fn request_coordinator(&self) -> &Coordinator {
        self.request_coordinator
            .as_ref()
            .expect("BUG: Driver leaked a QueryResult with an unknown Coordinator, even though such results are driver-internal.")
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
    pub fn column_specs(&self) -> ColumnSpecs<'_, '_> {
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

    /// Deconstructs the `QueryRowsResult` into its components, which can be used by the caller
    /// directly. Intended for use in CPP-Rust Driver only.
    #[cfg(cpp_rust_unstable)]
    pub fn into_inner(
        self,
    ) -> (
        DeserializedMetadataAndRawRows,
        Option<Uuid>,
        Vec<String>,
        Option<Coordinator>,
    ) {
        let Self {
            raw_rows_with_metadata,
            tracing_id,
            warnings,
            request_coordinator,
        } = self;

        (
            raw_rows_with_metadata,
            tracing_id,
            warnings,
            request_coordinator,
        )
    }
}

/// An error returned by [`QueryResult::into_rows_result`]
///
/// The `ResultNotRows` variant contains original [`QueryResult`],
/// which otherwise would be consumed and lost.
#[derive(Debug, Error, Clone)]
pub enum IntoRowsResultError {
    /// Result is not of Rows kind
    #[error("Result is not of Rows kind")]
    ResultNotRows(QueryResult),

    // transparent because the underlying error provides enough context.
    /// Failed to lazily deserialize result metadata.
    #[error(transparent)]
    ResultMetadataLazyDeserializationError(#[from] ResultMetadataAndRowsCountParseError),
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
    use scylla_cql::frame::response::result::{ColumnType, NativeType, ResultMetadata, TableSpec};
    use scylla_cql::frame::types;

    use super::*;

    const TABLE_SPEC: TableSpec<'static> = TableSpec::borrowed("ks", "tbl");

    fn column_spec_infinite_iter() -> impl Iterator<Item = ColumnSpec<'static>> {
        (0..).map(|k| {
            ColumnSpec::owned(
                format!("col_{k}"),
                match k % 3 {
                    0 => ColumnType::Native(NativeType::Ascii),
                    1 => ColumnType::Native(NativeType::Boolean),
                    2 => ColumnType::Native(NativeType::Float),
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

        fn sample_raw_rows(cols: usize, rows: usize) -> DeserializedMetadataAndRawRows {
            let metadata = sample_result_metadata(cols);

            static STRING: &[u8] = "MOCK".as_bytes();
            static BOOLEAN: &[u8] = &(true as i8).to_be_bytes();
            static FLOAT: &[u8] = &12341_i32.to_be_bytes();
            let cells = metadata.col_specs().iter().map(|spec| match spec.typ() {
                ColumnType::Native(NativeType::Ascii) => STRING,
                ColumnType::Native(NativeType::Boolean) => BOOLEAN,
                ColumnType::Native(NativeType::Float) => FLOAT,
                _ => unreachable!(),
            });
            let bytes = serialize_cells(cells.map(Some));
            DeserializedMetadataAndRawRows::new_for_test(metadata, rows, bytes)
        }

        // Used to trigger DeserializationError.
        fn sample_raw_rows_invalid_bytes(
            cols: usize,
            rows: usize,
        ) -> DeserializedMetadataAndRawRows {
            let metadata = sample_result_metadata(cols);

            DeserializedMetadataAndRawRows::new_for_test(metadata, rows, Bytes::new())
        }

        // Check tracing ID
        for tracing_id in [None, Some(Uuid::from_u128(0x_feed_dead))] {
            for raw_rows in [None, Some(sample_raw_rows(7, 6))] {
                let qr = QueryResult::new_with_unknown_coordinator(raw_rows, tracing_id, vec![]);
                assert_eq!(qr.tracing_id(), tracing_id);
            }
        }

        // Check warnings
        for raw_rows in [None, Some(sample_raw_rows(7, 6))] {
            let warnings = &["Ooops", "Meltdown..."];
            let qr = QueryResult::new_with_unknown_coordinator(
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
                let rqr = QueryResult::new_with_unknown_coordinator(None, None, Vec::new());
                let qr = rqr.into_rows_result();
                assert_matches!(qr, Err(IntoRowsResultError::ResultNotRows(_)));
            }

            // RESULT::Rows response -> some column specs
            {
                let n = 5;
                let metadata = sample_result_metadata(n);
                let rr = DeserializedMetadataAndRawRows::new_for_test(metadata, 0, Bytes::new());
                let rqr = QueryResult::new_with_unknown_coordinator(Some(rr), None, Vec::new());
                let qr = rqr.into_rows_result().unwrap();
                let column_specs = qr.column_specs();
                assert_eq!(column_specs.len(), n);

                // By index
                {
                    for (i, expected_col_spec) in column_spec_infinite_iter().enumerate().take(n) {
                        assert_eq!(column_specs.get_by_index(i), Some(&expected_col_spec));
                    }

                    assert_matches!(column_specs.get_by_index(n), None);
                }

                // By name
                {
                    for (idx, expected_col_spec) in column_spec_infinite_iter().enumerate().take(n)
                    {
                        let name = expected_col_spec.name();
                        assert_eq!(
                            column_specs.get_by_name(name),
                            Some((idx, &expected_col_spec))
                        );
                    }

                    assert_matches!(column_specs.get_by_name("ala ma kota"), None);
                }

                // By iter
                {
                    for (got_view, expected_col_spec) in
                        column_specs.iter().zip(column_spec_infinite_iter())
                    {
                        assert_eq!(got_view, &expected_col_spec);
                    }
                }
            }
        }

        // rows(), maybe_rows(), result_not_rows(), first_row(), maybe_first_row(), single_row()
        // All errors are checked.
        {
            // Not RESULT::Rows
            {
                let rqr = QueryResult::new_with_unknown_coordinator(None, None, Vec::new());
                let qr = rqr.into_rows_result();
                assert_matches!(qr, Err(IntoRowsResultError::ResultNotRows(_)));
            }

            // RESULT::Rows with 0 rows
            {
                let rr = sample_raw_rows(1, 0);
                let rqr = QueryResult::new_with_unknown_coordinator(Some(rr), None, Vec::new());
                assert_matches!(rqr.result_not_rows(), Err(ResultNotRowsError));

                let qr = rqr.into_rows_result().unwrap();

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
                let rqr_good_data =
                    QueryResult::new_with_unknown_coordinator(Some(rr_good_data), None, Vec::new());
                let rqr_bad_data =
                    QueryResult::new_with_unknown_coordinator(Some(rr_bad_data), None, Vec::new());

                for rqr in [&rqr_good_data, &rqr_bad_data] {
                    assert_matches!(rqr.result_not_rows(), Err(ResultNotRowsError));
                }

                let qr_good_data = rqr_good_data.into_rows_result().unwrap();
                let qr_bad_data = rqr_bad_data.into_rows_result().unwrap();

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
                let rqr = QueryResult::new_with_unknown_coordinator(Some(rr), None, Vec::new());
                assert_matches!(rqr.result_not_rows(), Err(ResultNotRowsError));

                let qr = rqr.into_rows_result().unwrap();

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

    #[test]
    fn test_query_result_returns_self_if_not_rows() {
        // Check tracing ID
        for tracing_id in [None, Some(Uuid::from_u128(0x_feed_dead))] {
            let qr = QueryResult::new_with_unknown_coordinator(None, tracing_id, vec![]);
            let err = qr.into_rows_result().unwrap_err();
            match err {
                IntoRowsResultError::ResultNotRows(query_result) => {
                    assert_eq!(query_result.tracing_id, tracing_id)
                }
                IntoRowsResultError::ResultMetadataLazyDeserializationError(_) => {
                    panic!("Expected ResultNotRows error")
                }
            }
        }

        // Check warnings
        {
            let warnings = &["Ooops", "Meltdown..."];
            let qr = QueryResult::new_with_unknown_coordinator(
                None,
                None,
                warnings.iter().copied().map(String::from).collect(),
            );
            let err = qr.into_rows_result().unwrap_err();
            match err {
                IntoRowsResultError::ResultNotRows(query_result) => {
                    assert_eq!(query_result.warnings().collect_vec(), warnings)
                }
                IntoRowsResultError::ResultMetadataLazyDeserializationError(_) => {
                    panic!("Expected ResultNotRows error")
                }
            }
        }
    }
}
