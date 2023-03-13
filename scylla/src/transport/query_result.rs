use std::fmt::Debug;
use std::sync::Arc;

use thiserror::Error;
use uuid::Uuid;

use scylla_cql::frame::frame_errors::RowsParseError;
use scylla_cql::frame::response::result::{
    ColumnSpec, ColumnType, DeserializedMetadataAndRawRows, RawMetadataAndRawRows, RawRowsBorrowed,
    RawRowsKind, RawRowsOwned, Row, TableSpec,
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
/// are kept in a raw binary form. To deserialize and access them, this struct works
/// in tandem with [`RowsDeserializer`] struct, which borrows from [`QueryResult`].
/// By borrowing, [`RowsDeserializer`] can avoid heap allocations of metadata strings,
/// borrowing them from the Result frame instead.
/// To create a [`RowsDeserializer`], use [`QueryResult::rows_deserializer`] method.
/// Upon creation, [`RowsDeserializer`] deserializes result metadata and allocates it,
/// so this should be considered a moderately costly operation and performed only once.
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
    /// Opposite of [`rows_deserializer()`](QueryResult::rows_deserializer).
    #[inline]
    pub fn result_not_rows(&self) -> Result<(), ResultNotRowsError> {
        match &self.raw_metadata_and_rows {
            Some(_) => Err(ResultNotRowsError),
            None => Ok(()),
        }
    }

    /// Creates a lifetime-bound [`RowsDeserializer`] to enable deserializing rows contained
    /// in this [`QueryResult`]'s frame. Deserializes result metadata and allocates it,
    /// so **this should be considered a moderately costly operation and performed only once**.
    ///
    /// Returns `None` if the response is not of Rows kind.
    ///
    /// The created [`RowsDeserializer`] borrows from the [`QueryResult`], which saves some
    /// string heap allocations, but limits flexibility (e.g., such borrowing [`RowsDeserializer`]
    /// can't be stored aside on a heap due to lifetime issues).
    /// To gain more flexibility on cost of additional allocations,
    /// use [`QueryResult::rows_deserializer_owned`].
    ///
    /// ```rust
    /// # use scylla::transport::query_result::{QueryResult, RowsDeserializer};
    /// # fn example(query_result: QueryResult) -> Result<(), Box<dyn std::error::Error>> {
    /// let rows_deserializer = query_result.rows_deserializer()?;
    /// if let Some(rows_result) = rows_deserializer {
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
    pub fn rows_deserializer(
        &self,
    ) -> Result<Option<RowsDeserializer<'_, RawRowsBorrowed<'_>>>, RowsParseError> {
        self.raw_metadata_and_rows
            .as_ref()
            .map(|raw_rows| {
                let raw_rows_with_metadata = raw_rows.deserialize_borrowed_metadata()?;
                Ok(RowsDeserializer {
                    raw_rows_with_metadata,
                })
            })
            .transpose()
    }

    /// Creates an owned [`RowsDeserializer`] to enable deserializing rows contained
    /// in this [`QueryResult`]'s frame. Deserializes result metadata and allocates it,
    /// so this should be considered a moderately costly operation and performed only once.
    ///
    /// Returns `None` if the response is not of Rows kind.
    ///
    /// The created [`RowsDeserializer`] does not borrow from the [`QueryResult`],
    /// so it does not not limit flexibility. However, the cost involves more string
    /// heap allocations.
    /// If you don't need that flexibility, use cheaper [`QueryResult::rows_deserializer`].
    ///
    /// ```compile_fail
    /// # use scylla::transport::QueryResult;
    /// fn example(query: impl FnOnce() -> QueryResult) -> Result<(), Box<dyn std::error::Error>> {
    ///     let deserializer = query().rows_deserializer()?.unwrap();
    ///
    ///     // Compiler complains: "Temporary value dropped when borrowed".
    ///     let col_specs = deserializer.column_specs();
    ///
    ///     Ok(())
    /// }
    /// ```
    ///
    /// ```rust
    /// # use scylla::transport::query_result::{QueryResult, RowsDeserializerOwning};
    /// fn example(
    ///     query: impl FnOnce() -> QueryResult
    /// ) -> Result<RowsDeserializerOwning, Box<dyn std::error::Error>> {
    ///     let deserializer = query().rows_deserializer_owned()?.unwrap();
    ///
    ///     // This compiles.
    ///     let col_specs = deserializer.column_specs();
    ///
    ///     // RowsDeserializer is fully owned and independent, but at cost
    ///     // of moderately more expensive metadata deserialization.
    ///     Ok(deserializer)
    /// }
    /// ```
    pub fn rows_deserializer_owned(
        &self,
    ) -> Result<Option<RowsDeserializer<'static, RawRowsOwned>>, RowsParseError> {
        self.raw_metadata_and_rows
            .as_ref()
            .map(|raw_rows| {
                let raw_rows_with_metadata = raw_rows.deserialize_owned_metadata()?;
                Ok(RowsDeserializer {
                    raw_rows_with_metadata,
                })
            })
            .transpose()
    }

    /// Transforms itself into the legacy result type, by eagerly deserializing rows
    /// into the Row type. This is inefficient, and should only be used during transition
    /// period to the new API.
    pub fn into_legacy_result(self) -> Result<LegacyQueryResult, RowsParseError> {
        if let Some(raw_rows) = self.raw_metadata_and_rows {
            let raw_rows_with_metadata = raw_rows.deserialize_owned_metadata()?;

            let deserialized_rows = raw_rows_with_metadata
                .rows_iter::<Row>()?
                .collect::<Result<Vec<_>, DeserializationError>>()?;
            let serialized_size = raw_rows_with_metadata.rows_bytes_size();
            let metadata = raw_rows_with_metadata.into_metadata();

            Ok(LegacyQueryResult {
                rows: Some(deserialized_rows),
                warnings: self.warnings,
                tracing_id: self.tracing_id,
                metadata: Some(Arc::new((*metadata).clone())),
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

/// Enables deserialization of rows contained in a [`QueryResult`].
///
/// Upon creation, it deserializes result metadata and allocates it,
/// so this should be considered a moderately costly operation and performed
/// only once.
///
/// This struct provides generic methods which enable typed access to the data,
/// by deserializing rows on the fly to the type provided as a type parameter.
/// Those methods are:
/// - rows() - for iterating through rows,
/// - first_row() and maybe_first_row() - for accessing the first row first,
/// - single_row() - for accessing the first row, additionally asserting
///   that it's the only one in the response.
///
/// ```rust
/// # use scylla::transport::query_result::QueryResult;
/// # fn example(query_result: QueryResult) -> Result<(), Box<dyn std::error::Error>> {
/// let rows_deserializer = query_result.rows_deserializer()?;
/// if let Some(rows_result) = rows_deserializer {
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
pub struct RowsDeserializer<'frame, Kind: RawRowsKind> {
    raw_rows_with_metadata: DeserializedMetadataAndRawRows<'frame, Kind>,
}

pub type RowsDeserializerOwning = RowsDeserializer<'static, RawRowsOwned>;
pub type RowsDeserializerBorrowing<'frame> = RowsDeserializer<'frame, RawRowsBorrowed<'frame>>;

impl<'frame, RawRows: RawRowsKind> RowsDeserializer<'frame, RawRows> {
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
}

impl<'frame> RowsDeserializer<'frame, RawRowsBorrowed<'frame>> {
    /// Returns the received rows when present.
    ///
    /// Returns an error if the rows in the response are of incorrect type.
    #[inline]
    pub fn rows<'metadata, R: DeserializeRow<'frame, 'metadata>>(
        &'metadata self,
    ) -> Result<TypedRowIterator<'frame, 'metadata, R>, RowsError> {
        self.raw_rows_with_metadata
            .rows_iter()
            .map_err(RowsError::TypeCheckFailed)
    }

    /// Returns `Option<R>` containing the first of a result.
    ///
    /// Fails when the the rows in the response are of incorrect type,
    /// or when the deserialization fails.
    pub fn maybe_first_row<'metadata, R: DeserializeRow<'frame, 'metadata>>(
        &'metadata self,
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

    /// Returns first row from the received rows.
    ///
    /// When the first row is not available, returns an error.
    /// Fails when the the rows in the response are of incorrect type,
    /// or when the deserialization fails.
    pub fn first_row<'metadata, R: DeserializeRow<'frame, 'metadata>>(
        &'metadata self,
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
    pub fn single_row<'metadata, R: DeserializeRow<'frame, 'metadata>>(
        &'metadata self,
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

impl RowsDeserializer<'static, RawRowsOwned> {
    /// Returns the received rows when present.
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

    /// Returns `Option<R>` containing the first of a result.
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

    /// Returns first row from the received rows.
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

/// An error returned by [`RowsDeserializer::rows`].
#[derive(Debug, Error)]
pub enum RowsError {
    /// Type check failed
    #[error("Type check failed: {0}")]
    TypeCheckFailed(#[from] TypeCheckError),
}

/// An error returned by [`RowsDeserializer::maybe_first_row`].
#[derive(Debug, Error)]
pub enum MaybeFirstRowError {
    /// Type check failed
    #[error("Type check failed: {0}")]
    TypeCheckFailed(#[from] TypeCheckError),

    /// Deserialization failed
    #[error("Deserialization failed: {0}")]
    DeserializationFailed(#[from] DeserializationError),
}

/// An error returned by [`RowsDeserializer::first_row`].
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

/// An error returned by [`RowsDeserializer::single_row`].
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
