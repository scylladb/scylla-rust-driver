use std::fmt::Debug;
use std::fmt;
use chrono::{NaiveTime, TimeZone, Utc};
use scylla_cql::frame::value::{CqlTime, CqlTimestamp};
use tabled::{builder::Builder, settings::Style, settings::themes::Colorization, settings::Color};

use thiserror::Error;
use uuid::Uuid;

use scylla_cql::frame::frame_errors::ResultMetadataAndRowsCountParseError;
use scylla_cql::frame::response::result::{
    ColumnSpec, ColumnType, CqlValue, DeserializedMetadataAndRawRows, RawMetadataAndRawRows, Row, TableSpec
};
use scylla_cql::types::deserialize::result::TypedRowIterator;
use scylla_cql::types::deserialize::row::DeserializeRow;
use scylla_cql::types::deserialize::{DeserializationError, TypeCheckError};

#[allow(deprecated)]
use super::legacy_query_result::{IntoLegacyQueryResultError, LegacyQueryResult};

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
#[derive(Debug, Clone)]
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
    /// Returns an error if the response is not of Rows kind or metadata deserialization failed.
    ///
    /// ```rust
    /// # use scylla::transport::query_result::{QueryResult, QueryRowsResult};
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
    /// # use scylla::transport::query_result::{QueryResult, QueryRowsResult, IntoRowsResultError};
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
    pub fn into_rows_result(self) -> Result<QueryRowsResult, IntoRowsResultError> {
        let Some(raw_metadata_and_rows) = self.raw_metadata_and_rows else {
            return Err(IntoRowsResultError::ResultNotRows(self));
        };
        let tracing_id = self.tracing_id;
        let warnings = self.warnings;

        let raw_rows_with_metadata = raw_metadata_and_rows.deserialize_metadata()?;
        Ok(QueryRowsResult {
            raw_rows_with_metadata,
            warnings,
            tracing_id,
        })
    }

    /// Transforms itself into the legacy result type, by eagerly deserializing rows
    /// into the Row type. This is inefficient, and should only be used during transition
    /// period to the new API.
    #[deprecated(
        since = "0.15.0",
        note = "Legacy deserialization API is inefficient and is going to be removed soon"
    )]
    #[allow(deprecated)]
    pub fn into_legacy_result(self) -> Result<LegacyQueryResult, IntoLegacyQueryResultError> {
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

    pub fn rows_displayer<'a>(&'a self) -> RowsDisplayer<'a>
        {
            RowsDisplayer::new(self)
        }
}

pub struct RowsDisplayer<'a> {
    query_result: &'a QueryRowsResult,
    display_settings: RowsDisplayerSettings,
    terminal_width: usize,
}

impl<'a> RowsDisplayer<'a>
{
    pub fn new(query_result: &'a QueryRowsResult) -> Self {
        Self {
            query_result,
            display_settings: RowsDisplayerSettings::new(),
            terminal_width: 80,
        }
    }

    pub fn set_terminal_width(&mut self, terminal_width: usize) {
        self.terminal_width = terminal_width;
    }


    fn get_item_wrapper(&'a self, item: &'a std::option::Option<CqlValue>) -> Box<dyn StringConvertible<'a>> {
        match item {
            Some(CqlValue::Ascii(value)) => {
                Box::new(WrapperDisplay{value: value, settings: &self.display_settings})
            },
            Some(CqlValue::BigInt(value)) => {
                Box::new(WrapperDisplay{value: &None, settings: &self.display_settings})
            },
            Some(CqlValue::Blob(value)) => {
                Box::new(WrapperDisplay{value: value, settings: &self.display_settings})
            },
            Some(CqlValue::Boolean(value)) => {
                Box::new(WrapperDisplay{value: &None, settings: &self.display_settings})
            },
            Some(CqlValue::Counter(value)) => {
                Box::new(WrapperDisplay{value: &None, settings: &self.display_settings})
            },
            Some(CqlValue::Decimal(value)) => {
                Box::new(WrapperDisplay{value: &None, settings: &self.display_settings})
            },
            Some(CqlValue::Double(value)) => { // TODO set formating for real numbers
                Box::new(WrapperDisplay{value: value, settings: &self.display_settings})
            },
            Some(CqlValue::Float(value)) => {
                Box::new(WrapperDisplay{value: value, settings: &self.display_settings})
            },
            Some(CqlValue::Int(value)) => { // TODO set formating for integers
                Box::new(WrapperDisplay{value: value, settings: &self.display_settings})
            },
            Some(CqlValue::Text(value)) => {
                Box::new(WrapperDisplay{value: value, settings: &self.display_settings})
            },
            Some(CqlValue::Timestamp(value)) => { // TOOD set formating for timestamp
                Box::new(WrapperDisplay{value: value, settings: &self.display_settings})
            },
            Some(CqlValue::Uuid(value)) => {
                Box::new(WrapperDisplay{value: &None, settings: &self.display_settings})
            },
            Some(CqlValue::Inet(value)) => {
                Box::new(WrapperDisplay{value: &None, settings: &self.display_settings})
            },
            Some(CqlValue::List(value)) => { // TODO set formating for list
                Box::new(WrapperDisplay{value: &None, settings: &self.display_settings})
            },
            Some(CqlValue::Map(value)) => { // TODO set formating for map
                Box::new(WrapperDisplay{value: &None, settings: &self.display_settings})
            },
            Some(CqlValue::Set(value)) => { // TODO set formating for set
                Box::new(WrapperDisplay{value: &None, settings: &self.display_settings})
            },
            Some(CqlValue::UserDefinedType { keyspace, type_name, fields }) => { // Idk what to do with this
                Box::new(WrapperDisplay{value: &None, settings: &self.display_settings})
            },
            Some(CqlValue::Tuple(value)) => { // TODO set formating for tuple
                Box::new(WrapperDisplay{value: &None, settings: &self.display_settings})
            },
            Some(CqlValue::Date(value)) => { // TODO set formating for date
                Box::new(WrapperDisplay{value: &None, settings: &self.display_settings})
            },
            Some(CqlValue::Duration(value)) => {
                Box::new(WrapperDisplay{value: &None, settings: &self.display_settings})
            },
            Some(CqlValue::Empty) => {
                Box::new(WrapperDisplay{value: &None, settings: &self.display_settings})
            },
            Some(CqlValue::SmallInt(value)) => {
                Box::new(WrapperDisplay{value: &None, settings: &self.display_settings})
            },
            Some(CqlValue::TinyInt(value)) => {
                Box::new(WrapperDisplay{value: value, settings: &self.display_settings})
            },
            Some(CqlValue::Varint(value)) => {
                Box::new(WrapperDisplay{value: &None, settings: &self.display_settings})
            },
            Some(CqlValue::Time(value)) => {
                Box::new(WrapperDisplay{value: value, settings: &self.display_settings})
            },
            Some(CqlValue::Timeuuid(value)) => {
                Box::new(WrapperDisplay{value: &None, settings: &self.display_settings})
            },
            None => {
                Box::new(WrapperDisplay{value: &None, settings: &self.display_settings})
            },
        }
    }


}

// wrappers for scylla datatypes implementing Display

struct WrapperDisplay<'a, T: 'a>{
    value: &'a T,
    settings: &'a RowsDisplayerSettings,
}

// Trait bound to ensure From<WrapperDisplay<T>> for String is implemented
trait StringConvertible<'a>: 'a {
    fn to_string(&self) -> String;
}

// Implement the trait for types that have From<WrapperDisplay<T>> for String
impl<'a, T> StringConvertible<'a> for WrapperDisplay<'a, T> 
where 
    WrapperDisplay<'a, T>: fmt::Display
{
    fn to_string(&self) -> String {
        format!("{}", self)
    }
}

// generic impl of From ... for String
impl<'a> From<Box<dyn StringConvertible<'a>>> for String {
    fn from(wrapper: Box<dyn StringConvertible<'a>>) -> Self {
        wrapper.to_string()
    }
}

impl<'a> From<&dyn StringConvertible<'a>> for String {
    fn from(wrapper: &dyn StringConvertible<'a>) -> Self {
        // println!("before &dyn StringConvertible<'a>");
        wrapper.into()
    }
}

impl<'a, T> From<WrapperDisplay<'a, T>> for String
where
    T: 'a,
    WrapperDisplay<'a, T>: fmt::Display,
{
    fn from(wrapper: WrapperDisplay<'a, T>) -> Self {
        // println!("before WrapperDisplay<'a, T>");
        format!("{}", wrapper)
    }
}

// Actual implementations of Display for scylla datatypes

// none WrapperDisplay
impl fmt::Display for WrapperDisplay<'_, std::option::Option<CqlValue>> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "None")
    }    
}

// tiny int
impl fmt::Display for WrapperDisplay<'_, i8> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.settings.exponent_displaying_floats {
            write!(f, "{:e}", self.value)
        } else {
            write!(f, "{}", self.value)
        }
    }
}

// small int
impl fmt::Display for WrapperDisplay<'_, i16> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.settings.exponent_displaying_floats {
            write!(f, "{:e}", self.value)
        } else {
            write!(f, "{}", self.value)
        }
    }
}

// int
impl fmt::Display for WrapperDisplay<'_, i32> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.settings.exponent_displaying_floats {
            write!(f, "{:e}", self.value)
        } else {
            write!(f, "{}", self.value)
        }
    }
}

// bigint

impl fmt::Display for WrapperDisplay<'_, i64> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.settings.exponent_displaying_floats {
            write!(f, "{:e}", self.value)
        } else {
            write!(f, "{}", self.value)
        }
    }
}

// float

impl fmt::Display for WrapperDisplay<'_, f32> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.settings.exponent_displaying_floats {
            write!(f, "{:e}", self.value)
        } else {
            write!(f, "{}", self.value)
        }
    }
}

// double

impl fmt::Display for WrapperDisplay<'_, f64> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.settings.exponent_displaying_floats {
            write!(f, "{:e}", self.value)
        } else {
            write!(f, "{:.digits$}", self.value, digits = self.settings.double_precision)
        }
    }
}

// blob

impl fmt::Display for WrapperDisplay<'_, Vec<u8>> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut result = String::new();
        if self.settings.byte_displaying == ByteDisplaying::Hex {
            result.push_str("0x");
        }
        for byte in self.value {
            match self.settings.byte_displaying {
                ByteDisplaying::Ascii => {
                    result.push_str(&format!("{}", *byte as char));
                },
                ByteDisplaying::Hex => {
                    result.push_str(&format!("{:02x}", byte));
                },
                ByteDisplaying::Dec => {
                    result.push_str(&format!("{}", byte));
                },
            }
        }
        write!(f, "{}", result)
    }
}

// string

impl fmt::Display for WrapperDisplay<'_, String> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.value)
    }
}

// timestamp

impl fmt::Display for WrapperDisplay<'_, CqlTimestamp> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // egzample of formating timestamp 14:30:00.000000000
        let seconds_from_epoch = self.value.0;
        let datetime = Utc.timestamp_millis_opt(seconds_from_epoch)
        .single()
        .expect("Invalid timestamp");
    
        write!(f, "{}.{:06}+0000", 
            datetime.format("%Y-%m-%d %H:%M:%S"), 
            datetime.timestamp_subsec_micros())
    }
}

// time

impl fmt::Display for WrapperDisplay<'_, CqlTime> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // egzample of formating time 14:30:00.000000000
        let nanoseconds = self.value.0;
        let total_seconds = nanoseconds / 1_000_000_000;
        let hours = total_seconds / 3600;
        let minutes = (total_seconds % 3600) / 60;
        let seconds = total_seconds % 60;
        let nanos = nanoseconds % 1_000_000_000;

        // Create NaiveTime with the calculated components
        let time = NaiveTime::from_hms_nano_opt(
            hours as u32, 
            minutes as u32, 
            seconds as u32, 
            nanos as u32
        ).expect("Invalid time");

        // Format the time with 9 digits of nanoseconds
    
        write!(f, "{}", time.format("%H:%M:%S.%9f"))
    }
}




impl fmt::Display for RowsDisplayer<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let row_iter : TypedRowIterator<'_, '_, Row> = match self.query_result.rows::<Row>(){
            Ok(row_iter) => row_iter,
            Err(_) => return write!(f, "Error"),
        };

        // put columns names to the table
        let column_names : Vec<&str> = self.query_result.column_specs().iter().map(|column_spec| column_spec.name()).collect();
        let mut builder: Builder = Builder::new();
        builder.push_record(column_names);

        // put rows to the table
        for row_result in row_iter {
            // println!("row");
            let row_result : Row = match row_result {
                Ok(row_result) => row_result,
                Err(_) => return write!(f, "Error"),
            };
            let columns : Vec<std::option::Option<CqlValue>> =  row_result.columns;
            let mut row_values: Vec<Box<dyn StringConvertible>> = Vec::new();
            for item in &columns {
                let wrapper = self.get_item_wrapper(item);
                row_values.push(wrapper);
            }
            builder.push_record(row_values);
        }

        // write table to the formatter
        let mut table = builder.build();
        table.with(Style::psql())
            // Width::wrap(self.terminal_width).priority(Priority::max(true)),
            .with(Colorization::columns([Color::FG_GREEN]))
            .with(Colorization::exact([Color::FG_MAGENTA], tabled::settings::object::Rows::first()));
        
        write!(f, "{}", table)
    }
}

struct RowsDisplayerSettings {
    byte_displaying: ByteDisplaying, // for blobs
    exponent_displaying_floats: bool, // for floats
    exponent_displaying_integers: bool, // for integers 
    double_precision: usize, // for doubles
}

impl RowsDisplayerSettings {
    fn new() -> Self { // TODO write Default trait
        Self {
            byte_displaying: ByteDisplaying::Hex,
            exponent_displaying_floats: false,
            exponent_displaying_integers: false,
            double_precision: 5,
        }
    }

    fn set_byte_displaying(&mut self, byte_displaying: ByteDisplaying) {
        self.byte_displaying = byte_displaying;
    }

    fn set_exponent_displaying_floats(&mut self, exponent_displaying_floats: bool) {
        self.exponent_displaying_floats = exponent_displaying_floats;
    }

    fn set_exponent_displaying_integers(&mut self, exponent_displaying_integers: bool) {
        self.exponent_displaying_integers = exponent_displaying_integers;
    }
}

#[derive(PartialEq)]
enum ByteDisplaying {
    Ascii,
    Hex,
    Dec,
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
    use scylla_cql::frame::response::result::ResultMetadata;
    use scylla_cql::frame::types;
use std::fmt::Write;

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
                let qr = rqr.into_rows_result();
                assert_matches!(qr, Err(IntoRowsResultError::ResultNotRows(_)));
            }

            // RESULT::Rows response -> some column specs
            {
                let n = 5;
                let metadata = sample_result_metadata(n);
                let rr = RawMetadataAndRawRows::new_for_test(None, Some(metadata), false, 0, &[])
                    .unwrap();
                let rqr = QueryResult::new(Some(rr), None, Vec::new());
                let qr = rqr.into_rows_result().unwrap();
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
                let qr = rqr.into_rows_result();
                assert_matches!(qr, Err(IntoRowsResultError::ResultNotRows(_)));
            }

            // RESULT::Rows with 0 rows
            {
                let rr = sample_raw_rows(1, 0);
                let rqr = QueryResult::new(Some(rr), None, Vec::new());
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
                let rqr_good_data = QueryResult::new(Some(rr_good_data), None, Vec::new());
                let rqr_bad_data = QueryResult::new(Some(rr_bad_data), None, Vec::new());

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
                let rqr = QueryResult::new(Some(rr), None, Vec::new());
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
            let qr = QueryResult::new(None, tracing_id, vec![]);
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
            let qr = QueryResult::new(
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
