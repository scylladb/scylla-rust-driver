use crate::cql_to_rust::{FromRow, FromRowError};
use crate::frame::frame_errors::{
    ColumnSpecParseError, ColumnSpecParseErrorKind, CqlResultParseError, CqlTypeParseError,
    LowLevelDeserializationError, PreparedParseError, ResultMetadataParseError, RowsParseError,
    SchemaChangeEventParseError, SetKeyspaceParseError, TableSpecParseError,
};
use crate::frame::request::query::PagingStateResponse;
use crate::frame::response::event::SchemaChangeEvent;
use crate::frame::types;
use crate::frame::value::{
    Counter, CqlDate, CqlDecimal, CqlDuration, CqlTime, CqlTimestamp, CqlTimeuuid, CqlVarint,
};
use crate::types::deserialize::result::{RowIterator, TypedRowIterator};
use crate::types::deserialize::value::{
    mk_deser_err, BuiltinDeserializationErrorKind, DeserializeValue, MapIterator, UdtIterator,
};
use crate::types::deserialize::{DeserializationError, FrameSlice};
use bytes::{Buf, Bytes};
use std::borrow::Cow;
use std::sync::Arc;
use std::{net::IpAddr, result::Result as StdResult, str};
use uuid::Uuid;

#[derive(Debug)]
pub struct SetKeyspace {
    pub keyspace_name: String,
}

#[derive(Debug)]
pub struct Prepared {
    pub id: Bytes,
    pub prepared_metadata: PreparedMetadata,
    pub result_metadata: ResultMetadata<'static>,
}

#[derive(Debug)]
pub struct SchemaChange {
    pub event: SchemaChangeEvent,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct TableSpec<'a> {
    ks_name: Cow<'a, str>,
    table_name: Cow<'a, str>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ColumnType<'frame> {
    Custom(Cow<'frame, str>),
    Ascii,
    Boolean,
    Blob,
    Counter,
    Date,
    Decimal,
    Double,
    Duration,
    Float,
    Int,
    BigInt,
    Text,
    Timestamp,
    Inet,
    List(Box<ColumnType<'frame>>),
    Map(Box<ColumnType<'frame>>, Box<ColumnType<'frame>>),
    Set(Box<ColumnType<'frame>>),
    UserDefinedType {
        type_name: Cow<'frame, str>,
        keyspace: Cow<'frame, str>,
        field_types: Vec<(Cow<'frame, str>, ColumnType<'frame>)>,
    },
    SmallInt,
    TinyInt,
    Time,
    Timeuuid,
    Tuple(Vec<ColumnType<'frame>>),
    Uuid,
    Varint,
}

impl<'frame> ColumnType<'frame> {
    pub fn into_owned(self) -> ColumnType<'static> {
        match self {
            ColumnType::Custom(cow) => ColumnType::Custom(cow.into_owned().into()),
            ColumnType::Ascii => ColumnType::Ascii,
            ColumnType::Boolean => ColumnType::Boolean,
            ColumnType::Blob => ColumnType::Blob,
            ColumnType::Counter => ColumnType::Counter,
            ColumnType::Date => ColumnType::Date,
            ColumnType::Decimal => ColumnType::Decimal,
            ColumnType::Double => ColumnType::Double,
            ColumnType::Duration => ColumnType::Duration,
            ColumnType::Float => ColumnType::Float,
            ColumnType::Int => ColumnType::Int,
            ColumnType::BigInt => ColumnType::BigInt,
            ColumnType::Text => ColumnType::Text,
            ColumnType::Timestamp => ColumnType::Timestamp,
            ColumnType::Inet => ColumnType::Inet,
            ColumnType::List(elem_type) => ColumnType::List(Box::new(elem_type.into_owned())),
            ColumnType::Map(key_type, value_type) => ColumnType::Map(
                Box::new(key_type.into_owned()),
                Box::new(value_type.into_owned()),
            ),
            ColumnType::Set(elem_type) => ColumnType::Set(Box::new(elem_type.into_owned())),
            ColumnType::UserDefinedType {
                type_name,
                keyspace,
                field_types,
            } => ColumnType::UserDefinedType {
                type_name: type_name.into_owned().into(),
                keyspace: keyspace.into_owned().into(),
                field_types: field_types
                    .into_iter()
                    .map(|(cow, column_type)| (cow.into_owned().into(), column_type.into_owned()))
                    .collect(),
            },
            ColumnType::SmallInt => ColumnType::SmallInt,
            ColumnType::TinyInt => ColumnType::TinyInt,
            ColumnType::Time => ColumnType::Time,
            ColumnType::Timeuuid => ColumnType::Timeuuid,
            ColumnType::Tuple(vec) => {
                ColumnType::Tuple(vec.into_iter().map(ColumnType::into_owned).collect())
            }
            ColumnType::Uuid => ColumnType::Uuid,
            ColumnType::Varint => ColumnType::Varint,
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub enum CqlValue {
    Ascii(String),
    Boolean(bool),
    Blob(Vec<u8>),
    Counter(Counter),
    Decimal(CqlDecimal),
    /// Days since -5877641-06-23 i.e. 2^31 days before unix epoch
    /// Can be converted to chrono::NaiveDate (-262145-1-1 to 262143-12-31) using as_date
    Date(CqlDate),
    Double(f64),
    Duration(CqlDuration),
    Empty,
    Float(f32),
    Int(i32),
    BigInt(i64),
    Text(String),
    /// Milliseconds since unix epoch
    Timestamp(CqlTimestamp),
    Inet(IpAddr),
    List(Vec<CqlValue>),
    Map(Vec<(CqlValue, CqlValue)>),
    Set(Vec<CqlValue>),
    UserDefinedType {
        keyspace: String,
        type_name: String,
        /// Order of `fields` vector must match the order of fields as defined in the UDT. The
        /// driver does not check it by itself, so incorrect data will be written if the order is
        /// wrong.
        fields: Vec<(String, Option<CqlValue>)>,
    },
    SmallInt(i16),
    TinyInt(i8),
    /// Nanoseconds since midnight
    Time(CqlTime),
    Timeuuid(CqlTimeuuid),
    Tuple(Vec<Option<CqlValue>>),
    Uuid(Uuid),
    Varint(CqlVarint),
}

impl<'a> TableSpec<'a> {
    pub const fn borrowed(ks: &'a str, table: &'a str) -> Self {
        Self {
            ks_name: Cow::Borrowed(ks),
            table_name: Cow::Borrowed(table),
        }
    }

    pub fn ks_name(&'a self) -> &'a str {
        self.ks_name.as_ref()
    }

    pub fn table_name(&'a self) -> &'a str {
        self.table_name.as_ref()
    }

    pub fn into_owned(self) -> TableSpec<'static> {
        TableSpec::owned(self.ks_name.into_owned(), self.table_name.into_owned())
    }

    pub fn to_owned(&self) -> TableSpec<'static> {
        TableSpec::owned(self.ks_name().to_owned(), self.table_name().to_owned())
    }
}

impl TableSpec<'static> {
    pub fn owned(ks_name: String, table_name: String) -> Self {
        Self {
            ks_name: Cow::Owned(ks_name),
            table_name: Cow::Owned(table_name),
        }
    }
}

impl ColumnType<'_> {
    // Returns true if the type allows a special, empty value in addition to its
    // natural representation. For example, bigint represents a 32-bit integer,
    // but it can also hold a 0-bit empty value.
    //
    // It looks like Cassandra 4.1.3 rejects empty values for some more types than
    // Scylla: date, time, smallint and tinyint. We will only check against
    // Scylla's set of types supported for empty values as it's smaller;
    // with Cassandra, some rejects will just have to be rejected on the db side.
    pub(crate) fn supports_special_empty_value(&self) -> bool {
        #[allow(clippy::match_like_matches_macro)]
        match self {
            ColumnType::Counter
            | ColumnType::Duration
            | ColumnType::List(_)
            | ColumnType::Map(_, _)
            | ColumnType::Set(_)
            | ColumnType::UserDefinedType { .. }
            | ColumnType::Custom(_) => false,

            _ => true,
        }
    }
}

impl CqlValue {
    pub fn as_ascii(&self) -> Option<&String> {
        match self {
            Self::Ascii(s) => Some(s),
            _ => None,
        }
    }

    pub fn as_cql_date(&self) -> Option<CqlDate> {
        match self {
            Self::Date(d) => Some(*d),
            _ => None,
        }
    }

    #[cfg(test)]
    #[cfg(feature = "chrono-04")]
    fn as_naive_date_04(&self) -> Option<chrono_04::NaiveDate> {
        self.as_cql_date().and_then(|date| date.try_into().ok())
    }

    #[cfg(test)]
    #[cfg(feature = "time-03")]
    fn as_date_03(&self) -> Option<time_03::Date> {
        self.as_cql_date().and_then(|date| date.try_into().ok())
    }

    pub fn as_cql_timestamp(&self) -> Option<CqlTimestamp> {
        match self {
            Self::Timestamp(i) => Some(*i),
            _ => None,
        }
    }

    #[cfg(test)]
    #[cfg(feature = "chrono-04")]
    fn as_datetime_04(&self) -> Option<chrono_04::DateTime<chrono_04::Utc>> {
        self.as_cql_timestamp().and_then(|ts| ts.try_into().ok())
    }

    #[cfg(test)]
    #[cfg(feature = "time-03")]
    fn as_offset_date_time_03(&self) -> Option<time_03::OffsetDateTime> {
        self.as_cql_timestamp().and_then(|ts| ts.try_into().ok())
    }

    pub fn as_cql_time(&self) -> Option<CqlTime> {
        match self {
            Self::Time(i) => Some(*i),
            _ => None,
        }
    }

    #[cfg(test)]
    #[cfg(feature = "chrono-04")]
    fn as_naive_time_04(&self) -> Option<chrono_04::NaiveTime> {
        self.as_cql_time().and_then(|ts| ts.try_into().ok())
    }

    #[cfg(test)]
    #[cfg(feature = "time-03")]
    fn as_time_03(&self) -> Option<time_03::Time> {
        self.as_cql_time().and_then(|ts| ts.try_into().ok())
    }

    pub fn as_cql_duration(&self) -> Option<CqlDuration> {
        match self {
            Self::Duration(i) => Some(*i),
            _ => None,
        }
    }

    pub fn as_counter(&self) -> Option<Counter> {
        match self {
            Self::Counter(i) => Some(*i),
            _ => None,
        }
    }

    pub fn as_boolean(&self) -> Option<bool> {
        match self {
            Self::Boolean(i) => Some(*i),
            _ => None,
        }
    }

    pub fn as_double(&self) -> Option<f64> {
        match self {
            Self::Double(d) => Some(*d),
            _ => None,
        }
    }

    pub fn as_uuid(&self) -> Option<Uuid> {
        match self {
            Self::Uuid(u) => Some(*u),
            _ => None,
        }
    }

    pub fn as_float(&self) -> Option<f32> {
        match self {
            Self::Float(f) => Some(*f),
            _ => None,
        }
    }

    pub fn as_int(&self) -> Option<i32> {
        match self {
            Self::Int(i) => Some(*i),
            _ => None,
        }
    }

    pub fn as_bigint(&self) -> Option<i64> {
        match self {
            Self::BigInt(i) => Some(*i),
            _ => None,
        }
    }

    pub fn as_tinyint(&self) -> Option<i8> {
        match self {
            Self::TinyInt(i) => Some(*i),
            _ => None,
        }
    }

    pub fn as_smallint(&self) -> Option<i16> {
        match self {
            Self::SmallInt(i) => Some(*i),
            _ => None,
        }
    }

    pub fn as_blob(&self) -> Option<&Vec<u8>> {
        match self {
            Self::Blob(v) => Some(v),
            _ => None,
        }
    }

    pub fn as_text(&self) -> Option<&String> {
        match self {
            Self::Text(s) => Some(s),
            _ => None,
        }
    }

    pub fn as_timeuuid(&self) -> Option<CqlTimeuuid> {
        match self {
            Self::Timeuuid(u) => Some(*u),
            _ => None,
        }
    }

    pub fn into_string(self) -> Option<String> {
        match self {
            Self::Ascii(s) => Some(s),
            Self::Text(s) => Some(s),
            _ => None,
        }
    }

    pub fn into_blob(self) -> Option<Vec<u8>> {
        match self {
            Self::Blob(b) => Some(b),
            _ => None,
        }
    }

    pub fn as_inet(&self) -> Option<IpAddr> {
        match self {
            Self::Inet(a) => Some(*a),
            _ => None,
        }
    }

    pub fn as_list(&self) -> Option<&Vec<CqlValue>> {
        match self {
            Self::List(s) => Some(s),
            _ => None,
        }
    }

    pub fn as_set(&self) -> Option<&Vec<CqlValue>> {
        match self {
            Self::Set(s) => Some(s),
            _ => None,
        }
    }

    pub fn as_map(&self) -> Option<&Vec<(CqlValue, CqlValue)>> {
        match self {
            Self::Map(s) => Some(s),
            _ => None,
        }
    }

    pub fn as_udt(&self) -> Option<&Vec<(String, Option<CqlValue>)>> {
        match self {
            Self::UserDefinedType { fields, .. } => Some(fields),
            _ => None,
        }
    }

    pub fn into_vec(self) -> Option<Vec<CqlValue>> {
        match self {
            Self::List(s) => Some(s),
            Self::Set(s) => Some(s),
            _ => None,
        }
    }

    pub fn into_pair_vec(self) -> Option<Vec<(CqlValue, CqlValue)>> {
        match self {
            Self::Map(s) => Some(s),
            _ => None,
        }
    }

    pub fn into_udt_pair_vec(self) -> Option<Vec<(String, Option<CqlValue>)>> {
        match self {
            Self::UserDefinedType { fields, .. } => Some(fields),
            _ => None,
        }
    }

    pub fn into_cql_varint(self) -> Option<CqlVarint> {
        match self {
            Self::Varint(i) => Some(i),
            _ => None,
        }
    }

    pub fn into_cql_decimal(self) -> Option<CqlDecimal> {
        match self {
            Self::Decimal(i) => Some(i),
            _ => None,
        }
    }
    // TODO
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnSpec<'frame> {
    pub(crate) table_spec: TableSpec<'frame>,
    pub(crate) name: Cow<'frame, str>,
    pub(crate) typ: ColumnType<'frame>,
}

impl ColumnSpec<'static> {
    #[inline]
    pub fn owned(name: String, typ: ColumnType<'static>, table_spec: TableSpec<'static>) -> Self {
        Self {
            table_spec,
            name: Cow::Owned(name),
            typ,
        }
    }
}

impl<'frame> ColumnSpec<'frame> {
    #[inline]
    pub fn borrowed(
        name: &'frame str,
        typ: ColumnType<'frame>,
        table_spec: TableSpec<'frame>,
    ) -> Self {
        Self {
            table_spec,
            name: Cow::Borrowed(name),
            typ,
        }
    }

    #[inline]
    pub fn table_spec(&self) -> &TableSpec<'frame> {
        &self.table_spec
    }

    #[inline]
    pub fn name(&self) -> &str {
        &self.name
    }

    #[inline]
    pub fn typ(&self) -> &ColumnType<'frame> {
        &self.typ
    }
}

#[derive(Debug, Clone)]
pub struct ResultMetadata<'a> {
    col_count: usize,
    col_specs: Vec<ColumnSpec<'a>>,
}

impl<'a> ResultMetadata<'a> {
    #[inline]
    pub fn mock_empty() -> Self {
        Self {
            col_count: 0,
            col_specs: Vec::new(),
        }
    }

    #[inline]
    #[doc(hidden)]
    pub fn new_for_test(col_count: usize, col_specs: Vec<ColumnSpec<'static>>) -> Self {
        Self {
            col_count,
            col_specs,
        }
    }

    #[inline]
    pub fn col_count(&self) -> usize {
        self.col_count
    }

    #[inline]
    pub fn col_specs(&self) -> &[ColumnSpec<'a>] {
        &self.col_specs
    }
}

#[derive(Debug, Copy, Clone)]
pub struct PartitionKeyIndex {
    /// index in the serialized values
    pub index: u16,
    /// sequence number in partition key
    pub sequence: u16,
}

#[derive(Debug, Clone)]
pub struct PreparedMetadata {
    pub flags: i32,
    pub col_count: usize,
    /// pk_indexes are sorted by `index` and can be reordered in partition key order
    /// using `sequence` field
    pub pk_indexes: Vec<PartitionKeyIndex>,
    pub col_specs: Vec<ColumnSpec<'static>>,
}

#[derive(Debug, Default, PartialEq)]
pub struct Row {
    pub columns: Vec<Option<CqlValue>>,
}

impl Row {
    /// Allows converting Row into tuple of rust types or custom struct deriving FromRow
    pub fn into_typed<RowT: FromRow>(self) -> StdResult<RowT, FromRowError> {
        RowT::from_row(self)
    }
}

#[derive(Debug)]
pub struct Rows {
    pub metadata: Arc<ResultMetadata<'static>>,
    pub paging_state_response: PagingStateResponse,
    pub rows_count: usize,
    pub rows: Vec<Row>,
    /// Original size of the serialized rows.
    pub serialized_size: usize,
}

#[derive(Debug)]
pub enum Result {
    Void,
    Rows(Rows),
    SetKeyspace(SetKeyspace),
    Prepared(Prepared),
    SchemaChange(SchemaChange),
}

fn deser_type_generic<'frame, 'result, StrT: Into<Cow<'result, str>>>(
    buf: &mut &'frame [u8],
    read_string: fn(&mut &'frame [u8]) -> StdResult<StrT, LowLevelDeserializationError>,
) -> StdResult<ColumnType<'result>, CqlTypeParseError> {
    use ColumnType::*;
    let id =
        types::read_short(buf).map_err(|err| CqlTypeParseError::TypeIdParseError(err.into()))?;
    Ok(match id {
        0x0000 => {
            // We use types::read_string instead of read_string argument here on purpose.
            // Chances are the underlying string is `...DurationType`, in which case
            // we don't need to allocate it at all. Only for Custom types
            // (which we don't support anyway) do we need to allocate.
            // OTOH, the macro argument function deserializes borrowed OR owned string;
            // here we want to always deserialize borrowed string.
            let type_str =
                types::read_string(buf).map_err(CqlTypeParseError::CustomTypeNameParseError)?;
            match type_str {
                "org.apache.cassandra.db.marshal.DurationType" => Duration,
                _ => Custom(type_str.to_owned().into()),
            }
        }
        0x0001 => Ascii,
        0x0002 => BigInt,
        0x0003 => Blob,
        0x0004 => Boolean,
        0x0005 => Counter,
        0x0006 => Decimal,
        0x0007 => Double,
        0x0008 => Float,
        0x0009 => Int,
        0x000B => Timestamp,
        0x000C => Uuid,
        0x000D => Text,
        0x000E => Varint,
        0x000F => Timeuuid,
        0x0010 => Inet,
        0x0011 => Date,
        0x0012 => Time,
        0x0013 => SmallInt,
        0x0014 => TinyInt,
        0x0015 => Duration,
        0x0020 => List(Box::new(deser_type_generic(buf, read_string)?)),
        0x0021 => Map(
            Box::new(deser_type_generic(buf, read_string)?),
            Box::new(deser_type_generic(buf, read_string)?),
        ),
        0x0022 => Set(Box::new(deser_type_generic(buf, read_string)?)),
        0x0030 => {
            let keyspace_name =
                read_string(buf).map_err(CqlTypeParseError::UdtKeyspaceNameParseError)?;
            let type_name = read_string(buf).map_err(CqlTypeParseError::UdtNameParseError)?;
            let fields_size: usize = types::read_short(buf)
                .map_err(|err| CqlTypeParseError::UdtFieldsCountParseError(err.into()))?
                .into();

            let mut field_types: Vec<(Cow<'result, str>, ColumnType)> =
                Vec::with_capacity(fields_size);

            for _ in 0..fields_size {
                let field_name =
                    read_string(buf).map_err(CqlTypeParseError::UdtFieldNameParseError)?;
                let field_type = deser_type_generic(buf, read_string)?;

                field_types.push((field_name.into(), field_type));
            }

            UserDefinedType {
                type_name: type_name.into(),
                keyspace: keyspace_name.into(),
                field_types,
            }
        }
        0x0031 => {
            let len: usize = types::read_short(buf)
                .map_err(|err| CqlTypeParseError::TupleLengthParseError(err.into()))?
                .into();
            let mut types = Vec::with_capacity(len);
            for _ in 0..len {
                types.push(deser_type_generic(buf, read_string)?);
            }
            Tuple(types)
        }
        id => {
            return Err(CqlTypeParseError::TypeNotImplemented(id));
        }
    })
}

fn _deser_type_borrowed<'frame>(
    buf: &mut &'frame [u8],
) -> StdResult<ColumnType<'frame>, CqlTypeParseError> {
    deser_type_generic(buf, |buf| types::read_string(buf))
}

fn deser_type_owned(buf: &mut &[u8]) -> StdResult<ColumnType<'static>, CqlTypeParseError> {
    deser_type_generic(buf, |buf| types::read_string(buf).map(ToOwned::to_owned))
}

/// Deserializes a table spec, be it per-column one or a global one,
/// in the borrowed form.
///
/// This function does not allocate.
/// To obtain TableSpec<'static>, use `.into_owned()` on its result.
fn deser_table_spec<'frame>(
    buf: &mut &'frame [u8],
) -> StdResult<TableSpec<'frame>, TableSpecParseError> {
    let ks_name = types::read_string(buf).map_err(TableSpecParseError::MalformedKeyspaceName)?;
    let table_name = types::read_string(buf).map_err(TableSpecParseError::MalformedTableName)?;
    Ok(TableSpec::borrowed(ks_name, table_name))
}

fn mk_col_spec_parse_error(
    col_idx: usize,
    err: impl Into<ColumnSpecParseErrorKind>,
) -> ColumnSpecParseError {
    ColumnSpecParseError {
        column_index: col_idx,
        kind: err.into(),
    }
}

/// Deserializes table spec of a column spec in the borrowed form.
///
/// Checks for equality of table specs across columns, because the protocol
/// does not guarantee that and we want to be sure that the assumption
/// of them being all the same is correct.
/// To this end, the first column's table spec is written to `known_table_spec`
/// and compared with remaining columns' table spec.
///
/// To avoid needless allocations, it is advised to pass `known_table_spec`
/// in the borrowed form, so that cloning it is cheap.
fn deser_table_spec_for_col_spec<'frame>(
    buf: &'_ mut &'frame [u8],
    global_table_spec_provided: bool,
    known_table_spec: &'_ mut Option<TableSpec<'frame>>,
    col_idx: usize,
) -> StdResult<TableSpec<'frame>, ColumnSpecParseError> {
    let table_spec = match known_table_spec {
        // If global table spec was provided, we simply clone it to each column spec.
        Some(ref known_spec) if global_table_spec_provided => known_spec.clone(),

        // Else, we deserialize the table spec for a column and, if we already know some
        // previous spec (i.e. that of the first column), we perform equality check
        // against it.
        Some(_) | None => {
            let table_spec =
                deser_table_spec(buf).map_err(|err| mk_col_spec_parse_error(col_idx, err))?;

            if let Some(ref known_spec) = known_table_spec {
                // We assume that for each column, table spec is the same.
                // As this is not guaranteed by the CQL protocol specification but only by how
                // Cassandra and ScyllaDB work (no support for joins), we perform a sanity check here.
                if known_spec.table_name != table_spec.table_name
                    || known_spec.ks_name != table_spec.ks_name
                {
                    return Err(mk_col_spec_parse_error(
                        col_idx,
                        ColumnSpecParseErrorKind::TableSpecDiffersAcrossColumns(
                            known_spec.clone().into_owned(),
                            table_spec.into_owned(),
                        ),
                    ));
                }
            } else {
                // Once we have read the first column spec, we save its table spec
                // in order to verify its equality with other columns'.
                *known_table_spec = Some(table_spec.clone());
            }

            table_spec
        }
    };

    Ok(table_spec)
}

/// Deserializes col specs (part of ResultMetadata or PreparedMetadata)
/// in the form mentioned by its name.
///
/// Checks for equality of table specs across columns, because the protocol
/// does not guarantee that and we want to be sure that the assumption
/// of them being all the same is correct.
///
/// To avoid needless allocations, it is advised to pass `global_table_spec`
/// in the borrowed form, so that cloning it is cheap.
fn deser_col_specs_generic<'frame, 'result>(
    buf: &mut &'frame [u8],
    global_table_spec: Option<TableSpec<'frame>>,
    col_count: usize,
    make_col_spec: fn(&'frame str, ColumnType<'result>, TableSpec<'frame>) -> ColumnSpec<'result>,
    deser_type: fn(&mut &'frame [u8]) -> StdResult<ColumnType<'result>, CqlTypeParseError>,
) -> StdResult<Vec<ColumnSpec<'result>>, ColumnSpecParseError> {
    let global_table_spec_provided = global_table_spec.is_some();
    let mut known_table_spec = global_table_spec;

    let mut col_specs = Vec::with_capacity(col_count);
    for col_idx in 0..col_count {
        let table_spec = deser_table_spec_for_col_spec(
            buf,
            global_table_spec_provided,
            &mut known_table_spec,
            col_idx,
        )?;

        let name = types::read_string(buf).map_err(|err| mk_col_spec_parse_error(col_idx, err))?;
        let typ = deser_type(buf).map_err(|err| mk_col_spec_parse_error(col_idx, err))?;
        let col_spec = make_col_spec(name, typ, table_spec);
        col_specs.push(col_spec);
    }
    Ok(col_specs)
}

fn _deser_col_specs_borrowed<'frame>(
    buf: &mut &'frame [u8],
    global_table_spec: Option<TableSpec<'frame>>,
    col_count: usize,
) -> StdResult<Vec<ColumnSpec<'frame>>, ColumnSpecParseError> {
    deser_col_specs_generic(
        buf,
        global_table_spec,
        col_count,
        ColumnSpec::borrowed,
        _deser_type_borrowed,
    )
}

fn deser_col_specs_owned<'frame>(
    buf: &mut &'frame [u8],
    global_table_spec: Option<TableSpec<'frame>>,
    col_count: usize,
) -> StdResult<Vec<ColumnSpec<'static>>, ColumnSpecParseError> {
    let result: StdResult<Vec<ColumnSpec<'static>>, ColumnSpecParseError> = deser_col_specs_generic(
        buf,
        global_table_spec,
        col_count,
        |name: &str, typ, table_spec: TableSpec| {
            ColumnSpec::owned(name.to_owned(), typ, table_spec.into_owned())
        },
        deser_type_owned,
    );

    result
}

fn deser_result_metadata(
    buf: &mut &[u8],
) -> StdResult<(ResultMetadata<'static>, PagingStateResponse), ResultMetadataParseError> {
    let flags = types::read_int(buf)
        .map_err(|err| ResultMetadataParseError::FlagsParseError(err.into()))?;
    let global_tables_spec = flags & 0x0001 != 0;
    let has_more_pages = flags & 0x0002 != 0;
    let no_metadata = flags & 0x0004 != 0;

    let col_count =
        types::read_int_length(buf).map_err(ResultMetadataParseError::ColumnCountParseError)?;

    let raw_paging_state = has_more_pages
        .then(|| types::read_bytes(buf).map_err(ResultMetadataParseError::PagingStateParseError))
        .transpose()?;

    let paging_state = PagingStateResponse::new_from_raw_bytes(raw_paging_state);

    let col_specs = if no_metadata {
        vec![]
    } else {
        let global_table_spec = global_tables_spec
            .then(|| deser_table_spec(buf))
            .transpose()?;

        deser_col_specs_owned(buf, global_table_spec, col_count)?
    };

    let metadata = ResultMetadata {
        col_count,
        col_specs,
    };
    Ok((metadata, paging_state))
}

fn deser_prepared_metadata(
    buf: &mut &[u8],
) -> StdResult<PreparedMetadata, ResultMetadataParseError> {
    let flags = types::read_int(buf)
        .map_err(|err| ResultMetadataParseError::FlagsParseError(err.into()))?;
    let global_tables_spec = flags & 0x0001 != 0;

    let col_count =
        types::read_int_length(buf).map_err(ResultMetadataParseError::ColumnCountParseError)?;

    let pk_count: usize =
        types::read_int_length(buf).map_err(ResultMetadataParseError::PkCountParseError)?;

    let mut pk_indexes = Vec::with_capacity(pk_count);
    for i in 0..pk_count {
        pk_indexes.push(PartitionKeyIndex {
            index: types::read_short(buf)
                .map_err(|err| ResultMetadataParseError::PkIndexParseError(err.into()))?
                as u16,
            sequence: i as u16,
        });
    }
    pk_indexes.sort_unstable_by_key(|pki| pki.index);

    let global_table_spec = global_tables_spec
        .then(|| deser_table_spec(buf))
        .transpose()?;

    let col_specs = deser_col_specs_owned(buf, global_table_spec, col_count)?;

    Ok(PreparedMetadata {
        flags,
        col_count,
        pk_indexes,
        col_specs,
    })
}

pub fn deser_cql_value(
    typ: &ColumnType,
    buf: &mut &[u8],
) -> StdResult<CqlValue, DeserializationError> {
    use ColumnType::*;

    if buf.is_empty() {
        match typ {
            Ascii | Blob | Text => {
                // can't be empty
            }
            _ => return Ok(CqlValue::Empty),
        }
    }
    // The `new_borrowed` version of FrameSlice is deficient in that it does not hold
    // a `Bytes` reference to the frame, only a slice.
    // This is not a problem here, fortunately, because none of CqlValue variants contain
    // any `Bytes` - only exclusively owned types - so we never call FrameSlice::to_bytes().
    let v = Some(FrameSlice::new_borrowed(buf));

    Ok(match typ {
        Custom(type_str) => {
            return Err(mk_deser_err::<CqlValue>(
                typ,
                BuiltinDeserializationErrorKind::CustomTypeNotSupported(type_str.to_string()),
            ))
        }
        Ascii => {
            let s = String::deserialize(typ, v)?;
            CqlValue::Ascii(s)
        }
        Boolean => {
            let b = bool::deserialize(typ, v)?;
            CqlValue::Boolean(b)
        }
        Blob => {
            let b = Vec::<u8>::deserialize(typ, v)?;
            CqlValue::Blob(b)
        }
        Date => {
            let d = CqlDate::deserialize(typ, v)?;
            CqlValue::Date(d)
        }
        Counter => {
            let c = crate::frame::response::result::Counter::deserialize(typ, v)?;
            CqlValue::Counter(c)
        }
        Decimal => {
            let d = CqlDecimal::deserialize(typ, v)?;
            CqlValue::Decimal(d)
        }
        Double => {
            let d = f64::deserialize(typ, v)?;
            CqlValue::Double(d)
        }
        Float => {
            let f = f32::deserialize(typ, v)?;
            CqlValue::Float(f)
        }
        Int => {
            let i = i32::deserialize(typ, v)?;
            CqlValue::Int(i)
        }
        SmallInt => {
            let si = i16::deserialize(typ, v)?;
            CqlValue::SmallInt(si)
        }
        TinyInt => {
            let ti = i8::deserialize(typ, v)?;
            CqlValue::TinyInt(ti)
        }
        BigInt => {
            let bi = i64::deserialize(typ, v)?;
            CqlValue::BigInt(bi)
        }
        Text => {
            let s = String::deserialize(typ, v)?;
            CqlValue::Text(s)
        }
        Timestamp => {
            let t = CqlTimestamp::deserialize(typ, v)?;
            CqlValue::Timestamp(t)
        }
        Time => {
            let t = CqlTime::deserialize(typ, v)?;
            CqlValue::Time(t)
        }
        Timeuuid => {
            let t = CqlTimeuuid::deserialize(typ, v)?;
            CqlValue::Timeuuid(t)
        }
        Duration => {
            let d = CqlDuration::deserialize(typ, v)?;
            CqlValue::Duration(d)
        }
        Inet => {
            let i = IpAddr::deserialize(typ, v)?;
            CqlValue::Inet(i)
        }
        Uuid => {
            let uuid = uuid::Uuid::deserialize(typ, v)?;
            CqlValue::Uuid(uuid)
        }
        Varint => {
            let vi = CqlVarint::deserialize(typ, v)?;
            CqlValue::Varint(vi)
        }
        List(_type_name) => {
            let l = Vec::<CqlValue>::deserialize(typ, v)?;
            CqlValue::List(l)
        }
        Map(_key_type, _value_type) => {
            let iter = MapIterator::<'_, '_, CqlValue, CqlValue>::deserialize(typ, v)?;
            let m: Vec<(CqlValue, CqlValue)> = iter.collect::<StdResult<_, _>>()?;
            CqlValue::Map(m)
        }
        Set(_type_name) => {
            let s = Vec::<CqlValue>::deserialize(typ, v)?;
            CqlValue::Set(s)
        }
        UserDefinedType {
            type_name,
            keyspace,
            ..
        } => {
            let iter = UdtIterator::deserialize(typ, v)?;
            let fields: Vec<(String, Option<CqlValue>)> = iter
                .map(|((col_name, col_type), res)| {
                    res.and_then(|v| {
                        let val = Option::<CqlValue>::deserialize(col_type, v.flatten())?;
                        Ok((col_name.clone().into_owned(), val))
                    })
                })
                .collect::<StdResult<_, _>>()?;

            CqlValue::UserDefinedType {
                keyspace: keyspace.clone().into_owned(),
                type_name: type_name.clone().into_owned(),
                fields,
            }
        }
        Tuple(type_names) => {
            let t = type_names
                .iter()
                .map(|typ| -> StdResult<_, DeserializationError> {
                    let raw = types::read_bytes_opt(buf).map_err(|e| {
                        mk_deser_err::<CqlValue>(
                            typ,
                            BuiltinDeserializationErrorKind::RawCqlBytesReadError(e),
                        )
                    })?;
                    raw.map(|v| CqlValue::deserialize(typ, Some(FrameSlice::new_borrowed(v))))
                        .transpose()
                })
                .collect::<StdResult<_, _>>()?;
            CqlValue::Tuple(t)
        }
    })
}

fn deser_rows(
    buf_bytes: Bytes,
    cached_metadata: Option<&Arc<ResultMetadata<'static>>>,
) -> StdResult<Rows, RowsParseError> {
    let buf = &mut &*buf_bytes;
    let (server_metadata, paging_state_response) = deser_result_metadata(buf)?;

    let metadata = match cached_metadata {
        Some(cached) => Arc::clone(cached),
        None => {
            // No cached_metadata provided. Server is supposed to provide the result metadata.
            if server_metadata.col_count != server_metadata.col_specs.len() {
                return Err(RowsParseError::ColumnCountMismatch {
                    col_count: server_metadata.col_count,
                    col_specs_count: server_metadata.col_specs.len(),
                });
            }
            Arc::new(server_metadata)
        }
    };

    let original_size = buf.len();

    let rows_count: usize =
        types::read_int_length(buf).map_err(RowsParseError::RowsCountParseError)?;

    let raw_rows_iter = RowIterator::new(
        rows_count,
        &metadata.col_specs,
        FrameSlice::new_borrowed(buf),
    );
    let rows_iter = TypedRowIterator::<Row>::new(raw_rows_iter)
        .map_err(|err| DeserializationError::new(err.0))?;

    let rows = rows_iter.collect::<StdResult<_, _>>()?;

    Ok(Rows {
        metadata,
        paging_state_response,
        rows_count,
        rows,
        serialized_size: original_size - buf.len(),
    })
}

fn deser_set_keyspace(buf: &mut &[u8]) -> StdResult<SetKeyspace, SetKeyspaceParseError> {
    let keyspace_name = types::read_string(buf)?.to_string();

    Ok(SetKeyspace { keyspace_name })
}

fn deser_prepared(buf: &mut &[u8]) -> StdResult<Prepared, PreparedParseError> {
    let id_len = types::read_short(buf)
        .map_err(|err| PreparedParseError::IdLengthParseError(err.into()))?
        as usize;
    let id: Bytes = buf[0..id_len].to_owned().into();
    buf.advance(id_len);
    let prepared_metadata =
        deser_prepared_metadata(buf).map_err(PreparedParseError::PreparedMetadataParseError)?;
    let (result_metadata, paging_state_response) =
        deser_result_metadata(buf).map_err(PreparedParseError::ResultMetadataParseError)?;
    if let PagingStateResponse::HasMorePages { state } = paging_state_response {
        return Err(PreparedParseError::NonZeroPagingState(
            state
                .as_bytes_slice()
                .cloned()
                .unwrap_or_else(|| Arc::from([])),
        ));
    }

    Ok(Prepared {
        id,
        prepared_metadata,
        result_metadata,
    })
}

fn deser_schema_change(buf: &mut &[u8]) -> StdResult<SchemaChange, SchemaChangeEventParseError> {
    Ok(SchemaChange {
        event: SchemaChangeEvent::deserialize(buf)?,
    })
}

pub fn deserialize(
    buf_bytes: Bytes,
    cached_metadata: Option<&Arc<ResultMetadata<'static>>>,
) -> StdResult<Result, CqlResultParseError> {
    let buf = &mut &*buf_bytes;
    use self::Result::*;
    Ok(
        match types::read_int(buf)
            .map_err(|err| CqlResultParseError::ResultIdParseError(err.into()))?
        {
            0x0001 => Void,
            0x0002 => Rows(deser_rows(buf_bytes.slice_ref(buf), cached_metadata)?),
            0x0003 => SetKeyspace(deser_set_keyspace(buf)?),
            0x0004 => Prepared(deser_prepared(buf)?),
            0x0005 => SchemaChange(deser_schema_change(buf)?),
            id => return Err(CqlResultParseError::UnknownResultId(id)),
        },
    )
}

#[cfg(test)]
mod tests {
    use crate as scylla;
    use crate::frame::value::{Counter, CqlDate, CqlDuration, CqlTime, CqlTimestamp, CqlTimeuuid};
    use scylla::frame::response::result::{ColumnType, CqlValue};
    use std::str::FromStr;
    use uuid::Uuid;

    #[test]
    fn test_deserialize_text_types() {
        let buf: Vec<u8> = vec![0x41];
        let int_slice = &mut &buf[..];
        let ascii_serialized = super::deser_cql_value(&ColumnType::Ascii, int_slice).unwrap();
        let text_serialized = super::deser_cql_value(&ColumnType::Text, int_slice).unwrap();
        assert_eq!(ascii_serialized, CqlValue::Ascii("A".to_string()));
        assert_eq!(text_serialized, CqlValue::Text("A".to_string()));
    }

    #[test]
    fn test_deserialize_uuid_inet_types() {
        let my_uuid = Uuid::parse_str("00000000000000000000000000000001").unwrap();

        let uuid_buf: Vec<u8> = vec![0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1];
        let uuid_slice = &mut &uuid_buf[..];
        let uuid_serialize = super::deser_cql_value(&ColumnType::Uuid, uuid_slice).unwrap();
        assert_eq!(uuid_serialize, CqlValue::Uuid(my_uuid));

        let my_timeuuid = CqlTimeuuid::from_str("00000000000000000000000000000001").unwrap();
        let time_uuid_serialize =
            super::deser_cql_value(&ColumnType::Timeuuid, uuid_slice).unwrap();
        assert_eq!(time_uuid_serialize, CqlValue::Timeuuid(my_timeuuid));

        let my_ip = "::1".parse().unwrap();
        let ip_buf: Vec<u8> = vec![0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1];
        let ip_slice = &mut &ip_buf[..];
        let ip_serialize = super::deser_cql_value(&ColumnType::Inet, ip_slice).unwrap();
        assert_eq!(ip_serialize, CqlValue::Inet(my_ip));

        let max_ip = "ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff".parse().unwrap();
        let max_ip_buf: Vec<u8> = vec![
            255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
        ];
        let max_ip_slice = &mut &max_ip_buf[..];
        let max_ip_serialize = super::deser_cql_value(&ColumnType::Inet, max_ip_slice).unwrap();
        assert_eq!(max_ip_serialize, CqlValue::Inet(max_ip));
    }

    #[test]
    fn test_floating_points() {
        let float: f32 = 0.5;
        let double: f64 = 2.0;

        let float_buf: Vec<u8> = vec![63, 0, 0, 0];
        let float_slice = &mut &float_buf[..];
        let float_serialize = super::deser_cql_value(&ColumnType::Float, float_slice).unwrap();
        assert_eq!(float_serialize, CqlValue::Float(float));

        let double_buf: Vec<u8> = vec![64, 0, 0, 0, 0, 0, 0, 0];
        let double_slice = &mut &double_buf[..];
        let double_serialize = super::deser_cql_value(&ColumnType::Double, double_slice).unwrap();
        assert_eq!(double_serialize, CqlValue::Double(double));
    }

    #[cfg(any(feature = "num-bigint-03", feature = "num-bigint-04"))]
    struct VarintTestCase {
        value: i32,
        encoding: Vec<u8>,
    }

    #[cfg(any(feature = "num-bigint-03", feature = "num-bigint-04"))]
    fn varint_test_cases_from_spec() -> Vec<VarintTestCase> {
        /*
            Table taken from CQL Binary Protocol v4 spec

            Value | Encoding
            ------|---------
                0 |     0x00
                1 |     0x01
              127 |     0x7F
              128 |   0x0080
              129 |   0x0081
               -1 |     0xFF
             -128 |     0x80
             -129 |   0xFF7F
        */
        vec![
            VarintTestCase {
                value: 0,
                encoding: vec![0x00],
            },
            VarintTestCase {
                value: 1,
                encoding: vec![0x01],
            },
            VarintTestCase {
                value: 127,
                encoding: vec![0x7F],
            },
            VarintTestCase {
                value: 128,
                encoding: vec![0x00, 0x80],
            },
            VarintTestCase {
                value: 129,
                encoding: vec![0x00, 0x81],
            },
            VarintTestCase {
                value: -1,
                encoding: vec![0xFF],
            },
            VarintTestCase {
                value: -128,
                encoding: vec![0x80],
            },
            VarintTestCase {
                value: -129,
                encoding: vec![0xFF, 0x7F],
            },
        ]
    }

    #[cfg(feature = "num-bigint-03")]
    #[test]
    fn test_bigint03() {
        use num_bigint_03::ToBigInt;

        let tests = varint_test_cases_from_spec();

        for t in tests.iter() {
            let value = super::deser_cql_value(&ColumnType::Varint, &mut &*t.encoding).unwrap();
            assert_eq!(CqlValue::Varint(t.value.to_bigint().unwrap().into()), value);
        }
    }

    #[cfg(feature = "num-bigint-04")]
    #[test]
    fn test_bigint04() {
        use num_bigint_04::ToBigInt;

        let tests = varint_test_cases_from_spec();

        for t in tests.iter() {
            let value = super::deser_cql_value(&ColumnType::Varint, &mut &*t.encoding).unwrap();
            assert_eq!(CqlValue::Varint(t.value.to_bigint().unwrap().into()), value);
        }
    }

    #[cfg(feature = "bigdecimal-04")]
    #[test]
    fn test_decimal() {
        use bigdecimal_04::BigDecimal;
        struct Test<'a> {
            value: BigDecimal,
            encoding: &'a [u8],
        }

        let tests = [
            Test {
                value: BigDecimal::from_str("-1.28").unwrap(),
                encoding: &[0x0, 0x0, 0x0, 0x2, 0x80],
            },
            Test {
                value: BigDecimal::from_str("1.29").unwrap(),
                encoding: &[0x0, 0x0, 0x0, 0x2, 0x0, 0x81],
            },
            Test {
                value: BigDecimal::from_str("0").unwrap(),
                encoding: &[0x0, 0x0, 0x0, 0x0, 0x0],
            },
            Test {
                value: BigDecimal::from_str("123").unwrap(),
                encoding: &[0x0, 0x0, 0x0, 0x0, 0x7b],
            },
        ];

        for t in tests.iter() {
            let value = super::deser_cql_value(&ColumnType::Decimal, &mut &*t.encoding).unwrap();
            assert_eq!(
                CqlValue::Decimal(t.value.clone().try_into().unwrap()),
                value
            );
        }
    }

    #[test]
    fn test_deserialize_counter() {
        let counter: Vec<u8> = vec![0, 0, 0, 0, 0, 0, 1, 0];
        let counter_slice = &mut &counter[..];
        let counter_serialize =
            super::deser_cql_value(&ColumnType::Counter, counter_slice).unwrap();
        assert_eq!(counter_serialize, CqlValue::Counter(Counter(256)));
    }

    #[test]
    fn test_deserialize_blob() {
        let blob: Vec<u8> = vec![0, 1, 2, 3];
        let blob_slice = &mut &blob[..];
        let blob_serialize = super::deser_cql_value(&ColumnType::Blob, blob_slice).unwrap();
        assert_eq!(blob_serialize, CqlValue::Blob(blob));
    }

    #[test]
    fn test_deserialize_bool() {
        let bool_buf: Vec<u8> = vec![0x00];
        let bool_slice = &mut &bool_buf[..];
        let bool_serialize = super::deser_cql_value(&ColumnType::Boolean, bool_slice).unwrap();
        assert_eq!(bool_serialize, CqlValue::Boolean(false));

        let bool_buf: Vec<u8> = vec![0x01];
        let bool_slice = &mut &bool_buf[..];
        let bool_serialize = super::deser_cql_value(&ColumnType::Boolean, bool_slice).unwrap();
        assert_eq!(bool_serialize, CqlValue::Boolean(true));
    }

    #[test]
    fn test_deserialize_int_types() {
        let int_buf: Vec<u8> = vec![0, 0, 0, 4];
        let int_slice = &mut &int_buf[..];
        let int_serialized = super::deser_cql_value(&ColumnType::Int, int_slice).unwrap();
        assert_eq!(int_serialized, CqlValue::Int(4));

        let smallint_buf: Vec<u8> = vec![0, 4];
        let smallint_slice = &mut &smallint_buf[..];
        let smallint_serialized =
            super::deser_cql_value(&ColumnType::SmallInt, smallint_slice).unwrap();
        assert_eq!(smallint_serialized, CqlValue::SmallInt(4));

        let tinyint_buf: Vec<u8> = vec![4];
        let tinyint_slice = &mut &tinyint_buf[..];
        let tinyint_serialized =
            super::deser_cql_value(&ColumnType::TinyInt, tinyint_slice).unwrap();
        assert_eq!(tinyint_serialized, CqlValue::TinyInt(4));

        let bigint_buf: Vec<u8> = vec![0, 0, 0, 0, 0, 0, 0, 4];
        let bigint_slice = &mut &bigint_buf[..];
        let bigint_serialized = super::deser_cql_value(&ColumnType::BigInt, bigint_slice).unwrap();
        assert_eq!(bigint_serialized, CqlValue::BigInt(4));
    }

    #[test]
    fn test_list_from_cql() {
        let my_vec: Vec<CqlValue> = vec![CqlValue::Int(20), CqlValue::Int(2), CqlValue::Int(13)];

        let cql: CqlValue = CqlValue::List(my_vec);
        let decoded = cql.into_vec().unwrap();

        assert_eq!(decoded[0], CqlValue::Int(20));
        assert_eq!(decoded[1], CqlValue::Int(2));
        assert_eq!(decoded[2], CqlValue::Int(13));
    }

    #[test]
    fn test_set_from_cql() {
        let my_vec: Vec<CqlValue> = vec![CqlValue::Int(20), CqlValue::Int(2), CqlValue::Int(13)];

        let cql: CqlValue = CqlValue::Set(my_vec);
        let decoded = cql.as_set().unwrap();

        assert_eq!(decoded[0], CqlValue::Int(20));
        assert_eq!(decoded[1], CqlValue::Int(2));
        assert_eq!(decoded[2], CqlValue::Int(13));
    }

    #[test]
    fn test_map_from_cql() {
        let my_vec: Vec<(CqlValue, CqlValue)> = vec![
            (CqlValue::Int(20), CqlValue::Int(21)),
            (CqlValue::Int(2), CqlValue::Int(3)),
        ];

        let cql: CqlValue = CqlValue::Map(my_vec);

        // Test borrowing.
        let decoded = cql.as_map().unwrap();

        assert_eq!(CqlValue::Int(20), decoded[0].0);
        assert_eq!(CqlValue::Int(21), decoded[0].1);

        assert_eq!(CqlValue::Int(2), decoded[1].0);
        assert_eq!(CqlValue::Int(3), decoded[1].1);

        // Test taking the ownership.
        let decoded = cql.into_pair_vec().unwrap();

        assert_eq!(CqlValue::Int(20), decoded[0].0);
        assert_eq!(CqlValue::Int(21), decoded[0].1);

        assert_eq!(CqlValue::Int(2), decoded[1].0);
        assert_eq!(CqlValue::Int(3), decoded[1].1);
    }

    #[test]
    fn test_udt_from_cql() {
        let my_fields: Vec<(String, Option<CqlValue>)> = vec![
            ("fst".to_string(), Some(CqlValue::Int(10))),
            ("snd".to_string(), Some(CqlValue::Boolean(true))),
        ];

        let cql: CqlValue = CqlValue::UserDefinedType {
            keyspace: "".to_string(),
            type_name: "".to_string(),
            fields: my_fields,
        };

        // Test borrowing.
        let decoded = cql.as_udt().unwrap();

        assert_eq!("fst".to_string(), decoded[0].0);
        assert_eq!(Some(CqlValue::Int(10)), decoded[0].1);

        assert_eq!("snd".to_string(), decoded[1].0);
        assert_eq!(Some(CqlValue::Boolean(true)), decoded[1].1);

        let decoded = cql.into_udt_pair_vec().unwrap();

        assert_eq!("fst".to_string(), decoded[0].0);
        assert_eq!(Some(CqlValue::Int(10)), decoded[0].1);

        assert_eq!("snd".to_string(), decoded[1].0);
        assert_eq!(Some(CqlValue::Boolean(true)), decoded[1].1);
    }

    #[test]
    fn test_deserialize_date() {
        // Date is correctly parsed from a 4 byte array
        let four_bytes: [u8; 4] = [12, 23, 34, 45];
        let date: CqlValue =
            super::deser_cql_value(&ColumnType::Date, &mut four_bytes.as_ref()).unwrap();
        assert_eq!(
            date,
            CqlValue::Date(CqlDate(u32::from_be_bytes(four_bytes)))
        );

        // Date is parsed as u32 not i32, u32::MAX is u32::MAX
        let date: CqlValue =
            super::deser_cql_value(&ColumnType::Date, &mut u32::MAX.to_be_bytes().as_ref())
                .unwrap();
        assert_eq!(date, CqlValue::Date(CqlDate(u32::MAX)));

        // Trying to parse a 0, 3 or 5 byte array fails
        super::deser_cql_value(&ColumnType::Date, &mut [].as_ref()).unwrap();
        super::deser_cql_value(&ColumnType::Date, &mut [1, 2, 3].as_ref()).unwrap_err();
        super::deser_cql_value(&ColumnType::Date, &mut [1, 2, 3, 4, 5].as_ref()).unwrap_err();

        // Deserialize unix epoch
        let unix_epoch_bytes = 2_u32.pow(31).to_be_bytes();

        let date =
            super::deser_cql_value(&ColumnType::Date, &mut unix_epoch_bytes.as_ref()).unwrap();
        assert_eq!(date.as_cql_date(), Some(CqlDate(1 << 31)));

        // 2^31 - 30 when converted to NaiveDate is 1969-12-02
        let before_epoch = CqlDate((1 << 31) - 30);
        let date: CqlValue = super::deser_cql_value(
            &ColumnType::Date,
            &mut ((1_u32 << 31) - 30).to_be_bytes().as_ref(),
        )
        .unwrap();

        assert_eq!(date.as_cql_date(), Some(before_epoch));

        // 2^31 + 30 when converted to NaiveDate is 1970-01-31
        let after_epoch = CqlDate((1 << 31) + 30);
        let date = super::deser_cql_value(
            &ColumnType::Date,
            &mut ((1_u32 << 31) + 30).to_be_bytes().as_ref(),
        )
        .unwrap();

        assert_eq!(date.as_cql_date(), Some(after_epoch));

        // Min date
        let min_date = CqlDate(u32::MIN);
        let date = super::deser_cql_value(&ColumnType::Date, &mut u32::MIN.to_be_bytes().as_ref())
            .unwrap();
        assert_eq!(date.as_cql_date(), Some(min_date));

        // Max date
        let max_date = CqlDate(u32::MAX);
        let date = super::deser_cql_value(&ColumnType::Date, &mut u32::MAX.to_be_bytes().as_ref())
            .unwrap();
        assert_eq!(date.as_cql_date(), Some(max_date));
    }

    #[cfg(feature = "chrono-04")]
    #[test]
    fn test_naive_date_04_from_cql() {
        use chrono_04::NaiveDate;

        // 2^31 when converted to NaiveDate is 1970-01-01
        let unix_epoch = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
        let date =
            super::deser_cql_value(&ColumnType::Date, &mut (1u32 << 31).to_be_bytes().as_ref())
                .unwrap();

        assert_eq!(date.as_naive_date_04(), Some(unix_epoch));

        // 2^31 - 30 when converted to NaiveDate is 1969-12-02
        let before_epoch = NaiveDate::from_ymd_opt(1969, 12, 2).unwrap();
        let date = super::deser_cql_value(
            &ColumnType::Date,
            &mut ((1u32 << 31) - 30).to_be_bytes().as_ref(),
        )
        .unwrap();

        assert_eq!(date.as_naive_date_04(), Some(before_epoch));

        // 2^31 + 30 when converted to NaiveDate is 1970-01-31
        let after_epoch = NaiveDate::from_ymd_opt(1970, 1, 31).unwrap();
        let date = super::deser_cql_value(
            &ColumnType::Date,
            &mut ((1u32 << 31) + 30).to_be_bytes().as_ref(),
        )
        .unwrap();

        assert_eq!(date.as_naive_date_04(), Some(after_epoch));

        // 0 and u32::MAX are out of NaiveDate range, fails with an error, not panics
        assert_eq!(
            super::deser_cql_value(&ColumnType::Date, &mut 0_u32.to_be_bytes().as_ref())
                .unwrap()
                .as_naive_date_04(),
            None
        );

        assert_eq!(
            super::deser_cql_value(&ColumnType::Date, &mut u32::MAX.to_be_bytes().as_ref())
                .unwrap()
                .as_naive_date_04(),
            None
        );
    }

    #[cfg(feature = "time-03")]
    #[test]
    fn test_date_03_from_cql() {
        use time_03::Date;
        use time_03::Month::*;

        // 2^31 when converted to time_03::Date is 1970-01-01
        let unix_epoch = Date::from_calendar_date(1970, January, 1).unwrap();
        let date =
            super::deser_cql_value(&ColumnType::Date, &mut (1u32 << 31).to_be_bytes().as_ref())
                .unwrap();

        assert_eq!(date.as_date_03(), Some(unix_epoch));

        // 2^31 - 30 when converted to time_03::Date is 1969-12-02
        let before_epoch = Date::from_calendar_date(1969, December, 2).unwrap();
        let date = super::deser_cql_value(
            &ColumnType::Date,
            &mut ((1u32 << 31) - 30).to_be_bytes().as_ref(),
        )
        .unwrap();

        assert_eq!(date.as_date_03(), Some(before_epoch));

        // 2^31 + 30 when converted to time_03::Date is 1970-01-31
        let after_epoch = Date::from_calendar_date(1970, January, 31).unwrap();
        let date = super::deser_cql_value(
            &ColumnType::Date,
            &mut ((1u32 << 31) + 30).to_be_bytes().as_ref(),
        )
        .unwrap();

        assert_eq!(date.as_date_03(), Some(after_epoch));

        // 0 and u32::MAX are out of NaiveDate range, fails with an error, not panics
        assert_eq!(
            super::deser_cql_value(&ColumnType::Date, &mut 0_u32.to_be_bytes().as_ref())
                .unwrap()
                .as_date_03(),
            None
        );

        assert_eq!(
            super::deser_cql_value(&ColumnType::Date, &mut u32::MAX.to_be_bytes().as_ref())
                .unwrap()
                .as_date_03(),
            None
        );
    }

    #[test]
    fn test_deserialize_time() {
        // Time is an i64 - nanoseconds since midnight
        // in range 0..=86399999999999

        let max_time: i64 = 24 * 60 * 60 * 1_000_000_000 - 1;
        assert_eq!(max_time, 86399999999999);

        // Check that basic values are deserialized correctly
        for test_val in [0, 1, 18463, max_time].iter() {
            let bytes: [u8; 8] = test_val.to_be_bytes();
            let cql_value: CqlValue =
                super::deser_cql_value(&ColumnType::Time, &mut &bytes[..]).unwrap();
            assert_eq!(cql_value, CqlValue::Time(CqlTime(*test_val)));
        }

        // Negative values cause an error
        // Values bigger than 86399999999999 cause an error
        for test_val in [-1, i64::MIN, max_time + 1, i64::MAX].iter() {
            let bytes: [u8; 8] = test_val.to_be_bytes();
            super::deser_cql_value(&ColumnType::Time, &mut &bytes[..]).unwrap_err();
        }
    }

    #[cfg(feature = "chrono-04")]
    #[test]
    fn test_naive_time_04_from_cql() {
        use chrono_04::NaiveTime;

        // 0 when converted to NaiveTime is 0:0:0.0
        let midnight = NaiveTime::from_hms_nano_opt(0, 0, 0, 0).unwrap();
        let time =
            super::deser_cql_value(&ColumnType::Time, &mut (0i64).to_be_bytes().as_ref()).unwrap();

        assert_eq!(time.as_naive_time_04(), Some(midnight));

        // 10:10:30.500,000,001
        let (h, m, s, n) = (10, 10, 30, 500_000_001);
        let midnight = NaiveTime::from_hms_nano_opt(h, m, s, n).unwrap();
        let time = super::deser_cql_value(
            &ColumnType::Time,
            &mut ((h as i64 * 3600 + m as i64 * 60 + s as i64) * 1_000_000_000 + n as i64)
                .to_be_bytes()
                .as_ref(),
        )
        .unwrap();

        assert_eq!(time.as_naive_time_04(), Some(midnight));

        // 23:59:59.999,999,999
        let (h, m, s, n) = (23, 59, 59, 999_999_999);
        let midnight = NaiveTime::from_hms_nano_opt(h, m, s, n).unwrap();
        let time = super::deser_cql_value(
            &ColumnType::Time,
            &mut ((h as i64 * 3600 + m as i64 * 60 + s as i64) * 1_000_000_000 + n as i64)
                .to_be_bytes()
                .as_ref(),
        )
        .unwrap();

        assert_eq!(time.as_naive_time_04(), Some(midnight));
    }

    #[cfg(feature = "time-03")]
    #[test]
    fn test_primitive_time_03_from_cql() {
        use time_03::Time;

        // 0 when converted to NaiveTime is 0:0:0.0
        let midnight = Time::from_hms_nano(0, 0, 0, 0).unwrap();
        let time =
            super::deser_cql_value(&ColumnType::Time, &mut (0i64).to_be_bytes().as_ref()).unwrap();

        assert_eq!(time.as_time_03(), Some(midnight));

        // 10:10:30.500,000,001
        let (h, m, s, n) = (10, 10, 30, 500_000_001);
        let midnight = Time::from_hms_nano(h, m, s, n).unwrap();
        let time = super::deser_cql_value(
            &ColumnType::Time,
            &mut ((h as i64 * 3600 + m as i64 * 60 + s as i64) * 1_000_000_000 + n as i64)
                .to_be_bytes()
                .as_ref(),
        )
        .unwrap();

        assert_eq!(time.as_time_03(), Some(midnight));

        // 23:59:59.999,999,999
        let (h, m, s, n) = (23, 59, 59, 999_999_999);
        let midnight = Time::from_hms_nano(h, m, s, n).unwrap();
        let time = super::deser_cql_value(
            &ColumnType::Time,
            &mut ((h as i64 * 3600 + m as i64 * 60 + s as i64) * 1_000_000_000 + n as i64)
                .to_be_bytes()
                .as_ref(),
        )
        .unwrap();

        assert_eq!(time.as_time_03(), Some(midnight));
    }

    #[test]
    fn test_timestamp_deserialize() {
        // Timestamp is an i64 - milliseconds since unix epoch

        // Check that test values are deserialized correctly
        for test_val in &[0, -1, 1, 74568745, -4584658, i64::MIN, i64::MAX] {
            let bytes: [u8; 8] = test_val.to_be_bytes();
            let cql_value: CqlValue =
                super::deser_cql_value(&ColumnType::Timestamp, &mut &bytes[..]).unwrap();
            assert_eq!(cql_value, CqlValue::Timestamp(CqlTimestamp(*test_val)));
        }
    }

    #[cfg(feature = "chrono-04")]
    #[test]
    fn test_datetime_04_from_cql() {
        use chrono_04::{DateTime, NaiveDate, NaiveDateTime, NaiveTime};

        // 0 when converted to DateTime is 1970-01-01 0:00:00.00
        let unix_epoch = DateTime::from_timestamp(0, 0).unwrap();
        let date = super::deser_cql_value(&ColumnType::Timestamp, &mut 0i64.to_be_bytes().as_ref())
            .unwrap();

        assert_eq!(date.as_datetime_04(), Some(unix_epoch));

        // When converted to NaiveDateTime, this is 1969-12-01 11:29:29.5
        let timestamp: i64 = -((((30 * 24 + 12) * 60 + 30) * 60 + 30) * 1000 + 500);
        let before_epoch = NaiveDateTime::new(
            NaiveDate::from_ymd_opt(1969, 12, 1).unwrap(),
            NaiveTime::from_hms_milli_opt(11, 29, 29, 500).unwrap(),
        )
        .and_utc();
        let date = super::deser_cql_value(
            &ColumnType::Timestamp,
            &mut timestamp.to_be_bytes().as_ref(),
        )
        .unwrap();

        assert_eq!(date.as_datetime_04(), Some(before_epoch));

        // when converted to NaiveDateTime, this is is 1970-01-31 12:30:30.5
        let timestamp: i64 = (((30 * 24 + 12) * 60 + 30) * 60 + 30) * 1000 + 500;
        let after_epoch = NaiveDateTime::new(
            NaiveDate::from_ymd_opt(1970, 1, 31).unwrap(),
            NaiveTime::from_hms_milli_opt(12, 30, 30, 500).unwrap(),
        )
        .and_utc();
        let date = super::deser_cql_value(
            &ColumnType::Timestamp,
            &mut timestamp.to_be_bytes().as_ref(),
        )
        .unwrap();

        assert_eq!(date.as_datetime_04(), Some(after_epoch));

        // 0 and u32::MAX are out of NaiveDate range, fails with an error, not panics
        assert_eq!(
            super::deser_cql_value(&ColumnType::Timestamp, &mut i64::MIN.to_be_bytes().as_ref())
                .unwrap()
                .as_datetime_04(),
            None
        );

        assert_eq!(
            super::deser_cql_value(&ColumnType::Timestamp, &mut i64::MAX.to_be_bytes().as_ref())
                .unwrap()
                .as_datetime_04(),
            None
        );
    }

    #[cfg(feature = "time-03")]
    #[test]
    fn test_offset_datetime_03_from_cql() {
        use time_03::{Date, Month::*, OffsetDateTime, PrimitiveDateTime, Time};

        // 0 when converted to OffsetDateTime is 1970-01-01 0:00:00.00
        let unix_epoch = OffsetDateTime::from_unix_timestamp(0).unwrap();
        let date = super::deser_cql_value(&ColumnType::Timestamp, &mut 0i64.to_be_bytes().as_ref())
            .unwrap();

        assert_eq!(date.as_offset_date_time_03(), Some(unix_epoch));

        // When converted to NaiveDateTime, this is 1969-12-01 11:29:29.5
        let timestamp: i64 = -((((30 * 24 + 12) * 60 + 30) * 60 + 30) * 1000 + 500);
        let before_epoch = PrimitiveDateTime::new(
            Date::from_calendar_date(1969, December, 1).unwrap(),
            Time::from_hms_milli(11, 29, 29, 500).unwrap(),
        )
        .assume_utc();
        let date = super::deser_cql_value(
            &ColumnType::Timestamp,
            &mut timestamp.to_be_bytes().as_ref(),
        )
        .unwrap();

        assert_eq!(date.as_offset_date_time_03(), Some(before_epoch));

        // when converted to NaiveDateTime, this is is 1970-01-31 12:30:30.5
        let timestamp: i64 = (((30 * 24 + 12) * 60 + 30) * 60 + 30) * 1000 + 500;
        let after_epoch = PrimitiveDateTime::new(
            Date::from_calendar_date(1970, January, 31).unwrap(),
            Time::from_hms_milli(12, 30, 30, 500).unwrap(),
        )
        .assume_utc();
        let date = super::deser_cql_value(
            &ColumnType::Timestamp,
            &mut timestamp.to_be_bytes().as_ref(),
        )
        .unwrap();

        assert_eq!(date.as_offset_date_time_03(), Some(after_epoch));

        // 0 and u32::MAX are out of NaiveDate range, fails with an error, not panics
        assert_eq!(
            super::deser_cql_value(&ColumnType::Timestamp, &mut i64::MIN.to_be_bytes().as_ref())
                .unwrap()
                .as_offset_date_time_03(),
            None
        );

        assert_eq!(
            super::deser_cql_value(&ColumnType::Timestamp, &mut i64::MAX.to_be_bytes().as_ref())
                .unwrap()
                .as_offset_date_time_03(),
            None
        );
    }

    #[test]
    fn test_serialize_empty() {
        use crate::frame::value::Value;

        let empty = CqlValue::Empty;
        let mut v = Vec::new();
        empty.serialize(&mut v).unwrap();

        assert_eq!(v, vec![0, 0, 0, 0]);
    }

    #[test]
    fn test_duration_deserialize() {
        let bytes = [0xc, 0x12, 0xe2, 0x8c, 0x39, 0xd2];
        let cql_value: CqlValue =
            super::deser_cql_value(&ColumnType::Duration, &mut &bytes[..]).unwrap();
        assert_eq!(
            cql_value,
            CqlValue::Duration(CqlDuration {
                months: 6,
                days: 9,
                nanoseconds: 21372137
            })
        );
    }

    #[test]
    fn test_deserialize_empty_payload() {
        for (test_type, res_cql) in [
            (ColumnType::Ascii, CqlValue::Ascii("".to_owned())),
            (ColumnType::Boolean, CqlValue::Empty),
            (ColumnType::Blob, CqlValue::Blob(vec![])),
            (ColumnType::Counter, CqlValue::Empty),
            (ColumnType::Date, CqlValue::Empty),
            (ColumnType::Decimal, CqlValue::Empty),
            (ColumnType::Double, CqlValue::Empty),
            (ColumnType::Float, CqlValue::Empty),
            (ColumnType::Int, CqlValue::Empty),
            (ColumnType::BigInt, CqlValue::Empty),
            (ColumnType::Text, CqlValue::Text("".to_owned())),
            (ColumnType::Timestamp, CqlValue::Empty),
            (ColumnType::Inet, CqlValue::Empty),
            (ColumnType::List(Box::new(ColumnType::Int)), CqlValue::Empty),
            (
                ColumnType::Map(Box::new(ColumnType::Int), Box::new(ColumnType::Int)),
                CqlValue::Empty,
            ),
            (ColumnType::Set(Box::new(ColumnType::Int)), CqlValue::Empty),
            (
                ColumnType::UserDefinedType {
                    type_name: "".into(),
                    keyspace: "".into(),
                    field_types: vec![],
                },
                CqlValue::Empty,
            ),
            (ColumnType::SmallInt, CqlValue::Empty),
            (ColumnType::TinyInt, CqlValue::Empty),
            (ColumnType::Time, CqlValue::Empty),
            (ColumnType::Timeuuid, CqlValue::Empty),
            (ColumnType::Tuple(vec![]), CqlValue::Empty),
            (ColumnType::Uuid, CqlValue::Empty),
            (ColumnType::Varint, CqlValue::Empty),
        ] {
            let cql_value: CqlValue = super::deser_cql_value(&test_type, &mut &[][..]).unwrap();

            assert_eq!(cql_value, res_cql);
        }
    }

    #[test]
    fn test_timeuuid_deserialize() {
        // A few random timeuuids generated manually
        let tests = [
            (
                "8e14e760-7fa8-11eb-bc66-000000000001",
                [
                    0x8e, 0x14, 0xe7, 0x60, 0x7f, 0xa8, 0x11, 0xeb, 0xbc, 0x66, 0, 0, 0, 0, 0, 0x01,
                ],
            ),
            (
                "9b349580-7fa8-11eb-bc66-000000000001",
                [
                    0x9b, 0x34, 0x95, 0x80, 0x7f, 0xa8, 0x11, 0xeb, 0xbc, 0x66, 0, 0, 0, 0, 0, 0x01,
                ],
            ),
            (
                "5d74bae0-7fa3-11eb-bc66-000000000001",
                [
                    0x5d, 0x74, 0xba, 0xe0, 0x7f, 0xa3, 0x11, 0xeb, 0xbc, 0x66, 0, 0, 0, 0, 0, 0x01,
                ],
            ),
        ];

        for (uuid_str, uuid_bytes) in &tests {
            let cql_val: CqlValue =
                super::deser_cql_value(&ColumnType::Timeuuid, &mut &uuid_bytes[..]).unwrap();

            match cql_val {
                CqlValue::Timeuuid(uuid) => {
                    assert_eq!(uuid.as_bytes(), uuid_bytes);
                    assert_eq!(CqlTimeuuid::from_str(uuid_str).unwrap(), uuid);
                }
                _ => panic!("Timeuuid parsed as wrong CqlValue"),
            }
        }
    }
}
