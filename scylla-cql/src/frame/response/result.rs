use crate::cql_to_rust::{FromRow, FromRowError};
use crate::frame::response::event::SchemaChangeEvent;
use crate::frame::types::vint_decode;
use crate::frame::value::{
    Counter, CqlDate, CqlDuration, CqlTime, CqlTimestamp, CqlTimeuuid, CqlVarint,
};
use crate::frame::{frame_errors::ParseError, types};
use bigdecimal::BigDecimal;
use byteorder::{BigEndian, ReadBytesExt};
use bytes::{Buf, Bytes};
use std::{
    convert::{TryFrom, TryInto},
    net::IpAddr,
    result::Result as StdResult,
    str,
};
use uuid::Uuid;

#[cfg(feature = "chrono")]
use chrono::{DateTime, NaiveDate, Utc};

#[derive(Debug)]
pub struct SetKeyspace {
    pub keyspace_name: String,
}

#[derive(Debug)]
pub struct Prepared {
    pub id: Bytes,
    pub prepared_metadata: PreparedMetadata,
    pub result_metadata: ResultMetadata,
}

#[derive(Debug)]
pub struct SchemaChange {
    pub event: SchemaChangeEvent,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableSpec {
    pub ks_name: String,
    pub table_name: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ColumnType {
    Custom(String),
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
    List(Box<ColumnType>),
    Map(Box<ColumnType>, Box<ColumnType>),
    Set(Box<ColumnType>),
    UserDefinedType {
        type_name: String,
        keyspace: String,
        field_types: Vec<(String, ColumnType)>,
    },
    SmallInt,
    TinyInt,
    Time,
    Timeuuid,
    Tuple(Vec<ColumnType>),
    Uuid,
    Varint,
}

#[derive(Clone, Debug, PartialEq)]
pub enum CqlValue {
    Ascii(String),
    Boolean(bool),
    Blob(Vec<u8>),
    Counter(Counter),
    Decimal(BigDecimal),
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

impl ColumnType {
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

    #[cfg(feature = "chrono")]
    pub fn as_naive_date(&self) -> Option<NaiveDate> {
        self.as_cql_date().and_then(|date| date.try_into().ok())
    }

    #[cfg(feature = "time")]
    pub fn as_date(&self) -> Option<time::Date> {
        self.as_cql_date().and_then(|date| date.try_into().ok())
    }

    pub fn as_cql_timestamp(&self) -> Option<CqlTimestamp> {
        match self {
            Self::Timestamp(i) => Some(*i),
            _ => None,
        }
    }

    #[cfg(feature = "chrono")]
    pub fn as_datetime(&self) -> Option<DateTime<Utc>> {
        self.as_cql_timestamp().and_then(|ts| ts.try_into().ok())
    }

    #[cfg(feature = "time")]
    pub fn as_offset_date_time(&self) -> Option<time::OffsetDateTime> {
        self.as_cql_timestamp().and_then(|ts| ts.try_into().ok())
    }

    pub fn as_cql_time(&self) -> Option<CqlTime> {
        match self {
            Self::Time(i) => Some(*i),
            _ => None,
        }
    }

    #[cfg(feature = "chrono")]
    pub fn as_naive_time(&self) -> Option<chrono::NaiveTime> {
        self.as_cql_time().and_then(|ts| ts.try_into().ok())
    }

    #[cfg(feature = "time")]
    pub fn as_time(&self) -> Option<time::Time> {
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

    pub fn into_decimal(self) -> Option<BigDecimal> {
        match self {
            Self::Decimal(i) => Some(i),
            _ => None,
        }
    }
    // TODO
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnSpec {
    pub table_spec: TableSpec,
    pub name: String,
    pub typ: ColumnType,
}

#[derive(Debug, Default)]
pub struct ResultMetadata {
    col_count: usize,
    pub paging_state: Option<Bytes>,
    pub col_specs: Vec<ColumnSpec>,
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
    pub col_specs: Vec<ColumnSpec>,
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
    pub metadata: ResultMetadata,
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

fn deser_table_spec(buf: &mut &[u8]) -> StdResult<TableSpec, ParseError> {
    let ks_name = types::read_string(buf)?.to_owned();
    let table_name = types::read_string(buf)?.to_owned();
    Ok(TableSpec {
        ks_name,
        table_name,
    })
}

fn deser_type(buf: &mut &[u8]) -> StdResult<ColumnType, ParseError> {
    use ColumnType::*;
    let id = types::read_short(buf)?;
    Ok(match id {
        0x0000 => {
            let type_str: String = types::read_string(buf)?.to_string();
            match type_str.as_str() {
                "org.apache.cassandra.db.marshal.DurationType" => Duration,
                _ => Custom(type_str),
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
        0x0020 => List(Box::new(deser_type(buf)?)),
        0x0021 => Map(Box::new(deser_type(buf)?), Box::new(deser_type(buf)?)),
        0x0022 => Set(Box::new(deser_type(buf)?)),
        0x0030 => {
            let keyspace_name: String = types::read_string(buf)?.to_string();
            let type_name: String = types::read_string(buf)?.to_string();
            let fields_size: usize = types::read_short(buf)?.into();

            let mut field_types: Vec<(String, ColumnType)> = Vec::with_capacity(fields_size);

            for _ in 0..fields_size {
                let field_name: String = types::read_string(buf)?.to_string();
                let field_type: ColumnType = deser_type(buf)?;

                field_types.push((field_name, field_type));
            }

            UserDefinedType {
                type_name,
                keyspace: keyspace_name,
                field_types,
            }
        }
        0x0031 => {
            let len: usize = types::read_short(buf)?.into();
            let mut types = Vec::with_capacity(len);
            for _ in 0..len {
                types.push(deser_type(buf)?);
            }
            Tuple(types)
        }
        id => {
            // TODO implement other types
            return Err(ParseError::TypeNotImplemented(id));
        }
    })
}

fn deser_col_specs(
    buf: &mut &[u8],
    global_table_spec: &Option<TableSpec>,
    col_count: usize,
) -> StdResult<Vec<ColumnSpec>, ParseError> {
    let mut col_specs = Vec::with_capacity(col_count);
    for _ in 0..col_count {
        let table_spec = if let Some(spec) = global_table_spec {
            spec.clone()
        } else {
            deser_table_spec(buf)?
        };
        let name = types::read_string(buf)?.to_owned();
        let typ = deser_type(buf)?;
        col_specs.push(ColumnSpec {
            table_spec,
            name,
            typ,
        });
    }
    Ok(col_specs)
}

fn deser_result_metadata(buf: &mut &[u8]) -> StdResult<ResultMetadata, ParseError> {
    let flags = types::read_int(buf)?;
    let global_tables_spec = flags & 0x0001 != 0;
    let has_more_pages = flags & 0x0002 != 0;
    let no_metadata = flags & 0x0004 != 0;

    let col_count: usize = types::read_int(buf)?.try_into()?;

    let paging_state = if has_more_pages {
        Some(types::read_bytes(buf)?.to_owned().into())
    } else {
        None
    };

    if no_metadata {
        return Ok(ResultMetadata {
            col_count,
            paging_state,
            col_specs: vec![],
        });
    }

    let global_table_spec = if global_tables_spec {
        Some(deser_table_spec(buf)?)
    } else {
        None
    };

    let col_specs = deser_col_specs(buf, &global_table_spec, col_count)?;

    Ok(ResultMetadata {
        col_count,
        paging_state,
        col_specs,
    })
}

fn deser_prepared_metadata(buf: &mut &[u8]) -> StdResult<PreparedMetadata, ParseError> {
    let flags = types::read_int(buf)?;
    let global_tables_spec = flags & 0x0001 != 0;

    let col_count = types::read_int_length(buf)?;

    let pk_count: usize = types::read_int(buf)?.try_into()?;

    let mut pk_indexes = Vec::with_capacity(pk_count);
    for i in 0..pk_count {
        pk_indexes.push(PartitionKeyIndex {
            index: types::read_short(buf)? as u16,
            sequence: i as u16,
        });
    }
    pk_indexes.sort_unstable_by_key(|pki| pki.index);

    let global_table_spec = if global_tables_spec {
        Some(deser_table_spec(buf)?)
    } else {
        None
    };

    let col_specs = deser_col_specs(buf, &global_table_spec, col_count)?;

    Ok(PreparedMetadata {
        flags,
        col_count,
        pk_indexes,
        col_specs,
    })
}

pub fn deser_cql_value(typ: &ColumnType, buf: &mut &[u8]) -> StdResult<CqlValue, ParseError> {
    use ColumnType::*;

    if buf.is_empty() {
        match typ {
            Ascii | Blob | Text => {
                // can't be empty
            }
            _ => return Ok(CqlValue::Empty),
        }
    }

    Ok(match typ {
        Custom(type_str) => {
            return Err(ParseError::BadIncomingData(format!(
                "Support for custom types is not yet implemented: {}",
                type_str
            )));
        }
        Ascii => {
            if !buf.is_ascii() {
                return Err(ParseError::BadIncomingData(
                    "String is not ascii!".to_string(),
                ));
            }
            CqlValue::Ascii(str::from_utf8(buf)?.to_owned())
        }
        Boolean => {
            if buf.len() != 1 {
                return Err(ParseError::BadIncomingData(format!(
                    "Buffer length should be 1 not {}",
                    buf.len()
                )));
            }
            CqlValue::Boolean(buf[0] != 0x00)
        }
        Blob => CqlValue::Blob(buf.to_vec()),
        Date => {
            if buf.len() != 4 {
                return Err(ParseError::BadIncomingData(format!(
                    "Buffer length should be 4 not {}",
                    buf.len()
                )));
            }

            let date_value = buf.read_u32::<BigEndian>()?;
            CqlValue::Date(CqlDate(date_value))
        }
        Counter => {
            if buf.len() != 8 {
                return Err(ParseError::BadIncomingData(format!(
                    "Buffer length should be 8 not {}",
                    buf.len()
                )));
            }
            CqlValue::Counter(crate::frame::value::Counter(buf.read_i64::<BigEndian>()?))
        }
        Decimal => {
            let scale = types::read_int(buf)? as i64;
            let int_value = bigdecimal::num_bigint::BigInt::from_signed_bytes_be(buf);
            let big_decimal: BigDecimal = BigDecimal::from((int_value, scale));

            CqlValue::Decimal(big_decimal)
        }
        Double => {
            if buf.len() != 8 {
                return Err(ParseError::BadIncomingData(format!(
                    "Buffer length should be 8 not {}",
                    buf.len()
                )));
            }
            CqlValue::Double(buf.read_f64::<BigEndian>()?)
        }
        Float => {
            if buf.len() != 4 {
                return Err(ParseError::BadIncomingData(format!(
                    "Buffer length should be 4 not {}",
                    buf.len()
                )));
            }
            CqlValue::Float(buf.read_f32::<BigEndian>()?)
        }
        Int => {
            if buf.len() != 4 {
                return Err(ParseError::BadIncomingData(format!(
                    "Buffer length should be 4 not {}",
                    buf.len()
                )));
            }
            CqlValue::Int(buf.read_i32::<BigEndian>()?)
        }
        SmallInt => {
            if buf.len() != 2 {
                return Err(ParseError::BadIncomingData(format!(
                    "Buffer length should be 2 not {}",
                    buf.len()
                )));
            }

            CqlValue::SmallInt(buf.read_i16::<BigEndian>()?)
        }
        TinyInt => {
            if buf.len() != 1 {
                return Err(ParseError::BadIncomingData(format!(
                    "Buffer length should be 1 not {}",
                    buf.len()
                )));
            }
            CqlValue::TinyInt(buf.read_i8()?)
        }
        BigInt => {
            if buf.len() != 8 {
                return Err(ParseError::BadIncomingData(format!(
                    "Buffer length should be 8 not {}",
                    buf.len()
                )));
            }
            CqlValue::BigInt(buf.read_i64::<BigEndian>()?)
        }
        Text => CqlValue::Text(str::from_utf8(buf)?.to_owned()),
        Timestamp => {
            if buf.len() != 8 {
                return Err(ParseError::BadIncomingData(format!(
                    "Buffer length should be 8 not {}",
                    buf.len()
                )));
            }
            let millis = buf.read_i64::<BigEndian>()?;

            CqlValue::Timestamp(CqlTimestamp(millis))
        }
        Time => {
            if buf.len() != 8 {
                return Err(ParseError::BadIncomingData(format!(
                    "Buffer length should be 8 not {}",
                    buf.len()
                )));
            }
            let nanoseconds: i64 = buf.read_i64::<BigEndian>()?;

            // Valid values are in the range 0 to 86399999999999
            if !(0..=86399999999999).contains(&nanoseconds) {
                return Err(ParseError::BadIncomingData(format! {
                    "Invalid time value only 0 to 86399999999999 allowed: {}.", nanoseconds
                }));
            }

            CqlValue::Time(CqlTime(nanoseconds))
        }
        Timeuuid => {
            if buf.len() != 16 {
                return Err(ParseError::BadIncomingData(format!(
                    "Buffer length should be 16 not {}",
                    buf.len()
                )));
            }
            let uuid = uuid::Uuid::from_slice(buf).expect("Deserializing Uuid failed.");
            CqlValue::Timeuuid(CqlTimeuuid::from(uuid))
        }
        Duration => {
            let months = i32::try_from(vint_decode(buf)?)?;
            let days = i32::try_from(vint_decode(buf)?)?;
            let nanoseconds = vint_decode(buf)?;

            CqlValue::Duration(CqlDuration {
                months,
                days,
                nanoseconds,
            })
        }
        Inet => CqlValue::Inet(match buf.len() {
            4 => {
                let ret = IpAddr::from(<[u8; 4]>::try_from(&buf[0..4])?);
                buf.advance(4);
                ret
            }
            16 => {
                let ret = IpAddr::from(<[u8; 16]>::try_from(&buf[0..16])?);
                buf.advance(16);
                ret
            }
            v => {
                return Err(ParseError::BadIncomingData(format!(
                    "Invalid inet bytes length: {}",
                    v
                )));
            }
        }),
        Uuid => {
            if buf.len() != 16 {
                return Err(ParseError::BadIncomingData(format!(
                    "Buffer length should be 16 not {}",
                    buf.len()
                )));
            }
            let uuid = uuid::Uuid::from_slice(buf).expect("Deserializing Uuid failed.");
            CqlValue::Uuid(uuid)
        }
        Varint => CqlValue::Varint(CqlVarint::from_signed_bytes_be(buf.to_vec())),
        List(type_name) => {
            let len: usize = types::read_int(buf)?.try_into()?;
            let mut res = Vec::with_capacity(len);
            for _ in 0..len {
                let mut b = types::read_bytes(buf)?;
                res.push(deser_cql_value(type_name, &mut b)?);
            }
            CqlValue::List(res)
        }
        Map(key_type, value_type) => {
            let len: usize = types::read_int(buf)?.try_into()?;
            let mut res = Vec::with_capacity(len);
            for _ in 0..len {
                let mut b = types::read_bytes(buf)?;
                let key = deser_cql_value(key_type, &mut b)?;
                b = types::read_bytes(buf)?;
                let val = deser_cql_value(value_type, &mut b)?;
                res.push((key, val));
            }
            CqlValue::Map(res)
        }
        Set(type_name) => {
            let len: usize = types::read_int(buf)?.try_into()?;
            let mut res = Vec::with_capacity(len);
            for _ in 0..len {
                // TODO: is `null` allowed as set element? Should we use read_bytes_opt?
                let mut b = types::read_bytes(buf)?;
                res.push(deser_cql_value(type_name, &mut b)?);
            }
            CqlValue::Set(res)
        }
        UserDefinedType {
            type_name,
            keyspace,
            field_types,
        } => {
            let mut fields: Vec<(String, Option<CqlValue>)> = Vec::new();

            for (field_name, field_type) in field_types {
                // If a field is added to a UDT and we read an old (frozen ?) version of it,
                // the driver will fail to parse the whole UDT.
                // This is why we break the parsing after we reach the end of the serialized UDT.
                if buf.is_empty() {
                    break;
                }

                let mut field_value: Option<CqlValue> = None;
                if let Some(mut field_val_bytes) = types::read_bytes_opt(buf)? {
                    field_value = Some(deser_cql_value(field_type, &mut field_val_bytes)?);
                }

                fields.push((field_name.clone(), field_value));
            }

            CqlValue::UserDefinedType {
                keyspace: keyspace.clone(),
                type_name: type_name.clone(),
                fields,
            }
        }
        Tuple(type_names) => {
            let mut res = Vec::with_capacity(type_names.len());
            for type_name in type_names {
                match types::read_bytes_opt(buf)? {
                    Some(mut b) => res.push(Some(deser_cql_value(type_name, &mut b)?)),
                    None => res.push(None),
                };
            }

            CqlValue::Tuple(res)
        }
    })
}

fn deser_rows(buf: &mut &[u8]) -> StdResult<Rows, ParseError> {
    let metadata = deser_result_metadata(buf)?;

    let original_size = buf.len();

    // TODO: the protocol allows an optimization (which must be explicitly requested on query by
    // the driver) where the column metadata is not sent with the result.
    // Implement this optimization. We'll then need to take the column types by a parameter.
    // Beware of races; our column types may be outdated.
    assert!(metadata.col_count == metadata.col_specs.len());

    let rows_count: usize = types::read_int(buf)?.try_into()?;

    let mut rows = Vec::with_capacity(rows_count);
    for _ in 0..rows_count {
        let mut columns = Vec::with_capacity(metadata.col_count);
        for i in 0..metadata.col_count {
            let v = if let Some(mut b) = types::read_bytes_opt(buf)? {
                Some(deser_cql_value(&metadata.col_specs[i].typ, &mut b)?)
            } else {
                None
            };
            columns.push(v);
        }
        rows.push(Row { columns });
    }
    Ok(Rows {
        metadata,
        rows_count,
        rows,
        serialized_size: original_size - buf.len(),
    })
}

fn deser_set_keyspace(buf: &mut &[u8]) -> StdResult<SetKeyspace, ParseError> {
    let keyspace_name = types::read_string(buf)?.to_string();

    Ok(SetKeyspace { keyspace_name })
}

fn deser_prepared(buf: &mut &[u8]) -> StdResult<Prepared, ParseError> {
    let id_len = types::read_short(buf)? as usize;
    let id: Bytes = buf[0..id_len].to_owned().into();
    buf.advance(id_len);
    let prepared_metadata = deser_prepared_metadata(buf)?;
    let result_metadata = deser_result_metadata(buf)?;
    Ok(Prepared {
        id,
        prepared_metadata,
        result_metadata,
    })
}

#[allow(clippy::unnecessary_wraps)]
fn deser_schema_change(buf: &mut &[u8]) -> StdResult<SchemaChange, ParseError> {
    Ok(SchemaChange {
        event: SchemaChangeEvent::deserialize(buf)?,
    })
}

pub fn deserialize(buf: &mut &[u8]) -> StdResult<Result, ParseError> {
    use self::Result::*;
    Ok(match types::read_int(buf)? {
        0x0001 => Void,
        0x0002 => Rows(deser_rows(buf)?),
        0x0003 => SetKeyspace(deser_set_keyspace(buf)?),
        0x0004 => Prepared(deser_prepared(buf)?),
        0x0005 => SchemaChange(deser_schema_change(buf)?),
        k => {
            return Err(ParseError::BadIncomingData(format!(
                "Unknown query result id: {}",
                k
            )))
        }
    })
}

#[cfg(test)]
mod tests {
    use crate as scylla;
    use crate::frame::value::{Counter, CqlDate, CqlDuration, CqlTime, CqlTimestamp, CqlTimeuuid};
    use bigdecimal::BigDecimal;
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

    #[cfg(feature = "num-bigint-03")]
    #[test]
    fn test_varint() {
        use num_bigint_03::ToBigInt;

        struct Test<'a> {
            value: num_bigint_03::BigInt,
            encoding: &'a [u8],
        }

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
        let tests = [
            Test {
                value: 0.to_bigint().unwrap(),
                encoding: &[0x00],
            },
            Test {
                value: 1.to_bigint().unwrap(),
                encoding: &[0x01],
            },
            Test {
                value: 127.to_bigint().unwrap(),
                encoding: &[0x7F],
            },
            Test {
                value: 128.to_bigint().unwrap(),
                encoding: &[0x00, 0x80],
            },
            Test {
                value: 129.to_bigint().unwrap(),
                encoding: &[0x00, 0x81],
            },
            Test {
                value: (-1).to_bigint().unwrap(),
                encoding: &[0xFF],
            },
            Test {
                value: (-128).to_bigint().unwrap(),
                encoding: &[0x80],
            },
            Test {
                value: (-129).to_bigint().unwrap(),
                encoding: &[0xFF, 0x7F],
            },
        ];

        for t in tests.iter() {
            let value = super::deser_cql_value(&ColumnType::Varint, &mut &*t.encoding).unwrap();
            assert_eq!(CqlValue::Varint(t.value.clone().into()), value);
        }
    }

    #[test]
    fn test_decimal() {
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
            assert_eq!(CqlValue::Decimal(t.value.clone()), value);
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

    #[cfg(feature = "chrono")]
    #[test]
    fn test_naive_date_from_cql() {
        use chrono::NaiveDate;

        // 2^31 when converted to NaiveDate is 1970-01-01
        let unix_epoch = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
        let date =
            super::deser_cql_value(&ColumnType::Date, &mut (1u32 << 31).to_be_bytes().as_ref())
                .unwrap();

        assert_eq!(date.as_naive_date(), Some(unix_epoch));

        // 2^31 - 30 when converted to NaiveDate is 1969-12-02
        let before_epoch = NaiveDate::from_ymd_opt(1969, 12, 2).unwrap();
        let date = super::deser_cql_value(
            &ColumnType::Date,
            &mut ((1u32 << 31) - 30).to_be_bytes().as_ref(),
        )
        .unwrap();

        assert_eq!(date.as_naive_date(), Some(before_epoch));

        // 2^31 + 30 when converted to NaiveDate is 1970-01-31
        let after_epoch = NaiveDate::from_ymd_opt(1970, 1, 31).unwrap();
        let date = super::deser_cql_value(
            &ColumnType::Date,
            &mut ((1u32 << 31) + 30).to_be_bytes().as_ref(),
        )
        .unwrap();

        assert_eq!(date.as_naive_date(), Some(after_epoch));

        // 0 and u32::MAX are out of NaiveDate range, fails with an error, not panics
        assert_eq!(
            super::deser_cql_value(&ColumnType::Date, &mut 0_u32.to_be_bytes().as_ref())
                .unwrap()
                .as_naive_date(),
            None
        );

        assert_eq!(
            super::deser_cql_value(&ColumnType::Date, &mut u32::MAX.to_be_bytes().as_ref())
                .unwrap()
                .as_naive_date(),
            None
        );
    }

    #[cfg(feature = "time")]
    #[test]
    fn test_date_from_cql() {
        use time::Date;
        use time::Month::*;

        // 2^31 when converted to time::Date is 1970-01-01
        let unix_epoch = Date::from_calendar_date(1970, January, 1).unwrap();
        let date =
            super::deser_cql_value(&ColumnType::Date, &mut (1u32 << 31).to_be_bytes().as_ref())
                .unwrap();

        assert_eq!(date.as_date(), Some(unix_epoch));

        // 2^31 - 30 when converted to time::Date is 1969-12-02
        let before_epoch = Date::from_calendar_date(1969, December, 2).unwrap();
        let date = super::deser_cql_value(
            &ColumnType::Date,
            &mut ((1u32 << 31) - 30).to_be_bytes().as_ref(),
        )
        .unwrap();

        assert_eq!(date.as_date(), Some(before_epoch));

        // 2^31 + 30 when converted to time::Date is 1970-01-31
        let after_epoch = Date::from_calendar_date(1970, January, 31).unwrap();
        let date = super::deser_cql_value(
            &ColumnType::Date,
            &mut ((1u32 << 31) + 30).to_be_bytes().as_ref(),
        )
        .unwrap();

        assert_eq!(date.as_date(), Some(after_epoch));

        // 0 and u32::MAX are out of NaiveDate range, fails with an error, not panics
        assert_eq!(
            super::deser_cql_value(&ColumnType::Date, &mut 0_u32.to_be_bytes().as_ref())
                .unwrap()
                .as_date(),
            None
        );

        assert_eq!(
            super::deser_cql_value(&ColumnType::Date, &mut u32::MAX.to_be_bytes().as_ref())
                .unwrap()
                .as_date(),
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

    #[cfg(feature = "chrono")]
    #[test]
    fn test_naive_time_from_cql() {
        use chrono::NaiveTime;

        // 0 when converted to NaiveTime is 0:0:0.0
        let midnight = NaiveTime::from_hms_nano_opt(0, 0, 0, 0).unwrap();
        let time =
            super::deser_cql_value(&ColumnType::Time, &mut (0i64).to_be_bytes().as_ref()).unwrap();

        assert_eq!(time.as_naive_time(), Some(midnight));

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

        assert_eq!(time.as_naive_time(), Some(midnight));

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

        assert_eq!(time.as_naive_time(), Some(midnight));
    }

    #[cfg(feature = "time")]
    #[test]
    fn test_primitive_time_from_cql() {
        use time::Time;

        // 0 when converted to NaiveTime is 0:0:0.0
        let midnight = Time::from_hms_nano(0, 0, 0, 0).unwrap();
        let time =
            super::deser_cql_value(&ColumnType::Time, &mut (0i64).to_be_bytes().as_ref()).unwrap();

        dbg!(&time);
        assert_eq!(time.as_time(), Some(midnight));

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

        assert_eq!(time.as_time(), Some(midnight));

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

        assert_eq!(time.as_time(), Some(midnight));
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

    #[cfg(feature = "chrono")]
    #[test]
    fn test_datetime_from_cql() {
        use chrono::{NaiveDate, NaiveDateTime, NaiveTime};

        // 0 when converted to DateTime is 1970-01-01 0:00:00.00
        let unix_epoch = NaiveDateTime::from_timestamp_opt(0, 0).unwrap().and_utc();
        let date = super::deser_cql_value(&ColumnType::Timestamp, &mut 0i64.to_be_bytes().as_ref())
            .unwrap();

        assert_eq!(date.as_datetime(), Some(unix_epoch));

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

        assert_eq!(date.as_datetime(), Some(before_epoch));

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

        assert_eq!(date.as_datetime(), Some(after_epoch));

        // 0 and u32::MAX are out of NaiveDate range, fails with an error, not panics
        assert_eq!(
            super::deser_cql_value(&ColumnType::Timestamp, &mut i64::MIN.to_be_bytes().as_ref())
                .unwrap()
                .as_datetime(),
            None
        );

        assert_eq!(
            super::deser_cql_value(&ColumnType::Timestamp, &mut i64::MAX.to_be_bytes().as_ref())
                .unwrap()
                .as_datetime(),
            None
        );
    }

    #[cfg(feature = "time")]
    #[test]
    fn test_offset_datetime_from_cql() {
        use time::{Date, Month::*, OffsetDateTime, PrimitiveDateTime, Time};

        // 0 when converted to OffsetDateTime is 1970-01-01 0:00:00.00
        let unix_epoch = OffsetDateTime::from_unix_timestamp(0).unwrap();
        let date = super::deser_cql_value(&ColumnType::Timestamp, &mut 0i64.to_be_bytes().as_ref())
            .unwrap();

        assert_eq!(date.as_offset_date_time(), Some(unix_epoch));

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

        assert_eq!(date.as_offset_date_time(), Some(before_epoch));

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

        assert_eq!(date.as_offset_date_time(), Some(after_epoch));

        // 0 and u32::MAX are out of NaiveDate range, fails with an error, not panics
        assert_eq!(
            super::deser_cql_value(&ColumnType::Timestamp, &mut i64::MIN.to_be_bytes().as_ref())
                .unwrap()
                .as_offset_date_time(),
            None
        );

        assert_eq!(
            super::deser_cql_value(&ColumnType::Timestamp, &mut i64::MAX.to_be_bytes().as_ref())
                .unwrap()
                .as_offset_date_time(),
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
                    type_name: "".to_owned(),
                    keyspace: "".to_owned(),
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
