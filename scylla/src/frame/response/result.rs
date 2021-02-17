use crate::cql_to_rust::{FromRow, FromRowError};
use crate::frame::{frame_errors::ParseError, types};
use byteorder::{BigEndian, ReadBytesExt};
use bytes::{Buf, Bytes};
use std::{
    collections::BTreeMap,
    convert::{TryFrom, TryInto},
    net::IpAddr,
    result::Result as StdResult,
    str,
};

#[derive(Debug)]
pub struct SetKeyspace {
    // TODO
}

#[derive(Debug)]
pub struct Prepared {
    pub id: Bytes,
    pub prepared_metadata: PreparedMetadata,
    result_metadata: ResultMetadata,
}

#[derive(Debug)]
pub struct SchemaChange {
    // TODO
}

#[derive(Clone, Debug)]
struct TableSpec {
    ks_name: String,
    table_name: String,
}

#[derive(Debug, Clone)]
enum ColumnType {
    Ascii,
    Boolean,
    Blob,
    Date,
    Double,
    Float,
    Timestamp,
    Int,
    BigInt,
    Text,
    Inet,
    List(Box<ColumnType>),
    Map(Box<ColumnType>, Box<ColumnType>),
    Set(Box<ColumnType>),
    UserDefinedType {
        type_name: String,
        keyspace: String,
        field_types: Vec<(String, ColumnType)>,
    },
    Tuple(Vec<ColumnType>),
}

#[derive(Debug, PartialEq)]
pub enum CQLValue {
    Ascii(String),
    Boolean(bool),
    Blob(Vec<u8>),
    Date(u32),
    Double(f64),
    Float(f32),
    Timestamp(i64),
    Int(i32),
    BigInt(i64),
    Text(String),
    Inet(IpAddr),
    List(Vec<CQLValue>),
    Map(Vec<(CQLValue, CQLValue)>),
    Set(Vec<CQLValue>),
    UserDefinedType {
        keyspace: String,
        type_name: String,
        fields: BTreeMap<String, Option<CQLValue>>,
    },
    Tuple(Vec<CQLValue>),
}

impl CQLValue {
    pub fn as_ascii(&self) -> Option<&String> {
        match self {
            Self::Ascii(s) => Some(&s),
            _ => None,
        }
    }

    pub fn as_timestamp(&self) -> Option<i64> {
        match self {
            Self::Timestamp(i) => Some(*i),
            _ => None,
        }
    }

    pub fn as_date(&self) -> Option<u32> {
        match self {
            Self::Date(i) => Some(*i),
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

    pub fn as_text(&self) -> Option<&String> {
        match self {
            Self::Text(s) => Some(&s),
            _ => None,
        }
    }

    pub fn as_blob(&self) -> Option<&Vec<u8>> {
        match self {
            Self::Blob(v) => Some(&v),
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

    pub fn as_inet(&self) -> Option<IpAddr> {
        match self {
            Self::Inet(a) => Some(*a),
            _ => None,
        }
    }

    pub fn as_list(&self) -> Option<&Vec<CQLValue>> {
        match self {
            Self::List(s) => Some(&s),
            _ => None,
        }
    }

    pub fn as_set(&self) -> Option<&Vec<CQLValue>> {
        match self {
            Self::Set(s) => Some(&s),
            _ => None,
        }
    }

    pub fn into_vec(self) -> Option<Vec<CQLValue>> {
        match self {
            Self::List(s) => Some(s),
            Self::Set(s) => Some(s),
            _ => None,
        }
    }

    pub fn into_pair_vec(self) -> Option<Vec<(CQLValue, CQLValue)>> {
        match self {
            Self::Map(s) => Some(s),
            _ => None,
        }
    }

    // TODO
}

#[derive(Debug, Clone)]
pub struct ColumnSpec {
    table_spec: TableSpec,
    name: String,
    typ: ColumnType,
}

#[derive(Debug, Default)]
pub struct ResultMetadata {
    col_count: usize,
    pub paging_state: Option<Bytes>,
    col_specs: Vec<ColumnSpec>,
}

#[derive(Debug, Clone)]
pub struct PreparedMetadata {
    pub col_count: usize,
    pub pk_indexes: Vec<u16>,
    pub col_specs: Vec<ColumnSpec>,
}

#[derive(Debug, Default)]
pub struct Row {
    pub columns: Vec<Option<CQLValue>>,
}

impl Row {
    /// Allows converting Row into tuple of rust types or custom struct deriving FromRow
    pub fn into_typed<RowT: FromRow>(self) -> StdResult<RowT, FromRowError> {
        RowT::from_row(self)
    }
}

#[derive(Debug, Default)]
pub struct Rows {
    pub metadata: ResultMetadata,
    rows_count: usize,
    pub rows: Vec<Row>,
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
        0x0001 => Ascii,
        0x0002 => BigInt,
        0x0003 => Blob,
        0x0004 => Boolean,
        0x0005 => Date,
        0x0007 => Double,
        0x0008 => Float,
        0x0009 => Int,
        0x000B => Timestamp,
        0x000D => Text,
        0x0010 => Inet,
        0x0020 => List(Box::new(deser_type(buf)?)),
        0x0021 => Map(Box::new(deser_type(buf)?), Box::new(deser_type(buf)?)),
        0x0022 => Set(Box::new(deser_type(buf)?)),
        0x0030 => {
            let keyspace_name: String = types::read_string(buf)?.to_string();
            let type_name: String = types::read_string(buf)?.to_string();
            let fields_size: usize = types::read_short(buf)?.try_into()?;

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
            let len: usize = types::read_short(buf)?.try_into()?;
            let mut types = Vec::with_capacity(len as usize);
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

    let col_count = types::read_int_length(buf)? as usize;

    let pk_count: usize = types::read_int(buf)?.try_into()?;

    let mut pk_indexes = Vec::with_capacity(pk_count);
    for _ in 0..pk_count {
        pk_indexes.push(types::read_short(buf)? as u16);
    }

    let global_table_spec = if global_tables_spec {
        Some(deser_table_spec(buf)?)
    } else {
        None
    };

    let col_specs = deser_col_specs(buf, &global_table_spec, col_count)?;

    Ok(PreparedMetadata {
        col_count,
        pk_indexes,
        col_specs,
    })
}

fn deser_cql_value(typ: &ColumnType, buf: &mut &[u8]) -> StdResult<CQLValue, ParseError> {
    use ColumnType::*;
    Ok(match typ {
        Ascii => {
            if !buf.is_ascii() {
                return Err(ParseError::BadData("String is not ascii!".to_string()));
            }
            CQLValue::Ascii(str::from_utf8(buf)?.to_owned())
        }
        Boolean => {
            if buf.len() != 1 {
                return Err(ParseError::BadData(format!(
                    "Buffer length should be 1 not {}",
                    buf.len()
                )));
            }
            CQLValue::Boolean(buf[0] != 0x00)
        }
        Blob => CQLValue::Blob(buf.to_vec()),
        Date => {
            if buf.len() != 4 {
                return Err(ParseError::BadData(format!(
                    "Buffer length should be 4 not {}",
                    buf.len()
                )));
            }
            CQLValue::Date(buf.read_u32::<BigEndian>()?)
        }
        Double => {
            if buf.len() != 8 {
                return Err(ParseError::BadData(format!(
                    "Buffer length should be 8 not {}",
                    buf.len()
                )));
            }
            CQLValue::Double(buf.read_f64::<BigEndian>()?)
        }
        Float => {
            if buf.len() != 4 {
                return Err(ParseError::BadData(format!(
                    "Buffer length should be 4 not {}",
                    buf.len()
                )));
            }
            CQLValue::Float(buf.read_f32::<BigEndian>()?)
        }
        Int => {
            if buf.len() != 4 {
                return Err(ParseError::BadData(format!(
                    "Buffer length should be 4 not {}",
                    buf.len()
                )));
            }
            CQLValue::Int(buf.read_i32::<BigEndian>()?)
        }
        Timestamp => {
            if buf.len() != 8 {
                return Err(ParseError::BadData(format!(
                    "Buffer length should be 8 not {}",
                    buf.len()
                )));
            }
            CQLValue::Timestamp(buf.read_i64::<BigEndian>()?)
        }
        BigInt => {
            if buf.len() != 8 {
                return Err(ParseError::BadData(format!(
                    "Buffer length should be 8 not {}",
                    buf.len()
                )));
            }
            CQLValue::BigInt(buf.read_i64::<BigEndian>()?)
        }
        Text => CQLValue::Text(str::from_utf8(buf)?.to_owned()),
        Inet => CQLValue::Inet(match buf.len() {
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
                return Err(ParseError::BadData(format!(
                    "Invalid inet bytes length: {}",
                    v
                )));
            }
        }),
        List(type_name) => {
            let len: usize = types::read_int(buf)?.try_into()?;
            let mut res = Vec::with_capacity(len);
            for _ in 0..len {
                let mut b = types::read_bytes(buf)?;
                res.push(deser_cql_value(type_name, &mut b)?);
            }
            CQLValue::List(res)
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
            CQLValue::Map(res)
        }
        Set(type_name) => {
            let len: usize = types::read_int(buf)?.try_into()?;
            let mut res = Vec::with_capacity(len);
            for _ in 0..len {
                // TODO: is `null` allowed as set element? Should we use read_bytes_opt?
                let mut b = types::read_bytes(buf)?;
                res.push(deser_cql_value(type_name, &mut b)?);
            }
            CQLValue::Set(res)
        }
        UserDefinedType {
            type_name,
            keyspace,
            field_types,
        } => {
            let mut fields: BTreeMap<String, Option<CQLValue>> = BTreeMap::new();

            for (field_name, field_type) in field_types {
                let mut field_value: Option<CQLValue> = None;
                if let Some(mut field_val_bytes) = types::read_bytes_opt(buf)? {
                    field_value = Some(deser_cql_value(&field_type, &mut field_val_bytes)?);
                }

                fields.insert(field_name.clone(), field_value);
            }

            CQLValue::UserDefinedType {
                keyspace: keyspace.clone(),
                type_name: type_name.clone(),
                fields,
            }
        }
        Tuple(type_names) => {
            let mut res = Vec::with_capacity(type_names.len());
            for type_name in type_names {
                let mut b = types::read_bytes(buf)?;
                res.push(deser_cql_value(type_name, &mut b)?);
            }
            CQLValue::Tuple(res)
        }
    })
}

fn deser_rows(buf: &mut &[u8]) -> StdResult<Rows, ParseError> {
    let metadata = deser_result_metadata(buf)?;

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
    })
}

#[allow(clippy::unnecessary_unwrap)]
fn deser_set_keyspace(_buf: &mut &[u8]) -> StdResult<SetKeyspace, ParseError> {
    Ok(SetKeyspace {}) // TODO
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

#[allow(clippy::unnecessary_unwrap)]
fn deser_schema_change(_buf: &mut &[u8]) -> StdResult<SchemaChange, ParseError> {
    Ok(SchemaChange {}) // TODO
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
            return Err(ParseError::BadData(format!(
                "Unknown query result id: {}",
                k
            )))
        }
    })
}

#[cfg(test)]
mod tests {
    use crate as scylla;
    use scylla::frame::response::result::CQLValue;

    #[test]
    fn test_list_from_cql() {
        let mut my_vec: Vec<CQLValue> = Vec::new();

        my_vec.push(CQLValue::Int(20));
        my_vec.push(CQLValue::Int(2));
        my_vec.push(CQLValue::Int(13));

        let cql: CQLValue = CQLValue::List(my_vec);
        let decoded = cql.into_vec().unwrap();

        assert_eq!(decoded[0], CQLValue::Int(20));
        assert_eq!(decoded[1], CQLValue::Int(2));
        assert_eq!(decoded[2], CQLValue::Int(13));
    }

    #[test]
    fn test_set_from_cql() {
        let mut my_vec: Vec<CQLValue> = Vec::new();

        my_vec.push(CQLValue::Int(20));
        my_vec.push(CQLValue::Int(2));
        my_vec.push(CQLValue::Int(13));

        let cql: CQLValue = CQLValue::Set(my_vec);
        let decoded = cql.as_set().unwrap();

        assert_eq!(decoded[0], CQLValue::Int(20));
        assert_eq!(decoded[1], CQLValue::Int(2));
        assert_eq!(decoded[2], CQLValue::Int(13));
    }

    #[test]
    fn test_map_from_cql() {
        let mut my_vec: Vec<(CQLValue, CQLValue)> = Vec::new();

        my_vec.push((CQLValue::Int(20), CQLValue::Int(21)));
        my_vec.push((CQLValue::Int(2), CQLValue::Int(3)));

        let cql: CQLValue = CQLValue::Map(my_vec);

        let decoded = cql.into_pair_vec().unwrap();

        assert_eq!(CQLValue::Int(20), decoded[0].0);
        assert_eq!(CQLValue::Int(21), decoded[0].1);

        assert_eq!(CQLValue::Int(2), decoded[1].0);
        assert_eq!(CQLValue::Int(3), decoded[1].1);
    }
}
