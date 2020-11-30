use super::cql_to_rust::FromRowError;
use crate::cql_to_rust::FromRow;
use crate::frame::frame_errors::ParseError;
use crate::frame::types;
use byteorder::{BigEndian, ReadBytesExt};
use bytes::{Buf, Bytes};
use std::collections::HashMap;
use std::convert::TryFrom;
use std::convert::TryInto;
use std::net::IpAddr;
use std::result::Result as StdResult;
use std::str;

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
    Int,
    BigInt,
    Text,
    Inet,
    Set(Box<ColumnType>),
    UserDefinedType {
        type_name: String,
        keyspace: String,
        field_types: Vec<(String, ColumnType)>,
    },
    // TODO
}

#[derive(Debug)]
pub enum CQLValue {
    Ascii(String),
    Int(i32),
    BigInt(i64),
    Text(String),
    Inet(IpAddr),
    Set(Vec<CQLValue>),
    UserDefinedType {
        keyspace: String,
        type_name: String,
        fields: HashMap<String, Option<CQLValue>>,
    }, // TODO
}

impl CQLValue {
    pub fn as_ascii(&self) -> Option<&String> {
        match self {
            Self::Ascii(s) => Some(&s),
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

    pub fn as_set(&self) -> Option<&Vec<CQLValue>> {
        match self {
            Self::Set(s) => Some(&s),
            _ => None,
        }
    }

    pub fn into_vec(self) -> Option<Vec<CQLValue>> {
        match self {
            Self::Set(s) => Some(s),
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
        0x0009 => Int,
        0x000D => Text,
        0x0010 => Inet,
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
        Int => {
            if buf.len() != 4 {
                return Err(ParseError::BadData(format!(
                    "Buffer length should be 4 not {}",
                    buf.len()
                )));
            }
            CQLValue::Int(buf.read_i32::<BigEndian>()?)
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
        Set(typ) => {
            let len: usize = types::read_int(buf)?.try_into()?;

            let mut res = Vec::with_capacity(len);
            for _ in 0..len {
                // TODO: is `null` allowed as set element? Should we use read_bytes_opt?
                let mut b = types::read_bytes(buf)?;
                res.push(deser_cql_value(typ, &mut b)?);
            }
            CQLValue::Set(res)
        }
        UserDefinedType {
            type_name,
            keyspace,
            field_types,
        } => {
            let mut fields: HashMap<String, Option<CQLValue>> =
                HashMap::with_capacity(field_types.len());

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
