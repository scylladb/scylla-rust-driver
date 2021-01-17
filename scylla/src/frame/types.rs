//! CQL binary protocol in-wire types.

use super::frame_errors::ParseError;
use byteorder::{BigEndian, ReadBytesExt};
use bytes::BufMut;
use num_enum::TryFromPrimitive;
use std::collections::HashMap;
use std::convert::TryFrom;
use std::convert::TryInto;
use std::str;
use uuid::Uuid;

#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord, TryFromPrimitive)]
#[repr(i16)]
pub enum Consistency {
    Any = 0x0000,
    One = 0x0001,
    Two = 0x0002,
    Three = 0x0003,
    Quorum = 0x0004,
    All = 0x0005,
    LocalQuorum = 0x0006,
    EachQuorum = 0x0007,
    Serial = 0x0008,
    LocalSerial = 0x0009,
    LocalOne = 0x000A,
}

impl Default for Consistency {
    fn default() -> Self {
        Consistency::Quorum
    }
}

impl From<std::num::TryFromIntError> for ParseError {
    fn from(_err: std::num::TryFromIntError) -> Self {
        ParseError::BadData("Integer conversion out of range".to_string())
    }
}

impl From<std::str::Utf8Error> for ParseError {
    fn from(_err: std::str::Utf8Error) -> Self {
        ParseError::BadData("UTF8 serialization failed".to_string())
    }
}

impl From<std::array::TryFromSliceError> for ParseError {
    fn from(_err: std::array::TryFromSliceError) -> Self {
        ParseError::BadData("array try from slice failed".to_string())
    }
}

fn read_raw_bytes<'a>(count: usize, buf: &mut &'a [u8]) -> Result<&'a [u8], ParseError> {
    if buf.len() < count {
        return Err(ParseError::BadData(format!(
            "Not enough bytes! expected: {} received: {}",
            count,
            buf.len(),
        )));
    }
    let (ret, rest) = buf.split_at(count);
    *buf = rest;
    Ok(ret)
}

pub fn read_int(buf: &mut &[u8]) -> Result<i32, ParseError> {
    let v = buf.read_i32::<BigEndian>()?;
    Ok(v)
}

pub fn write_int(v: i32, buf: &mut impl BufMut) {
    buf.put_i32(v);
}

pub fn read_int_length(buf: &mut &[u8]) -> Result<usize, ParseError> {
    let v = read_int(buf)?;
    let v: usize = v.try_into()?;

    Ok(v)
}

fn write_int_length(v: usize, buf: &mut impl BufMut) -> Result<(), ParseError> {
    let v: i32 = v.try_into()?;

    write_int(v, buf);
    Ok(())
}

#[test]
fn type_int() {
    let vals = vec![i32::MIN, -1, 0, 1, i32::MAX];
    for val in vals.iter() {
        let mut buf = Vec::new();
        write_int(*val, &mut buf);
        assert_eq!(read_int(&mut &buf[..]).unwrap(), *val);
    }
}

pub fn read_long(buf: &mut &[u8]) -> Result<i64, ParseError> {
    let v = buf.read_i64::<BigEndian>()?;
    Ok(v)
}

pub fn write_long(v: i64, buf: &mut impl BufMut) {
    buf.put_i64(v);
}

#[test]
fn type_long() {
    let vals = vec![i64::MIN, -1, 0, 1, i64::MAX];
    for val in vals.iter() {
        let mut buf = Vec::new();
        write_long(*val, &mut buf);
        assert_eq!(read_long(&mut &buf[..]).unwrap(), *val);
    }
}

pub fn read_short(buf: &mut &[u8]) -> Result<i16, ParseError> {
    let v = buf.read_i16::<BigEndian>()?;
    Ok(v)
}

pub fn write_short(v: i16, buf: &mut impl BufMut) {
    buf.put_i16(v);
}

fn read_short_length(buf: &mut &[u8]) -> Result<usize, ParseError> {
    let v = read_short(buf)?;
    let v: usize = v.try_into()?;
    Ok(v)
}

fn write_short_length(v: usize, buf: &mut impl BufMut) -> Result<(), ParseError> {
    let v: i16 = v.try_into()?;
    write_short(v as i16, buf);
    Ok(())
}

#[test]
fn type_short() {
    let vals = vec![i16::MIN, -1, 0, 1, i16::MAX];
    for val in vals.iter() {
        let mut buf = Vec::new();
        write_short(*val, &mut buf);
        assert_eq!(read_short(&mut &buf[..]).unwrap(), *val);
    }
}

// https://github.com/apache/cassandra/blob/trunk/doc/native_protocol_v4.spec#L208
pub fn read_bytes_opt<'a>(buf: &mut &'a [u8]) -> Result<Option<&'a [u8]>, ParseError> {
    let len = read_int(buf)?;
    if len < 0 {
        return Ok(None);
    }
    let len = len as usize;
    let v = Some(read_raw_bytes(len, buf)?);
    Ok(v)
}

// Same as read_bytes, but we assume the value won't be `null`
pub fn read_bytes<'a>(buf: &mut &'a [u8]) -> Result<&'a [u8], ParseError> {
    let len = read_int_length(buf)?;
    let v = read_raw_bytes(len, buf)?;
    Ok(v)
}

pub fn write_bytes(v: &[u8], buf: &mut impl BufMut) -> Result<(), ParseError> {
    write_int_length(v.len(), buf)?;
    buf.put_slice(v);
    Ok(())
}

pub fn write_short_bytes(v: &[u8], buf: &mut impl BufMut) -> Result<(), ParseError> {
    write_short_length(v.len(), buf)?;
    buf.put_slice(v);
    Ok(())
}

pub fn read_bytes_map(buf: &mut &[u8]) -> Result<HashMap<String, Vec<u8>>, ParseError> {
    let len = read_short_length(buf)?;
    let mut v = HashMap::with_capacity(len);
    for _ in 0..len {
        let key = read_string(buf)?.to_owned();
        let val = read_bytes(buf)?.to_owned();
        v.insert(key, val);
    }
    Ok(v)
}

pub fn write_bytes_map<B>(v: &HashMap<String, B>, buf: &mut impl BufMut) -> Result<(), ParseError>
where
    B: AsRef<[u8]>,
{
    let len = v.len();
    write_short_length(len, buf)?;
    for (key, val) in v.iter() {
        write_string(key, buf)?;
        write_bytes(val.as_ref(), buf)?;
    }
    Ok(())
}

#[test]
fn type_bytes_map() {
    let mut val = HashMap::new();
    val.insert("".to_owned(), vec![]);
    val.insert("EXTENSION1".to_owned(), vec![1, 2, 3]);
    val.insert("EXTENSION2".to_owned(), vec![4, 5, 6]);
    let mut buf = Vec::new();
    write_bytes_map(&val, &mut buf).unwrap();
    assert_eq!(read_bytes_map(&mut &*buf).unwrap(), val);
}

pub fn read_string<'a>(buf: &mut &'a [u8]) -> Result<&'a str, ParseError> {
    let len = read_short_length(buf)?;
    let raw = read_raw_bytes(len, buf)?;
    let v = str::from_utf8(raw)?;
    Ok(v)
}

pub fn write_string(v: &str, buf: &mut impl BufMut) -> Result<(), ParseError> {
    let raw = v.as_bytes();
    write_short_length(v.len(), buf)?;
    buf.put_slice(raw);
    Ok(())
}

#[test]
fn type_string() {
    let vals = vec![String::from(""), String::from("hello, world!")];
    for val in vals.iter() {
        let mut buf = Vec::new();
        write_string(val, &mut buf).unwrap();
        assert_eq!(read_string(&mut &buf[..]).unwrap(), *val);
    }
}

pub fn read_long_string<'a>(buf: &mut &'a [u8]) -> Result<&'a str, ParseError> {
    let len = read_int_length(buf)?;
    let raw = read_raw_bytes(len, buf)?;
    let v = str::from_utf8(raw)?;
    Ok(v)
}

pub fn write_long_string(v: &str, buf: &mut impl BufMut) -> Result<(), ParseError> {
    let raw = v.as_bytes();
    let len = raw.len();
    write_int_length(len, buf)?;
    buf.put_slice(raw);
    Ok(())
}

#[test]
fn type_long_string() {
    let vals = vec![String::from(""), String::from("hello, world!")];
    for val in vals.iter() {
        let mut buf = Vec::new();
        write_long_string(val, &mut buf).unwrap();
        assert_eq!(read_long_string(&mut &buf[..]).unwrap(), *val);
    }
}

pub fn read_string_map(buf: &mut &[u8]) -> Result<HashMap<String, String>, ParseError> {
    let len = read_short_length(buf)?;
    let mut v = HashMap::with_capacity(len);
    for _ in 0..len {
        let key = read_string(buf)?.to_owned();
        let val = read_string(buf)?.to_owned();
        v.insert(key, val);
    }
    Ok(v)
}

pub fn write_string_map(
    v: &HashMap<String, String>,
    buf: &mut impl BufMut,
) -> Result<(), ParseError> {
    let len = v.len();
    write_short_length(len, buf)?;
    for (key, val) in v.iter() {
        write_string(key, buf)?;
        write_string(val, buf)?;
    }
    Ok(())
}

#[test]
fn type_string_map() {
    let mut val = HashMap::new();
    val.insert(String::from(""), String::from(""));
    val.insert(String::from("CQL_VERSION"), String::from("3.0.0"));
    val.insert(String::from("THROW_ON_OVERLOAD"), String::from(""));
    let mut buf = Vec::new();
    write_string_map(&val, &mut buf).unwrap();
    assert_eq!(read_string_map(&mut &buf[..]).unwrap(), val);
}

pub fn read_string_list(buf: &mut &[u8]) -> Result<Vec<String>, ParseError> {
    let len = read_short_length(buf)?;
    let mut v = Vec::with_capacity(len);
    for _ in 0..len {
        v.push(read_string(buf)?.to_owned());
    }
    Ok(v)
}

pub fn write_string_list(v: &[String], buf: &mut impl BufMut) -> Result<(), ParseError> {
    let len = v.len();
    write_short_length(len, buf)?;
    for v in v.iter() {
        write_string(v, buf)?;
    }
    Ok(())
}

#[test]
fn type_string_list() {
    let mut val = Vec::new();
    val.push("".to_owned());
    val.push("CQL_VERSION".to_owned());
    val.push("THROW_ON_OVERLOAD".to_owned());
    let mut buf = Vec::new();
    write_string_list(&val, &mut buf).unwrap();
    assert_eq!(read_string_list(&mut &buf[..]).unwrap(), val);
}

pub fn read_string_multimap(buf: &mut &[u8]) -> Result<HashMap<String, Vec<String>>, ParseError> {
    let len = read_short_length(buf)?;
    let mut v = HashMap::with_capacity(len);
    for _ in 0..len {
        let key = read_string(buf)?.to_owned();
        let val = read_string_list(buf)?;
        v.insert(key, val);
    }
    Ok(v)
}

pub fn write_string_multimap(
    v: &HashMap<String, Vec<String>>,
    buf: &mut impl BufMut,
) -> Result<(), ParseError> {
    let len = v.len();
    write_short_length(len, buf)?;
    for (key, val) in v.iter() {
        write_string(key, buf)?;
        write_string_list(val, buf)?;
    }
    Ok(())
}

#[test]
fn type_string_multimap() {
    let mut val = HashMap::new();
    val.insert(String::from(""), vec![String::from("")]);
    val.insert(
        String::from("versions"),
        vec![String::from("3.0.0"), String::from("4.2.0")],
    );
    val.insert(String::from("empty"), vec![]);
    let mut buf = Vec::new();
    write_string_multimap(&val, &mut buf).unwrap();
    assert_eq!(read_string_multimap(&mut &buf[..]).unwrap(), val);
}

pub fn read_uuid(buf: &mut &[u8]) -> Result<Uuid, ParseError> {
    let raw = read_raw_bytes(16, buf)?;

    // It's safe to unwrap here because Uuid::from_slice only fails
    // if the argument slice's length is not 16.
    Ok(Uuid::from_slice(raw).unwrap())
}

pub fn write_uuid(uuid: &Uuid, buf: &mut impl BufMut) {
    buf.put_slice(&uuid.as_bytes()[..]);
}

#[test]
fn type_uuid() {
    let u = Uuid::parse_str("f3b4958c-52a1-11e7-802a-010203040506").unwrap();
    let mut buf = Vec::new();
    write_uuid(&u, &mut buf);
    let u2 = read_uuid(&mut &*buf).unwrap();
    assert_eq!(u, u2);
}

pub fn read_consistency(buf: &mut &[u8]) -> Result<Consistency, ParseError> {
    let raw = read_short(buf)?;
    let parsed = Consistency::try_from(raw)
        .map_err(|_| ParseError::BadData(format!("unknown consistency: {}", raw)))?;
    Ok(parsed)
}

pub fn write_consistency(c: Consistency, buf: &mut impl BufMut) {
    write_short(c as i16, buf);
}

#[test]
fn type_consistency() {
    let c = Consistency::Quorum;
    let mut buf = Vec::new();
    write_consistency(c, &mut buf);
    let c2 = read_consistency(&mut &*buf).unwrap();
    assert_eq!(c, c2);

    let c: i16 = 0x1234;
    buf.clear();
    buf.put_i16(c);
    let c_result = read_consistency(&mut &*buf);
    assert!(c_result.is_err());

    // Check that the error message contains information about the invalid value
    let err_str = format!("{}", c_result.unwrap_err());
    assert!(err_str.contains(&format!("{}", c)));
}
