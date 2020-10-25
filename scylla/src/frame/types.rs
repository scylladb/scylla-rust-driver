//! CQL binary protocol in-wire types.

use anyhow::Result;
use byteorder::{BigEndian, ReadBytesExt};
use bytes::BufMut;
use std::collections::HashMap;
use std::str;

fn read_raw_bytes<'a>(count: usize, buf: &mut &'a [u8]) -> Result<&'a [u8]> {
    if buf.len() < count {
        return Err(anyhow!(
            "not enough bytes in buffer: expected {}, was {}",
            count,
            buf.len()
        ));
    }
    let (ret, rest) = buf.split_at(count);
    *buf = rest;
    Ok(ret)
}

pub fn read_int(buf: &mut &[u8]) -> Result<i32> {
    let v = buf.read_i32::<BigEndian>()?;
    Ok(v)
}

pub fn write_int(v: i32, buf: &mut impl BufMut) {
    buf.put_i32(v);
}

fn read_int_length(buf: &mut &[u8]) -> Result<usize> {
    let v = read_int(buf)?;
    if v < 0 {
        return Err(anyhow!("invalid length of type `int`: {}", v));
    }
    Ok(v as usize)
}

fn write_int_length(v: usize, buf: &mut impl BufMut) -> Result<()> {
    if v > i32::MAX as usize {
        return Err(anyhow!("length too big to be encoded as `int`: {}", v));
    }
    write_int(v as i32, buf);
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

pub fn read_long(buf: &mut &[u8]) -> Result<i64> {
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

pub fn read_short(buf: &mut &[u8]) -> Result<i16> {
    let v = buf.read_i16::<BigEndian>()?;
    Ok(v)
}

pub fn write_short(v: i16, buf: &mut impl BufMut) {
    buf.put_i16(v);
}

fn read_short_length(buf: &mut &[u8]) -> Result<usize> {
    let v = read_short(buf)?;
    if v < 0 {
        return Err(anyhow!("invalid length of type `short`: {}", v));
    }
    Ok(v as usize)
}

fn write_short_length(v: usize, buf: &mut impl BufMut) -> Result<()> {
    if v > i16::MAX as usize {
        return Err(anyhow!("length too big to be encoded as `short`: {}", v));
    }
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
pub fn read_bytes_opt<'a>(buf: &mut &'a [u8]) -> Result<Option<&'a [u8]>> {
    let len = read_int(buf)?;
    if len < 0 {
        return Ok(None);
    }
    let len = len as usize;
    let v = Some(read_raw_bytes(len, buf)?);
    Ok(v)
}

// Same as read_bytes, but we assume the value won't be `null`
pub fn read_bytes<'a>(buf: &mut &'a [u8]) -> Result<&'a [u8]> {
    let len = read_int_length(buf)?;
    let v = read_raw_bytes(len, buf)?;
    Ok(v)
}

pub fn write_bytes(v: &[u8], buf: &mut impl BufMut) -> Result<()> {
    write_int_length(v.len(), buf)?;
    buf.put_slice(v);
    Ok(())
}

pub fn write_short_bytes(v: &[u8], buf: &mut impl BufMut) -> Result<()> {
    write_short_length(v.len(), buf)?;
    buf.put_slice(v);
    Ok(())
}

pub fn read_string<'a>(buf: &mut &'a [u8]) -> Result<&'a str> {
    let len = read_short_length(buf)?;
    let raw = read_raw_bytes(len, buf)?;
    let v = str::from_utf8(raw)?;
    Ok(v)
}

pub fn write_string(v: &str, buf: &mut impl BufMut) -> Result<()> {
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

pub fn read_long_string<'a>(buf: &mut &'a [u8]) -> Result<&'a str> {
    let len = read_int_length(buf)?;
    let raw = read_raw_bytes(len, buf)?;
    let v = str::from_utf8(raw)?;
    Ok(v)
}

pub fn write_long_string(v: &str, buf: &mut impl BufMut) -> Result<()> {
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

pub fn read_string_map(buf: &mut &[u8]) -> Result<HashMap<String, String>> {
    let mut v = HashMap::new();
    let len = read_short_length(buf)?;
    for _ in 0..len {
        let key = read_string(buf)?.to_owned();
        let val = read_string(buf)?.to_owned();
        v.insert(key, val);
    }
    Ok(v)
}

pub fn write_string_map(v: &HashMap<String, String>, buf: &mut impl BufMut) -> Result<()> {
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

pub fn read_string_list(buf: &mut &[u8]) -> Result<Vec<String>> {
    let mut v = Vec::new();
    let len = read_short_length(buf)?;
    for _ in 0..len {
        v.push(read_string(buf)?.to_owned());
    }
    Ok(v)
}

pub fn write_string_list(v: &[String], buf: &mut impl BufMut) -> Result<()> {
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

pub fn read_string_multimap(buf: &mut &[u8]) -> Result<HashMap<String, Vec<String>>> {
    let mut v = HashMap::new();
    let len = read_short_length(buf)?;
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
) -> Result<()> {
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
