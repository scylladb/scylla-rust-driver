//! CQL binary protocol in-wire types.

use anyhow::Result;
use byteorder::{BigEndian, ReadBytesExt};
use bytes::{Buf, BufMut};
use std::collections::HashMap;
use std::str;

pub fn read_int(buf: &mut &[u8]) -> Result<i32> {
    let v = buf.read_i32::<BigEndian>()?;
    Ok(v)
}

pub fn write_int(v: i32, buf: &mut impl BufMut) {
    buf.put_i32(v);
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
    let v = Some(&buf[0..len]);
    buf.advance(len);
    Ok(v)
}

// Same as read_bytes, but we assume the value won't be `null`
pub fn read_bytes<'a>(buf: &mut &'a [u8]) -> Result<&'a [u8]> {
    let len = read_int(buf)?;
    if len < 0 {
        return Err(anyhow!(
            "unexpected length when deserializing `bytes` value: {}",
            len
        ));
    }
    let len = len as usize;
    let v = &buf[0..len];
    buf.advance(len);
    Ok(v)
}

pub fn write_short_bytes<'a>(v: &[u8], buf: &mut impl BufMut) -> Result<()> {
    let len = v.len();
    if len > i16::MAX as usize {
        return Err(anyhow!("Byte slice is too long for 16-bits: {} bytes", len));
    }
    write_short(len as i16, buf);
    buf.put_slice(v);
    Ok(())
}

pub fn read_string<'a>(buf: &mut &'a [u8]) -> Result<&'a str> {
    let len = read_short(buf)? as usize;
    if buf.len() < len {
        return Err(anyhow!(
            "Not enough bytes in buffer: expected {}, was {}",
            len,
            buf.len()
        ));
    }
    let raw = &buf[0..len];
    let v = str::from_utf8(raw)?;
    buf.advance(len as usize);
    Ok(v)
}

pub fn write_string(v: &str, buf: &mut impl BufMut) -> Result<()> {
    let raw = v.as_bytes();
    let len = raw.len();
    if len > i16::MAX as usize {
        return Err(anyhow!("String is too long for 16-bits: {} bytes", len));
    }
    write_short(len as i16, buf);
    buf.put_slice(&raw[0..len]);
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
    let len = read_int(buf)? as usize;
    if buf.len() < len {
        return Err(anyhow!(
            "Not enough bytes in buffer: expected {}, was {}",
            len,
            buf.len()
        ));
    }
    let raw = &buf[0..len];
    let v = str::from_utf8(raw)?;
    buf.advance(len as usize);
    Ok(v)
}

pub fn write_long_string(v: &str, buf: &mut impl BufMut) -> Result<()> {
    let raw = v.as_bytes();
    let len = raw.len();
    if len > i32::MAX as usize {
        return Err(anyhow!("String is too long for 32-bits: {} bytes", len));
    }
    write_int(len as i32, buf);
    buf.put_slice(&raw[0..len]);
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
    let len = read_short(buf)?;
    for _ in 0..len {
        let key = read_string(buf)?.to_owned();
        let val = read_string(buf)?.to_owned();
        v.insert(key, val);
    }
    Ok(v)
}

pub fn write_string_map(v: &HashMap<String, String>, buf: &mut impl BufMut) -> Result<()> {
    let len = v.len();
    if v.len() > i16::MAX as usize {
        return Err(anyhow!(
            "String map has too many entries for 16-bits: {}",
            len
        ));
    }
    write_short(len as i16, buf);
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
    let len = read_short(buf)?;
    for _ in 0..len {
        v.push(read_string(buf)?.to_owned());
    }
    Ok(v)
}

pub fn write_string_list(v: &[String], buf: &mut impl BufMut) -> Result<()> {
    let len = v.len();
    if v.len() > i16::MAX as usize {
        return Err(anyhow!(
            "String list has too many entries for 16-bits: {}",
            len
        ));
    }
    write_short(len as i16, buf);
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
    let len = read_short(buf)?;
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
    if v.len() > i16::MAX as usize {
        return Err(anyhow!(
            "String map has too many entries for 16-bits: {}",
            len
        ));
    }
    write_short(len as i16, buf);
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
