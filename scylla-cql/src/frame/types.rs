//! CQL binary protocol in-wire types.

use super::frame_errors::LowLevelDeserializationError;
use byteorder::{BigEndian, ReadBytesExt};
use bytes::BufMut;
use bytes::Bytes;
#[cfg(test)]
use bytes::BytesMut;
use std::collections::HashMap;
use std::convert::TryFrom;
use std::convert::TryInto;
use std::net::IpAddr;
use std::net::SocketAddr;
use std::str;
use uuid::Uuid;

// Re-export stable public types from scylla-cql-core.
pub use scylla_cql_core::frame::types::{
    Consistency, NonSerialConsistencyError, RawValue, SerialConsistency, read_bytes_opt, read_int,
    read_int_length, read_short, read_value,
};

pub(crate) fn read_raw_bytes<'a>(
    count: usize,
    buf: &mut &'a [u8],
) -> Result<&'a [u8], LowLevelDeserializationError> {
    if buf.len() < count {
        return Err(LowLevelDeserializationError::TooFewBytesReceived {
            expected: count,
            received: buf.len(),
        });
    }
    let (ret, rest) = buf.split_at(count);
    *buf = rest;
    Ok(ret)
}

pub fn write_int(v: i32, buf: &mut impl BufMut) {
    buf.put_i32(v);
}

pub(crate) fn write_int_length(
    v: usize,
    buf: &mut impl BufMut,
) -> Result<(), std::num::TryFromIntError> {
    let v: i32 = v.try_into()?;

    write_int(v, buf);
    Ok(())
}

#[test]
fn type_int() {
    let vals = [i32::MIN, -1, 0, 1, i32::MAX];
    for val in vals.iter() {
        let mut buf = Vec::new();
        write_int(*val, &mut buf);
        assert_eq!(read_int(&mut &buf[..]).unwrap(), *val);
    }
}

pub fn read_long(buf: &mut &[u8]) -> Result<i64, std::io::Error> {
    let v = buf.read_i64::<BigEndian>()?;
    Ok(v)
}

pub fn write_long(v: i64, buf: &mut impl BufMut) {
    buf.put_i64(v);
}

#[test]
fn type_long() {
    let vals = [i64::MIN, -1, 0, 1, i64::MAX];
    for val in vals.iter() {
        let mut buf = Vec::new();
        write_long(*val, &mut buf);
        assert_eq!(read_long(&mut &buf[..]).unwrap(), *val);
    }
}

pub fn write_short(v: u16, buf: &mut impl BufMut) {
    buf.put_u16(v);
}

pub(crate) fn read_short_length(buf: &mut &[u8]) -> Result<usize, std::io::Error> {
    let v = read_short(buf)?;
    let v: usize = v.into();
    Ok(v)
}

pub(crate) fn write_short_length(
    v: usize,
    buf: &mut impl BufMut,
) -> Result<(), std::num::TryFromIntError> {
    let v: u16 = v.try_into()?;
    write_short(v, buf);
    Ok(())
}

#[test]
fn type_short() {
    let vals: [u16; 3] = [0, 1, u16::MAX];
    for val in vals.iter() {
        let mut buf = Vec::new();
        write_short(*val, &mut buf);
        assert_eq!(read_short(&mut &buf[..]).unwrap(), *val);
    }
}

// Same as read_bytes, but we assume the value won't be `null`
pub fn read_bytes<'a>(buf: &mut &'a [u8]) -> Result<&'a [u8], LowLevelDeserializationError> {
    let len = read_int_length(buf)?;
    let v = read_raw_bytes(len, buf)?;
    Ok(v)
}

pub fn read_short_bytes<'a>(buf: &mut &'a [u8]) -> Result<&'a [u8], LowLevelDeserializationError> {
    let len = read_short_length(buf)?;
    let v = read_raw_bytes(len, buf)?;
    Ok(v)
}

pub fn write_bytes(v: &[u8], buf: &mut impl BufMut) -> Result<(), std::num::TryFromIntError> {
    write_int_length(v.len(), buf)?;
    buf.put_slice(v);
    Ok(())
}

pub fn write_bytes_opt(
    v: Option<impl AsRef<[u8]>>,
    buf: &mut impl BufMut,
) -> Result<(), std::num::TryFromIntError> {
    match v {
        Some(bytes) => {
            write_int_length(bytes.as_ref().len(), buf)?;
            buf.put_slice(bytes.as_ref());
        }
        None => write_int(-1, buf),
    }

    Ok(())
}

pub fn write_short_bytes(v: &[u8], buf: &mut impl BufMut) -> Result<(), std::num::TryFromIntError> {
    write_short_length(v.len(), buf)?;
    buf.put_slice(v);
    Ok(())
}

pub fn read_bytes_map(
    buf: &mut &[u8],
) -> Result<HashMap<String, Bytes>, LowLevelDeserializationError> {
    let len = read_short_length(buf)?;
    let mut v = HashMap::with_capacity(len);
    for _ in 0..len {
        let key = read_string(buf)?.to_owned();
        let val = Bytes::copy_from_slice(read_bytes(buf)?);
        v.insert(key, val);
    }
    Ok(v)
}

pub fn write_bytes_map<B>(
    v: &HashMap<String, B>,
    buf: &mut impl BufMut,
) -> Result<(), std::num::TryFromIntError>
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
    val.insert("".to_owned(), Bytes::new());
    val.insert("EXTENSION1".to_owned(), Bytes::from_static(&[1, 2, 3]));
    val.insert("EXTENSION2".to_owned(), Bytes::from_static(&[4, 5, 6]));
    let mut buf = BytesMut::new();
    write_bytes_map(&val, &mut buf).unwrap();
    assert_eq!(read_bytes_map(&mut &*buf).unwrap(), val);
}

pub fn read_string<'a>(buf: &mut &'a [u8]) -> Result<&'a str, LowLevelDeserializationError> {
    let len = read_short_length(buf)?;
    let raw = read_raw_bytes(len, buf)?;
    let v = str::from_utf8(raw)?;
    Ok(v)
}

pub fn write_string(v: &str, buf: &mut impl BufMut) -> Result<(), std::num::TryFromIntError> {
    let raw = v.as_bytes();
    write_short_length(v.len(), buf)?;
    buf.put_slice(raw);
    Ok(())
}

#[test]
fn type_string() {
    let vals = [String::from(""), String::from("hello, world!")];
    for val in vals.iter() {
        let mut buf = Vec::new();
        write_string(val, &mut buf).unwrap();
        assert_eq!(read_string(&mut &buf[..]).unwrap(), *val);
    }
}

pub fn read_long_string<'a>(buf: &mut &'a [u8]) -> Result<&'a str, LowLevelDeserializationError> {
    let len = read_int_length(buf)?;
    let raw = read_raw_bytes(len, buf)?;
    let v = str::from_utf8(raw)?;
    Ok(v)
}

pub fn write_long_string(v: &str, buf: &mut impl BufMut) -> Result<(), std::num::TryFromIntError> {
    let raw = v.as_bytes();
    let len = raw.len();
    write_int_length(len, buf)?;
    buf.put_slice(raw);
    Ok(())
}

#[test]
fn type_long_string() {
    let vals = [String::from(""), String::from("hello, world!")];
    for val in vals.iter() {
        let mut buf = Vec::new();
        write_long_string(val, &mut buf).unwrap();
        assert_eq!(read_long_string(&mut &buf[..]).unwrap(), *val);
    }
}

pub fn read_string_map(
    buf: &mut &[u8],
) -> Result<HashMap<String, String>, LowLevelDeserializationError> {
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
    v: &HashMap<impl AsRef<str>, impl AsRef<str>>,
    buf: &mut impl BufMut,
) -> Result<(), std::num::TryFromIntError> {
    let len = v.len();
    write_short_length(len, buf)?;
    for (key, val) in v.iter() {
        write_string(key.as_ref(), buf)?;
        write_string(val.as_ref(), buf)?;
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

pub fn read_string_list(buf: &mut &[u8]) -> Result<Vec<String>, LowLevelDeserializationError> {
    let len = read_short_length(buf)?;
    let mut v = Vec::with_capacity(len);
    for _ in 0..len {
        v.push(read_string(buf)?.to_owned());
    }
    Ok(v)
}

pub fn write_string_list(
    v: &[String],
    buf: &mut impl BufMut,
) -> Result<(), std::num::TryFromIntError> {
    let len = v.len();
    write_short_length(len, buf)?;
    for v in v.iter() {
        write_string(v, buf)?;
    }
    Ok(())
}

#[test]
fn type_string_list() {
    let val = vec![
        "".to_owned(),
        "CQL_VERSION".to_owned(),
        "THROW_ON_OVERLOAD".to_owned(),
    ];

    let mut buf = Vec::new();
    write_string_list(&val, &mut buf).unwrap();
    assert_eq!(read_string_list(&mut &buf[..]).unwrap(), val);
}

pub fn read_string_multimap(
    buf: &mut &[u8],
) -> Result<HashMap<String, Vec<String>>, LowLevelDeserializationError> {
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
) -> Result<(), std::num::TryFromIntError> {
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

pub fn read_uuid(buf: &mut &[u8]) -> Result<Uuid, LowLevelDeserializationError> {
    let raw = read_raw_bytes(16, buf)?;

    // It's safe to unwrap here because the conversion only fails
    // if the argument slice's length does not match, which
    // `read_raw_bytes` prevents.
    let raw_array: &[u8; 16] = raw.try_into().unwrap();

    Ok(Uuid::from_bytes(*raw_array))
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

pub fn read_consistency(buf: &mut &[u8]) -> Result<Consistency, LowLevelDeserializationError> {
    let raw = read_short(buf)?;
    Consistency::try_from(raw).map_err(LowLevelDeserializationError::UnknownConsistency)
}

pub fn write_consistency(c: Consistency, buf: &mut impl BufMut) {
    write_short(c as u16, buf);
}

pub fn write_serial_consistency(c: SerialConsistency, buf: &mut impl BufMut) {
    write_short(c as u16, buf);
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
    assert!(err_str.contains(&format!("{c}")));
}

pub fn read_inet(buf: &mut &[u8]) -> Result<SocketAddr, LowLevelDeserializationError> {
    let len = buf.read_u8()?;
    let ip_addr = match len {
        4 => {
            let ip_bytes = read_raw_bytes(4, buf)?;
            IpAddr::from(<[u8; 4]>::try_from(ip_bytes)?)
        }
        16 => {
            let ip_bytes = read_raw_bytes(16, buf)?;
            IpAddr::from(<[u8; 16]>::try_from(ip_bytes)?)
        }
        v => return Err(LowLevelDeserializationError::InvalidInetLength(v)),
    };
    let port = read_int(buf)?;

    Ok(SocketAddr::new(ip_addr, port as u16))
}

pub fn write_inet(addr: SocketAddr, buf: &mut impl BufMut) {
    match addr.ip() {
        IpAddr::V4(v4) => {
            buf.put_u8(4);
            buf.put_slice(&v4.octets());
        }
        IpAddr::V6(v6) => {
            buf.put_u8(16);
            buf.put_slice(&v6.octets());
        }
    }

    write_int(addr.port() as i32, buf)
}

#[test]
fn type_inet() {
    use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};

    let iv4 = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 1234);
    let iv6 = SocketAddr::new(IpAddr::V6(Ipv6Addr::new(0, 0, 0, 0, 0, 0, 0, 1)), 2345);
    let mut buf = Vec::new();

    write_inet(iv4, &mut buf);
    let read_iv4 = read_inet(&mut &*buf).unwrap();
    assert_eq!(iv4, read_iv4);
    buf.clear();

    write_inet(iv6, &mut buf);
    let read_iv6 = read_inet(&mut &*buf).unwrap();
    assert_eq!(iv6, read_iv6);
}
