//! CQL binary protocol in-wire types.

use super::frame_errors::LowLevelDeserializationError;
use super::TryFromPrimitiveError;
use byteorder::{BigEndian, ReadBytesExt};
use bytes::Bytes;
#[cfg(test)]
use bytes::BytesMut;
use bytes::{Buf, BufMut};
use std::collections::HashMap;
use std::convert::TryFrom;
use std::convert::TryInto;
use std::net::IpAddr;
use std::net::SocketAddr;
use std::str;
use thiserror::Error;
use uuid::Uuid;

#[derive(Debug, Copy, Clone, Default, PartialEq, Eq, PartialOrd, Ord)]
#[cfg_attr(feature = "serde", derive(serde::Deserialize))]
#[cfg_attr(feature = "serde", serde(rename_all = "SCREAMING_SNAKE_CASE"))]
#[repr(u16)]
pub enum Consistency {
    Any = 0x0000,
    One = 0x0001,
    Two = 0x0002,
    Three = 0x0003,
    Quorum = 0x0004,
    All = 0x0005,
    #[default]
    LocalQuorum = 0x0006,
    EachQuorum = 0x0007,
    LocalOne = 0x000A,

    // Apparently, Consistency can be set to Serial or LocalSerial in SELECT statements
    // to make them use Paxos.
    Serial = 0x0008,
    LocalSerial = 0x0009,
}

impl TryFrom<u16> for Consistency {
    type Error = TryFromPrimitiveError<u16>;

    fn try_from(value: u16) -> Result<Self, Self::Error> {
        match value {
            0x0000 => Ok(Consistency::Any),
            0x0001 => Ok(Consistency::One),
            0x0002 => Ok(Consistency::Two),
            0x0003 => Ok(Consistency::Three),
            0x0004 => Ok(Consistency::Quorum),
            0x0005 => Ok(Consistency::All),
            0x0006 => Ok(Consistency::LocalQuorum),
            0x0007 => Ok(Consistency::EachQuorum),
            0x000A => Ok(Consistency::LocalOne),
            0x0008 => Ok(Consistency::Serial),
            0x0009 => Ok(Consistency::LocalSerial),
            _ => Err(TryFromPrimitiveError {
                enum_name: "Consistency",
                primitive: value,
            }),
        }
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord)]
#[cfg_attr(feature = "serde", derive(serde::Deserialize))]
#[cfg_attr(feature = "serde", serde(rename_all = "SCREAMING_SNAKE_CASE"))]
#[repr(i16)]
pub enum SerialConsistency {
    Serial = 0x0008,
    LocalSerial = 0x0009,
}

impl TryFrom<i16> for SerialConsistency {
    type Error = TryFromPrimitiveError<i16>;

    fn try_from(value: i16) -> Result<Self, Self::Error> {
        match value {
            0x0008 => Ok(Self::Serial),
            0x0009 => Ok(Self::LocalSerial),
            _ => Err(TryFromPrimitiveError {
                enum_name: "SerialConsistency",
                primitive: value,
            }),
        }
    }
}

impl Consistency {
    pub fn is_serial(&self) -> bool {
        matches!(self, Consistency::Serial | Consistency::LocalSerial)
    }
}

#[derive(Debug, Error)]
#[error("Expected Consistency Serial or LocalSerial, got: {0}")]
pub struct NonSerialConsistencyError(Consistency);

impl TryFrom<Consistency> for SerialConsistency {
    type Error = NonSerialConsistencyError;

    fn try_from(c: Consistency) -> Result<Self, Self::Error> {
        match c {
            Consistency::Any
            | Consistency::One
            | Consistency::Two
            | Consistency::Three
            | Consistency::Quorum
            | Consistency::All
            | Consistency::LocalQuorum
            | Consistency::EachQuorum
            | Consistency::LocalOne => Err(NonSerialConsistencyError(c)),
            Consistency::Serial => Ok(SerialConsistency::Serial),
            Consistency::LocalSerial => Ok(SerialConsistency::LocalSerial),
        }
    }
}

impl std::fmt::Display for Consistency {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl std::fmt::Display for SerialConsistency {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum RawValue<'a> {
    Null,
    Unset,
    Value(&'a [u8]),
}

impl<'a> RawValue<'a> {
    #[inline]
    pub fn as_value(&self) -> Option<&'a [u8]> {
        match self {
            RawValue::Value(v) => Some(v),
            RawValue::Null | RawValue::Unset => None,
        }
    }
}

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

pub fn read_int(buf: &mut &[u8]) -> Result<i32, std::io::Error> {
    let v = buf.read_i32::<BigEndian>()?;
    Ok(v)
}

pub fn write_int(v: i32, buf: &mut impl BufMut) {
    buf.put_i32(v);
}

pub fn read_int_length(buf: &mut &[u8]) -> Result<usize, LowLevelDeserializationError> {
    let v = read_int(buf)?;
    let v: usize = v.try_into()?;

    Ok(v)
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

pub fn read_short(buf: &mut &[u8]) -> Result<u16, std::io::Error> {
    let v = buf.read_u16::<BigEndian>()?;
    Ok(v)
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

// https://github.com/apache/cassandra/blob/trunk/doc/native_protocol_v4.spec#L208
pub fn read_bytes_opt<'a>(
    buf: &mut &'a [u8],
) -> Result<Option<&'a [u8]>, LowLevelDeserializationError> {
    let len = read_int(buf)?;
    if len < 0 {
        return Ok(None);
    }
    let len = len as usize;
    let v = Some(read_raw_bytes(len, buf)?);
    Ok(v)
}

// Same as read_bytes, but we assume the value won't be `null`
pub fn read_bytes<'a>(buf: &mut &'a [u8]) -> Result<&'a [u8], LowLevelDeserializationError> {
    let len = read_int_length(buf)?;
    let v = read_raw_bytes(len, buf)?;
    Ok(v)
}

pub fn read_value<'a>(buf: &mut &'a [u8]) -> Result<RawValue<'a>, LowLevelDeserializationError> {
    let len = read_int(buf)?;
    match len {
        -2 => Ok(RawValue::Unset),
        -1 => Ok(RawValue::Null),
        len if len >= 0 => {
            let v = read_raw_bytes(len as usize, buf)?;
            Ok(RawValue::Value(v))
        }
        len => Err(LowLevelDeserializationError::InvalidValueLength(len)),
    }
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
    assert!(err_str.contains(&format!("{}", c)));
}

pub fn read_inet(buf: &mut &[u8]) -> Result<SocketAddr, LowLevelDeserializationError> {
    let len = buf.read_u8()?;
    let ip_addr = match len {
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

fn zig_zag_encode(v: i64) -> u64 {
    ((v >> 63) ^ (v << 1)) as u64
}

fn zig_zag_decode(v: u64) -> i64 {
    ((v >> 1) as i64) ^ -((v & 1) as i64)
}

pub(crate) fn unsigned_vint_encode(v: u64, buf: &mut Vec<u8>) {
    let mut v = v;
    let mut number_of_bytes = (639 - 9 * v.leading_zeros()) >> 6;
    if number_of_bytes <= 1 {
        return buf.put_u8(v as u8);
    }

    if number_of_bytes != 9 {
        let extra_bytes = number_of_bytes - 1;
        let length_bits = !(0xff >> extra_bytes);
        v |= (length_bits as u64) << (8 * extra_bytes);
    } else {
        buf.put_u8(0xff);
        number_of_bytes -= 1;
    }
    buf.put_uint(v, number_of_bytes as usize)
}

pub(crate) fn unsigned_vint_decode(buf: &mut &[u8]) -> Result<u64, std::io::Error> {
    let first_byte = buf.read_u8()?;
    let extra_bytes = first_byte.leading_ones() as usize;

    let mut v = if extra_bytes != 8 {
        let first_byte_bits = first_byte & (0xffu8 >> extra_bytes);
        (first_byte_bits as u64) << (8 * extra_bytes)
    } else {
        0
    };

    if extra_bytes != 0 {
        v += buf.read_uint::<BigEndian>(extra_bytes)?;
    }

    Ok(v)
}

pub(crate) fn vint_encode(v: i64, buf: &mut Vec<u8>) {
    unsigned_vint_encode(zig_zag_encode(v), buf)
}

pub(crate) fn vint_decode(buf: &mut &[u8]) -> Result<i64, std::io::Error> {
    unsigned_vint_decode(buf).map(zig_zag_decode)
}

#[test]
fn zig_zag_encode_test() {
    assert_eq!(zig_zag_encode(0), 0);
    assert_eq!(zig_zag_encode(-1), 1);
    assert_eq!(zig_zag_encode(1), 2);
    assert_eq!(zig_zag_encode(-2), 3);
    assert_eq!(zig_zag_encode(2), 4);
    assert_eq!(zig_zag_encode(-3), 5);
    assert_eq!(zig_zag_encode(3), 6);
}

#[test]
fn zig_zag_decode_test() {
    assert_eq!(zig_zag_decode(0), 0);
    assert_eq!(zig_zag_decode(1), -1);
    assert_eq!(zig_zag_decode(2), 1);
    assert_eq!(zig_zag_decode(3), -2);
    assert_eq!(zig_zag_decode(4), 2);
    assert_eq!(zig_zag_decode(5), -3);
    assert_eq!(zig_zag_decode(6), 3);
}

#[test]
fn unsigned_vint_encode_and_decode_test() {
    let unsigned_vint_encoding = vec![
        (0, vec![0]),
        (1, vec![1]),
        (2, vec![2]),
        ((1 << 2) - 1, vec![3]),
        (1 << 2, vec![4]),
        ((1 << 2) + 1, vec![5]),
        ((1 << 3) - 1, vec![7]),
        (1 << 3, vec![8]),
        ((1 << 3) + 1, vec![9]),
        ((1 << 4) - 1, vec![15]),
        (1 << 4, vec![16]),
        ((1 << 4) + 1, vec![17]),
        ((1 << 5) - 1, vec![31]),
        (1 << 5, vec![32]),
        ((1 << 5) + 1, vec![33]),
        ((1 << 6) - 1, vec![63]),
        (1 << 6, vec![64]),
        ((1 << 6) + 1, vec![65]),
        ((1 << 7) - 1, vec![127]),
        (1 << 7, vec![128, 128]),
        ((1 << 7) + 1, vec![128, 129]),
        ((1 << 8) - 1, vec![128, 255]),
        (1 << 8, vec![129, 0]),
        ((1 << 8) + 1, vec![129, 1]),
        ((1 << 9) - 1, vec![129, 255]),
        (1 << 9, vec![130, 0]),
        ((1 << 9) + 1, vec![130, 1]),
        ((1 << 10) - 1, vec![131, 255]),
        (1 << 10, vec![132, 0]),
        ((1 << 10) + 1, vec![132, 1]),
        ((1 << 11) - 1, vec![135, 255]),
        (1 << 11, vec![136, 0]),
        ((1 << 11) + 1, vec![136, 1]),
        ((1 << 12) - 1, vec![143, 255]),
        (1 << 12, vec![144, 0]),
        ((1 << 12) + 1, vec![144, 1]),
        ((1 << 13) - 1, vec![159, 255]),
        (1 << 13, vec![160, 0]),
        ((1 << 13) + 1, vec![160, 1]),
        ((1 << 14) - 1, vec![191, 255]),
        (1 << 14, vec![192, 64, 0]),
        ((1 << 14) + 1, vec![192, 64, 1]),
        ((1 << 15) - 1, vec![192, 127, 255]),
        (1 << 15, vec![192, 128, 0]),
        ((1 << 15) + 1, vec![192, 128, 1]),
        ((1 << 16) - 1, vec![192, 255, 255]),
        (1 << 16, vec![193, 0, 0]),
        ((1 << 16) + 1, vec![193, 0, 1]),
        ((1 << 17) - 1, vec![193, 255, 255]),
        (1 << 17, vec![194, 0, 0]),
        ((1 << 17) + 1, vec![194, 0, 1]),
        ((1 << 18) - 1, vec![195, 255, 255]),
        (1 << 18, vec![196, 0, 0]),
        ((1 << 18) + 1, vec![196, 0, 1]),
        ((1 << 19) - 1, vec![199, 255, 255]),
        (1 << 19, vec![200, 0, 0]),
        ((1 << 19) + 1, vec![200, 0, 1]),
        ((1 << 20) - 1, vec![207, 255, 255]),
        (1 << 20, vec![208, 0, 0]),
        ((1 << 20) + 1, vec![208, 0, 1]),
        ((1 << 21) - 1, vec![223, 255, 255]),
        (1 << 21, vec![224, 32, 0, 0]),
        ((1 << 21) + 1, vec![224, 32, 0, 1]),
        ((1 << 22) - 1, vec![224, 63, 255, 255]),
        (1 << 22, vec![224, 64, 0, 0]),
        ((1 << 22) + 1, vec![224, 64, 0, 1]),
        ((1 << 23) - 1, vec![224, 127, 255, 255]),
        (1 << 23, vec![224, 128, 0, 0]),
        ((1 << 23) + 1, vec![224, 128, 0, 1]),
        ((1 << 24) - 1, vec![224, 255, 255, 255]),
        (1 << 24, vec![225, 0, 0, 0]),
        ((1 << 24) + 1, vec![225, 0, 0, 1]),
        ((1 << 25) - 1, vec![225, 255, 255, 255]),
        (1 << 25, vec![226, 0, 0, 0]),
        ((1 << 25) + 1, vec![226, 0, 0, 1]),
        ((1 << 26) - 1, vec![227, 255, 255, 255]),
        (1 << 26, vec![228, 0, 0, 0]),
        ((1 << 26) + 1, vec![228, 0, 0, 1]),
        ((1 << 27) - 1, vec![231, 255, 255, 255]),
        (1 << 27, vec![232, 0, 0, 0]),
        ((1 << 27) + 1, vec![232, 0, 0, 1]),
        ((1 << 28) - 1, vec![239, 255, 255, 255]),
        (1 << 28, vec![240, 16, 0, 0, 0]),
        ((1 << 28) + 1, vec![240, 16, 0, 0, 1]),
        ((1 << 29) - 1, vec![240, 31, 255, 255, 255]),
        (1 << 29, vec![240, 32, 0, 0, 0]),
        ((1 << 29) + 1, vec![240, 32, 0, 0, 1]),
        ((1 << 30) - 1, vec![240, 63, 255, 255, 255]),
        (1 << 30, vec![240, 64, 0, 0, 0]),
        ((1 << 30) + 1, vec![240, 64, 0, 0, 1]),
        ((1 << 31) - 1, vec![240, 127, 255, 255, 255]),
        (1 << 31, vec![240, 128, 0, 0, 0]),
        ((1 << 31) + 1, vec![240, 128, 0, 0, 1]),
        ((1 << 32) - 1, vec![240, 255, 255, 255, 255]),
        (1 << 32, vec![241, 0, 0, 0, 0]),
        ((1 << 32) + 1, vec![241, 0, 0, 0, 1]),
        ((1 << 33) - 1, vec![241, 255, 255, 255, 255]),
        (1 << 33, vec![242, 0, 0, 0, 0]),
        ((1 << 33) + 1, vec![242, 0, 0, 0, 1]),
        ((1 << 34) - 1, vec![243, 255, 255, 255, 255]),
        (1 << 34, vec![244, 0, 0, 0, 0]),
        ((1 << 34) + 1, vec![244, 0, 0, 0, 1]),
        ((1 << 35) - 1, vec![247, 255, 255, 255, 255]),
        (1 << 35, vec![248, 8, 0, 0, 0, 0]),
        ((1 << 35) + 1, vec![248, 8, 0, 0, 0, 1]),
        ((1 << 36) - 1, vec![248, 15, 255, 255, 255, 255]),
        (1 << 36, vec![248, 16, 0, 0, 0, 0]),
        ((1 << 36) + 1, vec![248, 16, 0, 0, 0, 1]),
        ((1 << 37) - 1, vec![248, 31, 255, 255, 255, 255]),
        (1 << 37, vec![248, 32, 0, 0, 0, 0]),
        ((1 << 37) + 1, vec![248, 32, 0, 0, 0, 1]),
        ((1 << 38) - 1, vec![248, 63, 255, 255, 255, 255]),
        (1 << 38, vec![248, 64, 0, 0, 0, 0]),
        ((1 << 38) + 1, vec![248, 64, 0, 0, 0, 1]),
        ((1 << 39) - 1, vec![248, 127, 255, 255, 255, 255]),
        (1 << 39, vec![248, 128, 0, 0, 0, 0]),
        ((1 << 39) + 1, vec![248, 128, 0, 0, 0, 1]),
        ((1 << 40) - 1, vec![248, 255, 255, 255, 255, 255]),
        (1 << 40, vec![249, 0, 0, 0, 0, 0]),
        ((1 << 40) + 1, vec![249, 0, 0, 0, 0, 1]),
        ((1 << 41) - 1, vec![249, 255, 255, 255, 255, 255]),
        (1 << 41, vec![250, 0, 0, 0, 0, 0]),
        ((1 << 41) + 1, vec![250, 0, 0, 0, 0, 1]),
        ((1 << 42) - 1, vec![251, 255, 255, 255, 255, 255]),
        (1 << 42, vec![252, 4, 0, 0, 0, 0, 0]),
        ((1 << 42) + 1, vec![252, 4, 0, 0, 0, 0, 1]),
        ((1 << 43) - 1, vec![252, 7, 255, 255, 255, 255, 255]),
        (1 << 43, vec![252, 8, 0, 0, 0, 0, 0]),
        ((1 << 43) + 1, vec![252, 8, 0, 0, 0, 0, 1]),
        ((1 << 44) - 1, vec![252, 15, 255, 255, 255, 255, 255]),
        (1 << 44, vec![252, 16, 0, 0, 0, 0, 0]),
        ((1 << 44) + 1, vec![252, 16, 0, 0, 0, 0, 1]),
        ((1 << 45) - 1, vec![252, 31, 255, 255, 255, 255, 255]),
        (1 << 45, vec![252, 32, 0, 0, 0, 0, 0]),
        ((1 << 45) + 1, vec![252, 32, 0, 0, 0, 0, 1]),
        ((1 << 46) - 1, vec![252, 63, 255, 255, 255, 255, 255]),
        (1 << 46, vec![252, 64, 0, 0, 0, 0, 0]),
        ((1 << 46) + 1, vec![252, 64, 0, 0, 0, 0, 1]),
        ((1 << 47) - 1, vec![252, 127, 255, 255, 255, 255, 255]),
        (1 << 47, vec![252, 128, 0, 0, 0, 0, 0]),
        ((1 << 47) + 1, vec![252, 128, 0, 0, 0, 0, 1]),
        ((1 << 48) - 1, vec![252, 255, 255, 255, 255, 255, 255]),
        (1 << 48, vec![253, 0, 0, 0, 0, 0, 0]),
        ((1 << 48) + 1, vec![253, 0, 0, 0, 0, 0, 1]),
        ((1 << 49) - 1, vec![253, 255, 255, 255, 255, 255, 255]),
        (1 << 49, vec![254, 2, 0, 0, 0, 0, 0, 0]),
        ((1 << 49) + 1, vec![254, 2, 0, 0, 0, 0, 0, 1]),
        ((1 << 50) - 1, vec![254, 3, 255, 255, 255, 255, 255, 255]),
        (1 << 50, vec![254, 4, 0, 0, 0, 0, 0, 0]),
        ((1 << 50) + 1, vec![254, 4, 0, 0, 0, 0, 0, 1]),
        ((1 << 51) - 1, vec![254, 7, 255, 255, 255, 255, 255, 255]),
        (1 << 51, vec![254, 8, 0, 0, 0, 0, 0, 0]),
        ((1 << 51) + 1, vec![254, 8, 0, 0, 0, 0, 0, 1]),
        ((1 << 52) - 1, vec![254, 15, 255, 255, 255, 255, 255, 255]),
        (1 << 52, vec![254, 16, 0, 0, 0, 0, 0, 0]),
        ((1 << 52) + 1, vec![254, 16, 0, 0, 0, 0, 0, 1]),
        ((1 << 53) - 1, vec![254, 31, 255, 255, 255, 255, 255, 255]),
        (1 << 53, vec![254, 32, 0, 0, 0, 0, 0, 0]),
        ((1 << 53) + 1, vec![254, 32, 0, 0, 0, 0, 0, 1]),
        ((1 << 54) - 1, vec![254, 63, 255, 255, 255, 255, 255, 255]),
        (1 << 54, vec![254, 64, 0, 0, 0, 0, 0, 0]),
        ((1 << 54) + 1, vec![254, 64, 0, 0, 0, 0, 0, 1]),
        ((1 << 55) - 1, vec![254, 127, 255, 255, 255, 255, 255, 255]),
        (1 << 55, vec![254, 128, 0, 0, 0, 0, 0, 0]),
        ((1 << 55) + 1, vec![254, 128, 0, 0, 0, 0, 0, 1]),
        ((1 << 56) - 1, vec![254, 255, 255, 255, 255, 255, 255, 255]),
        (1 << 56, vec![255, 1, 0, 0, 0, 0, 0, 0, 0]),
        ((1 << 56) + 1, vec![255, 1, 0, 0, 0, 0, 0, 0, 1]),
        (
            (1 << 57) - 1,
            vec![255, 1, 255, 255, 255, 255, 255, 255, 255],
        ),
        (1 << 57, vec![255, 2, 0, 0, 0, 0, 0, 0, 0]),
        ((1 << 57) + 1, vec![255, 2, 0, 0, 0, 0, 0, 0, 1]),
        (
            (1 << 58) - 1,
            vec![255, 3, 255, 255, 255, 255, 255, 255, 255],
        ),
        (1 << 58, vec![255, 4, 0, 0, 0, 0, 0, 0, 0]),
        ((1 << 58) + 1, vec![255, 4, 0, 0, 0, 0, 0, 0, 1]),
        (
            (1 << 59) - 1,
            vec![255, 7, 255, 255, 255, 255, 255, 255, 255],
        ),
        (1 << 59, vec![255, 8, 0, 0, 0, 0, 0, 0, 0]),
        ((1 << 59) + 1, vec![255, 8, 0, 0, 0, 0, 0, 0, 1]),
        (
            (1 << 60) - 1,
            vec![255, 15, 255, 255, 255, 255, 255, 255, 255],
        ),
        (1 << 60, vec![255, 16, 0, 0, 0, 0, 0, 0, 0]),
        ((1 << 60) + 1, vec![255, 16, 0, 0, 0, 0, 0, 0, 1]),
        (
            (1 << 61) - 1,
            vec![255, 31, 255, 255, 255, 255, 255, 255, 255],
        ),
        (1 << 61, vec![255, 32, 0, 0, 0, 0, 0, 0, 0]),
        ((1 << 61) + 1, vec![255, 32, 0, 0, 0, 0, 0, 0, 1]),
        (
            (1 << 62) - 1,
            vec![255, 63, 255, 255, 255, 255, 255, 255, 255],
        ),
        (1 << 62, vec![255, 64, 0, 0, 0, 0, 0, 0, 0]),
        ((1 << 62) + 1, vec![255, 64, 0, 0, 0, 0, 0, 0, 1]),
        (
            (1 << 63) - 1,
            vec![255, 127, 255, 255, 255, 255, 255, 255, 255],
        ),
        (1 << 63, vec![255, 128, 0, 0, 0, 0, 0, 0, 0]),
        ((1 << 63) + 1, vec![255, 128, 0, 0, 0, 0, 0, 0, 1]),
        (u64::MAX, vec![255, 255, 255, 255, 255, 255, 255, 255, 255]),
    ];

    let mut buf = Vec::new();

    for (v, result) in unsigned_vint_encoding.into_iter() {
        unsigned_vint_encode(v, &mut buf);
        assert_eq!(buf, result);
        let decoded_v = unsigned_vint_decode(&mut buf.as_slice()).unwrap();
        assert_eq!(v, decoded_v);
        buf.clear();
    }
}

#[test]
fn vint_encode_and_decode_test() {
    let mut buf: Vec<u8> = Vec::with_capacity(128);

    let mut check = |n: i64| {
        vint_encode(n, &mut buf);
        assert_eq!(vint_decode(&mut buf.as_slice()).unwrap(), n);
        buf.clear();
    };

    for i in 0..63 {
        check((1 << i) - 1);
        check(1 - (1 << i));
        check(1 << i);
        check(-(1 << i));
        check((1 << i) + 1);
        check(-1 - (1 << i));
    }
    check(i64::MAX);
    check(-i64::MAX);
    check(i64::MIN)
}
