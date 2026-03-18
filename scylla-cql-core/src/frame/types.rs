//! CQL binary protocol in-wire types.

use byteorder::{BigEndian, ReadBytesExt};

use crate::frame::frame_errors::LowLevelDeserializationError;

use super::TryFromPrimitiveError;
use std::convert::TryFrom;
use thiserror::Error;

/// A setting that defines a successful write or read by the number of cluster replicas
/// that acknowledge the write or respond to the read request, respectively.
/// See [ScyllaDB docs](https://docs.scylladb.com/manual/stable/cql/consistency.html)
/// for more detailed description and guidelines.
#[derive(Debug, Copy, Clone, Default, PartialEq, Eq, PartialOrd, Ord)]
#[cfg_attr(feature = "serde", derive(serde::Deserialize))]
#[cfg_attr(feature = "serde", serde(rename_all = "SCREAMING_SNAKE_CASE"))]
#[repr(u16)]
pub enum Consistency {
    /// **Write-only**. Closest replica, as determined by the
    /// [Snitch](https://docs.scylladb.com/manual/stable/reference/glossary.html#term-Snitch),
    /// must respond. If all replica nodes are down, write succeeds after a hinted handoff.
    /// Provides low latency, guarantees writes never fail.
    Any = 0x0000,
    /// The closest replica as determined by the Snitch must respond. Consistency requirements
    /// are not too strict.
    One = 0x0001,
    /// The closest two replicas as determined by the Snitch must respond.
    Two = 0x0002,
    /// The closest three replicas as determined by the Snitch must respond.
    Three = 0x0003,
    /// A simple majority of all replicas across all datacenters must respond.
    /// This CL allows for some level of failure.
    Quorum = 0x0004,
    /// _All_ replicas in the cluster must respond. May cause performance issues.
    All = 0x0005,
    /// Same as [QUORUM](Consistency::Quorum), but confined to the same datacenter as the coordinator.
    #[default]
    LocalQuorum = 0x0006,
    /// **Write-only**. A simple majority in each datacenter must respond.
    EachQuorum = 0x0007,
    /// Same as [ONE](Consistency::One), but confined to the local datacenter.
    LocalOne = 0x000A,

    // Apparently, Consistency can be set to Serial or LocalSerial in SELECT statements
    // to make them use Paxos.
    /// **Read-only**. Returns results with the most recent data. Including uncommitted in-flight LWTs.
    Serial = 0x0008,
    /// **Read-only**. Same as [SERIAL](Consistency::Serial), but confined to a local datacenter.
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
            _ => Err(TryFromPrimitiveError::new("Consistency", value)),
        }
    }
}

/// [Consistency] for Lightweight Transactions (LWTs).
///
/// [SerialConsistency] sets the consistency level for the serial phase of conditional updates.
/// This option will be ignored for anything else that a conditional update/insert.
#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord)]
#[cfg_attr(feature = "serde", derive(serde::Deserialize))]
#[cfg_attr(feature = "serde", serde(rename_all = "SCREAMING_SNAKE_CASE"))]
#[repr(i16)]
pub enum SerialConsistency {
    /// Guarantees linearizable semantics across the whole cluster.
    Serial = 0x0008,
    /// Guarantees linearizable semantics in a local datacenter.
    LocalSerial = 0x0009,
}

impl TryFrom<i16> for SerialConsistency {
    type Error = TryFromPrimitiveError<i16>;

    fn try_from(value: i16) -> Result<Self, Self::Error> {
        match value {
            0x0008 => Ok(Self::Serial),
            0x0009 => Ok(Self::LocalSerial),
            _ => Err(TryFromPrimitiveError::new("SerialConsistency", value)),
        }
    }
}

impl Consistency {
    /// Checks if the consistency is a serial consistency.
    pub fn is_serial(&self) -> bool {
        matches!(self, Consistency::Serial | Consistency::LocalSerial)
    }
}

/// Error returned when a serial consistency what expected, yet got another kind of consistency.
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
        write!(f, "{self:?}")
    }
}

impl std::fmt::Display for SerialConsistency {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
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

pub fn read_short(buf: &mut &[u8]) -> Result<u16, std::io::Error> {
    let v = buf.read_u16::<BigEndian>()?;
    Ok(v)
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
