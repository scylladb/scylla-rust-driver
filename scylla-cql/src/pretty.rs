use std::borrow::Borrow;
use std::fmt::{Display, LowerHex, UpperHex};

use chrono_04::TimeZone;
use itertools::Itertools;

use crate::value::{CqlDate, CqlTime, CqlTimestamp, CqlValue};

pub(crate) struct HexBytes<'a>(pub(crate) &'a [u8]);

impl LowerHex for HexBytes<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for b in self.0 {
            write!(f, "{:02x}", b)?;
        }
        Ok(())
    }
}

impl UpperHex for HexBytes<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for b in self.0 {
            write!(f, "{:02X}", b)?;
        }
        Ok(())
    }
}

// Displays a CqlValue. The syntax should resemble the CQL literal syntax
// (but no guarantee is given that it's always the same).
pub struct CqlValueDisplayer<C>(pub C);

impl<C> Display for CqlValueDisplayer<C>
where
    C: Borrow<CqlValue>,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.0.borrow() {
            // Scalar types
            CqlValue::Ascii(a) => write!(f, "{}", CqlStringLiteralDisplayer(a))?,
            CqlValue::Text(t) => write!(f, "{}", CqlStringLiteralDisplayer(t))?,
            CqlValue::Blob(b) => write!(f, "0x{:x}", HexBytes(b))?,
            CqlValue::Empty => write!(f, "0x")?,
            CqlValue::Decimal(d) => {
                let (bytes, scale) = d.as_signed_be_bytes_slice_and_exponent();
                write!(
                    f,
                    "blobAsDecimal(0x{:x}{:x})",
                    HexBytes(&scale.to_be_bytes()),
                    HexBytes(bytes)
                )?
            }
            CqlValue::Float(fl) => write!(f, "{}", fl)?,
            CqlValue::Double(d) => write!(f, "{}", d)?,
            CqlValue::Boolean(b) => write!(f, "{}", b)?,
            CqlValue::Int(i) => write!(f, "{}", i)?,
            CqlValue::BigInt(bi) => write!(f, "{}", bi)?,
            CqlValue::Inet(i) => write!(f, "'{}'", i)?,
            CqlValue::SmallInt(si) => write!(f, "{}", si)?,
            CqlValue::TinyInt(ti) => write!(f, "{}", ti)?,
            CqlValue::Varint(vi) => write!(
                f,
                "blobAsVarint(0x{:x})",
                HexBytes(vi.as_signed_bytes_be_slice())
            )?,
            CqlValue::Counter(c) => write!(f, "{}", c.0)?,
            CqlValue::Date(CqlDate(d)) => {
                // This is basically a copy of the code used in `impl TryInto<NaiveDate> for CqlDate` impl
                // in scylla-cql. We can't call this impl because it is behind chrono feature in scylla-cql.

                // date_days is u32 then converted to i64 then we subtract 2^31;
                // Max value is 2^31, min value is -2^31. Both values can safely fit in chrono::Duration, this call won't panic
                let days_since_epoch =
                    chrono_04::Duration::try_days(*d as i64 - (1 << 31)).unwrap();

                // TODO: chrono::NaiveDate does not handle the whole range
                // supported by the `date` datatype
                match chrono_04::NaiveDate::from_ymd_opt(1970, 1, 1)
                    .unwrap()
                    .checked_add_signed(days_since_epoch)
                {
                    Some(d) => write!(f, "'{}'", d)?,
                    None => f.write_str("<date out of representable range>")?,
                }
            }
            CqlValue::Duration(d) => write!(f, "{}mo{}d{}ns", d.months, d.days, d.nanoseconds)?,
            CqlValue::Time(CqlTime(t)) => {
                write!(
                    f,
                    "'{:02}:{:02}:{:02}.{:09}'",
                    t / 3_600_000_000_000,
                    t / 60_000_000_000 % 60,
                    t / 1_000_000_000 % 60,
                    t % 1_000_000_000,
                )?;
            }
            CqlValue::Timestamp(CqlTimestamp(t)) => {
                match chrono_04::Utc.timestamp_millis_opt(*t) {
                    chrono_04::LocalResult::Ambiguous(_, _) => unreachable!(), // not supposed to happen with timestamp_millis_opt
                    chrono_04::LocalResult::Single(d) => {
                        write!(f, "{}", d.format("'%Y-%m-%d %H:%M:%S%.3f%z'"))?
                    }
                    chrono_04::LocalResult::None => {
                        f.write_str("<timestamp out of representable range>")?
                    }
                }
            }
            CqlValue::Timeuuid(t) => write!(f, "{}", t)?,
            CqlValue::Uuid(u) => write!(f, "{}", u)?,

            // Compound types
            CqlValue::Tuple(t) => {
                f.write_str("(")?;
                t.iter()
                    .map(|x| MaybeNullDisplayer(x.as_ref().map(CqlValueDisplayer)))
                    .format(",")
                    .fmt(f)?;
                f.write_str(")")?;
            }
            CqlValue::List(v) => {
                f.write_str("[")?;
                v.iter().map(CqlValueDisplayer).format(",").fmt(f)?;
                f.write_str("]")?;
            }
            CqlValue::Set(v) => {
                f.write_str("{")?;
                v.iter().map(CqlValueDisplayer).format(",").fmt(f)?;
                f.write_str("}")?;
            }
            CqlValue::Map(m) => {
                f.write_str("{")?;
                m.iter()
                    .map(|(k, v)| PairDisplayer(CqlValueDisplayer(k), CqlValueDisplayer(v)))
                    .format(",")
                    .fmt(f)?;
                f.write_str("}")?;
            }
            CqlValue::UserDefinedType {
                keyspace: _,
                name: _,
                fields,
            } => {
                f.write_str("{")?;
                fields
                    .iter()
                    .map(|(k, v)| {
                        PairDisplayer(k, MaybeNullDisplayer(v.as_ref().map(CqlValueDisplayer)))
                    })
                    .format(",")
                    .fmt(f)?;
                f.write_str("}")?;
            }
        }
        Ok(())
    }
}

pub(crate) struct CqlStringLiteralDisplayer<'a>(&'a str);

impl Display for CqlStringLiteralDisplayer<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // CQL string literals use single quotes. The only character that
        // needs escaping is singular quote, and escaping is done by repeating
        // the quote character.
        f.write_str("'")?;
        let mut first = true;
        for part in self.0.split('\'') {
            if first {
                first = false;
            } else {
                f.write_str("''")?;
            }
            f.write_str(part)?;
        }
        f.write_str("'")?;
        Ok(())
    }
}

pub(crate) struct PairDisplayer<K, V>(K, V);

impl<K, V> Display for PairDisplayer<K, V>
where
    K: Display,
    V: Display,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}:{}", self.0, self.1)
    }
}

pub(crate) struct MaybeNullDisplayer<T>(Option<T>);

impl<T> Display for MaybeNullDisplayer<T>
where
    T: Display,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.0 {
            None => write!(f, "null")?,
            Some(v) => write!(f, "{}", v)?,
        }
        Ok(())
    }
}
