use chrono::{LocalResult, TimeZone, Utc};
use scylla_cql::frame::response::result::CqlValue;

use std::borrow::Borrow;
use std::fmt::{Display, LowerHex, UpperHex};

pub(crate) struct HexBytes<'a>(pub(crate) &'a [u8]);

impl<'a> LowerHex for HexBytes<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for b in self.0 {
            write!(f, "{:02x}", b)?;
        }
        Ok(())
    }
}

impl<'a> UpperHex for HexBytes<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for b in self.0 {
            write!(f, "{:02X}", b)?;
        }
        Ok(())
    }
}

// Displays a CqlValue. The syntax should resemble the CQL literal syntax
// (but no guarantee is given that it's always the same).
pub(crate) struct CqlValueDisplayer<C>(pub(crate) C);

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
            CqlValue::Decimal(d) => write!(f, "{}", d)?,
            CqlValue::Float(fl) => write!(f, "{}", fl)?,
            CqlValue::Double(d) => write!(f, "{}", d)?,
            CqlValue::Boolean(b) => write!(f, "{}", b)?,
            CqlValue::Int(i) => write!(f, "{}", i)?,
            CqlValue::BigInt(bi) => write!(f, "{}", bi)?,
            CqlValue::Inet(i) => write!(f, "'{}'", i)?,
            CqlValue::SmallInt(si) => write!(f, "{}", si)?,
            CqlValue::TinyInt(ti) => write!(f, "{}", ti)?,
            CqlValue::Varint(vi) => write!(f, "{}", vi)?,
            CqlValue::Counter(c) => write!(f, "{}", c.0)?,
            CqlValue::Date(d) => {
                let days_since_epoch = chrono::Duration::days(*d as i64 - (1 << 31));

                // TODO: chrono::NaiveDate does not handle the whole range
                // supported by the `date` datatype
                match chrono::NaiveDate::from_ymd_opt(1970, 1, 1)
                    .unwrap()
                    .checked_add_signed(days_since_epoch)
                {
                    Some(d) => write!(f, "'{}'", d)?,
                    None => f.write_str("<date out of representable range>")?,
                }
            }
            CqlValue::Duration(d) => write!(f, "{}mo{}d{}ns", d.months, d.days, d.nanoseconds)?,
            CqlValue::Time(t) => {
                write!(
                    f,
                    "'{:02}:{:02}:{:02}.{:09}'",
                    t / 3_600_000_000_000,
                    t / 60_000_000_000 % 60,
                    t / 1_000_000_000 % 60,
                    t % 1_000_000_000,
                )?;
            }
            CqlValue::Timestamp(t) => {
                match Utc.timestamp_millis_opt(*t) {
                    LocalResult::Ambiguous(_, _) => unreachable!(), // not supposed to happen with timestamp_millis_opt
                    LocalResult::Single(d) => {
                        write!(f, "{}", d.format("'%Y-%m-%d %H:%M:%S%.3f%z'"))?
                    }
                    LocalResult::None => f.write_str("<timestamp out of representable range>")?,
                }
            }
            CqlValue::Timeuuid(t) => write!(f, "{}", t)?,
            CqlValue::Uuid(u) => write!(f, "{}", u)?,

            // Compound types
            CqlValue::Tuple(t) => {
                f.write_str("(")?;
                CommaSeparatedDisplayer(
                    t.iter()
                        .map(|x| MaybeNullDisplayer(x.as_ref().map(CqlValueDisplayer))),
                )
                .fmt(f)?;
                f.write_str(")")?;
            }
            CqlValue::List(v) => {
                f.write_str("[")?;
                CommaSeparatedDisplayer(v.iter().map(CqlValueDisplayer)).fmt(f)?;
                f.write_str("]")?;
            }
            CqlValue::Set(v) => {
                f.write_str("{")?;
                CommaSeparatedDisplayer(v.iter().map(CqlValueDisplayer)).fmt(f)?;
                f.write_str("}")?;
            }
            CqlValue::Map(m) => {
                f.write_str("{")?;
                CommaSeparatedDisplayer(
                    m.iter()
                        .map(|(k, v)| PairDisplayer(CqlValueDisplayer(k), CqlValueDisplayer(v))),
                )
                .fmt(f)?;
                f.write_str("}")?;
            }
            CqlValue::UserDefinedType {
                keyspace: _,
                type_name: _,
                fields,
            } => {
                f.write_str("{")?;
                CommaSeparatedDisplayer(fields.iter().map(|(k, v)| {
                    PairDisplayer(k, MaybeNullDisplayer(v.as_ref().map(CqlValueDisplayer)))
                }))
                .fmt(f)?;
                f.write_str("}")?;
            }
        }
        Ok(())
    }
}

pub(crate) struct CqlStringLiteralDisplayer<'a>(&'a str);

impl<'a> Display for CqlStringLiteralDisplayer<'a> {
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

pub(crate) struct CommaSeparatedDisplayer<I>(pub(crate) I);

impl<I, T> Display for CommaSeparatedDisplayer<I>
where
    I: Iterator<Item = T> + Clone,
    T: Display,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut first = true;
        for t in self.0.clone() {
            if first {
                first = false;
            } else {
                f.write_str(",")?;
            }
            write!(f, "{}", t)?;
        }
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

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use bigdecimal::BigDecimal;
    use chrono::{NaiveDate, NaiveDateTime, NaiveTime};

    use scylla_cql::frame::response::result::CqlValue;
    use scylla_cql::frame::value::CqlDuration;

    use crate::utils::pretty::CqlValueDisplayer;

    #[test]
    fn test_cql_value_displayer() {
        assert_eq!(
            format!("{}", CqlValueDisplayer(CqlValue::Boolean(true))),
            "true"
        );
        assert_eq!(format!("{}", CqlValueDisplayer(CqlValue::Int(123))), "123");
        assert_eq!(
            format!(
                "{}",
                CqlValueDisplayer(CqlValue::Decimal(BigDecimal::from_str("123.456").unwrap()))
            ),
            "123.456"
        );
        assert_eq!(
            format!("{}", CqlValueDisplayer(CqlValue::Float(12.75))),
            "12.75"
        );
        assert_eq!(
            format!(
                "{}",
                CqlValueDisplayer(CqlValue::Text("Ala ma kota".to_owned()))
            ),
            "'Ala ma kota'"
        );
        assert_eq!(
            format!("{}", CqlValueDisplayer(CqlValue::Text("Foo's".to_owned()))),
            "'Foo''s'"
        );

        // Time types are the most tricky
        assert_eq!(
            format!("{}", CqlValueDisplayer(CqlValue::Date(40 + (1 << 31)))),
            "'1970-02-10'"
        );
        assert_eq!(
            format!(
                "{}",
                CqlValueDisplayer(CqlValue::Duration(CqlDuration {
                    months: 1,
                    days: 2,
                    nanoseconds: 3,
                }))
            ),
            "1mo2d3ns"
        );
        let t = chrono::NaiveTime::from_hms_nano_opt(6, 5, 4, 123)
            .unwrap()
            .signed_duration_since(chrono::NaiveTime::MIN);
        let t = t.num_nanoseconds().unwrap();
        assert_eq!(
            format!("{}", CqlValueDisplayer(CqlValue::Time(t))),
            "'06:05:04.000000123'"
        );

        let t = NaiveDate::from_ymd_opt(2005, 4, 2)
            .unwrap()
            .and_time(NaiveTime::from_hms_opt(19, 37, 42).unwrap());
        assert_eq!(
            format!(
                "{}",
                CqlValueDisplayer(CqlValue::Timestamp(
                    t.signed_duration_since(NaiveDateTime::default())
                        .num_milliseconds()
                ))
            ),
            "'2005-04-02 19:37:42.000+0000'"
        );

        // Compound types
        let list_or_set = vec![CqlValue::Int(1), CqlValue::Int(3), CqlValue::Int(2)];
        assert_eq!(
            format!("{}", CqlValueDisplayer(CqlValue::List(list_or_set.clone()))),
            "[1,3,2]"
        );
        assert_eq!(
            format!("{}", CqlValueDisplayer(CqlValue::Set(list_or_set.clone()))),
            "{1,3,2}"
        );

        let tuple: Vec<_> = list_or_set
            .into_iter()
            .map(Some)
            .chain(std::iter::once(None))
            .collect();
        assert_eq!(
            format!("{}", CqlValueDisplayer(CqlValue::Tuple(tuple))),
            "(1,3,2,null)"
        );

        let map = vec![
            (CqlValue::Text("foo".to_owned()), CqlValue::Int(123)),
            (CqlValue::Text("bar".to_owned()), CqlValue::Int(321)),
        ];
        assert_eq!(
            format!("{}", CqlValueDisplayer(CqlValue::Map(map))),
            "{'foo':123,'bar':321}"
        );

        let fields = vec![
            ("foo".to_owned(), Some(CqlValue::Int(123))),
            ("bar".to_owned(), Some(CqlValue::Int(321))),
        ];
        assert_eq!(
            format!(
                "{}",
                CqlValueDisplayer(CqlValue::UserDefinedType {
                    keyspace: "ks".to_owned(),
                    type_name: "typ".to_owned(),
                    fields,
                })
            ),
            "{foo:123,bar:321}"
        );
    }
}
