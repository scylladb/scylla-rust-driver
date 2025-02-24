use std::fmt::{Display, LowerHex, UpperHex};

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

pub(crate) struct CqlStringLiteralDisplayer<'a>(pub(crate) &'a str);

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

pub(crate) struct PairDisplayer<K, V>(pub(crate) K, pub(crate) V);

impl<K, V> Display for PairDisplayer<K, V>
where
    K: Display,
    V: Display,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}:{}", self.0, self.1)
    }
}

pub(crate) struct MaybeNullDisplayer<T>(pub(crate) Option<T>);

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
