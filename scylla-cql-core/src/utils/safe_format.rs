use std::fmt::{Debug, Display, Formatter};

pub(crate) struct SafeFormat<'a, I: ?Sized> {
    sep: &'a str,
    cloneable_iter: I,
}

impl<I> Display for SafeFormat<'_, I>
where
    I: Iterator + Clone,
    <I as Iterator>::Item: Display,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write_separated(self.cloneable_iter.clone(), self.sep, f, Display::fmt)
    }
}

impl<I> Debug for SafeFormat<'_, I>
where
    I: Iterator + Clone,
    <I as Iterator>::Item: Debug,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write_separated(self.cloneable_iter.clone(), self.sep, f, Debug::fmt)
    }
}

fn write_separated<T>(
    items: impl Iterator<Item = T>,
    sep: &str,
    f: &mut Formatter<'_>,
    fmt_item: fn(&T, &mut Formatter<'_>) -> std::fmt::Result,
) -> std::fmt::Result {
    for (i, item) in items.enumerate() {
        if i > 0 {
            f.write_str(sep)?;
        }
        fmt_item(&item, f)?;
    }
    Ok(())
}

pub(crate) trait IteratorSafeFormatExt: Iterator {
    fn safe_format(self, sep: &str) -> SafeFormat<'_, Self>
    where
        Self: Clone;
}

impl<I: Iterator> IteratorSafeFormatExt for I {
    fn safe_format(self, sep: &str) -> SafeFormat<'_, Self>
    where
        Self: Clone,
    {
        SafeFormat {
            sep,
            cloneable_iter: self,
        }
    }
}
