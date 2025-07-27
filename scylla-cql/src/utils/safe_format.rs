use itertools::Itertools;
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
        self.cloneable_iter.clone().format(self.sep).fmt(f)
    }
}

impl<I> Debug for SafeFormat<'_, I>
where
    I: Iterator + Clone,
    <I as Iterator>::Item: Debug,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        self.cloneable_iter.clone().format(self.sep).fmt(f)
    }
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
