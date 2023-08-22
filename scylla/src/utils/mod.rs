pub(crate) mod parse;

pub(crate) mod pretty;
pub mod test_utils;

// FIXME: replace this with `Option::unzip()` once MSRV is bumped to at least 1.66
pub(crate) fn unzip_option<T, U>(opt: Option<(T, U)>) -> (Option<T>, Option<U>) {
    match opt {
        Some((a, b)) => (Some(a), Some(b)),
        None => (None, None),
    }
}
