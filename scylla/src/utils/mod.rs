#[cfg(test)]
pub(crate) mod test_utils;

pub(crate) mod safe_format;

/// Stores `value` in `slot`, reusing the slot's allocation if it has one.
pub(crate) fn set_boxed<T>(slot: &mut Option<Box<T>>, value: T) {
    match slot {
        Some(boxed) => **boxed = value,
        None => *slot = Some(Box::new(value)),
    }
}
