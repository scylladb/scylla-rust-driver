use std::fmt::Display;

/// Cause of the parsing error.
/// Should be lightweight so that it can be quickly discarded.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum ParseErrorCause {
    /// Expected a specific string, but it was not found.
    Expected(&'static str),
    /// Other error, described by a string.
    Other(&'static str),
}

impl Display for ParseErrorCause {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ParseErrorCause::Expected(e) => write!(f, "expected {e:?}"),
            ParseErrorCause::Other(e) => f.write_str(e),
        }
    }
}
