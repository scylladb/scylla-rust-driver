use std::fmt::Display;

/// An error that can occur during parsing.
#[derive(Copy, Clone)]
pub(crate) struct ParseError {
    pub(crate) remaining: usize,
    pub(crate) cause: ParseErrorCause,
}

impl ParseError {
    /// Given the original string, returns the 1-based position
    /// of the error in characters.
    /// If an incorrect string was given, the function may return 0.
    pub(crate) fn calculate_position(&self, original: &str) -> Option<usize> {
        calculate_position(original, self.remaining)
    }

    /// Returns the error cause.
    pub(crate) fn get_cause(&self) -> ParseErrorCause {
        self.cause
    }
}

/// Cause of the parsing error.
/// Should be lightweight so that it can be quickly discarded.
#[derive(Copy, Clone)]
pub(crate) enum ParseErrorCause {
    Expected(&'static str),
    Other(&'static str),
}

impl Display for ParseErrorCause {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ParseErrorCause::Expected(e) => write!(f, "expected {:?}", e),
            ParseErrorCause::Other(e) => f.write_str(e),
        }
    }
}

pub(crate) type ParseResult<T> = Result<T, ParseError>;

/// A utility class for building simple recursive-descent parsers.
///
/// Basically, a wrapper over &str with nice methods that help with parsing.
#[derive(Clone, Copy)]
#[must_use]
pub(crate) struct ParserState<'s> {
    s: &'s str,
}

impl<'s> ParserState<'s> {
    /// Creates a new parser from given input string.
    pub(crate) fn new(s: &'s str) -> Self {
        Self { s }
    }

    /// Applies given parsing function until it returns false
    /// and returns the final parser state.
    pub(crate) fn parse_while(
        self,
        mut parser: impl FnMut(Self) -> ParseResult<(bool, Self)>,
    ) -> ParseResult<Self> {
        let mut me = self;
        loop {
            let (proceed, new_me) = parser(me)?;
            if !proceed {
                return Ok(new_me);
            }
            me = new_me;
        }
    }

    /// If the input string contains given string at the beginning,
    /// returns a new parser state with given string skipped.
    /// Otherwise, returns an error.
    pub(crate) fn accept(self, part: &'static str) -> ParseResult<Self> {
        match self.s.strip_prefix(part) {
            Some(s) => Ok(Self { s }),
            None => Err(self.error(ParseErrorCause::Expected(part))),
        }
    }

    /// Returns new parser state with whitespace skipped from the beginning.
    pub(crate) fn skip_white(self) -> Self {
        let (_, me) = self.take_while(char::is_whitespace);
        me
    }

    /// Parses a sequence of digits and '-' as an integer.
    /// Consumes characters until it finds a character that is not a digit or '-'.
    ///
    /// An error is returned if:
    /// * The first character is not a digit or '-'
    /// * The integer is larger than i32
    pub(crate) fn parse_i32(self) -> ParseResult<(i32, Self)> {
        let (digits, p) = self.take_while(|c| c.is_ascii_digit() || c == '-');
        if let Ok(value) = digits.parse() {
            Ok((value, p))
        } else {
            Err(p.error(ParseErrorCause::Expected("integer of max length 2**32")))
        }
    }

    /// Skips characters from the beginning while they satisfy given predicate
    /// and returns new parser state which
    pub(crate) fn take_while(self, mut pred: impl FnMut(char) -> bool) -> (&'s str, Self) {
        let idx = self.s.find(move |c| !pred(c)).unwrap_or(self.s.len());
        let new = Self { s: &self.s[idx..] };
        (&self.s[..idx], new)
    }

    /// Returns the number of remaining bytes to parse.
    pub(crate) fn get_remaining(self) -> usize {
        self.s.len()
    }

    /// Returns true if the input string was parsed completely.
    pub(crate) fn is_at_eof(self) -> bool {
        self.s.is_empty()
    }

    /// Returns an error with given cause, associated with given position.
    pub(crate) fn error(self, cause: ParseErrorCause) -> ParseError {
        ParseError {
            remaining: self.get_remaining(),
            cause,
        }
    }

    /// Given the original string, returns the 1-based position
    /// of the error in characters.
    /// If an incorrect string was given, the function may return None.
    pub(crate) fn calculate_position(self, original: &str) -> Option<usize> {
        calculate_position(original, self.get_remaining())
    }
}

fn calculate_position(original: &str, remaining: usize) -> Option<usize> {
    let prefix_len = original.len().checked_sub(remaining)?;
    let prefix = original.get(..prefix_len)?;
    Some(prefix.chars().count() + 1)
}
