/// CQL query statement.
///
/// This represents a CQL query that can be executed on a server.
pub struct Query {
    contents: String,
}

impl Query {
    /// Creates a new `Query` from a CQL query string.
    pub fn new(contents: String) -> Self {
        Self { contents }
    }

    /// Returns the string representation of the CQL query.
    pub fn get_contents(&self) -> &str {
        &self.contents
    }
}

impl From<String> for Query {
    fn from(s: String) -> Query {
        Query::new(s)
    }
}

impl<'a> From<&'a str> for Query {
    fn from(s: &'a str) -> Query {
        Query::new(s.to_owned())
    }
}
