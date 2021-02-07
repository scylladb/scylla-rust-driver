use super::Consistency;

/// CQL query statement.
///
/// This represents a CQL query that can be executed on a server.
#[derive(Clone)]
pub struct Query {
    contents: String,
    page_size: Option<i32>,
    consistency: Consistency,
}

impl Query {
    /// Creates a new `Query` from a CQL query string.
    pub fn new(contents: String) -> Self {
        Self {
            contents,
            page_size: None,
            consistency: Default::default(),
        }
    }

    /// Returns the string representation of the CQL query.
    pub fn get_contents(&self) -> &str {
        &self.contents
    }

    /// Sets the page size for this CQL query.
    pub fn set_page_size(&mut self, page_size: i32) {
        assert!(page_size > 0, "page size must be larger than 0");
        self.page_size = Some(page_size);
    }

    /// Disables paging for this CQL query.
    pub fn disable_paging(&mut self) {
        self.page_size = None;
    }

    /// Returns the page size for this CQL query.
    pub fn get_page_size(&self) -> Option<i32> {
        self.page_size
    }

    /// Sets the consistency to be used when executing this query.
    pub fn set_consistency(&mut self, c: Consistency) {
        self.consistency = c;
    }

    /// Gets the consistency to be used when executing this query.
    pub fn get_consistency(&self) -> Consistency {
        self.consistency
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

#[derive(Clone)]
pub struct QueryBuilder(pub Query);

impl QueryBuilder {
    pub fn new(text: impl Into<String>) -> QueryBuilder {
        QueryBuilder(Query::new(text.into()))
    }

    pub fn disable_paging(mut self) -> Self {
        self.0.disable_paging();
        self
    }

    pub fn page_size(mut self, page_size: i32) -> Self {
        self.0.set_page_size(page_size);
        self
    }

    pub fn consistency(mut self, consistency: Consistency) -> Self {
        self.0.set_consistency(consistency);
        self
    }

    pub fn build(self) -> Query {
        self.0
    }
}
