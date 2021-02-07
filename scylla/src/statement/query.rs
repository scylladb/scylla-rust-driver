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

/// `QueryBuilder` makes creating a [`Query`] convenient
///
/// # Example:
/// ```rust
/// # use scylla::query::{Query, QueryBuilder};
/// # use scylla::frame::types::Consistency;
/// let query: Query = QueryBuilder::new("SELECT * FROM keyspace.table")
///     .consistency(Consistency::One)
///     .build();
/// ```
#[derive(Clone)]
pub struct QueryBuilder(pub Query);

impl QueryBuilder {
    /// Creates new QueryBuilder for a [`Query`] with given query text
    pub fn new(text: impl Into<String>) -> QueryBuilder {
        QueryBuilder(Query::new(text.into()))
    }

    /// Disables paging for this CQL query.
    pub fn disable_paging(mut self) -> Self {
        self.0.disable_paging();
        self
    }

    /// Sets the page size for this CQL query
    pub fn page_size(mut self, page_size: i32) -> Self {
        self.0.set_page_size(page_size);
        self
    }

    /// Sets the consistency to be used when executing this query.
    pub fn consistency(mut self, consistency: Consistency) -> Self {
        self.0.set_consistency(consistency);
        self
    }

    /// Builds the result Query, turns `QueryBuilder` into `Query`
    pub fn build(self) -> Query {
        self.0
    }
}

#[cfg(test)]
mod tests {
    use super::{Query, QueryBuilder};
    use crate::frame::types::Consistency;

    #[test]
    fn default_query_builder() {
        let query: Query = QueryBuilder::new("query text").build();

        assert_eq!(query.contents, "query text".to_string());
        assert_eq!(query.page_size, None);
        assert_eq!(query.consistency, Consistency::Quorum);
    }

    #[test]
    fn query_builder_all_options() {
        let query: Query = QueryBuilder::new("other query text")
            .disable_paging()
            .page_size(128)
            .consistency(Consistency::LocalQuorum)
            .build();

        assert_eq!(query.contents, "other query text".to_string());
        assert_eq!(query.page_size, Some(128));
        assert_eq!(query.consistency, Consistency::LocalQuorum);
    }

    #[test]
    #[should_panic]
    fn query_builder_invalid_page_size() {
        QueryBuilder::new("bad page size").page_size(-16);
    }
}
