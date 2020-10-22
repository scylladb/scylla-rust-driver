pub struct Query {
    contents: String,
}

impl Query {
    pub fn new(contents: String) -> Self {
        Self { contents }
    }

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
