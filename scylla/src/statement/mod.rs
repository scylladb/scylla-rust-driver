pub mod prepared_statement;
pub mod query;

#[derive(Copy, Clone, Debug)]
pub enum Consistency {
    Any = 0,
    One = 1,
    Two = 2,
    Three = 3,
    Quorum = 4,
    All = 5,
    LocalQuorum = 6,
    EachQuorum = 7,
    Serial = 8,
    LocalSerial = 9,
    LocalOne = 10,
}

#[derive(Clone, Debug)]
pub(crate) struct QueryParameters {
    pub consistency: Consistency,
    pub page_size: Option<i32>,
}

impl Default for QueryParameters {
    fn default() -> Self {
        Self {
            consistency: Consistency::Quorum,
            page_size: Some(1024),
        }
    }
}
