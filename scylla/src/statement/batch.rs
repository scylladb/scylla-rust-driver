use crate::statement::{prepared_statement::PreparedStatement, query::Query};

pub enum Batched {
    Query(Query),
    PreparedStatement(PreparedStatement),
}

pub struct Batch {
    queries: Vec<Batched>,
}
