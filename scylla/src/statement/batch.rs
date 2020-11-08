use crate::statement::{prepared_statement::PreparedStatement, query::Query};

pub use crate::frame::request::batch::BatchType;

pub struct Batch {
    statements: Vec<BatchStatement>,
    batch_type: BatchType,
}

impl Batch {
    pub fn new<E: Into<BatchStatement> + Clone>(statements: &[E], batch_type: BatchType) -> Self {
        Self {
            statements: statements.iter().map(|e| e.clone().into()).collect(),
            batch_type,
        }
    }

    pub fn append_statement(&mut self, statement: impl Into<BatchStatement>) {
        self.statements.push(statement.into());
    }

    pub fn clear_statements(&mut self) {
        self.statements.clear();
    }

    pub fn get_type(&self) -> BatchType {
        self.batch_type
    }

    pub fn get_statements(&self) -> &[BatchStatement] {
        self.statements.as_ref()
    }
}

impl Default for Batch {
    fn default() -> Self {
        Self {
            statements: Vec::new(),
            batch_type: BatchType::Logged,
        }
    }
}

#[derive(Clone)]
pub enum BatchStatement {
    Query(Query),
    PreparedStatement(PreparedStatement),
}

impl From<Query> for BatchStatement {
    fn from(q: Query) -> Self {
        BatchStatement::Query(q)
    }
}

impl From<PreparedStatement> for BatchStatement {
    fn from(p: PreparedStatement) -> Self {
        BatchStatement::PreparedStatement(p)
    }
}
