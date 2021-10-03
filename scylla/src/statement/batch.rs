use crate::statement::{prepared_statement::PreparedStatement, query::Query};

pub use super::Consistency;
use super::StatementConfig;
pub use crate::frame::request::batch::BatchType;

/// CQL batch statement.
///
/// This represents a CQL batch that can be executed on a server.
#[derive(Clone)]
pub struct Batch {
    pub config: StatementConfig,
    pub statements: Vec<BatchStatement>,
    pub batch_type: BatchType,
}

impl Batch {
    /// Uses the default value for field `config`
    pub fn new(statements: Vec<BatchStatement>, batch_type: BatchType) -> Batch {
        Batch {
            config: Default::default(),
            statements,
            batch_type
        }
    }

    /// Appends a new statement to the batch.
    pub fn append_statement(&mut self, statement: impl Into<BatchStatement>) {
        self.statements.push(statement.into());
    }
}

impl Default for Batch {
    fn default() -> Self {
        Self {
            statements: Vec::new(),
            batch_type: BatchType::Logged,
            config: Default::default(),
        }
    }
}

/// This enum represents a CQL statement, that can be part of batch.
#[derive(Clone)]
pub enum BatchStatement {
    Query(Query),
    PreparedStatement(PreparedStatement),
}

impl From<&str> for BatchStatement {
    fn from(s: &str) -> Self {
        BatchStatement::Query(Query::from(s))
    }
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
