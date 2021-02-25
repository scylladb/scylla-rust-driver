use crate::statement::{prepared_statement::PreparedStatement, query::Query};

pub use super::Consistency;
pub use crate::frame::request::batch::BatchType;
use crate::transport::retry_policy::RetryPolicy;

/// CQL batch statement.
///
/// This represents a CQL batch that can be executed on a server.
pub struct Batch {
    statements: Vec<BatchStatement>,
    batch_type: BatchType,
    pub consistency: Consistency,
    pub serial_consistency: Option<Consistency>,
    pub is_idempotent: bool,
    pub retry_policy: Option<Box<dyn RetryPolicy + Send + Sync>>,
}

impl Batch {
    /// Creates a new, empty `Batch` of `batch_type` type.
    pub fn new(batch_type: BatchType) -> Self {
        Self {
            batch_type,
            ..Default::default()
        }
    }

    /// Appends a new statement to the batch.
    pub fn append_statement(&mut self, statement: impl Into<BatchStatement>) {
        self.statements.push(statement.into());
    }

    /// Gets type of batch.
    pub fn get_type(&self) -> BatchType {
        self.batch_type
    }

    /// Returns statements contained in the batch.
    pub fn get_statements(&self) -> &[BatchStatement] {
        self.statements.as_ref()
    }

    /// Sets the consistency to be used when executing this batch.
    pub fn set_consistency(&mut self, c: Consistency) {
        self.consistency = c;
    }

    /// Gets the consistency to be used when executing this batch.
    pub fn get_consistency(&self) -> Consistency {
        self.consistency
    }

    /// Sets the serial consistency to be used when executing this batch.
    /// (Ignored unless the batch is an LWT)
    pub fn set_serial_consistency(&mut self, sc: Option<Consistency>) {
        self.serial_consistency = sc;
    }

    /// Gets the serial consistency to be used when executing this batch.
    /// (Ignored unless the batch is an LWT)
    pub fn get_serial_consistency(&self) -> Option<Consistency> {
        self.serial_consistency
    }

    /// Sets the idempotence of this statement  
    /// A query is idempotent if it can be applied multiple times without changing the result of the initial application  
    /// If set to `true` we can be sure that it is idempotent  
    /// If set to `false` it is unknown whether it is idempotent  
    /// This is used in [`RetryPolicy`] to decide if retrying a query is safe
    pub fn set_is_idempotent(&mut self, is_idempotent: bool) {
        self.is_idempotent = is_idempotent;
    }

    /// Gets the idempotence of this statement
    pub fn get_is_idempotent(&self) -> bool {
        self.is_idempotent
    }

    /// Sets a custom [`RetryPolicy`] to be used with this statement  
    /// By default Session's retry policy is used, this allows to use a custom retry policy
    pub fn set_retry_policy(&mut self, retry_policy: Box<dyn RetryPolicy + Send + Sync>) {
        self.retry_policy = Some(retry_policy);
    }

    /// Gets custom [`RetryPolicy`] used by this statement
    pub fn get_retry_policy(&self) -> &Option<Box<dyn RetryPolicy + Send + Sync>> {
        &self.retry_policy
    }
}

impl Default for Batch {
    fn default() -> Self {
        Self {
            statements: Vec::new(),
            batch_type: BatchType::Logged,
            consistency: Default::default(),
            serial_consistency: None,
            is_idempotent: false,
            retry_policy: None,
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

impl Clone for Batch {
    fn clone(&self) -> Batch {
        Batch {
            statements: self.statements.clone(),
            batch_type: self.batch_type,
            consistency: self.consistency,
            serial_consistency: self.serial_consistency,
            is_idempotent: self.is_idempotent,
            retry_policy: self
                .retry_policy
                .as_ref()
                .map(|policy| policy.clone_boxed()),
        }
    }
}
