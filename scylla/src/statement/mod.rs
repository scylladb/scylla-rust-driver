use crate::transport::retry_policy::RetryPolicy;
use crate::transport::speculative_execution::SpeculativeExecutionPolicy;

pub mod batch;
pub mod prepared_statement;
pub mod query;

pub use crate::frame::types::Consistency;

pub struct StatementConfig {
    pub consistency: Consistency,
    pub serial_consistency: Option<Consistency>,

    pub is_idempotent: bool,

    pub retry_policy: Option<Box<dyn RetryPolicy>>,
    pub speculative_execution_policy: Option<SpeculativeExecutionPolicy>,

    pub tracing: bool,
}

impl StatementConfig {
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
    pub fn set_retry_policy(&mut self, retry_policy: Box<dyn RetryPolicy>) {
        self.retry_policy = Some(retry_policy);
    }

    /// Gets custom [`RetryPolicy`] used by this statement
    pub fn get_retry_policy(&self) -> &Option<Box<dyn RetryPolicy>> {
        &self.retry_policy
    }

    /// Enable or disable CQL Tracing for this batch
    /// If enabled session.batch() will return a BatchResult containing tracing_id
    /// which can be used to query tracing information about the execution of this query
    pub fn set_tracing(&mut self, should_trace: bool) {
        self.tracing = should_trace;
    }

    /// Gets whether tracing is enabled for this batch
    pub fn get_tracing(&self) -> bool {
        self.tracing
    }
}

impl Default for StatementConfig {
    fn default() -> Self {
        Self {
            consistency: Default::default(),
            serial_consistency: None,
            is_idempotent: false,
            retry_policy: None,
            speculative_execution_policy: None,
            tracing: false,
        }
    }
}

impl Clone for StatementConfig {
    fn clone(&self) -> Self {
        Self {
            consistency: self.consistency,
            serial_consistency: self.serial_consistency,
            is_idempotent: self.is_idempotent,
            retry_policy: self
                .retry_policy
                .as_ref()
                .map(|policy| policy.clone_boxed()),
            speculative_execution_policy: self.speculative_execution_policy.clone(),
            tracing: self.tracing,
        }
    }
}
