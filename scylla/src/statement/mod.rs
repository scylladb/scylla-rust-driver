use std::sync::Arc;

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
    pub speculative_execution_policy: Option<Arc<dyn SpeculativeExecutionPolicy>>,

    pub tracing: bool,
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
