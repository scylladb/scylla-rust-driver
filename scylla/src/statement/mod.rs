use std::{sync::Arc, time::Duration};

use crate::transport::retry_policy::RetryPolicy;
use crate::transport::speculative_execution::SpeculativeExecutionPolicy;

pub mod batch;
pub mod prepared_statement;
pub mod query;

pub use crate::frame::types::{Consistency, SerialConsistency};

#[derive(Debug)]
pub struct StatementConfig {
    pub consistency: Option<Consistency>,
    pub serial_consistency: Option<SerialConsistency>,

    pub is_idempotent: bool,

    pub retry_policy: Option<Box<dyn RetryPolicy>>,
    pub speculative_execution_policy: Option<Arc<dyn SpeculativeExecutionPolicy>>,

    pub tracing: bool,
    pub timestamp: Option<i64>,
    pub request_timeout: Option<Duration>,
}

impl Default for StatementConfig {
    fn default() -> Self {
        Self {
            consistency: Default::default(),
            serial_consistency: Some(SerialConsistency::LocalSerial),
            is_idempotent: false,
            retry_policy: None,
            speculative_execution_policy: None,
            tracing: false,
            timestamp: None,
            request_timeout: None,
        }
    }
}

impl Clone for StatementConfig {
    fn clone(&self) -> Self {
        Self {
            retry_policy: self
                .retry_policy
                .as_ref()
                .map(|policy| policy.clone_boxed()),
            speculative_execution_policy: self.speculative_execution_policy.clone(),
            ..*self
        }
    }
}

impl StatementConfig {
    /// Determines the consistency of a query
    pub fn determine_consistency(&self, default_consistency: Consistency) -> Consistency {
        self.consistency.unwrap_or(default_consistency)
    }
}
