use std::sync::Arc;

use crate::transport::load_balancing::LoadBalancingPolicy;
use crate::transport::retry_policy::RetryPolicy;
use crate::transport::speculative_execution::SpeculativeExecutionPolicy;

pub mod batch;
pub mod prepared_statement;
pub mod query;

pub use crate::frame::types::{Consistency, SerialConsistency};

pub struct StatementConfig {
    pub consistency: Option<Consistency>,
    pub serial_consistency: Option<SerialConsistency>,

    pub is_idempotent: bool,

    pub retry_policy: Option<Box<dyn RetryPolicy>>,
    pub speculative_execution_policy: Option<Arc<dyn SpeculativeExecutionPolicy>>,
    pub load_balancing_policy: Option<Arc<dyn LoadBalancingPolicy>>,

    pub tracing: bool,
    pub timestamp: Option<i64>,
}

impl Default for StatementConfig {
    fn default() -> Self {
        Self {
            consistency: Default::default(),
            serial_consistency: Some(SerialConsistency::LocalSerial),
            is_idempotent: false,
            retry_policy: None,
            speculative_execution_policy: None,
            load_balancing_policy: None,
            tracing: false,
            timestamp: None,
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
            load_balancing_policy: self.load_balancing_policy.clone(),
            tracing: self.tracing,
            timestamp: self.timestamp,
        }
    }
}

impl StatementConfig {
    /// Determines the consistency of a query
    pub fn determine_consistency(&self, default_consistency: Consistency) -> Consistency {
        self.consistency.unwrap_or(default_consistency)
    }
}
