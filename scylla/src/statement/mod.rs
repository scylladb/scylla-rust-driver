use std::{sync::Arc, time::Duration};

use crate::execution::ExecutionProfileHandle;
use crate::{execution::retries::RetryPolicy, history::HistoryListener};

pub mod batch;
pub mod prepared_statement;
pub mod query;

pub use crate::frame::types::{Consistency, SerialConsistency};

#[derive(Debug, Clone, Default)]
pub(crate) struct StatementConfig {
    pub(crate) consistency: Option<Consistency>,
    pub(crate) serial_consistency: Option<Option<SerialConsistency>>,

    pub(crate) is_idempotent: bool,

    pub(crate) tracing: bool,
    pub(crate) timestamp: Option<i64>,
    pub(crate) request_timeout: Option<Duration>,

    pub(crate) history_listener: Option<Arc<dyn HistoryListener>>,

    pub(crate) execution_profile_handle: Option<ExecutionProfileHandle>,
    pub(crate) retry_policy: Option<Arc<dyn RetryPolicy>>,
}

impl StatementConfig {
    /// Determines the consistency of a query
    #[must_use]
    pub(crate) fn determine_consistency(&self, default_consistency: Consistency) -> Consistency {
        self.consistency.unwrap_or(default_consistency)
    }
}
