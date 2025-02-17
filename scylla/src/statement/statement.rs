use super::{PageSize, StatementConfig};
use crate::client::execution_profile::ExecutionProfileHandle;
use crate::frame::types::{Consistency, SerialConsistency};
use crate::observability::history::HistoryListener;
use crate::policies::retry::RetryPolicy;
use std::sync::Arc;
use std::time::Duration;

/// CQL query statement.
///
/// This represents a CQL query that can be executed on a server.
#[derive(Clone)]
pub struct Statement {
    pub(crate) config: StatementConfig,

    pub contents: String,
    page_size: PageSize,
}

impl Statement {
    /// Creates a new [`Statement`] from a CQL query string.
    pub fn new(query_text: impl Into<String>) -> Self {
        Self {
            contents: query_text.into(),
            page_size: PageSize::default(),
            config: Default::default(),
        }
    }

    /// Returns self with page size set to the given value.
    ///
    /// Panics if given number is nonpositive.
    pub fn with_page_size(mut self, page_size: i32) -> Self {
        self.set_page_size(page_size);
        self
    }

    /// Sets the page size for this CQL query.
    ///
    /// Panics if given number is nonpositive.
    pub fn set_page_size(&mut self, page_size: i32) {
        self.page_size = page_size
            .try_into()
            .unwrap_or_else(|err| panic!("Query::set_page_size: {err}"));
    }

    /// Returns the page size for this CQL query.
    pub(crate) fn get_validated_page_size(&self) -> PageSize {
        self.page_size
    }

    /// Returns the page size for this CQL query.
    pub fn get_page_size(&self) -> i32 {
        self.page_size.inner()
    }

    /// Sets the consistency to be used when executing this statement.
    pub fn set_consistency(&mut self, c: Consistency) {
        self.config.consistency = Some(c);
    }

    /// Gets the consistency to be used when executing this query if it is filled.
    /// If this is empty, the default_consistency of the session will be used.
    pub fn get_consistency(&self) -> Option<Consistency> {
        self.config.consistency
    }

    /// Sets the serial consistency to be used when executing this statement.
    /// (Ignored unless the statement is an LWT)
    pub fn set_serial_consistency(&mut self, sc: Option<SerialConsistency>) {
        self.config.serial_consistency = Some(sc);
    }

    /// Gets the serial consistency to be used when executing this statement.
    /// (Ignored unless the statement is an LWT)
    pub fn get_serial_consistency(&self) -> Option<SerialConsistency> {
        self.config.serial_consistency.flatten()
    }

    /// Sets the idempotence of this statement
    /// A query is idempotent if it can be applied multiple times without changing the result of the initial application
    /// If set to `true` we can be sure that it is idempotent
    /// If set to `false` it is unknown whether it is idempotent
    /// This is used in [`RetryPolicy`] to decide if retrying a query is safe
    pub fn set_is_idempotent(&mut self, is_idempotent: bool) {
        self.config.is_idempotent = is_idempotent;
    }

    /// Gets the idempotence of this statement
    pub fn get_is_idempotent(&self) -> bool {
        self.config.is_idempotent
    }

    /// Enable or disable CQL Tracing for this statement
    /// If enabled session.query() will return a QueryResult containing tracing_id
    /// which can be used to query tracing information about the execution of this query
    pub fn set_tracing(&mut self, should_trace: bool) {
        self.config.tracing = should_trace;
    }

    /// Gets whether tracing is enabled for this statement
    pub fn get_tracing(&self) -> bool {
        self.config.tracing
    }

    /// Sets the default timestamp for this statement in microseconds.
    /// If not None, it will replace the server side assigned timestamp as default timestamp
    /// If a statement contains a `USING TIMESTAMP` clause, calling this method won't change
    /// anything
    pub fn set_timestamp(&mut self, timestamp: Option<i64>) {
        self.config.timestamp = timestamp
    }

    /// Gets the default timestamp for this statement in microseconds.
    pub fn get_timestamp(&self) -> Option<i64> {
        self.config.timestamp
    }

    /// Sets the client-side timeout for this statement.
    /// If not None, the driver will stop waiting for the request
    /// to finish after `timeout` passed.
    /// Otherwise, default session client timeout will be applied.
    pub fn set_request_timeout(&mut self, timeout: Option<Duration>) {
        self.config.request_timeout = timeout
    }

    /// Gets client timeout associated with this query
    pub fn get_request_timeout(&self) -> Option<Duration> {
        self.config.request_timeout
    }

    /// Set the retry policy for this statement, overriding the one from execution profile if not None.
    #[inline]
    pub fn set_retry_policy(&mut self, retry_policy: Option<Arc<dyn RetryPolicy>>) {
        self.config.retry_policy = retry_policy;
    }

    /// Get the retry policy set for the statement.
    #[inline]
    pub fn get_retry_policy(&self) -> Option<&Arc<dyn RetryPolicy>> {
        self.config.retry_policy.as_ref()
    }

    /// Sets the listener capable of listening what happens during query execution.
    pub fn set_history_listener(&mut self, history_listener: Arc<dyn HistoryListener>) {
        self.config.history_listener = Some(history_listener);
    }

    /// Removes the listener set by `set_history_listener`.
    pub fn remove_history_listener(&mut self) -> Option<Arc<dyn HistoryListener>> {
        self.config.history_listener.take()
    }

    /// Associates the query with execution profile referred by the provided handle.
    /// Handle may be later remapped to another profile, and query will reflect those changes.
    pub fn set_execution_profile_handle(&mut self, profile_handle: Option<ExecutionProfileHandle>) {
        self.config.execution_profile_handle = profile_handle;
    }

    /// Borrows the execution profile handle associated with this query.
    pub fn get_execution_profile_handle(&self) -> Option<&ExecutionProfileHandle> {
        self.config.execution_profile_handle.as_ref()
    }
}

impl From<String> for Statement {
    fn from(s: String) -> Statement {
        Statement::new(s)
    }
}

impl<'a> From<&'a str> for Statement {
    fn from(s: &'a str) -> Statement {
        Statement::new(s.to_owned())
    }
}
