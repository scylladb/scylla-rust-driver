//! Defines the [`BoundStatement`] type, which represents a prepared statement
//! that has already been bound with values to be executed with.

use std::fmt::Debug;
use std::sync::Arc;
use std::time::Duration;

use crate::client::execution_profile::ExecutionProfileHandle;
use crate::frame::types::{Consistency, SerialConsistency};
use crate::observability::history::HistoryListener;
use crate::policies::load_balancing::LoadBalancingPolicy;
use crate::policies::retry::RetryPolicy;
use crate::routing::Token;
use crate::serialize::SerializationError;
use crate::serialize::row::{SerializeRow, SerializedValues};

use super::prepared::{PartitionKey, PartitionKeyError, PreparedStatement};

/// Represents a prepared statement together with its values already bound.
#[derive(Debug, Clone)]
pub struct BoundStatement {
    pub(crate) prepared: PreparedStatement,
    pub(crate) values: SerializedValues,
}

impl BoundStatement {
    pub(crate) fn new(
        prepared: PreparedStatement,
        values: &impl SerializeRow,
    ) -> Result<Self, SerializationError> {
        let values = prepared.serialize_values(values)?;
        Ok(Self { prepared, values })
    }

    pub(crate) fn extract_partition_key_and_calculate_token<'ps>(
        &'ps self,
    ) -> Result<Option<(PartitionKey<'ps>, Token)>, PartitionKeyError> {
        self.prepared
            .extract_partition_key_and_calculate_token(&self.values)
    }

    /// Calculates the token for the bound statement.
    ///
    /// Returns the token that would be computed for executing the provided bound statement.
    pub fn calculate_token(&self) -> Result<Option<Token>, PartitionKeyError> {
        self.extract_partition_key_and_calculate_token()
            .map(|p| p.map(|(_, t)| t))
    }

    /// Returns the prepared statement behind the `BoundStatement`.
    pub fn prepared(&self) -> &PreparedStatement {
        &self.prepared
    }

    /// Sets the page size for this CQL query.
    ///
    /// Panics if the given number is nonpositive.
    pub fn set_page_size(&mut self, page_size: i32) {
        self.prepared.set_page_size(page_size);
    }

    /// Sets the consistency to be used when executing this statement.
    pub fn set_consistency(&mut self, consistency: Consistency) {
        self.prepared.set_consistency(consistency);
    }

    /// Unsets the consistency overridden on this statement.
    ///
    /// The consistency will be derived from the per-statement execution profile,
    /// or from the session's default execution profile if one is not set.
    pub fn unset_consistency(&mut self) {
        self.prepared.unset_consistency();
    }

    /// Sets the serial consistency to be used when executing this statement.
    ///
    /// This setting is ignored unless the statement is an LWT.
    pub fn set_serial_consistency(&mut self, serial_consistency: Option<SerialConsistency>) {
        self.prepared.set_serial_consistency(serial_consistency);
    }

    /// Unsets the serial consistency overridden on this statement.
    ///
    /// The serial consistency will be derived from the per-statement execution
    /// profile, or from the session's default execution profile if one is not set.
    pub fn unset_serial_consistency(&mut self) {
        self.prepared.unset_serial_consistency();
    }

    /// Sets whether this statement is idempotent.
    ///
    /// Retry policies use this information to decide whether retrying the
    /// statement is safe.
    pub fn set_is_idempotent(&mut self, is_idempotent: bool) {
        self.prepared.set_is_idempotent(is_idempotent);
    }

    /// Enables or disables CQL tracing for this statement.
    pub fn set_tracing(&mut self, should_trace: bool) {
        self.prepared.set_tracing(should_trace);
    }

    /// Sets whether cached result metadata should be used to decode results.
    pub fn set_use_cached_result_metadata(&mut self, use_cached_metadata: bool) {
        self.prepared
            .set_use_cached_result_metadata(use_cached_metadata);
    }

    /// Sets the default timestamp for this statement in microseconds.
    pub fn set_timestamp(&mut self, timestamp: Option<i64>) {
        self.prepared.set_timestamp(timestamp);
    }

    /// Sets the client-side timeout for this statement.
    pub fn set_request_timeout(&mut self, timeout: Option<Duration>) {
        self.prepared.set_request_timeout(timeout);
    }

    /// Sets the retry policy for this statement, overriding the execution
    /// profile's policy when one is provided.
    pub fn set_retry_policy(&mut self, retry_policy: Option<Arc<dyn RetryPolicy>>) {
        self.prepared.set_retry_policy(retry_policy);
    }

    /// Sets the load balancing policy for this statement, overriding the
    /// execution profile's policy when one is provided.
    pub fn set_load_balancing_policy(
        &mut self,
        load_balancing_policy: Option<Arc<dyn LoadBalancingPolicy>>,
    ) {
        self.prepared
            .set_load_balancing_policy(load_balancing_policy);
    }

    /// Sets the listener that observes this statement's execution history.
    pub fn set_history_listener(&mut self, history_listener: Arc<dyn HistoryListener>) {
        self.prepared.set_history_listener(history_listener);
    }

    /// Removes and returns the execution history listener set on this statement.
    pub fn remove_history_listener(&mut self) -> Option<Arc<dyn HistoryListener>> {
        self.prepared.remove_history_listener()
    }

    /// Associates this statement with the execution profile referred to by the
    /// provided handle.
    pub fn set_execution_profile_handle(&mut self, profile_handle: Option<ExecutionProfileHandle>) {
        self.prepared.set_execution_profile_handle(profile_handle);
    }
}
