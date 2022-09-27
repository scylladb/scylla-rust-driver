use bytes::{BufMut, Bytes, BytesMut};
use smallvec::{smallvec, SmallVec};
use std::convert::TryInto;
use std::sync::Arc;
use std::time::Duration;
use thiserror::Error;
use uuid::Uuid;

use super::StatementConfig;
use crate::frame::response::result::PreparedMetadata;
use crate::frame::types::{Consistency, SerialConsistency};
use crate::frame::value::SerializedValues;
use crate::history::HistoryListener;
use crate::transport::partitioner::PartitionerName;
use crate::transport::retry_policy::RetryPolicy;

/// Represents a statement prepared on the server.
#[derive(Debug)]
pub struct PreparedStatement {
    pub(crate) config: StatementConfig,
    pub prepare_tracing_ids: Vec<Uuid>,

    id: Bytes,
    metadata: PreparedMetadata,
    statement: String,
    page_size: Option<i32>,
    partitioner_name: PartitionerName,
}

impl Clone for PreparedStatement {
    fn clone(&self) -> Self {
        Self {
            config: self.config.clone(),
            prepare_tracing_ids: Vec::new(),
            id: self.id.clone(),
            metadata: self.metadata.clone(),
            statement: self.statement.clone(),
            page_size: self.page_size,
            partitioner_name: self.partitioner_name.clone(),
        }
    }
}

impl PreparedStatement {
    pub(crate) fn new(
        id: Bytes,
        metadata: PreparedMetadata,
        statement: String,
        page_size: Option<i32>,
        config: StatementConfig,
    ) -> Self {
        Self {
            id,
            metadata,
            statement,
            prepare_tracing_ids: Vec::new(),
            page_size,
            config,
            partitioner_name: Default::default(),
        }
    }

    pub fn get_id(&self) -> &Bytes {
        &self.id
    }

    pub fn get_statement(&self) -> &str {
        &self.statement
    }

    /// Sets the page size for this CQL query.
    pub fn set_page_size(&mut self, page_size: i32) {
        assert!(page_size > 0, "page size must be larger than 0");
        self.page_size = Some(page_size);
    }

    /// Disables paging for this CQL query.
    pub fn disable_paging(&mut self) {
        self.page_size = None;
    }

    /// Returns the page size for this CQL query.
    pub fn get_page_size(&self) -> Option<i32> {
        self.page_size
    }

    /// Gets tracing ids of queries used to prepare this statement
    pub fn get_prepare_tracing_ids(&self) -> &[Uuid] {
        &self.prepare_tracing_ids
    }

    /// Returns true if the prepared statement has necessary information
    /// to be routed in a token-aware manner. If false, the query
    /// will always be sent to a random node/shard.
    pub fn is_token_aware(&self) -> bool {
        !self.metadata.pk_indexes.is_empty()
    }

    /// Computes the partition key of the target table from given values —
    /// it assumes that all partition key columns are passed in values.
    /// Partition keys have a specific serialization rules.
    /// Ref: <https://github.com/scylladb/scylla/blob/40adf38915b6d8f5314c621a94d694d172360833/compound_compat.hh#L33-L47>
    pub fn compute_partition_key(
        &self,
        bound_values: &SerializedValues,
    ) -> Result<Bytes, PartitionKeyError> {
        let mut buf = BytesMut::new();

        if self.metadata.pk_indexes.len() == 1 {
            if let Some(v) = bound_values
                .iter()
                .nth(self.metadata.pk_indexes[0].index as usize)
                .ok_or_else(|| {
                    PartitionKeyError::NoPkIndexValue(
                        self.metadata.pk_indexes[0].index,
                        bound_values.len(),
                    )
                })?
            {
                buf.extend_from_slice(v);
            }
            return Ok(buf.into());
        }
        // Iterate on values using sorted pk_indexes (see deser_prepared_metadata),
        // and use PartitionKeyIndex.sequence to insert the value in pk_values with the correct order.
        // At the same time, compute the size of the buffer to reserve it before writing in it.
        let mut pk_values: SmallVec<[_; 8]> = smallvec![None; self.metadata.pk_indexes.len()];
        let mut values_iter = bound_values.iter();
        let mut buf_size = 0;
        // pk_indexes contains the indexes of the partition key value, so the current offset of the
        // iterator must be kept, in order to compute the next position of the pk in the iterator.
        // At each iteration values_iter.nth(0) will roughly correspond to values[values_iter_offset],
        // so values[pk_index.index] will be retrieved with values_iter.nth(pk_index.index - values_iter_offset)
        let mut values_iter_offset = 0;
        for pk_index in &self.metadata.pk_indexes {
            // Find value matching current pk_index
            let next_val = values_iter
                .nth((pk_index.index - values_iter_offset) as usize)
                .ok_or_else(|| {
                    PartitionKeyError::NoPkIndexValue(pk_index.index, bound_values.len())
                })?;
            // Add it in sequence order to pk_values
            if let Some(v) = next_val {
                pk_values[pk_index.sequence as usize] = Some(v);
                buf_size += std::mem::size_of::<u16>() + v.len() + std::mem::size_of::<u8>();
            }
            values_iter_offset = pk_index.index + 1;
        }
        // Add values' bytes
        buf.reserve(buf_size);
        for v in pk_values.iter().flatten() {
            let v_len_u16: u16 = v
                .len()
                .try_into()
                .map_err(|_| PartitionKeyError::ValueTooLong(v.len()))?;

            buf.put_u16(v_len_u16);
            buf.extend_from_slice(v);
            buf.put_u8(0);
        }
        Ok(buf.into())
    }

    /// Returns the name of the keyspace this statement is operating on.
    pub fn get_keyspace_name(&self) -> Option<&str> {
        self.metadata
            .col_specs
            .first()
            .map(|col_spec| col_spec.table_spec.ks_name.as_str())
    }

    /// Returns the name of the table this statement is operating on.
    pub fn get_table_name(&self) -> Option<&str> {
        self.metadata
            .col_specs
            .first()
            .map(|col_spec| col_spec.table_spec.table_name.as_str())
    }

    /// Sets the consistency to be used when executing this statement.
    pub fn set_consistency(&mut self, c: Consistency) {
        self.config.consistency = Some(c);
    }

    /// Gets the consistency to be used when executing this prepared statement if it is filled.
    /// If this is empty, the default_consistency of the session will be used.
    pub fn get_consistency(&self) -> Option<Consistency> {
        self.config.consistency
    }

    /// Sets the serial consistency to be used when executing this statement.
    /// (Ignored unless the statement is an LWT)
    pub fn set_serial_consistency(&mut self, sc: Option<SerialConsistency>) {
        self.config.serial_consistency = sc;
    }

    /// Gets the serial consistency to be used when executing this statement.
    /// (Ignored unless the statement is an LWT)
    pub fn get_serial_consistency(&self) -> Option<SerialConsistency> {
        self.config.serial_consistency
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

    /// Sets a custom [`RetryPolicy`] to be used with this statement
    /// By default Session's retry policy is used, this allows to use a custom retry policy
    pub fn set_retry_policy(&mut self, retry_policy: Box<dyn RetryPolicy>) {
        self.config.retry_policy = Some(retry_policy);
    }

    /// Gets custom [`RetryPolicy`] used by this statement
    pub fn get_retry_policy(&self) -> &Option<Box<dyn RetryPolicy>> {
        &self.config.retry_policy
    }

    /// Enable or disable CQL Tracing for this statement
    /// If enabled session.execute() will return a QueryResult containing tracing_id
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

    /// Sets the name of the partitioner used for this statement.
    pub(crate) fn set_partitioner_name(&mut self, partitioner_name: Option<&str>) {
        self.partitioner_name = partitioner_name
            .and_then(PartitionerName::from_str)
            .unwrap_or_default();
    }

    /// Access metadata about this prepared statement as returned by the database
    pub fn get_prepared_metadata(&self) -> &PreparedMetadata {
        &self.metadata
    }

    /// Get the name of the partitioner used for this statement.
    pub(crate) fn get_partitioner_name(&self) -> &PartitionerName {
        &self.partitioner_name
    }

    /// Sets the listener capable of listening what happens during query execution.
    pub fn set_history_listener(&mut self, history_listener: Arc<dyn HistoryListener>) {
        self.config.history_listener = Some(history_listener);
    }

    /// Removes the listener set by `set_history_listener`.
    pub fn remove_history_listener(&mut self) -> Option<Arc<dyn HistoryListener>> {
        self.config.history_listener.take()
    }
}

#[derive(Debug, Error, PartialEq, Eq, PartialOrd, Ord)]
pub enum PartitionKeyError {
    #[error("No value with given pk_index! pk_index: {0}, values.len(): {1}")]
    NoPkIndexValue(u16, i16),
    #[error("Value bytes too long to create partition key, max 65 535 allowed! value.len(): {0}")]
    ValueTooLong(usize),
}
