use bytes::{Bytes, BytesMut};
use scylla_cql::errors::{BadQuery, QueryError};
use scylla_cql::frame::types::RawValue;
use smallvec::{smallvec, SmallVec};
use std::convert::TryInto;
use std::sync::Arc;
use std::time::Duration;
use thiserror::Error;
use uuid::Uuid;

use scylla_cql::frame::response::result::ColumnSpec;

use super::StatementConfig;
use crate::frame::response::result::PreparedMetadata;
use crate::frame::types::{Consistency, SerialConsistency};
use crate::frame::value::SerializedValues;
use crate::history::HistoryListener;
use crate::retry_policy::RetryPolicy;
use crate::routing::Token;
use crate::transport::execution_profile::ExecutionProfileHandle;
use crate::transport::partitioner::{Partitioner, PartitionerHasher, PartitionerName};

/// Represents a statement prepared on the server.
#[derive(Debug)]
pub struct PreparedStatement {
    pub(crate) config: StatementConfig,
    pub prepare_tracing_ids: Vec<Uuid>,

    id: Bytes,
    shared: Arc<PreparedStatementSharedData>,
    page_size: Option<i32>,
    partitioner_name: PartitionerName,
    is_confirmed_lwt: bool,
}

#[derive(Debug)]
struct PreparedStatementSharedData {
    metadata: PreparedMetadata,
    statement: String,
}

impl Clone for PreparedStatement {
    fn clone(&self) -> Self {
        Self {
            config: self.config.clone(),
            prepare_tracing_ids: Vec::new(),
            id: self.id.clone(),
            shared: self.shared.clone(),
            page_size: self.page_size,
            partitioner_name: self.partitioner_name.clone(),
            is_confirmed_lwt: self.is_confirmed_lwt,
        }
    }
}

impl PreparedStatement {
    pub(crate) fn new(
        id: Bytes,
        is_lwt: bool,
        metadata: PreparedMetadata,
        statement: String,
        page_size: Option<i32>,
        config: StatementConfig,
    ) -> Self {
        Self {
            id,
            shared: Arc::new(PreparedStatementSharedData {
                metadata,
                statement,
            }),
            prepare_tracing_ids: Vec::new(),
            page_size,
            config,
            partitioner_name: Default::default(),
            is_confirmed_lwt: is_lwt,
        }
    }

    pub fn get_id(&self) -> &Bytes {
        &self.id
    }

    pub fn get_statement(&self) -> &str {
        &self.shared.statement
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
        !self.get_prepared_metadata().pk_indexes.is_empty()
    }

    /// Returns true if it is known that the prepared statement contains
    /// a Lightweight Transaction. If so, the optimisation can be performed:
    /// the query should be routed to the replicas in a predefined order
    /// (i. e. always try first to contact replica A, then B if it fails,
    /// then C, etc.). If false, the query should be routed normally.
    /// Note: this a Scylla-specific optimisation. Therefore, the result
    /// will be always false for Cassandra.
    pub fn is_confirmed_lwt(&self) -> bool {
        self.is_confirmed_lwt
    }

    /// Computes the partition key of the target table from given values —
    /// it assumes that all partition key columns are passed in values.
    /// Partition keys have specific serialization rules.
    /// Ref: <https://github.com/scylladb/scylla/blob/40adf38915b6d8f5314c621a94d694d172360833/compound_compat.hh#L33-L47>
    ///
    /// Note: this is no longer required to compute a token. This is because partitioners
    /// are now stateful and stream-based, so they do not require materialised partition key.
    /// Therefore, for computing a token, there are more efficient methods, such as
    /// [Self::calculate_token()].
    pub fn compute_partition_key(
        &self,
        bound_values: &SerializedValues,
    ) -> Result<Bytes, PartitionKeyError> {
        let partition_key = self.extract_partition_key(bound_values)?;
        let mut buf = BytesMut::new();
        let mut writer = |chunk: &[u8]| buf.extend_from_slice(chunk);

        partition_key.write_encoded_partition_key(&mut writer)?;

        Ok(buf.freeze())
    }

    /// Determines which values constitute the partition key and puts them in order.
    ///
    /// This is a preparation step necessary for calculating token based on a prepared statement.
    pub(crate) fn extract_partition_key<'ps>(
        &'ps self,
        bound_values: &'ps SerializedValues,
    ) -> Result<PartitionKey, PartitionKeyExtractionError> {
        PartitionKey::new(self.get_prepared_metadata(), bound_values)
    }

    pub(crate) fn extract_partition_key_and_calculate_token<'ps>(
        &'ps self,
        partitioner_name: &'ps PartitionerName,
        serialized_values: &'ps SerializedValues,
    ) -> Result<Option<(PartitionKey<'ps>, Token)>, QueryError> {
        if !self.is_token_aware() {
            return Ok(None);
        }

        let partition_key =
            self.extract_partition_key(serialized_values)
                .map_err(|err| match err {
                    PartitionKeyExtractionError::NoPkIndexValue(_, _) => {
                        QueryError::ProtocolError("No pk indexes - can't calculate token")
                    }
                })?;
        let token = partition_key
            .calculate_token(partitioner_name)
            .map_err(|err| match err {
                TokenCalculationError::ValueTooLong(values_len) => {
                    QueryError::BadQuery(BadQuery::ValuesTooLongForKey(values_len, u16::MAX.into()))
                }
            })?;

        Ok(Some((partition_key, token)))
    }

    /// Calculates the token for given prepared statement and serialized values.
    ///
    /// Returns the token that would be computed for executing the provided
    /// prepared statement with the provided values.
    // As this function creates a `PartitionKey`, it is intended rather for external usage (by users).
    // For internal purposes, `PartitionKey::calculate_token()` is preferred, as `PartitionKey`
    // is either way used internally, among others for display in traces.
    pub fn calculate_token(
        &self,
        serialized_values: &SerializedValues,
    ) -> Result<Option<Token>, QueryError> {
        self.extract_partition_key_and_calculate_token(&self.partitioner_name, serialized_values)
            .map(|opt| opt.map(|(_pk, token)| token))
    }

    /// Returns the name of the keyspace this statement is operating on.
    pub fn get_keyspace_name(&self) -> Option<&str> {
        self.get_prepared_metadata()
            .col_specs
            .first()
            .map(|col_spec| col_spec.table_spec.ks_name.as_str())
    }

    /// Returns the name of the table this statement is operating on.
    pub fn get_table_name(&self) -> Option<&str> {
        self.get_prepared_metadata()
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
    pub(crate) fn set_partitioner_name(&mut self, partitioner_name: PartitionerName) {
        self.partitioner_name = partitioner_name;
    }

    /// Access metadata about this prepared statement as returned by the database
    pub fn get_prepared_metadata(&self) -> &PreparedMetadata {
        &self.shared.metadata
    }

    /// Get the name of the partitioner used for this statement.
    pub(crate) fn get_partitioner_name(&self) -> &PartitionerName {
        &self.partitioner_name
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

#[derive(Clone, Debug, Error, PartialEq, Eq, PartialOrd, Ord)]
pub enum PartitionKeyExtractionError {
    #[error("No value with given pk_index! pk_index: {0}, values.len(): {1}")]
    NoPkIndexValue(u16, u16),
}

#[derive(Clone, Debug, Error, PartialEq, Eq, PartialOrd, Ord)]
pub enum TokenCalculationError {
    #[error("Value bytes too long to create partition key, max 65 535 allowed! value.len(): {0}")]
    ValueTooLong(usize),
}

#[derive(Clone, Debug, Error, PartialEq, Eq, PartialOrd, Ord)]
pub enum PartitionKeyError {
    #[error(transparent)]
    PartitionKeyExtraction(PartitionKeyExtractionError),
    #[error(transparent)]
    TokenCalculation(TokenCalculationError),
}

impl From<PartitionKeyExtractionError> for PartitionKeyError {
    fn from(err: PartitionKeyExtractionError) -> Self {
        Self::PartitionKeyExtraction(err)
    }
}

impl From<TokenCalculationError> for PartitionKeyError {
    fn from(err: TokenCalculationError) -> Self {
        Self::TokenCalculation(err)
    }
}

pub(crate) type PartitionKeyValue<'ps> = (&'ps [u8], &'ps ColumnSpec);

pub(crate) struct PartitionKey<'ps> {
    pk_values: SmallVec<[Option<PartitionKeyValue<'ps>>; PartitionKey::SMALLVEC_ON_STACK_SIZE]>,
}

impl<'ps> PartitionKey<'ps> {
    const SMALLVEC_ON_STACK_SIZE: usize = 8;

    fn new(
        prepared_metadata: &'ps PreparedMetadata,
        bound_values: &'ps SerializedValues,
    ) -> Result<Self, PartitionKeyExtractionError> {
        // Iterate on values using sorted pk_indexes (see deser_prepared_metadata),
        // and use PartitionKeyIndex.sequence to insert the value in pk_values with the correct order.
        let mut pk_values: SmallVec<[_; PartitionKey::SMALLVEC_ON_STACK_SIZE]> =
            smallvec![None; prepared_metadata.pk_indexes.len()];
        let mut values_iter = bound_values.iter();
        // pk_indexes contains the indexes of the partition key value, so the current offset of the
        // iterator must be kept, in order to compute the next position of the pk in the iterator.
        // At each iteration values_iter.nth(0) will roughly correspond to values[values_iter_offset],
        // so values[pk_index.index] will be retrieved with values_iter.nth(pk_index.index - values_iter_offset)
        let mut values_iter_offset = 0;
        for pk_index in prepared_metadata.pk_indexes.iter().copied() {
            // Find value matching current pk_index
            let next_val = values_iter
                .nth((pk_index.index - values_iter_offset) as usize)
                .ok_or_else(|| {
                    PartitionKeyExtractionError::NoPkIndexValue(pk_index.index, bound_values.len())
                })?;
            // Add it in sequence order to pk_values
            if let RawValue::Value(v) = next_val {
                let spec = &prepared_metadata.col_specs[pk_index.index as usize];
                pk_values[pk_index.sequence as usize] = Some((v, spec));
            }
            values_iter_offset = pk_index.index + 1;
        }
        Ok(Self { pk_values })
    }

    pub(crate) fn iter(&self) -> impl Iterator<Item = PartitionKeyValue<'ps>> + Clone + '_ {
        self.pk_values.iter().flatten().copied()
    }

    fn write_encoded_partition_key(
        &self,
        writer: &mut impl FnMut(&[u8]),
    ) -> Result<(), TokenCalculationError> {
        let mut pk_val_iter = self.iter().map(|(val, _spec)| val);
        if let Some(first_value) = pk_val_iter.next() {
            if let Some(second_value) = pk_val_iter.next() {
                // Composite partition key case
                for value in std::iter::once(first_value)
                    .chain(std::iter::once(second_value))
                    .chain(pk_val_iter)
                {
                    let v_len_u16: u16 = value
                        .len()
                        .try_into()
                        .map_err(|_| TokenCalculationError::ValueTooLong(value.len()))?;
                    writer(&v_len_u16.to_be_bytes());
                    writer(value);
                    writer(&[0u8]);
                }
            } else {
                // Single-value partition key case
                writer(first_value);
            }
        }
        Ok(())
    }

    pub(crate) fn calculate_token(
        &self,
        partitioner_name: &PartitionerName,
    ) -> Result<Token, TokenCalculationError> {
        let mut partitioner_hasher = partitioner_name.build_hasher();
        let mut writer = |chunk: &[u8]| partitioner_hasher.write(chunk);

        self.write_encoded_partition_key(&mut writer)?;

        Ok(partitioner_hasher.finish())
    }
}

#[cfg(test)]
mod tests {
    use scylla_cql::frame::{
        response::result::{
            ColumnSpec, ColumnType, PartitionKeyIndex, PreparedMetadata, TableSpec,
        },
        value::SerializedValues,
    };

    use crate::prepared_statement::PartitionKey;

    fn make_meta(
        cols: impl IntoIterator<Item = ColumnType>,
        idx: impl IntoIterator<Item = usize>,
    ) -> PreparedMetadata {
        let table_spec = TableSpec {
            ks_name: "ks".to_owned(),
            table_name: "t".to_owned(),
        };
        let col_specs: Vec<_> = cols
            .into_iter()
            .enumerate()
            .map(|(i, typ)| ColumnSpec {
                name: format!("col_{}", i),
                table_spec: table_spec.clone(),
                typ,
            })
            .collect();
        let mut pk_indexes = idx
            .into_iter()
            .enumerate()
            .map(|(sequence, index)| PartitionKeyIndex {
                index: index as u16,
                sequence: sequence as u16,
            })
            .collect::<Vec<_>>();
        pk_indexes.sort_unstable_by_key(|pki| pki.index);
        PreparedMetadata {
            flags: 0,
            col_count: col_specs.len(),
            col_specs,
            pk_indexes,
        }
    }

    #[test]
    fn test_partition_key_multiple_columns_shuffled() {
        let meta = make_meta(
            [
                ColumnType::TinyInt,
                ColumnType::SmallInt,
                ColumnType::Int,
                ColumnType::BigInt,
                ColumnType::Blob,
            ],
            [4, 0, 3],
        );
        let mut values = SerializedValues::new();
        values.add_value(&67i8).unwrap();
        values.add_value(&42i16).unwrap();
        values.add_value(&23i32).unwrap();
        values.add_value(&89i64).unwrap();
        values.add_value(&[1u8, 2, 3, 4, 5]).unwrap();

        let pk = PartitionKey::new(&meta, &values).unwrap();
        let pk_cols = Vec::from_iter(pk.iter());
        assert_eq!(
            pk_cols,
            vec![
                ([1u8, 2, 3, 4, 5].as_slice(), &meta.col_specs[4]),
                (67i8.to_be_bytes().as_ref(), &meta.col_specs[0]),
                (89i64.to_be_bytes().as_ref(), &meta.col_specs[3]),
            ]
        );
    }
}
