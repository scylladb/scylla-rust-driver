use byteorder::{BigEndian, ReadBytesExt};
use bytes::{BufMut, Bytes, BytesMut};
use smallvec::{smallvec, SmallVec};
use std::convert::TryInto;
use std::sync::Arc;
use std::time::Duration;
use thiserror::Error;
use uuid::Uuid;

use scylla_cql::frame::frame_errors::ParseError;
use scylla_cql::frame::response::result::{deser_cql_value, CqlValue};

use super::StatementConfig;
use crate::frame::response::result::PreparedMetadata;
use crate::frame::types::{Consistency, SerialConsistency};
use crate::frame::value::SerializedValues;
use crate::history::HistoryListener;
use crate::retry_policy::RetryPolicy;
use crate::transport::execution_profile::ExecutionProfileHandle;
use crate::transport::partitioner::PartitionerName;

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
    pub fn compute_partition_key(
        &self,
        bound_values: &SerializedValues,
    ) -> Result<Bytes, PartitionKeyError> {
        let mut buf = BytesMut::new();

        // Single-value partition key case
        if self.get_prepared_metadata().pk_indexes.len() == 1 {
            let pk_index = self.get_prepared_metadata().pk_indexes[0].index;
            if let Some(v) = bound_values.iter().nth(pk_index as usize).ok_or_else(|| {
                PartitionKeyExtractionError::NoPkIndexValue(pk_index, bound_values.len())
            })? {
                buf.extend_from_slice(v);
            }
        } else {
            // Composite partition key case

            // Iterate on values using sorted pk_indexes (see deser_prepared_metadata),
            // and use PartitionKeyIndex.sequence to insert the value in pk_values with the correct order.
            // At the same time, compute the size of the buffer to reserve it before writing in it.
            let mut pk_values: SmallVec<[_; 8]> =
                smallvec![None; self.get_prepared_metadata().pk_indexes.len()];
            let mut values_iter = bound_values.iter();
            let mut buf_size = 0;
            // pk_indexes contains the indexes of the partition key value, so the current offset of the
            // iterator must be kept, in order to compute the next position of the pk in the iterator.
            // At each iteration values_iter.nth(0) will roughly correspond to values[values_iter_offset],
            // so values[pk_index.index] will be retrieved with values_iter.nth(pk_index.index - values_iter_offset)
            let mut values_iter_offset = 0;
            for pk_index in self.get_prepared_metadata().pk_indexes.iter().copied() {
                // Find value matching current pk_index
                let next_val = values_iter
                    .nth((pk_index.index - values_iter_offset) as usize)
                    .ok_or_else(|| {
                        PartitionKeyExtractionError::NoPkIndexValue(
                            pk_index.index,
                            bound_values.len(),
                        )
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
                    .map_err(|_| TokenCalculationError::ValueTooLong(v.len()))?;

                buf.put_u16(v_len_u16);
                buf.extend_from_slice(v);
                buf.put_u8(0);
            }
        }
        Ok(buf.into())
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
    /// This is used in [`RetryPolicy`](crate::retry_policy::RetryPolicy) to decide if retrying a query is safe
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
    NoPkIndexValue(u16, i16),
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

// The PartitionKeyDecoder reverses the process of PreparedStatement::compute_partition_key:
// it returns the consecutive values of partition key column that were encoded
// by the function into the Bytes object, additionally decoding them to CqlValue.
//
// The format follows the description here:
// <https://github.com/scylladb/scylla/blob/40adf38915b6d8f5314c621a94d694d172360833/compound_compat.hh#L33-L47>
//
// TODO: Currently, if there is a null value specified for a partition column,
// it will be skipped when creating the serialized partition key. We should
// not create such partition keys in the future, i.e. fail the request or
// route it to a random replica instead and let the DB reject it. In the
// meantime, this struct will return some garbage data while it tries to
// decode the key, but nothing bad like a panic should happen otherwise.
// The struct is currently only used for printing the partition key, so that's
// completely fine.
#[derive(Clone, Copy)]
pub(crate) struct PartitionKeyDecoder<'pk> {
    prepared_metadata: &'pk PreparedMetadata,
    partition_key: &'pk [u8],
    value_index: usize,
}

impl<'pk> PartitionKeyDecoder<'pk> {
    pub(crate) fn new(prepared_metadata: &'pk PreparedMetadata, partition_key: &'pk [u8]) -> Self {
        Self {
            prepared_metadata,
            partition_key,
            value_index: 0,
        }
    }
}

impl<'pk> Iterator for PartitionKeyDecoder<'pk> {
    type Item = Result<CqlValue, ParseError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.value_index >= self.prepared_metadata.pk_indexes.len() {
            return None;
        }

        // It should be safe to unwrap because PartitionKeyIndex::sequence
        // fields of the pk_indexes form a permutation, but let's err
        // on the safe side in case of bugs.
        let pk_index = self
            .prepared_metadata
            .pk_indexes
            .iter()
            .find(|pki| pki.sequence == self.value_index as u16)?;

        let col_idx = pk_index.index as usize;
        let spec = &self.prepared_metadata.col_specs[col_idx];

        let cell = if self.prepared_metadata.pk_indexes.len() == 1 {
            Ok(self.partition_key)
        } else {
            self.parse_cell()
        };
        self.value_index += 1;
        Some(cell.and_then(|mut cell| deser_cql_value(&spec.typ, &mut cell)))
    }
}

impl<'pk> PartitionKeyDecoder<'pk> {
    fn parse_cell(&mut self) -> Result<&'pk [u8], ParseError> {
        let buf = &mut self.partition_key;
        let len = buf.read_u16::<BigEndian>()? as usize;
        if buf.len() < len {
            return Err(ParseError::IoError(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "value too short",
            )));
        }
        let col = &buf[..len];
        *buf = &buf[len..];
        buf.read_u8()?;
        Ok(col)
    }
}

#[cfg(test)]
mod tests {
    use bytes::{BufMut, Bytes, BytesMut};
    use scylla_cql::frame::response::result::{
        ColumnSpec, ColumnType, CqlValue, PartitionKeyIndex, PreparedMetadata, TableSpec,
    };

    use super::PartitionKeyDecoder;

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

    fn make_key<'pk>(cols: impl IntoIterator<Item = &'pk [u8]>) -> Bytes {
        let cols: Vec<_> = cols.into_iter().collect();
        // TODO: Use compute_partition_key or one of the variants
        // after they are moved to a more sensible place
        // instead of constructing the PK manually
        let mut b = BytesMut::new();
        if cols.len() == 1 {
            b.extend_from_slice(cols[0]);
        } else {
            for c in cols {
                b.put_i16(c.len() as i16);
                b.extend_from_slice(c);
                b.put_u8(0);
            }
        }
        b.freeze()
    }

    #[test]
    fn test_pk_decoder_single_column() {
        let meta = make_meta([ColumnType::Int], [0]);
        let pk = make_key([0i32.to_be_bytes().as_ref()]);
        let cells = PartitionKeyDecoder::new(&meta, &pk)
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(cells, vec![CqlValue::Int(0)]);
    }

    #[test]
    fn test_pk_decoder_multiple_columns() {
        let meta = make_meta(std::iter::repeat(ColumnType::Int).take(3), [0, 1, 2]);
        let pk = make_key([
            12i32.to_be_bytes().as_ref(),
            34i32.to_be_bytes().as_ref(),
            56i32.to_be_bytes().as_ref(),
        ]);
        let cells = PartitionKeyDecoder::new(&meta, &pk)
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(
            cells,
            vec![CqlValue::Int(12), CqlValue::Int(34), CqlValue::Int(56)],
        );
    }

    #[test]
    fn test_pk_decoder_multiple_columns_shuffled() {
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
        let pk = make_key([
            &[1, 2, 3, 4, 5],
            67i8.to_be_bytes().as_ref(),
            89i64.to_be_bytes().as_ref(),
        ]);
        let cells = PartitionKeyDecoder::new(&meta, &pk)
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(
            cells,
            vec![
                CqlValue::Blob(vec![1, 2, 3, 4, 5]),
                CqlValue::TinyInt(67),
                CqlValue::BigInt(89),
            ],
        );
    }
}
