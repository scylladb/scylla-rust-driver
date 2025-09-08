//! Defines the [`PreparedStatement`] type, which represents a statement
//! that has been prepared in advance on the server.

use bytes::{Bytes, BytesMut};
use scylla_cql::frame::response::result::{
    ColumnSpec, PartitionKeyIndex, ResultMetadata, TableSpec,
};
use scylla_cql::frame::types::RawValue;
use scylla_cql::serialize::row::{RowSerializationContext, SerializeRow, SerializedValues};
use scylla_cql::serialize::SerializationError;
use smallvec::{smallvec, SmallVec};
use std::convert::TryInto;
use std::sync::Arc;
use std::time::Duration;
use thiserror::Error;
use uuid::Uuid;

use super::{PageSize, StatementConfig};
use crate::client::execution_profile::ExecutionProfileHandle;
use crate::errors::{BadQuery, ExecutionError};
use crate::frame::response::result::PreparedMetadata;
use crate::frame::types::{Consistency, SerialConsistency};
use crate::observability::history::HistoryListener;
use crate::policies::load_balancing::LoadBalancingPolicy;
use crate::policies::retry::RetryPolicy;
use crate::response::query_result::ColumnSpecs;
use crate::routing::partitioner::{Partitioner, PartitionerHasher, PartitionerName};
use crate::routing::Token;

/// Represents a statement prepared on the server.
///
/// To prepare a statement, simply execute [`Session::prepare`](crate::client::session::Session::prepare).
///
/// If you plan on reusing the statement, or bounding some values to it during execution, always
/// prefer using prepared statements over `Session::query_*` methods,
/// e.g. [`Session::query_unpaged`](crate::client::session::Session::query_unpaged).
///
/// Benefits that prepared statements have to offer:
/// * Performance - a prepared statement holds information about metadata
///   that allows to carry out a statement execution in a type safe manner.
///   When any of `Session::query_*` methods is called with non-empty bound values,
///   the driver has to prepare the statement before execution (to provide type safety).
///   This implies 2 round trips per [`Session::query_unpaged`](crate::client::session::Session::query_unpaged).
///   On the other hand, the cost of [`Session::execute_unpaged`](crate::client::session::Session::execute_unpaged)
///   is only 1 round trip.
/// * Increased type-safety - bound values' types are validated with
///   the [`PreparedMetadata`] received from the server during the serialization.
/// * Improved load balancing - thanks to statement metadata, the driver is able
///   to compute a set of destined replicas for the statement execution. These replicas
///   will be preferred when choosing the node (and shard) to send the request to.
/// * Result deserialization optimization - see [`PreparedStatement::set_use_cached_result_metadata`].
///
/// # Clone implementation
/// Cloning a prepared statement is a cheap operation. It only
/// requires copying a couple of small fields and some [Arc] pointers.
/// Always prefer cloning over executing [`Session::prepare`](crate::client::session::Session::prepare)
/// multiple times to save some roundtrips.
///
/// # Statement repreparation
/// When schema is updated, the server is supposed to invalidate its
/// prepared statement caches. Then, if client tries to execute a given statement,
/// the server will respond with an error. Users should not worry about it, since
/// the driver handles it properly and tries to reprepare the statement.
/// However, there are some cases when client-side prepared statement should be dropped
/// and prepared once again via [`Session::prepare`](crate::client::session::Session::prepare) -
/// see the mention about altering schema below.
///
/// # Altering schema
/// If for some reason you decided to alter the part of schema that corresponds to given prepared
/// statement, then the corresponding statement (and its copies obtained via [`PreparedStatement::clone`]) should
/// be dropped. The statement should be prepared again.
///
/// There are two reasons for this:
///
/// ### CQL v4 protocol limitations
/// The driver only supports CQL version 4.
///
/// In multi-client scenario, only the first client which reprepares the statement
/// will receive the updated metadata from the server.
/// The rest of the clients will still hold on the outdated metadata.
/// In version 4 of CQL protocol there is currently no way for the server to notify other
/// clients about prepared statement's metadata update.
///
/// ### Client-side metadata immutability
/// The decision was made to keep client-side metadata immutable.
/// Mainly because of the CQLv4 limitations mentioned above. This means
/// that metadata is not updated during statement repreparation.
/// This raises two issues:
/// * bound values serialization errors - since [`PreparedMetadata`] is not updated
/// * result deserialization errors - when [`PreparedStatement::set_use_cached_result_metadata`] is enabled,
///   since [`ResultMetadata`] is not updated
///
/// So, to mitigate those issues, drop the outdated [`PreparedStatement`] manually
/// and prepare it again against the new schema.
#[derive(Debug)]
pub struct PreparedStatement {
    pub(crate) config: StatementConfig,
    /// Tracing IDs of all queries used to prepare this statement.
    pub prepare_tracing_ids: Vec<Uuid>,

    id: Bytes,
    shared: Arc<PreparedStatementSharedData>,
    page_size: PageSize,
    partitioner_name: PartitionerName,
    is_confirmed_lwt: bool,
}

#[derive(Debug)]
struct PreparedStatementSharedData {
    metadata: PreparedMetadata,
    result_metadata: Arc<ResultMetadata<'static>>,
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
        result_metadata: Arc<ResultMetadata<'static>>,
        statement: String,
        page_size: PageSize,
        config: StatementConfig,
    ) -> Self {
        Self {
            id,
            shared: Arc::new(PreparedStatementSharedData {
                metadata,
                result_metadata,
                statement,
            }),
            prepare_tracing_ids: Vec::new(),
            page_size,
            config,
            partitioner_name: Default::default(),
            is_confirmed_lwt: is_lwt,
        }
    }

    /// Retrieves the ID of this prepared statement.
    pub fn get_id(&self) -> &Bytes {
        &self.id
    }

    /// Retrieves the statement string of this prepared statement.
    pub fn get_statement(&self) -> &str {
        &self.shared.statement
    }

    /// Sets the page size for this CQL query.
    ///
    /// Panics if given number is nonpositive.
    pub fn set_page_size(&mut self, page_size: i32) {
        self.page_size = page_size
            .try_into()
            .unwrap_or_else(|err| panic!("PreparedStatement::set_page_size: {err}"));
    }

    /// Returns the page size for this CQL query.
    pub(crate) fn get_validated_page_size(&self) -> PageSize {
        self.page_size
    }

    /// Returns the page size for this CQL query.
    pub fn get_page_size(&self) -> i32 {
        self.page_size.inner()
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
        bound_values: &impl SerializeRow,
    ) -> Result<Bytes, PartitionKeyError> {
        let serialized = self.serialize_values(bound_values)?;
        let partition_key = self.extract_partition_key(&serialized)?;
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
    ) -> Result<PartitionKey<'ps>, PartitionKeyExtractionError> {
        PartitionKey::new(self.get_prepared_metadata(), bound_values)
    }

    pub(crate) fn extract_partition_key_and_calculate_token<'ps>(
        &'ps self,
        partitioner_name: &'ps PartitionerName,
        serialized_values: &'ps SerializedValues,
    ) -> Result<Option<(PartitionKey<'ps>, Token)>, PartitionKeyError> {
        if !self.is_token_aware() {
            return Ok(None);
        }

        let partition_key = self.extract_partition_key(serialized_values)?;
        let token = partition_key.calculate_token(partitioner_name)?;

        Ok(Some((partition_key, token)))
    }

    /// Calculates the token for given prepared statement and values.
    ///
    /// Returns the token that would be computed for executing the provided
    /// prepared statement with the provided values.
    // As this function creates a `PartitionKey`, it is intended rather for external usage (by users).
    // For internal purposes, `PartitionKey::calculate_token()` is preferred, as `PartitionKey`
    // is either way used internally, among others for display in traces.
    pub fn calculate_token(
        &self,
        values: &impl SerializeRow,
    ) -> Result<Option<Token>, PartitionKeyError> {
        self.calculate_token_untyped(&self.serialize_values(values)?)
    }

    // A version of calculate_token which skips serialization and uses SerializedValues directly.
    // Not type-safe, so not exposed to users.
    pub(crate) fn calculate_token_untyped(
        &self,
        values: &SerializedValues,
    ) -> Result<Option<Token>, PartitionKeyError> {
        self.extract_partition_key_and_calculate_token(&self.partitioner_name, values)
            .map(|opt| opt.map(|(_pk, token)| token))
    }

    /// Return keyspace name and table name this statement is operating on.
    pub fn get_table_spec(&self) -> Option<&TableSpec<'_>> {
        self.get_prepared_metadata()
            .col_specs
            .first()
            .map(|spec| spec.table_spec())
    }

    /// Returns the name of the keyspace this statement is operating on.
    pub fn get_keyspace_name(&self) -> Option<&str> {
        self.get_prepared_metadata()
            .col_specs
            .first()
            .map(|col_spec| col_spec.table_spec().ks_name())
    }

    /// Returns the name of the table this statement is operating on.
    pub fn get_table_name(&self) -> Option<&str> {
        self.get_prepared_metadata()
            .col_specs
            .first()
            .map(|col_spec| col_spec.table_spec().table_name())
    }

    /// Sets the consistency to be used when executing this statement.
    pub fn set_consistency(&mut self, c: Consistency) {
        self.config.consistency = Some(c);
    }

    /// Unsets the consistency overridden on this statement.
    /// This means that consistency will be derived from the execution profile
    /// (per-statement or, if absent, the default one).
    pub fn unset_consistency(&mut self) {
        self.config.consistency = None;
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

    /// Unsets the serial consistency overridden on this statement.
    /// This means that serial consistency will be derived from the execution profile
    /// (per-statement or, if absent, the default one).
    pub fn unset_serial_consistency(&mut self) {
        self.config.serial_consistency = None;
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

    /// Make use of cached metadata to decode results
    /// of the statement's execution.
    ///
    /// If true, the driver will request the server not to
    /// attach the result metadata in response to the statement execution.
    ///
    /// The driver will cache the result metadata received from the server
    /// after statement preparation and will use it
    /// to deserialize the results of statement execution.
    ///
    /// This option is false by default.
    pub fn set_use_cached_result_metadata(&mut self, use_cached_metadata: bool) {
        self.config.skip_result_metadata = use_cached_metadata;
    }

    /// Gets the information whether the driver uses cached metadata
    /// to decode the results of the statement's execution.
    pub fn get_use_cached_result_metadata(&self) -> bool {
        self.config.skip_result_metadata
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
    /// Otherwise, execution profile timeout will be applied.
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

    /// Access metadata about the bind variables of this statement as returned by the database
    pub(crate) fn get_prepared_metadata(&self) -> &PreparedMetadata {
        &self.shared.metadata
    }

    /// Access column specifications of the bind variables of this statement
    pub fn get_variable_col_specs(&self) -> ColumnSpecs<'_, 'static> {
        ColumnSpecs::new(&self.shared.metadata.col_specs)
    }

    /// Access info about partition key indexes of the bind variables of this statement
    pub fn get_variable_pk_indexes(&self) -> &[PartitionKeyIndex] {
        &self.shared.metadata.pk_indexes
    }

    /// Access metadata about the result of prepared statement returned by the database
    pub(crate) fn get_result_metadata(&self) -> &Arc<ResultMetadata<'static>> {
        &self.shared.result_metadata
    }

    /// Access column specifications of the result set returned after the execution of this statement
    pub fn get_result_set_col_specs(&self) -> ColumnSpecs<'_, 'static> {
        ColumnSpecs::new(self.shared.result_metadata.col_specs())
    }

    /// Get the name of the partitioner used for this statement.
    pub fn get_partitioner_name(&self) -> &PartitionerName {
        &self.partitioner_name
    }

    /// Set the retry policy for this statement, overriding the one from execution profile if not None.
    #[inline]
    pub fn set_retry_policy(&mut self, retry_policy: Option<Arc<dyn RetryPolicy>>) {
        self.config.retry_policy = retry_policy;
    }

    /// Get the retry policy set for the statement.
    ///
    /// This method returns the retry policy that is **overridden** on this statement.
    /// In other words, it returns the retry policy set using [`PreparedStatement::set_retry_policy`].
    /// This does not take the retry policy from the set execution profile into account.
    #[inline]
    pub fn get_retry_policy(&self) -> Option<&Arc<dyn RetryPolicy>> {
        self.config.retry_policy.as_ref()
    }

    /// Set the load balancing policy for this statement, overriding the one from execution profile if not None.
    #[inline]
    pub fn set_load_balancing_policy(
        &mut self,
        load_balancing_policy: Option<Arc<dyn LoadBalancingPolicy>>,
    ) {
        self.config.load_balancing_policy = load_balancing_policy;
    }

    /// Get the load balancing policy set for the statement.
    ///
    /// This method returns the load balancing policy that is **overridden** on this statement.
    /// In other words, it returns the load balancing policy set using [`PreparedStatement::set_load_balancing_policy`].
    /// This does not take the load balancing policy from the set execution profile into account.
    #[inline]
    pub fn get_load_balancing_policy(&self) -> Option<&Arc<dyn LoadBalancingPolicy>> {
        self.config.load_balancing_policy.as_ref()
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

    pub(crate) fn serialize_values(
        &self,
        values: &impl SerializeRow,
    ) -> Result<SerializedValues, SerializationError> {
        let ctx = RowSerializationContext::from_prepared(self.get_prepared_metadata());
        SerializedValues::from_serializable(&ctx, values)
    }
}

/// Error when extracting partition key from bound values.
#[derive(Clone, Debug, Error, PartialEq, Eq, PartialOrd, Ord)]
#[non_exhaustive]
pub enum PartitionKeyExtractionError {
    /// No value with given partition key index was found in bound values.
    #[error("No value with given pk_index! pk_index: {0}, values.len(): {1}")]
    NoPkIndexValue(u16, u16),
}

/// Error when calculating token from partition key values.
#[derive(Clone, Debug, Error, PartialEq, Eq, PartialOrd, Ord)]
#[non_exhaustive]
pub enum TokenCalculationError {
    /// Value was too long to be used in partition key.
    #[error("Value bytes too long to create partition key, max 65 535 allowed! value.len(): {0}")]
    ValueTooLong(usize),
}

/// An error returned by [`PreparedStatement::compute_partition_key()`].
#[derive(Clone, Debug, Error)]
#[non_exhaustive]
pub enum PartitionKeyError {
    /// Failed to extract partition key.
    #[error(transparent)]
    PartitionKeyExtraction(#[from] PartitionKeyExtractionError),

    /// Failed to calculate token.
    #[error(transparent)]
    TokenCalculation(#[from] TokenCalculationError),

    /// Failed to serialize values required to compute partition key.
    #[error(transparent)]
    Serialization(#[from] SerializationError),
}

impl PartitionKeyError {
    /// Converts the error to [`ExecutionError`].
    pub fn into_execution_error(self) -> ExecutionError {
        match self {
            PartitionKeyError::PartitionKeyExtraction(_) => {
                ExecutionError::BadQuery(BadQuery::PartitionKeyExtraction)
            }
            PartitionKeyError::TokenCalculation(TokenCalculationError::ValueTooLong(
                values_len,
            )) => {
                ExecutionError::BadQuery(BadQuery::ValuesTooLongForKey(values_len, u16::MAX.into()))
            }
            PartitionKeyError::Serialization(err) => {
                ExecutionError::BadQuery(BadQuery::SerializationError(err))
            }
        }
    }
}

pub(crate) type PartitionKeyValue<'ps> = (&'ps [u8], &'ps ColumnSpec<'ps>);

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
                    PartitionKeyExtractionError::NoPkIndexValue(
                        pk_index.index,
                        bound_values.element_count(),
                    )
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
    use scylla_cql::frame::response::result::{
        ColumnSpec, ColumnType, NativeType, PartitionKeyIndex, PreparedMetadata, TableSpec,
    };
    use scylla_cql::serialize::row::SerializedValues;

    use crate::statement::prepared::PartitionKey;
    use crate::test_utils::setup_tracing;

    fn make_meta(
        cols: impl IntoIterator<Item = ColumnType<'static>>,
        idx: impl IntoIterator<Item = usize>,
    ) -> PreparedMetadata {
        let table_spec = TableSpec::owned("ks".to_owned(), "t".to_owned());
        let col_specs: Vec<_> = cols
            .into_iter()
            .enumerate()
            .map(|(i, typ)| ColumnSpec::owned(format!("col_{i}"), typ, table_spec.clone()))
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
        setup_tracing();
        let meta = make_meta(
            [
                ColumnType::Native(NativeType::TinyInt),
                ColumnType::Native(NativeType::SmallInt),
                ColumnType::Native(NativeType::Int),
                ColumnType::Native(NativeType::BigInt),
                ColumnType::Native(NativeType::Blob),
            ],
            [4, 0, 3],
        );
        let mut values = SerializedValues::new();
        values
            .add_value(&67i8, &ColumnType::Native(NativeType::TinyInt))
            .unwrap();
        values
            .add_value(&42i16, &ColumnType::Native(NativeType::SmallInt))
            .unwrap();
        values
            .add_value(&23i32, &ColumnType::Native(NativeType::Int))
            .unwrap();
        values
            .add_value(&89i64, &ColumnType::Native(NativeType::BigInt))
            .unwrap();
        values
            .add_value(&[1u8, 2, 3, 4, 5], &ColumnType::Native(NativeType::Blob))
            .unwrap();

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
