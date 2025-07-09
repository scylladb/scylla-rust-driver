use std::borrow::Cow;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use scylla_cql::frame::frame_errors::{
    BatchSerializationError, BatchStatementSerializationError, CqlRequestSerializationError,
};
use scylla_cql::frame::request;
use scylla_cql::serialize::batch::{BatchValues, BatchValuesIterator};
use scylla_cql::serialize::row::{RowSerializationContext, SerializeRow, SerializedValues};
use scylla_cql::serialize::{RowWriter, SerializationError};
use thiserror::Error;
use tracing::Instrument;

use crate::client::execution_profile::{ExecutionProfileHandle, ExecutionProfileInner};
use crate::client::session::{RunRequestResult, Session};
use crate::errors::{BadQuery, ExecutionError, RequestAttemptError};
use crate::network::Connection;
use crate::observability::driver_tracing::RequestSpan;
use crate::observability::history::HistoryListener;
use crate::policies::load_balancing::LoadBalancingPolicy;
use crate::policies::load_balancing::RoutingInfo;
use crate::policies::retry::RetryPolicy;
use crate::response::query_result::QueryResult;
use crate::response::{Coordinator, NonErrorQueryResponse, QueryResponse};
use crate::routing::Token;
use crate::statement::prepared::{PartitionKeyError, PreparedStatement};
use crate::statement::unprepared::Statement;

use super::bound::BoundStatement;
use super::execute::Execute;
use super::StatementConfig;
use super::{Consistency, SerialConsistency};
pub use crate::frame::request::batch::BatchType;

/// CQL batch statement.
///
/// This represents a CQL batch that can be executed on a server.
#[derive(Clone)]
pub struct Batch {
    pub(crate) config: StatementConfig,

    pub statements: Vec<BatchStatement>,
    batch_type: BatchType,
}

impl Batch {
    /// Creates a new, empty `Batch` of `batch_type` type.
    pub fn new(batch_type: BatchType) -> Self {
        Self {
            batch_type,
            ..Default::default()
        }
    }

    /// Creates an empty batch, with the configuration of existing batch.
    pub(crate) fn new_from(batch: &Batch) -> Batch {
        let batch_type = batch.get_type();
        let config = batch.config.clone();
        Batch {
            batch_type,
            config,
            ..Default::default()
        }
    }

    /// Creates a new, empty `Batch` of `batch_type` type with the provided statements.
    pub fn new_with_statements(batch_type: BatchType, statements: Vec<BatchStatement>) -> Self {
        Self {
            batch_type,
            statements,
            ..Default::default()
        }
    }

    /// Appends a new statement to the batch.
    pub fn append_statement(&mut self, statement: impl Into<BatchStatement>) {
        self.statements.push(statement.into());
    }

    /// Gets type of batch.
    pub fn get_type(&self) -> BatchType {
        self.batch_type
    }

    /// Sets the consistency to be used when executing this batch.
    pub fn set_consistency(&mut self, c: Consistency) {
        self.config.consistency = Some(c);
    }

    /// Unsets the consistency overridden on this batch.
    /// This means that consistency will be derived from the execution profile
    /// (per-batch or, if absent, the default one).
    pub fn unset_consistency(&mut self) {
        self.config.consistency = None;
    }

    /// Gets the consistency to be used when executing this batch if it is filled.
    /// If this is empty, the default_consistency of the session will be used.
    pub fn get_consistency(&self) -> Option<Consistency> {
        self.config.consistency
    }

    /// Sets the serial consistency to be used when executing this batch.
    /// (Ignored unless the batch is an LWT)
    pub fn set_serial_consistency(&mut self, sc: Option<SerialConsistency>) {
        self.config.serial_consistency = Some(sc);
    }

    /// Unsets the serial consistency overridden on this batch.
    /// This means that serial consistency will be derived from the execution profile
    /// (per-batch or, if absent, the default one).
    pub fn unset_serial_consistency(&mut self) {
        self.config.serial_consistency = None;
    }

    /// Gets the serial consistency to be used when executing this batch.
    /// (Ignored unless the batch is an LWT)
    pub fn get_serial_consistency(&self) -> Option<SerialConsistency> {
        self.config.serial_consistency.flatten()
    }

    /// Sets the idempotence of this batch
    /// A query is idempotent if it can be applied multiple times without changing the result of the initial application
    /// If set to `true` we can be sure that it is idempotent
    /// If set to `false` it is unknown whether it is idempotent
    /// This is used in [`RetryPolicy`] to decide if retrying a query is safe
    pub fn set_is_idempotent(&mut self, is_idempotent: bool) {
        self.config.is_idempotent = is_idempotent;
    }

    /// Gets the idempotence of this batch
    pub fn get_is_idempotent(&self) -> bool {
        self.config.is_idempotent
    }

    /// Enable or disable CQL Tracing for this batch
    /// If enabled session.batch() will return a QueryResult containing tracing_id
    /// which can be used to query tracing information about the execution of this query
    pub fn set_tracing(&mut self, should_trace: bool) {
        self.config.tracing = should_trace;
    }

    /// Gets whether tracing is enabled for this batch
    pub fn get_tracing(&self) -> bool {
        self.config.tracing
    }

    /// Sets the default timestamp for this batch in microseconds.
    /// If not None, it will replace the server side assigned timestamp as default timestamp for
    /// all the statements contained in the batch.
    pub fn set_timestamp(&mut self, timestamp: Option<i64>) {
        self.config.timestamp = timestamp
    }

    /// Gets the default timestamp for this batch in microseconds.
    pub fn get_timestamp(&self) -> Option<i64> {
        self.config.timestamp
    }

    /// Sets the client-side timeout for this batch.
    /// If not None, the driver will stop waiting for the request
    /// to finish after `timeout` passed.
    /// Otherwise, execution profile timeout will be applied.
    pub fn set_request_timeout(&mut self, timeout: Option<Duration>) {
        self.config.request_timeout = timeout
    }

    /// Gets client timeout associated with this batch.
    pub fn get_request_timeout(&self) -> Option<Duration> {
        self.config.request_timeout
    }

    /// Set the retry policy for this batch, overriding the one from execution profile if not None.
    #[inline]
    pub fn set_retry_policy(&mut self, retry_policy: Option<Arc<dyn RetryPolicy>>) {
        self.config.retry_policy = retry_policy;
    }

    /// Get the retry policy set for the batch.
    ///
    /// This method returns the retry policy that is **overridden** on this statement.
    /// In other words, it returns the retry policy set using [`Batch::set_retry_policy`].
    /// This does not take the retry policy from the set execution profile into account.
    #[inline]
    pub fn get_retry_policy(&self) -> Option<&Arc<dyn RetryPolicy>> {
        self.config.retry_policy.as_ref()
    }

    /// Set the load balancing policy for this batch, overriding the one from execution profile if not None.
    #[inline]
    pub fn set_load_balancing_policy(
        &mut self,
        load_balancing_policy: Option<Arc<dyn LoadBalancingPolicy>>,
    ) {
        self.config.load_balancing_policy = load_balancing_policy;
    }

    /// Get the load balancing policy set for the batch.
    ///
    /// This method returns the load balancing policy that is **overridden** on this statement.
    /// In other words, it returns the load balancing policy set using [`Batch::set_load_balancing_policy`].
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

    /// Associates the batch with execution profile referred by the provided handle.
    /// Handle may be later remapped to another profile, and batch will reflect those changes.
    pub fn set_execution_profile_handle(&mut self, profile_handle: Option<ExecutionProfileHandle>) {
        self.config.execution_profile_handle = profile_handle;
    }

    /// Borrows the execution profile handle associated with this batch.
    pub fn get_execution_profile_handle(&self) -> Option<&ExecutionProfileHandle> {
        self.config.execution_profile_handle.as_ref()
    }
}

impl Default for Batch {
    fn default() -> Self {
        Self {
            statements: Vec::new(),
            batch_type: BatchType::Logged,
            config: Default::default(),
        }
    }
}

/// This enum represents a CQL statement, that can be part of batch.
#[derive(Clone)]
#[non_exhaustive]
pub enum BatchStatement {
    Query(Statement),
    PreparedStatement(PreparedStatement),
}

impl From<&str> for BatchStatement {
    fn from(s: &str) -> Self {
        BatchStatement::Query(Statement::from(s))
    }
}

impl From<Statement> for BatchStatement {
    fn from(q: Statement) -> Self {
        BatchStatement::Query(q)
    }
}

impl From<PreparedStatement> for BatchStatement {
    fn from(p: PreparedStatement) -> Self {
        BatchStatement::PreparedStatement(p)
    }
}

impl<'a: 'b, 'b> From<&'a BatchStatement>
    for scylla_cql::frame::request::batch::BatchStatement<'b>
{
    fn from(val: &'a BatchStatement) -> Self {
        match val {
            BatchStatement::Query(query) => {
                scylla_cql::frame::request::batch::BatchStatement::Query {
                    text: Cow::Borrowed(&query.contents),
                }
            }
            BatchStatement::PreparedStatement(prepared) => {
                scylla_cql::frame::request::batch::BatchStatement::Prepared {
                    id: Cow::Borrowed(prepared.get_id()),
                }
            }
        }
    }
}

/// A batch with all of its statements bound to values
pub struct BoundBatch {
    pub(crate) config: StatementConfig,
    batch_type: BatchType,
    pub(crate) buffer: Vec<u8>,
    pub(crate) prepared: HashMap<Bytes, PreparedStatement>,
    first_prepared: Option<(PreparedStatement, Token)>,
    statements_len: u16,
}

impl BoundBatch {
    pub fn new(batch_type: BatchType) -> Self {
        Self {
            batch_type,
            ..Default::default()
        }
    }

    /// Appends a new statement to the batch.
    pub fn append_statement<'p, V: SerializeRow>(
        &mut self,
        statement: impl Into<BoundBatchStatement<'p, V>>,
    ) -> Result<(), BoundBatchStatementError> {
        let initial_len = self.buffer.len();
        self.raw_append_statement(statement).inspect_err(|_| {
            // if we error'd at any point we should put the buffer back to its old length to not
            // corrupt the buffer in case the user doesn't drop the boundbatch but instead skips and
            // tries with a successful statement later
            self.buffer.truncate(initial_len);
        })
    }

    #[allow(clippy::result_large_err)]
    pub(crate) fn from_batch(
        batch: &Batch,
        values: impl BatchValues,
    ) -> Result<Self, ExecutionError> {
        let mut bound_batch = BoundBatch {
            config: batch.config.clone(),
            batch_type: batch.batch_type,
            statements_len: batch.statements.len().try_into().map_err(|_| {
                ExecutionError::BadQuery(BadQuery::TooManyQueriesInBatchStatement(
                    batch.statements.len(),
                ))
            })?,
            ..Default::default()
        };

        let mut values = values.batch_values_iter();
        let mut statements = batch.statements.iter().enumerate();

        if let Some((idx, statement)) = statements.next() {
            match statement {
                BatchStatement::Query(_) => {
                    bound_batch.serialize_from_batch_statement(statement, idx, |writer| {
                        let ctx = RowSerializationContext::empty();
                        values.serialize_next(&ctx, writer).transpose()
                    })?;
                }
                BatchStatement::PreparedStatement(ps) => {
                    let values =
                        bound_batch.serialize_from_batch_statement(statement, idx, |writer| {
                            let ctx =
                                RowSerializationContext::from_prepared(ps.get_prepared_metadata());

                            let values = SerializedValues::from_closure(|writer| {
                                values.serialize_next(&ctx, writer).transpose()
                            })
                            .map(|(values, opt)| opt.map(|_| values));

                            if let Ok(Some(values)) = &values {
                                writer.append_serialize_row(values);
                            }

                            values
                        })?;

                    let bound = BoundStatement::new_untyped(Cow::Borrowed(ps), values);
                    let token = bound
                        .token()
                        .map_err(PartitionKeyError::into_execution_error)?;

                    let prepared = bound.prepared.into_owned();
                    bound_batch.first_prepared = token.map(|token| (prepared.clone(), token));
                    bound_batch
                        .prepared
                        .insert(prepared.get_id().to_owned(), prepared);
                }
            }
        }

        for (idx, statement) in statements {
            bound_batch.serialize_from_batch_statement(statement, idx, |writer| {
                let ctx = match statement {
                    BatchStatement::Query(_) => RowSerializationContext::empty(),
                    BatchStatement::PreparedStatement(ps) => {
                        RowSerializationContext::from_prepared(ps.get_prepared_metadata())
                    }
                };
                values.serialize_next(&ctx, writer).transpose()
            })?;

            if let BatchStatement::PreparedStatement(ps) = statement {
                if !bound_batch.prepared.contains_key(ps.get_id()) {
                    bound_batch
                        .prepared
                        .insert(ps.get_id().to_owned(), ps.clone());
                }
            }
        }

        // At this point, we have all statements serialized. If any values are still left, we have a mismatch.
        if values.skip_next().is_some() {
            return Err(ExecutionError::LastAttemptError(
                RequestAttemptError::CqlRequestSerialization(
                    CqlRequestSerializationError::BatchSerialization(counts_mismatch_err(
                        bound_batch.statements_len as usize + 1 /*skipped above*/ + values.count(),
                        bound_batch.statements_len,
                    )),
                ),
            ));
        }

        Ok(bound_batch)
    }

    /// Borrows the execution profile handle associated with this batch.
    pub fn get_execution_profile_handle(&self) -> Option<&ExecutionProfileHandle> {
        self.config.execution_profile_handle.as_ref()
    }

    /// Gets the default timestamp for this batch in microseconds.
    pub fn get_timestamp(&self) -> Option<i64> {
        self.config.timestamp
    }

    /// Gets type of batch.
    pub fn get_type(&self) -> BatchType {
        self.batch_type
    }

    pub fn statements_len(&self) -> u16 {
        self.statements_len
    }

    // **IMPORTANT NOTE**: It is OK for this function to append to the buffer even if it errors
    // because the caller will fix the buffer, HOWEVER, it is *NOT OK* for *ANY* other field in
    // `self` to be modified if an error occured because the caller will not reset them.
    fn raw_append_statement<'p, V: SerializeRow>(
        &mut self,
        statement: impl Into<BoundBatchStatement<'p, V>>,
    ) -> Result<(), BoundBatchStatementError> {
        let mut statement = statement.into();
        let mut first_prepared = None;

        if self.statements_len == 0 {
            // save it into a local variable for now in case a latter steps fails
            first_prepared = match statement {
                BoundBatchStatement::Bound(ref b) => b
                    .token()?
                    .map(|token| (b.prepared.clone().into_owned(), token)),
                BoundBatchStatement::Prepared(ps, values) => {
                    let bound = ps
                        .into_bind(&values)
                        .map_err(BatchStatementSerializationError::ValuesSerialiation)?;
                    let first_prepared = bound
                        .token()?
                        .map(|token| (bound.prepared.clone().into_owned(), token));
                    // we already serialized it so to avoid re-serializing it, modify the statement to a
                    // BoundStatement
                    statement = BoundBatchStatement::Bound(bound);
                    first_prepared
                }
                BoundBatchStatement::Query(_) => None,
            };
        }

        let stmnt = match &statement {
            BoundBatchStatement::Prepared(ps, _) => request::batch::BatchStatement::Prepared {
                id: Cow::Borrowed(ps.get_id()),
            },
            BoundBatchStatement::Bound(b) => request::batch::BatchStatement::Prepared {
                id: Cow::Borrowed(b.prepared.get_id()),
            },
            BoundBatchStatement::Query(q) => request::batch::BatchStatement::Query {
                text: Cow::Borrowed(&q.contents),
            },
        };

        serialize_statement(stmnt, &mut self.buffer, |writer| match &statement {
            BoundBatchStatement::Prepared(ps, values) => {
                let ctx = RowSerializationContext::from_prepared(ps.get_prepared_metadata());
                values.serialize(&ctx, writer).map(Some)
            }
            BoundBatchStatement::Bound(b) => {
                writer.append_serialize_row(&b.values);
                Ok(Some(()))
            }
            // query has no values
            BoundBatchStatement::Query(_) => Ok(Some(())),
        })?;

        let new_statements_len = self
            .statements_len
            .checked_add(1)
            .ok_or(BoundBatchStatementError::TooManyQueriesInBatchStatement)?;

        /*** at this point nothing else should be fallible as we are going to be modifying
         * fields that do not get reset ***/

        self.statements_len = new_statements_len;

        if let Some(first_prepared) = first_prepared {
            self.first_prepared = Some(first_prepared);
        }

        let prepared = match statement {
            BoundBatchStatement::Prepared(ps, _) => Cow::Owned(ps),
            BoundBatchStatement::Bound(b) => b.prepared,
            BoundBatchStatement::Query(_) => return Ok(()),
        };

        if !self.prepared.contains_key(prepared.get_id()) {
            self.prepared
                .insert(prepared.get_id().to_owned(), prepared.into_owned());
        }

        Ok(())
    }

    #[allow(clippy::result_large_err)]
    fn serialize_from_batch_statement<T>(
        &mut self,
        statement: &BatchStatement,
        statement_idx: usize,
        serialize: impl FnOnce(&mut RowWriter<'_>) -> Result<Option<T>, SerializationError>,
    ) -> Result<T, ExecutionError> {
        serialize_statement(
            request::batch::BatchStatement::from(statement),
            &mut self.buffer,
            serialize,
        )
        .map_err(|error| BatchSerializationError::StatementSerialization {
            statement_idx,
            error,
        })
        .transpose()
        .unwrap_or_else(|| Err(counts_mismatch_err(statement_idx, self.statements_len)))
        .map_err(|e| {
            ExecutionError::LastAttemptError(RequestAttemptError::CqlRequestSerialization(
                CqlRequestSerializationError::BatchSerialization(e),
            ))
        })
    }
}

fn serialize_statement<T>(
    statement: request::batch::BatchStatement,
    buffer: &mut Vec<u8>,
    serialize: impl FnOnce(&mut RowWriter<'_>) -> Result<Option<T>, SerializationError>,
) -> Result<Option<T>, BatchStatementSerializationError> {
    statement.serialize(buffer)?;

    // Reserve two bytes for length
    let length_pos = buffer.len();
    buffer.extend_from_slice(&[0, 0]);

    // serialize the values
    let mut writer = RowWriter::new(buffer);
    let Some(res) =
        serialize(&mut writer).map_err(BatchStatementSerializationError::ValuesSerialiation)?
    else {
        return Ok(None);
    };

    // Go back and put the length
    let count: u16 = writer
        .value_count()
        .try_into()
        .map_err(|_| BatchStatementSerializationError::TooManyValues(writer.value_count()))?;

    buffer[length_pos..length_pos + 2].copy_from_slice(&count.to_be_bytes());

    Ok(Some(res))
}

impl Default for BoundBatch {
    fn default() -> Self {
        Self {
            config: StatementConfig::default(),
            batch_type: BatchType::Logged,
            buffer: Vec::new(),
            prepared: HashMap::new(),
            first_prepared: None,
            statements_len: 0,
        }
    }
}

fn counts_mismatch_err(n_value_lists: usize, n_statements: u16) -> BatchSerializationError {
    BatchSerializationError::ValuesAndStatementsLengthMismatch {
        n_value_lists,
        n_statements: n_statements as usize,
    }
}

/// This enum represents a CQL statement, that can be part of batch and its values
#[derive(Clone)]
#[non_exhaustive]
pub enum BoundBatchStatement<'p, V: SerializeRow> {
    /// A prepared statement and its not-yet serialized values
    Prepared(PreparedStatement, V),
    /// A statement whose values have already been bound (and thus serialized)
    Bound(BoundStatement<'p>),
    /// An unprepared statement with no values
    Query(Statement),
}

impl<'p> From<BoundStatement<'p>> for BoundBatchStatement<'p, ()> {
    fn from(b: BoundStatement<'p>) -> Self {
        BoundBatchStatement::Bound(b)
    }
}

impl<V: SerializeRow> From<(PreparedStatement, V)> for BoundBatchStatement<'static, V> {
    fn from((p, v): (PreparedStatement, V)) -> Self {
        BoundBatchStatement::Prepared(p, v)
    }
}

impl From<Statement> for BoundBatchStatement<'static, ()> {
    fn from(s: Statement) -> Self {
        BoundBatchStatement::Query(s)
    }
}

impl From<&str> for BoundBatchStatement<'static, ()> {
    fn from(s: &str) -> Self {
        BoundBatchStatement::Query(Statement::from(s))
    }
}

/// An error type returned when adding a statement to a bounded batch fails
#[non_exhaustive]
#[derive(Error, Debug, Clone)]
pub enum BoundBatchStatementError {
    /// Failed to serialize the batch statement
    #[error(transparent)]
    Statement(#[from] BatchStatementSerializationError),
    /// Failed to serialize statement's bound values.
    #[error("Failed to calculate partition key")]
    PartitionKey(#[from] PartitionKeyError),
    /// Too many statements in the batch statement.
    #[error("Added statement goes over exceeded max value of 65,535")]
    TooManyQueriesInBatchStatement,
}

impl Execute for BoundBatch {
    async fn execute(&self, session: &Session) -> Result<QueryResult, ExecutionError> {
        // Shard-awareness behavior for batch will be to pick shard based on first batch statement's shard
        // If users batch statements by shard, they will be rewarded with full shard awareness
        let execution_profile = self
            .get_execution_profile_handle()
            .unwrap_or_else(|| session.get_default_execution_profile_handle())
            .access();

        let consistency = self
            .config
            .consistency
            .unwrap_or(execution_profile.consistency);

        let serial_consistency = self
            .config
            .serial_consistency
            .unwrap_or(execution_profile.serial_consistency);

        let (table, token) = self
            .first_prepared
            .as_ref()
            .and_then(|(ps, token)| ps.get_table_spec().map(|table| (table, *token)))
            .unzip();

        let statement_info = RoutingInfo {
            consistency,
            serial_consistency,
            token,
            table,
            is_confirmed_lwt: false,
        };

        let span = RequestSpan::new_batch();

        let (run_request_result, coordinator): (
            RunRequestResult<NonErrorQueryResponse>,
            Coordinator,
        ) = session
            .run_request(
                statement_info,
                &self.config,
                execution_profile,
                |connection: Arc<Connection>,
                 consistency: Consistency,
                 execution_profile: &ExecutionProfileInner| {
                    let serial_consistency = self
                        .config
                        .serial_consistency
                        .unwrap_or(execution_profile.serial_consistency);
                    async move {
                        connection
                            .batch_with_consistency(self, consistency, serial_consistency)
                            .await
                            .and_then(QueryResponse::into_non_error_query_response)
                    }
                },
                &span,
            )
            .instrument(span.span().clone())
            .await?;

        let result = match run_request_result {
            RunRequestResult::IgnoredWriteError => QueryResult::mock_empty(coordinator),
            RunRequestResult::Completed(non_error_query_response) => {
                let result = non_error_query_response.into_query_result(coordinator)?;
                span.record_result_fields(&result);
                result
            }
        };

        Ok(result)
    }
}
