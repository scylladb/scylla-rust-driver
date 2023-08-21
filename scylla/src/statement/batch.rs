use std::borrow::Cow;
use std::sync::Arc;

use crate::history::HistoryListener;
use crate::load_balancing;
use crate::retry_policy::RetryPolicy;
use crate::statement::{prepared_statement::PreparedStatement, query::Query};
use crate::transport::{execution_profile::ExecutionProfileHandle, Node};
use crate::Session;

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

    /// Gets the serial consistency to be used when executing this batch.
    /// (Ignored unless the batch is an LWT)
    pub fn get_serial_consistency(&self) -> Option<SerialConsistency> {
        self.config.serial_consistency.flatten()
    }

    /// Sets the idempotence of this batch
    /// A query is idempotent if it can be applied multiple times without changing the result of the initial application
    /// If set to `true` we can be sure that it is idempotent
    /// If set to `false` it is unknown whether it is idempotent
    /// This is used in [`RetryPolicy`](crate::retry_policy::RetryPolicy) to decide if retrying a query is safe
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

    /// Set the retry policy for this batch, overriding the one from execution profile if not None.
    #[inline]
    pub fn set_retry_policy(&mut self, retry_policy: Option<Arc<dyn RetryPolicy>>) {
        self.config.retry_policy = retry_policy;
    }

    /// Get the retry policy set for the batch.
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

    /// Associates the batch with execution profile referred by the provided handle.
    /// Handle may be later remapped to another profile, and batch will reflect those changes.
    pub fn set_execution_profile_handle(&mut self, profile_handle: Option<ExecutionProfileHandle>) {
        self.config.execution_profile_handle = profile_handle;
    }

    /// Borrows the execution profile handle associated with this batch.
    pub fn get_execution_profile_handle(&self) -> Option<&ExecutionProfileHandle> {
        self.config.execution_profile_handle.as_ref()
    }

    /// Associates the batch with a new execution profile that will have a load balancing policy
    /// that will enforce the use of the provided [`Node`] to the extent possible.
    ///
    /// This should typically be used in conjunction with [`Session::shard_for_statement`], where
    /// you would constitute a batch by assigning to the same batch all the statements that would be executed in
    /// the same shard.
    ///
    /// Since it is not guaranteed that subsequent calls to the load balancer would re-assign the statement
    /// to the same node, you should use this method to enforce the use of the original node that was envisioned by
    /// `shard_for_statement` for the batch:
    ///
    /// ```rust
    /// # use scylla::Session;
    /// # use std::error::Error;
    /// # async fn check_only_compiles(session: &Session) -> Result<(), Box<dyn Error>> {
    /// use scylla::{
    ///     batch::Batch,
    ///     frame::value::{SerializedValues, ValueList},
    /// };
    ///
    /// let prepared_statement = session
    ///     .prepare("INSERT INTO ks.tab(a, b) VALUES(?, ?)")
    ///     .await?;
    ///
    /// let serialized_values: SerializedValues = (1, 2).serialized()?.into_owned();
    /// let shard = session.shard_for_statement(&prepared_statement, &serialized_values)?;
    ///
    /// // Send that to a task that will handle statements targeted to the same shard
    ///
    /// // On that task:
    /// // Constitute a batch with all the statements that would be executed in the same shard
    ///
    /// let mut batch: Batch = Default::default();
    /// if let Some((node, _shard_idx)) = shard {
    ///     batch.enforce_target_node(&node, &session);
    /// }
    /// let mut batch_values = Vec::new();
    ///
    /// // As the task handling statements targeted to this shard receives them,
    /// // it appends them to the batch
    /// batch.append_statement(prepared_statement);
    /// batch_values.push(serialized_values);
    ///
    /// // Run the batch
    /// session.batch(&batch, batch_values).await?;
    /// # Ok(())
    /// # }
    /// ```
    ///
    ///
    /// If the target node is not available anymore at the time of executing the statement, it will fallback to the
    /// original load balancing policy:
    /// - Either that currently set on the [`Batch`], if any
    /// - Or that of the [`Session`] if there isn't one on the `Batch`
    pub fn enforce_target_node(
        &mut self,
        node: &Arc<Node>,
        base_execution_profile_from_session: &Session,
    ) {
        let execution_profile_handle = self.get_execution_profile_handle().unwrap_or_else(|| {
            base_execution_profile_from_session.get_default_execution_profile_handle()
        });
        self.set_execution_profile_handle(Some(
            execution_profile_handle
                .pointee_to_builder()
                .load_balancing_policy(Arc::new(load_balancing::EnforceTargetNodePolicy::new(
                    node,
                    execution_profile_handle.load_balancing_policy(),
                )))
                .build()
                .into_handle(),
        ))
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
pub enum BatchStatement {
    Query(Query),
    PreparedStatement(PreparedStatement),
}

impl From<&str> for BatchStatement {
    fn from(s: &str) -> Self {
        BatchStatement::Query(Query::from(s))
    }
}

impl From<Query> for BatchStatement {
    fn from(q: Query) -> Self {
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
