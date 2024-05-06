use std::borrow::Cow;
use std::sync::Arc;

use crate::history::HistoryListener;
use crate::retry_policy::RetryPolicy;
use crate::statement::{prepared_statement::PreparedStatement, unprepared_statement::Query};
use crate::transport::execution_profile::ExecutionProfileHandle;

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

pub(crate) mod batch_values {
    use scylla_cql::errors::QueryError;
    use scylla_cql::types::serialize::batch::BatchValues;
    use scylla_cql::types::serialize::batch::BatchValuesIterator;
    use scylla_cql::types::serialize::row::RowSerializationContext;
    use scylla_cql::types::serialize::row::SerializedValues;
    use scylla_cql::types::serialize::{RowWriter, SerializationError};

    use crate::routing::Token;

    use super::BatchStatement;

    // Takes an optional reference to the first statement in the batch and
    // the batch values, and tries to compute the token for the statement.
    // Returns the (optional) token and batch values. If the function needed
    // to serialize values for the first statement, the returned batch values
    // will cache the results of the serialization.
    //
    // NOTE: Batch values returned by this function might not type check
    // the first statement when it is serialized! However, if they don't,
    // then the first row was already checked by the function. It is assumed
    // that `statement` holds the first prepared statement of the batch (if
    // there is one), and that it will be used later to serialize the values.
    pub(crate) fn peek_first_token<'bv>(
        values: impl BatchValues + 'bv,
        statement: Option<&BatchStatement>,
    ) -> Result<(Option<Token>, impl BatchValues + 'bv), QueryError> {
        let mut values_iter = values.batch_values_iter();
        let (token, first_values) = match statement {
            Some(BatchStatement::PreparedStatement(ps)) => {
                let ctx = RowSerializationContext::from_prepared(ps.get_prepared_metadata());
                let (first_values, did_write) = SerializedValues::from_closure(|writer| {
                    values_iter
                        .serialize_next(&ctx, writer)
                        .transpose()
                        .map(|o| o.is_some())
                })?;
                if did_write {
                    let token = ps.calculate_token_untyped(&first_values)?;
                    (token, Some(first_values))
                } else {
                    (None, None)
                }
            }
            _ => (None, None),
        };

        // Need to do it explicitly, otherwise the next line will complain
        // that `values_iter` still borrows `values`.
        std::mem::drop(values_iter);

        // Reuse the already serialized first value via `BatchValuesFirstSerialized`.
        let values = BatchValuesFirstSerialized::new(values, first_values);

        Ok((token, values))
    }

    struct BatchValuesFirstSerialized<BV> {
        // Contains the first value of BV in a serialized form.
        // The first value in the iterator returned from `rest` should be skipped!
        first: Option<SerializedValues>,
        rest: BV,
    }

    impl<BV> BatchValuesFirstSerialized<BV> {
        fn new(rest: BV, first: Option<SerializedValues>) -> Self {
            Self { first, rest }
        }
    }

    impl<BV> BatchValues for BatchValuesFirstSerialized<BV>
    where
        BV: BatchValues,
    {
        type BatchValuesIter<'r> = BatchValuesFirstSerializedIterator<'r, BV::BatchValuesIter<'r>>
    where
        Self: 'r;

        fn batch_values_iter(&self) -> Self::BatchValuesIter<'_> {
            BatchValuesFirstSerializedIterator {
                first: self.first.as_ref(),
                rest: self.rest.batch_values_iter(),
            }
        }
    }

    struct BatchValuesFirstSerializedIterator<'f, BVI> {
        first: Option<&'f SerializedValues>,
        rest: BVI,
    }

    impl<'f, BVI> BatchValuesIterator<'f> for BatchValuesFirstSerializedIterator<'f, BVI>
    where
        BVI: BatchValuesIterator<'f>,
    {
        #[inline]
        fn serialize_next(
            &mut self,
            ctx: &RowSerializationContext<'_>,
            writer: &mut RowWriter,
        ) -> Option<Result<(), SerializationError>> {
            match self.first.take() {
                Some(sr) => {
                    writer.append_serialize_row(sr);
                    self.rest.skip_next();
                    Some(Ok(()))
                }
                None => self.rest.serialize_next(ctx, writer),
            }
        }

        #[inline]
        fn is_empty_next(&mut self) -> Option<bool> {
            match self.first.take() {
                Some(s) => {
                    self.rest.skip_next();
                    Some(s.is_empty())
                }
                None => self.rest.is_empty_next(),
            }
        }

        #[inline]
        fn skip_next(&mut self) -> Option<()> {
            self.first = None;
            self.rest.skip_next()
        }

        #[inline]
        fn count(self) -> usize
        where
            Self: Sized,
        {
            self.rest.count()
        }
    }
}
