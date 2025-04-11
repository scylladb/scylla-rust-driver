//! Defines the [`Statement`] type, which represents an unprepared CQL statement.

use scylla_cql::frame::request::query::{PagingState, PagingStateResponse};
use scylla_cql::frame::response::NonErrorResponseWithDeserializedMetadata;
use scylla_cql::serialize::row::SerializeRow;
use tracing::Instrument;

use super::execute::ExecutePageable;
use super::{PageSize, StatementConfig};
use crate::client::execution_profile::{ExecutionProfileHandle, ExecutionProfileInner};
use crate::client::session::{RunRequestResult, Session};
use crate::errors::ExecutionError;
use crate::frame::response::result;
use crate::frame::types::{Consistency, SerialConsistency};
use crate::network::Connection;
use crate::observability::driver_tracing::RequestSpan;
use crate::observability::history::HistoryListener;
use crate::policies::load_balancing::LoadBalancingPolicy;
use crate::policies::load_balancing::RoutingInfo;
use crate::policies::retry::RetryPolicy;
use crate::response::query_result::QueryResult;
use crate::response::{Coordinator, NonErrorQueryResponse, QueryResponse};
use std::sync::Arc;
use std::time::Duration;

/// **Unprepared** CQL statement.
///
/// This represents a CQL statement that can be executed on a server.
#[derive(Clone)]
pub struct Statement {
    pub(crate) config: StatementConfig,

    /// The CQL statement text.
    pub contents: String,
    page_size: PageSize,
}

impl Statement {
    /// Creates a new [`Statement`] from a CQL statement string.
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

    /// Sets the page size for this CQL statement.
    ///
    /// Panics if given number is nonpositive.
    pub fn set_page_size(&mut self, page_size: i32) {
        self.page_size = page_size
            .try_into()
            .unwrap_or_else(|err| panic!("Query::set_page_size: {err}"));
    }

    /// Returns the page size for this CQL statement.
    pub(crate) fn get_validated_page_size(&self) -> PageSize {
        self.page_size
    }

    /// Returns the page size for this CQL statement.
    pub fn get_page_size(&self) -> i32 {
        self.page_size.inner()
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

    /// Gets the consistency to be used when executing this statement if it is filled.
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
    /// A statement is idempotent if it can be applied multiple times without changing the result of the initial application
    /// If set to `true` we can be sure that it is idempotent
    /// If set to `false` it is unknown whether it is idempotent
    /// This is used in [`RetryPolicy`] to decide if retrying a statement execution is safe
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
    /// Otherwise, execution profile timeout will be applied.
    pub fn set_request_timeout(&mut self, timeout: Option<Duration>) {
        self.config.request_timeout = timeout
    }

    /// Gets client timeout associated with this statement.
    pub fn get_request_timeout(&self) -> Option<Duration> {
        self.config.request_timeout
    }

    /// Set the retry policy for this statement, overriding the one from execution profile if not None.
    #[inline]
    pub fn set_retry_policy(&mut self, retry_policy: Option<Arc<dyn RetryPolicy>>) {
        self.config.retry_policy = retry_policy;
    }

    /// Get the retry policy set for the statement.
    ///
    /// This method returns the retry policy that is **overridden** on this statement.
    /// In other words, it returns the retry policy set using [`Statement::set_retry_policy`].
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
    /// In other words, it returns the load balancing policy set using [`Statement::set_load_balancing_policy`].
    /// This does not take the load balancing policy from the set execution profile into account.
    #[inline]
    pub fn get_load_balancing_policy(&self) -> Option<&Arc<dyn LoadBalancingPolicy>> {
        self.config.load_balancing_policy.as_ref()
    }

    /// Sets the listener capable of listening what happens during statement execution.
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

    /// Borrows the execution profile handle associated with this statement.
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
impl<V: SerializeRow> ExecutePageable for (&Statement, V) {
    async fn execute_pageable<const SINGLE_PAGE: bool>(
        &self,
        session: &Session,
        paging_state: PagingState,
    ) -> Result<(QueryResult, PagingStateResponse), ExecutionError> {
        let (statement, values) = self;
        let page_size = if SINGLE_PAGE {
            Some(statement.get_validated_page_size())
        } else {
            None
        };

        let execution_profile = statement
            .get_execution_profile_handle()
            .unwrap_or_else(|| session.get_default_execution_profile_handle())
            .access();

        let statement_info = RoutingInfo {
            consistency: statement
                .config
                .consistency
                .unwrap_or(execution_profile.consistency),
            serial_consistency: statement
                .config
                .serial_consistency
                .unwrap_or(execution_profile.serial_consistency),
            ..Default::default()
        };

        let span = RequestSpan::new_query(&statement.contents);
        let span_ref = &span;
        let (run_request_result, coordinator): (
            RunRequestResult<NonErrorQueryResponse>,
            Coordinator,
        ) = session
            .run_request(
                statement_info,
                &statement.config,
                execution_profile,
                |connection: Arc<Connection>,
                 consistency: Consistency,
                 execution_profile: &ExecutionProfileInner| {
                    let serial_consistency = statement
                        .config
                        .serial_consistency
                        .unwrap_or(execution_profile.serial_consistency);
                    // Needed to avoid moving into async move block
                    let paging_state_ref = &paging_state;
                    async move {
                        if values.is_empty() {
                            span_ref.record_request_size(0);
                            connection
                                .query_raw_with_consistency(
                                    statement,
                                    consistency,
                                    serial_consistency,
                                    page_size,
                                    paging_state_ref.clone(),
                                )
                                .await
                                .and_then(QueryResponse::into_non_error_query_response)
                        } else {
                            let statement =
                                connection.prepare(statement).await?.into_bind(values)?;
                            span_ref.record_request_size(statement.values.buffer_size());
                            connection
                                .execute_raw_with_consistency(
                                    &statement,
                                    consistency,
                                    serial_consistency,
                                    page_size,
                                    paging_state_ref.clone(),
                                )
                                .await
                                .and_then(QueryResponse::into_non_error_query_response)
                        }
                    }
                },
                &span,
            )
            .instrument(span.span().clone())
            .await?;

        let response = match run_request_result {
            RunRequestResult::IgnoredWriteError => NonErrorQueryResponse {
                response: NonErrorResponseWithDeserializedMetadata::Result(
                    result::ResultWithDeserializedMetadata::Void,
                ),
                tracing_id: None,
                warnings: Vec::new(),
            },
            RunRequestResult::Completed(response) => response,
        };

        let (result, paging_state_response) =
            response.into_query_result_and_paging_state(coordinator)?;
        span.record_result_fields(&result);

        Ok((result, paging_state_response))
    }
}
