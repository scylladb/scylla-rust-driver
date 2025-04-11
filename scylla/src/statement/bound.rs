//! Defines the [`BoundStatement`] type, which represents a prepared statement whose values have
//! already been bound (serialized).
use std::{borrow::Cow, sync::Arc};

use scylla_cql::{
    Consistency,
    frame::{
        request::query::{PagingState, PagingStateResponse},
        response::NonErrorResponseWithDeserializedMetadata,
    },
    serialize::{
        SerializationError,
        row::{SerializeRow, SerializedValues},
    },
};
use tracing::Instrument;

use crate::{
    client::{
        execution_profile::ExecutionProfileInner,
        session::{RunRequestResult, Session},
    },
    errors::ExecutionError,
    frame::response::result,
    network::Connection,
    observability::driver_tracing::RequestSpan,
    policies::load_balancing::RoutingInfo,
    response::{Coordinator, NonErrorQueryResponse, QueryResponse, query_result::QueryResult},
    routing::Token,
};

use super::{
    execute::ExecutePageable,
    prepared::{PartitionKey, PartitionKeyError, PartitionKeyExtractionError, PreparedStatement},
};

/// Represents a statement that already had all its values bound
#[derive(Debug, Clone)]
pub struct BoundStatement<'p> {
    pub(crate) prepared: Cow<'p, PreparedStatement>,
    pub(crate) values: SerializedValues,
}

impl<'p> BoundStatement<'p> {
    pub(crate) fn new(
        prepared: Cow<'p, PreparedStatement>,
        values: &impl SerializeRow,
    ) -> Result<Self, SerializationError> {
        let values = prepared.serialize_values(values)?;
        Ok(Self::new_untyped(prepared, values))
    }

    pub(crate) fn new_untyped(
        prepared: Cow<'p, PreparedStatement>,
        values: SerializedValues,
    ) -> Self {
        Self { prepared, values }
    }

    #[cfg(test)]
    /// Create a new bound statement with no values to serialize
    pub fn empty(prepared: Cow<'p, PreparedStatement>) -> Self {
        Self::new_untyped(prepared, SerializedValues::new())
    }

    /// Determines which values constitute the partition key and puts them in order.
    ///
    /// This is a preparation step necessary for calculating token based on a prepared statement.
    pub(crate) fn pk(&self) -> Result<PartitionKey<'_>, PartitionKeyExtractionError> {
        PartitionKey::new(self.prepared.get_prepared_metadata(), &self.values)
    }

    pub(crate) fn pk_and_token(
        &self,
    ) -> Result<Option<(PartitionKey<'_>, Token)>, PartitionKeyError> {
        if !self.prepared.is_token_aware() {
            return Ok(None);
        }

        let partition_key = self.pk()?;
        let token = partition_key.calculate_token(self.prepared.get_partitioner_name())?;
        Ok(Some((partition_key, token)))
    }

    /// Calculates the token for the prepared statement and its bound values
    ///
    /// Returns the token that would be computed for executing the provided prepared statement with
    /// the provided values.
    pub fn token(&self) -> Result<Option<Token>, PartitionKeyError> {
        self.pk_and_token().map(|p| p.map(|(_, t)| t))
    }

    /// Returns the prepared statement behind the `BoundStatement`
    pub fn prepared(&self) -> &PreparedStatement {
        &self.prepared
    }
}

impl ExecutePageable for BoundStatement<'_> {
    async fn execute_pageable<const SINGLE_PAGE: bool>(
        &self,
        session: &Session,
        paging_state: PagingState,
    ) -> Result<(QueryResult, PagingStateResponse), ExecutionError> {
        let page_size = if SINGLE_PAGE {
            Some(self.prepared.get_validated_page_size())
        } else {
            None
        };

        let paging_state_ref = &paging_state;

        let (partition_key, token) = self
            .pk_and_token()
            .map_err(PartitionKeyError::into_execution_error)?
            .unzip();

        let execution_profile = self
            .prepared
            .get_execution_profile_handle()
            .unwrap_or_else(|| session.get_default_execution_profile_handle())
            .access();

        let table_spec = self.prepared.get_table_spec();

        let statement_info = RoutingInfo {
            consistency: self
                .prepared
                .config
                .consistency
                .unwrap_or(execution_profile.consistency),
            serial_consistency: self
                .prepared
                .config
                .serial_consistency
                .unwrap_or(execution_profile.serial_consistency),
            token,
            table: table_spec,
            is_confirmed_lwt: self.prepared.is_confirmed_lwt(),
        };

        let span = RequestSpan::new_prepared(
            partition_key.as_ref().map(|pk| pk.iter()),
            token,
            self.values.buffer_size(),
        );

        if !span.span().is_disabled() {
            if let (Some(table_spec), Some(token)) = (statement_info.table, token) {
                let cluster_state = session.get_cluster_state();
                let replicas = cluster_state.get_token_endpoints_iter(table_spec, token);
                span.record_replicas(replicas)
            }
        }

        let (run_request_result, coordinator): (
            RunRequestResult<NonErrorQueryResponse>,
            Coordinator,
        ) = session
            .run_request(
                statement_info,
                &self.prepared.config,
                execution_profile,
                |connection: Arc<Connection>,
                 consistency: Consistency,
                 execution_profile: &ExecutionProfileInner| {
                    let serial_consistency = self
                        .prepared
                        .config
                        .serial_consistency
                        .unwrap_or(execution_profile.serial_consistency);
                    async move {
                        connection
                            .execute_raw_with_consistency(
                                self,
                                consistency,
                                serial_consistency,
                                page_size,
                                paging_state_ref.clone(),
                            )
                            .await
                            .and_then(QueryResponse::into_non_error_query_response)
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
