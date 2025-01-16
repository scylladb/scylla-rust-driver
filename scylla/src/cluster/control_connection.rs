//! Specially treated single connection used to fetch metadata
//! and receive events from the cluster.

use std::net::SocketAddr;
use std::sync::Arc;

use scylla_cql::serialize::row::SerializedValues;

use crate::client::pager::QueryPager;
use crate::errors::{NextRowError, RequestAttemptError};
use crate::network::Connection;
use crate::statement::prepared::PreparedStatement;
use crate::statement::Statement;

/// The single connection used to fetch metadata and receive events from the cluster.
pub(super) struct ControlConnection {
    conn: Arc<Connection>,
}

impl ControlConnection {
    pub(super) fn new(conn: Arc<Connection>) -> Self {
        Self { conn }
    }

    pub(super) fn get_connect_address(&self) -> SocketAddr {
        self.conn.get_connect_address()
    }

    /// Executes a query and fetches its results over multiple pages, using
    /// the asynchronous iterator interface.
    pub(super) async fn query_iter(
        &self,
        statement: Statement,
    ) -> Result<QueryPager, NextRowError> {
        Arc::clone(&self.conn).query_iter(statement).await
    }

    pub(crate) async fn prepare(
        &self,
        statement: Statement,
    ) -> Result<PreparedStatement, RequestAttemptError> {
        self.conn.prepare(&statement).await
    }

    /// Executes a prepared statements and fetches its results over multiple pages, using
    /// the asynchronous iterator interface.
    pub(crate) async fn execute_iter(
        &self,
        prepared_statement: PreparedStatement,
        values: SerializedValues,
    ) -> Result<QueryPager, NextRowError> {
        Arc::clone(&self.conn)
            .execute_iter(prepared_statement, values)
            .await
    }
}
