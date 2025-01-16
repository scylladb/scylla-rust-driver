//! Specially treated single connection used to fetch metadata
//! and receive events from the cluster.

use std::fmt::Write as _;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use scylla_cql::serialize::row::SerializedValues;

use crate::client::pager::QueryPager;
use crate::errors::{NextRowError, RequestAttemptError};
use crate::network::Connection;
use crate::statement::prepared::PreparedStatement;
use crate::statement::Statement;

/// The single connection used to fetch metadata and receive events from the cluster.
pub(super) struct ControlConnection {
    conn: Arc<Connection>,
    /// The custom server-side timeout set for requests executed on the control connection.
    overriden_serverside_timeout: Option<Duration>,
}

impl ControlConnection {
    pub(super) fn new(conn: Arc<Connection>) -> Self {
        Self {
            conn,
            overriden_serverside_timeout: None,
        }
    }

    /// Sets the custom server-side timeout set for requests executed on the control connection.
    pub(super) fn override_serverside_timeout(self, overriden_timeout: Option<Duration>) -> Self {
        Self {
            overriden_serverside_timeout: overriden_timeout,
            ..self
        }
    }

    pub(super) fn get_connect_address(&self) -> SocketAddr {
        self.conn.get_connect_address()
    }

    /// Returns true iff the target node is a ScyllaDB node (and not a, e.g., Cassandra node).
    pub(super) fn is_to_scylladb(&self) -> bool {
        self.conn.get_shard_info().is_some()
    }

    /// Appends the custom server-side timeout to the statement string, if such custom timeout
    /// is provided and we are connected to ScyllaDB (since custom timeouts is ScyllaDB-only feature).
    fn maybe_append_timeout_override(&self, statement: &mut Statement) {
        if let Some(timeout) = self.overriden_serverside_timeout {
            if self.is_to_scylladb() {
                // SAFETY: io::fmt::Write impl for String is infallible.
                write!(
                    statement.contents,
                    " USING TIMEOUT {}ms",
                    timeout.as_millis()
                )
                .unwrap()
            }
        }
    }

    /// Executes a query and fetches its results over multiple pages, using
    /// the asynchronous iterator interface.
    pub(super) async fn query_iter(
        &self,
        mut statement: Statement,
    ) -> Result<QueryPager, NextRowError> {
        self.maybe_append_timeout_override(&mut statement);
        Arc::clone(&self.conn).query_iter(statement).await
    }

    pub(crate) async fn prepare(
        &self,
        mut statement: Statement,
    ) -> Result<PreparedStatement, RequestAttemptError> {
        self.maybe_append_timeout_override(&mut statement);
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
