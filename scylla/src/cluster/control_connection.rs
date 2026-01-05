//! Specially treated single connection used to fetch metadata
//! and receive events from the cluster.

use std::fmt::Write as _;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use dashmap::DashMap;
use scylla_cql::serialize::row::SerializedValues;

use crate::client::pager::QueryPager;
use crate::errors::{NextPageError, NextRowError, RequestAttemptError};
use crate::network::Connection;
use crate::statement::Statement;
use crate::statement::prepared::PreparedStatement;

const METADATA_QUERY_PAGE_SIZE: i32 = 1024;

/// The single connection used to fetch metadata and receive events from the cluster.
pub(super) struct ControlConnection {
    conn: Arc<Connection>,
    /// The custom server-side timeout set for requests executed on the control connection.
    overridden_serverside_timeout: Option<Duration>,
    cache: DashMap<String, PreparedStatement>,
}

impl ControlConnection {
    pub(super) fn new(conn: Arc<Connection>) -> Self {
        Self {
            conn,
            overridden_serverside_timeout: None,
            cache: DashMap::new(),
        }
    }

    /// Sets the custom server-side timeout set for requests executed on the control connection.
    pub(super) fn override_serverside_timeout(self, overridden_timeout: Option<Duration>) -> Self {
        Self {
            overridden_serverside_timeout: overridden_timeout,
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
        if let Some(timeout) = self.overridden_serverside_timeout {
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

    async fn get_or_prepare_statement(
        &self,
        statement_str: &str,
    ) -> Result<PreparedStatement, RequestAttemptError> {
        if let Some(statement) = self.cache.get(statement_str) {
            return Ok(statement.clone());
        }

        let mut statement = Statement::new(statement_str);
        self.maybe_append_timeout_override(&mut statement);
        statement.set_page_size(METADATA_QUERY_PAGE_SIZE);
        let prepared = Arc::clone(&self.conn).prepare(&statement).await?;
        // Inserting with pre-`maybe_append_timeout_override` key, because
        // that is the way we will query the map later.
        self.cache
            .insert(statement_str.to_string(), prepared.clone());
        Ok(prepared)
    }

    /// Executes a query and fetches its results over multiple pages, using
    /// the asynchronous iterator interface.
    pub(super) async fn query_iter(&self, statement: &str) -> Result<QueryPager, NextRowError> {
        let prepared: PreparedStatement =
            self.get_or_prepare_statement(statement)
                .await
                .map_err(|attempt_err| {
                    NextRowError::NextPageError(NextPageError::RequestFailure(attempt_err.into()))
                })?;

        Arc::clone(&self.conn)
            .execute_iter(prepared, SerializedValues::new())
            .await
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

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::net::SocketAddr;
    use std::sync::Arc;
    use std::time::Duration;

    use scylla_proxy::{
        Condition, Node, Proxy, Reaction as _, RequestFrame, RequestOpcode, RequestReaction,
        RequestRule, ResponseFrame,
    };
    use tokio::sync::mpsc;

    use std::num::NonZeroU16;

    use crate::cluster::metadata::UntranslatedEndpoint;
    use crate::cluster::node::ResolvedContactPoint;
    use crate::network::open_connection;
    use crate::routing::ShardInfo;
    use crate::test_utils::setup_tracing;

    use super::ControlConnection;

    /// Tests that ControlConnection enforces the provided custom timeout
    /// iff ScyllaDB is the target node (else ignores the custom timeout).
    #[tokio::test]
    #[ntest::timeout(2000)]
    async fn test_custom_timeouts() {
        setup_tracing();

        let proxy_addr = SocketAddr::new(scylla_proxy::get_exclusive_local_address(), 9042);

        let (feedback_tx, mut feedback_rx) = mpsc::unbounded_channel();

        let make_rules = |shard_info: Option<ShardInfo>| {
            vec![
                // OPTIONS -> SUPPORTED rule
                RequestRule(
                    Condition::RequestOpcode(RequestOpcode::Options),
                    RequestReaction::forge_response(Arc::new(move |frame: RequestFrame| {
                        ResponseFrame::forged_supported(frame.params, &{
                            let mut options = HashMap::new();
                            if let Some(shard_info) = shard_info.as_ref() {
                                shard_info.add_to_options(&mut options);
                            }
                            options
                        })
                        .unwrap()
                    })),
                ),
                // STARTUP -> READY rule
                // REGISTER -> READY rule
                RequestRule(
                    Condition::or(
                        Condition::RequestOpcode(RequestOpcode::Startup),
                        Condition::RequestOpcode(RequestOpcode::Register),
                    ),
                    RequestReaction::forge_response(Arc::new(move |frame: RequestFrame| {
                        ResponseFrame::forged_ready(frame.params)
                    })),
                ),
                // Metadata query feedback rule
                RequestRule(
                    Condition::or(
                        Condition::RequestOpcode(RequestOpcode::Query),
                        Condition::RequestOpcode(RequestOpcode::Prepare),
                    ),
                    RequestReaction::forge()
                        .server_error()
                        .with_feedback_when_performed(feedback_tx),
                ),
            ]
        };

        let mut proxy = Proxy::builder()
            .with_node(
                Node::builder()
                    .proxy_address(proxy_addr)
                    .request_rules(make_rules.clone()(None))
                    .build_dry_mode(),
            )
            .build()
            .run()
            .await
            .unwrap();

        const QUERY_STR: &str = "SELECT host_id FROM system.local";

        fn expected_query_body(dur: Duration) -> String {
            format!("{} USING TIMEOUT {}ms", QUERY_STR, dur.as_millis())
        }

        fn contains_subslice(slice: &[u8], subslice: &[u8]) -> bool {
            slice
                .windows(subslice.len())
                .any(|window| window == subslice)
        }

        async fn assert_no_custom_timeout(
            feedback_rx: &mut mpsc::UnboundedReceiver<(RequestFrame, Option<u16>)>,
        ) {
            let (frame, _) = feedback_rx.recv().await.unwrap();
            let clause = "USING TIMEOUT";
            assert!(
                !contains_subslice(&frame.body, clause.as_bytes()),
                "slice {:?} does contain subslice {:?}",
                &frame.body,
                clause,
            );
        }

        async fn assert_custom_timeout(
            feedback_rx: &mut mpsc::UnboundedReceiver<(RequestFrame, Option<u16>)>,
            dur: Duration,
        ) {
            let (frame, _) = feedback_rx.recv().await.unwrap();
            let expected = expected_query_body(dur);
            assert!(
                contains_subslice(&frame.body, expected.as_bytes()),
                "slice {:?} does not contain subslice {:?}",
                &frame.body,
                expected,
            );
        }

        async fn assert_custom_timeout_iff_scylladb(
            feedback_rx: &mut mpsc::UnboundedReceiver<(RequestFrame, Option<u16>)>,
            dur: Duration,
            connected_to_scylladb: bool,
        ) {
            if connected_to_scylladb {
                assert_custom_timeout(feedback_rx, dur).await;
            } else {
                assert_no_custom_timeout(feedback_rx).await;
            }
        }

        async fn test_metadata_timeouts(
            proxy_addr: SocketAddr,
            feedback_rx: &mut mpsc::UnboundedReceiver<(RequestFrame, Option<u16>)>,
        ) {
            let (conn, _error_receiver) = open_connection(
                &UntranslatedEndpoint::ContactPoint(ResolvedContactPoint {
                    address: proxy_addr,
                }),
                None,
                &Default::default(),
            )
            .await
            .unwrap();

            let connected_to_scylladb = conn.get_shard_info().is_some();
            let conn_with_default_timeout = ControlConnection::new(Arc::new(conn));

            // No custom timeout set.
            {
                conn_with_default_timeout
                    .query_iter(QUERY_STR)
                    .await
                    .unwrap_err();

                assert_no_custom_timeout(feedback_rx).await;

                conn_with_default_timeout
                    .prepare(QUERY_STR.into())
                    .await
                    .unwrap_err();

                assert_no_custom_timeout(feedback_rx).await;
            }

            // Custom timeout set, so it should be set in query strings iff the target node is ScyllaDB.
            {
                let custom_timeout = Duration::from_millis(2137);
                let conn_with_custom_timeout =
                    conn_with_default_timeout.override_serverside_timeout(Some(custom_timeout));

                conn_with_custom_timeout
                    .query_iter(QUERY_STR)
                    .await
                    .unwrap_err();

                assert_custom_timeout_iff_scylladb(
                    feedback_rx,
                    custom_timeout,
                    connected_to_scylladb,
                )
                .await;

                conn_with_custom_timeout
                    .prepare(QUERY_STR.into())
                    .await
                    .unwrap_err();

                assert_custom_timeout_iff_scylladb(
                    feedback_rx,
                    custom_timeout,
                    connected_to_scylladb,
                )
                .await;
            }
        }

        // Simulated non-ScyllaDB node (no sharding info in SUPPORTED)
        {
            // Proxy starts without shards. No additional config needed.

            test_metadata_timeouts(proxy_addr, &mut feedback_rx).await;
        }

        // Simulated ScyllaDB node (sharding info present in SUPPORTED)
        {
            proxy.running_nodes[0].change_request_rules(Some(make_rules(Some(ShardInfo {
                shard: 2,
                nr_shards: NonZeroU16::new(4).unwrap(),
                msb_ignore: 1,
            }))));

            test_metadata_timeouts(proxy_addr, &mut feedback_rx).await;
        }

        let _ = proxy.finish().await;
    }
}
