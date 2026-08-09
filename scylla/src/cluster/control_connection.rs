//! Specially treated single connection used to fetch metadata
//! and receive events from the cluster.

use std::fmt::Write as _;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use dashmap::DashMap;
use tokio::sync::{mpsc, oneshot};
use tracing::warn;

use crate::client::client_routes::ClientRoutesSubscriber;
use crate::client::pager::QueryPager;
use crate::cluster::metadata::{SchemaMetadataFetchMode, UntranslatedEndpoint};
use crate::errors::{
    ConnectionError, NextPageError, NextRowError, RequestAttemptError, RequestError,
};
use crate::frame::response::event::EventV2 as Event;
use crate::network::Connection;
use crate::serialize::row::SerializeRow;
use crate::statement::Statement;
use crate::statement::prepared::PreparedStatement;

const METADATA_QUERY_PAGE_SIZE: i32 = 1024;

/// How much longer the client-side timeout of a control connection request is
/// than the server-side one it is derived from.
const CLIENTSIDE_TIMEOUT_MARGIN: Duration = Duration::from_secs(1);

/// The client-side timeout of a control connection request when there is no
/// server-side timeout configured to derive it from.
const DEFAULT_CLIENTSIDE_TIMEOUT: Duration = Duration::from_secs(30);

/// Timeouts applied to requests executed on the control connection.
///
/// The server-side timeout is only an override of the server's own limit, appended
/// to the statement as a `USING TIMEOUT` clause; it is a ScyllaDB-only feature, so
/// it is silently inapplicable to other targets (e.g. Cassandra).
///
/// The client-side timeout is applicable to all targets.
/// By default it is server-side timeout + CLIENTSIDE_TIMEOUT_MARGIN.
/// If no server-side timeout is set, then a default of 30s is used for client-side timeout.
#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct MetadataRequestTimeouts {
    pub(crate) serverside_override: Option<Duration>,
    pub(crate) clientside_override: Option<Duration>,
}

impl MetadataRequestTimeouts {
    /// The server-side timeout override actually in effect.
    fn serverside(&self, target_is_scylladb: bool) -> Option<Duration> {
        self.serverside_override.filter(|_| target_is_scylladb)
    }

    /// The client-side timeout actually in effect, derived from the configured
    /// server-side timeout unless explicitly overridden.
    fn clientside(&self) -> Duration {
        self.clientside_override.unwrap_or_else(|| {
            self.serverside_override.map_or(
                DEFAULT_CLIENTSIDE_TIMEOUT,
                // Saturating, as a pathologically large configured server-side timeout
                // must not panic the driver.
                |t| t.saturating_add(CLIENTSIDE_TIMEOUT_MARGIN),
            )
        })
    }
}

pub(crate) type ControlConnectionCache = DashMap<String, PreparedStatement>;

pub(crate) enum ControlConnectionEvent {
    Broken(ConnectionError),
    ServerEvent(Event),
    Shutdown,
}

/// Configuration for the queries a [`ControlConnection`] runs on behalf of the
/// driver: what metadata to fetch and how.
///
/// Not to be confused with [`ConnectionConfig`](crate::network::ConnectionConfig),
/// which configures how the underlying network connection is opened.
#[derive(Clone)]
pub(super) struct ControlConnectionConfig {
    /// Keyspaces to restrict the schema metadata fetch to. Empty means no restriction.
    pub(super) keyspaces_to_fetch: Vec<String>,
    /// Whether (and in what detail) to fetch schema metadata.
    pub(super) schema_metadata_fetch_mode: SchemaMetadataFetchMode,
    /// The subscriber interested in `system.client_routes`, if any. Provides the
    /// connection ids to filter routes by; its presence also makes the control
    /// connection register for CLIENT_ROUTES_CHANGE events.
    pub(super) client_routes_subscriber: Option<Arc<dyn ClientRoutesSubscriber>>,
    /// The custom server-side timeout set for requests executed on the control connection.
    pub(super) request_timeouts: MetadataRequestTimeouts,
}

/// The single connection used to fetch metadata and receive events from the cluster.
pub(super) struct ControlConnection {
    conn: Arc<Connection>,
    endpoint: UntranslatedEndpoint,
    /// The timeouts applied to requests executed on the control connection.
    request_timeouts: MetadataRequestTimeouts,
    cache: Arc<ControlConnectionCache>,
}

/// The event side of a control connection: the channels on which the connection
/// reports server events and its own failure.
///
/// Kept apart from [`ControlConnection`] so that awaiting events (which requires
/// a mutable borrow) does not conflict with running metadata queries on the
/// connection (which require a shared borrow).
pub(super) struct ControlConnectionEvents {
    error_channel: oneshot::Receiver<ConnectionError>,
    events_channel: mpsc::Receiver<Event>,
}

impl ControlConnection {
    pub(super) fn new(
        conn: Arc<Connection>,
        endpoint: UntranslatedEndpoint,
        cache: Arc<ControlConnectionCache>,
        error_channel: oneshot::Receiver<ConnectionError>,
        events_channel: mpsc::Receiver<Event>,
    ) -> (Self, ControlConnectionEvents) {
        (
            Self {
                conn,
                endpoint,
                request_timeouts: MetadataRequestTimeouts::default(),
                cache,
            },
            ControlConnectionEvents {
                error_channel,
                events_channel,
            },
        )
    }

    pub(super) fn endpoint(&self) -> &UntranslatedEndpoint {
        &self.endpoint
    }

    /// Sets the timeouts applied to requests executed on the control connection.
    pub(super) fn with_request_timeouts(self, timeouts: MetadataRequestTimeouts) -> Self {
        Self {
            request_timeouts: timeouts,
            ..self
        }
    }

    pub(super) fn get_connect_address(&self) -> SocketAddr {
        self.conn.get_connect_address()
    }

    /// Returns true iff the target node is a ScyllaDB node (and not a, e.g., Cassandra node).
    pub(super) fn is_to_scylladb(&self) -> bool {
        self.conn.is_to_scylladb()
    }

    /// Appends the custom server-side timeout to the statement string, if such custom timeout
    /// is provided and we are connected to ScyllaDB (since custom timeouts is ScyllaDB-only feature).
    fn maybe_append_timeout_override(&self, statement: &mut Statement) {
        if let Some(timeout) = self.request_timeouts.serverside(self.is_to_scylladb()) {
            // SAFETY: io::fmt::Write impl for String is infallible.
            write!(
                statement.contents,
                " USING TIMEOUT {}ms",
                timeout.as_millis()
            )
            .unwrap()
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
        statement.set_is_idempotent(true);
        let prepared = Arc::clone(&self.conn).prepare(&statement).await?;
        // Inserting with pre-`maybe_append_timeout_override` key, because
        // that is the way we will query the map later.
        self.cache
            .insert(statement_str.to_string(), prepared.clone());
        Ok(prepared)
    }

    /// Executes a query and fetches its results over multiple pages, using
    /// the asynchronous iterator interface.
    ///
    /// NOTE: This function only supports executing SELECT statements.
    /// More specifically, it expects that each response is of Rows kind.
    /// Other kinds of responses will result in an error.
    pub(super) async fn query_iter(
        &self,
        statement: &str,
        // Without this `Sync` compiler complains that cluster worker future is not Send.
        values: &(dyn SerializeRow + Sync),
    ) -> Result<QueryPager, NextRowError> {
        let mut prepared: PreparedStatement = self
            .get_or_prepare_statement(statement)
            .await
            .map_err(|attempt_err| {
                NextRowError::NextPageError(NextPageError::RequestFailure(attempt_err.into()))
            })?;

        // Set on this per-use clone rather than in `get_or_prepare_statement`, so that
        // the shared cache never bakes a timeout in.
        prepared.set_request_timeout(Some(self.request_timeouts.clientside()));

        let serialized_values = prepared.serialize_values(&values).map_err(|ser_err| {
            NextRowError::NextPageError(NextPageError::RequestFailure(
                RequestError::LastAttemptError(RequestAttemptError::SerializationError(ser_err)),
            ))
        })?;
        Arc::clone(&self.conn)
            .execute_iter(prepared, serialized_values)
            .await
    }
}

impl ControlConnectionEvents {
    pub(super) async fn wait_for_event(&mut self) -> ControlConnectionEvent {
        tokio::select! {
            // Why only `Some`? `None` means that event channel was dropped.
            // In current implementation (as of writing this comment)
            // this should not be possible: events sender is stored in HostConnectionConfig,
            // which is a field of Connection that we own. If we got `None`, then most likely
            // two things happened:
            //  - The implementation changed, for example by moving event sender to router.
            //  - Connection was closed, router shutdown.
            //  - `tokio::select!` chose this branch instead of error channel.
            // The best thing we can imo do is ignore this `None`. `error_channel` should receive
            // info about connection shutdown very soon.
            Some(cql_event) = self.events_channel.recv() => {
                ControlConnectionEvent::ServerEvent(cql_event)
            },
            maybe_control_connection_failed = &mut self.error_channel => {
                let err = match maybe_control_connection_failed {
                    Ok(err) => err,
                    Err(_recv_error) => {
                        // If we got here then error channel, in a Connection that we own,
                        // was dropped without sending anything. This is definitely a bug in the driver!
                        // We could theoretically recover by dropping a connection and creating new one,
                        // but we would need to add an error variant to `BrokenConnectionErrorKind` that
                        // could basically never happen. Let's panic instead.
                        warn!(concat!("Error sender of control connection unexpectedly dropped. The only case when this ",
                        "may happen is during runtime shutdown. If you see this and the runtime isn't shutting down, ",
                        "this is a bug in the driver. Then please open an issue!"));
                        return ControlConnectionEvent::Shutdown;
                    },
                };
                ControlConnectionEvent::Broken(err)
            }
        }
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

    use crate::cluster::control_connection::ControlConnectionCache;
    use crate::cluster::metadata::UntranslatedEndpoint;
    use crate::cluster::node::ResolvedContactPoint;
    use crate::network::HostConnectionConfig;
    use crate::network::open_connection;
    use crate::routing::ShardInfo;
    use crate::test_utils::setup_tracing;

    use super::{ControlConnection, MetadataRequestTimeouts};

    /// Tests that ControlConnection enforces the provided custom timeout
    /// iff ScyllaDB is the target node (else ignores the custom timeout).
    #[tokio::test]
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
                frame.body,
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
                frame.body,
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
            let endpoint = UntranslatedEndpoint::ContactPoint(ResolvedContactPoint {
                address: proxy_addr,
            });
            let (events_sender, events_receiver) = mpsc::channel(32);
            let (conn, error_receiver) = open_connection(
                &endpoint,
                None,
                &HostConnectionConfig {
                    event_sender: Some((events_sender, vec![])),
                    ..Default::default()
                },
            )
            .await
            .unwrap();

            let connected_to_scylladb = conn.get_shard_info().is_some();
            let (conn_with_default_timeout, _events) = ControlConnection::new(
                Arc::new(conn),
                endpoint,
                Arc::new(ControlConnectionCache::new()),
                error_receiver,
                events_receiver,
            );

            // No custom timeout set.
            {
                conn_with_default_timeout
                    .query_iter(QUERY_STR, &())
                    .await
                    .unwrap_err();

                assert_no_custom_timeout(feedback_rx).await;
            }

            // Custom timeout set, so it should be set in query strings iff the target node is ScyllaDB.
            {
                let custom_timeout = Duration::from_millis(2137);
                let conn_with_custom_timeout =
                    conn_with_default_timeout.with_request_timeouts(MetadataRequestTimeouts {
                        serverside_override: Some(custom_timeout),
                        clientside_override: None,
                    });

                conn_with_custom_timeout
                    .query_iter(QUERY_STR, &())
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
