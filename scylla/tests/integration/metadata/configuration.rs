//! Tests that the driver enforces the provided custom metadata fetching timeout
//! iff ScyllaDB is the target node (else ignores the custom timeout).

use crate::utils::{setup_tracing, test_with_3_node_cluster};
use scylla::client::session::Session;
use scylla::client::session_builder::SessionBuilder;
use scylla_proxy::{
    Condition, Reaction as _, RequestFrame, RequestOpcode, RequestReaction, RequestRule,
    ShardAwareness,
};
use scylla_proxy::{ProxyError, RunningProxy, WorkerError};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::sync::mpsc::error::TryRecvError;
use tokio::sync::mpsc::UnboundedReceiver;
use tracing::info;

// By default, custom metadata request timeout is set to 2 seconds.
const DEFAULT_TIMEOUT: Duration = Duration::from_secs(2);

#[cfg_attr(scylla_cloud_tests, ignore)]
#[tokio::test]
#[ntest::timeout(20000)]
async fn test_custom_metadata_timeouts() {
    setup_tracing();

    fn expected_clause(dur: Duration) -> String {
        format!("USING TIMEOUT {}ms", dur.as_millis())
    }

    fn contains_subslice(slice: &[u8], subslice: &[u8]) -> bool {
        slice
            .windows(subslice.len())
            .any(|window| window == subslice)
    }

    fn check_if_connected_to_scylladb(session: &Session) -> bool {
        session
            .get_cluster_state()
            .get_nodes_info()
            .first()
            .and_then(|node| node.sharder())
            .is_some()
    }

    fn assert_no_custom_timeout(frame: RequestFrame) {
        let clause = "USING TIMEOUT";
        assert!(
            !contains_subslice(&frame.body, clause.as_bytes()),
            "slice {:?} does contain subslice {:?}",
            &frame.body,
            clause,
        );
    }

    fn assert_custom_timeout(frame: RequestFrame, dur: Duration) {
        let expected = expected_clause(dur);
        assert!(
            contains_subslice(&frame.body, expected.as_bytes()),
            "slice {:?} does not contain subslice {:?}",
            &frame.body,
            expected,
        );
    }

    fn assert_custom_timeout_iff_scylladb(
        frame: RequestFrame,
        dur: Duration,
        connected_to_scylladb: bool,
    ) {
        if connected_to_scylladb {
            info!(
                "Connected to ScyllaDB, so expecting custom timeout to be set to {}ms",
                dur.as_millis()
            );
            assert_custom_timeout(frame, dur);
        } else {
            info!("Connected to NOT ScyllaDB, so expecting custom timeout to not be set");
            assert_no_custom_timeout(frame);
        }
    }

    let res = test_with_3_node_cluster(
        ShardAwareness::QueryNode,
        |proxy_uris, translation_map, mut running_proxy| async move {
            let (feedback_tx, mut feedback_rx) = mpsc::unbounded_channel();

            // This rule feeds back all QUERY and PREPARE requests that are executed
            // on a control connection.
            let metadata_query_feedback_rule = RequestRule(
                Condition::and(
                    Condition::ConnectionRegisteredAnyEvent,
                    Condition::or(
                        Condition::RequestOpcode(RequestOpcode::Query),
                        Condition::RequestOpcode(RequestOpcode::Prepare),
                    ),
                ),
                RequestReaction::noop().with_feedback_when_performed(feedback_tx),
            );

            async fn check_fedback_messages_with_session(
                proxy_uris: [String; 3],
                translation_map: HashMap<SocketAddr, SocketAddr>,
                rule: RequestRule,
                running_proxy: &mut RunningProxy,
                feedback_rx: &mut UnboundedReceiver<(RequestFrame, Option<u16>)>,
                builder_modifier: impl Fn(SessionBuilder) -> SessionBuilder,
                check: impl Fn(RequestFrame, bool),
            ) {
                for node in running_proxy.running_nodes.iter_mut() {
                    node.change_request_rules(Some(vec![rule.clone()]));
                }

                let builder = SessionBuilder::new()
                    .known_node(proxy_uris[0].as_str())
                    .address_translator(Arc::new(translation_map));
                let builder = builder_modifier(builder);

                let session = builder.build().await.unwrap();

                let connected_to_scylladb = check_if_connected_to_scylladb(&session);

                // Turn off rules, so that no races are possible about some messages fed
                // to the feedback channel after we have already cleared it.
                running_proxy.turn_off_rules();

                fn map_fedback_message<'a, T>(
                    rx: &'a mut UnboundedReceiver<(RequestFrame, Option<u16>)>,
                    f: impl Fn(RequestFrame) -> T + 'a,
                ) -> impl Iterator<Item = T> + 'a {
                    std::iter::from_fn(move || match rx.try_recv() {
                        Ok((frame, _)) => Some(f(frame)),
                        Err(TryRecvError::Disconnected) => {
                            panic!("feedback tx disconnected unexpectedly")
                        }
                        Err(TryRecvError::Empty) => None,
                    })
                }

                let n_fedback =
                    map_fedback_message(feedback_rx, |frame| check(frame, connected_to_scylladb))
                        .count();

                info!("Checked {} fedback messages", n_fedback);
            }

            // First check - explicitly disabled custom metadata request timeouts.
            {
                info!("Test case 1: checking for no custom timeout when explicitly disabled");
                check_fedback_messages_with_session(
                    proxy_uris.clone(),
                    translation_map.clone(),
                    metadata_query_feedback_rule.clone(),
                    &mut running_proxy,
                    &mut feedback_rx,
                    |mut builder| {
                        builder.config.metadata_request_serverside_timeout = None;
                        builder
                    },
                    |frame, _is_scylladb| assert_no_custom_timeout(frame),
                )
                .await;
            }

            // Second check - explicitly set custom metadata request timeout.
            {
                let custom_timeout = Duration::from_millis(2137);
                info!("Test case 2: custom timeout explicitly set");
                check_fedback_messages_with_session(
                    proxy_uris.clone(),
                    translation_map.clone(),
                    metadata_query_feedback_rule.clone(),
                    &mut running_proxy,
                    &mut feedback_rx,
                    |builder| builder.metadata_request_serverside_timeout(custom_timeout),
                    |frame, is_scylladb| {
                        assert_custom_timeout_iff_scylladb(frame, custom_timeout, is_scylladb)
                    },
                )
                .await;
            }

            // Third check - by default, a custom metadata request timeout is set to some number.
            {
                info!("Test case 3: custom timeout is set by default");
                check_fedback_messages_with_session(
                    proxy_uris,
                    translation_map,
                    metadata_query_feedback_rule.clone(),
                    &mut running_proxy,
                    &mut feedback_rx,
                    |builder| builder,
                    |frame, is_scylladb| {
                        assert_custom_timeout_iff_scylladb(frame, DEFAULT_TIMEOUT, is_scylladb)
                    },
                )
                .await;
            }

            running_proxy
        },
    )
    .await;

    match res {
        Ok(()) => (),
        Err(ProxyError::Worker(WorkerError::DriverDisconnected(_))) => (),
        Err(err) => panic!("{}", err),
    }
}
