use std::sync::Arc;

use scylla::client::caching_session::{CachingSession, CachingSessionBuilder};
use scylla::client::session_builder::SessionBuilder;
use scylla_cql::frame::request::Execute;
use scylla_cql::frame::request::Request;
use scylla_proxy::Condition;
use scylla_proxy::ProxyError;
use scylla_proxy::Reaction;
use scylla_proxy::RequestFrame;
use scylla_proxy::RequestOpcode;
use scylla_proxy::RequestReaction;
use scylla_proxy::RequestRule;
use scylla_proxy::WorkerError;
use tokio::sync::mpsc;

use crate::utils::test_with_3_node_cluster;

#[tokio::test]
#[cfg_attr(scylla_cloud_tests, ignore)]
async fn test_caching_session_metadata_cache() {
    let res = test_with_3_node_cluster(
        scylla_proxy::ShardAwareness::QueryNode,
        |proxy_uris, translation_map, mut running_proxy| async move {
            let (feedback_tx, mut feedback_rx) = mpsc::unbounded_channel();
            let prepared_request_feedback_rule = RequestRule(
                Condition::and(
                    Condition::not(Condition::ConnectionRegisteredAnyEvent),
                    Condition::RequestOpcode(RequestOpcode::Execute),
                ),
                RequestReaction::noop().with_feedback_when_performed(feedback_tx),
            );
            for node in running_proxy.running_nodes.iter_mut() {
                node.change_request_rules(Some(vec![prepared_request_feedback_rule.clone()]));
            }

            async fn verify_statement_metadata(
                session: &CachingSession,
                statement: &str,
                should_have_metadata: bool,
                feedback: &mut mpsc::UnboundedReceiver<(RequestFrame, Option<u16>)>,
            ) {
                let _result = session.execute_unpaged(statement, ()).await.unwrap();
                let (req_frame, _) = feedback.recv().await.unwrap();
                let _ = feedback.try_recv().unwrap_err(); // There should be only one frame.
                let request = req_frame.deserialize().unwrap();
                let Request::Execute(Execute { parameters, .. }) = request else {
                    panic!("Unexpected request type");
                };
                let has_metadata = !parameters.skip_metadata;
                assert_eq!(has_metadata, should_have_metadata);
            }

            const REQUEST: &str = "SELECT * FROM system.local WHERE key = 'local'";

            let session = SessionBuilder::new()
                .known_node(proxy_uris[0].as_str())
                .address_translator(Arc::new(translation_map.clone()))
                .build()
                .await
                .unwrap();
            let caching_session: CachingSession = CachingSessionBuilder::new(session)
                .use_cached_result_metadata(false) // Default, set just to be more explicit
                .build();

            // Skipping metadata was not set, so metadata should be present
            verify_statement_metadata(&caching_session, REQUEST, true, &mut feedback_rx).await;

            // It should also be present when executing statement already in cache
            verify_statement_metadata(&caching_session, REQUEST, true, &mut feedback_rx).await;

            let session = SessionBuilder::new()
                .known_node(proxy_uris[0].as_str())
                .address_translator(Arc::new(translation_map))
                .build()
                .await
                .unwrap();
            let caching_session: CachingSession = CachingSessionBuilder::new(session)
                .use_cached_result_metadata(true)
                .build();

            // Now we set skip_metadata to true, so metadata should not be present for a new query
            verify_statement_metadata(&caching_session, REQUEST, false, &mut feedback_rx).await;

            // It should also not be present when executing statement already in cache
            verify_statement_metadata(&caching_session, REQUEST, false, &mut feedback_rx).await;

            // Test also without setting it explicitly, to verify that it is false by default.
            let caching_session: CachingSession =
                CachingSessionBuilder::new_shared(Arc::clone(&session)).build();

            // Skipping metadata was not set, so metadata should be present
            verify_statement_metadata(&caching_session, REQUEST, true, &mut feedback_rx).await;

            // It should also be present when executing statement already in cache
            verify_statement_metadata(&caching_session, REQUEST, true, &mut feedback_rx).await;

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
