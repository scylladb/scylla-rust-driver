use crate::utils::test_with_3_node_cluster;
use scylla::frame::types;
use scylla::retry_policy::FallthroughRetryPolicy;
use scylla::transport::session::Session;
use scylla::{frame::protocol_features::ProtocolFeatures, test_utils::unique_keyspace_name};
use scylla::{ExecutionProfile, SessionBuilder};
use std::sync::Arc;
use tokio::sync::mpsc;

use scylla_proxy::{
    Condition, ProxyError, Reaction, RequestFrame, RequestOpcode, RequestReaction, RequestRule,
    ResponseOpcode, ResponseReaction, ResponseRule, ShardAwareness, WorkerError,
};

#[tokio::test]
#[ntest::timeout(20000)]
#[cfg(not(scylla_cloud_tests))]
async fn if_lwt_optimisation_mark_offered_then_negotiatied_and_lwt_routed_optimally() {
    // This is just to increase the likelihood that only intended prepared statements (which contain this mark) are captured by the proxy.
    const MAGIC_MARK: i32 = 123;

    let res = test_with_3_node_cluster(ShardAwareness::QueryNode, |proxy_uris, translation_map, mut running_proxy| async move {

        // We set up proxy, so that it informs us (via supported_rx) about cluster's Supported features (including LWT optimisation mark),
        // and also passes us information about which node was queried (via prepared_rx).

        let (supported_tx, mut supported_rx) = mpsc::unbounded_channel();

        running_proxy.running_nodes[0].change_response_rules(Some(vec![ResponseRule(
            Condition::ResponseOpcode(ResponseOpcode::Supported),
            ResponseReaction::noop().with_feedback_when_performed(supported_tx)
        )]));

        let prepared_rule = |tx| RequestRule(
            Condition::and(Condition::RequestOpcode(RequestOpcode::Execute), Condition::BodyContainsCaseSensitive(Box::new(MAGIC_MARK.to_be_bytes()))),
            RequestReaction::noop().with_feedback_when_performed(tx)
        );

        let mut prepared_rxs = [0, 1, 2].map(|i| {
            let (prepared_tx, prepared_rx) = mpsc::unbounded_channel();
            running_proxy.running_nodes[i].change_request_rules(Some(vec![prepared_rule(prepared_tx)]));
            prepared_rx
        });

        let handle = ExecutionProfile::builder()
            .retry_policy(Box::new(FallthroughRetryPolicy))
            .build()
            .into_handle();

        // DB preparation phase
        let session: Session = SessionBuilder::new()
            .known_node(proxy_uris[0].as_str())
            .default_execution_profile_handle(handle)
            .address_translator(Arc::new(translation_map))
            .build()
            .await
            .unwrap();

        let supported_frame = supported_rx.recv().await.unwrap();
        let supported_options = types::read_string_multimap(&mut &*supported_frame.body).unwrap();
        let supported_features = ProtocolFeatures::parse_from_supported(&supported_options);

        // This will branch our test for cases both when cluster supports the optimisations and when it does not.
        let supports_optimisation_mark = supported_features.lwt_optimization_meta_bit_mask.is_some();

        // Create schema
        let ks = unique_keyspace_name();
        session.query(format!("CREATE KEYSPACE IF NOT EXISTS {} WITH REPLICATION = {{'class' : 'NetworkTopologyStrategy', 'replication_factor' : 3}}", ks), &[]).await.unwrap();
        session.use_keyspace(ks, false).await.unwrap();

        session
            .query("CREATE TABLE t (a int primary key, b int)", &[])
            .await
            .unwrap();


        fn clear_rxs(rxs: &mut [mpsc::UnboundedReceiver<RequestFrame>; 3]) {
            for rx in rxs.iter_mut() {
                while rx.try_recv().is_ok() {}
            }
        }

        async fn assert_all_replicas_queried(rxs: &mut [mpsc::UnboundedReceiver<RequestFrame>; 3]) {
            for rx in rxs.iter_mut() {
                rx.recv().await.unwrap();
            }
            clear_rxs(rxs);
        }

        fn assert_one_replica_queried(rxs: &mut [mpsc::UnboundedReceiver<RequestFrame>; 3]) {
            let mut found_queried = false;
            for rx in rxs.iter_mut() {
                if rx.try_recv().is_ok() {
                    assert!(!found_queried);
                    found_queried = true;
                };
            }
            assert!(found_queried);
            clear_rxs(rxs);
        }

        #[allow(unused)]
        fn who_was_queried(rxs: &mut [mpsc::UnboundedReceiver<RequestFrame>; 3]) {
            for (i, rx) in rxs.iter_mut().enumerate() {
                if rx.try_recv().is_ok() {
                    println!("{} was queried.", i);
                    return;
                }
            }
            println!("NOBODY was queried!");
        }

        // We will check which nodes where queries, for both LWT and non-LWT prepared statements.
        let prepared_non_lwt = session.prepare("INSERT INTO t (a, b) VALUES (?, 1)").await.unwrap();
        let prepared_lwt = session.prepare("UPDATE t SET b=3 WHERE a=? IF b=2").await.unwrap();

        if supports_optimisation_mark {
            // We make sure that the driver properly marked prepared statements wrt being LWT.
            assert!(!prepared_non_lwt.is_confirmed_lwt());
            assert!(prepared_lwt.is_confirmed_lwt());
        }

        assert!(prepared_non_lwt.is_token_aware());
        assert!(prepared_lwt.is_token_aware());

        // We execute non-LWT statements and ensure that all nodes were queried (due to RoundRobin).
        for _ in 0..15 {
            session.execute(&prepared_non_lwt, (MAGIC_MARK,)).await.unwrap();
        }

        assert_all_replicas_queried(&mut prepared_rxs).await;

        // We execute LWT statements, and...
        for _ in 0..15 {
            session.execute(&prepared_lwt, (MAGIC_MARK,)).await.unwrap();
        }

        if supports_optimisation_mark {
            // ...if cluster supports LWT, we assert that one replica was always queried first (and effectively only that one was queried).
            assert_one_replica_queried(&mut prepared_rxs);
        } else {
            // ...else we assert that replicas were shuffled as in case of non-LWT.
            assert_all_replicas_queried(&mut prepared_rxs).await;
        }

        running_proxy
    }).await;

    match res {
        Ok(()) => (),
        Err(ProxyError::Worker(WorkerError::DriverDisconnected(_))) => (),
        Err(err) => panic!("{}", err),
    }
}
