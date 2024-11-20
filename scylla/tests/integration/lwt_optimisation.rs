use crate::utils::{
    scylla_supports_tablets, setup_tracing, test_with_3_node_cluster, unique_keyspace_name,
};
use scylla::retry_policy::FallthroughRetryPolicy;
use scylla::transport::session::Session;
use scylla::{ExecutionProfile, SessionBuilder};
use scylla_cql::frame::protocol_features::ProtocolFeatures;
use scylla_cql::frame::types;
use std::sync::Arc;
use tokio::sync::mpsc;

use scylla_proxy::{
    Condition, ProxyError, Reaction, RequestFrame, RequestOpcode, RequestReaction, RequestRule,
    ResponseOpcode, ResponseReaction, ResponseRule, ShardAwareness, TargetShard, WorkerError,
};

#[tokio::test]
#[ntest::timeout(20000)]
#[cfg(not(scylla_cloud_tests))]
async fn if_lwt_optimisation_mark_offered_then_negotiatied_and_lwt_routed_optimally() {
    setup_tracing();

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
            .retry_policy(Arc::new(FallthroughRetryPolicy))
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

        let (supported_frame, _shard) = supported_rx.recv().await.unwrap();
        let supported_options = types::read_string_multimap(&mut &*supported_frame.body).unwrap();
        let supported_features = ProtocolFeatures::parse_from_supported(&supported_options);

        // This will branch our test for cases both when cluster supports the optimisations and when it does not.
        let supports_optimisation_mark = supported_features.lwt_optimization_meta_bit_mask.is_some();

        // Create schema
        let ks = unique_keyspace_name();
        let mut create_ks = format!("CREATE KEYSPACE IF NOT EXISTS {} WITH REPLICATION = {{'class' : 'NetworkTopologyStrategy', 'replication_factor' : 3}}", ks);
        if scylla_supports_tablets(&session).await {
            create_ks += " and TABLETS = { 'enabled': false}";
        }
        session.query_unpaged(create_ks, &[]).await.unwrap();
        session.use_keyspace(ks, false).await.unwrap();

        session
            .query_unpaged("CREATE TABLE t (a int primary key, b int)", &[])
            .await
            .unwrap();

        type Rxs = [mpsc::UnboundedReceiver<(RequestFrame, Option<TargetShard>)>; 3];

        fn clear_rxs(rxs: &mut Rxs) {
            for rx in rxs.iter_mut() {
                while rx.try_recv().is_ok() {}
            }
        }

        fn get_num_queried(rxs: &mut Rxs) -> usize {
            let num_queried = rxs.iter_mut().filter_map(|rx| rx.try_recv().ok()).count();
            clear_rxs(rxs);
            num_queried
        }

        fn assert_multiple_replicas_queried(rxs: &mut Rxs) {
            let num_queried = get_num_queried(rxs);

            assert!(num_queried > 1);
        }

        fn assert_one_replica_queried(rxs: &mut Rxs) {
            let num_queried = get_num_queried(rxs);

            assert!(num_queried == 1);
        }

        #[allow(unused)]
        fn who_was_queried(rxs: &mut Rxs) {
            for (i, rx) in rxs.iter_mut().enumerate() {
                if rx.try_recv().is_ok() {
                    println!("{} was queried.", i);
                    return;
                }
            }
            println!("NOBODY was queried!");
        }

        // We will check which nodes were queried, for both LWT and non-LWT prepared statements.
        let prepared_non_lwt = session.prepare("INSERT INTO t (a, b) VALUES (?, 1)").await.unwrap();
        let prepared_lwt = session.prepare("UPDATE t SET b=3 WHERE a=? IF b=2").await.unwrap();

        if supports_optimisation_mark {
            // We make sure that the driver properly marked prepared statements wrt being LWT.
            assert!(!prepared_non_lwt.is_confirmed_lwt());
            assert!(prepared_lwt.is_confirmed_lwt());
        }

        assert!(prepared_non_lwt.is_token_aware());
        assert!(prepared_lwt.is_token_aware());

        // We execute non-LWT statements and ensure that multiple nodes were queried.
        //
        // Note that our DefaultPolicy no longer performs round robin, but instead randomly picks a replica.
        // To see multiple replicas here, we cannot choose a fixed pick seed, so we must rely on randomness.
        // It happened several times in CI that *not all* replicas were queried, but now we only
        // assert that *more than one* replica is queried. Moreover, we increased iterations
        // from 15 to 30 in hope this will suffice to prevent flakiness.
        // Alternatively, we could give up this part of the test and only test LWT part, but then
        // we couldn't be sure that in non-LWT case the driver truly chooses various replicas.
        for _ in 0..30 {
            session.execute_unpaged(&prepared_non_lwt, (MAGIC_MARK,)).await.unwrap();
        }

        assert_multiple_replicas_queried(&mut prepared_rxs);

        // We execute LWT statements, and...
        for _ in 0..15 {
            session.execute_unpaged(&prepared_lwt, (MAGIC_MARK,)).await.unwrap();
        }

        if supports_optimisation_mark {
            // ...if cluster supports LWT, we assert that one replica was always queried first (and effectively only that one was queried).
            assert_one_replica_queried(&mut prepared_rxs);
        } else {
            // ...else we assert that replicas were shuffled as in case of non-LWT.
            assert_multiple_replicas_queried(&mut prepared_rxs);
        }

        running_proxy
    }).await;

    match res {
        Ok(()) => (),
        Err(ProxyError::Worker(WorkerError::DriverDisconnected(_))) => (),
        Err(err) => panic!("{}", err),
    }
}
