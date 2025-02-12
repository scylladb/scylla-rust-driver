use crate::common::utils::{
    create_new_session_builder, scylla_supports_tablets, setup_tracing, unique_keyspace_name, PerformDDL
};
use scylla::client::execution_profile::ExecutionProfile;
use scylla::policies::load_balancing::{DefaultPolicy, LatencyAwarenessBuilder, LoadBalancingPolicy};
use scylla::policies::retry::FallthroughRetryPolicy;
use scylla::client::session::Session;
use scylla::query::Query;
use scylla::observability::history::{HistoryCollector, StructuredHistory,};
use std::sync::Arc;
use tokio::sync::Mutex;
use crate::ccm::cluster::{Cluster, ClusterOptions};
use crate::ccm::{run_ccm_test, CLUSTER_VERSION};

fn get_queried_node(mut history: StructuredHistory) -> String {
    let mut node_addr: String = "".to_string();
    for query in &mut history.requests {
        for fiber in std::iter::once(&mut query.non_speculative_fiber)
            .chain(query.speculative_fibers.iter_mut())
        {
            for attempt in &mut fiber.attempts {
                println!("Attempt node_addr: {}", attempt.node_addr);
                node_addr = attempt.node_addr.to_string();
            }
        }
    }

    node_addr
}

fn cluster_3_node() -> ClusterOptions {
    ClusterOptions {
        name: "cluster_3_node".to_string(),
        version: CLUSTER_VERSION.clone(),
        nodes: vec![3],
        ..ClusterOptions::default()
    }
}
// run_ccm_test(cluster_1_node, test).await;
async fn test_query_routed_according_to_policy(cluster: Arc<Mutex<Cluster>>, policy: Arc<dyn LoadBalancingPolicy>, tablets_enabled: bool, _running_nodes_amount: i32) {
    // Test that DefaultPolicy was as expected for LWT and regular queries for tablets and vnodes
    // policy: DefaultPolicy object defined according to test
    // supports_tablets: True (tablets test) / False (vnodes test)
    // running_nodes_amount: cluster state - how much alive nodes (maximum is 3)

    setup_tracing();

    // This is just to increase the likelihood that only intended prepared statements (which contain this mark) are captured by the proxy.
    const MAGIC_MARK: i32 = 123;
    let cluster = cluster.lock().await;

    let handle = ExecutionProfile::builder()
        .load_balancing_policy(policy.clone())
        .retry_policy(Arc::new(FallthroughRetryPolicy))
        .build()
        .into_handle();

    let history_listener = Arc::new(HistoryCollector::new());

    // DB preparation phase
    let session: Session = cluster.make_session_builder()
        .await
        .default_execution_profile_handle(handle)
        .build()
        .await
        .unwrap();

    // *** This is part of original test "lwt_optimisation.rs", uses proxy. Check may be need be implemented with CCM
    // let (supported_frame, _shard) = supported_rx.recv().await.unwrap();
    // let supported_options = types::read_string_multimap(&mut &*supported_frame.body).unwrap();
    // let supported_features = ProtocolFeatures::parse_from_supported(&supported_options);

    // This will branch our test for cases both when cluster supports the optimisations and when it does not.
    // let supports_optimisation_mark = supported_features.lwt_optimization_meta_bit_mask.is_some();
    // *** END - This is part of original test "lwt_optimisation.rs", uses proxy. Check may be need be implemented with CCM

    // Create schema
    let ks = unique_keyspace_name();

    // Enable or disable tablets for the keyspace
    let mut tablets_clause = "";
    if scylla_supports_tablets(&session).await{
        if tablets_enabled{
            tablets_clause =  " AND tablets = {'enabled': true}";
        } else{
            tablets_clause =  " AND tablets = {'enabled': false}";
        }
    }
    
    let create_ks = format!("CREATE KEYSPACE IF NOT EXISTS {} WITH REPLICATION = {{'class' : 'NetworkTopologyStrategy', 'replication_factor' : 3}}{}", ks, tablets_clause);
    
    session.ddl(create_ks).await.unwrap();
    session.use_keyspace(ks, false).await.unwrap();

    session
        .ddl("CREATE TABLE t (a int primary key, b int)")
        .await
        .unwrap();

    let mut non_lwt_query: Query = Query::new("INSERT INTO t (a, b) VALUES (?, 1)");
    let mut lwt_query: Query = Query::new("UPDATE t SET b=3 WHERE a=? IF b=2");
    non_lwt_query.set_history_listener(history_listener.clone());
    lwt_query.set_history_listener(history_listener.clone());

    // We will check which nodes were queried, for both LWT and non-LWT prepared statements.
    for _ in 0..15 {
        let _ignore_non_lwt_error = session.query_unpaged(non_lwt_query.clone(), (MAGIC_MARK,)).await;
        let non_lwt_query_history: StructuredHistory = history_listener.clone_structured_history();
        // Query history like:
        // === Request #0 ===
        // | start_time: 2025-02-13 12:49:15.035087534 UTC
        // | Non-speculative attempts:
        // | - Attempt #0 sent to 127.0.2.3:9042
        // |   request send time: 2025-02-13 12:49:15.035286454 UTC
        // |   Success at 2025-02-13 12:49:15.036940277 UTC
        // |
        // | Request successful at 2025-02-13 12:49:15.036962012 UTC

        println!("Non LWT query history: {}", non_lwt_query_history);        
        let queried_node_port = get_queried_node(non_lwt_query_history);
        let queried_node = queried_node_port.split(":").next().unwrap();
        println!("non-LWT queried_node: {:?}", queried_node);
        // TODO: get expected node IP
        let expected_result = "127.0.0.1";
        assert_eq!(
            expected_result,
            queried_node
        );
    }

    for _ in 0..15 {
        let _ignore_lwt_error = session.query_unpaged(lwt_query.clone(), (MAGIC_MARK,)).await;    
        let lwt_query_history: StructuredHistory = history_listener.clone_structured_history();
        println!("LWT query history: {}", lwt_query_history);
        let queried_node_port = get_queried_node(lwt_query_history);
        let queried_node = queried_node_port.split(":").next().unwrap();
        println!("LWT queried_node: {:?}", queried_node);
        // TODO: get expected node IP
        let expected_result = "127.0.0.1";
        assert_eq!(
            expected_result,
            queried_node
        );
    }

    // *** This is part of original test "lwt_optimisation.rs", uses proxy. Check may be need be implemented with CCM
    // if supports_optimisation_mark {
    //     // We make sure that the driver properly marked prepared statements wrt being LWT.
    //     assert!(!prepared_non_lwt.is_confirmed_lwt());
    //     assert!(prepared_lwt.is_confirmed_lwt());
    // }

    // assert!(prepared_non_lwt.is_token_aware());
    // assert!(prepared_lwt.is_token_aware());

    // We execute non-LWT statements and ensure that multiple nodes were queried.
    //
    // Note that our DefaultPolicy no longer performs round robin, but instead randomly picks a replica.
    // To see multiple replicas here, we cannot choose a fixed pick seed, so we must rely on randomness.
    // It happened several times in CI that *not all* replicas were queried, but now we only
    // assert that *more than one* replica is queried. Moreover, we increased iterations
    // from 15 to 30 in hope this will suffice to prevent flakiness.
    // Alternatively, we could give up this part of the test and only test LWT part, but then
    // we couldn't be sure that in non-LWT case the driver truly chooses various replicas.
    // for _ in 0..30 {
    //     session.execute_unpaged(&prepared_non_lwt, (MAGIC_MARK,)).await.unwrap();
    // }

    // assert_multiple_replicas_queried(&mut prepared_rxs);

    // We execute LWT statements, and...
    // for _ in 0..15 {
    //     session.execute_unpaged(&prepared_lwt, (MAGIC_MARK,)).await.unwrap();
    // }

    // if supports_optimisation_mark {
    //     // ...if cluster supports LWT, we assert that one replica was always queried first (and effectively only that one was queried).
    //     assert_one_replica_queried(&mut prepared_rxs);
    // } else {
    //     // ...else we assert that replicas were shuffled as in case of non-LWT.
    //     assert_multiple_replicas_queried(&mut prepared_rxs);
    // }

    // running_proxy
    // *** END - This is part of original test "lwt_optimisation.rs", uses proxy. Check may be need be implemented with CCM
}


#[tokio::test]
#[ntest::timeout(20000)]
#[cfg_attr(not(ccm_tests), ignore)]
async fn query_routed_optimally_with_tablets_and_policy_token_aware_on_preferences_none_permit_failover_on(){
    async fn inner_test(cluster: Arc<Mutex<Cluster>>){
        let policy = DefaultPolicy::builder()
        .token_aware(true)
        .permit_dc_failover(true)
        .build();
        test_query_routed_according_to_policy(cluster, policy, true, 3).await;
    }
    run_ccm_test(cluster_3_node, inner_test).await;
}

#[tokio::test]
#[ntest::timeout(20000)]
#[cfg_attr(not(ccm_tests), ignore)]
async fn query_routed_optimally_with_tablets_and_policy_token_aware_on_preferences_none_permit_failover_off(){
    async fn inner_test(cluster: Arc<Mutex<Cluster>>){
        let policy = DefaultPolicy::builder()
        .token_aware(true)
        .permit_dc_failover(false)
        .build();
        test_query_routed_according_to_policy(cluster, policy, true, 3).await;
    }
    run_ccm_test(cluster_3_node, inner_test).await;
}

#[tokio::test]
#[ntest::timeout(20000)]
#[cfg_attr(not(ccm_tests), ignore)]
async fn query_routed_optimally_with_tablets_and_policy_token_aware_on_preferences_dc_permit_failover_on(){
    async fn inner_test(cluster: Arc<Mutex<Cluster>>){ 
        // TODO: 
        // let cluster_lock = cluster.lock().await;
        // let node = cluster_lock.nodes().iter().next();
        // let datacenter_id: String = "1".to_string();
        let policy = DefaultPolicy::builder()
        .prefer_datacenter("dc1".to_string())
        .token_aware(true)
        .permit_dc_failover(true)
        .build();
        test_query_routed_according_to_policy(cluster, policy, true, 3).await;
    }
    run_ccm_test(cluster_3_node, inner_test).await;
    
}

// This is a regression test for #696.
#[tokio::test]
#[ntest::timeout(2000)]
async fn latency_aware_query_completes() {
    setup_tracing();
    let policy = DefaultPolicy::builder()
        .latency_awareness(LatencyAwarenessBuilder::default())
        .build();
    let handle = ExecutionProfile::builder()
        .load_balancing_policy(policy)
        .build()
        .into_handle();

    let session = create_new_session_builder()
        .default_execution_profile_handle(handle)
        .build()
        .await
        .unwrap();

    session.query_unpaged("whatever", ()).await.unwrap_err();
}