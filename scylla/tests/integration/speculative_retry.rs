use crate::speculative_tests_utils::{
    check_expectations, convert_into_simple_history, into_proxy_request_rules,
    AttemptRecordExpectation, NodeExpectation, SimpleProxyRules,
};
use crate::utils::{setup_tracing, test_with_3_node_cluster, unique_keyspace_name, PerformDDL};
use scylla::client::execution_profile::ExecutionProfile;
use scylla::client::session::Session;
use scylla::client::session_builder;
use scylla::errors::RequestAttemptError;
use scylla::observability::history::HistoryCollector;
use scylla::policies::retry::{
    FallthroughRetryPolicy, RequestInfo, RetryDecision, RetryPolicy,
    RetrySession,
};
use scylla::policies::speculative_execution::{
    SimpleSpeculativeExecutionPolicy, SpeculativeExecutionPolicy,
};
use scylla::query::Query;
use scylla_cql::frame::request::RequestOpcode;
use scylla_cql::frame::response::error::DbError;
use scylla_proxy::{example_db_errors, ProxyError, ShardAwareness, WorkerError};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime};

#[tokio::test]
#[ntest::timeout(300000)]
#[cfg(not(scylla_cloud_tests))]
async fn speculative_execution_is_fired2() {
    setup_tracing();

    let ks = unique_keyspace_name();

    // There is no way to isolate attempts, test logic relays on ticks,
    // which is time when one given attempts is going to be 100% executed
    let tick = Duration::from_millis(200);
    let never = tick * 10000;

    let target_opcode = RequestOpcode::Execute;
    let simple_speculative = Some(Arc::new(SimpleSpeculativeExecutionPolicy {
        max_retry_count: 3,
        retry_interval: tick * 2,
    }) as Arc<dyn SpeculativeExecutionPolicy>);

    enum QueryResultExpectation {
        Success,
        Failed,
    }

    struct TestCase {
        name: String,
        request_rules: Vec<SimpleProxyRules>,
        speculative_policy: Option<Arc<dyn SpeculativeExecutionPolicy>>,
        retry_policy: Arc<dyn RetryPolicy>,
        expectation: Vec<AttemptRecordExpectation>,
        expected_query_result: QueryResultExpectation,
    }

    // an error that is retried only once on the next host
    let error_retry_next = example_db_errors::overloaded;

    // an error that is retried only once on the next host
    let error_retry_once = example_db_errors::protocol_error;

    // an error that is retried only once by on the same host
    let error_retry_same = example_db_errors::is_bootstrapping;

    // an error that is not retried
    let error_noretry = example_db_errors::truncate_error;

    #[derive(Debug)]
    struct TestRetryPolicy {
        max_retry: usize,
    }

    impl TestRetryPolicy {
        fn new(max_retry: usize) -> Self {
            Self { max_retry }
        }
    }

    impl RetryPolicy for TestRetryPolicy {
        fn new_session(&self) -> Box<dyn RetrySession> {
            Box::new(TestRetrySession::new(self.max_retry))
        }
    }

    struct TestRetrySession {
        try_done: AtomicUsize,
        max_retry: usize,
    }

    impl TestRetrySession {
        fn new(max_retry: usize) -> Self {
            TestRetrySession {
                max_retry,
                try_done: AtomicUsize::new(0),
            }
        }
    }

    impl RetrySession for TestRetrySession {
        fn decide_should_retry(&mut self, rq: RequestInfo) -> RetryDecision {
            if self.try_done.fetch_add(1, Ordering::Relaxed) >= self.max_retry {
                return RetryDecision::DontRetry;
            }
            match rq.error {
                RequestAttemptError::DbError(DbError::Overloaded, _) => {
                    RetryDecision::RetryNextTarget(None)
                }
                RequestAttemptError::DbError(DbError::IsBootstrapping, _) => {
                    RetryDecision::RetrySameTarget(None)
                }
                _ => RetryDecision::DontRetry,
            }
        }
        fn reset(&mut self) {}
    }

    let test_cases = vec![
        TestCase {
            name: "pass on first try".to_string(),
            request_rules: vec![], // No error injected, query is going just pass
            speculative_policy: simple_speculative.clone(),
            retry_policy: Arc::new(TestRetryPolicy::new(3)),
            expectation: vec![
                // Should just succeed
                AttemptRecordExpectation::RegularSuccess(NodeExpectation::AnyNode(false)),
            ],
            expected_query_result: QueryResultExpectation::Success,
        },
        TestCase {
            name: "failure on first try then pass".to_string(),
            request_rules: vec![
                // Will make query fail once with error that is retried on the next node
                // And then it is going to pass
                SimpleProxyRules::fail(None, error_retry_next),
            ],
            speculative_policy: simple_speculative.clone(),
            retry_policy: Arc::new(TestRetryPolicy::new(3)),
            expectation: vec![
                // Should retry on next node once and then succeed
                AttemptRecordExpectation::RegularFailure(NodeExpectation::AnyNode(false)),
                AttemptRecordExpectation::RegularSuccess(NodeExpectation::UniqueNode(false)),
            ],
            expected_query_result: QueryResultExpectation::Success,
        },
        TestCase {
            name: "block regular fiber, succeeds on speculative".to_string(),
            request_rules: vec![
                // Will make first query scheduled to fail too far in future
                // Next retry should be speculative and should succeed, not waiting for regular fiber response
                SimpleProxyRules::delay(never),
            ],
            speculative_policy: simple_speculative.clone(),
            retry_policy: Arc::new(TestRetryPolicy::new(3)),
            expectation: vec![
                // First attempt on regular fiber stays without response (response comes too late)
                // Second attempts on speculative fiber succeeds
                AttemptRecordExpectation::RegularNoResponse(NodeExpectation::AnyNode(false)),
                AttemptRecordExpectation::SpeculativeSuccess(NodeExpectation::UniqueNode(false)),
            ],
            expected_query_result: QueryResultExpectation::Success,
        },
        TestCase {
            name: "block regular fiber, fails and then succeeds on speculative".to_string(),
            request_rules: vec![
                // Will make query fail once with error that is retried on the next node
                // Next retry should be speculative and should fail and to be retried on next node
                // Next attempt (speculative as well) should succeed
                SimpleProxyRules::delay(never),
                SimpleProxyRules::fail(None, error_retry_next),
            ],
            speculative_policy: simple_speculative.clone(),
            retry_policy: Arc::new(TestRetryPolicy::new(3)),
            expectation: vec![
                // First attempt on regular fiber stays without response (response comes too late)
                // Second attempts on speculative fiber succeeds
                AttemptRecordExpectation::RegularNoResponse(NodeExpectation::AnyNode(false)),
                AttemptRecordExpectation::SpeculativeFailure(NodeExpectation::UniqueNode(false)),
                AttemptRecordExpectation::SpeculativeSuccess(NodeExpectation::UniqueNode(false)),
            ],
            expected_query_result: QueryResultExpectation::Success,
        },

        TestCase {
            name: "Regular fails after speculative fiber fails twice".to_string(),
            request_rules: vec![
                // Will make query fail once with error that is retried on the next node
                // Next retry should be speculative and should fail and to be retried on next node
                // Next attempt (speculative as well) should succeed
                SimpleProxyRules::fail(Some(tick*4), error_retry_next),
                SimpleProxyRules::fail(None, error_retry_next),
                SimpleProxyRules::fail(None, error_retry_next),
            ],
            speculative_policy: simple_speculative.clone(),
            retry_policy: Arc::new(TestRetryPolicy::new(3)),
            expectation: vec![
                // First attempt on regular fiber stays without response (response comes too late)
                // Second attempts on speculative fiber fails and only next one completes
                // Regular fiber should stay locked.
                AttemptRecordExpectation::SpeculativeFailure(NodeExpectation::AnyNode(false)),
                AttemptRecordExpectation::SpeculativeFailure(NodeExpectation::UniqueNode(false)),
                AttemptRecordExpectation::RegularFailure(NodeExpectation::UniqueNode(false)),
            ],
            expected_query_result: QueryResultExpectation::Failed,
        },
        TestCase {
            name: "Regular succeeds after speculative fiber fails twice".to_string(),
            request_rules: vec![
                // Will make query fail once with error that is retried on the next node
                // Next retry should be speculative and should fail and to be retried on next node
                // Next attempt (speculative as well) should succeed
                SimpleProxyRules::delay(tick*3),
                SimpleProxyRules::fail(None, error_retry_next),
                SimpleProxyRules::fail(None, error_retry_next),
            ],
            speculative_policy: simple_speculative.clone(),
            retry_policy: Arc::new(TestRetryPolicy::new(3)),
            expectation: vec![
                // First attempt on regular fiber stays without response (response comes too late)
                // Second attempts on speculative fiber fails and only next one completes
                // Regular fiber should stay locked.
                AttemptRecordExpectation::SpeculativeFailure(NodeExpectation::AnyNode(false)),
                AttemptRecordExpectation::SpeculativeFailure(NodeExpectation::UniqueNode(false)),
                AttemptRecordExpectation::RegularSuccess(NodeExpectation::UniqueNode(false)),
            ],
            expected_query_result: QueryResultExpectation::Success,
        },

        TestCase {
            name: "regular fiber exhausting query plan".to_string(),
            request_rules: vec![
                // Will make query fail all the time with error that is retried on the next node
                SimpleProxyRules::fail_all(error_retry_next),
            ],
            speculative_policy: simple_speculative.clone(),
            retry_policy: Arc::new(TestRetryPolicy::new(3)),
            expectation: vec![
                // Since there are three nodes only, it is going retry only 3 times, one time per node
                AttemptRecordExpectation::RegularFailure(NodeExpectation::AnyNode(false)),
                AttemptRecordExpectation::RegularFailure(NodeExpectation::UniqueNode(false)),
                AttemptRecordExpectation::RegularFailure(NodeExpectation::UniqueNode(false)),
            ],
            expected_query_result: QueryResultExpectation::Failed,
        },
        TestCase {
            name: "regular failed speculative failed exhausting query plan".to_string(),
            request_rules: vec![
                // Will make query fail all the time with error that is retried on the next node
                SimpleProxyRules::fail_all(error_retry_next),
            ],
            speculative_policy: simple_speculative.clone(),
            retry_policy: Arc::new(TestRetryPolicy::new(1)),
            expectation: vec![
                // Since there are three nodes only, it is going retry only 3 times, one time per node
                AttemptRecordExpectation::RegularFailure(NodeExpectation::AnyNode(false)),
                AttemptRecordExpectation::RegularFailure(NodeExpectation::UniqueNode(false)),
                AttemptRecordExpectation::SpeculativeFailure(NodeExpectation::UniqueNode(false)),
            ],
            expected_query_result: QueryResultExpectation::Failed,
        },
        TestCase {
            name: "Speculative max_retry_count(1) - block one fiber, let last speculative to complete".to_string(),
            request_rules: vec![
                // Blocks first query, next one is going to be delayed to give driver a chance to start new fiber
                SimpleProxyRules::delay(never),
                SimpleProxyRules::delay(tick*3),
            ],
            speculative_policy: Some(Arc::new(SimpleSpeculativeExecutionPolicy {
                max_retry_count: 1,
                retry_interval: tick * 2,
            })),
            retry_policy: Arc::new(TestRetryPolicy::new(3)),
            expectation: vec![
                // Since max_retry_count=1, it is going retry only 1 time,
                //  even though there are more nodes in the cluster, one time per node
                AttemptRecordExpectation::RegularNoResponse(NodeExpectation::AnyNode(false)),
                AttemptRecordExpectation::SpeculativeSuccess(NodeExpectation::UniqueNode(false)),
            ],
            expected_query_result: QueryResultExpectation::Success,
        },
        TestCase {
            name: "Speculative max_retry_count(2) - block two fibers, let last one to complete".to_string(),
            request_rules: vec![
                // Blocks first two queries, next one is going to be delayed to give driver a chance to start new fiber
                SimpleProxyRules::delay(never),
                SimpleProxyRules::delay(never),
                SimpleProxyRules::delay(tick*3),
            ],
            speculative_policy: Some(Arc::new(SimpleSpeculativeExecutionPolicy {
                max_retry_count: 2,
                retry_interval: tick * 2,
            })),
            retry_policy: Arc::new(TestRetryPolicy::new(3)),
            expectation: vec![
                // Since there are three nodes only, it is going retry only 2 times, one attempt per node
                AttemptRecordExpectation::RegularNoResponse(NodeExpectation::AnyNode(false)),
                AttemptRecordExpectation::SpeculativeNoResponse(NodeExpectation::UniqueNode(false)),
                AttemptRecordExpectation::SpeculativeSuccess(NodeExpectation::UniqueNode(false)),
            ],
            expected_query_result: QueryResultExpectation::Success,
        },
        TestCase {
            name: "Speculative max_retry_count(3) - block two fibers, let last one to complete".to_string(),
            request_rules: vec![
                // Blocks first two queries, next one is going to be delayed to give driver a chance to start new fiber
                // It should not start them since there is only three nodes in the cluster
                SimpleProxyRules::delay(never),
                SimpleProxyRules::delay(never),
                SimpleProxyRules::delay(tick*5),
            ],
            speculative_policy: simple_speculative.clone(),
            retry_policy: Arc::new(TestRetryPolicy::new(3)),
            expectation: vec![
                // Since there are three nodes only, it is going retry only 2 times, one time per node
                AttemptRecordExpectation::RegularNoResponse(NodeExpectation::AnyNode(false)),
                AttemptRecordExpectation::SpeculativeNoResponse(NodeExpectation::UniqueNode(false)),
                AttemptRecordExpectation::SpeculativeSuccess(NodeExpectation::UniqueNode(false)),
            ],
            expected_query_result: QueryResultExpectation::Success,
        },
        
        // ============= DONE ===============

        // TODO: speculative fiber starts prematurely, it still should be retried on regular fiber
        // TestCase {
        //     name: "Speculative - error_retry_same, pass".to_string(),
        //     request_rules: vec![
        //         // Will make query fail once with error that is retried on the next node
        //         // And then it is going to pass
        //         SimpleProxyRules::fail(None, error_retry_same),
        //     ],
        //     speculative_policy: simple_speculative.clone(),
        //     retry_policy: Arc::new(TestRetryPolicy::new(3)),
        //     expectation: vec![
        //         // Should retry on next node once and then succeed
        //         AttemptRecordExpectation::RegularFailure(NodeExpectation::AnyNode(false)),
        //         AttemptRecordExpectation::RegularSuccess(NodeExpectation::SameNode(false)),
        //     ],
        //     expected_query_result: QueryResultExpectation::Success,
        // },
        // TestCase {
        //     name: "Speculative - error_retry_same<never>, pass".to_string(),
        //     request_rules: vec![
        //         // Will make first query scheduled to fail too far in future
        //         // Next retry should be speculative and should succeed, not waiting for regular fiber response
        //         SimpleProxyRules::delay(never),
        //     ],
        //     speculative_policy: simple_speculative.clone(),
        //     retry_policy: Arc::new(TestRetryPolicy::new(3)),
        //     expectation: vec![
        //         // First attempt on regular fiber stays without response (response comes too late)
        //         // Second attempts on speculative fiber succeeds
        //         AttemptRecordExpectation::RegularNoResponse(NodeExpectation::AnyNode(false)),
        //         AttemptRecordExpectation::SpeculativeSuccess(NodeExpectation::AnotherNode(false)),
        //     ],
        //     expected_query_result: QueryResultExpectation::Success,
        // },
        // TODO: By some reason speculative fiber start prematurely
        // TestCase {
        //     name: "Speculative - error_retry_same<never>, error_retry_same, pass".to_string(),
        //     request_rules: vec![
        //         // Will make query fail once with error that is retried on the next node
        //         // Next retry should be speculative and should fail and to be retried on next node
        //         // Next attempt (speculative as well) should succeed
        //         SimpleProxyRules::delay(never),
        //         SimpleProxyRules::fail(None, error_retry_same),
        //     ],
        //     speculative_policy: simple_speculative.clone(),
        //     retry_policy: Arc::new(TestRetryPolicy::new(3)),
        //     expectation: vec![
        //         // First attempt on regular fiber stays without response (response comes too late)
        //         // Second attempts on speculative fiber succeeds
        //         AttemptRecordExpectation::RegularNoResponse(NodeExpectation::AnyNode(false)),
        //         AttemptRecordExpectation::SpeculativeFailure(NodeExpectation::AnotherNode(false)),
        //         AttemptRecordExpectation::SpeculativeSuccess(NodeExpectation::SameNode(false)),
        //     ],
        //     expected_query_result: QueryResultExpectation::Success,
        // },

        // TODO: driver waits regular fiber when speculative fiber failed
        // TestCase {
        //     name: "Speculative - error_retry_same<never>, error_retry_same<all>".to_string(),
        //     request_rules: vec![
        //         // Will make query fail once with error that is retried on the next node
        //         // Next retry should be speculative and should fail and to be retried on next node
        //         // Next attempt (speculative as well) should succeed
        //         SimpleProxyRules::delay(never),
        //         SimpleProxyRules::fail_all(error_retry_same),
        //     ],
        //     speculative_policy: simple_speculative.clone(),
        //     retry_policy: Arc::new(TestRetryPolicy::new(3)),
        //     expectation: vec![
        //         // First attempt on regular fiber stays without response (response comes too late)
        //         // Second attempts on speculative fiber fails and only next one completes
        //         // Regular fiber should stay locked.
        //         AttemptRecordExpectation::RegularNoResponse(NodeExpectation::AnyNode(false)),
        //         AttemptRecordExpectation::SpeculativeFailure(NodeExpectation::SameNode(false)),
        //         AttemptRecordExpectation::SpeculativeFailure(NodeExpectation::SameNode(false)),
        //     ],
        //     expected_query_result: QueryResultExpectation::Failed,
        // },

        // TODO: speculative fiber starts prematurely, all requests should fail on regular fiber
        // TestCase {
        //     name: "Speculative - error_retry_same<all>".to_string(),
        //     request_rules: vec![
        //         // Will make query fail all the time with error that is retried on the next node
        //         SimpleProxyRules::fail_all(error_retry_same),
        //     ],
        //     speculative_policy: simple_speculative.clone(),
        //     retry_policy: Arc::new(TestRetryPolicy::new(3)),
        //     expectation: vec![
        //         // Since there are three nodes only, it is going retry only 3 times, one time per node
        //         AttemptRecordExpectation::RegularFailure(NodeExpectation::AnyNode(false)),
        //         AttemptRecordExpectation::RegularFailure(NodeExpectation::SameNode(false)),
        //         AttemptRecordExpectation::RegularFailure(NodeExpectation::SameNode(false)),
        //     ],
        //     expected_query_result: QueryResultExpectation::Failed,
        // },
    ];

    let res = test_with_3_node_cluster(ShardAwareness::QueryNode, |proxy_uris, translation_map, mut running_proxy| async move {
        let translation_map = Arc::new(translation_map);

        // DB preparation phase
        let setup_session: Session = session_builder::SessionBuilder::new()
            .known_node(proxy_uris[0].as_str())
            .address_translator(translation_map.clone())
            .default_execution_profile_handle(ExecutionProfile::builder().retry_policy(Arc::new(FallthroughRetryPolicy)).build().into_handle())
            .build()
            .await
            .unwrap();

        setup_session.ddl(
            format!("CREATE KEYSPACE IF NOT EXISTS {} WITH REPLICATION = {{'class' : 'NetworkTopologyStrategy', 'replication_factor' : 3}}", ks)
        ).await.expect("Failed to create keyspace");
        setup_session.use_keyspace(ks.clone(), false).await.unwrap();
        setup_session
            .ddl("CREATE TABLE speculative_retry_tests (a int primary key)")
            .await
            .expect("Unable to create table");

        // Test execution phase
        let mut errors: Vec<String> = Vec::new();
        for test_case in test_cases {
            let case_name = test_case.name;
            // Setup proxy request rules
            if test_case.request_rules.is_empty() {
                running_proxy.running_nodes.iter_mut().for_each(|node| {
                    node.change_request_rules(None);
                });
            } else {
                let rules = into_proxy_request_rules(test_case.request_rules, target_opcode);
                running_proxy.running_nodes.iter_mut().for_each(|node| {
                    node.change_request_rules(Some(rules.clone()));
                });
            }

            // Init session

            let profile = ExecutionProfile::builder()
                .speculative_execution_policy(test_case.speculative_policy)
                .retry_policy(test_case.retry_policy).build();

            let hs = Arc::new(HistoryCollector::new());
            let session: Session = session_builder::SessionBuilder::new()
                .known_node(proxy_uris[0].as_str())
                .default_execution_profile_handle(profile.into_handle())
                .address_translator(translation_map.clone())
                .build()
                .await
                .unwrap();

            session.use_keyspace(ks.clone(), false).await.unwrap();

            // Init query with history listener to catch query attempts
            let mut q = Query::from("INSERT INTO speculative_retry_tests (a) VALUES (?)");
            q.set_is_idempotent(true); // this is to allow speculative execution to fire
            q.set_history_listener(hs.clone());

            let start = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_millis();

            // Execute query and collect result
            let query_result = session.query_unpaged(q.clone(), (3,)).await;

            match test_case.expected_query_result {
                QueryResultExpectation::Failed => {
                    match query_result {
                        Ok(_) => {
                            errors.push(format!("{:30} -FAILED-> expected query to fail, but it succeeded", case_name));
                        }
                        Err(err) => {
                            println!("{:30} -> request failed as expected: {:?}", case_name, err);
                        }
                    }
                }
                QueryResultExpectation::Success => {
                    match query_result {
                        Ok(_) => {
                            println!("{:30} --> request succeeded as expected", case_name);
                        }
                        Err(err) => {
                            errors.push(format!("{:30} -FAILED-> expected query to succeed, but it failed: {:?}", case_name, err));
                        }
                    }
                }
            }

            // Read query history and convert it to simple representation
            let single_query_result = convert_into_simple_history(hs.clone_structured_history());
            let single_query_result = single_query_result[0].clone();

            // Check if test case expectation met
            if let Err(err) = check_expectations(test_case.expectation.clone(), single_query_result.clone()) {
                errors.push(format!("{:30} -FAILED-> recorded history does not match expectations, details:\n\t\thistory :\n\t\t{:?}\n\t\texpectations:\n\t\t{:?}\n\t\tdifferences:\n\t\t{}", case_name, single_query_result, test_case.expectation, err));
            } else {
                println!("{:30} -> request history expectations {:?} matches recorded one {:?}", case_name, test_case.expectation, single_query_result);
            }

            let end = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_millis();
            println!("{:30} -> elapsed {:?}", case_name, end - start);

        }

        if !errors.is_empty() {
            panic!("{}", errors.join("\n"));
        }

        running_proxy
    }).await;

    match res {
        Ok(()) => (),
        Err(ProxyError::Worker(WorkerError::DriverDisconnected(_))) => (),
        Err(err) => panic!("{}", err),
    }
}
