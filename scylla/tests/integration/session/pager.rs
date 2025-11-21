use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use assert_matches::assert_matches;
use futures::{StreamExt as _, TryStreamExt as _};
use scylla::{
    client::execution_profile::ExecutionProfile,
    policies::retry::{RequestInfo, RetryDecision, RetryPolicy, RetrySession},
    statement::Statement,
    value::Row,
};
use scylla::{
    client::{session::Session, session_builder::SessionBuilder},
    errors::{NextPageError, NextRowError, PagerExecutionError, RequestError},
};
use scylla_cql::Consistency;
use scylla_proxy::{
    Condition, ProxyError, Reaction as _, RequestOpcode, RequestReaction, RequestRule, WorkerError,
    example_db_errors,
};
use tracing::info;

use crate::utils::{
    PerformDDL as _, create_new_session_builder, scylla_supports_tablets, setup_tracing,
    test_with_3_node_cluster, unique_keyspace_name,
};

// Reproduces the problem with execute_iter mentioned in #608.
#[tokio::test]
async fn test_iter_works_when_retry_policy_returns_ignore_write_error() {
    setup_tracing();
    // It's difficult to reproduce the issue with a real downgrading consistency policy,
    // as it would require triggering a WriteTimeout. We just need the policy
    // to return IgnoreWriteError, so we will trigger a different error
    // and use a custom retry policy which returns IgnoreWriteError.
    let retried_flag = Arc::new(AtomicBool::new(false));

    #[derive(Debug)]
    struct MyRetryPolicy(Arc<AtomicBool>);
    impl RetryPolicy for MyRetryPolicy {
        fn new_session(&self) -> Box<dyn RetrySession> {
            Box::new(MyRetrySession(self.0.clone()))
        }
    }

    struct MyRetrySession(Arc<AtomicBool>);
    impl RetrySession for MyRetrySession {
        fn decide_should_retry(&mut self, _: RequestInfo) -> RetryDecision {
            self.0.store(true, Ordering::Relaxed);
            RetryDecision::IgnoreWriteError
        }
        fn reset(&mut self) {}
    }

    let handle = ExecutionProfile::builder()
        .consistency(Consistency::All)
        .retry_policy(Arc::new(MyRetryPolicy(retried_flag.clone())))
        .build()
        .into_handle();

    let session = create_new_session_builder()
        .default_execution_profile_handle(handle)
        .build()
        .await
        .unwrap();

    // Create a keyspace with replication factor that is larger than the cluster size
    let cluster_size = session.get_cluster_state().get_nodes_info().len();
    let ks = unique_keyspace_name();
    let mut create_ks = format!(
        "CREATE KEYSPACE {} WITH REPLICATION = {{'class': 'NetworkTopologyStrategy', 'replication_factor': {}}}",
        ks,
        cluster_size + 1
    );
    if scylla_supports_tablets(&session).await {
        create_ks += " and TABLETS = { 'enabled': false}";
    }
    session.ddl(create_ks).await.unwrap();
    session.use_keyspace(&ks, true).await.unwrap();
    session
        .ddl("CREATE TABLE t (pk int PRIMARY KEY, v int)")
        .await
        .unwrap();

    assert!(!retried_flag.load(Ordering::Relaxed));
    // Try to write something to the new table - it should fail and the policy
    // will tell us to ignore the error
    let mut stream = session
        .query_iter("INSERT INTO t (pk v) VALUES (1, 2)", ())
        .await
        .unwrap()
        .rows_stream::<Row>()
        .unwrap();

    assert!(retried_flag.load(Ordering::Relaxed));
    while stream.try_next().await.unwrap().is_some() {}

    retried_flag.store(false, Ordering::Relaxed);
    // Try the same with execute_iter()
    let p = session
        .prepare("INSERT INTO t (pk, v) VALUES (?, ?)")
        .await
        .unwrap();
    let mut iter = session
        .execute_iter(p, (1, 2))
        .await
        .unwrap()
        .rows_stream::<Row>()
        .unwrap()
        .into_stream();

    assert!(retried_flag.load(Ordering::Relaxed));
    while iter.try_next().await.unwrap().is_some() {}

    session.ddl(format!("DROP KEYSPACE {ks}")).await.unwrap();
}

#[tokio::test]
async fn test_iter_methods_with_modification_statements() {
    let session = create_new_session_builder().build().await.unwrap();
    let ks = unique_keyspace_name();

    session.ddl(format!("CREATE KEYSPACE IF NOT EXISTS {ks} WITH REPLICATION = {{'class' : 'NetworkTopologyStrategy', 'replication_factor' : 1}}")).await.unwrap();
    session
        .ddl(format!(
            "CREATE TABLE IF NOT EXISTS {ks}.t (a int, b int, c text, primary key (a, b))"
        ))
        .await
        .unwrap();

    let mut query = Statement::from(format!("INSERT INTO {ks}.t (a, b, c) VALUES (1, 2, 'abc')"));
    query.set_tracing(true);
    let mut rows_stream = session
        .query_iter(query, &[])
        .await
        .unwrap()
        .rows_stream::<Row>()
        .unwrap();
    rows_stream.next().await.ok_or(()).unwrap_err(); // assert empty
    assert!(!rows_stream.tracing_ids().is_empty());

    let prepared_statement = session
        .prepare(format!("INSERT INTO {ks}.t (a, b, c) VALUES (?, ?, ?)"))
        .await
        .unwrap();
    let query_pager = session
        .execute_iter(prepared_statement, (2, 3, "cba"))
        .await
        .unwrap();
    query_pager
        .rows_stream::<()>()
        .unwrap()
        .next()
        .await
        .ok_or(())
        .unwrap_err(); // assert empty

    session.ddl(format!("DROP KEYSPACE {ks}")).await.unwrap();
}

// Regression test for https://github.com/scylladb/scylla-rust-driver/issues/1448
// PR with fix: https://github.com/scylladb/scylla-rust-driver/pull/1449
#[tokio::test]
async fn test_iter_methods_when_altering_table() {
    let session = create_new_session_builder().build().await.unwrap();
    let ks = unique_keyspace_name();

    session.ddl(format!("CREATE KEYSPACE IF NOT EXISTS {ks} WITH REPLICATION = {{'class' : 'NetworkTopologyStrategy', 'replication_factor' : 1}}")).await.unwrap();
    session
        .ddl(format!(
            "CREATE TABLE IF NOT EXISTS {ks}.t (a int, b int, d int, primary key (a, b))"
        ))
        .await
        .unwrap();

    let insert_stmt = session
        .prepare(format!("INSERT INTO {ks}.t (a, b, d) VALUES (?, ?, ?)"))
        .await
        .unwrap();
    // First let's insert some data
    for a in 0..10 {
        for b in 0..10 {
            session
                .execute_unpaged(&insert_stmt, (a, b, 1337))
                .await
                .unwrap();
        }
    }

    let mut select_stmt = session
        .prepare(format!("SELECT * FROM {ks}.t",))
        .await
        .unwrap();
    select_stmt.set_page_size(10);
    select_stmt.set_use_cached_result_metadata(false);
    let pager = session.execute_iter(select_stmt, &[]).await.unwrap();
    let mut stream = pager.rows_stream::<(i32, i32, Option<i32>)>().unwrap();

    // Let's fetch a few pages, but not all.
    for _ in 0..50 {
        let _row = stream.next().await.unwrap().unwrap();
    }

    session
        .query_unpaged(format!("ALTER TABLE {ks}.t ADD c text"), &())
        .await
        .unwrap();

    // With the bug (typecheck only being done for first page), the code panics!
    // At some point, requests will return pages with new schema.
    // It contains new column, and the new schema was not type checked.
    // DeserializeRow::deserialize impl will panic because invariants that should
    // be enforced by type check are violated.
    let err = loop {
        match stream.next().await {
            None => panic!("No error. Expected typecheck error."),
            Some(Ok(_row)) => continue,
            Some(Err(e)) => break e,
        }
    };

    assert_matches!(
        err,
        NextRowError::NextPageError(NextPageError::TypeCheckError(_))
    );

    session.ddl(format!("DROP KEYSPACE {ks}")).await.unwrap();
}

#[tokio::test]
#[cfg_attr(scylla_cloud_tests, ignore)]
async fn test_pager_timeouts() {
    setup_tracing();

    let res = test_with_3_node_cluster(
        scylla_proxy::ShardAwareness::QueryNode,
        |proxy_uris, translation_map, mut running_proxy| async move {
            /* Prepare phase */
            let ks = unique_keyspace_name();

            let session: Session = SessionBuilder::new()
                .known_node(proxy_uris[0].as_str())
                .address_translator(Arc::new(translation_map))
                .build()
                .await
                .unwrap();

            session
            .ddl(format!(
                "CREATE KEYSPACE IF NOT EXISTS {ks} WITH REPLICATION = {{'class' : 'NetworkTopologyStrategy', 'replication_factor' : 1}}"
            ))
            .await
            .unwrap();

            session
                .ddl(format!("CREATE TABLE IF NOT EXISTS {ks}.t (a int PRIMARY KEY)"))
                .await
                .unwrap();

            for i in 0..5 {
                session
                    .query_unpaged(format!("INSERT INTO {ks}.t (a) VALUES (?)"), (i,))
                    .await
                    .unwrap();
            }

            let mut prepared = session.prepare(format!("SELECT a FROM {ks}.t")).await.unwrap();
            // Important to have multiple pages.
            prepared.set_page_size(1);
            // Important for retries to fire.
            prepared.set_is_idempotent(true);

            /* Test phase */

            // Case 1: the first page fetch times out.
            {
                let timeout = Duration::from_millis(10);
                prepared.set_request_timeout(Some(timeout));

                running_proxy.running_nodes.iter_mut().for_each(|node| {
                    node.change_request_rules(Some(vec![
                        RequestRule(
                            Condition::RequestOpcode(RequestOpcode::Execute)
                                .and(Condition::not(Condition::ConnectionRegisteredAnyEvent)),
                            RequestReaction::delay(timeout + Duration::from_millis(10))
                        )
                    ]));
                });

                let pager_err = session.execute_iter(prepared.clone(), ()).await.unwrap_err();
                let PagerExecutionError::NextPageError(NextPageError::RequestFailure(
                    RequestError::RequestTimeout(got_timeout),
                )) = pager_err
                else {
                    panic!("Expected RequestTimeout error, got: {:?}", pager_err);
                };
                assert_eq!(got_timeout, timeout);
                info!("Case 1 passed.");
            }

            // Case 2: the second page fetch times out.
            {
                let timeout = Duration::from_millis(100);
                prepared.set_request_timeout(Some(timeout));

                running_proxy.running_nodes.iter_mut().for_each(|node| {
                    node.change_request_rules(Some(vec![
                        // Pass one frame, then delay all subsequent ones.
                        RequestRule(
                            Condition::RequestOpcode(RequestOpcode::Execute)
                                .and(Condition::not(Condition::ConnectionRegisteredAnyEvent))
                                .and(Condition::TrueForLimitedTimes(1)),
                            RequestReaction::noop()
                        ),
                        RequestRule(
                            Condition::RequestOpcode(RequestOpcode::Execute)
                                .and(Condition::not(Condition::ConnectionRegisteredAnyEvent)),
                            RequestReaction::delay(timeout + Duration::from_millis(10))
                        )
                    ]));
                });

                let mut row_stream = session
                    .execute_iter(prepared.clone(), ())
                    .await
                    .unwrap()
                    .rows_stream::<(i32,)>()
                    .unwrap();

                // Observation that is not critical to the test, but good to note:
                // at this point, at most two pages have been fetched:
                // - the first page, fetched eagerly by execute_iter;
                // - possibly the second page, fetched lazily by rows_stream;
                // - no more pages may have been fetched yet, because the second page would be
                //   stuck on channel.send(), waiting for us to consume the first row.

                // First page (1 row) must have been fetched successfully.
                let (_a,) = row_stream.next().await.unwrap().unwrap();

                // The second page fetch must time out.
                let row_err = row_stream.next().await.unwrap().unwrap_err();
                let NextRowError::NextPageError(NextPageError::RequestFailure(
                    RequestError::RequestTimeout(got_timeout),
                )) = row_err
                else {
                    panic!("Expected RequestTimeout error, got: {:?}", row_err);
                };
                assert_eq!(got_timeout, timeout);
                info!("Case 2 passed.");
            }

            // Case 3: retries' cumulative duration exceed the timeout.
            {
                // Here, each retry will be delayed by 20ms.
                // With a 50ms timeout, this means that after 3 retries (60ms total delay),
                // the timeout will be exceeded.
                let per_retry_delay = Duration::from_millis(20);
                let timeout = Duration::from_millis(50);

                // Set timeout through the execution profile.
                {
                    let profile = ExecutionProfile::builder().request_timeout(Some(timeout)).build();
                    let handle = profile.into_handle();
                    prepared.set_execution_profile_handle(Some(handle));
                    prepared.set_request_timeout(None);
                }

                running_proxy.running_nodes.iter_mut().for_each(|node| {
                    node.change_request_rules(Some(vec![
                        RequestRule(
                            Condition::RequestOpcode(RequestOpcode::Execute)
                                .and(Condition::not(Condition::ConnectionRegisteredAnyEvent)),
                            RequestReaction::forge_with_error_lazy_delay(
                                Box::new(example_db_errors::overloaded),
                                Some(per_retry_delay))
                        )
                    ]));
                });

                let pager_err = session.execute_iter(prepared, ()).await.unwrap_err();
                let PagerExecutionError::NextPageError(NextPageError::RequestFailure(
                    RequestError::RequestTimeout(got_timeout),
                )) = pager_err
                else {
                    panic!("Expected RequestTimeout error, got: {:?}", pager_err);
                };
                assert_eq!(got_timeout, timeout);
                info!("Case 3 passed.");
            }

            /* Teardown */
            session.ddl(format!("DROP KEYSPACE {ks}")).await.unwrap();

            running_proxy
        },
    ).await;

    match res {
        Ok(()) => (),
        Err(ProxyError::Worker(WorkerError::DriverDisconnected(_))) => (),
        Err(err) => panic!("{}", err),
    }
}
