use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};
use tokio::sync::mpsc;

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

/// Basic pager functionality test
#[tokio::test]
async fn test_pager_basic() {
    setup_tracing();
    let session = create_new_session_builder().build().await.unwrap();
    let ks = unique_keyspace_name();

    session
        .ddl(format!(
            "CREATE KEYSPACE IF NOT EXISTS {ks} WITH \
             REPLICATION = {{'class' : 'NetworkTopologyStrategy', 'replication_factor' : 1}}"
        ))
        .await
        .unwrap();
    session
        .ddl(format!(
            "CREATE TABLE {ks}.t (pk int, ck int, PRIMARY KEY (pk, ck))"
        ))
        .await
        .unwrap();

    test_pager_single_page(&ks, &session).await;
    test_pager_multi_page(&ks, &session).await;

    session.ddl(format!("DROP KEYSPACE {ks}")).await.unwrap();
}

/// Verifies that a single-page result (all rows fit in one page) is returned
/// correctly.
async fn test_pager_single_page(ks: &str, session: &Session) {
    for ck in 0..3 {
        session
            .query_unpaged(format!("INSERT INTO {ks}.t (pk, ck) VALUES (1, ?)"), (ck,))
            .await
            .unwrap();
    }

    let mut prepared = session
        .prepare(format!("SELECT ck FROM {ks}.t WHERE pk = ?"))
        .await
        .unwrap();
    // page_size larger than row count — everything fits in one page.
    prepared.set_page_size(10);

    let mut row_stream = session
        .execute_iter(prepared, (1,))
        .await
        .unwrap()
        .rows_stream::<(i32,)>()
        .unwrap();

    let mut cks = Vec::new();
    while let Some(row) = row_stream.next().await {
        let (ck,) = row.unwrap();
        cks.push(ck);
    }

    assert_eq!(
        cks,
        vec![0, 1, 2],
        "Expected all 3 rows in clustering order"
    );
}

/// Verifies that multi-page paging returns all rows exactly once, in the
/// correct order. Each row is fetched on a separate page (page_size=1).
///
/// This is also a regression test for a bug where I did not update the paging
/// state after fetching the first page. The consequence was that the worker re-fetched
/// the first page, causing the first row to appear twice and the total row count to be N+1
/// instead of N.
async fn test_pager_multi_page(ks: &str, session: &Session) {
    let n = 10;
    for ck in 0..n {
        session
            .query_unpaged(format!("INSERT INTO {ks}.t (pk, ck) VALUES (1, ?)"), (ck,))
            .await
            .unwrap();
    }

    let mut prepared = session
        .prepare(format!("SELECT ck FROM {ks}.t WHERE pk = ?"))
        .await
        .unwrap();
    // page_size=1 forces one row per page: 1 fetched eagerly + 9 by the worker.
    prepared.set_page_size(1);

    let mut row_stream = session
        .execute_iter(prepared, (1,))
        .await
        .unwrap()
        .rows_stream::<(i32,)>()
        .unwrap();

    let mut cks = Vec::new();
    while let Some(row) = row_stream.next().await {
        let (ck,) = row.unwrap();
        cks.push(ck);
    }

    let expected: Vec<i32> = (0..n).collect();
    assert_eq!(
        cks, expected,
        "Expected exactly {n} rows in clustering order with no duplicates"
    );
}

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
        "CREATE KEYSPACE {} WITH
            REPLICATION = {{'class': 'NetworkTopologyStrategy', 'replication_factor': {}}}",
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

    session
        .ddl(format!(
            "CREATE KEYSPACE IF NOT EXISTS {ks} WITH
        REPLICATION = {{'class' : 'NetworkTopologyStrategy', 'replication_factor' : 1}}"
        ))
        .await
        .unwrap();
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

    session
        .ddl(format!(
            "CREATE KEYSPACE IF NOT EXISTS {ks} WITH
        REPLICATION = {{'class' : 'NetworkTopologyStrategy', 'replication_factor' : 1}}"
        ))
        .await
        .unwrap();
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
#[ignore = "This test required coordinator stickiness in the pager,
    which is temporarily not implemented in the middle of the undergoing refactor.
    The next commit will reintroduce it and reenable this test."]
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
                "CREATE KEYSPACE IF NOT EXISTS {ks} WITH
                    REPLICATION = {{'class' : 'NetworkTopologyStrategy', 'replication_factor' : 1}}"
            ))
                .await
                .unwrap();

            session
                .ddl(format!(
                    "CREATE TABLE IF NOT EXISTS {ks}.t (a int PRIMARY KEY)"
                ))
                .await
                .unwrap();

            for i in 0..5 {
                session
                    .query_unpaged(format!("INSERT INTO {ks}.t (a) VALUES (?)"), (i,))
                    .await
                    .unwrap();
            }

            let mut prepared = session
                .prepare(format!("SELECT a FROM {ks}.t"))
                .await
                .unwrap();
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
                    node.change_request_rules(Some(vec![RequestRule(
                        Condition::RequestOpcode(RequestOpcode::Execute)
                            .and(Condition::not(Condition::ConnectionRegisteredAnyEvent)),
                        RequestReaction::delay(timeout + Duration::from_millis(10)),
                    )]));
                });

                let pager_err = session
                    .execute_iter(prepared.clone(), ())
                    .await
                    .unwrap_err();
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
                let timeout = Duration::from_millis(200);
                prepared.set_request_timeout(Some(timeout));

                running_proxy.running_nodes.iter_mut().for_each(|node| {
                    node.change_request_rules(Some(vec![
                        // Pass one frame, then delay all subsequent ones.
                        RequestRule(
                            Condition::RequestOpcode(RequestOpcode::Execute)
                                .and(Condition::not(Condition::ConnectionRegisteredAnyEvent))
                                .and(Condition::TrueForLimitedTimes(1)),
                            RequestReaction::noop(),
                        ),
                        RequestRule(
                            Condition::RequestOpcode(RequestOpcode::Execute)
                                .and(Condition::not(Condition::ConnectionRegisteredAnyEvent)),
                            RequestReaction::delay(timeout + Duration::from_millis(100)),
                        ),
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
                    let profile = ExecutionProfile::builder()
                        .request_timeout(Some(timeout))
                        .build();
                    let handle = profile.into_handle();
                    prepared.set_execution_profile_handle(Some(handle));
                    prepared.set_request_timeout(None);
                }

                running_proxy.running_nodes.iter_mut().for_each(|node| {
                    node.change_request_rules(Some(vec![RequestRule(
                        Condition::RequestOpcode(RequestOpcode::Execute)
                            .and(Condition::not(Condition::ConnectionRegisteredAnyEvent)),
                        RequestReaction::forge_with_error_lazy_delay(
                            Box::new(example_db_errors::overloaded),
                            Some(per_retry_delay),
                        ),
                    )]));
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
            running_proxy.turn_off_rules();
            session.ddl(format!("DROP KEYSPACE {ks}")).await.unwrap();

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

/// Regression test for the bug where `PagerWorker` passed the original
/// `query_consistency` instead of `current_consistency` to `RequestInfo`
/// in the retry loop.
///
/// Two cases are tested here. In each case a custom retry policy:
/// 1. On the first failure: returns `RetrySameTarget(Some(Consistency::Two))`
///    (simulating a downgrade from `All`).
/// 2. On the second failure: sends the observed `RequestInfo::consistency`
///    to a channel and returns `DontRetry`.
///
/// Before the fix, `current_consistency` was not passed to `RequestInfo`;
/// the original `query_consistency` (`All`) was used instead. After the
/// fix the downgraded value (`Two`) is correctly forwarded.
#[tokio::test]
async fn test_pager_retry_receives_current_consistency() {
    setup_tracing();

    /// On the first call: downgrade CL from `All` to `Two`.
    /// On the second call: send the observed consistency and stop.
    /// Resets `call_count` between pages (via `reset()`).
    #[derive(Debug)]
    struct CapturingRetryPolicy(mpsc::UnboundedSender<Consistency>);

    impl RetryPolicy for CapturingRetryPolicy {
        fn new_session(&self) -> Box<dyn RetrySession> {
            Box::new(CapturingRetrySession {
                tx: self.0.clone(),
                call_count: 0,
            })
        }
    }

    struct CapturingRetrySession {
        tx: mpsc::UnboundedSender<Consistency>,
        call_count: u32,
    }

    impl RetrySession for CapturingRetrySession {
        fn decide_should_retry(&mut self, request_info: RequestInfo) -> RetryDecision {
            self.call_count += 1;
            match self.call_count {
                // First failure: simulate a consistency downgrade (All → Two).
                1 => RetryDecision::RetrySameTarget(Some(Consistency::Two)),
                // Second failure: record what consistency was reported.
                _ => {
                    let _ = self.tx.send(request_info.consistency);
                    RetryDecision::DontRetry
                }
            }
        }

        fn reset(&mut self) {
            self.call_count = 0;
        }
    }

    let execute_not_control = Condition::RequestOpcode(RequestOpcode::Execute)
        .and(Condition::not(Condition::ConnectionRegisteredAnyEvent));

    // Case 1: first page fetch.
    // All Execute requests are forged to fail immediately.
    let (tx1, mut rx1) = mpsc::unbounded_channel::<Consistency>();
    let profile1 = ExecutionProfile::builder()
        .consistency(Consistency::All)
        .retry_policy(Arc::new(CapturingRetryPolicy(tx1)))
        .build();

    // Case 2: second and further page fetches.
    // The first Execute is passed through (first page succeeds), all subsequent
    // ones are forged to fail.
    let (tx2, mut rx2) = mpsc::unbounded_channel::<Consistency>();
    let profile2 = ExecutionProfile::builder()
        .consistency(Consistency::All)
        .retry_policy(Arc::new(CapturingRetryPolicy(tx2)))
        .build();

    let res = test_with_3_node_cluster(
        scylla_proxy::ShardAwareness::QueryNode,
        |proxy_uris, translation_map, mut running_proxy| async move {
            let session: Session = SessionBuilder::new()
                .known_node(proxy_uris[0].as_str())
                .address_translator(Arc::new(translation_map))
                .build()
                .await
                .unwrap();

            let mut prepared = session
                .prepare("SELECT peer FROM system.peers")
                .await
                .unwrap();
            prepared.set_is_idempotent(true);

            // --- Case 1: first page ---
            // Forge errors on all Execute requests.
            running_proxy.running_nodes.iter_mut().for_each(|node| {
                node.change_request_rules(Some(vec![RequestRule(
                    execute_not_control.clone(),
                    RequestReaction::forge().server_error(),
                )]));
            });
            prepared.set_execution_profile_handle(Some(profile1.into_handle()));
            let _ = session.execute_iter(prepared.clone(), ()).await;

            // --- Case 2: second page ---
            // system.peers has 2 rows in a 3-node cluster. With page_size=1,
            // the first page always returns HasMorePages on both ScyllaDB and
            // Cassandra (1 row returned, 1 still remaining), so the worker
            // is always spawned and the second Execute is reliably issued.
            //
            // Curiously, ScyllaDB returns HasMorePages on the second page as well
            // (1 row returned, 0 remaining, but HasMorePages=true since the server
            // hasn't yet tried to consume non-existing rows), while Cassandra returns
            // HasMorePages=false (the check `remaining_rows == 0` seems to happen eagerly).
            //
            // Pass the first Execute through (first page succeeds), then forge
            // errors on all subsequent Execute requests.
            prepared.set_page_size(1);
            prepared.set_execution_profile_handle(Some(profile2.into_handle()));
            running_proxy.running_nodes.iter_mut().for_each(|node| {
                node.change_request_rules(Some(vec![
                    // 1st Execute per node: pass through.
                    RequestRule(
                        execute_not_control
                            .clone()
                            .and(Condition::TrueForLimitedTimes(1)),
                        RequestReaction::noop(),
                    ),
                    // All subsequent Executes: forge error.
                    RequestRule(
                        execute_not_control.clone(),
                        RequestReaction::forge().server_error(),
                    ),
                ]));
            });
            let pager = session.execute_iter(prepared, ()).await.unwrap();
            // Drive the stream until it errors (triggering worker spawn).
            let mut stream = pager.rows_stream::<Row>().unwrap();
            while stream.next().await.transpose().is_ok_and(|r| r.is_some()) {}

            running_proxy
        },
    )
    .await;

    match res {
        Ok(()) => (),
        Err(ProxyError::Worker(WorkerError::DriverDisconnected(_))) => (),
        Err(err) => panic!("{}", err),
    }

    // Both retry sessions must have observed Consistency::Two (the downgraded
    // CL), not Consistency::All (the original query CL).
    for (label, rx) in [("first page", &mut rx1), ("second page", &mut rx2)] {
        let observed = rx.recv().await.unwrap_or_else(|| {
            panic!("[{label}] retry policy did not send an observed consistency")
        });
        assert_eq!(
            observed,
            Consistency::Two,
            "[{label}] pager must pass the downgraded consistency (Two) to \
             RequestInfo, not the original query consistency (All)"
        );
    }
}

/// Regression test: `execute_iter` and `query_iter` must not return a spurious
/// `PartitionKeyError` for token-unaware queries.
///
/// The bug was in `new_for_prepared_statement`: it called
/// `extract_partition_key_and_calculate_token` unconditionally, whereas
/// previously (before the "fetch first page without spawning" refactor) that
/// call was only reached inside the spawned worker task, which was guarded by
/// `is_token_aware()`. For non-token-aware statements the call returns
/// `Ok(None)` today, so there is no actual failure — but this test locks in
/// the correct behaviour and will catch any future regression.
///
/// Three cases are exercised:
///
/// 1. Unprepared `Statement` + `query_iter` (token is always `None` here;
///    baseline sanity check).
/// 2. Token-unaware `PreparedStatement` with **no bind markers**
///    (`SELECT peer FROM system.peers` — the PK column `peer` never appears
///    as `?`, so the server returns `pk_indexes = []`).
/// 3. Token-unaware `PreparedStatement` with **bind markers on non-PK
///    columns** (`system_schema.columns WHERE keyspace_name = 'system' AND
///    position >= ?` — the PK `keyspace_name` is a hardcoded literal, so
///    again `pk_indexes = []`).
///
/// All three cases must succeed end-to-end and return at least one row, using
/// `page_size = 1` to force the multi-page `work()` code path.
#[tokio::test]
async fn test_token_unaware_pager() {
    setup_tracing();

    let session = create_new_session_builder().build().await.unwrap();

    // Drain a pager and assert it yields at least one row without error.
    async fn assert_pager_succeeds(
        mut stream: impl futures::Stream<Item = Result<Row, impl std::error::Error>> + Unpin,
        label: &str,
    ) {
        let mut count = 0_usize;
        while let Some(row) = stream.next().await {
            row.unwrap_or_else(|e| panic!("[{label}] unexpected stream error: {e:?}"));
            count += 1;
        }
        assert!(count >= 1, "[{label}] expected at least one row, got none");
    }

    // Case 1: unprepared Statement + query_iter.
    // new_for_query always sets token = None; no partition-key extraction is attempted.
    {
        let label = "case 1: unprepared Statement";
        let mut stmt = Statement::from("SELECT peer FROM system.peers");
        stmt.set_page_size(1);
        let stream = session
            .query_iter(stmt, ())
            .await
            .unwrap_or_else(|e| panic!("[{label}] query_iter failed: {e}"))
            .rows_stream::<Row>()
            .unwrap();
        assert_pager_succeeds(stream, label).await;
    }

    // Case 2: token-unaware PreparedStatement, no bind markers.
    // `SELECT peer FROM system.peers` has no `?` at all, so the server returns
    // pk_indexes = [] and is_token_aware() is false.
    {
        let label = "case 2: prepared, no bind markers";
        let mut prepared = session
            .prepare("SELECT peer FROM system.peers")
            .await
            .unwrap();
        assert!(
            !prepared.is_token_aware(),
            "[{label}] expected non-token-aware statement"
        );
        prepared.set_page_size(1);
        let stream = session
            .execute_iter(prepared, ())
            .await
            .unwrap_or_else(|e| panic!("[{label}] execute_iter failed: {e}"))
            .rows_stream::<Row>()
            .unwrap();
        assert_pager_succeeds(stream, label).await;
    }

    // Case 3: token-unaware PreparedStatement, bind markers on non-PK columns.
    // `keyspace_name = 'system'` is the partition key as a hardcoded literal,
    // so it does not appear in pk_indexes. `position >= ?` is a bind marker on
    // a regular (non-PK) column. The server therefore returns pk_indexes = []
    // and is_token_aware() is false, even though the statement has bind values.
    // The query returns many rows (all columns of all system tables), ensuring
    // the multi-page `work()` code path is exercised.
    {
        let label = "case 3: prepared, non-PK bind markers";
        let mut prepared = session
            .prepare(
                "SELECT column_name FROM system_schema.columns \
                 WHERE keyspace_name = 'system' AND position >= ? \
                 ALLOW FILTERING",
            )
            .await
            .unwrap();
        assert!(
            !prepared.is_token_aware(),
            "[{label}] expected non-token-aware statement"
        );
        prepared.set_page_size(1);
        let stream = session
            .execute_iter(prepared, (0_i32,))
            .await
            .unwrap_or_else(|e| panic!("[{label}] execute_iter failed: {e}"))
            .rows_stream::<Row>()
            .unwrap();
        assert_pager_succeeds(stream, label).await;
    }
}
