use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;

use chrono::{DateTime, NaiveDate, NaiveDateTime, NaiveTime, Utc};
use futures::{StreamExt, TryStreamExt};
use scylla::client::execution_profile::ExecutionProfile;
use scylla::client::session::Session;
use scylla::client::session_builder::SessionBuilder;
use scylla::errors::{RequestAttemptError, RequestError};
use scylla::observability::history::{
    AttemptResult, HistoryCollector, RequestHistoryResult, StructuredHistory, TimePoint,
};
use scylla::policies::retry::{RequestInfo, RetryDecision, RetryPolicy, RetrySession};
use scylla::statement::prepared::PreparedStatement;
use scylla::statement::unprepared::Statement;
use scylla::value::Row;
use scylla_cql::Consistency;
use scylla_proxy::{
    Condition, ProxyError, Reaction as _, RequestOpcode, RequestReaction, RequestRule, WorkerError,
};

use crate::utils::{
    PerformDDL, create_new_session_builder, setup_tracing, test_with_3_node_cluster,
    unique_keyspace_name,
};

// Set a single time for all timestamps within StructuredHistory.
// HistoryCollector sets the timestamp to current time which changes with each test.
// Setting it to one makes it possible to test displaying consistently.
fn set_one_time(mut history: StructuredHistory) -> StructuredHistory {
    let the_time: TimePoint = DateTime::<Utc>::from_naive_utc_and_offset(
        NaiveDateTime::new(
            NaiveDate::from_ymd_opt(2022, 2, 22).unwrap(),
            NaiveTime::from_hms_opt(20, 22, 22).unwrap(),
        ),
        Utc,
    );

    for query in &mut history.requests {
        query.start_time = the_time;
        match &mut query.result {
            Some(RequestHistoryResult::Success(succ_time)) => *succ_time = the_time,
            Some(RequestHistoryResult::Error(err_time, _)) => *err_time = the_time,
            None => {}
        };

        for fiber in std::iter::once(&mut query.non_speculative_fiber)
            .chain(query.speculative_fibers.iter_mut())
        {
            fiber.start_time = the_time;
            for attempt in &mut fiber.attempts {
                attempt.send_time = the_time;
                match &mut attempt.result {
                    Some(AttemptResult::Success(succ_time)) => *succ_time = the_time,
                    Some(AttemptResult::Error(err_time, _, _)) => *err_time = the_time,
                    None => {}
                }
            }
        }
    }

    history
}

// Set a single node for all attempts within StructuredHistory.
// When running against real life nodes this address may change,
// setting it to one value makes it possible to run tests consistently.
fn set_one_node(mut history: StructuredHistory) -> StructuredHistory {
    let the_node: SocketAddr = node1_addr();

    for query in &mut history.requests {
        for fiber in std::iter::once(&mut query.non_speculative_fiber)
            .chain(query.speculative_fibers.iter_mut())
        {
            for attempt in &mut fiber.attempts {
                attempt.node_addr = the_node;
            }
        }
    }

    history
}

// Set a single error message for all DbErrors within StructuredHistory.
// The error message changes between Scylla/Cassandra/their versions.
// Setting it to one value makes it possible to run tests consistently.
fn set_one_db_error_message(mut history: StructuredHistory) -> StructuredHistory {
    let set_msg_attempt = |err: &mut RequestAttemptError| {
        if let RequestAttemptError::DbError(_, msg) = err {
            *msg = "Error message from database".to_string();
        }
    };
    let set_msg_request_error = |err: &mut RequestError| {
        if let RequestError::LastAttemptError(RequestAttemptError::DbError(_, msg)) = err {
            *msg = "Error message from database".to_string();
        }
    };

    for request in &mut history.requests {
        if let Some(RequestHistoryResult::Error(_, err)) = &mut request.result {
            set_msg_request_error(err);
        }
        for fiber in std::iter::once(&mut request.non_speculative_fiber)
            .chain(request.speculative_fibers.iter_mut())
        {
            for attempt in &mut fiber.attempts {
                if let Some(AttemptResult::Error(_, err, _)) = &mut attempt.result {
                    set_msg_attempt(err);
                }
            }
        }
    }

    history
}

fn node1_addr() -> SocketAddr {
    SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 19042)
}

#[tokio::test]
async fn successful_query_history() {
    setup_tracing();
    let session = create_new_session_builder().build().await.unwrap();

    let mut query = Statement::new("SELECT * FROM system.local WHERE key='local'");
    let history_collector = Arc::new(HistoryCollector::new());
    query.set_history_listener(history_collector.clone());

    session.query_unpaged(query.clone(), ()).await.unwrap();

    let history: StructuredHistory = history_collector.clone_structured_history();

    let displayed = "Requests History:
=== Request #0 ===
| start_time: 2022-02-22 20:22:22 UTC
| Non-speculative attempts:
| - Attempt #0 sent to 127.0.0.1:19042
|   request send time: 2022-02-22 20:22:22 UTC
|   Success at 2022-02-22 20:22:22 UTC
|
| Request successful at 2022-02-22 20:22:22 UTC
=================
";
    assert_eq!(
        displayed,
        format!(
            "{}",
            set_one_db_error_message(set_one_node(set_one_time(history)))
        )
    );

    // Prepared queries retain the history listener set in Query.
    let prepared = session.prepare(query).await.unwrap();
    session.execute_unpaged(&prepared, ()).await.unwrap();

    let history2: StructuredHistory = history_collector.clone_structured_history();

    let displayed2 = "Requests History:
=== Request #0 ===
| start_time: 2022-02-22 20:22:22 UTC
| Non-speculative attempts:
| - Attempt #0 sent to 127.0.0.1:19042
|   request send time: 2022-02-22 20:22:22 UTC
|   Success at 2022-02-22 20:22:22 UTC
|
| Request successful at 2022-02-22 20:22:22 UTC
=================
=== Request #1 ===
| start_time: 2022-02-22 20:22:22 UTC
| Non-speculative attempts:
| - Attempt #0 sent to 127.0.0.1:19042
|   request send time: 2022-02-22 20:22:22 UTC
|   Success at 2022-02-22 20:22:22 UTC
|
| Request successful at 2022-02-22 20:22:22 UTC
=================
";
    assert_eq!(
        displayed2,
        format!(
            "{}",
            set_one_db_error_message(set_one_node(set_one_time(history2)))
        )
    );
}

#[tokio::test]
async fn failed_query_history() {
    setup_tracing();
    let session = create_new_session_builder().build().await.unwrap();

    let mut query = Statement::new("This isnt even CQL");
    let history_collector = Arc::new(HistoryCollector::new());
    query.set_history_listener(history_collector.clone());

    assert!(session.query_unpaged(query.clone(), ()).await.is_err());

    let history: StructuredHistory = history_collector.clone_structured_history();

    let displayed =
"Requests History:
=== Request #0 ===
| start_time: 2022-02-22 20:22:22 UTC
| Non-speculative attempts:
| - Attempt #0 sent to 127.0.0.1:19042
|   request send time: 2022-02-22 20:22:22 UTC
|   Error at 2022-02-22 20:22:22 UTC
|   Error: Database returned an error: The submitted query has a syntax error, Error message: Error message from database
|   Retry decision: DontRetry
|
| Request failed at 2022-02-22 20:22:22 UTC
| Error: Database returned an error: The submitted query has a syntax error, Error message: Error message from database
=================
";
    assert_eq!(
        displayed,
        format!(
            "{}",
            set_one_db_error_message(set_one_node(set_one_time(history)))
        )
    );
}

#[tokio::test]
async fn pager_query_history() {
    setup_tracing();
    let session = create_new_session_builder().build().await.unwrap();
    let ks = unique_keyspace_name();
    session
    .ddl(format!("CREATE KEYSPACE {ks} WITH REPLICATION = {{'class' : 'NetworkTopologyStrategy', 'replication_factor' : 1}}"))
    .await
    .unwrap();
    session.use_keyspace(&ks, true).await.unwrap();

    session
        .ddl("CREATE TABLE t (p int primary key)")
        .await
        .unwrap();
    for i in 0..32 {
        session
            .query_unpaged("INSERT INTO t (p) VALUES (?)", (i,))
            .await
            .unwrap();
    }

    let mut paged_query: Statement = Statement::new("SELECT * FROM t");
    paged_query.set_page_size(8);
    let history_collector = Arc::new(HistoryCollector::new());
    paged_query.set_history_listener(history_collector.clone());

    let mut rows_stream = session
        .query_iter(paged_query, ())
        .await
        .unwrap()
        .rows_stream::<Row>()
        .unwrap();
    while let Some(_row) = rows_stream.next().await {
        // Receive rows...
    }

    let history = history_collector.clone_structured_history();

    assert!(history.requests.len() >= 4);

    let displayed_prefix = "Requests History:
=== Request #0 ===
| start_time: 2022-02-22 20:22:22 UTC
| Non-speculative attempts:
| - Attempt #0 sent to 127.0.0.1:19042
|   request send time: 2022-02-22 20:22:22 UTC
|   Success at 2022-02-22 20:22:22 UTC
|
| Request successful at 2022-02-22 20:22:22 UTC
=================
=== Request #1 ===
| start_time: 2022-02-22 20:22:22 UTC
| Non-speculative attempts:
| - Attempt #0 sent to 127.0.0.1:19042
|   request send time: 2022-02-22 20:22:22 UTC
|   Success at 2022-02-22 20:22:22 UTC
|
| Request successful at 2022-02-22 20:22:22 UTC
=================
=== Request #2 ===
| start_time: 2022-02-22 20:22:22 UTC
| Non-speculative attempts:
| - Attempt #0 sent to 127.0.0.1:19042
|   request send time: 2022-02-22 20:22:22 UTC
|   Success at 2022-02-22 20:22:22 UTC
|
| Request successful at 2022-02-22 20:22:22 UTC
=================
=== Request #3 ===
| start_time: 2022-02-22 20:22:22 UTC
| Non-speculative attempts:
| - Attempt #0 sent to 127.0.0.1:19042
|   request send time: 2022-02-22 20:22:22 UTC
|   Success at 2022-02-22 20:22:22 UTC
|
| Request successful at 2022-02-22 20:22:22 UTC
=================
";
    let displayed_str = format!(
        "{}",
        set_one_db_error_message(set_one_node(set_one_time(history)))
    );

    assert!(
        displayed_str.starts_with(displayed_prefix),
        "Expected history prefix:\n{displayed_prefix}\n\ngot history:\n {displayed_str}"
    );

    session.ddl(format!("DROP KEYSPACE {ks}")).await.unwrap();
}

/// Regression test: `query_iter` and `execute_iter` must not create an orphaned
/// history request when the result fits in a single page.
///
/// The bug: `PagerWorker::process_first_page` called `log_request_start()`
/// unconditionally after a successful first-page fetch, even when the server
/// returned `NoMorePages` and no background worker would be spawned.  This
/// created a new history request entry that was never closed with a success or
/// error event (`result = None`).
///
/// The fix: only call `log_request_start()` when `HasMorePages` is set,
/// i.e. only when a background worker will actually be spawned to continue
/// fetching.
///
/// Two cases are tested:
/// 1. `query_iter` on an unprepared `Statement`.
/// 2. `execute_iter` on a `PreparedStatement`.
///
/// Both use `SELECT * FROM system.local WHERE key = 'local'`, which returns
/// exactly one row and always fits in a single page.
#[tokio::test]
async fn pager_single_page_history() {
    setup_tracing();
    let session = create_new_session_builder().build().await.unwrap();

    let history_collector = Arc::new(HistoryCollector::new());

    // The expected history for a single successful paged request: exactly one
    // request and one attempt, both successful, with no orphaned extra request.
    let expected = "Requests History:
=== Request #0 ===
| start_time: 2022-02-22 20:22:22 UTC
| Non-speculative attempts:
| - Attempt #0 sent to 127.0.0.1:19042
|   request send time: 2022-02-22 20:22:22 UTC
|   Success at 2022-02-22 20:22:22 UTC
|
| Request successful at 2022-02-22 20:22:22 UTC
=================
";

    // Case 1: query_iter on an unprepared Statement.
    {
        let mut stmt = Statement::new("SELECT * FROM system.local WHERE key = 'local'");
        stmt.set_history_listener(history_collector.clone());

        let mut stream = session
            .query_iter(stmt, ())
            .await
            .unwrap()
            .rows_stream::<Row>()
            .unwrap();
        while stream.next().await.transpose().unwrap().is_some() {}

        let history = history_collector.take_structured_history();
        assert_eq!(
            expected,
            format!(
                "{}",
                set_one_db_error_message(set_one_node(set_one_time(history)))
            ),
            "query_iter: unexpected history (orphaned request?)"
        );
    }

    // Case 2: execute_iter on a PreparedStatement.
    // take_structured_history() above cleared the collector, so request
    // numbering starts again from #0.
    {
        let mut prepared: PreparedStatement = session
            .prepare("SELECT * FROM system.local WHERE key = 'local'")
            .await
            .unwrap();
        prepared.set_history_listener(history_collector.clone());

        let mut stream = session
            .execute_iter(prepared, ())
            .await
            .unwrap()
            .rows_stream::<Row>()
            .unwrap();
        while stream.next().await.transpose().unwrap().is_some() {}

        let history = history_collector.take_structured_history();
        assert_eq!(
            expected,
            format!(
                "{}",
                set_one_db_error_message(set_one_node(set_one_time(history)))
            ),
            "execute_iter: unexpected history (orphaned request?)"
        );
    }
}

/// Regression test: `query_iter` / `execute_iter` must correctly finalize the
/// history request when the retry policy returns `IgnoreWriteError`.
///
/// The bug: `PagerWorker::work()` handled `RetryDecision::IgnoreWriteError`
/// by immediately returning an empty page without calling
/// `log_request_success()` or `retry_session.reset()` first. This left the
/// history request opened by `log_request_start()` with `result = None`
/// ("still running") rather than closing it as successful.
///
/// The fix: call `log_request_success()` and `retry_session.reset()` before
/// returning the empty page.
///
/// Two cases are tested:
///
/// 1. `IgnoreWriteError` on the **first page**: the pager never successfully
///    fetches any page; the policy fires on the very first attempt.
///    Uses a syntax-error query so no DDL is needed.
/// 2. `IgnoreWriteError` on the **second page**: page 1 is fetched
///    successfully; the policy fires on the first attempt of page 2.
///    Uses a proxy to forge the second-page error while letting page 1
///    through.
#[tokio::test]
async fn pager_ignore_write_error_history() {
    setup_tracing();

    #[derive(Debug)]
    struct IgnoreWriteErrorPolicy;

    impl RetryPolicy for IgnoreWriteErrorPolicy {
        fn new_session(&self) -> Box<dyn RetrySession> {
            Box::new(IgnoreWriteErrorSession)
        }
    }

    struct IgnoreWriteErrorSession;

    impl RetrySession for IgnoreWriteErrorSession {
        fn decide_should_retry(&mut self, _: RequestInfo) -> RetryDecision {
            RetryDecision::IgnoreWriteError
        }
        fn reset(&mut self) {}
    }

    let history_collector = Arc::new(HistoryCollector::new());

    // ---- Case 1: IgnoreWriteError on the first page ----
    //
    // Use an intentional syntax error so the query fails immediately without
    // touching any table (no DDL required).
    {
        let handle = ExecutionProfile::builder()
            .consistency(Consistency::One)
            .retry_policy(Arc::new(IgnoreWriteErrorPolicy))
            .build()
            .into_handle();

        let session = create_new_session_builder()
            .default_execution_profile_handle(handle)
            .build()
            .await
            .unwrap();

        let mut stmt = Statement::new("INSERT INTO t (pk v) VALUES (1, 2)");
        stmt.set_history_listener(history_collector.clone());

        let mut stream = session
            .query_iter(stmt, ())
            .await
            .unwrap()
            .rows_stream::<Row>()
            .unwrap();
        while stream.try_next().await.unwrap().is_some() {}

        let history = history_collector.take_structured_history();

        // One attempt (error) and one request (successful despite the attempt
        // error, because IgnoreWriteError = "treat as done").
        // Before the fix the request showed "still running".
        let expected = "Requests History:
=== Request #0 ===
| start_time: 2022-02-22 20:22:22 UTC
| Non-speculative attempts:
| - Attempt #0 sent to 127.0.0.1:19042
|   request send time: 2022-02-22 20:22:22 UTC
|   Error at 2022-02-22 20:22:22 UTC
|   Error: Database returned an error: The submitted query has a syntax error, Error message: Error message from database
|   Retry decision: IgnoreWriteError
|
| Request successful at 2022-02-22 20:22:22 UTC
=================
";
        assert_eq!(
            expected,
            format!(
                "{}",
                set_one_db_error_message(set_one_node(set_one_time(history)))
            ),
            "case 1 (first page): unexpected history"
        );
    }

    // ---- Case 2: IgnoreWriteError on the second page ----
    //
    // Use a proxy to pass page 1 through but forge a server error on page 2,
    // triggering IgnoreWriteError in the background worker (PagerWorker::work).
    // system_schema.keyspaces has ≥4 rows on both backends; with page_size=1
    // page 1 always returns HasMorePages (1 row delivered, ≥3 remaining), so
    // work() is always spawned and issues a second Execute — deterministically
    // on both Scylla and Cassandra.
    let history_collector_ref = Arc::clone(&history_collector);
    let execute_not_control = Condition::RequestOpcode(RequestOpcode::Execute)
        .and(Condition::not(Condition::ConnectionRegisteredAnyEvent));

    let res = test_with_3_node_cluster(
        scylla_proxy::ShardAwareness::QueryNode,
        |proxy_uris, translation_map, mut running_proxy| async move {
            let profile = ExecutionProfile::builder()
                .retry_policy(Arc::new(IgnoreWriteErrorPolicy))
                .build()
                .into_handle();

            let session: Session = SessionBuilder::new()
                .known_node(proxy_uris[0].as_str())
                .address_translator(Arc::new(translation_map))
                .default_execution_profile_handle(profile)
                .build()
                .await
                .unwrap();

            // system_schema.keyspaces has ≥4 rows on both backends (system,
            // system_schema, system_auth, system_traces, …). With page_size=1
            // page 1 always returns HasMorePages (1 row delivered, ≥3
            // remaining), so work() is always spawned and issues a second
            // Execute — which the proxy forges as an error.
            let mut prepared = session
                .prepare("SELECT keyspace_name FROM system_schema.keyspaces")
                .await
                .unwrap();
            prepared.set_page_size(1);
            prepared.set_history_listener(history_collector.clone());

            // Let the first Execute through; forge a server error on all
            // subsequent ones so the second-page fetch fails.
            running_proxy.running_nodes.iter_mut().for_each(|node| {
                node.change_request_rules(Some(vec![
                    RequestRule(
                        execute_not_control
                            .clone()
                            .and(Condition::TrueForLimitedTimes(1)),
                        RequestReaction::noop(),
                    ),
                    RequestRule(
                        execute_not_control.clone(),
                        RequestReaction::forge().server_error(),
                    ),
                ]));
            });

            let pager = session.execute_iter(prepared, ()).await.unwrap();
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

    let history = history_collector_ref.take_structured_history();

    // The exact number of successfully-fetched pages before the forged error
    // depends on the backend and nondeterministic paging-token behaviour, so
    // we don't assert the full history string here.  What we do assert:
    //
    // 1. There is at least one request (sanity).
    // 2. Every request except the last one was fetched successfully (one
    //    successful attempt, request successful).
    // 3. The last request is the one where IgnoreWriteError fired: its single
    //    attempt ended with a server error + IgnoreWriteError decision, and
    //    the request itself is marked successful (not "still running").
    //    Before the fix, the last request had result = None.
    assert!(
        !history.requests.is_empty(),
        "case 2: expected at least one history request"
    );

    for (i, req) in history.requests[..history.requests.len() - 1]
        .iter()
        .enumerate()
    {
        assert!(
            matches!(req.result, Some(RequestHistoryResult::Success(_))),
            "case 2: request #{i} (successful page) must have result=Success, got {:?}",
            req.result,
        );
        assert_eq!(
            req.non_speculative_fiber.attempts.len(),
            1,
            "case 2: request #{i} must have exactly 1 attempt"
        );
        assert!(
            matches!(
                req.non_speculative_fiber.attempts[0].result,
                Some(AttemptResult::Success(_))
            ),
            "case 2: request #{i} attempt must be successful"
        );
    }

    let last = history.requests.last().unwrap();
    let last_i = history.requests.len() - 1;
    assert!(
        matches!(last.result, Some(RequestHistoryResult::Success(_))),
        "case 2: request #{last_i} (IgnoreWriteError page) must have result=Success \
         (not still-running), got {:?}",
        last.result,
    );
    assert_eq!(
        last.non_speculative_fiber.attempts.len(),
        1,
        "case 2: request #{last_i} must have exactly 1 attempt"
    );
    assert!(
        matches!(
            last.non_speculative_fiber.attempts[0].result,
            Some(AttemptResult::Error(_, _, RetryDecision::IgnoreWriteError))
        ),
        "case 2: request #{last_i} attempt must have IgnoreWriteError decision, got {:?}",
        last.non_speculative_fiber.attempts[0].result,
    );
}
