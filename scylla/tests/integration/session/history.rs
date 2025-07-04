use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;

use chrono::{DateTime, NaiveDate, NaiveDateTime, NaiveTime, Utc};
use futures::StreamExt;
use scylla::errors::{RequestAttemptError, RequestError};
use scylla::observability::history::{
    AttemptResult, HistoryCollector, RequestHistoryResult, StructuredHistory, TimePoint,
};
use scylla::statement::unprepared::Statement;
use scylla::value::Row;

use crate::utils::{create_new_session_builder, setup_tracing, unique_keyspace_name, PerformDDL};

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
async fn iterator_query_history() {
    setup_tracing();
    let session = create_new_session_builder().build().await.unwrap();
    let ks = unique_keyspace_name();
    session
    .ddl(format!("CREATE KEYSPACE {ks} WITH REPLICATION = {{'class' : 'NetworkTopologyStrategy', 'replication_factor' : 1}}"))
    .await
    .unwrap();
    session.use_keyspace(ks, true).await.unwrap();

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

    let mut iter_query: Statement = Statement::new("SELECT * FROM t");
    iter_query.set_page_size(8);
    let history_collector = Arc::new(HistoryCollector::new());
    iter_query.set_history_listener(history_collector.clone());

    let mut rows_iterator = session
        .query_iter(iter_query, ())
        .await
        .unwrap()
        .rows_stream::<Row>()
        .unwrap();
    while let Some(_row) = rows_iterator.next().await {
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

    assert!(displayed_str.starts_with(displayed_prefix),);
}
