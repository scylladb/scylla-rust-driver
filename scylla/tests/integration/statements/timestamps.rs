use std::{
    collections::HashSet,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
};

use rand::random;
use scylla::{
    client::session_builder::SessionBuilder,
    policies::timestamp_generator::TimestampGenerator,
    statement::{
        Statement,
        batch::{Batch, BatchType},
    },
};

use scylla_proxy::{
    Condition, ProxyError, RequestOpcode, RequestReaction, RequestRule, ShardAwareness, WorkerError,
};

use crate::entry_point::{EntryPoint, PagingMode};
use crate::utils::{
    PerformDDL as _, create_new_session_builder, setup_tracing, test_with_3_node_cluster,
    unique_keyspace_name,
};

#[tokio::test]
async fn test_timestamp() {
    setup_tracing();
    let session = create_new_session_builder().build().await.unwrap();
    let ks = unique_keyspace_name();

    session.ddl(format!("CREATE KEYSPACE IF NOT EXISTS {ks} WITH REPLICATION = {{'class' : 'NetworkTopologyStrategy', 'replication_factor' : 1}}")).await.unwrap();
    session
        .ddl(format!(
            "CREATE TABLE IF NOT EXISTS {ks}.t_timestamp (a text, b text, primary key (a))"
        ))
        .await
        .unwrap();

    session.await_schema_agreement().await.unwrap();

    let query_str = format!("INSERT INTO {ks}.t_timestamp (a, b) VALUES (?, ?)");

    // Each entry point must use the timestamp set on the statement.
    // The test sends the same two timestamps through each entry point.
    // The row must keep the higher timestamp.
    //
    // The test sets the timestamp on the statement that is sent:
    // - the unprepared statement, for the unprepared entry points,
    // - the prepared statement, for the prepared entry points.
    //
    // Thus, the test checks `PreparedStatement::set_timestamp`. It does not
    // check that `prepare` copies the configuration.
    const TIMESTAMPS: [(i64, &str); 2] = [(420, "higher timestamp"), (42, "lower timestamp")];

    // test unprepared statement timestamps

    for (paging_mode, key) in [
        (PagingMode::Unpaged, "regular query"),
        (PagingMode::Iter, "regular query iter"),
        (PagingMode::SinglePage, "regular query single page"),
    ] {
        for (timestamp, value) in TIMESTAMPS {
            let mut stmt = Statement::new(query_str.clone());
            stmt.set_timestamp(Some(timestamp));
            paging_mode
                .send_unprepared(&session, stmt, (key, value))
                .await
                .unwrap();
        }
    }

    // test prepared statement timestamps

    let prepared_statement = session.prepare(query_str.clone()).await.unwrap();

    for (paging_mode, key) in [
        (PagingMode::Unpaged, "prepared query"),
        (PagingMode::Iter, "prepared query iter"),
        (PagingMode::SinglePage, "prepared query single page"),
    ] {
        for (timestamp, value) in TIMESTAMPS {
            let mut stmt = prepared_statement.clone();
            stmt.set_timestamp(Some(timestamp));
            paging_mode
                .send_prepared(&session, &stmt, (key, value))
                .await
                .unwrap();
        }
    }

    // test batch statement timestamps

    let mut batch: Batch = Default::default();
    batch.append_statement(Statement::new(query_str));
    batch.append_statement(prepared_statement);

    batch.set_timestamp(Some(420));
    session
        .batch(
            &batch,
            (
                ("first query in batch", "higher timestamp"),
                ("second query in batch", "higher timestamp"),
            ),
        )
        .await
        .unwrap();

    batch.set_timestamp(Some(42));
    session
        .batch(
            &batch,
            (
                ("first query in batch", "lower timestamp"),
                ("second query in batch", "lower timestamp"),
            ),
        )
        .await
        .unwrap();

    let query_rows_result = session
        .query_unpaged(
            format!("SELECT a, b, WRITETIME(b) FROM {ks}.t_timestamp"),
            &[],
        )
        .await
        .unwrap()
        .into_rows_result()
        .unwrap();

    let mut results = query_rows_result
        .rows::<(&str, &str, i64)>()
        .unwrap()
        .map(Result::unwrap)
        .collect::<Vec<_>>();
    results.sort();

    let expected_results = [
        ("first query in batch", "higher timestamp", 420),
        ("prepared query", "higher timestamp", 420),
        ("prepared query iter", "higher timestamp", 420),
        ("prepared query single page", "higher timestamp", 420),
        ("regular query", "higher timestamp", 420),
        ("regular query iter", "higher timestamp", 420),
        ("regular query single page", "higher timestamp", 420),
        ("second query in batch", "higher timestamp", 420),
    ]
    .into_iter()
    .collect::<Vec<_>>();

    assert_eq!(results, expected_results);

    session.ddl(format!("DROP KEYSPACE {ks}")).await.unwrap();
}

#[tokio::test]
async fn test_timestamp_generator() {
    setup_tracing();
    struct LocalTimestampGenerator {
        generated_timestamps: Arc<Mutex<HashSet<i64>>>,
    }

    impl TimestampGenerator for LocalTimestampGenerator {
        fn next_timestamp(&self) -> i64 {
            // A `u64` shifted right by one is in `0..=i64::MAX`.
            // Thus, the timestamp is always positive.
            // Do not use `abs()`: it overflows on `i64::MIN`.
            let timestamp = (random::<u64>() >> 1) as i64;
            self.generated_timestamps.lock().unwrap().insert(timestamp);
            timestamp
        }
    }

    let timestamps = Arc::new(Mutex::new(HashSet::new()));
    let generator = LocalTimestampGenerator {
        generated_timestamps: timestamps.clone(),
    };

    let session = create_new_session_builder()
        .timestamp_generator(Arc::new(generator))
        .build()
        .await
        .unwrap();
    let ks = unique_keyspace_name();
    session.ddl(format!("CREATE KEYSPACE IF NOT EXISTS {ks} WITH REPLICATION = {{'class' : 'NetworkTopologyStrategy', 'replication_factor' : 1}}")).await.unwrap();
    session
        .ddl(format!(
            "CREATE TABLE IF NOT EXISTS {ks}.t_generator (a int primary key, b int)"
        ))
        .await
        .unwrap();

    let prepared = session
        .prepare(format!("INSERT INTO {ks}.t_generator (a, b) VALUES (1, 1)"))
        .await
        .unwrap();
    session.execute_unpaged(&prepared, []).await.unwrap();

    let unprepared = Statement::new(format!("INSERT INTO {ks}.t_generator (a, b) VALUES (2, 2)"));
    session.query_unpaged(unprepared, []).await.unwrap();

    let mut batch = Batch::new(BatchType::Unlogged);
    let stmt = session
        .prepare(format!("INSERT INTO {ks}.t_generator (a, b) VALUES (3, 3)"))
        .await
        .unwrap();
    batch.append_statement(stmt);
    session.batch(&batch, &((),)).await.unwrap();

    let query_rows_result = session
        .query_unpaged(
            format!("SELECT a, b, WRITETIME(b) FROM {ks}.t_generator"),
            &[],
        )
        .await
        .unwrap()
        .into_rows_result()
        .unwrap();

    {
        let timestamps_locked = timestamps.lock().unwrap();
        assert!(
            query_rows_result
                .rows::<(i32, i32, i64)>()
                .unwrap()
                .map(|row_result| row_result.unwrap())
                .all(|(_a, _b, writetime)| timestamps_locked.contains(&writetime))
        );
    }

    session.ddl(format!("DROP KEYSPACE {ks}")).await.unwrap();
}

/// The timestamp generator must be called once per request, not once per attempt:
/// a retried request has to carry the same timestamp on every attempt.
#[tokio::test]
async fn test_timestamp_generator_called_once_per_request() {
    setup_tracing();

    struct CountingGenerator(Arc<AtomicUsize>);

    impl TimestampGenerator for CountingGenerator {
        fn next_timestamp(&self) -> i64 {
            self.0.fetch_add(1, Ordering::Relaxed) as i64 + 1
        }
    }

    // Every node fails every request, so each request is attempted on all 3 nodes.
    let calls = Arc::new(AtomicUsize::new(0));
    let res = test_with_3_node_cluster(
        ShardAwareness::QueryNode,
        async |proxy_uris, translation_map, mut running_proxy| {
            let session = SessionBuilder::new()
                .known_node(proxy_uris[0].as_str())
                .address_translator(Arc::new(translation_map))
                .timestamp_generator(Arc::new(CountingGenerator(calls.clone())))
                .build()
                .await
                .unwrap();
            let ks = unique_keyspace_name();
            session.ddl(format!("CREATE KEYSPACE {ks} WITH REPLICATION = {{'class' : 'NetworkTopologyStrategy', 'replication_factor' : 1}}")).await.unwrap();
            session
                .ddl(format!("CREATE TABLE {ks}.t (a int primary key, b int)"))
                .await
                .unwrap();

            let insert = format!("INSERT INTO {ks}.t (a, b) VALUES (1, 1)");
            // Idempotent, so that the default retry policy tries every node.
            let mut stmt = Statement::new(insert.as_str());
            stmt.set_is_idempotent(true);
            let mut batch = Batch::new(BatchType::Unlogged);
            batch.append_statement(session.prepare(insert.as_str()).await.unwrap());
            batch.set_is_idempotent(true);

            let fail_requests = Condition::not(Condition::ConnectionRegisteredAnyEvent).and(
                Condition::RequestOpcode(RequestOpcode::Execute)
                    .or(Condition::RequestOpcode(RequestOpcode::Batch))
                    .or(Condition::RequestOpcode(RequestOpcode::Query)
                        .and(Condition::BodyContainsCaseSensitive(Box::new(*b"INTO")))),
            );
            for node in running_proxy.running_nodes.iter_mut() {
                node.change_request_rules(Some(vec![RequestRule(
                    fail_requests.clone(),
                    RequestReaction::forge().server_error(),
                )]));
            }

            for entry_point in EntryPoint::ALL {
                let before = calls.load(Ordering::Relaxed);
                entry_point.send(&session, stmt.clone(), ()).await.unwrap_err();
                let count = calls.load(Ordering::Relaxed) - before;
                assert_eq!(count, 3, "{}", entry_point.name());
            }

            let before = calls.load(Ordering::Relaxed);
            session.batch(&batch, ((),)).await.unwrap_err();
            assert_eq!(calls.load(Ordering::Relaxed) - before, 3, "batch");

            running_proxy.turn_off_rules();
            session.ddl(format!("DROP KEYSPACE {ks}")).await.unwrap();
            running_proxy
        },
    )
    .await;

    match res {
        Ok(()) => (),
        Err(ProxyError::Worker(WorkerError::DriverDisconnected(_))) => (),
        Err(err) => panic!("{err}"),
    }
}
