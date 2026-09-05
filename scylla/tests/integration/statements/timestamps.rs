use std::{
    collections::HashSet,
    sync::{Arc, Mutex},
};

use rand::random;
use scylla::{
    policies::timestamp_generator::TimestampGenerator,
    statement::{
        Statement,
        batch::{Batch, BatchType},
    },
};

use crate::entry_point::PagingMode;
use crate::utils::{
    PerformDDL as _, create_new_session_builder, setup_tracing, unique_keyspace_name,
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

    // A timestamp set on a statement must be honoured no matter which entry
    // point sends it, so every one of them is exercised with the same pair of
    // timestamps: the row must end up carrying the higher one. The timestamp
    // goes on the statement itself, on the prepared one for the prepared entry
    // points, so that it is `PreparedStatement::set_timestamp` under test and
    // not `prepare`'s carrying the configuration over.
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
            // Shifting a `u64` right by one yields `0..=i64::MAX`, so the
            // timestamp is positive without an `abs()` that would overflow on
            // `i64::MIN`.
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
