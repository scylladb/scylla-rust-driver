use crate::utils::{create_new_session_builder, setup_tracing, unique_keyspace_name, PerformDDL};
use scylla::{
    batch::{Batch, BatchType},
    query::Query,
    response::PagingState,
    statement::SerialConsistency,
};
use scylla_cql::Consistency;
use std::collections::HashSet;
use std::sync::{Arc, Mutex};

#[tokio::test]
async fn test_prepared_config() {
    use std::time::Duration;

    setup_tracing();
    let session = create_new_session_builder().build().await.unwrap();

    let mut query = Query::new("SELECT * FROM system_schema.tables");
    query.set_is_idempotent(true);
    query.set_page_size(42);
    query.set_consistency(Consistency::One);
    query.set_serial_consistency(Some(SerialConsistency::LocalSerial));
    query.set_tracing(true);
    query.set_request_timeout(Some(Duration::from_millis(1)));
    query.set_timestamp(Some(42));

    let prepared_statement = session.prepare(query).await.unwrap();

    assert!(prepared_statement.get_is_idempotent());
    assert_eq!(prepared_statement.get_page_size(), 42);
    assert_eq!(prepared_statement.get_consistency(), Some(Consistency::One));
    assert_eq!(
        prepared_statement.get_serial_consistency(),
        Some(SerialConsistency::LocalSerial)
    );
    assert!(prepared_statement.get_tracing());
    assert_eq!(
        prepared_statement.get_request_timeout(),
        Some(Duration::from_millis(1))
    );
    assert_eq!(prepared_statement.get_timestamp(), Some(42));
}

#[tokio::test]
async fn test_timestamp() {
    setup_tracing();
    let session = create_new_session_builder().build().await.unwrap();
    let ks = unique_keyspace_name();

    session.ddl(format!("CREATE KEYSPACE IF NOT EXISTS {} WITH REPLICATION = {{'class' : 'NetworkTopologyStrategy', 'replication_factor' : 1}}", ks)).await.unwrap();
    session.use_keyspace(ks, false).await.unwrap();
    session
        .ddl("CREATE TABLE IF NOT EXISTS t_timestamp (a text, b text, primary key (a))")
        .await
        .unwrap();

    let query_str = "INSERT INTO t_timestamp (a, b) VALUES (?, ?)";

    // test regular query timestamps

    let mut regular_query = Query::new(query_str.to_string());

    regular_query.set_timestamp(Some(420));
    session
        .query_unpaged(regular_query.clone(), ("regular query", "higher timestamp"))
        .await
        .unwrap();

    regular_query.set_timestamp(Some(42));
    session
        .query_unpaged(regular_query.clone(), ("regular query", "lower timestamp"))
        .await
        .unwrap();

    // test regular query iter timestamps

    let mut regular_query = Query::new(query_str.to_string());

    regular_query.set_timestamp(Some(420));
    session
        .query_iter(
            regular_query.clone(),
            ("regular query iter", "higher timestamp"),
        )
        .await
        .unwrap();

    regular_query.set_timestamp(Some(42));
    session
        .query_iter(
            regular_query.clone(),
            ("regular query iter", "lower timestamp"),
        )
        .await
        .unwrap();

    // test regular query single page timestamps

    let mut regular_query = Query::new(query_str.to_string());

    regular_query.set_timestamp(Some(420));
    session
        .query_single_page(
            regular_query.clone(),
            ("regular query single page", "higher timestamp"),
            PagingState::start(),
        )
        .await
        .unwrap();

    regular_query.set_timestamp(Some(42));
    session
        .query_single_page(
            regular_query.clone(),
            ("regular query single page", "lower timestamp"),
            PagingState::start(),
        )
        .await
        .unwrap();

    // test prepared statement timestamps

    let mut prepared_statement = session.prepare(query_str).await.unwrap();

    prepared_statement.set_timestamp(Some(420));
    session
        .execute_unpaged(&prepared_statement, ("prepared query", "higher timestamp"))
        .await
        .unwrap();

    prepared_statement.set_timestamp(Some(42));
    session
        .execute_unpaged(&prepared_statement, ("prepared query", "lower timestamp"))
        .await
        .unwrap();

    // test prepared statement iter timestamps

    let mut prepared_statement = session.prepare(query_str).await.unwrap();

    prepared_statement.set_timestamp(Some(420));
    session
        .execute_iter(
            prepared_statement.clone(),
            ("prepared query iter", "higher timestamp"),
        )
        .await
        .unwrap();

    prepared_statement.set_timestamp(Some(42));
    session
        .execute_iter(
            prepared_statement,
            ("prepared query iter", "lower timestamp"),
        )
        .await
        .unwrap();

    // test prepared statement single page timestamps

    let mut prepared_statement = session.prepare(query_str).await.unwrap();

    prepared_statement.set_timestamp(Some(420));
    session
        .execute_single_page(
            &prepared_statement,
            ("prepared query single page", "higher timestamp"),
            PagingState::start(),
        )
        .await
        .unwrap();

    prepared_statement.set_timestamp(Some(42));
    session
        .execute_single_page(
            &prepared_statement,
            ("prepared query single page", "lower timestamp"),
            PagingState::start(),
        )
        .await
        .unwrap();

    // test batch statement timestamps

    let mut batch: Batch = Default::default();
    batch.append_statement(regular_query);
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
        .query_unpaged("SELECT a, b, WRITETIME(b) FROM t_timestamp", &[])
        .await
        .unwrap()
        .into_rows_result()
        .unwrap();

    let mut results = query_rows_result
        .rows::<(&str, &str, i64)>()
        .unwrap()
        .map(Result::unwrap)
        .collect::<Vec<_>>();
    results.sort_unstable();

    let expected_results = [
        ("first query in batch", "higher timestamp", 420),
        ("prepared query", "higher timestamp", 420),
        ("prepared query iter", "higher timestamp", 420),
        ("prepared query single page", "higher timestamp", 420),
        ("regular query", "higher timestamp", 420),
        ("regular query iter", "higher timestamp", 420),
        ("regular query single page", "higher timestamp", 420),
        ("second query in batch", "higher timestamp", 420),
    ];

    assert_eq!(results, expected_results);
}

#[tokio::test]
async fn test_timestamp_generator() {
    use rand::random;
    use scylla::policies::timestamp_generator::TimestampGenerator;

    setup_tracing();
    struct LocalTimestampGenerator {
        generated_timestamps: Arc<Mutex<HashSet<i64>>>,
    }

    impl TimestampGenerator for LocalTimestampGenerator {
        fn next_timestamp(&self) -> i64 {
            let timestamp = random::<i64>().abs();
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
    session.ddl(format!("CREATE KEYSPACE IF NOT EXISTS {} WITH REPLICATION = {{'class' : 'NetworkTopologyStrategy', 'replication_factor' : 1}}", ks)).await.unwrap();
    session.use_keyspace(ks, false).await.unwrap();
    session
        .ddl("CREATE TABLE IF NOT EXISTS t_generator (a int primary key, b int)")
        .await
        .unwrap();

    let prepared = session
        .prepare("INSERT INTO t_generator (a, b) VALUES (1, 1)")
        .await
        .unwrap();
    session.execute_unpaged(&prepared, []).await.unwrap();

    let unprepared = Query::new("INSERT INTO t_generator (a, b) VALUES (2, 2)");
    session.query_unpaged(unprepared, []).await.unwrap();

    let mut batch = Batch::new(BatchType::Unlogged);
    let stmt = session
        .prepare("INSERT INTO t_generator (a, b) VALUES (3, 3)")
        .await
        .unwrap();
    batch.append_statement(stmt);
    session.batch(&batch, &((),)).await.unwrap();

    let query_rows_result = session
        .query_unpaged("SELECT a, b, WRITETIME(b) FROM t_generator", &[])
        .await
        .unwrap()
        .into_rows_result()
        .unwrap();

    let timestamps_locked = timestamps.lock().unwrap();
    assert!(query_rows_result
        .rows::<(i32, i32, i64)>()
        .unwrap()
        .map(|row_result| row_result.unwrap())
        .all(|(_a, _b, writetime)| timestamps_locked.contains(&writetime)));
}
