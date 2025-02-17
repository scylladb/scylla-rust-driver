use crate::utils::{create_new_session_builder, setup_tracing, unique_keyspace_name, PerformDDL};
use assert_matches::assert_matches;
use futures::TryStreamExt;
use scylla::{
    batch::{Batch, BatchType},
    client::session::Session,
    errors::{
        BadQuery, DbError, ExecutionError, NextPageError, PagerExecutionError, RequestAttemptError,
        RequestError,
    },
    query::Query,
    response::PagingState,
    serialize::{self, value::SerializeValue},
    statement::SerialConsistency,
};
use scylla_cql::Consistency;
use std::collections::{BTreeMap, HashMap, HashSet};
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
            let timestamp = (random::<u64>() as i64).abs();
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

#[tokio::test]
async fn test_named_bind_markers() {
    let session = create_new_session_builder().build().await.unwrap();
    let ks = unique_keyspace_name();

    session
        .ddl(format!("CREATE KEYSPACE {} WITH REPLICATION = {{'class' : 'NetworkTopologyStrategy', 'replication_factor' : 1}}", ks))
        .await
        .unwrap();

    session.use_keyspace(ks, false).await.unwrap();

    session
        .ddl("CREATE TABLE t (pk int, ck int, v int, PRIMARY KEY (pk, ck, v))")
        .await
        .unwrap();

    let prepared = session
        .prepare("INSERT INTO t (pk, ck, v) VALUES (:pk, :ck, :v)")
        .await
        .unwrap();
    let hashmap: HashMap<&str, i32> = HashMap::from([("pk", 7), ("v", 42), ("ck", 13)]);
    session.execute_unpaged(&prepared, &hashmap).await.unwrap();

    let btreemap: BTreeMap<&str, i32> = BTreeMap::from([("ck", 113), ("v", 142), ("pk", 17)]);
    session.execute_unpaged(&prepared, &btreemap).await.unwrap();

    let rows: Vec<(i32, i32, i32)> = session
        .query_unpaged("SELECT pk, ck, v FROM t", &[])
        .await
        .unwrap()
        .into_rows_result()
        .unwrap()
        .rows::<(i32, i32, i32)>()
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();

    assert_eq!(rows, vec![(7, 13, 42), (17, 113, 142)]);

    let wrongmaps = [
        HashMap::from([("v", 7), ("fefe", 42), ("ck", 13)]),
        HashMap::from([("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx", 7)]),
        HashMap::new(),
        HashMap::from([("ck", 9)]),
    ];
    for wrongmap in wrongmaps {
        let result = session.execute_unpaged(&prepared, &wrongmap).await;
        let Err(ExecutionError::BadQuery(BadQuery::SerializationError(e))) = result else {
            panic!("Expected ValueMissingForColumn error");
        };
        assert_matches!(&e
                    .downcast_ref::<serialize::row::BuiltinTypeCheckError>()
                    .unwrap()
                    .kind,
                    serialize::row::BuiltinTypeCheckErrorKind::ValueMissingForColumn { name }
                        if name == "pk");
    }
}

#[tokio::test]
async fn test_unusual_valuelists() {
    setup_tracing();

    let session = create_new_session_builder().build().await.unwrap();
    let ks = unique_keyspace_name();

    session.ddl(format!("CREATE KEYSPACE IF NOT EXISTS {} WITH REPLICATION = {{'class' : 'NetworkTopologyStrategy', 'replication_factor' : 1}}", ks)).await.unwrap();
    session.use_keyspace(ks, false).await.unwrap();

    session
        .ddl("CREATE TABLE IF NOT EXISTS tab (a int, b int, c varchar, primary key (a, b, c))")
        .await
        .unwrap();

    let insert_a_b_c = session
        .prepare("INSERT INTO tab (a, b, c) VALUES (?, ?, ?)")
        .await
        .unwrap();

    let values_dyn: Vec<&dyn SerializeValue> = vec![
        &1 as &dyn SerializeValue,
        &2 as &dyn SerializeValue,
        &"&dyn" as &dyn SerializeValue,
    ];
    session
        .execute_unpaged(&insert_a_b_c, values_dyn)
        .await
        .unwrap();

    let values_box_dyn: Vec<Box<dyn SerializeValue>> = vec![
        Box::new(1) as Box<dyn SerializeValue>,
        Box::new(3) as Box<dyn SerializeValue>,
        Box::new("Box dyn") as Box<dyn SerializeValue>,
    ];
    session
        .execute_unpaged(&insert_a_b_c, values_box_dyn)
        .await
        .unwrap();

    session
        .execute_unpaged(
            &insert_a_b_c,
            (
                Box::new(1) as Box<dyn SerializeValue>,
                &2 as &dyn SerializeValue,
                &"Box,&dyn" as &dyn SerializeValue,
            ),
        )
        .await
        .unwrap();

    session
        .execute_unpaged(
            &insert_a_b_c,
            (
                &2 as &dyn SerializeValue,
                Box::new(1) as Box<dyn SerializeValue>,
                &"&dyn,Box" as &dyn SerializeValue,
            ),
        )
        .await
        .unwrap();

    let mut all_rows: Vec<(i32, i32, String)> = session
        .query_unpaged("SELECT a, b, c FROM tab", ())
        .await
        .unwrap()
        .into_rows_result()
        .unwrap()
        .rows::<(i32, i32, String)>()
        .unwrap()
        .map(|r| r.unwrap())
        .collect();
    all_rows.sort_unstable();
    assert_eq!(
        all_rows,
        vec![
            (1i32, 2i32, "&dyn".to_owned()),
            (1, 2i32, "Box,&dyn".to_owned()),
            (1, 3, "Box dyn".to_owned()),
            (2i32, 1, "&dyn,Box".to_owned())
        ]
    );
}

const KEY: &str = "test";

/// Initialize a cluster with two tables table and insert some data into one of them.
/// Returns a session.
///
/// # Example
/// ```rust
/// let session = initialize_cluster_two_tables().await;
/// ```
async fn initialize_cluster_two_tables() -> Session {
    setup_tracing();
    let session = create_new_session_builder().build().await.unwrap();

    let ks = unique_keyspace_name();

    session.ddl(format!("CREATE KEYSPACE IF NOT EXISTS {} WITH REPLICATION = {{'class' : 'NetworkTopologyStrategy', 'replication_factor' : 1}}", ks)).await.unwrap();
    session.use_keyspace(ks, false).await.unwrap();
    session
        .ddl("CREATE TABLE IF NOT EXISTS test (k text, v int, PRIMARY KEY(k, v))")
        .await
        .unwrap();

    for i in 0..100 {
        session
            .query_unpaged("INSERT INTO test (k, v) VALUES (?, ?)", (KEY, i))
            .await
            .unwrap();
    }

    session
        .ddl("CREATE TABLE IF NOT EXISTS test2 (k text primary key, v int)")
        .await
        .unwrap();

    session
}

#[tokio::test]
async fn should_fail_when_too_many_positional_values_provided() {
    let session = initialize_cluster_two_tables().await;
    let query = Query::new("SELECT v FROM test WHERE k=?");

    let result = session.query_unpaged(query.clone(), (KEY, 1)).await;
    let Err(ExecutionError::LastAttemptError(RequestAttemptError::SerializationError(e))) = result
    else {
        panic!("Expected WrongColumnCount error");
    };
    assert_matches!(
        e.downcast_ref::<serialize::row::BuiltinTypeCheckError>()
            .unwrap()
            .kind,
        serialize::row::BuiltinTypeCheckErrorKind::WrongColumnCount {
            rust_cols: 2,
            cql_cols: 1,
        }
    );

    let result = session.query_iter(query.clone(), (KEY, 1)).await;
    let Err(PagerExecutionError::SerializationError(e)) = result else {
        panic!("Expected WrongColumnCount error");
    };

    assert_matches!(
        e.downcast_ref::<serialize::row::BuiltinTypeCheckError>()
            .unwrap()
            .kind,
        serialize::row::BuiltinTypeCheckErrorKind::WrongColumnCount {
            rust_cols: 2,
            cql_cols: 1,
        }
    );

    let result = session
        .query_single_page(query.clone(), (KEY, 1), PagingState::start())
        .await;
    let Err(ExecutionError::LastAttemptError(RequestAttemptError::SerializationError(e))) = result
    else {
        panic!("Expected WrongColumnCount error");
    };
    assert_matches!(
        e.downcast_ref::<serialize::row::BuiltinTypeCheckError>()
            .unwrap()
            .kind,
        serialize::row::BuiltinTypeCheckErrorKind::WrongColumnCount {
            rust_cols: 2,
            cql_cols: 1,
        }
    );

    let prepared_query = session.prepare(query).await.unwrap();

    let result = session.execute_unpaged(&prepared_query, (KEY, 1)).await;
    let Err(ExecutionError::BadQuery(BadQuery::SerializationError(e))) = result else {
        panic!("Expected WrongColumnCount error");
    };
    assert_matches!(
        e.downcast_ref::<serialize::row::BuiltinTypeCheckError>()
            .unwrap()
            .kind,
        serialize::row::BuiltinTypeCheckErrorKind::WrongColumnCount {
            rust_cols: 2,
            cql_cols: 1,
        }
    );

    let result = session.execute_iter(prepared_query.clone(), (KEY, 1)).await;
    let Err(PagerExecutionError::SerializationError(e)) = result else {
        panic!("Expected WrongColumnCount error");
    };
    assert_matches!(
        e.downcast_ref::<serialize::row::BuiltinTypeCheckError>()
            .unwrap()
            .kind,
        serialize::row::BuiltinTypeCheckErrorKind::WrongColumnCount {
            rust_cols: 2,
            cql_cols: 1,
        }
    );

    let result = session
        .execute_single_page(&prepared_query, (KEY, 1), PagingState::start())
        .await;
    let Err(ExecutionError::BadQuery(BadQuery::SerializationError(e))) = result else {
        panic!("Expected WrongColumnCount error");
    };
    assert_matches!(
        e.downcast_ref::<serialize::row::BuiltinTypeCheckError>()
            .unwrap()
            .kind,
        serialize::row::BuiltinTypeCheckErrorKind::WrongColumnCount {
            rust_cols: 2,
            cql_cols: 1,
        }
    );
}

#[tokio::test]
async fn should_fail_when_not_enough_positional_values_provided() {
    let session = initialize_cluster_two_tables().await;

    let query = Query::new("SELECT v FROM test WHERE k=?");

    let result = session.query_unpaged(query.clone(), ()).await;
    assert_matches!(
        result,
        Err(ExecutionError::LastAttemptError(
            RequestAttemptError::DbError(DbError::Invalid, message)
        )) if message.contains("Invalid amount of bind variables")
    );

    let result = session.query_iter(query.clone(), ()).await;
    assert_matches!(
        result,
        Err(PagerExecutionError::NextPageError(
            NextPageError::RequestFailure(RequestError::LastAttemptError(
                RequestAttemptError::DbError(DbError::Invalid, message)
            ))
        )) if message.contains("Invalid amount of bind variables")
    );

    let result = session
        .query_single_page(query.clone(), (), PagingState::start())
        .await;
    assert_matches!(
        result,
        Err(ExecutionError::LastAttemptError(
            RequestAttemptError::DbError(DbError::Invalid, message)
        )) if message.contains("Invalid amount of bind variables")
    );

    let prepared_query = session.prepare(query).await.unwrap();

    let result = session.execute_unpaged(&prepared_query, ()).await;
    let Err(ExecutionError::BadQuery(BadQuery::SerializationError(e))) = result else {
        panic!("Expected WrongColumnCount error");
    };
    assert_matches!(
        e.downcast_ref::<serialize::row::BuiltinTypeCheckError>()
            .unwrap()
            .kind,
        serialize::row::BuiltinTypeCheckErrorKind::WrongColumnCount {
            rust_cols: 0,
            cql_cols: 1,
        }
    );

    let result = session.execute_iter(prepared_query.clone(), ()).await;
    let Err(PagerExecutionError::SerializationError(e)) = result else {
        panic!("Expected WrongColumnCount error");
    };
    assert_matches!(
        e.downcast_ref::<serialize::row::BuiltinTypeCheckError>()
            .unwrap()
            .kind,
        serialize::row::BuiltinTypeCheckErrorKind::WrongColumnCount {
            rust_cols: 0,
            cql_cols: 1,
        }
    );

    let result = session
        .execute_single_page(&prepared_query, (), PagingState::start())
        .await;
    let Err(ExecutionError::BadQuery(BadQuery::SerializationError(e))) = result else {
        panic!("Expected WrongColumnCount error");
    };
    assert_matches!(
        e.downcast_ref::<serialize::row::BuiltinTypeCheckError>()
            .unwrap()
            .kind,
        serialize::row::BuiltinTypeCheckErrorKind::WrongColumnCount {
            rust_cols: 0,
            cql_cols: 1,
        }
    );
}

#[tokio::test]
async fn should_allow_nulls_in_positional_values() {
    let session = initialize_cluster_two_tables().await;

    let insert_query = Query::new("INSERT INTO test2 (k, v) VALUES (?, ?)");
    let select_query = Query::new("SELECT k, v FROM test2 WHERE k=?");

    {
        let name = "name1";
        let result = session
            .query_unpaged(insert_query.clone(), (name, None::<i32>))
            .await;
        assert!(result.is_ok());

        let rows = session
            .query_unpaged(select_query.clone(), (name,))
            .await
            .unwrap()
            .into_rows_result()
            .unwrap();
        assert_eq!(rows.rows_num(), 1);
        let row = rows.single_row::<(String, Option<i32>)>().unwrap();
        assert_eq!(row, (name.to_string(), None));
    }
    {
        let name = "name2";
        let result = session
            .query_iter(insert_query.clone(), (name, None::<i32>))
            .await;
        assert!(result.is_ok());

        let mut iter = session
            .query_iter(select_query.clone(), (name,))
            .await
            .unwrap()
            .rows_stream::<(String, Option<i32>)>()
            .unwrap();

        let mut i = 0;
        while let Some((a, b)) = iter.try_next().await.unwrap() {
            assert_eq!(a, name);
            assert!(b.is_none());
            i += 1;
        }
        assert_eq!(i, 1);
    }

    let result = session
        .query_single_page(
            insert_query.clone(),
            ("name3", None::<i32>),
            PagingState::start(),
        )
        .await;
    assert!(result.is_ok());

    let prepared_insert_query = session.prepare(insert_query).await.unwrap();
    let prepared_select_query = session.prepare(select_query).await.unwrap();

    {
        let name = "name4";
        let result = session
            .execute_unpaged(&prepared_insert_query, (name, None::<i32>))
            .await;
        assert!(result.is_ok());

        let rows = session
            .execute_unpaged(&prepared_select_query, (name,))
            .await
            .unwrap()
            .into_rows_result()
            .unwrap();
        assert_eq!(rows.rows_num(), 1);
        let row = rows.single_row::<(String, Option<i32>)>().unwrap();
        assert_eq!(row, (name.to_string(), None));
    }
    {
        let name = "name5";
        let result = session
            .execute_iter(prepared_insert_query.clone(), (name, None::<i32>))
            .await;
        assert!(result.is_ok());

        let mut iter = session
            .execute_iter(prepared_select_query.clone(), (name,))
            .await
            .unwrap()
            .rows_stream::<(String, Option<i32>)>()
            .unwrap();

        let mut i = 0;
        while let Some((a, b)) = iter.try_next().await.unwrap() {
            assert_eq!(a, name);
            assert!(b.is_none());
            i += 1;
        }
        assert_eq!(i, 1);
    }

    let result = session
        .execute_single_page(
            &prepared_insert_query,
            ("name6", None::<i32>),
            PagingState::start(),
        )
        .await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn should_allow_nulls_in_named_bind_markers() {
    let session = initialize_cluster_two_tables().await;

    let query = Query::new("INSERT INTO test2 (k, v) VALUES (:k, :v)");
    let select_query = Query::new("SELECT k, v FROM test2 WHERE k=?");

    let prepared_query = session.prepare(query.clone()).await.unwrap();

    #[derive(scylla::SerializeRow)]
    struct TestStruct<'a> {
        k: &'a str,
        v: Option<i32>,
    }

    {
        let name = "name1";
        session
            .query_unpaged(
                query.clone(),
                TestStruct {
                    k: name,
                    v: None::<i32>,
                },
            )
            .await
            .unwrap();
        let rows: Vec<(String, Option<i32>)> = session
            .query_unpaged(select_query.clone(), (name,))
            .await
            .unwrap()
            .into_rows_result()
            .unwrap()
            .rows::<(String, Option<i32>)>()
            .unwrap()
            .map(|res| res.unwrap())
            .collect();

        assert_eq!(rows, vec![(name.to_string(), None::<i32>)]);
    }
    {
        let name = "name2";
        session
            .query_iter(
                query.clone(),
                TestStruct {
                    k: name,
                    v: None::<i32>,
                },
            )
            .await
            .unwrap();
        let rows: Vec<(String, Option<i32>)> = session
            .query_unpaged(select_query.clone(), (name,))
            .await
            .unwrap()
            .into_rows_result()
            .unwrap()
            .rows::<(String, Option<i32>)>()
            .unwrap()
            .map(|res| res.unwrap())
            .collect();

        assert_eq!(rows, vec![(name.to_string(), None::<i32>)]);
    }
    {
        let name = "name3";
        session
            .execute_unpaged(
                &prepared_query,
                TestStruct {
                    k: name,
                    v: None::<i32>,
                },
            )
            .await
            .unwrap();
        let rows: Vec<(String, Option<i32>)> = session
            .query_unpaged(select_query.clone(), (name,))
            .await
            .unwrap()
            .into_rows_result()
            .unwrap()
            .rows::<(String, Option<i32>)>()
            .unwrap()
            .map(|res| res.unwrap())
            .collect();

        assert_eq!(rows, vec![(name.to_string(), None::<i32>)]);
    }
    {
        let name = "name4";
        session
            .execute_iter(
                prepared_query.clone(),
                TestStruct {
                    k: name,
                    v: None::<i32>,
                },
            )
            .await
            .unwrap();
        let rows: Vec<(String, Option<i32>)> = session
            .query_unpaged(select_query.clone(), (name,))
            .await
            .unwrap()
            .into_rows_result()
            .unwrap()
            .rows::<(String, Option<i32>)>()
            .unwrap()
            .map(|res| res.unwrap())
            .collect();

        assert_eq!(rows, vec![(name.to_string(), None::<i32>)]);
    }
}

#[tokio::test]
async fn test_named_bind_markers_with_case_sensitive_id() {
    let session = initialize_cluster_two_tables().await;

    let regular_query = Query::new("SELECT count(*) FROM test2 WHERE k=:theKey");
    let prepared_regular_query = session.prepare(regular_query.clone()).await.unwrap();
    let case_sensitive_query = Query::new("SELECT count(*) FROM test2 WHERE k=:\"theKey\"");
    let prepared_case_sensitive_query =
        session.prepare(case_sensitive_query.clone()).await.unwrap();

    let hashmap_case_sensitive: HashMap<&str, &str> = HashMap::from([("theKey", "name")]);
    let hashmap_regular: HashMap<&str, &str> = HashMap::from([("thekey", "name")]);
    {
        let result = session
            .query_unpaged(regular_query.clone(), &hashmap_case_sensitive)
            .await;
        let Err(ExecutionError::LastAttemptError(RequestAttemptError::SerializationError(e))) =
            result
        else {
            panic!("Expected ValueMissingForColumn error");
        };
        assert_matches!(&e
            .downcast_ref::<serialize::row::BuiltinTypeCheckError>()
            .unwrap()
            .kind,
            serialize::row::BuiltinTypeCheckErrorKind::ValueMissingForColumn { name }
                if name == "thekey");

        let rows = session
            .query_unpaged(regular_query.clone(), &hashmap_regular)
            .await
            .unwrap()
            .into_rows_result()
            .unwrap();
        assert_eq!(rows.rows_num(), 1);
        assert!(rows.single_row::<(i64,)>().unwrap().0 == 0);

        let result = session
            .query_unpaged(case_sensitive_query.clone(), &hashmap_regular)
            .await;
        let Err(ExecutionError::LastAttemptError(RequestAttemptError::SerializationError(e))) =
            result
        else {
            panic!("Expected ValueMissingForColumn error");
        };
        assert_matches!(&e
            .downcast_ref::<serialize::row::BuiltinTypeCheckError>()
            .unwrap()
            .kind,
            serialize::row::BuiltinTypeCheckErrorKind::ValueMissingForColumn { name }
                if name == "theKey");

        let rows = session
            .query_unpaged(case_sensitive_query.clone(), &hashmap_case_sensitive)
            .await
            .unwrap()
            .into_rows_result()
            .unwrap();
        assert_eq!(rows.rows_num(), 1);
        assert!(rows.single_row::<(i64,)>().unwrap().0 == 0);
    }
    {
        let result = session
            .query_iter(regular_query.clone(), &hashmap_case_sensitive)
            .await;
        let Err(PagerExecutionError::SerializationError(e)) = result else {
            panic!("Expected ValueMissingForColumn error");
        };

        assert_matches!(&e
            .downcast_ref::<serialize::row::BuiltinTypeCheckError>()
            .unwrap()
            .kind,
            serialize::row::BuiltinTypeCheckErrorKind::ValueMissingForColumn { name }
                if name == "thekey");

        let mut iter = session
            .query_iter(regular_query.clone(), &hashmap_regular)
            .await
            .unwrap()
            .rows_stream::<(i64,)>()
            .unwrap();
        let mut i = 0;
        while let Some((a,)) = iter.try_next().await.unwrap() {
            assert_eq!(a, 0);
            i += 1;
        }
        assert_eq!(i, 1);

        let result = session
            .query_iter(case_sensitive_query.clone(), &hashmap_regular)
            .await;
        let Err(PagerExecutionError::SerializationError(e)) = result else {
            panic!("Expected ValueMissingForColumn error");
        };
        assert_matches!(&e
            .downcast_ref::<serialize::row::BuiltinTypeCheckError>()
            .unwrap()
            .kind,
            serialize::row::BuiltinTypeCheckErrorKind::ValueMissingForColumn { name }
                if name == "theKey");

        let mut iter = session
            .query_iter(case_sensitive_query.clone(), &hashmap_case_sensitive)
            .await
            .unwrap()
            .rows_stream::<(i64,)>()
            .unwrap();
        let mut i = 0;
        while let Some((a,)) = iter.try_next().await.unwrap() {
            assert_eq!(a, 0);
            i += 1;
        }
        assert_eq!(i, 1);
    }
    {
        let result = session
            .execute_unpaged(&prepared_regular_query, &hashmap_case_sensitive)
            .await;
        let Err(ExecutionError::BadQuery(BadQuery::SerializationError(e))) = result else {
            panic!("Expected ValueMissingForColumn error");
        };
        assert_matches!(&e
            .downcast_ref::<serialize::row::BuiltinTypeCheckError>()
            .unwrap()
            .kind,
            serialize::row::BuiltinTypeCheckErrorKind::ValueMissingForColumn { name }
                if name == "thekey");

        let rows = session
            .execute_unpaged(&prepared_regular_query, &hashmap_regular)
            .await
            .unwrap()
            .into_rows_result()
            .unwrap();
        assert_eq!(rows.rows_num(), 1);
        assert!(rows.single_row::<(i64,)>().unwrap().0 == 0);

        let result = session
            .execute_unpaged(&prepared_case_sensitive_query, &hashmap_regular)
            .await;
        let Err(ExecutionError::BadQuery(BadQuery::SerializationError(e))) = result else {
            panic!("Expected ValueMissingForColumn error");
        };
        assert_matches!(&e
            .downcast_ref::<serialize::row::BuiltinTypeCheckError>()
            .unwrap()
            .kind,
            serialize::row::BuiltinTypeCheckErrorKind::ValueMissingForColumn { name }
                if name == "theKey");

        let rows = session
            .execute_unpaged(&prepared_case_sensitive_query, &hashmap_case_sensitive)
            .await
            .unwrap()
            .into_rows_result()
            .unwrap();
        assert_eq!(rows.rows_num(), 1);
        assert!(rows.single_row::<(i64,)>().unwrap().0 == 0);
    }
    {
        let result = session
            .execute_iter(prepared_regular_query.clone(), &hashmap_case_sensitive)
            .await;
        let Err(PagerExecutionError::SerializationError(e)) = result else {
            panic!("Expected ValueMissingForColumn error");
        };
        assert_matches!(&e
            .downcast_ref::<serialize::row::BuiltinTypeCheckError>()
            .unwrap()
            .kind,
            serialize::row::BuiltinTypeCheckErrorKind::ValueMissingForColumn { name }
                if name == "thekey");

        let mut iter = session
            .execute_iter(prepared_regular_query.clone(), &hashmap_regular)
            .await
            .unwrap()
            .rows_stream::<(i64,)>()
            .unwrap();
        let mut i = 0;
        while let Some((a,)) = iter.try_next().await.unwrap() {
            assert_eq!(a, 0);
            i += 1;
        }
        assert_eq!(i, 1);

        let result = session
            .execute_iter(prepared_case_sensitive_query.clone(), &hashmap_regular)
            .await;
        let Err(PagerExecutionError::SerializationError(e)) = result else {
            panic!("Expected ValueMissingForColumn error");
        };
        assert_matches!(&e
            .downcast_ref::<serialize::row::BuiltinTypeCheckError>()
            .unwrap()
            .kind,
            serialize::row::BuiltinTypeCheckErrorKind::ValueMissingForColumn { name }
                if name == "theKey");

        let mut iter = session
            .execute_iter(
                prepared_case_sensitive_query.clone(),
                &hashmap_case_sensitive,
            )
            .await
            .unwrap()
            .rows_stream::<(i64,)>()
            .unwrap();
        let mut i = 0;
        while let Some((a,)) = iter.try_next().await.unwrap() {
            assert_eq!(a, 0);
            i += 1;
        }
        assert_eq!(i, 1);
    }
}
