use crate::utils::{
    create_new_session_builder, setup_tracing, supports_feature, unique_keyspace_name, PerformDDL,
};

use scylla::client::execution_profile::ExecutionProfile;
use scylla::errors::OperationType;
use scylla::errors::{DbError, ExecutionError, RequestAttemptError};
use scylla::statement::unprepared::Statement;

use assert_matches::assert_matches;
use std::collections::{BTreeMap, HashMap};
use std::time::Duration;
use std::vec;

// Test that some Database Errors are parsed correctly
#[tokio::test]
async fn test_db_errors() {
    setup_tracing();
    let session = create_new_session_builder().build().await.unwrap();
    let ks = unique_keyspace_name();

    // SyntaxError on bad query
    assert!(matches!(
        session.query_unpaged("gibberish", &[]).await,
        Err(ExecutionError::LastAttemptError(
            RequestAttemptError::DbError(DbError::SyntaxError, _)
        ))
    ));

    // AlreadyExists when creating a keyspace for the second time
    session.ddl(format!("CREATE KEYSPACE IF NOT EXISTS {} WITH REPLICATION = {{'class' : 'NetworkTopologyStrategy', 'replication_factor' : 1}}", ks)).await.unwrap();

    let create_keyspace_res = session.ddl(format!("CREATE KEYSPACE {} WITH REPLICATION = {{'class' : 'NetworkTopologyStrategy', 'replication_factor' : 1}}", ks)).await;
    let keyspace_exists_error: DbError = match create_keyspace_res {
        Err(ExecutionError::LastAttemptError(RequestAttemptError::DbError(e, _))) => e,
        _ => panic!("Second CREATE KEYSPACE didn't return an error!"),
    };

    assert_eq!(
        keyspace_exists_error,
        DbError::AlreadyExists {
            keyspace: ks.clone(),
            table: "".to_string()
        }
    );

    // AlreadyExists when creating a table for the second time
    session
        .ddl(format!(
            "CREATE TABLE IF NOT EXISTS {}.tab (a text primary key)",
            ks
        ))
        .await
        .unwrap();

    let create_table_res = session
        .ddl(format!("CREATE TABLE {}.tab (a text primary key)", ks))
        .await;
    let create_tab_error: DbError = match create_table_res {
        Err(ExecutionError::LastAttemptError(RequestAttemptError::DbError(e, _))) => e,
        _ => panic!("Second CREATE TABLE didn't return an error!"),
    };

    assert_eq!(
        create_tab_error,
        DbError::AlreadyExists {
            keyspace: ks.clone(),
            table: "tab".to_string()
        }
    );
}

#[tokio::test]
async fn test_await_schema_agreement() {
    setup_tracing();
    let session = create_new_session_builder().build().await.unwrap();
    let _schema_version = session.await_schema_agreement().await.unwrap();
}

#[ignore = "works on remote Scylla instances only (local ones are too fast)"]
#[tokio::test]
async fn test_request_timeout() {
    setup_tracing();

    let fast_timeouting_profile_handle = ExecutionProfile::builder()
        .request_timeout(Some(Duration::from_millis(1)))
        .build()
        .into_handle();

    {
        let session = create_new_session_builder().build().await.unwrap();

        let mut query: Statement = Statement::new("SELECT * FROM system_schema.tables");
        query.set_request_timeout(Some(Duration::from_millis(1)));
        match session.query_unpaged(query, &[]).await {
            Ok(_) => panic!("the query should have failed due to a client-side timeout"),
            Err(e) => assert_matches!(e, ExecutionError::RequestTimeout(_)),
        }

        let mut prepared = session
            .prepare("SELECT * FROM system_schema.tables")
            .await
            .unwrap();

        prepared.set_request_timeout(Some(Duration::from_millis(1)));
        match session.execute_unpaged(&prepared, &[]).await {
            Ok(_) => panic!("the prepared query should have failed due to a client-side timeout"),
            Err(e) => assert_matches!(e, ExecutionError::RequestTimeout(_)),
        };
    }
    {
        let timeouting_session = create_new_session_builder()
            .default_execution_profile_handle(fast_timeouting_profile_handle)
            .build()
            .await
            .unwrap();

        let mut query = Statement::new("SELECT * FROM system_schema.tables");

        match timeouting_session.query_unpaged(query.clone(), &[]).await {
            Ok(_) => panic!("the query should have failed due to a client-side timeout"),
            Err(e) => assert_matches!(e, ExecutionError::RequestTimeout(_)),
        };

        query.set_request_timeout(Some(Duration::from_secs(10000)));

        timeouting_session.query_unpaged(query, &[]).await.expect(
            "the query should have not failed, because no client-side timeout was specified",
        );

        let mut prepared = timeouting_session
            .prepare("SELECT * FROM system_schema.tables")
            .await
            .unwrap();

        match timeouting_session.execute_unpaged(&prepared, &[]).await {
            Ok(_) => panic!("the prepared query should have failed due to a client-side timeout"),
            Err(e) => assert_matches!(e, ExecutionError::RequestTimeout(_)),
        };

        prepared.set_request_timeout(Some(Duration::from_secs(10000)));

        timeouting_session.execute_unpaged(&prepared, &[]).await.expect("the prepared query should have not failed, because no client-side timeout was specified");
    }
}

#[tokio::test]
async fn test_named_bind_markers() {
    setup_tracing();

    let session = create_new_session_builder().build().await.unwrap();
    let ks = unique_keyspace_name();

    session
        .ddl(format!("CREATE KEYSPACE {} WITH REPLICATION = {{'class' : 'NetworkTopologyStrategy', 'replication_factor' : 1}}", ks))
        .await
        .unwrap();

    session
        .query_unpaged(format!("USE {}", ks), &[])
        .await
        .unwrap();

    session
        .ddl("CREATE TABLE t (pk int, ck int, v int, PRIMARY KEY (pk, ck, v))")
        .await
        .unwrap();

    session.await_schema_agreement().await.unwrap();

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
        .map(|res| res.unwrap())
        .collect();

    assert_eq!(rows, vec![(7, 13, 42), (17, 113, 142)]);

    let wrongmaps: Vec<HashMap<&str, i32>> = vec![
        HashMap::from([("pk", 7), ("fefe", 42), ("ck", 13)]),
        HashMap::from([("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx", 7)]),
        HashMap::new(),
        HashMap::from([("ck", 9)]),
    ];
    for wrongmap in wrongmaps {
        assert!(session.execute_unpaged(&prepared, &wrongmap).await.is_err());
    }
}

#[tokio::test]
async fn test_rate_limit_exceeded_exception() {
    let session = create_new_session_builder().build().await.unwrap();

    // Typed errors in RPC were introduced along with per-partition rate limiting.
    // There is no dedicated feature for per-partition rate limiting, so we are
    // looking at the other one.
    if !supports_feature(&session, "TYPED_ERRORS_IN_READ_RPC").await {
        println!("Skipping because the cluster doesn't support per partition rate limiting");
        return;
    }

    let ks = unique_keyspace_name();
    session.ddl(format!("CREATE KEYSPACE IF NOT EXISTS {} WITH REPLICATION = {{'class' : 'NetworkTopologyStrategy', 'replication_factor' : 1}}", ks)).await.unwrap();
    session.use_keyspace(ks.clone(), false).await.unwrap();
    session.ddl("CREATE TABLE tbl (pk int PRIMARY KEY, v int) WITH per_partition_rate_limit = {'max_writes_per_second': 1}").await.unwrap();

    let stmt = session
        .prepare("INSERT INTO tbl (pk, v) VALUES (?, ?)")
        .await
        .unwrap();

    // The rate limit is 1 write/s, so repeat the same query
    // until an error occurs, it should happen quickly

    let mut maybe_err = None;

    for _ in 0..1000 {
        match session.execute_unpaged(&stmt, (123, 456)).await {
            Ok(_) => {} // Try again
            Err(err) => {
                maybe_err = Some(err);
                break;
            }
        }
    }

    match maybe_err.expect("Rate limit error didn't occur") {
        ExecutionError::LastAttemptError(RequestAttemptError::DbError(
            DbError::RateLimitReached { op_type, .. },
            _,
        )) => {
            assert_eq!(op_type, OperationType::Write);
        }
        err => panic!("Unexpected error type received: {:?}", err),
    }
}
