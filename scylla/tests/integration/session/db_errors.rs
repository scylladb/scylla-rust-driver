use crate::utils::{
    create_new_session_builder, setup_tracing, supports_feature, unique_keyspace_name, PerformDDL,
};

use scylla::errors::OperationType;
use scylla::errors::{DbError, ExecutionError, RequestAttemptError};

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
