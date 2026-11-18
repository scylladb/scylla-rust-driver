use crate::utils::{
    PerformDDL, create_new_session_builder, setup_tracing, supports_feature, unique_keyspace_name,
};

use crate::entry_point::EntryPoint;
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
    session.ddl(format!("CREATE KEYSPACE IF NOT EXISTS {ks} WITH REPLICATION = {{'class' : 'NetworkTopologyStrategy', 'replication_factor' : 1}}")).await.unwrap();

    let create_keyspace_res = session.ddl(format!("CREATE KEYSPACE {ks} WITH REPLICATION = {{'class' : 'NetworkTopologyStrategy', 'replication_factor' : 1}}")).await;
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
            "CREATE TABLE IF NOT EXISTS {ks}.tab (a text primary key)"
        ))
        .await
        .unwrap();

    let create_table_res = session
        .ddl(format!("CREATE TABLE {ks}.tab (a text primary key)"))
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

    session.ddl(format!("DROP KEYSPACE {ks}")).await.unwrap();
}

#[tokio::test]
async fn test_rate_limit_exceeded_exception() {
    setup_tracing();
    let session = create_new_session_builder().build().await.unwrap();

    // Typed errors in RPC were introduced along with per-partition rate limiting.
    // There is no dedicated feature for per-partition rate limiting, so we are
    // looking at the other one.
    if !supports_feature(&session, "TYPED_ERRORS_IN_READ_RPC").await {
        println!("Skipping because the cluster doesn't support per partition rate limiting");
        return;
    }

    let ks = unique_keyspace_name();
    session.ddl(format!("CREATE KEYSPACE IF NOT EXISTS {ks} WITH REPLICATION = {{'class' : 'NetworkTopologyStrategy', 'replication_factor' : 1}}")).await.unwrap();
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
        err => panic!("Unexpected error type received: {err:?}"),
    }

    session.ddl(format!("DROP KEYSPACE {ks}")).await.unwrap();
}

/// The database rejects a statement that reads a table that does not exist.
/// The error must reach the caller from every entry point.
///
/// The statement runs once without values and once with a value. The two
/// differ for `query_iter`. Without values, it sends the request, and the
/// error comes from the first page. With values, it prepares the statement
/// first, and the error comes from the preparation.
#[tokio::test]
async fn test_db_error_reaches_every_entry_point() {
    setup_tracing();
    let session = create_new_session_builder().build().await.unwrap();

    for entry_point in EntryPoint::ALL {
        let err = entry_point
            .send(&session, "SELECT * FROM system.no_such_table", ())
            .await
            .unwrap_err();
        let (db_error, message) = err.into_db_error(entry_point);
        assert_eq!(db_error, DbError::Invalid, "{}", entry_point.name());
        assert!(message.contains("no_such_table"), "{message}");

        let err = entry_point
            .send(
                &session,
                "SELECT * FROM system.no_such_table WHERE k = ?",
                ("key",),
            )
            .await
            .unwrap_err();
        let (db_error, message) = err.into_db_error(entry_point);
        assert_eq!(db_error, DbError::Invalid, "{}", entry_point.name());
        assert!(message.contains("no_such_table"), "{message}");
    }
}
