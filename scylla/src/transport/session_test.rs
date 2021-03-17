use crate::frame::value::ValueList;
use crate::query::Query;
use crate::routing::hash3_x64_128;
use crate::statement::Consistency;
use crate::transport::connection::QueryResult;
use crate::transport::errors::{BadKeyspaceName, BadQuery, DbError, QueryError};
use crate::{IntoTypedRows, Session, SessionBuilder};
use uuid::Uuid;

#[tokio::test]
async fn test_unprepared_statement() {
    let uri = std::env::var("SCYLLA_URI").unwrap_or_else(|_| "127.0.0.1:9042".to_string());
    let session = SessionBuilder::new().known_node(uri).build().await.unwrap();

    session.query("CREATE KEYSPACE IF NOT EXISTS ks WITH REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor' : 1}", &[]).await.unwrap();
    session
        .query("DROP TABLE IF EXISTS ks.t;", &[])
        .await
        .unwrap();
    session
        .query(
            "CREATE TABLE IF NOT EXISTS ks.t (a int, b int, c text, primary key (a, b))",
            &[],
        )
        .await
        .unwrap();
    // Wait for schema agreement
    std::thread::sleep(std::time::Duration::from_millis(300));
    session
        .query("INSERT INTO ks.t (a, b, c) VALUES (1, 2, 'abc')", &[])
        .await
        .unwrap();
    session
        .query("INSERT INTO ks.t (a, b, c) VALUES (7, 11, '')", &[])
        .await
        .unwrap();
    session
        .query("INSERT INTO ks.t (a, b, c) VALUES (1, 4, 'hello')", &[])
        .await
        .unwrap();

    let rs = session
        .query("SELECT a, b, c FROM ks.t", &[])
        .await
        .unwrap()
        .rows
        .unwrap();

    let mut results: Vec<(i32, i32, &String)> = rs
        .iter()
        .map(|r| {
            let a = r.columns[0].as_ref().unwrap().as_int().unwrap();
            let b = r.columns[1].as_ref().unwrap().as_int().unwrap();
            let c = r.columns[2].as_ref().unwrap().as_text().unwrap();
            (a, b, c)
        })
        .collect();
    results.sort();
    assert_eq!(
        results,
        vec![
            (1, 2, &String::from("abc")),
            (1, 4, &String::from("hello")),
            (7, 11, &String::from(""))
        ]
    );
}

#[tokio::test]
async fn test_prepared_statement() {
    let uri = std::env::var("SCYLLA_URI").unwrap_or_else(|_| "127.0.0.1:9042".to_string());
    let session = SessionBuilder::new().known_node(uri).build().await.unwrap();

    session.query("CREATE KEYSPACE IF NOT EXISTS ks WITH REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor' : 1}", &[]).await.unwrap();
    session
        .query("DROP TABLE IF EXISTS ks.t2;", &[])
        .await
        .unwrap();
    session
        .query("DROP TABLE IF EXISTS ks.complex_pk;", &[])
        .await
        .unwrap();
    session
        .query(
            "CREATE TABLE IF NOT EXISTS ks.t2 (a int, b int, c text, primary key (a, b))",
            &[],
        )
        .await
        .unwrap();
    session
        .query("CREATE TABLE IF NOT EXISTS ks.complex_pk (a int, b int, c text, d int, e int, primary key ((a,b,c),d))", &[])
        .await
        .unwrap();
    // Wait for schema agreement
    std::thread::sleep(std::time::Duration::from_millis(300));
    let prepared_statement = session
        .prepare("INSERT INTO ks.t2 (a, b, c) VALUES (?, ?, ?)")
        .await
        .unwrap();
    let prepared_complex_pk_statement = session
        .prepare("INSERT INTO ks.complex_pk (a, b, c, d) VALUES (?, ?, ?, 7)")
        .await
        .unwrap();

    let values = (17_i32, 16_i32, "I'm prepared!!!");
    let serialized_values = values.serialized().unwrap().into_owned();

    session.execute(&prepared_statement, &values).await.unwrap();
    session
        .execute(&prepared_complex_pk_statement, &values)
        .await
        .unwrap();

    // Verify that token calculation is compatible with Scylla
    {
        let rs = session
            .query("SELECT token(a) FROM ks.t2", &[])
            .await
            .unwrap()
            .rows
            .unwrap();
        let token: i64 = rs.first().unwrap().columns[0]
            .as_ref()
            .unwrap()
            .as_bigint()
            .unwrap();
        let expected_token = hash3_x64_128(
            &prepared_statement
                .compute_partition_key(&serialized_values)
                .unwrap(),
        ) as i64;

        assert_eq!(token, expected_token)
    }
    {
        let rs = session
            .query("SELECT token(a,b,c) FROM ks.complex_pk", &[])
            .await
            .unwrap()
            .rows
            .unwrap();
        let token: i64 = rs.first().unwrap().columns[0]
            .as_ref()
            .unwrap()
            .as_bigint()
            .unwrap();
        let expected_token = hash3_x64_128(
            &prepared_complex_pk_statement
                .compute_partition_key(&serialized_values)
                .unwrap(),
        ) as i64;

        assert_eq!(token, expected_token)
    }

    // Verify that correct data was insertd
    {
        let rs = session
            .query("SELECT a,b,c FROM ks.t2", &[])
            .await
            .unwrap()
            .rows
            .unwrap();
        let r = rs.first().unwrap();
        let a = r.columns[0].as_ref().unwrap().as_int().unwrap();
        let b = r.columns[1].as_ref().unwrap().as_int().unwrap();
        let c = r.columns[2].as_ref().unwrap().as_text().unwrap();
        assert_eq!((a, b, c), (17, 16, &String::from("I'm prepared!!!")))
    }
    {
        let rs = session
            .query("SELECT a,b,c,d,e FROM ks.complex_pk", &[])
            .await
            .unwrap()
            .rows
            .unwrap();
        let r = rs.first().unwrap();
        let a = r.columns[0].as_ref().unwrap().as_int().unwrap();
        let b = r.columns[1].as_ref().unwrap().as_int().unwrap();
        let c = r.columns[2].as_ref().unwrap().as_text().unwrap();
        let d = r.columns[3].as_ref().unwrap().as_int().unwrap();
        let e = r.columns[4].as_ref();
        assert!(e.is_none());
        assert_eq!((a, b, c, d), (17, 16, &String::from("I'm prepared!!!"), 7))
    }
}

#[tokio::test]
async fn test_batch() {
    let uri = std::env::var("SCYLLA_URI").unwrap_or_else(|_| "127.0.0.1:9042".to_string());
    let session = SessionBuilder::new().known_node(uri).build().await.unwrap();

    session.query("CREATE KEYSPACE IF NOT EXISTS ks WITH REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor' : 1}", &[]).await.unwrap();
    session
        .query("DROP TABLE IF EXISTS ks.t_batch;", &[])
        .await
        .unwrap();
    session
        .query(
            "CREATE TABLE IF NOT EXISTS ks.t_batch (a int, b int, c text, primary key (a, b))",
            &[],
        )
        .await
        .unwrap();

    // Wait for schema agreement
    std::thread::sleep(std::time::Duration::from_millis(300));

    let prepared_statement = session
        .prepare("INSERT INTO ks.t_batch (a, b, c) VALUES (?, ?, ?)")
        .await
        .unwrap();

    // TODO: Add API, that supports binding values to statements in batch creation process,
    // to avoid problem of statements/values count mismatch
    use crate::batch::Batch;
    let mut batch: Batch = Default::default();
    batch.append_statement("INSERT INTO ks.t_batch (a, b, c) VALUES (?, ?, ?)");
    batch.append_statement("INSERT INTO ks.t_batch (a, b, c) VALUES (7, 11, '')");
    batch.append_statement(prepared_statement);

    let values = ((1_i32, 2_i32, "abc"), (), (1_i32, 4_i32, "hello"));

    session.batch(&batch, values).await.unwrap();

    let rs = session
        .query("SELECT a, b, c FROM ks.t_batch", &[])
        .await
        .unwrap()
        .rows
        .unwrap();

    let mut results: Vec<(i32, i32, &String)> = rs
        .iter()
        .map(|r| {
            let a = r.columns[0].as_ref().unwrap().as_int().unwrap();
            let b = r.columns[1].as_ref().unwrap().as_int().unwrap();
            let c = r.columns[2].as_ref().unwrap().as_text().unwrap();
            (a, b, c)
        })
        .collect();
    results.sort();
    assert_eq!(
        results,
        vec![
            (1, 2, &String::from("abc")),
            (1, 4, &String::from("hello")),
            (7, 11, &String::from(""))
        ]
    );
}

#[tokio::test]
async fn test_token_calculation() {
    let uri = std::env::var("SCYLLA_URI").unwrap_or_else(|_| "127.0.0.1:9042".to_string());
    let session = SessionBuilder::new().known_node(uri).build().await.unwrap();

    session.query("CREATE KEYSPACE IF NOT EXISTS ks WITH REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor' : 1}", &[]).await.unwrap();
    session
        .query("DROP TABLE IF EXISTS ks.t3;", &[])
        .await
        .unwrap();
    session
        .query("CREATE TABLE IF NOT EXISTS ks.t3 (a text primary key)", &[])
        .await
        .unwrap();
    // Wait for schema agreement
    std::thread::sleep(std::time::Duration::from_millis(300));
    let prepared_statement = session
        .prepare("INSERT INTO ks.t3 (a) VALUES (?)")
        .await
        .unwrap();

    // Try calculating tokens for different sizes of the key
    for i in 1..50usize {
        eprintln!("Trying key size {}", i);
        let mut s = String::new();
        for _ in 0..i {
            s.push('a');
        }
        let values = (&s,);
        let serialized_values = values.serialized().unwrap().into_owned();
        session.execute(&prepared_statement, &values).await.unwrap();

        let rs = session
            .query("SELECT token(a) FROM ks.t3 WHERE a = ?", &values)
            .await
            .unwrap()
            .rows
            .unwrap();
        let token: i64 = rs.first().unwrap().columns[0]
            .as_ref()
            .unwrap()
            .as_bigint()
            .unwrap();
        let expected_token = hash3_x64_128(
            &prepared_statement
                .compute_partition_key(&serialized_values)
                .unwrap(),
        ) as i64;
        assert_eq!(token, expected_token)
    }
}

#[tokio::test]
async fn test_use_keyspace() {
    let uri = std::env::var("SCYLLA_URI").unwrap_or_else(|_| "127.0.0.1:9042".to_string());
    let session = SessionBuilder::new()
        .known_node(&uri)
        .build()
        .await
        .unwrap();

    session.query("CREATE KEYSPACE IF NOT EXISTS use_ks_test WITH REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor' : 1}", &[]).await.unwrap();

    session
        .query("DROP TABLE IF EXISTS use_ks_test.tab", &[])
        .await
        .unwrap();

    session
        .query(
            "CREATE TABLE IF NOT EXISTS use_ks_test.tab (a text primary key)",
            &[],
        )
        .await
        .unwrap();

    session
        .query("INSERT INTO use_ks_test.tab (a) VALUES ('test1')", &[])
        .await
        .unwrap();

    session.use_keyspace("use_ks_test", false).await.unwrap();

    session
        .query("INSERT INTO tab (a) VALUES ('test2')", &[])
        .await
        .unwrap();

    let mut rows: Vec<String> = session
        .query("SELECT * FROM tab", &[])
        .await
        .unwrap()
        .rows
        .unwrap()
        .into_typed::<(String,)>()
        .map(|res| res.unwrap().0)
        .collect();

    rows.sort();

    assert_eq!(rows, vec!["test1".to_string(), "test2".to_string()]);

    // Test that trying to use nonexisting keyspace fails
    assert!(session
        .use_keyspace("this_keyspace_does_not_exist_at_all", false)
        .await
        .is_err());

    // Test that invalid keyspaces get rejected
    assert!(matches!(
        session.use_keyspace("", false).await,
        Err(QueryError::BadQuery(BadQuery::BadKeyspaceName(
            BadKeyspaceName::Empty
        )))
    ));

    let long_name: String = vec!['a'; 49].iter().collect();
    assert!(matches!(
        session.use_keyspace(long_name, false).await,
        Err(QueryError::BadQuery(BadQuery::BadKeyspaceName(
            BadKeyspaceName::TooLong(_, _)
        )))
    ));

    assert!(matches!(
        session.use_keyspace("abcd;dfdsf", false).await,
        Err(QueryError::BadQuery(BadQuery::BadKeyspaceName(
            BadKeyspaceName::IllegalCharacter(_, ';')
        )))
    ));

    // Make sure that use_keyspace on SessionBuiler works
    let session2: Session = SessionBuilder::new()
        .known_node(uri)
        .use_keyspace("use_ks_test", false)
        .build()
        .await
        .unwrap();

    let mut rows2: Vec<String> = session2
        .query("SELECT * FROM tab", &[])
        .await
        .unwrap()
        .rows
        .unwrap()
        .into_typed::<(String,)>()
        .map(|res| res.unwrap().0)
        .collect();

    rows2.sort();

    assert_eq!(rows2, vec!["test1".to_string(), "test2".to_string()]);
}

#[tokio::test]
async fn test_use_keyspace_case_sensitivity() {
    let uri = std::env::var("SCYLLA_URI").unwrap_or_else(|_| "127.0.0.1:9042".to_string());
    let session = SessionBuilder::new()
        .known_node(&uri)
        .build()
        .await
        .unwrap();

    session.query("CREATE KEYSPACE IF NOT EXISTS \"ks_case_test\" WITH REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor' : 1}", &[]).await.unwrap();
    session.query("CREATE KEYSPACE IF NOT EXISTS \"KS_CASE_TEST\" WITH REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor' : 1}", &[]).await.unwrap();

    session
        .query("DROP TABLE IF EXISTS ks_case_test.tab", &[])
        .await
        .unwrap();

    session
        .query("DROP TABLE IF EXISTS \"KS_CASE_TEST\".tab", &[])
        .await
        .unwrap();

    session
        .query("CREATE TABLE ks_case_test.tab (a text primary key)", &[])
        .await
        .unwrap();

    session
        .query(
            "CREATE TABLE \"KS_CASE_TEST\".tab (a text primary key)",
            &[],
        )
        .await
        .unwrap();

    session
        .query("INSERT INTO ks_case_test.tab (a) VALUES ('lowercase')", &[])
        .await
        .unwrap();

    session
        .query(
            "INSERT INTO \"KS_CASE_TEST\".tab (a) VALUES ('uppercase')",
            &[],
        )
        .await
        .unwrap();

    // Use uppercase keyspace without case sesitivity
    // Should select the lowercase one
    session.use_keyspace("KS_CASE_TEST", false).await.unwrap();

    let rows: Vec<String> = session
        .query("SELECT * from tab", &[])
        .await
        .unwrap()
        .rows
        .unwrap()
        .into_typed::<(String,)>()
        .map(|row| row.unwrap().0)
        .collect();

    assert_eq!(rows, vec!["lowercase".to_string()]);

    // Use uppercase keyspace with case sesitivity
    // Should select the uppercase one
    session.use_keyspace("KS_CASE_TEST", true).await.unwrap();

    let rows: Vec<String> = session
        .query("SELECT * from tab", &[])
        .await
        .unwrap()
        .rows
        .unwrap()
        .into_typed::<(String,)>()
        .map(|row| row.unwrap().0)
        .collect();

    assert_eq!(rows, vec!["uppercase".to_string()]);
}

#[tokio::test]
async fn test_raw_use_keyspace() {
    let uri = std::env::var("SCYLLA_URI").unwrap_or_else(|_| "127.0.0.1:9042".to_string());
    let session = SessionBuilder::new()
        .known_node(&uri)
        .build()
        .await
        .unwrap();

    session.query("CREATE KEYSPACE IF NOT EXISTS raw_use_ks_test WITH REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor' : 1}", &[]).await.unwrap();

    session
        .query("DROP TABLE IF EXISTS raw_use_ks_test.tab", &[])
        .await
        .unwrap();

    session
        .query(
            "CREATE TABLE IF NOT EXISTS raw_use_ks_test.tab (a text primary key)",
            &[],
        )
        .await
        .unwrap();

    session
        .query(
            "INSERT INTO raw_use_ks_test.tab (a) VALUES ('raw_test')",
            &[],
        )
        .await
        .unwrap();

    session
        .query("use    \"raw_use_ks_test\"    ;", &[])
        .await
        .unwrap();

    let rows: Vec<String> = session
        .query("SELECT * FROM tab", &[])
        .await
        .unwrap()
        .rows
        .unwrap()
        .into_typed::<(String,)>()
        .map(|res| res.unwrap().0)
        .collect();

    assert_eq!(rows, vec!["raw_test".to_string()]);

    // Check if case sensitivity is correctly detected
    assert!(session
        .query("use    \"RAW_USE_KS_TEST\"    ;", &[])
        .await
        .is_err());

    assert!(session
        .query("use    RAW_USE_KS_TEST    ;", &[])
        .await
        .is_ok());
}

#[tokio::test]
async fn test_fetch_system_keyspace() {
    let uri = std::env::var("SCYLLA_URI").unwrap_or_else(|_| "127.0.0.1:9042".to_string());
    let session = SessionBuilder::new().known_node(uri).build().await.unwrap();

    let prepared_statement = session
        .prepare("SELECT * FROM system_schema.keyspaces")
        .await
        .unwrap();

    session.execute(&prepared_statement, &[]).await.unwrap();
}

// Test that some Database Errors are parsed correctly
#[tokio::test]
async fn test_db_errors() {
    let uri = std::env::var("SCYLLA_URI").unwrap_or_else(|_| "127.0.0.1:9042".to_string());
    let session = SessionBuilder::new().known_node(uri).build().await.unwrap();

    // SyntaxError on bad query
    assert!(matches!(
        session.query("gibberish", &[]).await,
        Err(QueryError::DbError(DbError::SyntaxError, _))
    ));

    // AlreadyExists when creating a keyspace for the second time
    session.query("CREATE KEYSPACE IF NOT EXISTS db_errors_ks WITH REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor' : 1}", &[]).await.unwrap();

    let create_keyspace_res = session.query("CREATE KEYSPACE db_errors_ks WITH REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor' : 1}", &[]).await;
    let keyspace_exists_error: DbError = match create_keyspace_res {
        Err(QueryError::DbError(e, _)) => e,
        _ => panic!("Second CREATE KEYSPACE didn't return an error!"),
    };

    assert_eq!(
        keyspace_exists_error,
        DbError::AlreadyExists {
            keyspace: "db_errors_ks".to_string(),
            table: "".to_string()
        }
    );

    // AlreadyExists when creating a table for the second time
    session
        .query(
            "CREATE TABLE IF NOT EXISTS db_errors_ks.tab (a text primary key)",
            &[],
        )
        .await
        .unwrap();

    let create_table_res = session
        .query("CREATE TABLE db_errors_ks.tab (a text primary key)", &[])
        .await;
    let create_tab_error: DbError = match create_table_res {
        Err(QueryError::DbError(e, _)) => e,
        _ => panic!("Second CREATE TABLE didn't return an error!"),
    };

    assert_eq!(
        create_tab_error,
        DbError::AlreadyExists {
            keyspace: "db_errors_ks".to_string(),
            table: "tab".to_string()
        }
    );
}

#[tokio::test]
async fn test_tracing() {
    let uri = std::env::var("SCYLLA_URI").unwrap_or_else(|_| "127.0.0.1:9042".to_string());
    let session = SessionBuilder::new().known_node(uri).build().await.unwrap();

    session.query("CREATE KEYSPACE IF NOT EXISTS test_tracing_ks WITH REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor' : 1}", &[]).await.unwrap();

    session
        .query(
            "CREATE TABLE IF NOT EXISTS test_tracing_ks.tab (a text primary key)",
            &[],
        )
        .await
        .unwrap();

    test_tracing_query(&session).await;
    test_tracing_execute(&session).await;
}

async fn test_tracing_query(session: &Session) {
    // A query without tracing enabled has no tracing uuid in result
    let untraced_query: Query = Query::new("SELECT * FROM test_tracing_ks.tab".to_string());
    let untraced_query_result: QueryResult = session.query(untraced_query, &[]).await.unwrap();

    assert!(untraced_query_result.tracing_id.is_none());

    // A query with tracing enabled has a tracing uuid in result
    let mut traced_query: Query = Query::new("SELECT * FROM test_tracing_ks.tab".to_string());
    traced_query.tracing = true;

    let traced_query_result: QueryResult = session.query(traced_query, &[]).await.unwrap();
    assert!(traced_query_result.tracing_id.is_some());

    // Querying this uuid from tracing table gives some results
    assert_in_tracing_table(session, traced_query_result.tracing_id.unwrap()).await;
}

async fn test_tracing_execute(session: &Session) {
    // Executing a prepared statement without tracing enabled has no tracing uuid in result
    let untraced_prepared = session
        .prepare("SELECT * FROM test_tracing_ks.tab")
        .await
        .unwrap();

    let untraced_prepared_result: QueryResult =
        session.execute(&untraced_prepared, &[]).await.unwrap();

    assert!(untraced_prepared_result.tracing_id.is_none());

    // Executing a prepared statement with tracing enabled has a tracing uuid in result
    let mut traced_prepared = session
        .prepare("SELECT * FROM test_tracing_ks.tab")
        .await
        .unwrap();

    traced_prepared.tracing = true;

    let traced_prepared_result: QueryResult = session.execute(&traced_prepared, &[]).await.unwrap();
    assert!(traced_prepared_result.tracing_id.is_some());

    // Querying this uuid from tracing table gives some results
    assert_in_tracing_table(session, traced_prepared_result.tracing_id.unwrap()).await;
}

async fn assert_in_tracing_table(session: &Session, tracing_uuid: Uuid) {
    let mut traces_query =
        Query::new("SELECT * FROM system_traces.sessions WHERE session_id = ?".to_string());
    traces_query.consistency = Consistency::One;

    // Tracing info might not be immediately available
    // If rows are empty perform 8 retries with a 32ms wait in between

    for _ in 0..8 {
        let row_opt = session
            .query(traces_query.clone(), (tracing_uuid,))
            .await
            .unwrap()
            .rows
            .into_iter()
            .next();

        if row_opt.is_some() {
            // Ok there was some row for this tracing_uuid
            return;
        }

        // Otherwise retry
        tokio::time::sleep(std::time::Duration::from_millis(32)).await;
    }

    // If all retries failed panic with an error
    panic!("No rows for tracing with this session id!");
}
