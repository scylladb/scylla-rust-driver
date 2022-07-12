use crate as scylla;
use crate::batch::Batch;
use crate::frame::response::result::Row;
use crate::frame::value::ValueList;
use crate::query::Query;
use crate::routing::Token;
use crate::statement::Consistency;
use crate::tracing::TracingInfo;
use crate::transport::connection::BatchResult;
use crate::transport::errors::{BadKeyspaceName, BadQuery, DbError, QueryError};
use crate::transport::partitioner::{Murmur3Partitioner, Partitioner, PartitionerName};
use crate::transport::topology::Strategy::SimpleStrategy;
use crate::transport::topology::{CollectionType, ColumnKind, CqlType, NativeType};
use crate::QueryResult;
use crate::{IntoTypedRows, Session, SessionBuilder};
use bytes::Bytes;
use futures::{FutureExt, StreamExt};
use itertools::Itertools;
use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::net::TcpListener;
use uuid::Uuid;

static UNIQUE_COUNTER: AtomicUsize = AtomicUsize::new(0);
pub(crate) fn unique_name() -> String {
    let cnt = UNIQUE_COUNTER.fetch_add(1, Ordering::SeqCst);
    let name = format!(
        "test_rust_{}_{}",
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs(),
        cnt
    );
    println!("Unique name: {}", name);
    name
}

#[tokio::test]
async fn test_connection_failure() {
    // Make sure that Session::create fails when the control connection
    // fails to connect.

    // Create a dummy server which immediately closes the connection.
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let (fut, _handle) = async move {
        loop {
            let _ = listener.accept().await;
        }
    }
    .remote_handle();
    tokio::spawn(fut);

    let res = SessionBuilder::new().known_node_addr(addr).build().await;
    match res {
        Ok(_) => panic!("Unexpected success"),
        Err(err) => println!("Connection error (it was expected): {:?}", err),
    }
}

#[tokio::test]
async fn test_unprepared_statement() {
    let uri = std::env::var("SCYLLA_URI").unwrap_or_else(|_| "127.0.0.1:9042".to_string());
    let session = SessionBuilder::new().known_node(uri).build().await.unwrap();
    let ks = unique_name();

    session.query(format!("CREATE KEYSPACE IF NOT EXISTS {} WITH REPLICATION = {{'class' : 'SimpleStrategy', 'replication_factor' : 1}}", ks), &[]).await.unwrap();
    session
        .query(
            format!(
                "CREATE TABLE IF NOT EXISTS {}.t (a int, b int, c text, primary key (a, b))",
                ks
            ),
            &[],
        )
        .await
        .unwrap();

    session
        .query(
            format!("INSERT INTO {}.t (a, b, c) VALUES (1, 2, 'abc')", ks),
            &[],
        )
        .await
        .unwrap();
    session
        .query(
            format!("INSERT INTO {}.t (a, b, c) VALUES (7, 11, '')", ks),
            &[],
        )
        .await
        .unwrap();
    session
        .query(
            format!("INSERT INTO {}.t (a, b, c) VALUES (1, 4, 'hello')", ks),
            &[],
        )
        .await
        .unwrap();

    let query_result = session
        .query(format!("SELECT a, b, c FROM {}.t", ks), &[])
        .await
        .unwrap();

    let (a_idx, _) = query_result.get_column_spec("a").unwrap();
    let (b_idx, _) = query_result.get_column_spec("b").unwrap();
    let (c_idx, _) = query_result.get_column_spec("c").unwrap();
    assert!(query_result.get_column_spec("d").is_none());

    let rs = query_result.rows.unwrap();

    let mut results: Vec<(i32, i32, &String)> = rs
        .iter()
        .map(|r| {
            let a = r.columns[a_idx].as_ref().unwrap().as_int().unwrap();
            let b = r.columns[b_idx].as_ref().unwrap().as_int().unwrap();
            let c = r.columns[c_idx].as_ref().unwrap().as_text().unwrap();
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
    let query_result = session
        .query_iter(format!("SELECT a, b, c FROM {}.t", ks), &[])
        .await
        .unwrap();
    let specs = query_result.get_column_specs();
    assert_eq!(specs.len(), 3);
    for (spec, name) in specs.iter().zip(["a", "b", "c"]) {
        assert_eq!(spec.name, name); // Check column name.
        assert_eq!(spec.table_spec.ks_name, ks);
    }
    let mut results_from_manual_paging: Vec<Row> = vec![];
    let query = Query::new(format!("SELECT a, b, c FROM {}.t", ks)).with_page_size(1);
    let mut paging_state: Option<Bytes> = None;
    let mut watchdog = 0;
    loop {
        let rs_manual = session
            .query_paged(query.clone(), &[], paging_state)
            .await
            .unwrap();
        results_from_manual_paging.append(&mut rs_manual.rows.unwrap());
        if watchdog > 30 || rs_manual.paging_state == None {
            break;
        }
        watchdog += 1;
        paging_state = rs_manual.paging_state;
    }
    assert_eq!(results_from_manual_paging, rs);
}

#[tokio::test]
async fn test_prepared_statement() {
    let uri = std::env::var("SCYLLA_URI").unwrap_or_else(|_| "127.0.0.1:9042".to_string());
    let session = SessionBuilder::new().known_node(uri).build().await.unwrap();
    let ks = unique_name();

    session.query(format!("CREATE KEYSPACE IF NOT EXISTS {} WITH REPLICATION = {{'class' : 'SimpleStrategy', 'replication_factor' : 1}}", ks), &[]).await.unwrap();
    session
        .query(
            format!(
                "CREATE TABLE IF NOT EXISTS {}.t2 (a int, b int, c text, primary key (a, b))",
                ks
            ),
            &[],
        )
        .await
        .unwrap();
    session
        .query(format!("CREATE TABLE IF NOT EXISTS {}.complex_pk (a int, b int, c text, d int, e int, primary key ((a,b,c),d))", ks), &[])
        .await
        .unwrap();

    let prepared_statement = session
        .prepare(format!("SELECT a, b, c FROM {}.t2", ks))
        .await
        .unwrap();
    let query_result = session.execute_iter(prepared_statement, &[]).await.unwrap();
    let specs = query_result.get_column_specs();
    assert_eq!(specs.len(), 3);
    for (spec, name) in specs.iter().zip(["a", "b", "c"]) {
        assert_eq!(spec.name, name); // Check column name.
        assert_eq!(spec.table_spec.ks_name, ks);
    }

    let prepared_statement = session
        .prepare(format!("INSERT INTO {}.t2 (a, b, c) VALUES (?, ?, ?)", ks))
        .await
        .unwrap();
    let prepared_complex_pk_statement = session
        .prepare(format!(
            "INSERT INTO {}.complex_pk (a, b, c, d) VALUES (?, ?, ?, 7)",
            ks
        ))
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
            .query(format!("SELECT token(a) FROM {}.t2", ks), &[])
            .await
            .unwrap()
            .rows
            .unwrap();
        let token = Token {
            value: rs.first().unwrap().columns[0]
                .as_ref()
                .unwrap()
                .as_bigint()
                .unwrap(),
        };
        let expected_token = Murmur3Partitioner::hash(
            prepared_statement
                .compute_partition_key(&serialized_values)
                .unwrap(),
        );

        assert_eq!(token, expected_token)
    }
    {
        let rs = session
            .query(format!("SELECT token(a,b,c) FROM {}.complex_pk", ks), &[])
            .await
            .unwrap()
            .rows
            .unwrap();
        let token = Token {
            value: rs.first().unwrap().columns[0]
                .as_ref()
                .unwrap()
                .as_bigint()
                .unwrap(),
        };
        let expected_token = Murmur3Partitioner::hash(
            prepared_complex_pk_statement
                .compute_partition_key(&serialized_values)
                .unwrap(),
        );

        assert_eq!(token, expected_token)
    }

    // Verify that correct data was insertd
    {
        let rs = session
            .query(format!("SELECT a,b,c FROM {}.t2", ks), &[])
            .await
            .unwrap()
            .rows
            .unwrap();
        let r = rs.first().unwrap();
        let a = r.columns[0].as_ref().unwrap().as_int().unwrap();
        let b = r.columns[1].as_ref().unwrap().as_int().unwrap();
        let c = r.columns[2].as_ref().unwrap().as_text().unwrap();
        assert_eq!((a, b, c), (17, 16, &String::from("I'm prepared!!!")));

        let mut results_from_manual_paging: Vec<Row> = vec![];
        let query = Query::new(format!("SELECT a, b, c FROM {}.t2", ks)).with_page_size(1);
        let prepared_paged = session.prepare(query).await.unwrap();
        let mut paging_state: Option<Bytes> = None;
        let mut watchdog = 0;
        loop {
            let rs_manual = session
                .execute_paged(&prepared_paged, &[], paging_state)
                .await
                .unwrap();
            results_from_manual_paging.append(&mut rs_manual.rows.unwrap());
            if watchdog > 30 || rs_manual.paging_state == None {
                break;
            }
            watchdog += 1;
            paging_state = rs_manual.paging_state;
        }
        assert_eq!(results_from_manual_paging, rs);
    }
    {
        let rs = session
            .query(format!("SELECT a,b,c,d,e FROM {}.complex_pk", ks), &[])
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
    // Check that ValueList macro works
    {
        #[derive(scylla::ValueList, scylla::FromRow, PartialEq, Debug, Clone)]
        struct ComplexPk {
            a: i32,
            b: i32,
            c: Option<String>,
            d: i32,
            e: i32,
        }
        let input: ComplexPk = ComplexPk {
            a: 9,
            b: 8,
            c: Some("seven".into()),
            d: 6,
            e: 5,
        };
        session
            .query(
                format!(
                    "INSERT INTO {}.complex_pk (a,b,c,d,e) VALUES (?,?,?,?,?)",
                    ks
                ),
                input.clone(),
            )
            .await
            .unwrap();
        let mut rs = session
            .query(
                format!(
                    "SELECT * FROM {}.complex_pk WHERE a = 9 and b = 8 and c = 'seven'",
                    ks
                ),
                &[],
            )
            .await
            .unwrap()
            .rows
            .unwrap()
            .into_typed::<ComplexPk>();
        let output = rs.next().unwrap().unwrap();
        assert_eq!(input, output)
    }
}

#[tokio::test]
async fn test_batch() {
    let uri = std::env::var("SCYLLA_URI").unwrap_or_else(|_| "127.0.0.1:9042".to_string());
    let session = SessionBuilder::new().known_node(uri).build().await.unwrap();
    let ks = unique_name();

    session.query(format!("CREATE KEYSPACE IF NOT EXISTS {} WITH REPLICATION = {{'class' : 'SimpleStrategy', 'replication_factor' : 1}}", ks), &[]).await.unwrap();
    session
        .query(
            format!(
                "CREATE TABLE IF NOT EXISTS {}.t_batch (a int, b int, c text, primary key (a, b))",
                ks
            ),
            &[],
        )
        .await
        .unwrap();

    let prepared_statement = session
        .prepare(format!(
            "INSERT INTO {}.t_batch (a, b, c) VALUES (?, ?, ?)",
            ks
        ))
        .await
        .unwrap();

    // TODO: Add API, that supports binding values to statements in batch creation process,
    // to avoid problem of statements/values count mismatch
    use crate::batch::Batch;
    let mut batch: Batch = Default::default();
    batch.append_statement(&format!("INSERT INTO {}.t_batch (a, b, c) VALUES (?, ?, ?)", ks)[..]);
    batch.append_statement(&format!("INSERT INTO {}.t_batch (a, b, c) VALUES (7, 11, '')", ks)[..]);
    batch.append_statement(prepared_statement.clone());

    let values = ((1_i32, 2_i32, "abc"), (), (1_i32, 4_i32, "hello"));

    session.batch(&batch, values).await.unwrap();

    let rs = session
        .query(format!("SELECT a, b, c FROM {}.t_batch", ks), &[])
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

    // Test repreparing statement inside a batch
    let mut batch: Batch = Default::default();
    batch.append_statement(prepared_statement);
    let values = ((4_i32, 20_i32, "foobar"),);

    // This statement flushes the prepared statement cache
    session
        .query(
            format!("ALTER TABLE {}.t_batch WITH gc_grace_seconds = 42", ks),
            &[],
        )
        .await
        .unwrap();
    session.batch(&batch, values).await.unwrap();

    let rs = session
        .query(
            format!("SELECT a, b, c FROM {}.t_batch WHERE a = 4", ks),
            &[],
        )
        .await
        .unwrap()
        .rows
        .unwrap();
    let results: Vec<(i32, i32, &String)> = rs
        .iter()
        .map(|r| {
            let a = r.columns[0].as_ref().unwrap().as_int().unwrap();
            let b = r.columns[1].as_ref().unwrap().as_int().unwrap();
            let c = r.columns[2].as_ref().unwrap().as_text().unwrap();
            (a, b, c)
        })
        .collect();

    assert_eq!(results, vec![(4, 20, &String::from("foobar"))]);
}

#[tokio::test]
async fn test_token_calculation() {
    let uri = std::env::var("SCYLLA_URI").unwrap_or_else(|_| "127.0.0.1:9042".to_string());
    let session = SessionBuilder::new().known_node(uri).build().await.unwrap();
    let ks = unique_name();

    session.query(format!("CREATE KEYSPACE IF NOT EXISTS {} WITH REPLICATION = {{'class' : 'SimpleStrategy', 'replication_factor' : 1}}", ks), &[]).await.unwrap();
    session
        .query(
            format!("CREATE TABLE IF NOT EXISTS {}.t3 (a text primary key)", ks),
            &[],
        )
        .await
        .unwrap();

    let prepared_statement = session
        .prepare(format!("INSERT INTO {}.t3 (a) VALUES (?)", ks))
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
            .query(
                format!("SELECT token(a) FROM {}.t3 WHERE a = ?", ks),
                &values,
            )
            .await
            .unwrap()
            .rows
            .unwrap();
        let token = Token {
            value: rs.first().unwrap().columns[0]
                .as_ref()
                .unwrap()
                .as_bigint()
                .unwrap(),
        };
        let expected_token = Murmur3Partitioner::hash(
            prepared_statement
                .compute_partition_key(&serialized_values)
                .unwrap(),
        );
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
    let ks = unique_name();

    session.query(format!("CREATE KEYSPACE IF NOT EXISTS {} WITH REPLICATION = {{'class' : 'SimpleStrategy', 'replication_factor' : 1}}", ks), &[]).await.unwrap();

    session
        .query(
            format!("CREATE TABLE IF NOT EXISTS {}.tab (a text primary key)", ks),
            &[],
        )
        .await
        .unwrap();

    session
        .query(format!("INSERT INTO {}.tab (a) VALUES ('test1')", ks), &[])
        .await
        .unwrap();

    session.use_keyspace(ks.clone(), false).await.unwrap();

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
        .use_keyspace(ks.clone(), false)
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
    let ks_lower = unique_name().to_lowercase();
    let ks_upper = ks_lower.to_uppercase();

    session.query(format!("CREATE KEYSPACE IF NOT EXISTS \"{}\" WITH REPLICATION = {{'class' : 'SimpleStrategy', 'replication_factor' : 1}}", ks_lower), &[]).await.unwrap();
    session.query(format!("CREATE KEYSPACE IF NOT EXISTS \"{}\" WITH REPLICATION = {{'class' : 'SimpleStrategy', 'replication_factor' : 1}}", ks_upper), &[]).await.unwrap();

    session
        .query(
            format!("CREATE TABLE {}.tab (a text primary key)", ks_lower),
            &[],
        )
        .await
        .unwrap();

    session
        .query(
            format!("CREATE TABLE \"{}\".tab (a text primary key)", ks_upper),
            &[],
        )
        .await
        .unwrap();

    session
        .query(
            format!("INSERT INTO {}.tab (a) VALUES ('lowercase')", ks_lower),
            &[],
        )
        .await
        .unwrap();

    session
        .query(
            format!("INSERT INTO \"{}\".tab (a) VALUES ('uppercase')", ks_upper),
            &[],
        )
        .await
        .unwrap();

    // Use uppercase keyspace without case sesitivity
    // Should select the lowercase one
    session.use_keyspace(ks_upper.clone(), false).await.unwrap();

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
    session.use_keyspace(ks_upper, true).await.unwrap();

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
    let ks = unique_name();

    session.query(format!("CREATE KEYSPACE IF NOT EXISTS {} WITH REPLICATION = {{'class' : 'SimpleStrategy', 'replication_factor' : 1}}", ks), &[]).await.unwrap();

    session
        .query(
            format!("CREATE TABLE IF NOT EXISTS {}.tab (a text primary key)", ks),
            &[],
        )
        .await
        .unwrap();

    session
        .query(
            format!("INSERT INTO {}.tab (a) VALUES ('raw_test')", ks),
            &[],
        )
        .await
        .unwrap();

    session
        .query(format!("use    \"{}\"    ;", ks), &[])
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
        .query(format!("use    \"{}\"    ;", ks.to_uppercase()), &[])
        .await
        .is_err());

    assert!(session
        .query(format!("use    {}    ;", ks.to_uppercase()), &[])
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
    let ks = unique_name();

    // SyntaxError on bad query
    assert!(matches!(
        session.query("gibberish", &[]).await,
        Err(QueryError::DbError(DbError::SyntaxError, _))
    ));

    // AlreadyExists when creating a keyspace for the second time
    session.query(format!("CREATE KEYSPACE IF NOT EXISTS {} WITH REPLICATION = {{'class' : 'SimpleStrategy', 'replication_factor' : 1}}", ks), &[]).await.unwrap();

    let create_keyspace_res = session.query(format!("CREATE KEYSPACE {} WITH REPLICATION = {{'class' : 'SimpleStrategy', 'replication_factor' : 1}}", ks), &[]).await;
    let keyspace_exists_error: DbError = match create_keyspace_res {
        Err(QueryError::DbError(e, _)) => e,
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
        .query(
            format!("CREATE TABLE IF NOT EXISTS {}.tab (a text primary key)", ks),
            &[],
        )
        .await
        .unwrap();

    let create_table_res = session
        .query(format!("CREATE TABLE {}.tab (a text primary key)", ks), &[])
        .await;
    let create_tab_error: DbError = match create_table_res {
        Err(QueryError::DbError(e, _)) => e,
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
async fn test_tracing() {
    let uri = std::env::var("SCYLLA_URI").unwrap_or_else(|_| "127.0.0.1:9042".to_string());
    let session = SessionBuilder::new().known_node(uri).build().await.unwrap();
    let ks = unique_name();

    session.query(format!("CREATE KEYSPACE IF NOT EXISTS {} WITH REPLICATION = {{'class' : 'SimpleStrategy', 'replication_factor' : 1}}", ks), &[]).await.unwrap();

    session
        .query(
            format!("CREATE TABLE IF NOT EXISTS {}.tab (a text primary key)", ks),
            &[],
        )
        .await
        .unwrap();

    test_tracing_query(&session, ks.clone()).await;
    test_tracing_execute(&session, ks.clone()).await;
    test_tracing_prepare(&session, ks.clone()).await;
    test_get_tracing_info(&session, ks.clone()).await;
    test_tracing_query_iter(&session, ks.clone()).await;
    test_tracing_execute_iter(&session, ks.clone()).await;
    test_tracing_batch(&session, ks.clone()).await;
}

async fn test_tracing_query(session: &Session, ks: String) {
    // A query without tracing enabled has no tracing uuid in result
    let untraced_query: Query = Query::new(format!("SELECT * FROM {}.tab", ks));
    let untraced_query_result: QueryResult = session.query(untraced_query, &[]).await.unwrap();

    assert!(untraced_query_result.tracing_id.is_none());

    // A query with tracing enabled has a tracing uuid in result
    let mut traced_query: Query = Query::new(format!("SELECT * FROM {}.tab", ks));
    traced_query.config.tracing = true;

    let traced_query_result: QueryResult = session.query(traced_query, &[]).await.unwrap();
    assert!(traced_query_result.tracing_id.is_some());

    // Querying this uuid from tracing table gives some results
    assert_in_tracing_table(session, traced_query_result.tracing_id.unwrap()).await;
}

async fn test_tracing_execute(session: &Session, ks: String) {
    // Executing a prepared statement without tracing enabled has no tracing uuid in result
    let untraced_prepared = session
        .prepare(format!("SELECT * FROM {}.tab", ks))
        .await
        .unwrap();

    let untraced_prepared_result: QueryResult =
        session.execute(&untraced_prepared, &[]).await.unwrap();

    assert!(untraced_prepared_result.tracing_id.is_none());

    // Executing a prepared statement with tracing enabled has a tracing uuid in result
    let mut traced_prepared = session
        .prepare(format!("SELECT * FROM {}.tab", ks))
        .await
        .unwrap();

    traced_prepared.config.tracing = true;

    let traced_prepared_result: QueryResult = session.execute(&traced_prepared, &[]).await.unwrap();
    assert!(traced_prepared_result.tracing_id.is_some());

    // Querying this uuid from tracing table gives some results
    assert_in_tracing_table(session, traced_prepared_result.tracing_id.unwrap()).await;
}

async fn test_tracing_prepare(session: &Session, ks: String) {
    // Preparing a statement without tracing enabled has no tracing uuids in result
    let untraced_prepared = session
        .prepare(format!("SELECT * FROM {}.tab", ks))
        .await
        .unwrap();

    assert!(untraced_prepared.prepare_tracing_ids.is_empty());

    // Preparing a statement with tracing enabled has tracing uuids in result
    let mut to_prepare_traced = Query::new(format!("SELECT * FROM {}.tab", ks));
    to_prepare_traced.config.tracing = true;

    let traced_prepared = session.prepare(to_prepare_traced).await.unwrap();
    assert!(!traced_prepared.prepare_tracing_ids.is_empty());

    // Querying this uuid from tracing table gives some results
    for tracing_id in traced_prepared.prepare_tracing_ids {
        assert_in_tracing_table(session, tracing_id).await;
    }
}

async fn test_get_tracing_info(session: &Session, ks: String) {
    // A query with tracing enabled has a tracing uuid in result
    let mut traced_query: Query = Query::new(format!("SELECT * FROM {}.tab", ks));
    traced_query.config.tracing = true;

    let traced_query_result: QueryResult = session.query(traced_query, &[]).await.unwrap();
    let tracing_id: Uuid = traced_query_result.tracing_id.unwrap();

    // Getting tracing info from session using this uuid works
    let tracing_info: TracingInfo = session.get_tracing_info(&tracing_id).await.unwrap();
    assert!(!tracing_info.events.is_empty());
}

async fn test_tracing_query_iter(session: &Session, ks: String) {
    // A query without tracing enabled has no tracing ids
    let untraced_query: Query = Query::new(format!("SELECT * FROM {}.tab", ks));

    let mut untraced_row_iter = session.query_iter(untraced_query, &[]).await.unwrap();
    while let Some(_row) = untraced_row_iter.next().await {
        // Receive rows
    }

    assert!(untraced_row_iter.get_tracing_ids().is_empty());

    // The same is true for TypedRowIter
    let untraced_typed_row_iter = untraced_row_iter.into_typed::<(i32,)>();
    assert!(untraced_typed_row_iter.get_tracing_ids().is_empty());

    // A query with tracing enabled has a tracing ids in result
    let mut traced_query: Query = Query::new(format!("SELECT * FROM {}.tab", ks));
    traced_query.config.tracing = true;

    let mut traced_row_iter = session.query_iter(traced_query, &[]).await.unwrap();
    while let Some(_row) = traced_row_iter.next().await {
        // Receive rows
    }

    assert!(!traced_row_iter.get_tracing_ids().is_empty());

    // The same is true for TypedRowIter
    let traced_typed_row_iter = traced_row_iter.into_typed::<(i32,)>();
    assert!(!traced_typed_row_iter.get_tracing_ids().is_empty());

    for tracing_id in traced_typed_row_iter.get_tracing_ids() {
        assert_in_tracing_table(session, *tracing_id).await;
    }
}

async fn test_tracing_execute_iter(session: &Session, ks: String) {
    // A prepared statement without tracing enabled has no tracing ids
    let untraced_prepared = session
        .prepare(format!("SELECT * FROM {}.tab", ks))
        .await
        .unwrap();

    let mut untraced_row_iter = session.execute_iter(untraced_prepared, &[]).await.unwrap();
    while let Some(_row) = untraced_row_iter.next().await {
        // Receive rows
    }

    assert!(untraced_row_iter.get_tracing_ids().is_empty());

    // The same is true for TypedRowIter
    let untraced_typed_row_iter = untraced_row_iter.into_typed::<(i32,)>();
    assert!(untraced_typed_row_iter.get_tracing_ids().is_empty());

    // A prepared statement with tracing enabled has a tracing ids in result
    let mut traced_prepared = session
        .prepare(format!("SELECT * FROM {}.tab", ks))
        .await
        .unwrap();
    traced_prepared.config.tracing = true;

    let mut traced_row_iter = session.execute_iter(traced_prepared, &[]).await.unwrap();
    while let Some(_row) = traced_row_iter.next().await {
        // Receive rows
    }

    assert!(!traced_row_iter.get_tracing_ids().is_empty());

    // The same is true for TypedRowIter
    let traced_typed_row_iter = traced_row_iter.into_typed::<(i32,)>();
    assert!(!traced_typed_row_iter.get_tracing_ids().is_empty());

    for tracing_id in traced_typed_row_iter.get_tracing_ids() {
        assert_in_tracing_table(session, *tracing_id).await;
    }
}

async fn test_tracing_batch(session: &Session, ks: String) {
    // A batch without tracing enabled has no tracing id
    let mut untraced_batch: Batch = Default::default();
    untraced_batch.append_statement(&format!("INSERT INTO {}.tab (a) VALUES('a')", ks)[..]);

    let untraced_batch_result: BatchResult = session.batch(&untraced_batch, ((),)).await.unwrap();
    assert!(untraced_batch_result.tracing_id.is_none());

    // Batch with tracing enabled has a tracing uuid in result
    let mut traced_batch: Batch = Default::default();
    traced_batch.append_statement(&format!("INSERT INTO {}.tab (a) VALUES('a')", ks)[..]);
    traced_batch.config.tracing = true;

    let traced_batch_result: BatchResult = session.batch(&traced_batch, ((),)).await.unwrap();
    assert!(traced_batch_result.tracing_id.is_some());

    assert_in_tracing_table(session, traced_batch_result.tracing_id.unwrap()).await;
}

async fn assert_in_tracing_table(session: &Session, tracing_uuid: Uuid) {
    let mut traces_query = Query::new("SELECT * FROM system_traces.sessions WHERE session_id = ?");
    traces_query.config.consistency = Some(Consistency::One);

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

#[tokio::test]
async fn test_fetch_schema_version() {
    let uri = std::env::var("SCYLLA_URI").unwrap_or_else(|_| "127.0.0.1:9042".to_string());
    let session = SessionBuilder::new().known_node(uri).build().await.unwrap();
    session.fetch_schema_version().await.unwrap();
}

#[tokio::test]
async fn test_await_schema_agreement() {
    let uri = std::env::var("SCYLLA_URI").unwrap_or_else(|_| "127.0.0.1:9042".to_string());
    let session = SessionBuilder::new().known_node(uri).build().await.unwrap();
    session.await_schema_agreement().await.unwrap();
}

#[tokio::test]
async fn test_await_timed_schema_agreement() {
    use std::time::Duration;
    let uri = std::env::var("SCYLLA_URI").unwrap_or_else(|_| "127.0.0.1:9042".to_string());
    let session = SessionBuilder::new().known_node(uri).build().await.unwrap();
    session
        .await_timed_schema_agreement(Duration::from_millis(50))
        .await
        .unwrap();
}

#[tokio::test]
async fn test_timestamp() {
    let uri = std::env::var("SCYLLA_URI").unwrap_or_else(|_| "127.0.0.1:9042".to_string());
    let session = SessionBuilder::new().known_node(uri).build().await.unwrap();
    let ks = unique_name();

    session.query(format!("CREATE KEYSPACE IF NOT EXISTS {} WITH REPLICATION = {{'class' : 'SimpleStrategy', 'replication_factor' : 1}}", ks), &[]).await.unwrap();
    session
        .query(
            format!(
                "CREATE TABLE IF NOT EXISTS {}.t_timestamp (a text, b text, primary key (a))",
                ks
            ),
            &[],
        )
        .await
        .unwrap();

    session.await_schema_agreement().await.unwrap();

    let query_str = format!("INSERT INTO {}.t_timestamp (a, b) VALUES (?, ?)", ks);

    // test regular query timestamps

    let mut regular_query = Query::new(query_str.to_string());

    regular_query.set_timestamp(Some(420));
    session
        .query(regular_query.clone(), ("regular query", "higher timestamp"))
        .await
        .unwrap();

    regular_query.set_timestamp(Some(42));
    session
        .query(regular_query.clone(), ("regular query", "lower timestamp"))
        .await
        .unwrap();

    // test prepared statement timestamps

    let mut prepared_statement = session.prepare(query_str).await.unwrap();

    prepared_statement.set_timestamp(Some(420));
    session
        .execute(&prepared_statement, ("prepared query", "higher timestamp"))
        .await
        .unwrap();

    prepared_statement.set_timestamp(Some(42));
    session
        .execute(&prepared_statement, ("prepared query", "lower timestamp"))
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

    let mut results = session
        .query(
            format!("SELECT a, b, WRITETIME(b) FROM {}.t_timestamp", ks),
            &[],
        )
        .await
        .unwrap()
        .rows
        .unwrap()
        .into_typed::<(String, String, i64)>()
        .map(Result::unwrap)
        .collect::<Vec<_>>();
    results.sort();

    let expected_results = [
        ("first query in batch", "higher timestamp", 420),
        ("prepared query", "higher timestamp", 420),
        ("regular query", "higher timestamp", 420),
        ("second query in batch", "higher timestamp", 420),
    ]
    .iter()
    .map(|(x, y, t)| (x.to_string(), y.to_string(), *t))
    .collect::<Vec<_>>();

    assert_eq!(results, expected_results);
}

#[tokio::test]
async fn test_prepared_config() {
    let uri = std::env::var("SCYLLA_URI").unwrap_or_else(|_| "127.0.0.1:9042".to_string());
    let session = SessionBuilder::new().known_node(uri).build().await.unwrap();

    let mut query = Query::new("SELECT * FROM system_schema.tables");
    query.set_is_idempotent(true);
    query.set_page_size(42);

    let prepared_statement = session.prepare(query).await.unwrap();

    assert!(prepared_statement.get_is_idempotent());
    assert_eq!(prepared_statement.get_page_size(), Some(42));
}

#[tokio::test]
async fn test_schema_types_in_metadata() {
    let uri = std::env::var("SCYLLA_URI").unwrap_or_else(|_| "127.0.0.1:9042".to_string());
    let session = SessionBuilder::new().known_node(uri).build().await.unwrap();
    let ks = unique_name();

    session
        .query(format!("CREATE KEYSPACE {} WITH REPLICATION = {{'class' : 'SimpleStrategy', 'replication_factor' : 1}}", ks), &[])
        .await
        .unwrap();

    session.query(format!("USE {}", ks), &[]).await.unwrap();

    session
        .query(
            "CREATE TYPE IF NOT EXISTS type_a (
                    a map<frozen<list<int>>, text>,
                    b frozen<map<frozen<list<int>>, frozen<set<text>>>>
                   )",
            &[],
        )
        .await
        .unwrap();

    session
        .query("CREATE TYPE IF NOT EXISTS type_b (a int, b text)", &[])
        .await
        .unwrap();

    session
        .query(
            "CREATE TYPE IF NOT EXISTS type_c (a map<frozen<set<text>>, frozen<type_b>>)",
            &[],
        )
        .await
        .unwrap();

    session
        .query(
            "CREATE TABLE IF NOT EXISTS table_a (
                    a frozen<type_a> PRIMARY KEY,
                    b type_b,
                    c frozen<type_c>,
                    d map<text, frozen<list<int>>>,
                    e tuple<int, text>
                  )",
            &[],
        )
        .await
        .unwrap();

    session
        .query(
            "CREATE TABLE IF NOT EXISTS table_b (
                        a text PRIMARY KEY,
                        b frozen<map<int, int>>
                     )",
            &[],
        )
        .await
        .unwrap();

    session.await_schema_agreement().await.unwrap();
    session.refresh_metadata().await.unwrap();

    let cluster_data = session.get_cluster_data();
    let tables = &cluster_data.get_keyspace_info()[&ks].tables;

    assert_eq!(
        tables.keys().sorted().collect::<Vec<_>>(),
        vec!["table_a", "table_b"]
    );

    let table_a_columns = &tables["table_a"].columns;

    assert_eq!(
        table_a_columns.keys().sorted().collect::<Vec<_>>(),
        vec!["a", "b", "c", "d", "e"]
    );

    let a = &table_a_columns["a"];

    assert_eq!(
        a.type_,
        CqlType::UserDefinedType {
            name: "type_a".to_string(),
            frozen: true
        }
    );

    let b = &table_a_columns["b"];

    assert_eq!(
        b.type_,
        CqlType::UserDefinedType {
            name: "type_b".to_string(),
            frozen: false,
        }
    );

    let c = &table_a_columns["c"];

    assert_eq!(
        c.type_,
        CqlType::UserDefinedType {
            name: "type_c".to_string(),
            frozen: true
        }
    );

    let d = &table_a_columns["d"];

    assert_eq!(
        d.type_,
        CqlType::Collection {
            type_: CollectionType::Map(
                Box::new(CqlType::Native(NativeType::Text)),
                Box::new(CqlType::Collection {
                    type_: CollectionType::List(Box::new(CqlType::Native(NativeType::Int))),
                    frozen: true
                })
            ),
            frozen: false
        }
    );

    let e = &table_a_columns["e"];

    assert_eq!(
        e.type_,
        CqlType::Tuple(vec![
            CqlType::Native(NativeType::Int),
            CqlType::Native(NativeType::Text)
        ])
    );

    let table_b_columns = &tables["table_b"].columns;

    let a = &table_b_columns["a"];

    assert_eq!(a.type_, CqlType::Native(NativeType::Text));

    let b = &table_b_columns["b"];

    assert_eq!(
        b.type_,
        CqlType::Collection {
            type_: CollectionType::Map(
                Box::new(CqlType::Native(NativeType::Int),),
                Box::new(CqlType::Native(NativeType::Int),)
            ),
            frozen: true
        }
    );
}

#[tokio::test]
async fn test_user_defined_types_in_metadata() {
    let uri = std::env::var("SCYLLA_URI").unwrap_or_else(|_| "127.0.0.1:9042".to_string());
    let session = SessionBuilder::new().known_node(uri).build().await.unwrap();
    let ks = unique_name();

    session
        .query(format!("CREATE KEYSPACE {} WITH REPLICATION = {{'class' : 'SimpleStrategy', 'replication_factor' : 1}}", ks), &[])
        .await
        .unwrap();

    session.query(format!("USE {}", ks), &[]).await.unwrap();

    session
        .query(
            "CREATE TYPE IF NOT EXISTS type_a (
                    a map<frozen<list<int>>, text>,
                    b frozen<map<frozen<list<int>>, frozen<set<text>>>>
                   )",
            &[],
        )
        .await
        .unwrap();

    session
        .query("CREATE TYPE IF NOT EXISTS type_b (a int, b text)", &[])
        .await
        .unwrap();

    session
        .query(
            "CREATE TYPE IF NOT EXISTS type_c (a map<frozen<set<text>>, frozen<type_b>>)",
            &[],
        )
        .await
        .unwrap();

    session.await_schema_agreement().await.unwrap();
    session.refresh_metadata().await.unwrap();

    let cluster_data = session.get_cluster_data();
    let user_defined_types = &cluster_data.get_keyspace_info()[&ks].user_defined_types;

    assert_eq!(
        user_defined_types.keys().sorted().collect::<Vec<_>>(),
        vec!["type_a", "type_b", "type_c"]
    );

    let type_a = &user_defined_types["type_a"];

    assert_eq!(
        type_a,
        &vec![
            (
                "a".to_string(),
                CqlType::Collection {
                    frozen: false,
                    type_: CollectionType::Map(
                        Box::new(CqlType::Collection {
                            frozen: true,
                            type_: CollectionType::List(Box::new(CqlType::Native(NativeType::Int)))
                        }),
                        Box::new(CqlType::Native(NativeType::Text))
                    )
                }
            ),
            (
                "b".to_string(),
                CqlType::Collection {
                    frozen: true,
                    type_: CollectionType::Map(
                        Box::new(CqlType::Collection {
                            frozen: true,
                            type_: CollectionType::List(Box::new(CqlType::Native(NativeType::Int)))
                        }),
                        Box::new(CqlType::Collection {
                            frozen: true,
                            type_: CollectionType::Set(Box::new(CqlType::Native(NativeType::Text)))
                        })
                    )
                }
            )
        ]
    );

    let type_b = &user_defined_types["type_b"];

    assert_eq!(
        type_b,
        &vec![
            ("a".to_string(), CqlType::Native(NativeType::Int)),
            ("b".to_string(), CqlType::Native(NativeType::Text))
        ]
    );

    let type_c = &user_defined_types["type_c"];

    assert_eq!(
        type_c,
        &vec![(
            "a".to_string(),
            CqlType::Collection {
                frozen: false,
                type_: CollectionType::Map(
                    Box::new(CqlType::Collection {
                        frozen: true,
                        type_: CollectionType::Set(Box::new(CqlType::Native(NativeType::Text)))
                    }),
                    Box::new(CqlType::UserDefinedType {
                        frozen: true,
                        name: "type_b".to_string()
                    })
                )
            }
        )]
    );
}

#[tokio::test]
async fn test_column_kinds_in_metadata() {
    let uri = std::env::var("SCYLLA_URI").unwrap_or_else(|_| "127.0.0.1:9042".to_string());
    let session = SessionBuilder::new().known_node(uri).build().await.unwrap();
    let ks = unique_name();

    session
        .query(format!("CREATE KEYSPACE {} WITH REPLICATION = {{'class' : 'SimpleStrategy', 'replication_factor' : 1}}", ks), &[])
        .await
        .unwrap();

    session.query(format!("USE {}", ks), &[]).await.unwrap();

    session
        .query(
            "CREATE TABLE IF NOT EXISTS t (
                    a int,
                    b int,
                    c int,
                    d int STATIC,
                    e int,
                    f int,
                    PRIMARY KEY ((c, e), b, a)
                  )",
            &[],
        )
        .await
        .unwrap();

    session.await_schema_agreement().await.unwrap();
    session.refresh_metadata().await.unwrap();

    let cluster_data = session.get_cluster_data();
    let columns = &cluster_data.get_keyspace_info()[&ks].tables["t"].columns;

    assert_eq!(columns["a"].kind, ColumnKind::Clustering);
    assert_eq!(columns["b"].kind, ColumnKind::Clustering);
    assert_eq!(columns["c"].kind, ColumnKind::PartitionKey);
    assert_eq!(columns["d"].kind, ColumnKind::Static);
    assert_eq!(columns["e"].kind, ColumnKind::PartitionKey);
    assert_eq!(columns["f"].kind, ColumnKind::Regular);
}

#[tokio::test]
async fn test_primary_key_ordering_in_metadata() {
    let uri = std::env::var("SCYLLA_URI").unwrap_or_else(|_| "127.0.0.1:9042".to_string());
    let session = SessionBuilder::new().known_node(uri).build().await.unwrap();
    let ks = unique_name();

    session
        .query(format!("CREATE KEYSPACE {} WITH REPLICATION = {{'class' : 'SimpleStrategy', 'replication_factor' : 1}}", ks), &[])
        .await
        .unwrap();

    session.query(format!("USE {}", ks), &[]).await.unwrap();

    session
        .query(
            "CREATE TABLE IF NOT EXISTS t (
                    a int,
                    b int,
                    c int,
                    d int STATIC,
                    e int,
                    f int,
                    g int,
                    h int,
                    i int STATIC,
                    PRIMARY KEY ((c, e), b, a)
                  )",
            &[],
        )
        .await
        .unwrap();

    session.await_schema_agreement().await.unwrap();
    session.refresh_metadata().await.unwrap();

    let cluster_data = session.get_cluster_data();
    let table = &cluster_data.get_keyspace_info()[&ks].tables["t"];

    assert_eq!(table.partition_key, vec!["c", "e"]);
    assert_eq!(table.clustering_key, vec!["b", "a"]);
}

#[tokio::test]
async fn test_table_partitioner_in_metadata() {
    if option_env!("CDC") == Some("disabled") {
        return;
    }

    let uri = std::env::var("SCYLLA_URI").unwrap_or_else(|_| "127.0.0.1:9042".to_string());
    let session = SessionBuilder::new().known_node(uri).build().await.unwrap();
    let ks = unique_name();

    session
        .query(format!("CREATE KEYSPACE {} WITH REPLICATION = {{'class' : 'SimpleStrategy', 'replication_factor' : 1}}", ks), &[])
        .await
        .unwrap();

    session.query(format!("USE {}", ks), &[]).await.unwrap();

    session
        .query(
            "CREATE TABLE t (pk int, ck int, v int, PRIMARY KEY (pk, ck, v))WITH cdc = {'enabled':true}",
            &[],
        )
        .await
        .unwrap();

    session.await_schema_agreement().await.unwrap();
    session.refresh_metadata().await.unwrap();

    let cluster_data = session.get_cluster_data();
    let tables = &cluster_data.get_keyspace_info()[&ks].tables;
    let table = &tables["t"];
    let cdc_table = &tables["t_scylla_cdc_log"];

    assert_eq!(table.partitioner, None);
    assert_eq!(
        cdc_table.partitioner.as_ref().unwrap(),
        "com.scylladb.dht.CDCPartitioner"
    );
}

#[tokio::test]
async fn test_turning_off_schema_fetching() {
    let uri = std::env::var("SCYLLA_URI").unwrap_or_else(|_| "127.0.0.1:9042".to_string());
    let session = SessionBuilder::new()
        .fetch_schema_metadata(false)
        .known_node(uri)
        .build()
        .await
        .unwrap();
    let ks = unique_name();

    session
        .query(format!("CREATE KEYSPACE IF NOT EXISTS {} WITH REPLICATION = {{'class' : 'SimpleStrategy', 'replication_factor' : 1}}", ks), &[])
        .await
        .unwrap();

    session.query(format!("USE {}", ks), &[]).await.unwrap();

    session
        .query(
            "CREATE TYPE IF NOT EXISTS type_a (
                    a map<frozen<list<int>>, text>,
                    b frozen<map<frozen<list<int>>, frozen<set<text>>>>
                   )",
            &[],
        )
        .await
        .unwrap();

    session
        .query("CREATE TYPE IF NOT EXISTS type_b (a int, b text)", &[])
        .await
        .unwrap();

    session
        .query(
            "CREATE TYPE IF NOT EXISTS type_c (a map<frozen<set<text>>, frozen<type_b>>)",
            &[],
        )
        .await
        .unwrap();

    session
        .query(
            "CREATE TABLE IF NOT EXISTS table_a (
                    a frozen<type_a> PRIMARY KEY,
                    b type_b,
                    c frozen<type_c>,
                    d map<text, frozen<list<int>>>,
                    e tuple<int, text>
                  )",
            &[],
        )
        .await
        .unwrap();

    session.refresh_metadata().await.unwrap();
    let cluster_data = &session.get_cluster_data();
    let keyspace = &cluster_data.get_keyspace_info()[&ks];

    assert_eq!(
        keyspace.strategy,
        SimpleStrategy {
            replication_factor: 1
        }
    );
    assert_eq!(keyspace.tables.len(), 0);
    assert_eq!(keyspace.user_defined_types.len(), 0);
}

#[tokio::test]
async fn test_named_bind_markers() {
    let uri = std::env::var("SCYLLA_URI").unwrap_or_else(|_| "127.0.0.1:9042".to_string());
    let session = SessionBuilder::new().known_node(uri).build().await.unwrap();
    let ks = unique_name();

    session
        .query(format!("CREATE KEYSPACE {} WITH REPLICATION = {{'class' : 'SimpleStrategy', 'replication_factor' : 1}}", ks), &[])
        .await
        .unwrap();

    session.query(format!("USE {}", ks), &[]).await.unwrap();

    session
        .query(
            "CREATE TABLE t (pk int, ck int, v int, PRIMARY KEY (pk, ck, v))",
            &[],
        )
        .await
        .unwrap();

    session.await_schema_agreement().await.unwrap();

    let prepared = session
        .prepare("INSERT INTO t (pk, ck, v) VALUES (:pk, :ck, :v)")
        .await
        .unwrap();
    let hashmap: HashMap<&str, i32> = HashMap::from([("pk", 7), ("v", 42), ("ck", 13)]);
    session.execute(&prepared, &hashmap).await.unwrap();

    let btreemap: BTreeMap<&str, i32> = BTreeMap::from([("ck", 113), ("v", 142), ("pk", 17)]);
    session.execute(&prepared, &btreemap).await.unwrap();

    let rows: Vec<(i32, i32, i32)> = session
        .query("SELECT pk, ck, v FROM t", &[])
        .await
        .unwrap()
        .rows
        .unwrap()
        .into_typed::<(i32, i32, i32)>()
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
        assert!(session.execute(&prepared, &wrongmap).await.is_err());
    }
}

#[tokio::test]
async fn test_prepared_partitioner() {
    let uri = std::env::var("SCYLLA_URI").unwrap_or_else(|_| "127.0.0.1:9042".to_string());
    let session = SessionBuilder::new().known_node(uri).build().await.unwrap();
    let ks = unique_name();

    session.query(format!("CREATE KEYSPACE IF NOT EXISTS {} WITH REPLICATION = {{'class' : 'SimpleStrategy', 'replication_factor' : 1}}", ks), &[]).await.unwrap();
    session.use_keyspace(ks, false).await.unwrap();

    session
        .query("CREATE TABLE IF NOT EXISTS t1 (a int primary key)", &[])
        .await
        .unwrap();

    session.await_schema_agreement().await.unwrap();
    session.refresh_metadata().await.unwrap();

    let prepared_statement_for_main_table = session
        .prepare("INSERT INTO t1 (a) VALUES (?)")
        .await
        .unwrap();

    assert_eq!(
        prepared_statement_for_main_table.get_partitioner_name(),
        &PartitionerName::Murmur3
    );

    if option_env!("CDC") == Some("disabled") {
        return;
    }

    session
        .query(
            "CREATE TABLE IF NOT EXISTS t2 (a int primary key) WITH cdc = {'enabled':true}",
            &[],
        )
        .await
        .unwrap();

    session.await_schema_agreement().await.unwrap();
    session.refresh_metadata().await.unwrap();

    let prepared_statement_for_cdc_log = session
        .prepare("SELECT a FROM t2_scylla_cdc_log WHERE \"cdc$stream_id\" = ?")
        .await
        .unwrap();

    assert_eq!(
        prepared_statement_for_cdc_log.get_partitioner_name(),
        &PartitionerName::CDC
    );
}

async fn rename(session: &Session, rename_str: &str) {
    session
        .query(format!("ALTER TABLE tab RENAME {}", rename_str), ())
        .await
        .unwrap();
}

// A tests which checks that Session::execute automatically reprepares PreparedStatemtns if they become unprepared.
// Doing an ALTER TABLE statement clears prepared statement cache and all prepared statements need
// to be prepared again.
// To verify that this indeed happens you can run:
// RUST_LOG=debug cargo test test_unprepared_reprepare_in_execute -- --nocapture
// And look for this line in the logs:
// Connection::execute: Got DbError::Unprepared - repreparing statement with id ...
#[tokio::test]
async fn test_unprepared_reprepare_in_execute() {
    let _ = tracing_subscriber::fmt::try_init();

    let uri = std::env::var("SCYLLA_URI").unwrap_or_else(|_| "127.0.0.1:9042".to_string());
    let session = SessionBuilder::new().known_node(uri).build().await.unwrap();
    let ks = unique_name();

    session.query(format!("CREATE KEYSPACE IF NOT EXISTS {} WITH REPLICATION = {{'class' : 'SimpleStrategy', 'replication_factor' : 1}}", ks), &[]).await.unwrap();
    session.use_keyspace(ks, false).await.unwrap();

    session
        .query(
            "CREATE TABLE IF NOT EXISTS tab (a int, b int, c int, primary key (a, b, c))",
            &[],
        )
        .await
        .unwrap();

    let insert_a_b_c = session
        .prepare("INSERT INTO tab (a, b, c) VALUES (?, ?, ?)")
        .await
        .unwrap();

    session.execute(&insert_a_b_c, (1, 2, 3)).await.unwrap();

    // Swap names of columns b and c
    rename(&session, "b TO tmp_name").await;

    // During rename the query should fail
    assert!(session.execute(&insert_a_b_c, (1, 2, 3)).await.is_err());
    rename(&session, "c TO b").await;
    assert!(session.execute(&insert_a_b_c, (1, 2, 3)).await.is_err());
    rename(&session, "tmp_name TO c").await;

    // Insert values again (b and c are swapped so those are different inserts)
    session.execute(&insert_a_b_c, (1, 2, 3)).await.unwrap();

    let mut all_rows: Vec<(i32, i32, i32)> = session
        .query("SELECT a, b, c FROM tab", ())
        .await
        .unwrap()
        .rows_typed::<(i32, i32, i32)>()
        .unwrap()
        .map(|r| r.unwrap())
        .collect();
    all_rows.sort();
    assert_eq!(all_rows, vec![(1, 2, 3), (1, 3, 2)]);
}
