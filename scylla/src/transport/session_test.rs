use crate as scylla;
use crate::batch::{Batch, BatchStatement};
use crate::frame::response::result::Row;
use crate::prepared_statement::PreparedStatement;
use crate::query::Query;
use crate::retry_policy::{QueryInfo, RetryDecision, RetryPolicy, RetrySession};
use crate::routing::Token;
use crate::statement::Consistency;
use crate::test_utils::{scylla_supports_tablets, setup_tracing};
use crate::tracing::TracingInfo;
use crate::transport::cluster::Datacenter;
use crate::transport::errors::{BadKeyspaceName, BadQuery, DbError, QueryError};
use crate::transport::partitioner::{
    calculate_token_for_partition_key, Murmur3Partitioner, Partitioner, PartitionerName,
};
use crate::transport::topology::Strategy::NetworkTopologyStrategy;
use crate::transport::topology::{
    CollectionType, ColumnKind, CqlType, NativeType, UserDefinedType,
};
use crate::utils::test_utils::{
    create_new_session_builder, supports_feature, unique_keyspace_name,
};
use crate::CachingSession;
use crate::ExecutionProfile;
use crate::QueryResult;
use crate::{Session, SessionBuilder};
use assert_matches::assert_matches;
use futures::{FutureExt, StreamExt, TryStreamExt};
use itertools::Itertools;
use scylla_cql::frame::request::query::{PagingState, PagingStateResponse};
use scylla_cql::frame::response::result::ColumnType;
use scylla_cql::types::serialize::row::{SerializeRow, SerializedValues};
use scylla_cql::types::serialize::value::SerializeValue;
use std::collections::BTreeSet;
use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio::net::TcpListener;
use uuid::Uuid;

#[tokio::test]
async fn test_connection_failure() {
    setup_tracing();
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
    setup_tracing();
    let session = create_new_session_builder().build().await.unwrap();
    let ks = unique_keyspace_name();

    session.query_unpaged(format!("CREATE KEYSPACE IF NOT EXISTS {} WITH REPLICATION = {{'class' : 'NetworkTopologyStrategy', 'replication_factor' : 1}}", ks), &[]).await.unwrap();
    session
        .query_unpaged(
            format!(
                "CREATE TABLE IF NOT EXISTS {}.t (a int, b int, c text, primary key (a, b))",
                ks
            ),
            &[],
        )
        .await
        .unwrap();

    session
        .query_unpaged(
            format!("INSERT INTO {}.t (a, b, c) VALUES (1, 2, 'abc')", ks),
            &[],
        )
        .await
        .unwrap();
    session
        .query_unpaged(
            format!("INSERT INTO {}.t (a, b, c) VALUES (7, 11, '')", ks),
            &[],
        )
        .await
        .unwrap();
    session
        .query_unpaged(
            format!("INSERT INTO {}.t (a, b, c) VALUES (1, 4, 'hello')", ks),
            &[],
        )
        .await
        .unwrap();

    let query_result = session
        .query_unpaged(format!("SELECT a, b, c FROM {}.t", ks), &[])
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
        assert_eq!(spec.name(), name); // Check column name.
        assert_eq!(spec.table_spec().ks_name(), ks);
    }
    let mut results_from_manual_paging: Vec<Row> = vec![];
    let query = Query::new(format!("SELECT a, b, c FROM {}.t", ks)).with_page_size(1);
    let mut paging_state = PagingState::start();
    let mut watchdog = 0;
    loop {
        let (rs_manual, paging_state_response) = session
            .query_single_page(query.clone(), &[], paging_state)
            .await
            .unwrap();
        results_from_manual_paging.append(&mut rs_manual.rows.unwrap());
        match paging_state_response {
            PagingStateResponse::HasMorePages { state } => {
                paging_state = state;
            }
            _ if watchdog > 30 => break,
            PagingStateResponse::NoMorePages => break,
        }
        watchdog += 1;
    }
    assert_eq!(results_from_manual_paging, rs);
}

#[tokio::test]
async fn test_prepared_statement() {
    setup_tracing();
    let session = create_new_session_builder().build().await.unwrap();
    let ks = unique_keyspace_name();

    session.query_unpaged(format!("CREATE KEYSPACE IF NOT EXISTS {} WITH REPLICATION = {{'class' : 'NetworkTopologyStrategy', 'replication_factor' : 1}}", ks), &[]).await.unwrap();
    session
        .query_unpaged(
            format!(
                "CREATE TABLE IF NOT EXISTS {}.t2 (a int, b int, c text, primary key (a, b))",
                ks
            ),
            &[],
        )
        .await
        .unwrap();
    session
        .query_unpaged(format!("CREATE TABLE IF NOT EXISTS {}.complex_pk (a int, b int, c text, d int, e int, primary key ((a,b,c),d))", ks), &[])
        .await
        .unwrap();

    // Refresh metadata as `ClusterData::compute_token` use them
    session.await_schema_agreement().await.unwrap();
    session.refresh_metadata().await.unwrap();

    let prepared_statement = session
        .prepare(format!("SELECT a, b, c FROM {}.t2", ks))
        .await
        .unwrap();
    let query_result = session.execute_iter(prepared_statement, &[]).await.unwrap();
    let specs = query_result.get_column_specs();
    assert_eq!(specs.len(), 3);
    for (spec, name) in specs.iter().zip(["a", "b", "c"]) {
        assert_eq!(spec.name(), name); // Check column name.
        assert_eq!(spec.table_spec().ks_name(), ks);
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
    let serialized_values_complex_pk = prepared_complex_pk_statement
        .serialize_values(&values)
        .unwrap();

    session
        .execute_unpaged(&prepared_statement, &values)
        .await
        .unwrap();
    session
        .execute_unpaged(&prepared_complex_pk_statement, &values)
        .await
        .unwrap();

    // Verify that token calculation is compatible with Scylla
    {
        let (value,): (i64,) = session
            .query_unpaged(format!("SELECT token(a) FROM {}.t2", ks), &[])
            .await
            .unwrap()
            .single_row_typed()
            .unwrap();
        let token = Token::new(value);
        let prepared_token = Murmur3Partitioner
            .hash_one(&prepared_statement.compute_partition_key(&values).unwrap());
        assert_eq!(token, prepared_token);
        let mut pk = SerializedValues::new();
        pk.add_value(&17_i32, &ColumnType::Int).unwrap();
        let cluster_data_token = session
            .get_cluster_data()
            .compute_token(&ks, "t2", &pk)
            .unwrap();
        assert_eq!(token, cluster_data_token);
    }
    {
        let (value,): (i64,) = session
            .query_unpaged(format!("SELECT token(a,b,c) FROM {}.complex_pk", ks), &[])
            .await
            .unwrap()
            .single_row_typed()
            .unwrap();
        let token = Token::new(value);
        let prepared_token = Murmur3Partitioner.hash_one(
            &prepared_complex_pk_statement
                .compute_partition_key(&values)
                .unwrap(),
        );
        assert_eq!(token, prepared_token);
        let cluster_data_token = session
            .get_cluster_data()
            .compute_token(&ks, "complex_pk", &serialized_values_complex_pk)
            .unwrap();
        assert_eq!(token, cluster_data_token);
    }

    // Verify that correct data was inserted
    {
        let rs = session
            .query_unpaged(format!("SELECT a,b,c FROM {}.t2", ks), &[])
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
        let mut paging_state = PagingState::start();
        let mut watchdog = 0;
        loop {
            let (rs_manual, paging_state_response) = session
                .execute_single_page(&prepared_paged, &[], paging_state)
                .await
                .unwrap();
            results_from_manual_paging.append(&mut rs_manual.rows.unwrap());
            match paging_state_response {
                PagingStateResponse::HasMorePages { state } => {
                    paging_state = state;
                }
                _ if watchdog > 30 => break,
                PagingStateResponse::NoMorePages => break,
            }
            watchdog += 1;
        }
        assert_eq!(results_from_manual_paging, rs);
    }
    {
        let (a, b, c, d, e): (i32, i32, String, i32, Option<i32>) = session
            .query_unpaged(format!("SELECT a,b,c,d,e FROM {}.complex_pk", ks), &[])
            .await
            .unwrap()
            .single_row_typed()
            .unwrap();
        assert!(e.is_none());
        assert_eq!(
            (a, b, c.as_str(), d, e),
            (17, 16, "I'm prepared!!!", 7, None)
        );
    }
    // Check that SerializeRow macro works
    {
        #[derive(scylla::SerializeRow, scylla::FromRow, PartialEq, Debug, Clone)]
        #[scylla(crate = crate)]
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
            .query_unpaged(
                format!(
                    "INSERT INTO {}.complex_pk (a,b,c,d,e) VALUES (?,?,?,?,?)",
                    ks
                ),
                input.clone(),
            )
            .await
            .unwrap();
        let output: ComplexPk = session
            .query_unpaged(
                format!(
                    "SELECT * FROM {}.complex_pk WHERE a = 9 and b = 8 and c = 'seven'",
                    ks
                ),
                &[],
            )
            .await
            .unwrap()
            .single_row_typed()
            .unwrap();
        assert_eq!(input, output)
    }
}

#[tokio::test]
async fn test_counter_batch() {
    use crate::frame::value::Counter;
    use scylla_cql::frame::request::batch::BatchType;

    setup_tracing();
    let session = Arc::new(create_new_session_builder().build().await.unwrap());
    let ks = unique_keyspace_name();

    // Need to disable tablets in this test because they don't support counters yet.
    // (https://github.com/scylladb/scylladb/commit/c70f321c6f581357afdf3fd8b4fe8e5c5bb9736e).
    let mut create_ks = format!("CREATE KEYSPACE IF NOT EXISTS {} WITH REPLICATION = {{'class' : 'NetworkTopologyStrategy', 'replication_factor' : 1}}", ks);
    if scylla_supports_tablets(&session).await {
        create_ks += " AND TABLETS = {'enabled': false}"
    }

    session.query_unpaged(create_ks, &[]).await.unwrap();
    session
        .query_unpaged(
            format!(
                "CREATE TABLE IF NOT EXISTS {}.t_batch (key int PRIMARY KEY, value counter)",
                ks
            ),
            &[],
        )
        .await
        .unwrap();

    let statement_str = format!("UPDATE {}.t_batch SET value = value + ? WHERE key = ?", ks);
    let query = Query::from(statement_str);
    let prepared = session.prepare(query.clone()).await.unwrap();

    let mut counter_batch = Batch::new(BatchType::Counter);
    counter_batch.append_statement(query.clone());
    counter_batch.append_statement(prepared.clone());
    counter_batch.append_statement(query.clone());
    counter_batch.append_statement(prepared.clone());
    counter_batch.append_statement(query.clone());
    counter_batch.append_statement(prepared.clone());

    // Check that we do not get a server error - the driver
    // should send a COUNTER batch instead of a LOGGED (default) one.
    session
        .batch(
            &counter_batch,
            (
                (Counter(1), 1),
                (Counter(2), 2),
                (Counter(3), 3),
                (Counter(4), 4),
                (Counter(5), 5),
                (Counter(6), 6),
            ),
        )
        .await
        .unwrap();
}

#[tokio::test]
async fn test_batch() {
    setup_tracing();
    let session = Arc::new(create_new_session_builder().build().await.unwrap());
    let ks = unique_keyspace_name();

    session.query_unpaged(format!("CREATE KEYSPACE IF NOT EXISTS {} WITH REPLICATION = {{'class' : 'NetworkTopologyStrategy', 'replication_factor' : 1}}", ks), &[]).await.unwrap();
    session
        .query_unpaged(
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

    let four_value: i32 = 4;
    let hello_value: String = String::from("hello");
    let session_clone = session.clone();
    // We're spawning to a separate task here to test that it works even in that case, because in some scenarios
    // (specifically if the `BatchValuesIter` associated type is not dropped before await boundaries)
    // the implicit auto trait propagation on batch will be such that the returned future is not Send (depending on
    // some lifetime for some unknown reason), so can't be spawned on tokio.
    // See https://github.com/scylladb/scylla-rust-driver/issues/599 for more details
    tokio::spawn(async move {
        let values = (
            (1_i32, 2_i32, "abc"),
            (),
            (1_i32, &four_value, hello_value.as_str()),
        );
        session_clone.batch(&batch, values).await.unwrap();
    })
    .await
    .unwrap();

    let mut results: Vec<(i32, i32, String)> = session
        .query_unpaged(format!("SELECT a, b, c FROM {}.t_batch", ks), &[])
        .await
        .unwrap()
        .rows_typed()
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();

    results.sort();
    assert_eq!(
        results,
        vec![
            (1, 2, String::from("abc")),
            (1, 4, String::from("hello")),
            (7, 11, String::from(""))
        ]
    );

    // Test repreparing statement inside a batch
    let mut batch: Batch = Default::default();
    batch.append_statement(prepared_statement);
    let values = ((4_i32, 20_i32, "foobar"),);

    // This statement flushes the prepared statement cache
    session
        .query_unpaged(
            format!("ALTER TABLE {}.t_batch WITH gc_grace_seconds = 42", ks),
            &[],
        )
        .await
        .unwrap();
    session.batch(&batch, values).await.unwrap();

    let results: Vec<(i32, i32, String)> = session
        .query_unpaged(
            format!("SELECT a, b, c FROM {}.t_batch WHERE a = 4", ks),
            &[],
        )
        .await
        .unwrap()
        .rows_typed()
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();

    assert_eq!(results, vec![(4, 20, String::from("foobar"))]);
}

#[tokio::test]
async fn test_token_calculation() {
    setup_tracing();
    let session = create_new_session_builder().build().await.unwrap();
    let ks = unique_keyspace_name();

    session.query_unpaged(format!("CREATE KEYSPACE IF NOT EXISTS {} WITH REPLICATION = {{'class' : 'NetworkTopologyStrategy', 'replication_factor' : 1}}", ks), &[]).await.unwrap();
    session
        .query_unpaged(
            format!("CREATE TABLE IF NOT EXISTS {}.t3 (a text primary key)", ks),
            &[],
        )
        .await
        .unwrap();

    // Refresh metadata as `ClusterData::compute_token` use them
    session.await_schema_agreement().await.unwrap();
    session.refresh_metadata().await.unwrap();

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
        let serialized_values = prepared_statement.serialize_values(&values).unwrap();
        session
            .execute_unpaged(&prepared_statement, &values)
            .await
            .unwrap();

        let (value,): (i64,) = session
            .query_unpaged(
                format!("SELECT token(a) FROM {}.t3 WHERE a = ?", ks),
                &values,
            )
            .await
            .unwrap()
            .single_row_typed()
            .unwrap();
        let token = Token::new(value);
        let prepared_token = Murmur3Partitioner
            .hash_one(&prepared_statement.compute_partition_key(&values).unwrap());
        assert_eq!(token, prepared_token);
        let cluster_data_token = session
            .get_cluster_data()
            .compute_token(&ks, "t3", &serialized_values)
            .unwrap();
        assert_eq!(token, cluster_data_token);
    }
}

#[tokio::test]
async fn test_token_awareness() {
    setup_tracing();
    let session = create_new_session_builder().build().await.unwrap();
    let ks = unique_keyspace_name();

    // Need to disable tablets in this test because they make token routing
    // work differently, and in this test we want to test the classic token ring
    // behavior.
    let mut create_ks = format!(
        "CREATE KEYSPACE IF NOT EXISTS {ks} WITH REPLICATION = {{'class' : 'NetworkTopologyStrategy', 'replication_factor' : 1}}"
    );
    if scylla_supports_tablets(&session).await {
        create_ks += " AND TABLETS = {'enabled': false}"
    }

    session.query_unpaged(create_ks, &[]).await.unwrap();
    session
        .query_unpaged(
            format!("CREATE TABLE IF NOT EXISTS {}.t (a text primary key)", ks),
            &[],
        )
        .await
        .unwrap();

    let mut prepared_statement = session
        .prepare(format!("INSERT INTO {}.t (a) VALUES (?)", ks))
        .await
        .unwrap();
    prepared_statement.set_tracing(true);

    // The default policy should be token aware
    for size in 1..50usize {
        let key = vec!['a'; size].into_iter().collect::<String>();
        let values = (&key,);

        // Execute a query and observe tracing info
        let res = session
            .execute_unpaged(&prepared_statement, values)
            .await
            .unwrap();
        let tracing_info = session
            .get_tracing_info(res.tracing_id.as_ref().unwrap())
            .await
            .unwrap();

        // Verify that only one node was involved
        assert_eq!(tracing_info.nodes().len(), 1);

        // Do the same with execute_iter (it now works with writes)
        let iter = session
            .execute_iter(prepared_statement.clone(), values)
            .await
            .unwrap();
        let tracing_id = iter.get_tracing_ids()[0];
        let tracing_info = session.get_tracing_info(&tracing_id).await.unwrap();

        // Again, verify that only one node was involved
        assert_eq!(tracing_info.nodes().len(), 1);
    }
}

#[tokio::test]
async fn test_use_keyspace() {
    setup_tracing();
    let session = create_new_session_builder().build().await.unwrap();
    let ks = unique_keyspace_name();

    session.query_unpaged(format!("CREATE KEYSPACE IF NOT EXISTS {} WITH REPLICATION = {{'class' : 'NetworkTopologyStrategy', 'replication_factor' : 1}}", ks), &[]).await.unwrap();

    session
        .query_unpaged(
            format!("CREATE TABLE IF NOT EXISTS {}.tab (a text primary key)", ks),
            &[],
        )
        .await
        .unwrap();

    session
        .query_unpaged(format!("INSERT INTO {}.tab (a) VALUES ('test1')", ks), &[])
        .await
        .unwrap();

    session.use_keyspace(ks.clone(), false).await.unwrap();

    session
        .query_unpaged("INSERT INTO tab (a) VALUES ('test2')", &[])
        .await
        .unwrap();

    let mut rows: Vec<String> = session
        .query_unpaged("SELECT * FROM tab", &[])
        .await
        .unwrap()
        .rows_typed::<(String,)>()
        .unwrap()
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

    let long_name: String = ['a'; 49].iter().collect();
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
    let session2: Session = create_new_session_builder()
        .use_keyspace(ks.clone(), false)
        .build()
        .await
        .unwrap();

    let mut rows2: Vec<String> = session2
        .query_unpaged("SELECT * FROM tab", &[])
        .await
        .unwrap()
        .rows_typed::<(String,)>()
        .unwrap()
        .map(|res| res.unwrap().0)
        .collect();

    rows2.sort();

    assert_eq!(rows2, vec!["test1".to_string(), "test2".to_string()]);
}

#[tokio::test]
async fn test_use_keyspace_case_sensitivity() {
    setup_tracing();
    let session = create_new_session_builder().build().await.unwrap();
    let ks_lower = unique_keyspace_name().to_lowercase();
    let ks_upper = ks_lower.to_uppercase();

    session.query_unpaged(format!("CREATE KEYSPACE IF NOT EXISTS \"{}\" WITH REPLICATION = {{'class' : 'NetworkTopologyStrategy', 'replication_factor' : 1}}", ks_lower), &[]).await.unwrap();
    session.query_unpaged(format!("CREATE KEYSPACE IF NOT EXISTS \"{}\" WITH REPLICATION = {{'class' : 'NetworkTopologyStrategy', 'replication_factor' : 1}}", ks_upper), &[]).await.unwrap();

    session
        .query_unpaged(
            format!("CREATE TABLE {}.tab (a text primary key)", ks_lower),
            &[],
        )
        .await
        .unwrap();

    session
        .query_unpaged(
            format!("CREATE TABLE \"{}\".tab (a text primary key)", ks_upper),
            &[],
        )
        .await
        .unwrap();

    session
        .query_unpaged(
            format!("INSERT INTO {}.tab (a) VALUES ('lowercase')", ks_lower),
            &[],
        )
        .await
        .unwrap();

    session
        .query_unpaged(
            format!("INSERT INTO \"{}\".tab (a) VALUES ('uppercase')", ks_upper),
            &[],
        )
        .await
        .unwrap();

    // Use uppercase keyspace without case sensitivity
    // Should select the lowercase one
    session.use_keyspace(ks_upper.clone(), false).await.unwrap();

    let rows: Vec<String> = session
        .query_unpaged("SELECT * from tab", &[])
        .await
        .unwrap()
        .rows_typed::<(String,)>()
        .unwrap()
        .map(|row| row.unwrap().0)
        .collect();

    assert_eq!(rows, vec!["lowercase".to_string()]);

    // Use uppercase keyspace with case sensitivity
    // Should select the uppercase one
    session.use_keyspace(ks_upper, true).await.unwrap();

    let rows: Vec<String> = session
        .query_unpaged("SELECT * from tab", &[])
        .await
        .unwrap()
        .rows_typed::<(String,)>()
        .unwrap()
        .map(|row| row.unwrap().0)
        .collect();

    assert_eq!(rows, vec!["uppercase".to_string()]);
}

#[tokio::test]
async fn test_raw_use_keyspace() {
    setup_tracing();
    let session = create_new_session_builder().build().await.unwrap();
    let ks = unique_keyspace_name();

    session.query_unpaged(format!("CREATE KEYSPACE IF NOT EXISTS {} WITH REPLICATION = {{'class' : 'NetworkTopologyStrategy', 'replication_factor' : 1}}", ks), &[]).await.unwrap();

    session
        .query_unpaged(
            format!("CREATE TABLE IF NOT EXISTS {}.tab (a text primary key)", ks),
            &[],
        )
        .await
        .unwrap();

    session
        .query_unpaged(
            format!("INSERT INTO {}.tab (a) VALUES ('raw_test')", ks),
            &[],
        )
        .await
        .unwrap();

    session
        .query_unpaged(format!("use    \"{}\"    ;", ks), &[])
        .await
        .unwrap();

    let rows: Vec<String> = session
        .query_unpaged("SELECT * FROM tab", &[])
        .await
        .unwrap()
        .rows_typed::<(String,)>()
        .unwrap()
        .map(|res| res.unwrap().0)
        .collect();

    assert_eq!(rows, vec!["raw_test".to_string()]);

    // Check if case sensitivity is correctly detected
    assert!(session
        .query_unpaged(format!("use    \"{}\"    ;", ks.to_uppercase()), &[])
        .await
        .is_err());

    assert!(session
        .query_unpaged(format!("use    {}    ;", ks.to_uppercase()), &[])
        .await
        .is_ok());
}

#[tokio::test]
async fn test_fetch_system_keyspace() {
    setup_tracing();
    let session = create_new_session_builder().build().await.unwrap();

    let prepared_statement = session
        .prepare("SELECT * FROM system_schema.keyspaces")
        .await
        .unwrap();

    session
        .execute_unpaged(&prepared_statement, &[])
        .await
        .unwrap();
}

// Test that some Database Errors are parsed correctly
#[tokio::test]
async fn test_db_errors() {
    setup_tracing();
    let session = create_new_session_builder().build().await.unwrap();
    let ks = unique_keyspace_name();

    // SyntaxError on bad query
    assert!(matches!(
        session.query_unpaged("gibberish", &[]).await,
        Err(QueryError::DbError(DbError::SyntaxError, _))
    ));

    // AlreadyExists when creating a keyspace for the second time
    session.query_unpaged(format!("CREATE KEYSPACE IF NOT EXISTS {} WITH REPLICATION = {{'class' : 'NetworkTopologyStrategy', 'replication_factor' : 1}}", ks), &[]).await.unwrap();

    let create_keyspace_res = session.query_unpaged(format!("CREATE KEYSPACE {} WITH REPLICATION = {{'class' : 'NetworkTopologyStrategy', 'replication_factor' : 1}}", ks), &[]).await;
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
        .query_unpaged(
            format!("CREATE TABLE IF NOT EXISTS {}.tab (a text primary key)", ks),
            &[],
        )
        .await
        .unwrap();

    let create_table_res = session
        .query_unpaged(format!("CREATE TABLE {}.tab (a text primary key)", ks), &[])
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
    setup_tracing();
    let session = create_new_session_builder().build().await.unwrap();
    let ks = unique_keyspace_name();

    session.query_unpaged(format!("CREATE KEYSPACE IF NOT EXISTS {} WITH REPLICATION = {{'class' : 'NetworkTopologyStrategy', 'replication_factor' : 1}}", ks), &[]).await.unwrap();

    session
        .query_unpaged(
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
    let untraced_query_result: QueryResult =
        session.query_unpaged(untraced_query, &[]).await.unwrap();

    assert!(untraced_query_result.tracing_id.is_none());

    // A query with tracing enabled has a tracing uuid in result
    let mut traced_query: Query = Query::new(format!("SELECT * FROM {}.tab", ks));
    traced_query.config.tracing = true;

    let traced_query_result: QueryResult = session.query_unpaged(traced_query, &[]).await.unwrap();
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

    let untraced_prepared_result: QueryResult = session
        .execute_unpaged(&untraced_prepared, &[])
        .await
        .unwrap();

    assert!(untraced_prepared_result.tracing_id.is_none());

    // Executing a prepared statement with tracing enabled has a tracing uuid in result
    let mut traced_prepared = session
        .prepare(format!("SELECT * FROM {}.tab", ks))
        .await
        .unwrap();

    traced_prepared.config.tracing = true;

    let traced_prepared_result: QueryResult = session
        .execute_unpaged(&traced_prepared, &[])
        .await
        .unwrap();
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

    let traced_query_result: QueryResult = session.query_unpaged(traced_query, &[]).await.unwrap();
    let tracing_id: Uuid = traced_query_result.tracing_id.unwrap();

    // Getting tracing info from session using this uuid works
    let tracing_info: TracingInfo = session.get_tracing_info(&tracing_id).await.unwrap();
    assert!(!tracing_info.events.is_empty());
    assert!(!tracing_info.nodes().is_empty());
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
    let untraced_typed_row_iter = untraced_row_iter.into_typed::<(String,)>();
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
    let traced_typed_row_iter = traced_row_iter.into_typed::<(String,)>();
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
    let untraced_typed_row_iter = untraced_row_iter.into_typed::<(String,)>();
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
    let traced_typed_row_iter = traced_row_iter.into_typed::<(String,)>();
    assert!(!traced_typed_row_iter.get_tracing_ids().is_empty());

    for tracing_id in traced_typed_row_iter.get_tracing_ids() {
        assert_in_tracing_table(session, *tracing_id).await;
    }
}

async fn test_tracing_batch(session: &Session, ks: String) {
    // A batch without tracing enabled has no tracing id
    let mut untraced_batch: Batch = Default::default();
    untraced_batch.append_statement(&format!("INSERT INTO {}.tab (a) VALUES('a')", ks)[..]);

    let untraced_batch_result: QueryResult = session.batch(&untraced_batch, ((),)).await.unwrap();
    assert!(untraced_batch_result.tracing_id.is_none());

    // Batch with tracing enabled has a tracing uuid in result
    let mut traced_batch: Batch = Default::default();
    traced_batch.append_statement(&format!("INSERT INTO {}.tab (a) VALUES('a')", ks)[..]);
    traced_batch.config.tracing = true;

    let traced_batch_result: QueryResult = session.batch(&traced_batch, ((),)).await.unwrap();
    assert!(traced_batch_result.tracing_id.is_some());

    assert_in_tracing_table(session, traced_batch_result.tracing_id.unwrap()).await;
}

async fn assert_in_tracing_table(session: &Session, tracing_uuid: Uuid) {
    let mut traces_query = Query::new("SELECT * FROM system_traces.sessions WHERE session_id = ?");
    traces_query.config.consistency = Some(Consistency::One);

    // Tracing info might not be immediately available
    // If rows are empty perform 8 retries with a 32ms wait in between

    // The reason why we enable so long waiting for TracingInfo is... Cassandra. (Yes, again.)
    // In Cassandra Java Driver, the wait time for tracing info is 10 seconds, so here we do the same.
    // However, as Scylla usually gets TracingInfo ready really fast (our default interval is hence 3ms),
    // we stick to a not-so-much-terribly-long interval here.
    for _ in 0..200 {
        let rows_num = session
            .query_unpaged(traces_query.clone(), (tracing_uuid,))
            .await
            .unwrap()
            .rows_num()
            .unwrap();

        if rows_num > 0 {
            // Ok there was some row for this tracing_uuid
            return;
        }

        // Otherwise retry
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }

    // If all retries failed panic with an error
    panic!("No rows for tracing with this session id!");
}

#[tokio::test]
async fn test_await_schema_agreement() {
    setup_tracing();
    let session = create_new_session_builder().build().await.unwrap();
    let _schema_version = session.await_schema_agreement().await.unwrap();
}

#[tokio::test]
async fn test_await_timed_schema_agreement() {
    setup_tracing();
    let session = create_new_session_builder().build().await.unwrap();
    session.await_schema_agreement().await.unwrap();
}

#[tokio::test]
async fn test_timestamp() {
    setup_tracing();
    let session = create_new_session_builder().build().await.unwrap();
    let ks = unique_keyspace_name();

    session.query_unpaged(format!("CREATE KEYSPACE IF NOT EXISTS {} WITH REPLICATION = {{'class' : 'NetworkTopologyStrategy', 'replication_factor' : 1}}", ks), &[]).await.unwrap();
    session
        .query_unpaged(
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
        .query_unpaged(regular_query.clone(), ("regular query", "higher timestamp"))
        .await
        .unwrap();

    regular_query.set_timestamp(Some(42));
    session
        .query_unpaged(regular_query.clone(), ("regular query", "lower timestamp"))
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
        .query_unpaged(
            format!("SELECT a, b, WRITETIME(b) FROM {}.t_timestamp", ks),
            &[],
        )
        .await
        .unwrap()
        .rows_typed::<(String, String, i64)>()
        .unwrap()
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

#[ignore = "works on remote Scylla instances only (local ones are too fast)"]
#[tokio::test]
async fn test_request_timeout() {
    setup_tracing();
    use std::time::Duration;

    let fast_timeouting_profile_handle = ExecutionProfile::builder()
        .request_timeout(Some(Duration::from_millis(1)))
        .build()
        .into_handle();

    {
        let session = create_new_session_builder().build().await.unwrap();

        let mut query: Query = Query::new("SELECT * FROM system_schema.tables");
        query.set_request_timeout(Some(Duration::from_millis(1)));
        match session.query_unpaged(query, &[]).await {
            Ok(_) => panic!("the query should have failed due to a client-side timeout"),
            Err(e) => assert_matches!(e, QueryError::RequestTimeout(_)),
        }

        let mut prepared = session
            .prepare("SELECT * FROM system_schema.tables")
            .await
            .unwrap();

        prepared.set_request_timeout(Some(Duration::from_millis(1)));
        match session.execute_unpaged(&prepared, &[]).await {
            Ok(_) => panic!("the prepared query should have failed due to a client-side timeout"),
            Err(e) => assert_matches!(e, QueryError::RequestTimeout(_)),
        };
    }
    {
        let timeouting_session = create_new_session_builder()
            .default_execution_profile_handle(fast_timeouting_profile_handle)
            .build()
            .await
            .unwrap();

        let mut query = Query::new("SELECT * FROM system_schema.tables");

        match timeouting_session.query_unpaged(query.clone(), &[]).await {
            Ok(_) => panic!("the query should have failed due to a client-side timeout"),
            Err(e) => assert_matches!(e, QueryError::RequestTimeout(_)),
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
            Err(e) => assert_matches!(e, QueryError::RequestTimeout(_)),
        };

        prepared.set_request_timeout(Some(Duration::from_secs(10000)));

        timeouting_session.execute_unpaged(&prepared, &[]).await.expect("the prepared query should have not failed, because no client-side timeout was specified");
    }
}

#[tokio::test]
async fn test_prepared_config() {
    setup_tracing();
    let session = create_new_session_builder().build().await.unwrap();

    let mut query = Query::new("SELECT * FROM system_schema.tables");
    query.set_is_idempotent(true);
    query.set_page_size(42);

    let prepared_statement = session.prepare(query).await.unwrap();

    assert!(prepared_statement.get_is_idempotent());
    assert_eq!(prepared_statement.get_page_size(), 42);
}

fn udt_type_a_def(ks: &str) -> Arc<UserDefinedType> {
    Arc::new(UserDefinedType {
        name: "type_a".to_string(),
        keyspace: ks.to_owned(),
        field_types: vec![
            (
                "a".into(),
                CqlType::Collection {
                    frozen: false,
                    type_: CollectionType::Map(
                        Box::new(CqlType::Collection {
                            frozen: true,
                            type_: CollectionType::List(Box::new(CqlType::Native(NativeType::Int))),
                        }),
                        Box::new(CqlType::Native(NativeType::Text)),
                    ),
                },
            ),
            (
                "b".into(),
                CqlType::Collection {
                    frozen: true,
                    type_: CollectionType::Map(
                        Box::new(CqlType::Collection {
                            frozen: true,
                            type_: CollectionType::List(Box::new(CqlType::Native(NativeType::Int))),
                        }),
                        Box::new(CqlType::Collection {
                            frozen: true,
                            type_: CollectionType::Set(Box::new(CqlType::Native(NativeType::Text))),
                        }),
                    ),
                },
            ),
        ],
    })
}

fn udt_type_b_def(ks: &str) -> Arc<UserDefinedType> {
    Arc::new(UserDefinedType {
        name: "type_b".to_string(),
        keyspace: ks.to_owned(),
        field_types: vec![
            ("a".to_string(), CqlType::Native(NativeType::Int)),
            ("b".to_string(), CqlType::Native(NativeType::Text)),
        ],
    })
}

fn udt_type_c_def(ks: &str) -> Arc<UserDefinedType> {
    Arc::new(UserDefinedType {
        name: "type_c".to_string(),
        keyspace: ks.to_owned(),
        field_types: vec![(
            "a".to_string(),
            CqlType::Collection {
                frozen: false,
                type_: CollectionType::Map(
                    Box::new(CqlType::Collection {
                        frozen: true,
                        type_: CollectionType::Set(Box::new(CqlType::Native(NativeType::Text))),
                    }),
                    Box::new(CqlType::UserDefinedType {
                        frozen: true,
                        definition: Ok(udt_type_b_def(ks)),
                    }),
                ),
            },
        )],
    })
}

#[tokio::test]
async fn test_schema_types_in_metadata() {
    setup_tracing();
    let session = create_new_session_builder().build().await.unwrap();
    let ks = unique_keyspace_name();

    session
        .query_unpaged(format!("CREATE KEYSPACE {} WITH REPLICATION = {{'class' : 'NetworkTopologyStrategy', 'replication_factor' : 1}}", ks), &[])
        .await
        .unwrap();

    session
        .query_unpaged(format!("USE {}", ks), &[])
        .await
        .unwrap();

    session
        .query_unpaged(
            "CREATE TYPE IF NOT EXISTS type_a (
                    a map<frozen<list<int>>, text>,
                    b frozen<map<frozen<list<int>>, frozen<set<text>>>>
                   )",
            &[],
        )
        .await
        .unwrap();

    session
        .query_unpaged("CREATE TYPE IF NOT EXISTS type_b (a int, b text)", &[])
        .await
        .unwrap();

    session
        .query_unpaged(
            "CREATE TYPE IF NOT EXISTS type_c (a map<frozen<set<text>>, frozen<type_b>>)",
            &[],
        )
        .await
        .unwrap();

    session
        .query_unpaged(
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
        .query_unpaged(
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
            frozen: true,
            definition: Ok(udt_type_a_def(&ks)),
        }
    );

    let b = &table_a_columns["b"];

    assert_eq!(
        b.type_,
        CqlType::UserDefinedType {
            frozen: false,
            definition: Ok(udt_type_b_def(&ks)),
        }
    );

    let c = &table_a_columns["c"];

    assert_eq!(
        c.type_,
        CqlType::UserDefinedType {
            frozen: true,
            definition: Ok(udt_type_c_def(&ks))
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
    setup_tracing();
    let session = create_new_session_builder().build().await.unwrap();
    let ks = unique_keyspace_name();

    session
        .query_unpaged(format!("CREATE KEYSPACE {} WITH REPLICATION = {{'class' : 'NetworkTopologyStrategy', 'replication_factor' : 1}}", ks), &[])
        .await
        .unwrap();

    session
        .query_unpaged(format!("USE {}", ks), &[])
        .await
        .unwrap();

    session
        .query_unpaged(
            "CREATE TYPE IF NOT EXISTS type_a (
                    a map<frozen<list<int>>, text>,
                    b frozen<map<frozen<list<int>>, frozen<set<text>>>>
                   )",
            &[],
        )
        .await
        .unwrap();

    session
        .query_unpaged("CREATE TYPE IF NOT EXISTS type_b (a int, b text)", &[])
        .await
        .unwrap();

    session
        .query_unpaged(
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

    assert_eq!(*type_a, udt_type_a_def(&ks));

    let type_b = &user_defined_types["type_b"];

    assert_eq!(*type_b, udt_type_b_def(&ks));

    let type_c = &user_defined_types["type_c"];

    assert_eq!(*type_c, udt_type_c_def(&ks));
}

#[tokio::test]
async fn test_column_kinds_in_metadata() {
    setup_tracing();
    let session = create_new_session_builder().build().await.unwrap();
    let ks = unique_keyspace_name();

    session
        .query_unpaged(format!("CREATE KEYSPACE {} WITH REPLICATION = {{'class' : 'NetworkTopologyStrategy', 'replication_factor' : 1}}", ks), &[])
        .await
        .unwrap();

    session
        .query_unpaged(format!("USE {}", ks), &[])
        .await
        .unwrap();

    session
        .query_unpaged(
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
    setup_tracing();
    let session = create_new_session_builder().build().await.unwrap();
    let ks = unique_keyspace_name();

    session
        .query_unpaged(format!("CREATE KEYSPACE {} WITH REPLICATION = {{'class' : 'NetworkTopologyStrategy', 'replication_factor' : 1}}", ks), &[])
        .await
        .unwrap();

    session
        .query_unpaged(format!("USE {}", ks), &[])
        .await
        .unwrap();

    session
        .query_unpaged(
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
    setup_tracing();
    if option_env!("CDC") == Some("disabled") {
        return;
    }

    let session = create_new_session_builder().build().await.unwrap();
    let ks = unique_keyspace_name();

    // This test uses CDC which is not yet compatible with Scylla's tablets.
    let mut create_ks = format!(
        "CREATE KEYSPACE {ks} WITH REPLICATION = {{'class' : 'NetworkTopologyStrategy', 'replication_factor' : 1}}"
    );
    if scylla_supports_tablets(&session).await {
        create_ks += " AND TABLETS = {'enabled': false}";
    }

    session.query_unpaged(create_ks, &[]).await.unwrap();

    session
        .query_unpaged(format!("USE {}", ks), &[])
        .await
        .unwrap();

    session
        .query_unpaged(
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
    setup_tracing();
    let session = create_new_session_builder()
        .fetch_schema_metadata(false)
        .build()
        .await
        .unwrap();
    let ks = unique_keyspace_name();

    session
        .query_unpaged(format!("CREATE KEYSPACE IF NOT EXISTS {} WITH REPLICATION = {{'class' : 'NetworkTopologyStrategy', 'replication_factor' : 1}}", ks), &[])
        .await
        .unwrap();

    session
        .query_unpaged(format!("USE {}", ks), &[])
        .await
        .unwrap();

    session
        .query_unpaged(
            "CREATE TYPE IF NOT EXISTS type_a (
                    a map<frozen<list<int>>, text>,
                    b frozen<map<frozen<list<int>>, frozen<set<text>>>>
                   )",
            &[],
        )
        .await
        .unwrap();

    session
        .query_unpaged("CREATE TYPE IF NOT EXISTS type_b (a int, b text)", &[])
        .await
        .unwrap();

    session
        .query_unpaged(
            "CREATE TYPE IF NOT EXISTS type_c (a map<frozen<set<text>>, frozen<type_b>>)",
            &[],
        )
        .await
        .unwrap();

    session
        .query_unpaged(
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

    let datacenters: HashMap<String, Datacenter> = cluster_data.get_datacenters_info();
    let datacenter_repfactors: HashMap<String, usize> = datacenters
        .into_keys()
        .map(|dc_name| (dc_name, 1))
        .collect();

    assert_eq!(
        keyspace.strategy,
        NetworkTopologyStrategy {
            datacenter_repfactors
        }
    );
    assert_eq!(keyspace.tables.len(), 0);
    assert_eq!(keyspace.user_defined_types.len(), 0);
}

#[tokio::test]
async fn test_named_bind_markers() {
    let session = create_new_session_builder().build().await.unwrap();
    let ks = unique_keyspace_name();

    session
        .query_unpaged(format!("CREATE KEYSPACE {} WITH REPLICATION = {{'class' : 'NetworkTopologyStrategy', 'replication_factor' : 1}}", ks), &[])
        .await
        .unwrap();

    session
        .query_unpaged(format!("USE {}", ks), &[])
        .await
        .unwrap();

    session
        .query_unpaged(
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
    session.execute_unpaged(&prepared, &hashmap).await.unwrap();

    let btreemap: BTreeMap<&str, i32> = BTreeMap::from([("ck", 113), ("v", 142), ("pk", 17)]);
    session.execute_unpaged(&prepared, &btreemap).await.unwrap();

    let rows: Vec<(i32, i32, i32)> = session
        .query_unpaged("SELECT pk, ck, v FROM t", &[])
        .await
        .unwrap()
        .rows_typed::<(i32, i32, i32)>()
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
async fn test_prepared_partitioner() {
    let session = create_new_session_builder().build().await.unwrap();
    let ks = unique_keyspace_name();

    // This test uses CDC which is not yet compatible with Scylla's tablets.
    let mut create_ks = format!(
        "CREATE KEYSPACE IF NOT EXISTS {ks} WITH REPLICATION = {{'class' : 'NetworkTopologyStrategy', 'replication_factor' : 1}}");
    if scylla_supports_tablets(&session).await {
        create_ks += " AND TABLETS = {'enabled': false}"
    }

    session.query_unpaged(create_ks, &[]).await.unwrap();
    session.use_keyspace(ks, false).await.unwrap();

    session
        .query_unpaged("CREATE TABLE IF NOT EXISTS t1 (a int primary key)", &[])
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
        .query_unpaged(
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
        .query_unpaged(format!("ALTER TABLE tab RENAME {}", rename_str), ())
        .await
        .unwrap();
}

async fn rename_caching(session: &CachingSession, rename_str: &str) {
    session
        .execute_unpaged(format!("ALTER TABLE tab RENAME {}", rename_str), &())
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

    let session = create_new_session_builder().build().await.unwrap();
    let ks = unique_keyspace_name();

    session.query_unpaged(format!("CREATE KEYSPACE IF NOT EXISTS {} WITH REPLICATION = {{'class' : 'NetworkTopologyStrategy', 'replication_factor' : 1}}", ks), &[]).await.unwrap();
    session.use_keyspace(ks, false).await.unwrap();

    session
        .query_unpaged(
            "CREATE TABLE IF NOT EXISTS tab (a int, b int, c int, primary key (a, b, c))",
            &[],
        )
        .await
        .unwrap();

    let insert_a_b_c = session
        .prepare("INSERT INTO tab (a, b, c) VALUES (?, ?, ?)")
        .await
        .unwrap();

    session
        .execute_unpaged(&insert_a_b_c, (1, 2, 3))
        .await
        .unwrap();

    // Swap names of columns b and c
    rename(&session, "b TO tmp_name").await;

    // During rename the query should fail
    assert!(session
        .execute_unpaged(&insert_a_b_c, (1, 2, 3))
        .await
        .is_err());
    rename(&session, "c TO b").await;
    assert!(session
        .execute_unpaged(&insert_a_b_c, (1, 2, 3))
        .await
        .is_err());
    rename(&session, "tmp_name TO c").await;

    // Insert values again (b and c are swapped so those are different inserts)
    session
        .execute_unpaged(&insert_a_b_c, (1, 2, 3))
        .await
        .unwrap();

    let mut all_rows: Vec<(i32, i32, i32)> = session
        .query_unpaged("SELECT a, b, c FROM tab", ())
        .await
        .unwrap()
        .rows_typed::<(i32, i32, i32)>()
        .unwrap()
        .map(|r| r.unwrap())
        .collect();
    all_rows.sort_unstable();
    assert_eq!(all_rows, vec![(1, 2, 3), (1, 3, 2)]);
}

#[tokio::test]
async fn test_unusual_valuelists() {
    let _ = tracing_subscriber::fmt::try_init();

    let session = create_new_session_builder().build().await.unwrap();
    let ks = unique_keyspace_name();

    session.query_unpaged(format!("CREATE KEYSPACE IF NOT EXISTS {} WITH REPLICATION = {{'class' : 'NetworkTopologyStrategy', 'replication_factor' : 1}}", ks), &[]).await.unwrap();
    session.use_keyspace(ks, false).await.unwrap();

    session
        .query_unpaged(
            "CREATE TABLE IF NOT EXISTS tab (a int, b int, c varchar, primary key (a, b, c))",
            &[],
        )
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

    let mut all_rows: Vec<(i32, i32, String)> = session
        .query_unpaged("SELECT a, b, c FROM tab", ())
        .await
        .unwrap()
        .rows_typed::<(i32, i32, String)>()
        .unwrap()
        .map(|r| r.unwrap())
        .collect();
    all_rows.sort();
    assert_eq!(
        all_rows,
        vec![
            (1i32, 2i32, "&dyn".to_owned()),
            (1, 3, "Box dyn".to_owned())
        ]
    );
}

// A tests which checks that Session::batch automatically reprepares PreparedStatemtns if they become unprepared.
// Doing an ALTER TABLE statement clears prepared statement cache and all prepared statements need
// to be prepared again.
// To verify that this indeed happens you can run:
// RUST_LOG=debug cargo test test_unprepared_reprepare_in_batch -- --nocapture
// And look for this line in the logs:
// Connection::batch: got DbError::Unprepared - repreparing statement with id ...
#[tokio::test]
async fn test_unprepared_reprepare_in_batch() {
    let _ = tracing_subscriber::fmt::try_init();

    let session = create_new_session_builder().build().await.unwrap();
    let ks = unique_keyspace_name();

    session.query_unpaged(format!("CREATE KEYSPACE IF NOT EXISTS {} WITH REPLICATION = {{'class' : 'NetworkTopologyStrategy', 'replication_factor' : 1}}", ks), &[]).await.unwrap();
    session.use_keyspace(ks, false).await.unwrap();

    session
        .query_unpaged(
            "CREATE TABLE IF NOT EXISTS tab (a int, b int, c int, primary key (a, b, c))",
            &[],
        )
        .await
        .unwrap();

    let insert_a_b_c = session
        .prepare("INSERT INTO tab (a, b, c) VALUES (?, ?, ?)")
        .await
        .unwrap();
    let insert_a_b_6 = session
        .prepare("INSERT INTO tab (a, b, c) VALUES (?, ?, 6)")
        .await
        .unwrap();

    use crate::batch::Batch;
    let mut batch: Batch = Default::default();
    batch.append_statement(insert_a_b_c);
    batch.append_statement(insert_a_b_6);

    session.batch(&batch, ((1, 2, 3), (4, 5))).await.unwrap();

    // Swap names of columns b and c
    rename(&session, "b TO tmp_name").await;

    // During rename the query should fail
    assert!(session.batch(&batch, ((1, 2, 3), (4, 5))).await.is_err());
    rename(&session, "c TO b").await;
    assert!(session.batch(&batch, ((1, 2, 3), (4, 5))).await.is_err());
    rename(&session, "tmp_name TO c").await;

    // Insert values again (b and c are swapped so those are different inserts)
    session.batch(&batch, ((1, 2, 3), (4, 5))).await.unwrap();

    let mut all_rows: Vec<(i32, i32, i32)> = session
        .query_unpaged("SELECT a, b, c FROM tab", ())
        .await
        .unwrap()
        .rows_typed::<(i32, i32, i32)>()
        .unwrap()
        .map(|r| r.unwrap())
        .collect();
    all_rows.sort_unstable();
    assert_eq!(all_rows, vec![(1, 2, 3), (1, 3, 2), (4, 5, 6), (4, 6, 5)]);
}

// A tests which checks that Session::execute automatically reprepares PreparedStatemtns if they become unprepared.
// Doing an ALTER TABLE statement clears prepared statement cache and all prepared statements need
// to be prepared again.
// To verify that this indeed happens you can run:
// RUST_LOG=debug cargo test test_unprepared_reprepare_in_caching_session_execute -- --nocapture
// And look for this line in the logs:
// Connection::execute: Got DbError::Unprepared - repreparing statement with id ...
#[tokio::test]
async fn test_unprepared_reprepare_in_caching_session_execute() {
    let _ = tracing_subscriber::fmt::try_init();

    let session = create_new_session_builder().build().await.unwrap();
    let ks = unique_keyspace_name();

    session.query_unpaged(format!("CREATE KEYSPACE IF NOT EXISTS {} WITH REPLICATION = {{'class' : 'NetworkTopologyStrategy', 'replication_factor' : 1}}", ks), &[]).await.unwrap();
    session.use_keyspace(ks, false).await.unwrap();

    let caching_session: CachingSession = CachingSession::from(session, 64);

    caching_session
        .execute_unpaged(
            "CREATE TABLE IF NOT EXISTS tab (a int, b int, c int, primary key (a, b, c))",
            &[],
        )
        .await
        .unwrap();

    let insert_a_b_c = "INSERT INTO tab (a, b, c) VALUES (?, ?, ?)";

    caching_session
        .execute_unpaged(insert_a_b_c, &(1, 2, 3))
        .await
        .unwrap();

    // Swap names of columns b and c
    rename_caching(&caching_session, "b TO tmp_name").await;

    // During rename the query should fail
    assert!(caching_session
        .execute_unpaged(insert_a_b_c, &(1, 2, 3))
        .await
        .is_err());
    rename_caching(&caching_session, "c TO b").await;
    assert!(caching_session
        .execute_unpaged(insert_a_b_c, &(1, 2, 3))
        .await
        .is_err());
    rename_caching(&caching_session, "tmp_name TO c").await;

    // Insert values again (b and c are swapped so those are different inserts)
    caching_session
        .execute_unpaged(insert_a_b_c, &(1, 2, 3))
        .await
        .unwrap();

    let mut all_rows: Vec<(i32, i32, i32)> = caching_session
        .execute_unpaged("SELECT a, b, c FROM tab", &())
        .await
        .unwrap()
        .rows_typed::<(i32, i32, i32)>()
        .unwrap()
        .map(|r| r.unwrap())
        .collect();
    all_rows.sort_unstable();
    assert_eq!(all_rows, vec![(1, 2, 3), (1, 3, 2)]);
}

#[tokio::test]
async fn test_views_in_schema_info() {
    let _ = tracing_subscriber::fmt::try_init();

    let session = create_new_session_builder().build().await.unwrap();
    let ks = unique_keyspace_name();

    session.query_unpaged(format!("CREATE KEYSPACE IF NOT EXISTS {} WITH REPLICATION = {{'class' : 'NetworkTopologyStrategy', 'replication_factor' : 1}}", ks), &[]).await.unwrap();
    session.use_keyspace(ks.clone(), false).await.unwrap();

    session
        .query_unpaged("CREATE TABLE t(id int PRIMARY KEY, v int)", &[])
        .await
        .unwrap();

    session.query_unpaged("CREATE MATERIALIZED VIEW mv1 AS SELECT * FROM t WHERE v IS NOT NULL PRIMARY KEY (v, id)", &[]).await.unwrap();
    session.query_unpaged("CREATE MATERIALIZED VIEW mv2 AS SELECT id, v FROM t WHERE v IS NOT NULL PRIMARY KEY (v, id)", &[]).await.unwrap();

    session.await_schema_agreement().await.unwrap();
    session.refresh_metadata().await.unwrap();

    let keyspace_meta = session
        .get_cluster_data()
        .get_keyspace_info()
        .get(&ks)
        .unwrap()
        .clone();

    let tables = keyspace_meta
        .tables
        .keys()
        .collect::<std::collections::HashSet<&String>>();

    let views = keyspace_meta
        .views
        .keys()
        .collect::<std::collections::HashSet<&String>>();
    let views_base_table = keyspace_meta
        .views
        .values()
        .map(|view_meta| &view_meta.base_table_name)
        .collect::<std::collections::HashSet<&String>>();

    assert_eq!(tables, std::collections::HashSet::from([&"t".to_string()]));
    assert_eq!(
        views,
        std::collections::HashSet::from([&"mv1".to_string(), &"mv2".to_string()])
    );
    assert_eq!(
        views_base_table,
        std::collections::HashSet::from([&"t".to_string()])
    )
}

async fn assert_test_batch_table_rows_contain(sess: &Session, expected_rows: &[(i32, i32)]) {
    let selected_rows: BTreeSet<(i32, i32)> = sess
        .query_unpaged("SELECT a, b FROM test_batch_table", ())
        .await
        .unwrap()
        .rows_typed::<(i32, i32)>()
        .unwrap()
        .map(|r| r.unwrap())
        .collect();
    for expected_row in expected_rows.iter() {
        if !selected_rows.contains(expected_row) {
            panic!(
                "Expected {:?} to contain row: {:?}, but they didn't",
                selected_rows, expected_row
            );
        }
    }
}

#[tokio::test]
async fn test_prepare_batch() {
    let session = create_new_session_builder().build().await.unwrap();

    let ks = unique_keyspace_name();
    session.query_unpaged(format!("CREATE KEYSPACE IF NOT EXISTS {} WITH REPLICATION = {{'class' : 'NetworkTopologyStrategy', 'replication_factor' : 1}}", ks), &[]).await.unwrap();
    session.use_keyspace(ks.clone(), false).await.unwrap();

    session
        .query_unpaged(
            "CREATE TABLE test_batch_table (a int, b int, primary key (a, b))",
            (),
        )
        .await
        .unwrap();

    let unprepared_insert_a_b: &str = "insert into test_batch_table (a, b) values (?, ?)";
    let unprepared_insert_a_7: &str = "insert into test_batch_table (a, b) values (?, 7)";
    let unprepared_insert_8_b: &str = "insert into test_batch_table (a, b) values (8, ?)";
    let prepared_insert_a_b: PreparedStatement =
        session.prepare(unprepared_insert_a_b).await.unwrap();
    let prepared_insert_a_7: PreparedStatement =
        session.prepare(unprepared_insert_a_7).await.unwrap();
    let prepared_insert_8_b: PreparedStatement =
        session.prepare(unprepared_insert_8_b).await.unwrap();

    let assert_batch_prepared = |b: &Batch| {
        for stmt in &b.statements {
            match stmt {
                BatchStatement::PreparedStatement(_) => {}
                _ => panic!("Unprepared statement in prepared batch!"),
            }
        }
    };

    {
        let mut unprepared_batch: Batch = Default::default();
        unprepared_batch.append_statement(unprepared_insert_a_b);
        unprepared_batch.append_statement(unprepared_insert_a_7);
        unprepared_batch.append_statement(unprepared_insert_8_b);

        let prepared_batch: Batch = session.prepare_batch(&unprepared_batch).await.unwrap();
        assert_batch_prepared(&prepared_batch);

        session
            .batch(&prepared_batch, ((10, 20), (10,), (20,)))
            .await
            .unwrap();
        assert_test_batch_table_rows_contain(&session, &[(10, 20), (10, 7), (8, 20)]).await;
    }

    {
        let mut partially_prepared_batch: Batch = Default::default();
        partially_prepared_batch.append_statement(unprepared_insert_a_b);
        partially_prepared_batch.append_statement(prepared_insert_a_7.clone());
        partially_prepared_batch.append_statement(unprepared_insert_8_b);

        let prepared_batch: Batch = session
            .prepare_batch(&partially_prepared_batch)
            .await
            .unwrap();
        assert_batch_prepared(&prepared_batch);

        session
            .batch(&prepared_batch, ((30, 40), (30,), (40,)))
            .await
            .unwrap();
        assert_test_batch_table_rows_contain(&session, &[(30, 40), (30, 7), (8, 40)]).await;
    }

    {
        let mut fully_prepared_batch: Batch = Default::default();
        fully_prepared_batch.append_statement(prepared_insert_a_b);
        fully_prepared_batch.append_statement(prepared_insert_a_7);
        fully_prepared_batch.append_statement(prepared_insert_8_b);

        let prepared_batch: Batch = session.prepare_batch(&fully_prepared_batch).await.unwrap();
        assert_batch_prepared(&prepared_batch);

        session
            .batch(&prepared_batch, ((50, 60), (50,), (60,)))
            .await
            .unwrap();

        assert_test_batch_table_rows_contain(&session, &[(50, 60), (50, 7), (8, 60)]).await;
    }

    {
        let mut bad_batch: Batch = Default::default();
        bad_batch.append_statement(unprepared_insert_a_b);
        bad_batch.append_statement("This isnt even CQL");
        bad_batch.append_statement(unprepared_insert_8_b);

        assert!(session.prepare_batch(&bad_batch).await.is_err());
    }
}

#[tokio::test]
async fn test_refresh_metadata_after_schema_agreement() {
    let session = create_new_session_builder().build().await.unwrap();

    let ks = unique_keyspace_name();
    session.query_unpaged(format!("CREATE KEYSPACE IF NOT EXISTS {} WITH REPLICATION = {{'class' : 'NetworkTopologyStrategy', 'replication_factor' : 1}}", ks), &[]).await.unwrap();
    session.use_keyspace(ks.clone(), false).await.unwrap();

    session
        .query_unpaged(
            "CREATE TYPE udt (field1 int, field2 uuid, field3 text)",
            &[],
        )
        .await
        .unwrap();

    let cluster_data = session.get_cluster_data();
    let metadata = cluster_data.get_keyspace_info();
    let keyspace_metadata = metadata.get(ks.as_str());
    assert_ne!(keyspace_metadata, None);

    let udt = keyspace_metadata.unwrap().user_defined_types.get("udt");
    assert_ne!(udt, None);

    assert_eq!(
        udt.unwrap(),
        &Arc::new(UserDefinedType {
            keyspace: ks,
            name: "udt".to_string(),
            field_types: Vec::from([
                ("field1".to_string(), CqlType::Native(NativeType::Int)),
                ("field2".to_string(), CqlType::Native(NativeType::Uuid)),
                ("field3".to_string(), CqlType::Native(NativeType::Text))
            ])
        })
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
    session.query_unpaged(format!("CREATE KEYSPACE IF NOT EXISTS {} WITH REPLICATION = {{'class' : 'NetworkTopologyStrategy', 'replication_factor' : 1}}", ks), &[]).await.unwrap();
    session.use_keyspace(ks.clone(), false).await.unwrap();
    session.query_unpaged("CREATE TABLE tbl (pk int PRIMARY KEY, v int) WITH per_partition_rate_limit = {'max_writes_per_second': 1}", ()).await.unwrap();

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

    use crate::transport::errors::OperationType;

    match maybe_err.expect("Rate limit error didn't occur") {
        QueryError::DbError(DbError::RateLimitReached { op_type, .. }, _) => {
            assert_eq!(op_type, OperationType::Write);
        }
        err => panic!("Unexpected error type received: {:?}", err),
    }
}

// Batches containing LWT queries (IF col = som) return rows with information whether the queries were applied.
#[tokio::test]
async fn test_batch_lwts() {
    let session = create_new_session_builder().build().await.unwrap();

    let ks = unique_keyspace_name();
    let mut create_ks = format!("CREATE KEYSPACE {} WITH REPLICATION = {{'class': 'NetworkTopologyStrategy', 'replication_factor': 1}}", ks);
    if scylla_supports_tablets(&session).await {
        create_ks += " and TABLETS = { 'enabled': false}";
    }
    session.query_unpaged(create_ks, &[]).await.unwrap();
    session.use_keyspace(ks.clone(), false).await.unwrap();

    session
        .query_unpaged(
            "CREATE TABLE tab (p1 int, c1 int, r1 int, r2 int, primary key (p1, c1))",
            (),
        )
        .await
        .unwrap();

    session
        .query_unpaged("INSERT INTO tab (p1, c1, r1, r2) VALUES (0, 0, 0, 0)", ())
        .await
        .unwrap();

    let mut batch: Batch = Batch::default();
    batch.append_statement("UPDATE tab SET r2 = 1 WHERE p1 = 0 AND c1 = 0 IF r1 = 0");
    batch.append_statement("INSERT INTO tab (p1, c1, r1, r2) VALUES (0, 123, 321, 312)");
    batch.append_statement("UPDATE tab SET r1 = 1 WHERE p1 = 0 AND c1 = 0 IF r2 = 0");

    let batch_res: QueryResult = session.batch(&batch, ((), (), ())).await.unwrap();

    // Scylla returns 5 columns, but Cassandra returns only 1
    let is_scylla: bool = batch_res.col_specs().len() == 5;

    if is_scylla {
        test_batch_lwts_for_scylla(&session, &batch, batch_res).await;
    } else {
        test_batch_lwts_for_cassandra(&session, &batch, batch_res).await;
    }
}

async fn test_batch_lwts_for_scylla(session: &Session, batch: &Batch, batch_res: QueryResult) {
    // Alias required by clippy
    type IntOrNull = Option<i32>;

    // Returned columns are:
    // [applied], p1, c1, r1, r2
    let batch_res_rows: Vec<(bool, IntOrNull, IntOrNull, IntOrNull, IntOrNull)> = batch_res
        .rows_typed()
        .unwrap()
        .map(|r| r.unwrap())
        .collect();

    let expected_batch_res_rows = vec![
        (true, Some(0), Some(0), Some(0), Some(0)),
        (true, None, None, None, None),
        (true, Some(0), Some(0), Some(0), Some(0)),
    ];

    assert_eq!(batch_res_rows, expected_batch_res_rows);

    let prepared_batch: Batch = session.prepare_batch(batch).await.unwrap();
    let prepared_batch_res: QueryResult =
        session.batch(&prepared_batch, ((), (), ())).await.unwrap();

    let prepared_batch_res_rows: Vec<(bool, IntOrNull, IntOrNull, IntOrNull, IntOrNull)> =
        prepared_batch_res
            .rows_typed()
            .unwrap()
            .map(|r| r.unwrap())
            .collect();

    let expected_prepared_batch_res_rows = vec![
        (false, Some(0), Some(0), Some(1), Some(1)),
        (false, None, None, None, None),
        (false, Some(0), Some(0), Some(1), Some(1)),
    ];

    assert_eq!(prepared_batch_res_rows, expected_prepared_batch_res_rows);
}

async fn test_batch_lwts_for_cassandra(session: &Session, batch: &Batch, batch_res: QueryResult) {
    // Alias required by clippy
    type IntOrNull = Option<i32>;

    // Returned columns are:
    // [applied]
    let batch_res_rows: Vec<(bool,)> = batch_res
        .rows_typed()
        .unwrap()
        .map(|r| r.unwrap())
        .collect();

    let expected_batch_res_rows = vec![(true,)];

    assert_eq!(batch_res_rows, expected_batch_res_rows);

    let prepared_batch: Batch = session.prepare_batch(batch).await.unwrap();
    let prepared_batch_res: QueryResult =
        session.batch(&prepared_batch, ((), (), ())).await.unwrap();

    // Returned columns are:
    // [applied], p1, c1, r1, r2
    let prepared_batch_res_rows: Vec<(bool, IntOrNull, IntOrNull, IntOrNull, IntOrNull)> =
        prepared_batch_res
            .rows_typed()
            .unwrap()
            .map(|r| r.unwrap())
            .collect();

    let expected_prepared_batch_res_rows = vec![(false, Some(0), Some(0), Some(1), Some(1))];

    assert_eq!(prepared_batch_res_rows, expected_prepared_batch_res_rows);
}

#[tokio::test]
async fn test_keyspaces_to_fetch() {
    let ks1 = unique_keyspace_name();
    let ks2 = unique_keyspace_name();

    let session_default = create_new_session_builder().build().await.unwrap();
    for ks in [&ks1, &ks2] {
        session_default
            .query_unpaged(format!("CREATE KEYSPACE {} WITH REPLICATION = {{'class' : 'NetworkTopologyStrategy', 'replication_factor' : 1}}", ks), &[])
            .await
            .unwrap();
    }
    session_default.await_schema_agreement().await.unwrap();
    assert!(session_default
        .get_cluster_data()
        .keyspaces
        .contains_key(&ks1));
    assert!(session_default
        .get_cluster_data()
        .keyspaces
        .contains_key(&ks2));

    let session1 = create_new_session_builder()
        .keyspaces_to_fetch([&ks1])
        .build()
        .await
        .unwrap();
    assert!(session1.get_cluster_data().keyspaces.contains_key(&ks1));
    assert!(!session1.get_cluster_data().keyspaces.contains_key(&ks2));

    let session_all = create_new_session_builder()
        .keyspaces_to_fetch([] as [String; 0])
        .build()
        .await
        .unwrap();
    assert!(session_all.get_cluster_data().keyspaces.contains_key(&ks1));
    assert!(session_all.get_cluster_data().keyspaces.contains_key(&ks2));
}

// Reproduces the problem with execute_iter mentioned in #608.
#[tokio::test]
async fn test_iter_works_when_retry_policy_returns_ignore_write_error() {
    setup_tracing();
    // It's difficult to reproduce the issue with a real downgrading consistency policy,
    // as it would require triggering a WriteTimeout. We just need the policy
    // to return IgnoreWriteError, so we will trigger a different error
    // and use a custom retry policy which returns IgnoreWriteError.
    let retried_flag = Arc::new(AtomicBool::new(false));

    #[derive(Debug)]
    struct MyRetryPolicy(Arc<AtomicBool>);
    impl RetryPolicy for MyRetryPolicy {
        fn new_session(&self) -> Box<dyn RetrySession> {
            Box::new(MyRetrySession(self.0.clone()))
        }
        fn clone_boxed(&self) -> Box<dyn RetryPolicy> {
            Box::new(MyRetryPolicy(self.0.clone()))
        }
    }

    struct MyRetrySession(Arc<AtomicBool>);
    impl RetrySession for MyRetrySession {
        fn decide_should_retry(&mut self, _: QueryInfo) -> RetryDecision {
            self.0.store(true, Ordering::Relaxed);
            RetryDecision::IgnoreWriteError
        }
        fn reset(&mut self) {}
    }

    let handle = ExecutionProfile::builder()
        .consistency(Consistency::All)
        .retry_policy(Box::new(MyRetryPolicy(retried_flag.clone())))
        .build()
        .into_handle();

    let session = create_new_session_builder()
        .default_execution_profile_handle(handle)
        .build()
        .await
        .unwrap();

    // Create a keyspace with replication factor that is larger than the cluster size
    let cluster_size = session.get_cluster_data().get_nodes_info().len();
    let ks = unique_keyspace_name();
    let mut create_ks = format!("CREATE KEYSPACE {} WITH REPLICATION = {{'class': 'NetworkTopologyStrategy', 'replication_factor': {}}}", ks, cluster_size + 1);
    if scylla_supports_tablets(&session).await {
        create_ks += " and TABLETS = { 'enabled': false}";
    }
    session.query_unpaged(create_ks, ()).await.unwrap();
    session.use_keyspace(ks, true).await.unwrap();
    session
        .query_unpaged("CREATE TABLE t (pk int PRIMARY KEY, v int)", ())
        .await
        .unwrap();

    assert!(!retried_flag.load(Ordering::Relaxed));
    // Try to write something to the new table - it should fail and the policy
    // will tell us to ignore the error
    let mut iter = session
        .query_iter("INSERT INTO t (pk v) VALUES (1, 2)", ())
        .await
        .unwrap();

    assert!(retried_flag.load(Ordering::Relaxed));
    while iter.try_next().await.unwrap().is_some() {}

    retried_flag.store(false, Ordering::Relaxed);
    // Try the same with execute_iter()
    let p = session
        .prepare("INSERT INTO t (pk, v) VALUES (?, ?)")
        .await
        .unwrap();
    let mut iter = session.execute_iter(p, (1, 2)).await.unwrap();

    assert!(retried_flag.load(Ordering::Relaxed));
    while iter.try_next().await.unwrap().is_some() {}
}

#[tokio::test]
async fn test_iter_methods_with_modification_statements() {
    let session = create_new_session_builder().build().await.unwrap();
    let ks = unique_keyspace_name();

    session.query_unpaged(format!("CREATE KEYSPACE IF NOT EXISTS {} WITH REPLICATION = {{'class' : 'NetworkTopologyStrategy', 'replication_factor' : 1}}", ks), &[]).await.unwrap();
    session
        .query_unpaged(
            format!(
                "CREATE TABLE IF NOT EXISTS {}.t (a int, b int, c text, primary key (a, b))",
                ks
            ),
            &[],
        )
        .await
        .unwrap();

    let mut query = Query::from(format!(
        "INSERT INTO {}.t (a, b, c) VALUES (1, 2, 'abc')",
        ks
    ));
    query.set_tracing(true);
    let mut row_iterator = session.query_iter(query, &[]).await.unwrap();
    row_iterator.next().await.ok_or(()).unwrap_err(); // assert empty
    assert!(!row_iterator.get_tracing_ids().is_empty());

    let prepared_statement = session
        .prepare(format!("INSERT INTO {}.t (a, b, c) VALUES (?, ?, ?)", ks))
        .await
        .unwrap();
    let mut row_iterator = session
        .execute_iter(prepared_statement, (2, 3, "cba"))
        .await
        .unwrap();
    row_iterator.next().await.ok_or(()).unwrap_err(); // assert empty
}

#[tokio::test]
async fn test_get_keyspace_name() {
    let ks = unique_keyspace_name();

    // Create the keyspace
    // No keyspace is set in config, so get_keyspace() should return None.
    let session = create_new_session_builder().build().await.unwrap();
    assert_eq!(session.get_keyspace(), None);
    session.query_unpaged(format!("CREATE KEYSPACE IF NOT EXISTS {} WITH REPLICATION = {{'class' : 'NetworkTopologyStrategy', 'replication_factor' : 1}}", ks), &[]).await.unwrap();
    assert_eq!(session.get_keyspace(), None);

    // Call use_keyspace(), get_keyspace now should return the new keyspace name
    session.use_keyspace(&ks, true).await.unwrap();
    assert_eq!(*session.get_keyspace().unwrap(), ks);

    // Creating a new session with the keyspace set in config should cause
    // get_keyspace to return that name
    let session = create_new_session_builder()
        .use_keyspace(&ks, true)
        .build()
        .await
        .unwrap();
    assert_eq!(*session.get_keyspace().unwrap(), ks);
}

/// It's recommended to use NetworkTopologyStrategy everywhere, so most tests use only NetworkTopologyStrategy.
/// We still support SimpleStrategy, so to make sure that SimpleStrategy works correctly this test runs
/// a few queries in a SimpleStrategy keyspace.
#[tokio::test]
async fn simple_strategy_test() {
    let ks = unique_keyspace_name();
    let session = create_new_session_builder().build().await.unwrap();

    session
        .query_unpaged(
            format!(
                "CREATE KEYSPACE {} WITH REPLICATION = \
                {{'class': 'SimpleStrategy', 'replication_factor': 1}}",
                ks
            ),
            (),
        )
        .await
        .unwrap();

    session
        .query_unpaged(
            format!(
                "CREATE TABLE {}.tab (p int, c int, r int, PRIMARY KEY (p, c, r))",
                ks
            ),
            (),
        )
        .await
        .unwrap();

    session
        .query_unpaged(
            format!("INSERT INTO {}.tab (p, c, r) VALUES (1, 2, 3)", ks),
            (),
        )
        .await
        .unwrap();

    session
        .query_unpaged(
            format!("INSERT INTO {}.tab (p, c, r) VALUES (?, ?, ?)", ks),
            (4, 5, 6),
        )
        .await
        .unwrap();

    let prepared = session
        .prepare(format!("INSERT INTO {}.tab (p, c, r) VALUES (?, ?, ?)", ks))
        .await
        .unwrap();

    session.execute_unpaged(&prepared, (7, 8, 9)).await.unwrap();

    let mut rows: Vec<(i32, i32, i32)> = session
        .query_unpaged(format!("SELECT p, c, r FROM {}.tab", ks), ())
        .await
        .unwrap()
        .rows_typed::<(i32, i32, i32)>()
        .unwrap()
        .map(|r| r.unwrap())
        .collect::<Vec<(i32, i32, i32)>>();
    rows.sort();

    assert_eq!(rows, vec![(1, 2, 3), (4, 5, 6), (7, 8, 9)]);
}

#[tokio::test]
async fn test_manual_primary_key_computation() {
    // Setup session
    let ks = unique_keyspace_name();
    let session = create_new_session_builder().build().await.unwrap();
    session.query_unpaged(format!("CREATE KEYSPACE IF NOT EXISTS {} WITH REPLICATION = {{'class' : 'NetworkTopologyStrategy', 'replication_factor' : 1}}", ks), &[]).await.unwrap();
    session.use_keyspace(&ks, true).await.unwrap();

    async fn assert_tokens_equal(
        session: &Session,
        prepared: &PreparedStatement,
        serialized_pk_values_in_pk_order: &SerializedValues,
        all_values_in_query_order: impl SerializeRow,
    ) {
        let token_by_prepared = prepared
            .calculate_token(&all_values_in_query_order)
            .unwrap()
            .unwrap();

        session
            .execute_unpaged(prepared, all_values_in_query_order)
            .await
            .unwrap();

        let token_by_hand = calculate_token_for_partition_key(
            serialized_pk_values_in_pk_order,
            &Murmur3Partitioner,
        )
        .unwrap();
        println!(
            "by_prepared: {}, by_hand: {}",
            token_by_prepared.value(),
            token_by_hand.value()
        );
        assert_eq!(token_by_prepared, token_by_hand);
    }

    // Single-column partition key
    {
        session
            .query_unpaged(
                "CREATE TABLE IF NOT EXISTS t2 (a int, b int, c text, primary key (a, b))",
                &[],
            )
            .await
            .unwrap();

        // Values are given non partition key order,
        let prepared_simple_pk = session
            .prepare("INSERT INTO t2 (a, b, c) VALUES (?, ?, ?)")
            .await
            .unwrap();

        let mut pk_values_in_pk_order = SerializedValues::new();
        pk_values_in_pk_order
            .add_value(&17_i32, &ColumnType::Int)
            .unwrap();
        let all_values_in_query_order = (17_i32, 16_i32, "I'm prepared!!!");

        assert_tokens_equal(
            &session,
            &prepared_simple_pk,
            &pk_values_in_pk_order,
            all_values_in_query_order,
        )
        .await;
    }

    // Composite partition key
    {
        session
            .query_unpaged("CREATE TABLE IF NOT EXISTS complex_pk (a int, b int, c text, d int, e int, primary key ((a,b,c),d))", &[])
            .await
            .unwrap();

        // Values are given in non partition key order, to check that such permutation
        // still yields consistent hashes.
        let prepared_complex_pk = session
            .prepare("INSERT INTO complex_pk (a, d, c, b) VALUES (?, 7, ?, ?)")
            .await
            .unwrap();

        let mut pk_values_in_pk_order = SerializedValues::new();
        pk_values_in_pk_order
            .add_value(&17_i32, &ColumnType::Int)
            .unwrap();
        pk_values_in_pk_order
            .add_value(&16_i32, &ColumnType::Int)
            .unwrap();
        pk_values_in_pk_order
            .add_value(&"I'm prepared!!!", &ColumnType::Ascii)
            .unwrap();
        let all_values_in_query_order = (17_i32, "I'm prepared!!!", 16_i32);

        assert_tokens_equal(
            &session,
            &prepared_complex_pk,
            &pk_values_in_pk_order,
            all_values_in_query_order,
        )
        .await;
    }
}

#[cfg(cassandra_tests)]
#[tokio::test]
async fn test_vector_type_metadata() {
    setup_tracing();
    let session = create_new_session_builder().build().await.unwrap();
    let ks = unique_keyspace_name();

    session.query_unpaged(format!("CREATE KEYSPACE IF NOT EXISTS {} WITH REPLICATION = {{'class' : 'NetworkTopologyStrategy', 'replication_factor' : 1}}", ks), &[]).await.unwrap();
    session
        .query_unpaged(
            format!(
                "CREATE TABLE IF NOT EXISTS {}.t (a int PRIMARY KEY, b vector<int, 4>, c vector<text, 2>)",
                ks
            ),
            &[],
        )
        .await
        .unwrap();

    session.refresh_metadata().await.unwrap();
    let metadata = session.get_cluster_data();
    let columns = &metadata.keyspaces[&ks].tables["t"].columns;
    assert_eq!(
        columns["b"].type_,
        CqlType::Vector {
            type_: Box::new(CqlType::Native(NativeType::Int)),
            dimensions: 4,
        },
    );
    assert_eq!(
        columns["c"].type_,
        CqlType::Vector {
            type_: Box::new(CqlType::Native(NativeType::Text)),
            dimensions: 2,
        },
    );
}

#[cfg(cassandra_tests)]
#[tokio::test]
async fn test_vector_type_unprepared() {
    setup_tracing();
    let session = create_new_session_builder().build().await.unwrap();
    let ks = unique_keyspace_name();

    session.query_unpaged(format!("CREATE KEYSPACE IF NOT EXISTS {} WITH REPLICATION = {{'class' : 'NetworkTopologyStrategy', 'replication_factor' : 1}}", ks), &[]).await.unwrap();
    session
        .query_unpaged(
            format!(
                "CREATE TABLE IF NOT EXISTS {}.t (a int PRIMARY KEY, b vector<int, 4>, c vector<text, 2>)",
                ks
            ),
            &[],
        )
        .await
        .unwrap();

    session
        .query_unpaged(
            format!(
                "INSERT INTO {}.t (a, b, c) VALUES (1, [1, 2, 3, 4], ['foo', 'bar'])",
                ks
            ),
            &[],
        )
        .await
        .unwrap();

    // TODO: Implement and test SELECT statements and bind values (`?`)
}

#[cfg(cassandra_tests)]
#[tokio::test]
async fn test_vector_type_prepared() {
    setup_tracing();
    let session = create_new_session_builder().build().await.unwrap();
    let ks = unique_keyspace_name();

    session.query_unpaged(format!("CREATE KEYSPACE IF NOT EXISTS {} WITH REPLICATION = {{'class' : 'NetworkTopologyStrategy', 'replication_factor' : 1}}", ks), &[]).await.unwrap();
    session
        .query_unpaged(
            format!(
                "CREATE TABLE IF NOT EXISTS {}.t (a int PRIMARY KEY, b vector<int, 4>, c vector<text, 2>)",
                ks
            ),
            &[],
        )
        .await
        .unwrap();

    let prepared_statement = session
        .prepare(format!(
            "INSERT INTO {}.t (a, b, c) VALUES (?, [11, 12, 13, 14], ['afoo', 'abar'])",
            ks
        ))
        .await
        .unwrap();
    session
        .execute_unpaged(&prepared_statement, &(2,))
        .await
        .unwrap();

    // TODO: Implement and test SELECT statements and bind values (`?`)
}
