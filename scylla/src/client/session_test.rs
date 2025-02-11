use super::session_builder::SessionBuilder;
use crate as scylla;
use crate::cluster::metadata::{ColumnType, NativeType};
use crate::query::Query;
use crate::routing::partitioner::{Murmur3Partitioner, Partitioner};
use crate::routing::Token;
use crate::utils::test_utils::{
    create_new_session_builder, setup_tracing, unique_keyspace_name, PerformDDL,
};
use futures::FutureExt;
use scylla_cql::frame::request::query::{PagingState, PagingStateResponse};
use scylla_cql::serialize::row::SerializedValues;
use tokio::net::TcpListener;

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
async fn test_prepared_statement() {
    setup_tracing();
    let session = create_new_session_builder().build().await.unwrap();
    let ks = unique_keyspace_name();

    session.ddl(format!("CREATE KEYSPACE IF NOT EXISTS {} WITH REPLICATION = {{'class' : 'NetworkTopologyStrategy', 'replication_factor' : 1}}", ks)).await.unwrap();
    session
        .ddl(format!(
            "CREATE TABLE IF NOT EXISTS {}.t2 (a int, b int, c text, primary key (a, b))",
            ks
        ))
        .await
        .unwrap();
    session
        .ddl(format!("CREATE TABLE IF NOT EXISTS {}.complex_pk (a int, b int, c text, d int, e int, primary key ((a,b,c),d))", ks))
        .await
        .unwrap();

    // Refresh metadata as `ClusterState::compute_token` use them
    session.await_schema_agreement().await.unwrap();
    session.refresh_metadata().await.unwrap();

    let prepared_statement = session
        .prepare(format!("SELECT a, b, c FROM {}.t2", ks))
        .await
        .unwrap();
    let query_result = session.execute_iter(prepared_statement, &[]).await.unwrap();
    let specs = query_result.column_specs();
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
            .into_rows_result()
            .unwrap()
            .single_row::<(i64,)>()
            .unwrap();
        let token = Token::new(value);
        let prepared_token = Murmur3Partitioner
            .hash_one(&prepared_statement.compute_partition_key(&values).unwrap());
        assert_eq!(token, prepared_token);
        let mut pk = SerializedValues::new();
        pk.add_value(&17_i32, &ColumnType::Native(NativeType::Int))
            .unwrap();
        let cluster_state_token = session
            .get_cluster_state()
            .compute_token(&ks, "t2", &pk)
            .unwrap();
        assert_eq!(token, cluster_state_token);
    }
    {
        let (value,): (i64,) = session
            .query_unpaged(format!("SELECT token(a,b,c) FROM {}.complex_pk", ks), &[])
            .await
            .unwrap()
            .into_rows_result()
            .unwrap()
            .single_row::<(i64,)>()
            .unwrap();
        let token = Token::new(value);
        let prepared_token = Murmur3Partitioner.hash_one(
            &prepared_complex_pk_statement
                .compute_partition_key(&values)
                .unwrap(),
        );
        assert_eq!(token, prepared_token);
        let cluster_state_token = session
            .get_cluster_state()
            .compute_token(&ks, "complex_pk", &serialized_values_complex_pk)
            .unwrap();
        assert_eq!(token, cluster_state_token);
    }

    // Verify that correct data was inserted
    {
        let rs = session
            .query_unpaged(format!("SELECT a,b,c FROM {}.t2", ks), &[])
            .await
            .unwrap()
            .into_rows_result()
            .unwrap()
            .rows::<(i32, i32, String)>()
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        let r = &rs[0];
        assert_eq!(r, &(17, 16, String::from("I'm prepared!!!")));

        let mut results_from_manual_paging = vec![];
        let query = Query::new(format!("SELECT a, b, c FROM {}.t2", ks)).with_page_size(1);
        let prepared_paged = session.prepare(query).await.unwrap();
        let mut paging_state = PagingState::start();
        let mut watchdog = 0;
        loop {
            let (rs_manual, paging_state_response) = session
                .execute_single_page(&prepared_paged, &[], paging_state)
                .await
                .unwrap();
            let mut page_results = rs_manual
                .into_rows_result()
                .unwrap()
                .rows::<(i32, i32, String)>()
                .unwrap()
                .collect::<Result<Vec<_>, _>>()
                .unwrap();
            results_from_manual_paging.append(&mut page_results);
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
            .into_rows_result()
            .unwrap()
            .single_row::<(i32, i32, String, i32, Option<i32>)>()
            .unwrap();
        assert!(e.is_none());
        assert_eq!(
            (a, b, c.as_str(), d, e),
            (17, 16, "I'm prepared!!!", 7, None)
        );
    }
    // Check that SerializeRow and DeserializeRow macros work
    {
        #[derive(scylla::SerializeRow, scylla::DeserializeRow, PartialEq, Debug, Clone)]
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
            .into_rows_result()
            .unwrap()
            .single_row()
            .unwrap();
        assert_eq!(input, output)
    }
}

#[tokio::test]
async fn test_token_calculation() {
    setup_tracing();
    let session = create_new_session_builder().build().await.unwrap();
    let ks = unique_keyspace_name();

    session.ddl(format!("CREATE KEYSPACE IF NOT EXISTS {} WITH REPLICATION = {{'class' : 'NetworkTopologyStrategy', 'replication_factor' : 1}}", ks)).await.unwrap();
    session
        .ddl(format!(
            "CREATE TABLE IF NOT EXISTS {}.t3 (a text primary key)",
            ks
        ))
        .await
        .unwrap();

    // Refresh metadata as `ClusterState::compute_token` use them
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
            .into_rows_result()
            .unwrap()
            .single_row::<(i64,)>()
            .unwrap();
        let token = Token::new(value);
        let prepared_token = Murmur3Partitioner
            .hash_one(&prepared_statement.compute_partition_key(&values).unwrap());
        assert_eq!(token, prepared_token);
        let cluster_state_token = session
            .get_cluster_state()
            .compute_token(&ks, "t3", &serialized_values)
            .unwrap();
        assert_eq!(token, cluster_state_token);
    }
}
