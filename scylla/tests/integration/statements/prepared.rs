use scylla::{
    response::{PagingState, PagingStateResponse},
    routing::{partitioner::PartitionerName, Token},
    statement::Statement,
};

use crate::utils::{
    create_new_session_builder, scylla_supports_tablets, setup_tracing, unique_keyspace_name,
    PerformDDL as _,
};

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
        let prepared_token = prepared_statement
            .calculate_token(&values)
            .unwrap()
            .unwrap();
        assert_eq!(token, prepared_token);
        let cluster_state_token = session
            .get_cluster_state()
            .compute_token(&ks, "t2", &(values.0,))
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
        let prepared_token = prepared_complex_pk_statement
            .calculate_token(&values)
            .unwrap()
            .unwrap();
        assert_eq!(token, prepared_token);
        let cluster_state_token = session
            .get_cluster_state()
            .compute_token(&ks, "complex_pk", &values)
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
        let query = Statement::new(format!("SELECT a, b, c FROM {}.t2", ks)).with_page_size(1);
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
}

/// Tests that PreparedStatement inherits the StatementConfig from Statement
/// that it originates from.
// Commit message:
//
// statement: preserve query configuration during prepare()
// When preparing a statement based on a Query instance, we should preserve the whole
// configuration. It’s especially important for CachingSession users, because prepare()
// happens implicitly in this case, so there’s no other way to customize the configuration
// for a particular query.
// Fixes #340
#[tokio::test]
async fn test_prepared_config() {
    setup_tracing();
    let session = create_new_session_builder().build().await.unwrap();

    let mut query = Statement::new("SELECT * FROM system_schema.tables");
    query.set_is_idempotent(true);
    query.set_page_size(42);

    let prepared_statement = session.prepare(query).await.unwrap();

    assert!(prepared_statement.get_is_idempotent());
    assert_eq!(prepared_statement.get_page_size(), 42);
}

#[tokio::test]
async fn test_prepared_partitioner() {
    setup_tracing();

    let session = create_new_session_builder().build().await.unwrap();
    let ks = unique_keyspace_name();

    // This test uses CDC which is not yet compatible with Scylla's tablets.
    let mut create_ks = format!(
        "CREATE KEYSPACE IF NOT EXISTS {ks} WITH REPLICATION = {{'class' : 'NetworkTopologyStrategy', 'replication_factor' : 1}}");
    if scylla_supports_tablets(&session).await {
        create_ks += " AND TABLETS = {'enabled': false}"
    }

    session.ddl(create_ks).await.unwrap();
    session.use_keyspace(ks, false).await.unwrap();

    session
        .ddl("CREATE TABLE IF NOT EXISTS t1 (a int primary key)")
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
        .ddl("CREATE TABLE IF NOT EXISTS t2 (a int primary key) WITH cdc = {'enabled':true}")
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
