use scylla::client::session::Session;
use scylla::cluster::metadata::{ColumnType, NativeType};
use scylla::frame::response::result::{ColumnSpec, TableSpec};
use scylla::response::{PagingState, PagingStateResponse};
use scylla::routing::partitioner::PartitionerName;
use scylla::routing::Token;
use scylla::serialize::row::SerializeRow;
use scylla::statement::prepared::PreparedStatement;
use scylla::statement::Statement;

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

#[tokio::test]
async fn test_token_calculation() {
    setup_tracing();
    let session = create_new_session_builder().build().await.unwrap();
    let ks = unique_keyspace_name();

    session.ddl(format!("CREATE KEYSPACE IF NOT EXISTS {} WITH REPLICATION = {{'class' : 'NetworkTopologyStrategy', 'replication_factor' : 1}}", ks)).await.unwrap();
    session.use_keyspace(ks.as_str(), true).await.unwrap();

    #[allow(clippy::too_many_arguments)]
    async fn assert_tokens_equal(
        session: &Session,
        prepared: &PreparedStatement,
        all_values_in_query_order: impl SerializeRow,
        token_select: &PreparedStatement,
        pk_ck_values: impl SerializeRow,
        ks_name: &str,
        table_name: &str,
        pk_values: impl SerializeRow,
    ) {
        session
            .execute_unpaged(prepared, &all_values_in_query_order)
            .await
            .unwrap();

        let (value,): (i64,) = session
            .execute_unpaged(token_select, &pk_ck_values)
            .await
            .unwrap()
            .into_rows_result()
            .unwrap()
            .single_row::<(i64,)>()
            .unwrap();
        let token = Token::new(value);
        let prepared_token = prepared
            .calculate_token(&all_values_in_query_order)
            .unwrap()
            .unwrap();
        assert_eq!(token, prepared_token);
        let cluster_state_token = session
            .get_cluster_state()
            .compute_token(ks_name, table_name, &pk_values)
            .unwrap();
        assert_eq!(token, cluster_state_token);
    }

    // Different sizes of the key
    {
        session
            .ddl("CREATE TABLE IF NOT EXISTS t1 (a text primary key)")
            .await
            .unwrap();

        let prepared_statement = session
            .prepare("INSERT INTO t1 (a) VALUES (?)")
            .await
            .unwrap();

        let token_selection = session
            .prepare("SELECT token(a) FROM t1 WHERE a = ?")
            .await
            .unwrap();

        for i in 1..50usize {
            eprintln!("Trying key size {}", i);
            let mut s = String::new();
            for _ in 0..i {
                s.push('a');
            }
            let values = (&s,);
            assert_tokens_equal(
                &session,
                &prepared_statement,
                &values,
                &token_selection,
                &values,
                ks.as_str(),
                "t1",
                &values,
            )
            .await;
        }
    }

    // Single column PK and single column CK
    {
        session
            .ddl("CREATE TABLE IF NOT EXISTS t2 (a int, b int, c text, primary key (a, b))")
            .await
            .unwrap();

        // Values are given non partition key order,
        let prepared_simple_pk = session
            .prepare("INSERT INTO t2 (c, a, b) VALUES (?, ?, ?)")
            .await
            .unwrap();

        let all_values_in_query_order = ("I'm prepared!!!", 17_i32, 16_i32);

        let token_select = session
            .prepare("SELECT token(a) from t2 WHERE a = ? AND b = ?")
            .await
            .unwrap();
        let pk_ck_values = (17_i32, 16_i32);
        let pk_values = (17_i32,);

        assert_tokens_equal(
            &session,
            &prepared_simple_pk,
            all_values_in_query_order,
            &token_select,
            pk_ck_values,
            ks.as_str(),
            "t2",
            pk_values,
        )
        .await;
    }

    // Composite partition key
    {
        session
        .ddl("CREATE TABLE IF NOT EXISTS complex_pk (a int, b int, c text, d int, e int, primary key ((a,b,c),d))")
        .await
        .unwrap();

        // Values are given in non partition key order, to check that such permutation
        // still yields consistent hashes.
        let prepared_complex_pk = session
            .prepare("INSERT INTO complex_pk (a, d, c, e, b) VALUES (?, ?, ?, ?, ?)")
            .await
            .unwrap();

        let all_values_in_query_order = (17_i32, 7_i32, "I'm prepared!!!", 1234_i32, 16_i32);

        let token_select = session
            .prepare(
                "SELECT token(a, b, c) FROM complex_pk WHERE a = ? AND b = ? AND c = ? AND d = ?",
            )
            .await
            .unwrap();
        let pk_ck_values = (17_i32, 16_i32, "I'm prepared!!!", 7_i32);
        let pk_values = (17_i32, 16_i32, "I'm prepared!!!");

        assert_tokens_equal(
            &session,
            &prepared_complex_pk,
            all_values_in_query_order,
            &token_select,
            pk_ck_values,
            ks.as_str(),
            "complex_pk",
            pk_values,
        )
        .await;
    }
}

#[tokio::test]
async fn test_prepared_statement_col_specs() {
    setup_tracing();
    let session = create_new_session_builder().build().await.unwrap();

    let ks = unique_keyspace_name();
    session
        .ddl(format!(
            "CREATE KEYSPACE IF NOT EXISTS {} WITH REPLICATION =
            {{'class' : 'NetworkTopologyStrategy', 'replication_factor' : 1}}",
            ks
        ))
        .await
        .unwrap();
    session.use_keyspace(&ks, false).await.unwrap();

    session
        .ddl(
            "CREATE TABLE t (k1 int, k2 varint, c1 timestamp,
            a tinyint, b text, c smallint, PRIMARY KEY ((k1, k2), c1))",
        )
        .await
        .unwrap();

    let spec = |name: &'static str, typ: ColumnType<'static>| -> ColumnSpec<'_> {
        ColumnSpec::borrowed(name, typ, TableSpec::borrowed(&ks, "t"))
    };

    let prepared = session
        .prepare("SELECT * FROM t WHERE k1 = ? AND k2 = ? AND c1 > ?")
        .await
        .unwrap();

    let variable_col_specs = prepared.get_variable_col_specs().as_slice();
    let expected_variable_col_specs = &[
        spec("k1", ColumnType::Native(NativeType::Int)),
        spec("k2", ColumnType::Native(NativeType::Varint)),
        spec("c1", ColumnType::Native(NativeType::Timestamp)),
    ];
    assert_eq!(variable_col_specs, expected_variable_col_specs);

    let result_set_col_specs = prepared.get_result_set_col_specs().as_slice();
    let expected_result_set_col_specs = &[
        spec("k1", ColumnType::Native(NativeType::Int)),
        spec("k2", ColumnType::Native(NativeType::Varint)),
        spec("c1", ColumnType::Native(NativeType::Timestamp)),
        spec("a", ColumnType::Native(NativeType::TinyInt)),
        spec("b", ColumnType::Native(NativeType::Text)),
        spec("c", ColumnType::Native(NativeType::SmallInt)),
    ];
    assert_eq!(result_set_col_specs, expected_result_set_col_specs);
}
