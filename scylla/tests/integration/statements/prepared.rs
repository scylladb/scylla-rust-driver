use assert_matches::assert_matches;
use itertools::Itertools;
use scylla::client::session::Session;
use scylla::cluster::metadata::{ColumnType, NativeType};
use scylla::errors::{DbError, PrepareError, RequestAttemptError};
use scylla::frame::response::result::{ColumnSpec, TableSpec};
use scylla::policies::load_balancing::{NodeIdentifier, SingleTargetLoadBalancingPolicy};
use scylla::response::{PagingState, PagingStateResponse};
use scylla::routing::Token;
use scylla::routing::partitioner::PartitionerName;
use scylla::serialize::row::SerializeRow;
use scylla::statement::Statement;
use scylla::statement::prepared::PreparedStatement;
use scylla_cql::frame::types;
use scylla_proxy::{
    Condition, ProxyError, Reaction, RequestFrame, RequestOpcode, RequestReaction, RequestRule,
    ResponseFrame, ResponseOpcode, ResponseReaction, ResponseRule, RunningProxy, ShardAwareness,
    TargetShard, WorkerError,
};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::mpsc;
use tracing::{debug, info};
use uuid::Uuid;

use crate::utils::{
    PerformDDL as _, create_new_session_builder, fetch_negotiated_features,
    scylla_supports_tablets, setup_tracing, test_with_3_node_cluster, unique_keyspace_name,
};

#[tokio::test]
async fn test_prepared_statement() {
    setup_tracing();
    let session = create_new_session_builder().build().await.unwrap();
    let ks = unique_keyspace_name();

    session.ddl(format!("CREATE KEYSPACE IF NOT EXISTS {ks} WITH REPLICATION = {{'class' : 'NetworkTopologyStrategy', 'replication_factor' : 1}}")).await.unwrap();
    session
        .ddl(format!(
            "CREATE TABLE IF NOT EXISTS {ks}.t2 (a int, b int, c text, primary key (a, b))"
        ))
        .await
        .unwrap();
    session
        .ddl(format!("CREATE TABLE IF NOT EXISTS {ks}.complex_pk (a int, b int, c text, d int, e int, primary key ((a,b,c),d))"))
        .await
        .unwrap();

    // Refresh metadata as `ClusterState::compute_token` use them
    session.await_schema_agreement().await.unwrap();
    session.refresh_metadata().await.unwrap();

    let prepared_statement = session
        .prepare(format!("SELECT a, b, c FROM {ks}.t2"))
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
        .prepare(format!("INSERT INTO {ks}.t2 (a, b, c) VALUES (?, ?, ?)"))
        .await
        .unwrap();
    let prepared_complex_pk_statement = session
        .prepare(format!(
            "INSERT INTO {ks}.complex_pk (a, b, c, d) VALUES (?, ?, ?, 7)"
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
            .query_unpaged(format!("SELECT token(a) FROM {ks}.t2"), &[])
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
            .query_unpaged(format!("SELECT token(a,b,c) FROM {ks}.complex_pk"), &[])
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
            .query_unpaged(format!("SELECT a,b,c FROM {ks}.t2"), &[])
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
        let query = Statement::new(format!("SELECT a, b, c FROM {ks}.t2")).with_page_size(1);
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
            .query_unpaged(format!("SELECT a,b,c,d,e FROM {ks}.complex_pk"), &[])
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

    session.ddl(format!("DROP KEYSPACE {ks}")).await.unwrap();
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
#[cfg_attr(cassandra_tests, ignore)]
async fn test_prepared_partitioner() {
    setup_tracing();

    let session = create_new_session_builder().build().await.unwrap();
    let ks = unique_keyspace_name();

    // This test uses CDC which is not yet compatible with Scylla's tablets.
    let mut create_ks = format!(
        "CREATE KEYSPACE IF NOT EXISTS {ks} WITH REPLICATION = {{'class' : 'NetworkTopologyStrategy', 'replication_factor' : 1}}"
    );
    if scylla_supports_tablets(&session).await {
        create_ks += " AND TABLETS = {'enabled': false}"
    }

    session.ddl(create_ks).await.unwrap();
    session.use_keyspace(&ks, false).await.unwrap();

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

    session.ddl(format!("DROP KEYSPACE {ks}")).await.unwrap();
}

#[tokio::test]
async fn test_token_calculation() {
    setup_tracing();
    let session = create_new_session_builder().build().await.unwrap();
    let ks = unique_keyspace_name();

    session.ddl(format!("CREATE KEYSPACE IF NOT EXISTS {ks} WITH REPLICATION = {{'class' : 'NetworkTopologyStrategy', 'replication_factor' : 1}}")).await.unwrap();
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
            eprintln!("Trying key size {i}");
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

    session.ddl(format!("DROP KEYSPACE {ks}")).await.unwrap();
}

#[tokio::test]
async fn test_prepared_statement_col_specs() {
    setup_tracing();
    let session = create_new_session_builder().build().await.unwrap();

    let ks = unique_keyspace_name();
    session
        .ddl(format!(
            "CREATE KEYSPACE IF NOT EXISTS {ks} WITH REPLICATION =
            {{'class' : 'NetworkTopologyStrategy', 'replication_factor' : 1}}"
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

    let col_specs_guard = prepared.get_current_result_set_col_specs();
    let result_set_col_specs = col_specs_guard.get().as_slice();
    let expected_result_set_col_specs = &[
        spec("k1", ColumnType::Native(NativeType::Int)),
        spec("k2", ColumnType::Native(NativeType::Varint)),
        spec("c1", ColumnType::Native(NativeType::Timestamp)),
        spec("a", ColumnType::Native(NativeType::TinyInt)),
        spec("b", ColumnType::Native(NativeType::Text)),
        spec("c", ColumnType::Native(NativeType::SmallInt)),
    ];
    assert_eq!(result_set_col_specs, expected_result_set_col_specs);

    session.ddl(format!("DROP KEYSPACE {ks}")).await.unwrap();
}

#[tokio::test]
#[ntest::timeout(20000)]
async fn test_skip_result_metadata() {
    use scylla::client::session::Session;
    use scylla::client::session_builder::SessionBuilder;

    setup_tracing();

    const NO_METADATA_FLAG: i32 = 0x0004;

    let res = test_with_3_node_cluster(ShardAwareness::QueryNode, |proxy_uris, translation_map, mut running_proxy| async move {
        let features = fetch_negotiated_features(Some(proxy_uris[0].clone())).await;
        // DB preparation phase
        let session: Session = SessionBuilder::new()
            .known_node(proxy_uris[0].as_str())
            .address_translator(Arc::new(translation_map))
            .build()
            .await
            .unwrap();

        let ks = unique_keyspace_name();
        session.ddl(format!("CREATE KEYSPACE IF NOT EXISTS {ks} WITH REPLICATION = {{'class' : 'NetworkTopologyStrategy', 'replication_factor' : 3}}")).await.unwrap();
        session.use_keyspace(&ks, false).await.unwrap();
        session
            .ddl("CREATE TABLE t (a int primary key, b int, c text)")
            .await
            .unwrap();
        session.query_unpaged("INSERT INTO t (a, b, c) VALUES (1, 2, 'foo_filter_data')", &[]).await.unwrap();

        let mut prepared = session.prepare("SELECT a, b, c FROM t").await.unwrap();

        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();

        // We inserted this string to filter responses
        let body_rows = b"foo_filter_data";
        for node in running_proxy.running_nodes.iter_mut() {
            let rule = ResponseRule(
                Condition::ResponseOpcode(ResponseOpcode::Result).and(Condition::BodyContainsCaseSensitive(Box::new(*body_rows))),
                ResponseReaction::noop().with_feedback_when_performed(tx.clone())
            );
            node.change_response_rules(Some(vec![rule]));
        }

        async fn test_with_flags_predicate(
            session: &Session,
            prepared: &PreparedStatement,
            rx: &mut tokio::sync::mpsc::UnboundedReceiver<(ResponseFrame, Option<TargetShard>)>,
            predicate: impl FnOnce(i32) -> bool
        ) {
            session.execute_unpaged(prepared, &[]).await.unwrap();

            let (frame, _shard) = rx.recv().await.unwrap();
            let mut buf = &*frame.body;

            // FIXME: make use of scylla_cql::frame utilities, instead of deserializing frame manually.
            // This will probably be possible once https://github.com/scylladb/scylla-rust-driver/issues/462 is fixed.
            match types::read_int(&mut buf).unwrap() {
                0x0002 => (),
                _ => panic!("Invalid result type"),
            }
            let result_metadata_flags = types::read_int(&mut buf).unwrap();
            assert!(predicate(result_metadata_flags));
        }

        // Verify that server sends metadata when driver doesn't send SKIP_METADATA flag.
        // If the result metadata extension is enabled, then driver ignores the setting on PreparedStatement
        // and always sends the SKIP_METADATA flag, so NO_METADATA should be present iff extension is enabled.
        prepared.set_use_cached_result_metadata(false);
        test_with_flags_predicate(&session, &prepared, &mut rx, |flags| (flags & NO_METADATA_FLAG != 0) == features.scylla_metadata_id_supported).await;

        // Verify that server doesn't send metadata when driver sends SKIP_METADATA flag.
        prepared.set_use_cached_result_metadata(true);
        test_with_flags_predicate(&session, &prepared, &mut rx, |flags| flags & NO_METADATA_FLAG != 0).await;

        // Verify that the optimisation does not break paging
        {
            let ks = unique_keyspace_name();

            session.ddl(format!("CREATE KEYSPACE IF NOT EXISTS {ks} WITH REPLICATION = {{'class' : 'NetworkTopologyStrategy', 'replication_factor' : 1}}")).await.unwrap();
            session.use_keyspace(&ks, true).await.unwrap();

            type RowT = (i32, i32, String);
            session
                .ddl(
                    "CREATE TABLE IF NOT EXISTS t2 (a int, b int, c text, primary key (a, b))",
                )
                .await
                .unwrap();

            let insert_stmt = session
                .prepare("INSERT INTO t2 (a, b, c) VALUES (?, ?, ?)")
                .await
                .unwrap();

            for idx in 0..10 {
                session
                    .execute_unpaged(&insert_stmt, (idx, idx + 1, "Some text"))
                    .await
                    .unwrap();
            }

            {
                let select_query = "SELECT a, b, c FROM t2";

                let rs = session
                    .query_unpaged(select_query, ())
                    .await
                    .unwrap()
                    .into_rows_result()
                    .unwrap()
                    .rows::<RowT>()
                    .unwrap()
                    .collect::<Result<Vec<_>, _>>()
                    .unwrap();

                let mut results_from_manual_paging: Vec<RowT> = vec![];
                let mut prepared_paged = session.prepare(select_query).await.unwrap();
                prepared_paged.set_use_cached_result_metadata(true);
                prepared_paged.set_page_size(1);
                let mut paging_state = PagingState::start();
                let mut watchdog = 0;
                loop {
                    let (rs_manual, paging_state_response) = session
                        .execute_single_page(&prepared_paged, &[], paging_state)
                        .await
                        .unwrap();
                    results_from_manual_paging.extend(
                        rs_manual.into_rows_result()
                            .unwrap()
                            .rows::<RowT>()
                            .unwrap()
                            .map(Result::unwrap)
                    );

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

            session.ddl(format!("DROP KEYSPACE {ks}")).await.unwrap();
        }

        session.ddl(format!("DROP KEYSPACE {ks}")).await.unwrap();

        running_proxy
    }).await;

    match res {
        Ok(()) => (),
        Err(ProxyError::Worker(WorkerError::DriverDisconnected(_))) => (),
        Err(err) => panic!("{}", err),
    }
}

/// Counts number of feedbacks, aggregated per shard.
/// Unknown shards feedbacks are counted too.
fn count_per_shard_feedbacks(
    rx: &mut mpsc::UnboundedReceiver<(RequestFrame, Option<TargetShard>)>,
) -> HashMap<Option<TargetShard>, usize> {
    let per_shard_feedback_counts = std::iter::from_fn(|| rx.try_recv().ok())
        .map(|(_frame, shard)| shard)
        .counts();

    debug!(
        "Got per_shard_feedback_counts: {:#?}",
        per_shard_feedback_counts
    );

    per_shard_feedback_counts
}

/// Asserts that there was exactly one preparation attempt on every node.
fn assert_preparation_attempted_exactly_once_on_each_node(
    rxs: &mut [mpsc::UnboundedReceiver<(RequestFrame, Option<TargetShard>)>],
) {
    let per_shard_feedbacks_counts = rxs.iter_mut().map(count_per_shard_feedbacks);

    for (node_num, per_shard_feedbacks_count) in per_shard_feedbacks_counts.enumerate() {
        let preparation_attempts_to_node: usize = per_shard_feedbacks_count.values().sum();
        assert_eq!(
            1, preparation_attempts_to_node,
            "Unexpected number of preparation attempts on node {node_num}: expected 1, got {preparation_attempts_to_node}",
        );
    }
}

/// Asserts that there was at least one preparation attempt on every shard.
fn assert_preparation_attempted_on_all_shards(
    rxs: &mut [mpsc::UnboundedReceiver<(RequestFrame, Option<TargetShard>)>],
) {
    let per_shard_feedbacks_counts = rxs.iter_mut().map(count_per_shard_feedbacks);
    for (node_num, per_shard_feedbacks_count) in per_shard_feedbacks_counts.enumerate() {
        if per_shard_feedbacks_count.contains_key(&None) {
            // We are shard-unaware. This is most likely due to Cassandra being our DB, not ScyllaDB.
            // In such case, nothing to check here.
        } else {
            // We are shard-aware. Then let's make sure that preparation was attempted on all shards.
            for (shard, preparation_attempts_to_shard) in per_shard_feedbacks_count
                .into_iter()
                .filter_map(|(shard, count)| shard.map(|shard| (shard, count)))
            {
                assert!(
                    preparation_attempts_to_shard >= 1,
                    "Preparation was not attempted on node {node_num}, shard {shard}"
                );
            }
        }
    }
}

/// Assumptions:
/// - Preparation is expected to succeed iff at least one shard sends a successful PREPARED response.
/// - If preparation succeeds, the statement is executed on all nodes and this way repreparation logic is additionally checked.
///
/// Outline:
/// 1. All nodes enabled -> preparation succeeds.
/// 2. One or two nodes fully disabled -> preparation succeeds.
/// 3. All three nodes fully disabled -> preparation fails, and we assert that all shards were attempted.
/// 4. All three nodes disabled once (simulation of only part of shards broken) -> preparation succeeds,
///     and we assert that all shards were attempted.
///
#[tokio::test]
#[ntest::timeout(30000)]
async fn test_preparation_logic() {
    setup_tracing();

    const STATEMENT: &str = "SELECT host_id FROM system.local WHERE key='local'";

    let res = test_with_3_node_cluster(
        ShardAwareness::QueryNode,
        |proxy_uris, translation_map, mut running_proxy| async move {
            let session = scylla::client::session_builder::SessionBuilder::new()
                .known_node(proxy_uris[0].as_str())
                .address_translator(Arc::new(translation_map))
                .build()
                .await
                .unwrap();
            let cluster_state = session.get_cluster_state();

            // Prepares a statement and asserts that its execution succeeds on all nodes.
            let expect_success = || async {
                let mut prepared = session.prepare(STATEMENT).await.unwrap();

                for node in cluster_state.get_nodes_info() {
                    prepared.set_load_balancing_policy(Some(SingleTargetLoadBalancingPolicy::new(
                        NodeIdentifier::Node(Arc::clone(node)),
                        None,
                    )));
                    let res = session
                        .execute_unpaged(&prepared, ())
                        .await
                        .unwrap()
                        .into_rows_result()
                        .unwrap();
                    let (coordinator_host_id,) = res.single_row::<(Uuid,)>().unwrap();

                    // Just a sanity check that we got the expected result.
                    assert_eq!(coordinator_host_id, node.host_id);
                    assert_eq!(
                        coordinator_host_id,
                        res.request_coordinator().node().host_id
                    );
                }
            };
            let expect_failure = || async {
                assert_matches!(
                    session.prepare(STATEMENT).await,
                    Err(PrepareError::AllAttemptsFailed {
                        first_attempt: RequestAttemptError::DbError(DbError::ServerError, _)
                    })
                );
            };

            struct NodeConfig {
                /// How many times should the node fail preparation until it finally succeeds.
                prepare_rejections_count: usize,
            }

            /// Configures proxy so that nodes reject preparation requested number of times, then always succeed.
            fn configure_proxy<const N: usize>(
                running_proxy: &mut RunningProxy,
                node_config: [NodeConfig; N],
            ) -> [mpsc::UnboundedReceiver<(RequestFrame, Option<TargetShard>)>; N] {
                let (feedback_txs, feedback_rxs): (Vec<_>, Vec<_>) = (0..N)
                    .map(|_| mpsc::unbounded_channel::<(RequestFrame, Option<TargetShard>)>())
                    .unzip();

                running_proxy
                    .running_nodes
                    .iter_mut()
                    .zip(node_config.iter().zip(feedback_txs))
                    .for_each(|(node, (config, tx))| {
                        let noncontrol_prepare_condition = || {
                            Condition::RequestOpcode(RequestOpcode::Prepare)
                                .and(Condition::not(Condition::ConnectionRegisteredAnyEvent))
                        };
                        let accept_rule = || {
                            RequestRule(
                                noncontrol_prepare_condition(),
                                RequestReaction::noop().with_feedback_when_performed(tx.clone()),
                            )
                        };
                        let reject_rule = |rejections| {
                            RequestRule(
                                noncontrol_prepare_condition()
                                    .and(Condition::TrueForLimitedTimes(rejections)),
                                RequestReaction::forge()
                                    .server_error()
                                    .with_feedback_when_performed(tx.clone()),
                            )
                        };
                        let rules = match config.prepare_rejections_count {
                            0 => vec![accept_rule()],
                            n => {
                                vec![reject_rule(n), accept_rule()]
                            }
                        };

                        node.change_request_rules(Some(rules));
                    });

                feedback_rxs.try_into().unwrap()
            }

            // 1. All nodes enabled -> preparation succeeds, and we assert that each node was attempted preparation only once.
            {
                info!("Test case 1: All nodes enabled.");
                let mut rxs = configure_proxy(
                    &mut running_proxy,
                    [0, 0, 0].map(|n| NodeConfig {
                        prepare_rejections_count: n,
                    }),
                );
                expect_success().await;
                assert_preparation_attempted_exactly_once_on_each_node(&mut rxs);
            }

            // 2. One or two nodes fully disabled -> preparation succeeds, and we assert that each node was attempted preparation only once.
            {
                info!("Test case 2: Two nodes disabled.");
                let mut rxs = configure_proxy(
                    &mut running_proxy,
                    [usize::MAX, usize::MAX, 0].map(|n| NodeConfig {
                        prepare_rejections_count: n,
                    }),
                );
                expect_success().await;
                assert_preparation_attempted_exactly_once_on_each_node(&mut rxs);
            }

            // 3. All three nodes fully disabled -> preparation fails, and we assert that all shards were attempted.
            {
                info!("Test case 3: All nodes disabled.");
                let mut rxs = configure_proxy(
                    &mut running_proxy,
                    [usize::MAX, usize::MAX, usize::MAX].map(|n| NodeConfig {
                        prepare_rejections_count: n,
                    }),
                );
                expect_failure().await;
                assert_preparation_attempted_on_all_shards(&mut rxs);
            }

            // 4. All three nodes disabled once (simulation of only part of shards broken) -> preparation succeeds,
            //    and we assert that all shards per each node were attempted.
            {
                info!("Test case 4: Simulated part of shards disabled on every node.");
                let mut rxs = configure_proxy(
                    &mut running_proxy,
                    [1, 1, 1].map(|n| NodeConfig {
                        prepare_rejections_count: n,
                    }),
                );
                expect_success().await;
                assert_preparation_attempted_on_all_shards(&mut rxs);
            }

            running_proxy
        },
    )
    .await;
    match res {
        Ok(()) => (),
        Err(ProxyError::Worker(WorkerError::DriverDisconnected(_))) => (),
        Err(err) => panic!("{}", err),
    }
}
