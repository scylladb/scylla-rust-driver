use crate::utils::{
    PerformDDL as _, create_new_session_builder, disable_tablets_unless_supported, setup_tracing,
    unique_keyspace_name,
};
use assert_matches::assert_matches;
use scylla::client::session::Session;
use scylla::errors::{BadQuery, DbError, ExecutionError, RequestAttemptError};
use scylla::frame::frame_errors::{BatchSerializationError, CqlRequestSerializationError};
use scylla::response::query_result::{QueryResult, QueryRowsResult};
use scylla::statement::batch::{Batch, BatchStatement, BatchType};
use scylla::statement::prepared::PreparedStatement;
use scylla::statement::unprepared::Statement;
use scylla::value::{Counter, CqlValue, MaybeUnset, Row};
use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;

#[tokio::test]
async fn batch_statements_and_values_mismatch_detected() {
    setup_tracing();
    let session = create_new_session_builder().build().await.unwrap();
    let ks = unique_keyspace_name();
    session.ddl(format!("CREATE KEYSPACE IF NOT EXISTS {ks} WITH REPLICATION = {{'class' : 'NetworkTopologyStrategy', 'replication_factor' : 1}}")).await.unwrap();
    session.use_keyspace(&ks, false).await.unwrap();
    session
        .ddl("CREATE TABLE IF NOT EXISTS batch_serialization_test (p int PRIMARY KEY, val int)")
        .await
        .unwrap();

    let mut batch = Batch::new(BatchType::Logged);
    let stmt = session
        .prepare("INSERT INTO batch_serialization_test (p, val) VALUES (?, ?)")
        .await
        .unwrap();
    batch.append_statement(stmt.clone());
    batch.append_statement(Statement::new(
        "INSERT INTO batch_serialization_test (p, val) VALUES (3, 4)",
    ));
    batch.append_statement(stmt);

    // Subtest 1: counts are correct
    {
        session.batch(&batch, &((1, 2), (), (5, 6))).await.unwrap();
    }

    // Subtest 2: not enough values
    {
        let err = session.batch(&batch, &((1, 2), ())).await.unwrap_err();
        assert_matches!(
            err,
            ExecutionError::LastAttemptError(RequestAttemptError::CqlRequestSerialization(
                CqlRequestSerializationError::BatchSerialization(
                    BatchSerializationError::ValuesAndStatementsLengthMismatch {
                        n_value_lists: 2,
                        n_statements: 3
                    }
                )
            ))
        )
    }

    // Subtest 3: too many values
    {
        let err = session
            .batch(&batch, &((1, 2), (), (5, 6), (7, 8)))
            .await
            .unwrap_err();
        assert_matches!(
            err,
            ExecutionError::LastAttemptError(RequestAttemptError::CqlRequestSerialization(
                CqlRequestSerializationError::BatchSerialization(
                    BatchSerializationError::ValuesAndStatementsLengthMismatch {
                        n_value_lists: 4,
                        n_statements: 3
                    }
                )
            ))
        )
    }

    session.ddl(format!("DROP KEYSPACE {ks}")).await.unwrap();
}

#[tokio::test]
#[cfg_attr(cassandra_tests, ignore)]
async fn test_large_batch_statements() {
    setup_tracing();
    let mut session = create_new_session_builder().build().await.unwrap();

    let ks = unique_keyspace_name();
    session = create_test_session(session, &ks).await;

    let max_queries = u16::MAX as usize;
    let batch_insert_result = write_batch(&session, max_queries, &ks).await;

    batch_insert_result.unwrap();

    let too_many_queries = u16::MAX as usize + 1;
    let batch_insert_result = write_batch(&session, too_many_queries, &ks).await;
    assert_matches!(
        batch_insert_result.unwrap_err(),
        ExecutionError::BadQuery(BadQuery::TooManyQueriesInBatchStatement(_too_many_queries)) if _too_many_queries == too_many_queries
    );

    session.ddl(format!("DROP KEYSPACE {ks}")).await.unwrap();
}

async fn create_test_session(session: Session, ks: &str) -> Session {
    session
        .ddl(
            format!("CREATE KEYSPACE {ks} WITH REPLICATION = {{ 'class' : 'NetworkTopologyStrategy', 'replication_factor' : 1 }}"),
        )
        .await.unwrap();
    session
        .ddl(format!(
            "CREATE TABLE {ks}.pairs (dummy int, k blob, v blob, primary key (dummy, k))"
        ))
        .await
        .unwrap();
    session
}

async fn write_batch(session: &Session, n: usize, ks: &str) -> Result<QueryResult, ExecutionError> {
    let mut batch_query = Batch::new(BatchType::Unlogged);
    let mut batch_values = Vec::new();
    let statement_str = format!("INSERT INTO {ks}.pairs (dummy, k, v) VALUES (0, ?, ?)");
    let statement = Statement::new(statement_str);
    let prepared_statement = session.prepare(statement).await.unwrap();
    for i in 0..n {
        let mut key = vec![0];
        key.extend(i.to_be_bytes().as_slice());
        let value = key.clone();
        let values = vec![key, value];
        batch_values.push(values);
        batch_query.append_statement(prepared_statement.clone());
    }
    session.batch(&batch_query, batch_values).await
}

#[tokio::test]
async fn test_quietly_prepare_batch() {
    setup_tracing();
    let session = create_new_session_builder().build().await.unwrap();

    let ks = unique_keyspace_name();
    session.ddl(format!("CREATE KEYSPACE IF NOT EXISTS {ks} WITH REPLICATION = {{'class' : 'NetworkTopologyStrategy', 'replication_factor' : 1}}")).await.unwrap();
    session.use_keyspace(ks.clone(), false).await.unwrap();

    session
        .ddl("CREATE TABLE test_batch_table (a int, b int, primary key (a, b))")
        .await
        .unwrap();

    let unprepared_insert_a_b: &str = "insert into test_batch_table (a, b) values (?, ?)";
    let unprepared_insert_a_7: &str = "insert into test_batch_table (a, b) values (?, 7)";
    let unprepared_insert_8_b: &str = "insert into test_batch_table (a, b) values (8, ?)";
    let unprepared_insert_1_2: &str = "insert into test_batch_table (a, b) values (1, 2)";
    let unprepared_insert_2_3: &str = "insert into test_batch_table (a, b) values (2, 3)";
    let unprepared_insert_3_4: &str = "insert into test_batch_table (a, b) values (3, 4)";
    let unprepared_insert_4_5: &str = "insert into test_batch_table (a, b) values (4, 5)";
    let prepared_insert_a_b: PreparedStatement =
        session.prepare(unprepared_insert_a_b).await.unwrap();
    let prepared_insert_a_7: PreparedStatement =
        session.prepare(unprepared_insert_a_7).await.unwrap();
    let prepared_insert_8_b: PreparedStatement =
        session.prepare(unprepared_insert_8_b).await.unwrap();

    {
        let mut fully_prepared_batch: Batch = Default::default();
        fully_prepared_batch.append_statement(prepared_insert_a_b);
        fully_prepared_batch.append_statement(prepared_insert_a_7.clone());
        fully_prepared_batch.append_statement(prepared_insert_8_b);

        session
            .batch(&fully_prepared_batch, ((50, 60), (50,), (60,)))
            .await
            .unwrap();

        assert_test_batch_table_rows_contain(&session, &[(50, 60), (50, 7), (8, 60)]).await;
    }

    {
        let mut unprepared_batch1: Batch = Default::default();
        unprepared_batch1.append_statement(unprepared_insert_1_2);
        unprepared_batch1.append_statement(unprepared_insert_2_3);
        unprepared_batch1.append_statement(unprepared_insert_3_4);

        session
            .batch(&unprepared_batch1, ((), (), ()))
            .await
            .unwrap();
        assert_test_batch_table_rows_contain(&session, &[(1, 2), (2, 3), (3, 4)]).await;
    }

    {
        let mut unprepared_batch2: Batch = Default::default();
        unprepared_batch2.append_statement(unprepared_insert_a_b);
        unprepared_batch2.append_statement(unprepared_insert_a_7);
        unprepared_batch2.append_statement(unprepared_insert_8_b);

        session
            .batch(&unprepared_batch2, ((12, 22), (12,), (22,)))
            .await
            .unwrap();
        assert_test_batch_table_rows_contain(&session, &[(12, 22), (12, 7), (8, 22)]).await;
    }

    {
        let mut partially_prepared_batch: Batch = Default::default();
        partially_prepared_batch.append_statement(unprepared_insert_a_b);
        partially_prepared_batch.append_statement(prepared_insert_a_7);
        partially_prepared_batch.append_statement(unprepared_insert_4_5);

        session
            .batch(&partially_prepared_batch, ((33, 43), (33,), ()))
            .await
            .unwrap();
        assert_test_batch_table_rows_contain(&session, &[(33, 43), (33, 7), (4, 5)]).await;
    }

    session.ddl(format!("DROP KEYSPACE {ks}")).await.unwrap();
}

async fn assert_test_batch_table_rows_contain(sess: &Session, expected_rows: &[(i32, i32)]) {
    let selected_rows: BTreeSet<(i32, i32)> = sess
        .query_unpaged("SELECT a, b FROM test_batch_table", ())
        .await
        .unwrap()
        .into_rows_result()
        .unwrap()
        .rows::<(i32, i32)>()
        .unwrap()
        .map(|r| r.unwrap())
        .collect();
    for expected_row in expected_rows.iter() {
        if !selected_rows.contains(expected_row) {
            panic!("Expected {selected_rows:?} to contain row: {expected_row:?}, but they didn't");
        }
    }
}

// Batches containing LWT queries (IF col = som) return rows with information whether the queries were applied.
#[tokio::test]
async fn test_batch_lwts() {
    let session = create_new_session_builder().build().await.unwrap();

    let ks = unique_keyspace_name();
    // This test uses LWT, which older ScyllaDB versions do not support on tablet keyspaces.
    let create_ks = format!(
        "CREATE KEYSPACE {ks} WITH REPLICATION = {{'class': 'NetworkTopologyStrategy', 'replication_factor': 1}}{}",
        disable_tablets_unless_supported(&session, "LWT_WITH_TABLETS").await
    );
    session.ddl(create_ks).await.unwrap();
    session.use_keyspace(ks.clone(), false).await.unwrap();

    session
        .ddl("CREATE TABLE tab (p1 int, c1 int, r1 int, r2 int, primary key (p1, c1))")
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
    let batch_deserializer = batch_res.into_rows_result().unwrap();

    // Scylla returns 5 columns, but Cassandra returns only 1
    let is_scylla: bool = batch_deserializer.column_specs().len() == 5;

    if is_scylla {
        test_batch_lwts_for_scylla(&session, &batch, &batch_deserializer).await;
    } else {
        test_batch_lwts_for_cassandra(&session, &batch, &batch_deserializer).await;
    }

    session.ddl(format!("DROP KEYSPACE {ks}")).await.unwrap();
}

async fn test_batch_lwts_for_scylla(
    session: &Session,
    batch: &Batch,
    query_rows_result: &QueryRowsResult,
) {
    // Alias required by clippy
    type IntOrNull = Option<i32>;

    // Returned columns are:
    // [applied], p1, c1, r1, r2
    let batch_res_rows: Vec<(bool, IntOrNull, IntOrNull, IntOrNull, IntOrNull)> = query_rows_result
        .rows()
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();

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
            .into_rows_result()
            .unwrap()
            .rows()
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

async fn test_batch_lwts_for_cassandra(
    session: &Session,
    batch: &Batch,
    query_rows_result: &QueryRowsResult,
) {
    // Alias required by clippy
    type IntOrNull = Option<i32>;

    // Returned columns are:
    // [applied]
    let batch_res_rows: Vec<(bool,)> = query_rows_result
        .rows()
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
            .into_rows_result()
            .unwrap()
            .rows()
            .unwrap()
            .map(|r| r.unwrap())
            .collect();

    let expected_prepared_batch_res_rows = vec![(false, Some(0), Some(0), Some(1), Some(1))];

    assert_eq!(prepared_batch_res_rows, expected_prepared_batch_res_rows);
}

#[tokio::test]
async fn test_prepare_batch() {
    let session = create_new_session_builder().build().await.unwrap();

    let ks = unique_keyspace_name();
    session.ddl(format!("CREATE KEYSPACE IF NOT EXISTS {ks} WITH REPLICATION = {{'class' : 'NetworkTopologyStrategy', 'replication_factor' : 1}}")).await.unwrap();
    session.use_keyspace(ks.clone(), false).await.unwrap();

    session
        .ddl("CREATE TABLE test_batch_table (a int, b int, primary key (a, b))")
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

    session.ddl(format!("DROP KEYSPACE {ks}")).await.unwrap();
}

#[tokio::test]
async fn test_batch() {
    setup_tracing();
    let session = Arc::new(create_new_session_builder().build().await.unwrap());
    let ks = unique_keyspace_name();

    session.ddl(format!("CREATE KEYSPACE IF NOT EXISTS {ks} WITH REPLICATION = {{'class' : 'NetworkTopologyStrategy', 'replication_factor' : 1}}")).await.unwrap();
    session
        .ddl(format!(
            "CREATE TABLE IF NOT EXISTS {ks}.t_batch (a int, b int, c text, primary key (a, b))"
        ))
        .await
        .unwrap();

    let prepared_statement = session
        .prepare(format!(
            "INSERT INTO {ks}.t_batch (a, b, c) VALUES (?, ?, ?)"
        ))
        .await
        .unwrap();

    // TODO: Add API that supports binding values to statements in batch creation process,
    // to avoid problem of statements/values count mismatch
    let mut batch: Batch = Default::default();
    batch.append_statement(&format!("INSERT INTO {ks}.t_batch (a, b, c) VALUES (?, ?, ?)")[..]);
    batch.append_statement(&format!("INSERT INTO {ks}.t_batch (a, b, c) VALUES (7, 11, '')")[..]);
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
        .query_unpaged(format!("SELECT a, b, c FROM {ks}.t_batch"), &[])
        .await
        .unwrap()
        .into_rows_result()
        .unwrap()
        .rows::<(i32, i32, String)>()
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
        .ddl(format!(
            "ALTER TABLE {ks}.t_batch WITH gc_grace_seconds = 42"
        ))
        .await
        .unwrap();
    session.batch(&batch, values).await.unwrap();

    let results: Vec<(i32, i32, String)> = session
        .query_unpaged(format!("SELECT a, b, c FROM {ks}.t_batch WHERE a = 4"), &[])
        .await
        .unwrap()
        .into_rows_result()
        .unwrap()
        .rows::<(i32, i32, String)>()
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();

    assert_eq!(results, vec![(4, 20, String::from("foobar"))]);

    session.ddl(format!("DROP KEYSPACE {ks}")).await.unwrap();
}

#[tokio::test]
async fn test_counter_batch() {
    setup_tracing();
    let session = Arc::new(create_new_session_builder().build().await.unwrap());
    let ks = unique_keyspace_name();

    // This test uses counters, which older ScyllaDB versions do not support
    // on tablet keyspaces.
    let create_ks = format!(
        "CREATE KEYSPACE IF NOT EXISTS {ks} WITH REPLICATION = {{'class' : 'NetworkTopologyStrategy', 'replication_factor' : 1}}{}",
        disable_tablets_unless_supported(&session, "COUNTERS_WITH_TABLETS").await
    );
    session.ddl(create_ks).await.unwrap();
    session
        .ddl(format!(
            "CREATE TABLE IF NOT EXISTS {ks}.t_batch (key int PRIMARY KEY, value counter)"
        ))
        .await
        .unwrap();

    let statement_str = format!("UPDATE {ks}.t_batch SET value = value + ? WHERE key = ?");
    let query = Statement::from(statement_str);
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

    session.ddl(format!("DROP KEYSPACE {ks}")).await.unwrap();
}

// This is a regression test for #1134.
#[tokio::test]
async fn test_batch_to_multiple_tables() {
    setup_tracing();
    let session = create_new_session_builder().build().await.unwrap();
    let ks = unique_keyspace_name();

    session.ddl(format!("CREATE KEYSPACE IF NOT EXISTS {ks} WITH REPLICATION = {{'class' : 'NetworkTopologyStrategy', 'replication_factor' : 1}}")).await.unwrap();
    session.use_keyspace(&ks, true).await.unwrap();
    session
        .ddl("CREATE TABLE IF NOT EXISTS t_batch1 (a int, b int, c text, primary key (a, b))")
        .await
        .unwrap();
    session
        .ddl("CREATE TABLE IF NOT EXISTS t_batch2 (a int, b int, c text, primary key (a, b))")
        .await
        .unwrap();

    let prepared_statement = session
        .prepare(
            "
            BEGIN BATCH
                INSERT INTO t_batch1 (a, b, c) VALUES (?, ?, ?);
                INSERT INTO t_batch2 (a, b, c) VALUES (?, ?, ?);
            APPLY BATCH;
            ",
        )
        .await
        .unwrap();

    session
        .execute_unpaged(&prepared_statement, (1, 2, "ala", 4, 5, "ma"))
        .await
        .unwrap();

    session.ddl(format!("DROP KEYSPACE {ks}")).await.unwrap();
}

/// Tests of batching statements of every supported kind: simple, prepared,
/// batch-prepared, with unset values, with named variables, and mixtures of
/// those - plus the batch types that constrain what may be batched together
/// (LWT/CAS, counter, logged).
///
/// All the cases live in one `#[tokio::test]`, sharing a single session and a
/// single keyspace, so that the shared cluster is not burdened with a keyspace
/// per case. The cases that write to the shared table each use a partition
/// key of their own, so they do not observe each other's rows.
#[tokio::test]
async fn test_batches_of_various_statement_kinds() {
    setup_tracing();
    let session = create_new_session_builder().build().await.unwrap();
    let ks = unique_keyspace_name();

    // Some of the cases below use counters, others use LWTs; older ScyllaDB
    // versions support neither on tablet keyspaces.
    let disable_tablets = if disable_tablets_unless_supported(&session, "COUNTERS_WITH_TABLETS")
        .await
        .is_empty()
    {
        disable_tablets_unless_supported(&session, "LWT_WITH_TABLETS").await
    } else {
        disable_tablets_unless_supported(&session, "COUNTERS_WITH_TABLETS").await
    };

    session.ddl(format!(
        "CREATE KEYSPACE {ks} WITH REPLICATION = {{'class' : 'NetworkTopologyStrategy', 'replication_factor' : 1}}{disable_tablets}"
    )).await.unwrap();
    session
        .ddl(format!(
            "CREATE TABLE {ks}.t (k0 text, k1 int, v int, PRIMARY KEY (k0, k1))"
        ))
        .await
        .unwrap();
    for i in 1..=3 {
        session
            .ddl(format!(
                "CREATE TABLE {ks}.counter{i} (k0 text PRIMARY KEY, c counter)"
            ))
            .await
            .unwrap();
    }

    batch_of_simple_statements(&session, &ks).await;
    batch_of_prepared_statements(&session, &ks).await;
    batch_prepared_as_a_whole(&session, &ks).await;
    batch_of_bound_statements_with_unset_values(&session, &ks).await;
    batch_of_bound_statements_with_named_variables(&session, &ks).await;
    batch_of_mixed_bound_and_simple_statements(&session, &ks).await;
    cas_batch_is_applied_only_once(&session, &ks).await;
    counter_batch_increments_counters(&session, &ks).await;
    logged_batch_rejects_counter_increments(&session, &ks).await;
    counter_batch_rejects_non_counter_increments(&session, &ks).await;

    session.ddl(format!("DROP KEYSPACE {ks}")).await.unwrap();
}

/// Number of statements appended to each of the batches below.
const BATCH_COUNT: usize = 100;

/// Asserts that the case identified by `partition` inserted exactly
/// `BATCH_COUNT` rows, each of them holding `v == k1 + 1`.
async fn assert_batch_inserted_rows(session: &Session, ks: &str, partition: &str) {
    let rows: Vec<(String, i32, i32)> = session
        .query_unpaged(
            format!("SELECT k0, k1, v FROM {ks}.t WHERE k0 = ?"),
            (partition,),
        )
        .await
        .unwrap()
        .into_rows_result()
        .unwrap()
        .rows::<(String, i32, i32)>()
        .unwrap()
        .map(Result::unwrap)
        .collect();

    assert_eq!(rows.len(), BATCH_COUNT);
    for (k0, k1, v) in rows {
        assert_eq!(k0, partition);
        assert_eq!(v, k1 + 1);
    }
}

async fn prepare_insert(session: &Session, ks: &str) -> PreparedStatement {
    session
        .prepare(format!("INSERT INTO {ks}.t (k0, k1, v) VALUES (?, ?, ?)"))
        .await
        .unwrap()
}

/// A batch of unprepared statements with all values inlined in their text.
async fn batch_of_simple_statements(session: &Session, ks: &str) {
    let partition = "simple_statements";

    let mut batch = Batch::new(BatchType::Unlogged);
    for i in 0..BATCH_COUNT {
        batch.append_statement(Statement::new(format!(
            "INSERT INTO {ks}.t (k0, k1, v) VALUES ('{partition}', {i}, {})",
            i + 1
        )));
    }
    session.batch(&batch, vec![(); BATCH_COUNT]).await.unwrap();

    assert_batch_inserted_rows(session, ks, partition).await;
}

/// A batch of one prepared statement repeated, each time with its own values.
async fn batch_of_prepared_statements(session: &Session, ks: &str) {
    let partition = "prepared_statements";

    let prepared = prepare_insert(session, ks).await;
    let mut batch = Batch::new(BatchType::Unlogged);
    let mut values = Vec::with_capacity(BATCH_COUNT);
    for i in 0..BATCH_COUNT as i32 {
        batch.append_statement(prepared.clone());
        values.push((partition, i, i + 1));
    }
    session.batch(&batch, values).await.unwrap();

    assert_batch_inserted_rows(session, ks, partition).await;
}

/// A batch of unprepared statements prepared in one go, with
/// [`Session::prepare_batch`], rather than statement by statement.
async fn batch_prepared_as_a_whole(session: &Session, ks: &str) {
    let partition = "prepared_batch";

    let mut batch = Batch::new(BatchType::Unlogged);
    let mut values = Vec::with_capacity(BATCH_COUNT);
    for i in 0..BATCH_COUNT as i32 {
        batch.append_statement(Statement::new(format!(
            "INSERT INTO {ks}.t (k0, k1, v) VALUES (?, ?, ?)"
        )));
        values.push((partition, i, i + 1));
    }
    let prepared_batch = session.prepare_batch(&batch).await.unwrap();
    session.batch(&prepared_batch, values).await.unwrap();

    assert_batch_inserted_rows(session, ks, partition).await;
}

/// An unset value leaves the column as it was, rather than overwriting it
/// with NULL.
async fn batch_of_bound_statements_with_unset_values(session: &Session, ks: &str) {
    let partition = "unset_values";

    let prepared = prepare_insert(session, ks).await;

    let mut batch = Batch::new(BatchType::Unlogged);
    let mut values = Vec::with_capacity(BATCH_COUNT);
    for i in 0..BATCH_COUNT as i32 {
        batch.append_statement(prepared.clone());
        values.push((partition, i, i + 1));
    }
    session.batch(&batch, values).await.unwrap();

    // Update v to (k1 + 2), but for every 20th row leave v unset.
    let mut batch = Batch::new(BatchType::Unlogged);
    let mut values = Vec::with_capacity(BATCH_COUNT);
    for i in 0..BATCH_COUNT as i32 {
        batch.append_statement(prepared.clone());
        values.push((
            MaybeUnset::Set(partition),
            MaybeUnset::Set(i),
            if i % 20 == 0 {
                MaybeUnset::Unset
            } else {
                MaybeUnset::Set(i + 2)
            },
        ));
    }
    session.batch(&batch, values).await.unwrap();

    let rows: Vec<(String, i32, i32)> = session
        .query_unpaged(
            format!("SELECT k0, k1, v FROM {ks}.t WHERE k0 = ?"),
            (partition,),
        )
        .await
        .unwrap()
        .into_rows_result()
        .unwrap()
        .rows::<(String, i32, i32)>()
        .unwrap()
        .map(Result::unwrap)
        .collect();

    assert_eq!(rows.len(), BATCH_COUNT);
    for (k0, k1, v) in rows {
        assert_eq!(k0, partition);
        // The rows left unset retain the value the first batch gave them.
        assert_eq!(v, if k1 % 20 == 0 { k1 + 1 } else { k1 + 2 });
    }
}

/// Values may also be bound by name, rather than by position.
async fn batch_of_bound_statements_with_named_variables(session: &Session, ks: &str) {
    let partition = "named_variables";

    let prepared = session
        .prepare(format!(
            "INSERT INTO {ks}.t (k0, k1, v) VALUES (:k0, :k1, :v)"
        ))
        .await
        .unwrap();

    let mut batch = Batch::new(BatchType::Unlogged);
    let mut values = Vec::with_capacity(BATCH_COUNT);
    for i in 0..BATCH_COUNT as i32 {
        batch.append_statement(prepared.clone());
        values.push(HashMap::from([
            ("k0", CqlValue::Text(partition.to_owned())),
            ("k1", CqlValue::Int(i)),
            ("v", CqlValue::Int(i + 1)),
        ]));
    }
    session.batch(&batch, values).await.unwrap();

    assert_batch_inserted_rows(session, ks, partition).await;
}

/// Prepared and unprepared statements may be mixed within one batch.
async fn batch_of_mixed_bound_and_simple_statements(session: &Session, ks: &str) {
    let partition = "mixed_statements";

    let prepared = prepare_insert(session, ks).await;

    let mut batch = Batch::new(BatchType::Unlogged);
    let mut values: Vec<Vec<CqlValue>> = Vec::with_capacity(BATCH_COUNT);
    for i in 0..BATCH_COUNT as i32 {
        if i % 2 == 1 {
            batch.append_statement(Statement::new(format!(
                "INSERT INTO {ks}.t (k0, k1, v) VALUES ('{partition}', {i}, {})",
                i + 1
            )));
            values.push(vec![]);
        } else {
            batch.append_statement(prepared.clone());
            values.push(vec![
                CqlValue::Text(partition.to_owned()),
                CqlValue::Int(i),
                CqlValue::Int(i + 1),
            ]);
        }
    }
    session.batch(&batch, values).await.unwrap();

    assert_batch_inserted_rows(session, ks, partition).await;
}

/// A batch of conditional (LWT) statements is applied only if the condition
/// holds; running the very same batch again is a no-op.
async fn cas_batch_is_applied_only_once(session: &Session, ks: &str) {
    let partition = "cas_batch";

    // All the statements condition on the same partition, as a CAS batch
    // must not span partitions.
    let prepared = session
        .prepare(format!(
            "INSERT INTO {ks}.t (k0, k1, v) VALUES (?, ?, ?) IF NOT EXISTS"
        ))
        .await
        .unwrap();

    let mut batch = Batch::new(BatchType::Logged);
    let mut values = Vec::with_capacity(BATCH_COUNT);
    for i in 0..BATCH_COUNT as i32 {
        batch.append_statement(prepared.clone());
        values.push((partition, i, i + 1));
    }

    // Scylla answers a CAS batch with `[applied]` followed by the row's
    // columns, Cassandra with `[applied]` alone - so read the first column
    // and ignore whatever follows it.
    let applied = |result: QueryResult| -> bool {
        let row = result
            .into_rows_result()
            .unwrap()
            .first_row::<Row>()
            .unwrap();
        match row.columns.first().unwrap() {
            Some(CqlValue::Boolean(applied)) => *applied,
            other => panic!("Expected `[applied]` to be a boolean, got {other:?}"),
        }
    };

    assert!(
        applied(session.batch(&batch, values.clone()).await.unwrap()),
        "The first CAS batch should have been applied - the rows did not exist yet"
    );
    assert_batch_inserted_rows(session, ks, partition).await;

    assert!(
        !applied(session.batch(&batch, values).await.unwrap()),
        "The second CAS batch should not have been applied - the rows exist by now"
    );
}

/// A counter batch increments the counters it targets.
async fn counter_batch_increments_counters(session: &Session, ks: &str) {
    let partition = "counter_batch";

    let mut batch = Batch::new(BatchType::Counter);
    let mut values = Vec::with_capacity(3);
    for i in 1..=3_i64 {
        batch.append_statement(
            session
                .prepare(format!("UPDATE {ks}.counter{i} SET c = c + ? WHERE k0 = ?"))
                .await
                .unwrap(),
        );
        values.push((Counter(i), partition));
    }
    session.batch(&batch, values).await.unwrap();

    for i in 1..=3_i64 {
        let (Counter(c),) = session
            .query_unpaged(
                format!("SELECT c FROM {ks}.counter{i} WHERE k0 = ?"),
                (partition,),
            )
            .await
            .unwrap()
            .into_rows_result()
            .unwrap()
            .single_row::<(Counter,)>()
            .unwrap();
        assert_eq!(c, i);
    }
}

/// Counter increments may not appear in a LOGGED batch.
async fn logged_batch_rejects_counter_increments(session: &Session, ks: &str) {
    let partition = "logged_batch_with_counters";

    let mut batch = Batch::new(BatchType::Logged);
    let mut values = Vec::with_capacity(3);
    for i in 1..=3_i64 {
        batch.append_statement(
            session
                .prepare(format!("UPDATE {ks}.counter{i} SET c = c + ? WHERE k0 = ?"))
                .await
                .unwrap(),
        );
        values.push((Counter(i), partition));
    }

    let err = session.batch(&batch, values).await.unwrap_err();
    assert_matches!(
        err,
        ExecutionError::LastAttemptError(RequestAttemptError::DbError(DbError::Invalid, _))
    );
}

/// Conversely, a COUNTER batch may not carry a non-counter statement.
async fn counter_batch_rejects_non_counter_increments(session: &Session, ks: &str) {
    let partition = "counter_batch_with_non_counters";

    let mut batch = Batch::new(BatchType::Counter);
    let mut values: Vec<Vec<CqlValue>> = Vec::with_capacity(4);
    for i in 1..=3_i64 {
        batch.append_statement(
            session
                .prepare(format!("UPDATE {ks}.counter{i} SET c = c + ? WHERE k0 = ?"))
                .await
                .unwrap(),
        );
        values.push(vec![
            CqlValue::Counter(Counter(i)),
            CqlValue::Text(partition.to_owned()),
        ]);
    }

    batch.append_statement(prepare_insert(session, ks).await);
    values.push(vec![
        CqlValue::Text(partition.to_owned()),
        CqlValue::Int(0),
        CqlValue::Int(1),
    ]);

    let err = session.batch(&batch, values).await.unwrap_err();
    assert_matches!(
        err,
        ExecutionError::LastAttemptError(RequestAttemptError::DbError(DbError::Invalid, _))
    );
}
