use crate::utils::{create_new_session_builder, setup_tracing, unique_keyspace_name, PerformDDL};
use assert_matches::assert_matches;
use scylla::client::session::Session;
use scylla::errors::{BadQuery, ExecutionError, RequestAttemptError};
use scylla::frame::frame_errors::{BatchSerializationError, CqlRequestSerializationError};
use scylla::response::query_result::QueryResult;
use scylla::statement::batch::{Batch, BatchType};
use scylla::statement::prepared::PreparedStatement;
use scylla::statement::unprepared::Statement;
use std::collections::BTreeSet;

#[tokio::test]
#[ntest::timeout(60000)]
async fn batch_statements_and_values_mismatch_detected() {
    setup_tracing();
    let session = create_new_session_builder().build().await.unwrap();
    let ks = unique_keyspace_name();
    session.ddl(format!("CREATE KEYSPACE IF NOT EXISTS {} WITH REPLICATION = {{'class' : 'NetworkTopologyStrategy', 'replication_factor' : 1}}", ks)).await.unwrap();
    session.use_keyspace(ks, false).await.unwrap();
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
}

#[tokio::test]
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
    )
}

async fn create_test_session(session: Session, ks: &String) -> Session {
    session
        .ddl(
            format!("CREATE KEYSPACE {} WITH REPLICATION = {{ 'class' : 'NetworkTopologyStrategy', 'replication_factor' : 1 }}",ks),
        )
        .await.unwrap();
    session
        .ddl(format!(
            "CREATE TABLE {}.pairs (dummy int, k blob, v blob, primary key (dummy, k))",
            ks
        ))
        .await
        .unwrap();
    session
}

async fn write_batch(
    session: &Session,
    n: usize,
    ks: &String,
) -> Result<QueryResult, ExecutionError> {
    let mut batch_query = Batch::new(BatchType::Unlogged);
    let mut batch_values = Vec::new();
    let statement_str = format!("INSERT INTO {}.pairs (dummy, k, v) VALUES (0, ?, ?)", ks);
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
    session.ddl(format!("CREATE KEYSPACE IF NOT EXISTS {} WITH REPLICATION = {{'class' : 'NetworkTopologyStrategy', 'replication_factor' : 1}}", ks)).await.unwrap();
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
            panic!(
                "Expected {:?} to contain row: {:?}, but they didn't",
                selected_rows, expected_row
            );
        }
    }
}
