use std::collections::BTreeSet;

use scylla::{
    client::session::Session,
    statement::{
        batch::{Batch, BatchStatement},
        prepared::PreparedStatement,
    },
};

use crate::utils::{create_new_session_builder, unique_keyspace_name, PerformDDL as _};

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

#[tokio::test]
async fn test_prepare_batch() {
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
