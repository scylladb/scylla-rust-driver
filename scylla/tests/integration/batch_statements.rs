use crate::utils::{create_new_session_builder, setup_tracing, unique_keyspace_name, PerformDDL};
use assert_matches::assert_matches;
use scylla::batch::{Batch, BatchType};
use scylla::client::session::Session;
use scylla::errors::ExecutionError;
use scylla::prepared_statement::PreparedStatement;
use scylla::query::Query;
use scylla::value::{CqlValue, MaybeUnset};
use std::collections::HashMap;
use std::string::String;

const BATCH_COUNT: usize = 100;

async fn create_test_session(table_name: &str) -> (Session, String) {
    let session = create_new_session_builder().build().await.unwrap();
    let ks = unique_keyspace_name();
    session
        .ddl(format!(
            "CREATE KEYSPACE IF NOT EXISTS {} WITH REPLICATION = {{'class' : 'NetworkTopologyStrategy', 'replication_factor' : 1}}", ks))
        .await
        .unwrap();
    session.use_keyspace(&ks, false).await.unwrap();
    session
        .ddl(format!(
            "CREATE TABLE IF NOT EXISTS {} (k0 text, k1 int, v int, PRIMARY KEY (k0, k1))",
            table_name
        ))
        .await
        .unwrap();

    (session, ks)
}

async fn create_counter_tables(session: &Session, ks: &str) {
    for &table in ["counter1", "counter2", "counter3"].iter() {
        session
            .ddl(format!(
                "CREATE TABLE {}.{} (k0 text PRIMARY KEY, c counter)",
                ks, table
            ))
            .await
            .unwrap();
    }
}

async fn verify_batch_insert(session: &Session, keyspace: &str, test_name: &str, count: usize) {
    let select_query = format!(
        "SELECT k0, k1, v FROM {}.{} WHERE k0 = ?",
        keyspace, test_name
    );
    let query_result = session
        .query_unpaged(select_query, (test_name,))
        .await
        .unwrap()
        .into_rows_result()
        .unwrap();
    let rows: Vec<(String, i32, i32)> = query_result
        .rows::<(String, i32, i32)>()
        .unwrap()
        .map(|r| r.unwrap())
        .collect();
    assert_eq!(rows.len(), count);
    for (k0, k1, v) in rows {
        assert_eq!(k0, test_name);
        assert_eq!(v, k1 + 1);
    }
}

async fn prepare_insert_statement(session: &Session, ks: &str, table: &str) -> PreparedStatement {
    let query_str = format!("INSERT INTO {}.{} (k0, k1, v) VALUES (?, ?, ?)", ks, table);
    session.prepare(Query::new(query_str)).await.unwrap()
}

#[tokio::test]
async fn test_batch_of_simple_statements() {
    setup_tracing();
    let test_name = String::from("test_batch_simple_statements");
    let (session, ks) = create_test_session(&test_name).await;

    let mut batch = Batch::new(BatchType::Unlogged);
    for i in 0..BATCH_COUNT {
        let simple_statement = Query::new(format!(
            "INSERT INTO {}.{} (k0, k1, v) VALUES ('{}', {}, {})",
            ks,
            &test_name,
            &test_name,
            i,
            i + 1
        ));
        batch.append_statement(simple_statement);
    }
    session.batch(&batch, vec![(); BATCH_COUNT]).await.unwrap();
    verify_batch_insert(&session, &ks, &test_name, BATCH_COUNT).await;
}

#[tokio::test]
async fn test_batch_of_prepared_statements() {
    setup_tracing();
    let test_name = String::from("test_batch_prepared_statements");
    let (session, ks) = create_test_session(&test_name).await;

    let prepared = prepare_insert_statement(&session, &ks, &test_name).await;
    let mut batch = Batch::new(BatchType::Unlogged);
    let mut batch_values: Vec<_> = Vec::with_capacity(BATCH_COUNT);
    for i in 0..BATCH_COUNT as i32 {
        batch.append_statement(prepared.clone());
        batch_values.push((test_name.as_str(), i, i + 1));
    }
    session.batch(&batch, batch_values).await.unwrap();
    verify_batch_insert(&session, &ks, &test_name, BATCH_COUNT).await;
}

#[tokio::test]
async fn test_prepared_batch() {
    setup_tracing();
    let test_name = String::from("test_prepared_batch");
    let (session, ks) = create_test_session(&test_name).await;

    let mut batch = Batch::new(BatchType::Unlogged);
    let mut batch_values = Vec::with_capacity(BATCH_COUNT);
    let query_str = format!(
        "INSERT INTO {}.{} (k0, k1, v) VALUES (?, ?, ?)",
        ks, &test_name
    );
    for i in 0..BATCH_COUNT as i32 {
        batch.append_statement(Query::new(query_str.clone()));
        batch_values.push((&test_name, i, i + 1));
    }
    let prepared_batch = session.prepare_batch(&batch).await.unwrap();
    session.batch(&prepared_batch, batch_values).await.unwrap();
    verify_batch_insert(&session, &ks, &test_name, BATCH_COUNT).await;
}

#[tokio::test]
async fn test_batch_of_bound_statements_with_unset_values() {
    setup_tracing();
    let test_name = String::from("test_batch_bound_statements_with_unset_values");
    let (session, ks) = create_test_session(&test_name).await;

    let prepared = prepare_insert_statement(&session, &ks, &test_name).await;
    let mut batch1 = Batch::new(BatchType::Unlogged);
    let mut batch1_values = Vec::with_capacity(BATCH_COUNT);
    for i in 0..BATCH_COUNT as i32 {
        batch1.append_statement(prepared.clone());
        batch1_values.push((test_name.as_str(), i, i + 1));
    }
    session.batch(&batch1, batch1_values).await.unwrap();

    // Update v to (k1 + 2), but for every 20th row leave v unset.
    let mut batch2 = Batch::new(BatchType::Unlogged);
    let mut batch2_values = Vec::with_capacity(BATCH_COUNT);
    for i in 0..BATCH_COUNT as i32 {
        batch2.append_statement(prepared.clone());
        if i % 20 == 0 {
            batch2_values.push((
                MaybeUnset::Set(&test_name),
                MaybeUnset::Set(i),
                MaybeUnset::Unset,
            ));
        } else {
            batch2_values.push((
                MaybeUnset::Set(&test_name),
                MaybeUnset::Set(i),
                MaybeUnset::Set(i + 2),
            ));
        }
    }
    session.batch(&batch2, batch2_values).await.unwrap();

    // Verify that rows with k1 % 20 == 0 retain the original value.
    let select_query = format!("SELECT k0, k1, v FROM {}.{} WHERE k0 = ?", ks, &test_name);
    let query_result = session
        .query_unpaged(select_query, (&test_name,))
        .await
        .unwrap()
        .into_rows_result()
        .unwrap();
    let rows: Vec<(String, i32, i32)> = query_result
        .rows::<(String, i32, i32)>()
        .unwrap()
        .map(|r| r.unwrap())
        .collect();
    assert_eq!(
        rows.len(),
        BATCH_COUNT,
        "Expected {} rows, got {}",
        BATCH_COUNT,
        rows.len()
    );
    for (k0, k1, v) in rows {
        assert_eq!(k0, test_name);
        assert_eq!(v, if k1 % 20 == 0 { k1 + 1 } else { k1 + 2 });
    }
}

#[tokio::test]
async fn test_batch_of_bound_statements_named_variables() {
    setup_tracing();
    let test_name = String::from("test_batch_bound_statements_named_variables");
    let (session, ks) = create_test_session(&test_name).await;

    let query_str = format!(
        "INSERT INTO {}.{} (k0, k1, v) VALUES (:k0, :k1, :v)",
        ks, &test_name
    );
    let prepared = session.prepare(query_str).await.unwrap();

    let mut batch = Batch::new(BatchType::Unlogged);
    let mut batch_values = Vec::with_capacity(BATCH_COUNT);
    for i in 0..BATCH_COUNT as i32 {
        batch.append_statement(prepared.clone());
        let mut values = HashMap::new();
        values.insert("k0", CqlValue::Text(test_name.clone()));
        values.insert("k1", CqlValue::Int(i));
        values.insert("v", CqlValue::Int(i + 1));
        batch_values.push(values);
    }
    session.batch(&batch, batch_values).await.unwrap();
    verify_batch_insert(&session, &ks, &test_name, BATCH_COUNT).await;
}

#[tokio::test]
async fn test_batch_of_mixed_bound_and_simple_statements() {
    setup_tracing();
    let test_name = String::from("test_batch_mixed_bound_and_simple_statements");
    let (session, ks) = create_test_session(&test_name).await;

    let query_str = format!(
        "INSERT INTO {}.{} (k0, k1, v) VALUES (?, ?, ?)",
        ks, &test_name
    );
    let prepared_bound = session
        .prepare(Query::new(query_str.clone()))
        .await
        .unwrap();

    let mut batch = Batch::new(BatchType::Unlogged);
    let mut batch_values = Vec::with_capacity(BATCH_COUNT);
    for i in 0..BATCH_COUNT as i32 {
        if i % 2 == 1 {
            let simple_statement = Query::new(format!(
                "INSERT INTO {}.{} (k0, k1, v) VALUES ('{}', {}, {})",
                ks,
                &test_name,
                &test_name,
                i,
                i + 1
            ));
            batch.append_statement(simple_statement);
            batch_values.push(vec![]);
        } else {
            batch.append_statement(prepared_bound.clone());
            batch_values.push(vec![
                CqlValue::Text(test_name.clone()),
                CqlValue::Int(i),
                CqlValue::Int(i + 1),
            ]);
        }
    }
    session.batch(&batch, batch_values).await.unwrap();
    verify_batch_insert(&session, &ks, &test_name, BATCH_COUNT).await;
}

/// TODO: Remove #[ignore] once LWTs are supported with tablets.
#[tokio::test]
#[ignore]
async fn test_cas_batch() {
    setup_tracing();
    let test_name = String::from("test_cas_batch");
    let (session, ks) = create_test_session(&test_name).await;

    let prepared = prepare_insert_statement(&session, &ks, &test_name).await;
    let mut batch = Batch::new(BatchType::Unlogged);
    let mut batch_values = Vec::with_capacity(BATCH_COUNT);
    for i in 0..BATCH_COUNT as i32 {
        batch.append_statement(prepared.clone());
        batch_values.push((&test_name, i, i + 1));
    }
    let result = session.batch(&batch, batch_values.clone()).await.unwrap();
    let (applied,): (bool,) = result
        .into_rows_result()
        .unwrap()
        .first_row::<(bool,)>()
        .unwrap();
    assert!(applied, "First CAS batch should be applied");

    verify_batch_insert(&session, &ks, &test_name, BATCH_COUNT).await;

    let result2 = session.batch(&batch, batch_values).await.unwrap();
    let (applied2,): (bool,) = result2
        .into_rows_result()
        .unwrap()
        .first_row::<(bool,)>()
        .unwrap();
    assert!(applied2, "Second CAS batch should not be applied");
}

/// TODO: Remove #[ignore] once counters are supported with tablets.
#[tokio::test]
#[ignore]
async fn test_counter_batch() {
    setup_tracing();
    let test_name = String::from("test_counter_batch");
    let (session, ks) = create_test_session(&test_name).await;
    create_counter_tables(&session, &ks).await;

    let mut batch = Batch::new(BatchType::Counter);
    let mut batch_values = Vec::with_capacity(3);
    for i in 1..=3 {
        let query_str = format!("UPDATE {}.counter{} SET c = c + ? WHERE k0 = ?", ks, i);
        let prepared = session.prepare(Query::new(query_str)).await.unwrap();
        batch.append_statement(prepared);
        batch_values.push((i, &test_name));
    }
    session.batch(&batch, batch_values).await.unwrap();

    for i in 1..=3 {
        let query_str = format!("SELECT c FROM {}.counter{} WHERE k0 = ?", ks, i);
        let query_result = session
            .query_unpaged(query_str, (&test_name,))
            .await
            .unwrap()
            .into_rows_result()
            .unwrap();
        let row = query_result.single_row::<(i64,)>().unwrap();
        let (c,) = row;
        assert_eq!(c, i as i64);
    }
}

/// TODO: Remove #[ignore] once counters are supported with tablets.
#[tokio::test]
#[ignore]
async fn test_fail_logged_batch_with_counter_increment() {
    setup_tracing();
    let test_name = String::from("test_fail_logged_batch");
    let (session, ks) = create_test_session(&test_name).await;
    create_counter_tables(&session, &ks).await;

    let mut batch = Batch::new(BatchType::Logged);
    let mut batch_values: Vec<_> = Vec::with_capacity(3);
    for i in 1..=3 {
        let query_str = format!("UPDATE {}.counter{} SET c = c + ? WHERE k0 = ?", ks, i);
        let prepared = session.prepare(Query::new(query_str)).await.unwrap();
        batch.append_statement(prepared);
        batch_values.push((i, &test_name));
    }
    let err = session.batch(&batch, batch_values).await.unwrap_err();
    assert_matches!(
        err,
        ExecutionError::BadQuery(_),
        "Expected a BadQuery error when using counter statements in a LOGGED batch"
    );
}

/// TODO: Remove #[ignore] once counters are supported with tablets.
#[tokio::test]
#[ignore]
async fn test_fail_counter_batch_with_non_counter_increment() {
    setup_tracing();
    let test_name = String::from("test_fail_counter_batch");
    let (session, ks) = create_test_session(&test_name).await;
    create_counter_tables(&session, &ks).await;

    let mut batch = Batch::new(BatchType::Counter);
    let mut batch_values: Vec<Vec<CqlValue>> = Vec::new();
    for i in 1..=3 {
        let query_str = format!("UPDATE {}.counter{} SET c = c + ? WHERE k0 = ?", ks, i);
        let prepared = session.prepare(Query::new(query_str)).await.unwrap();
        batch.append_statement(prepared);
        batch_values.push(vec![CqlValue::Int(i), CqlValue::Text(test_name.clone())]);
    }

    let prepared = prepare_insert_statement(&session, &ks, &test_name).await;
    batch.append_statement(prepared);
    batch_values.push(vec![
        CqlValue::Text(test_name.clone()),
        CqlValue::Int(0),
        CqlValue::Int(1),
    ]);
    let err = session.batch(&batch, batch_values).await.unwrap_err();
    assert_matches!(
        err,
        ExecutionError::BadQuery(_),
        "Expected a BadQuery error when including a non-counter statement in a COUNTER batch"
    );
}
