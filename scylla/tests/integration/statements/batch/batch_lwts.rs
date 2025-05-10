use scylla::{
    client::session::Session,
    response::query_result::{QueryResult, QueryRowsResult},
    statement::batch::Batch,
};

use crate::utils::{
    create_new_session_builder, scylla_supports_tablets, unique_keyspace_name, PerformDDL as _,
};

// Batches containing LWT queries (IF col = som) return rows with information whether the queries were applied.
#[tokio::test]
async fn test_batch_lwts() {
    let session = create_new_session_builder().build().await.unwrap();

    let ks = unique_keyspace_name();
    let mut create_ks = format!("CREATE KEYSPACE {} WITH REPLICATION = {{'class': 'NetworkTopologyStrategy', 'replication_factor': 1}}", ks);
    if scylla_supports_tablets(&session).await {
        create_ks += " and TABLETS = { 'enabled': false}";
    }
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
