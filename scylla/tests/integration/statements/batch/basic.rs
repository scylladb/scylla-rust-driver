use std::sync::Arc;

use scylla::statement::batch::Batch;

use crate::utils::{
    create_new_session_builder, setup_tracing, unique_keyspace_name, PerformDDL as _,
};

#[tokio::test]
async fn test_batch() {
    setup_tracing();
    let session = Arc::new(create_new_session_builder().build().await.unwrap());
    let ks = unique_keyspace_name();

    session.ddl(format!("CREATE KEYSPACE IF NOT EXISTS {} WITH REPLICATION = {{'class' : 'NetworkTopologyStrategy', 'replication_factor' : 1}}", ks)).await.unwrap();
    session
        .ddl(format!(
            "CREATE TABLE IF NOT EXISTS {}.t_batch (a int, b int, c text, primary key (a, b))",
            ks
        ))
        .await
        .unwrap();

    let prepared_statement = session
        .prepare(format!(
            "INSERT INTO {}.t_batch (a, b, c) VALUES (?, ?, ?)",
            ks
        ))
        .await
        .unwrap();

    // TODO: Add API that supports binding values to statements in batch creation process,
    // to avoid problem of statements/values count mismatch
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
            "ALTER TABLE {}.t_batch WITH gc_grace_seconds = 42",
            ks
        ))
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
        .into_rows_result()
        .unwrap()
        .rows::<(i32, i32, String)>()
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();

    assert_eq!(results, vec![(4, 20, String::from("foobar"))]);
}
