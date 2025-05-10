use std::sync::Arc;

use scylla::{
    statement::{
        batch::{Batch, BatchType},
        Statement,
    },
    value::Counter,
};

use crate::utils::{
    create_new_session_builder, scylla_supports_tablets, setup_tracing, unique_keyspace_name,
    PerformDDL as _,
};

#[tokio::test]
async fn test_counter_batch() {
    setup_tracing();
    let session = Arc::new(create_new_session_builder().build().await.unwrap());
    let ks = unique_keyspace_name();

    // Need to disable tablets in this test because they don't support counters yet.
    // (https://github.com/scylladb/scylladb/commit/c70f321c6f581357afdf3fd8b4fe8e5c5bb9736e).
    let mut create_ks = format!("CREATE KEYSPACE IF NOT EXISTS {} WITH REPLICATION = {{'class' : 'NetworkTopologyStrategy', 'replication_factor' : 1}}", ks);
    if scylla_supports_tablets(&session).await {
        create_ks += " AND TABLETS = {'enabled': false}"
    }

    session.ddl(create_ks).await.unwrap();
    session
        .ddl(format!(
            "CREATE TABLE IF NOT EXISTS {}.t_batch (key int PRIMARY KEY, value counter)",
            ks
        ))
        .await
        .unwrap();

    let statement_str = format!("UPDATE {}.t_batch SET value = value + ? WHERE key = ?", ks);
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
}
