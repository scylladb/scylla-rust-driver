use crate::utils::{
    create_new_session_builder, scylla_supports_tablets, setup_tracing, unique_keyspace_name,
    PerformDDL as _,
};

#[tokio::test]
async fn test_token_awareness() {
    setup_tracing();
    let session = create_new_session_builder().build().await.unwrap();
    let ks = unique_keyspace_name();

    // Need to disable tablets in this test because they make token routing
    // work differently, and in this test we want to test the classic token ring
    // behavior.
    let mut create_ks = format!(
        "CREATE KEYSPACE IF NOT EXISTS {ks} WITH REPLICATION = {{'class' : 'NetworkTopologyStrategy', 'replication_factor' : 1}}"
    );
    if scylla_supports_tablets(&session).await {
        create_ks += " AND TABLETS = {'enabled': false}"
    }

    session.ddl(create_ks).await.unwrap();
    session
        .ddl(format!(
            "CREATE TABLE IF NOT EXISTS {ks}.t (a text primary key)"
        ))
        .await
        .unwrap();

    let mut prepared_statement = session
        .prepare(format!("INSERT INTO {ks}.t (a) VALUES (?)"))
        .await
        .unwrap();
    prepared_statement.set_tracing(true);

    // The default policy should be token aware
    for size in 1..50usize {
        let key = vec!['a'; size].into_iter().collect::<String>();
        let values = (&key,);

        // Execute a query and observe tracing info
        let res = session
            .execute_unpaged(&prepared_statement, values)
            .await
            .unwrap();
        let tracing_info = session
            .get_tracing_info(res.tracing_id().as_ref().unwrap())
            .await
            .unwrap();

        // Verify that only one node was involved
        assert_eq!(tracing_info.nodes().len(), 1);

        // Do the same with execute_iter (it now works with writes)
        let iter = session
            .execute_iter(prepared_statement.clone(), values)
            .await
            .unwrap();
        let tracing_id = iter.tracing_ids()[0];
        let tracing_info = session.get_tracing_info(&tracing_id).await.unwrap();

        // Again, verify that only one node was involved
        assert_eq!(tracing_info.nodes().len(), 1);
    }

    session.ddl(format!("DROP KEYSPACE {ks}")).await.unwrap();
}
