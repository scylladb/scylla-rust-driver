use scylla::{
    client::session::Session,
    observability::tracing::TracingInfo,
    response::query_result::QueryResult,
    statement::{batch::Batch, Statement},
};
use scylla_cql::Consistency;
use uuid::Uuid;

use crate::utils::{
    create_new_session_builder, setup_tracing, unique_keyspace_name, PerformDDL as _,
};

#[tokio::test]
async fn test_tracing() {
    setup_tracing();
    let session = create_new_session_builder().build().await.unwrap();
    let ks = unique_keyspace_name();

    session.ddl(format!("CREATE KEYSPACE IF NOT EXISTS {ks} WITH REPLICATION = {{'class' : 'NetworkTopologyStrategy', 'replication_factor' : 1}}")).await.unwrap();

    session
        .ddl(format!(
            "CREATE TABLE IF NOT EXISTS {ks}.tab (a text primary key)"
        ))
        .await
        .unwrap();

    test_tracing_query(&session, ks.clone()).await;
    test_tracing_execute(&session, ks.clone()).await;
    test_tracing_prepare(&session, ks.clone()).await;
    test_get_tracing_info(&session, ks.clone()).await;
    test_tracing_query_iter(&session, ks.clone()).await;
    test_tracing_execute_iter(&session, ks.clone()).await;
    test_tracing_batch(&session, ks.clone()).await;

    session.ddl(format!("DROP KEYSPACE {ks}")).await.unwrap();
}

async fn test_tracing_query(session: &Session, ks: String) {
    // A query without tracing enabled has no tracing uuid in result
    let untraced_query: Statement = Statement::new(format!("SELECT * FROM {ks}.tab"));
    let untraced_query_result: QueryResult =
        session.query_unpaged(untraced_query, &[]).await.unwrap();

    assert!(untraced_query_result.tracing_id().is_none());

    // A query with tracing enabled has a tracing uuid in result
    let mut traced_query: Statement = Statement::new(format!("SELECT * FROM {ks}.tab"));
    traced_query.set_tracing(true);

    let traced_query_result: QueryResult = session.query_unpaged(traced_query, &[]).await.unwrap();
    assert!(traced_query_result.tracing_id().is_some());

    // Querying this uuid from tracing table gives some results
    assert_in_tracing_table(session, traced_query_result.tracing_id().unwrap()).await;
}

async fn test_tracing_execute(session: &Session, ks: String) {
    // Executing a prepared statement without tracing enabled has no tracing uuid in result
    let untraced_prepared = session
        .prepare(format!("SELECT * FROM {ks}.tab"))
        .await
        .unwrap();

    let untraced_prepared_result: QueryResult = session
        .execute_unpaged(&untraced_prepared, &[])
        .await
        .unwrap();

    assert!(untraced_prepared_result.tracing_id().is_none());

    // Executing a prepared statement with tracing enabled has a tracing uuid in result
    let mut traced_prepared = session
        .prepare(format!("SELECT * FROM {ks}.tab"))
        .await
        .unwrap();

    traced_prepared.set_tracing(true);

    let traced_prepared_result: QueryResult = session
        .execute_unpaged(&traced_prepared, &[])
        .await
        .unwrap();
    assert!(traced_prepared_result.tracing_id().is_some());

    // Querying this uuid from tracing table gives some results
    assert_in_tracing_table(session, traced_prepared_result.tracing_id().unwrap()).await;
}

async fn test_tracing_prepare(session: &Session, ks: String) {
    // Preparing a statement without tracing enabled has no tracing uuids in result
    let untraced_prepared = session
        .prepare(format!("SELECT * FROM {ks}.tab"))
        .await
        .unwrap();

    assert!(untraced_prepared.prepare_tracing_ids.is_empty());

    // Preparing a statement with tracing enabled has tracing uuids in result
    let mut to_prepare_traced = Statement::new(format!("SELECT * FROM {ks}.tab"));
    to_prepare_traced.set_tracing(true);

    let traced_prepared = session.prepare(to_prepare_traced).await.unwrap();
    assert!(!traced_prepared.prepare_tracing_ids.is_empty());

    // Querying this uuid from tracing table gives some results
    for tracing_id in traced_prepared.prepare_tracing_ids {
        assert_in_tracing_table(session, tracing_id).await;
    }
}

async fn test_get_tracing_info(session: &Session, ks: String) {
    // A query with tracing enabled has a tracing uuid in result
    let mut traced_query: Statement = Statement::new(format!("SELECT * FROM {ks}.tab"));
    traced_query.set_tracing(true);

    let traced_query_result: QueryResult = session.query_unpaged(traced_query, &[]).await.unwrap();
    let tracing_id: Uuid = traced_query_result.tracing_id().unwrap();

    // Getting tracing info from session using this uuid works
    let tracing_info: TracingInfo = session.get_tracing_info(&tracing_id).await.unwrap();
    assert!(!tracing_info.events.is_empty());
    assert!(!tracing_info.nodes().is_empty());
}

async fn test_tracing_query_iter(session: &Session, ks: String) {
    // A query without tracing enabled has no tracing ids
    let untraced_query: Statement = Statement::new(format!("SELECT * FROM {ks}.tab"));

    let untraced_query_pager = session.query_iter(untraced_query, &[]).await.unwrap();
    assert!(untraced_query_pager.tracing_ids().is_empty());

    let untraced_typed_row_iter = untraced_query_pager.rows_stream::<(String,)>().unwrap();
    assert!(untraced_typed_row_iter.tracing_ids().is_empty());

    // A query with tracing enabled has a tracing ids in result
    let mut traced_query: Statement = Statement::new(format!("SELECT * FROM {ks}.tab"));
    traced_query.set_tracing(true);

    let traced_query_pager = session.query_iter(traced_query, &[]).await.unwrap();

    let traced_typed_row_stream = traced_query_pager.rows_stream::<(String,)>().unwrap();
    assert!(!traced_typed_row_stream.tracing_ids().is_empty());

    for tracing_id in traced_typed_row_stream.tracing_ids() {
        assert_in_tracing_table(session, *tracing_id).await;
    }
}

async fn test_tracing_execute_iter(session: &Session, ks: String) {
    // A prepared statement without tracing enabled has no tracing ids
    let untraced_prepared = session
        .prepare(format!("SELECT * FROM {ks}.tab"))
        .await
        .unwrap();

    let untraced_query_pager = session.execute_iter(untraced_prepared, &[]).await.unwrap();
    assert!(untraced_query_pager.tracing_ids().is_empty());

    let untraced_typed_row_stream = untraced_query_pager.rows_stream::<(String,)>().unwrap();
    assert!(untraced_typed_row_stream.tracing_ids().is_empty());

    // A prepared statement with tracing enabled has a tracing ids in result
    let mut traced_prepared = session
        .prepare(format!("SELECT * FROM {ks}.tab"))
        .await
        .unwrap();
    traced_prepared.set_tracing(true);

    let traced_query_pager = session.execute_iter(traced_prepared, &[]).await.unwrap();

    let traced_typed_row_stream = traced_query_pager.rows_stream::<(String,)>().unwrap();
    assert!(!traced_typed_row_stream.tracing_ids().is_empty());

    for tracing_id in traced_typed_row_stream.tracing_ids() {
        assert_in_tracing_table(session, *tracing_id).await;
    }
}

async fn test_tracing_batch(session: &Session, ks: String) {
    // A batch without tracing enabled has no tracing id
    let mut untraced_batch: Batch = Default::default();
    untraced_batch.append_statement(&format!("INSERT INTO {ks}.tab (a) VALUES('a')")[..]);

    let untraced_batch_result: QueryResult = session.batch(&untraced_batch, ((),)).await.unwrap();
    assert!(untraced_batch_result.tracing_id().is_none());

    // Batch with tracing enabled has a tracing uuid in result
    let mut traced_batch: Batch = Default::default();
    traced_batch.append_statement(&format!("INSERT INTO {ks}.tab (a) VALUES('a')")[..]);
    traced_batch.set_tracing(true);

    let traced_batch_result: QueryResult = session.batch(&traced_batch, ((),)).await.unwrap();
    assert!(traced_batch_result.tracing_id().is_some());

    assert_in_tracing_table(session, traced_batch_result.tracing_id().unwrap()).await;
}

async fn assert_in_tracing_table(session: &Session, tracing_uuid: Uuid) {
    let mut traces_query =
        Statement::new("SELECT * FROM system_traces.sessions WHERE session_id = ?");
    traces_query.set_consistency(Consistency::One);

    // Tracing info might not be immediately available
    // If rows are empty perform 8 retries with a 32ms wait in between

    // The reason why we enable so long waiting for TracingInfo is... Cassandra. (Yes, again.)
    // In Cassandra Java Driver, the wait time for tracing info is 10 seconds, so here we do the same.
    // However, as Scylla usually gets TracingInfo ready really fast (our default interval is hence 3ms),
    // we stick to a not-so-much-terribly-long interval here.
    for _ in 0..200 {
        let rows_num = session
            .query_unpaged(traces_query.clone(), (tracing_uuid,))
            .await
            .unwrap()
            .into_rows_result()
            .unwrap()
            .rows_num();
        if rows_num > 0 {
            // Ok there was some row for this tracing_uuid
            return;
        }

        // Otherwise retry
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }

    // If all retries failed panic with an error
    panic!("No rows for tracing with this session id!");
}
