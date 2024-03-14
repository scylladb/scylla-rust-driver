use assert_matches::assert_matches;

use scylla_cql::errors::{BadQuery, QueryError};

use crate::batch::BatchType;
use crate::query::Query;
use crate::test_utils::setup_tracing;
use crate::{
    batch::Batch,
    test_utils::{create_new_session_builder, unique_keyspace_name},
    QueryResult, Session,
};

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
        QueryError::BadQuery(BadQuery::TooManyQueriesInBatchStatement(_too_many_queries)) if _too_many_queries == too_many_queries
    )
}

async fn create_test_session(session: Session, ks: &String) -> Session {
    session
        .query(
            format!("CREATE KEYSPACE {} WITH REPLICATION = {{ 'class' : 'NetworkTopologyStrategy', 'replication_factor' : 1 }}",ks),
            &[],
        )
        .await.unwrap();
    session
        .query(
            format!(
                "CREATE TABLE {}.pairs (dummy int, k blob, v blob, primary key (dummy, k))",
                ks
            ),
            &[],
        )
        .await
        .unwrap();
    session
}

async fn write_batch(session: &Session, n: usize, ks: &String) -> Result<QueryResult, QueryError> {
    let mut batch_query = Batch::new(BatchType::Unlogged);
    let mut batch_values = Vec::new();
    let query = format!("INSERT INTO {}.pairs (dummy, k, v) VALUES (0, ?, ?)", ks);
    let query = Query::new(query);
    let prepared_statement = session.prepare(query).await.unwrap();
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
