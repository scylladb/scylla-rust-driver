use crate::batch::BatchType;
use crate::query::Query;
use crate::{
    batch::Batch,
    test_utils::{create_new_session_builder, unique_keyspace_name},
    QueryResult, Session,
};
use assert_matches::assert_matches;
use scylla_cql::errors::{BadQuery, DbError, QueryError};

#[tokio::test]
async fn test_large_batch_statements() {
    let mut session = create_new_session_builder().build().await.unwrap();
    let ks = unique_keyspace_name();
    session = create_test_session(session, &ks).await;

    // Add batch
    let max_number_of_queries = u16::MAX as usize;
    let batch_result = write_batch(&session, max_number_of_queries, &ks).await;

    if batch_result.is_err() {
        assert_matches!(
            batch_result.unwrap_err(),
            QueryError::DbError(DbError::WriteTimeout { .. }, _)
        )
    }

    // Now try with too many queries
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
            format!("CREATE KEYSPACE IF NOT EXISTS {} WITH REPLICATION = {{ 'class' : 'NetworkTopologyStrategy', 'replication_factor' : 1 }}",ks),
            &[],
        )
        .await.unwrap();
    session
        .query(format!("DROP TABLE IF EXISTS {}.pairs;", ks), &[])
        .await
        .unwrap();
    session
        .query(
            format!("CREATE TABLE IF NOT EXISTS {}.pairs (dummy int, k blob, v blob, primary key (dummy, k))", ks),
            &[],
        )
        .await.unwrap();
    session
}

async fn write_batch(session: &Session, n: usize, ks: &String) -> Result<QueryResult, QueryError> {
    let mut batch_query = Batch::new(BatchType::Logged);
    let mut batch_values = Vec::new();
    for i in 0..n {
        let mut key = vec![0];
        key.extend(i.to_be_bytes().as_slice());
        let value = key.clone();
        let query = format!("INSERT INTO {}.pairs (dummy, k, v) VALUES (0, ?, ?)", ks);
        let values = vec![key, value];
        batch_values.push(values);
        let query = Query::new(query);
        batch_query.append_statement(query);
    }
    session.batch(&batch_query, batch_values).await
}
