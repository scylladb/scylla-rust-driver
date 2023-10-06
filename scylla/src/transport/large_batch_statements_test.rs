use bcs::serialize_into;
use scylla_cql::errors::{BadQuery, QueryError};

use crate::batch::BatchType;
use crate::query::Query;
use crate::{
    batch::Batch,
    prepared_statement::PreparedStatement,
    test_utils::{create_new_session_builder, unique_keyspace_name},
    IntoTypedRows, QueryResult, Session,
};

#[tokio::test]
async fn test_large_batch_statements() {
    let mut session = create_new_session_builder().build().await.unwrap();
    let ks = unique_keyspace_name();
    session = create_test_session(session, &ks).await;

    let max_number_of_queries = u16::MAX as usize;
    write_batch(&session, max_number_of_queries).await;

    let key_prefix = vec![0];
    let keys = find_keys_by_prefix(&session, key_prefix.clone()).await;
    assert_eq!(keys.len(), max_number_of_queries);

    let too_many_queries = u16::MAX as usize + 1;

    let err = write_batch(&session, too_many_queries).await;

    assert!(err.is_err());
}

async fn create_test_session(session: Session, ks: &String) -> Session {
    session
        .query(
            format!("CREATE KEYSPACE IF NOT EXISTS {} WITH REPLICATION = {{ 'class' : 'SimpleStrategy', 'replication_factor' : 1 }}",ks),
            &[],
        )
        .await.unwrap();
    session
        .query("DROP TABLE IF EXISTS kv.pairs;", &[])
        .await
        .unwrap();
    session
        .query(
            "CREATE TABLE IF NOT EXISTS kv.pairs (dummy int, k blob, v blob, primary key (dummy, k))",
            &[],
        )
        .await.unwrap();
    session
}

async fn write_batch(session: &Session, n: usize) -> Result<QueryResult, QueryError> {
    let mut batch_query = Batch::new(BatchType::Logged);
    let mut batch_values = Vec::new();
    for i in 0..n {
        let mut key = vec![0];
        serialize_into(&mut key, &(i as usize)).unwrap();
        let value = key.clone();
        let query = "INSERT INTO kv.pairs (dummy, k, v) VALUES (0, ?, ?)";
        let values = vec![key, value];
        batch_values.push(values);
        let query = Query::new(query);
        batch_query.append_statement(query);
    }
    session.batch(&batch_query, batch_values).await
}

async fn find_keys_by_prefix(session: &Session, key_prefix: Vec<u8>) -> Vec<Vec<u8>> {
    let len = key_prefix.len();
    let rows = match get_upper_bound_option(&key_prefix) {
        None => {
            let values = (key_prefix,);
            let query = "SELECT k FROM kv.pairs WHERE dummy = 0 AND k >= ? ALLOW FILTERING";
            session.query(query, values).await.unwrap()
        }
        Some(upper_bound) => {
            let values = (key_prefix, upper_bound);
            let query =
                "SELECT k FROM kv.pairs WHERE dummy = 0 AND k >= ? AND k < ? ALLOW FILTERING";
            session.query(query, values).await.unwrap()
        }
    };
    let mut keys = Vec::new();
    if let Some(rows) = rows.rows {
        for row in rows.into_typed::<(Vec<u8>,)>() {
            let key = row.unwrap();
            let short_key = key.0[len..].to_vec();
            keys.push(short_key);
        }
    }
    keys
}

fn get_upper_bound_option(key_prefix: &[u8]) -> Option<Vec<u8>> {
    let len = key_prefix.len();
    for i in (0..len).rev() {
        let val = key_prefix[i];
        if val < u8::MAX {
            let mut upper_bound = key_prefix[0..i + 1].to_vec();
            upper_bound[i] += 1;
            return Some(upper_bound);
        }
    }
    None
}
