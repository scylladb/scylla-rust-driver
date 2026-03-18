use futures::future::join_all;
use scylla::client::session::Session;
use scylla::statement::prepared::PreparedStatement;
use scylla::value::Row;
use std::sync::Arc;

use crate::common::DESER_INSERT_QUERY;

mod common;

const CONCURRENCY: usize = 100;

async fn insert_data(
    session: Arc<Session>,
    start_index: usize,
    n: i32,
    insert_query: &PreparedStatement,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut index = start_index;

    while index < n as usize {
        session
            .execute_unpaged(insert_query, common::get_deser_data())
            .await?;
        index += CONCURRENCY;
    }

    Ok(())
}

async fn select_data(
    session: Arc<Session>,
    start_index: usize,
    n: i32,
    select_query: &PreparedStatement,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut index = start_index;

    while index < n as usize {
        let r = session
            .execute_unpaged(select_query, &[])
            .await?
            .into_rows_result()?
            .rows::<Row>()?
            .collect::<Vec<_>>();
        assert_eq!(r.len() as i32, n);
        index += CONCURRENCY;
    }

    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let n: i32 = common::get_cnt();

    let session = common::init_deser_table().await?;

    let insert_query = session.prepare(DESER_INSERT_QUERY).await?;

    let mut handles = vec![];
    let session = Arc::new(session);

    for i in 0..CONCURRENCY {
        let session_clone = Arc::clone(&session);
        let insert_query_clone = insert_query.clone();
        handles.push(tokio::spawn(async move {
            insert_data(session_clone, i, n, &insert_query_clone)
                .await
                .unwrap();
        }));
    }

    let results = join_all(handles).await;

    for result in results {
        result.unwrap();
    }

    let select_query = session.prepare("SELECT * FROM benchmarks.basic").await?;

    let mut handles = vec![];

    for i in 0..CONCURRENCY {
        let session_clone = Arc::clone(&session);
        let select_query_clone = select_query.clone();
        handles.push(tokio::spawn(async move {
            select_data(session_clone, i, n, &select_query_clone)
                .await
                .unwrap();
        }));
    }

    let results = join_all(handles).await;

    for result in results {
        result.unwrap();
    }

    Ok(())
}
