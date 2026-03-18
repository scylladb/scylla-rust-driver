use futures::future::join_all;
use scylla::client::session::Session;
use scylla::statement::prepared::PreparedStatement;
use scylla::value::Row;
use std::sync::Arc;
use uuid::Uuid;

use crate::common::SIMPLE_INSERT_QUERY;

mod common;

const CONCURRENCY: usize = 100;

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
        assert_eq!(r.len() as i32, 10);
        index += CONCURRENCY;
    }

    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let n: i32 = common::get_cnt();

    let session = common::init_simple_table().await?;

    let insert_query = SIMPLE_INSERT_QUERY;
    for _ in 0..10 {
        let id = Uuid::new_v4();
        session.query_unpaged(insert_query, (id, 100)).await?;
    }

    let select_query = session.prepare("SELECT * FROM benchmarks.basic").await?;

    let mut handles = vec![];
    let session = Arc::new(session);

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
