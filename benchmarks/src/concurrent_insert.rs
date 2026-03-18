use futures::future::join_all;
use scylla::client::session::Session;
use scylla::statement::prepared::PreparedStatement;
use std::sync::Arc;
use uuid::Uuid;

use crate::common::SIMPLE_INSERT_QUERY;

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
        let id = Uuid::new_v4();
        session.execute_unpaged(insert_query, (id, 100)).await?;
        index += CONCURRENCY;
    }

    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let n: i32 = common::get_cnt();

    let session = common::init_simple_table().await?;

    let insert_query = session.prepare(SIMPLE_INSERT_QUERY).await?;

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

    common::check_row_cnt(&session, n).await?;

    Ok(())
}
