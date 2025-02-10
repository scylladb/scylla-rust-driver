use futures::TryStreamExt;
use scylla::{
    batch::{Batch, BatchType},
    client::session::Session,
    query::Query,
};
use scylla_cql::frame::request::query::{PagingState, PagingStateResponse};

use crate::utils::{create_new_session_builder, setup_tracing, unique_keyspace_name, PerformDDL};

const PAGE_SIZE: i32 = 100;
const ROWS_PER_PARTITION: i32 = 1000;
const PARTITION_KEY1: &str = "part";
const PARTITION_KEY2: &str = "part2";

/// Initialize a cluster with a table and insert data into two partitions.
/// Returns a session and the keyspace name.
///
/// # Example
/// ```rust
/// let (session, ks) = initialize_cluster_two_partitions().await;
/// ```
async fn initialize_cluster_two_partitions() -> (Session, String) {
    setup_tracing();
    let session = create_new_session_builder().build().await.unwrap();

    let ks = unique_keyspace_name();

    session.ddl(format!("CREATE KEYSPACE IF NOT EXISTS {} WITH REPLICATION = {{'class' : 'NetworkTopologyStrategy', 'replication_factor' : 1}}", ks)).await.unwrap();
    session
        .ddl(format!(
            "CREATE TABLE IF NOT EXISTS {}.t (k0 text, k1 int, v int, PRIMARY KEY(k0, k1))",
            ks
        ))
        .await
        .unwrap();

    let prepared = session
        .prepare(format!("INSERT INTO {}.t (k0, k1, v) VALUES (?, ?, ?)", ks))
        .await
        .unwrap();

    let mut batch_part1 = Batch::new(BatchType::Unlogged);
    let mut batch_part2 = Batch::new(BatchType::Unlogged);
    let mut batch_values1 = Vec::new();
    let mut batch_values2 = Vec::new();

    for i in 0..ROWS_PER_PARTITION {
        batch_part1.append_statement(prepared.clone());
        batch_values1.push((PARTITION_KEY1, i, i));
        batch_part2.append_statement(prepared.clone());
        batch_values2.push((
            PARTITION_KEY2,
            i + ROWS_PER_PARTITION,
            i + ROWS_PER_PARTITION,
        ));
    }

    session.batch(&batch_part1, &batch_values1).await.unwrap();
    session.batch(&batch_part2, &batch_values2).await.unwrap();

    (session, ks)
}

#[tokio::test]
async fn query_single_page_should_only_iterate_over_rows_in_current_page() {
    let (session, ks) = initialize_cluster_two_partitions().await;

    let mut query = Query::new(format!("SELECT * FROM {}.t where k0 = ?", ks));
    query.set_page_size(PAGE_SIZE);

    let paging_state = PagingState::start();
    let (rs_manual, paging_state_response) = session
        .query_single_page(query, (PARTITION_KEY1,), paging_state)
        .await
        .unwrap();
    let page_results = rs_manual
        .into_rows_result()
        .unwrap()
        .rows::<(String, i32, i32)>()
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();

    assert!(page_results.len() <= PAGE_SIZE as usize);
    match paging_state_response {
        PagingStateResponse::HasMorePages { state: _ } => {}
        PagingStateResponse::NoMorePages => {
            panic!("Expected more pages");
        }
    }
}

#[tokio::test]
async fn query_iter_should_iterate_over_all_pages_asynchronously_single_partition() {
    let (session, ks) = initialize_cluster_two_partitions().await;

    let mut query = Query::new(format!("SELECT * FROM {}.t where k0 = ?", ks));
    query.set_page_size(PAGE_SIZE);

    let query_result = session.query_iter(query, (PARTITION_KEY1,)).await.unwrap();
    let mut iter = query_result.rows_stream::<(String, i32, i32)>().unwrap();

    let mut i = 0;

    while let Some((a, b, c)) = iter.try_next().await.unwrap() {
        assert_eq!(a, PARTITION_KEY1);
        assert_eq!(b, i);
        assert_eq!(c, i);
        i += 1;
    }
    assert_eq!(i, ROWS_PER_PARTITION);
}

#[tokio::test]
async fn query_iter_should_iterate_over_all_pages_asynchronously_cross_partition() {
    let (session, ks) = initialize_cluster_two_partitions().await;

    let mut query = Query::new(format!("SELECT * FROM {}.t", ks));
    query.set_page_size(PAGE_SIZE);

    let query_result = session.query_iter(query, ()).await.unwrap();
    let mut iter = query_result.rows_stream::<(String, i32, i32)>().unwrap();

    let mut i = 0;
    while let Some((a, b, c)) = iter.try_next().await.unwrap() {
        if i < ROWS_PER_PARTITION {
            assert_eq!(a, PARTITION_KEY1);
        } else {
            assert_eq!(a, PARTITION_KEY2);
        }
        assert_eq!(b, i);
        assert_eq!(c, i);
        i += 1;
    }
    assert_eq!(i, 2 * ROWS_PER_PARTITION);
}

#[tokio::test]
async fn execute_single_page_should_only_iterate_over_rows_in_current_page() {
    let (session, ks) = initialize_cluster_two_partitions().await;

    let mut prepared_query = session
        .prepare(format!("SELECT * FROM {}.t where k0 = ?", ks))
        .await
        .unwrap();
    prepared_query.set_page_size(PAGE_SIZE);

    let paging_state = PagingState::start();
    let (rs_manual, paging_state_response) = session
        .execute_single_page(&prepared_query, (PARTITION_KEY1,), paging_state)
        .await
        .unwrap();
    let page_results = rs_manual
        .into_rows_result()
        .unwrap()
        .rows::<(String, i32, i32)>()
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();

    assert!(page_results.len() <= PAGE_SIZE as usize);
    match paging_state_response {
        PagingStateResponse::HasMorePages { state: _ } => {}
        PagingStateResponse::NoMorePages => {
            panic!("Expected more pages");
        }
    }
}

#[tokio::test]
async fn execute_iter_should_iterate_over_all_pages_asynchronously_single_partition() {
    let (session, ks) = initialize_cluster_two_partitions().await;

    let mut prepared_query = session
        .prepare(format!("SELECT * FROM {}.t where k0 = ?", ks))
        .await
        .unwrap();
    prepared_query.set_page_size(PAGE_SIZE);

    let query_result = session
        .execute_iter(prepared_query, (PARTITION_KEY1,))
        .await
        .unwrap();
    let mut iter = query_result.rows_stream::<(String, i32, i32)>().unwrap();

    let mut i = 0;

    while let Some((a, b, c)) = iter.try_next().await.unwrap() {
        assert_eq!(a, PARTITION_KEY1);
        assert_eq!(b, i);
        assert_eq!(c, i);
        i += 1;
    }
    assert_eq!(i, ROWS_PER_PARTITION);
}

#[tokio::test]
async fn execute_iter_should_iterate_over_all_pages_asynchronously_cross_partition() {
    let (session, ks) = initialize_cluster_two_partitions().await;

    let mut prepared_query = session
        .prepare(format!("SELECT * FROM {}.t", ks))
        .await
        .unwrap();
    prepared_query.set_page_size(PAGE_SIZE);

    let query_result = session.execute_iter(prepared_query, ()).await.unwrap();
    let mut iter = query_result.rows_stream::<(String, i32, i32)>().unwrap();

    let mut i = 0;
    while let Some((a, b, c)) = iter.try_next().await.unwrap() {
        if i < ROWS_PER_PARTITION {
            assert_eq!(a, PARTITION_KEY1);
        } else {
            assert_eq!(a, PARTITION_KEY2);
        }
        assert_eq!(b, i);
        assert_eq!(c, i);
        i += 1;
    }
    assert_eq!(i, 2 * ROWS_PER_PARTITION);
}
