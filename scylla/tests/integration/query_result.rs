use assert_matches::assert_matches;
use futures::TryStreamExt;
use scylla::errors::{ExecutionError, PagerExecutionError};
use scylla::{
    batch::{Batch, BatchType},
    client::session::Session,
    query::Query,
    response::query_result::QueryResult,
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
async fn query_iter_no_results() {
    let (session, ks) = initialize_cluster_two_partitions().await;

    let query = Query::new(format!("SELECT * FROM {}.t where k0 = ?", ks));

    let query_result = session.query_iter(query, ("part3",)).await.unwrap();
    let mut iter = query_result.rows_stream::<(String, i32, i32)>().unwrap();

    assert_eq!(iter.try_next().await.unwrap(), None);
}

#[tokio::test]
async fn query_iter_prepare_error() {
    let (session, ks) = initialize_cluster_two_partitions().await;

    // Wrong table name
    let query = Query::new(format!("SELECT * FROM {}.test where k0 = ?", ks));

    assert_matches!(
        session.query_iter(query, (PARTITION_KEY1,)).await,
        Err(PagerExecutionError::PrepareError(_))
    );
}

#[tokio::test]
async fn query_iter_serialization_error() {
    let (session, ks) = initialize_cluster_two_partitions().await;

    let query = Query::new(format!("SELECT * FROM {}.t where k0 = ?", ks));

    // Wrong value type
    assert_matches!(
        session.query_iter(query, (1,)).await,
        Err(PagerExecutionError::SerializationError(_))
    );
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

#[tokio::test]
async fn execute_iter_no_results() {
    let (session, ks) = initialize_cluster_two_partitions().await;

    let prepared_query = session
        .prepare(format!("SELECT * FROM {}.t where k0 = ?", ks))
        .await
        .unwrap();

    let query_result = session
        .execute_iter(prepared_query, ("part3",))
        .await
        .unwrap();
    let mut iter = query_result.rows_stream::<(String, i32, i32)>().unwrap();

    assert_eq!(iter.try_next().await.unwrap(), None);
}

#[tokio::test]
async fn execute_iter_serialization_error() {
    let (session, ks) = initialize_cluster_two_partitions().await;

    let prepared_query = session
        .prepare(format!("SELECT * FROM {}.t where k0 = ?", ks))
        .await
        .unwrap();

    // Wrong value type
    assert_matches!(
        session.execute_iter(prepared_query, (1,)).await,
        Err(PagerExecutionError::SerializationError(_))
    )
}

async fn create_session(table_name: &str) -> Session {
    let session: Session = create_new_session_builder().build().await.unwrap();
    let ks = unique_keyspace_name();

    let cql_create_ks = format!(
        "CREATE KEYSPACE IF NOT EXISTS {} WITH REPLICATION =\
         {{'class' : 'NetworkTopologyStrategy', 'replication_factor' : 1}}",
        ks
    );
    session.ddl(cql_create_ks).await.unwrap();
    session.use_keyspace(ks, false).await.unwrap();

    let cql_create_table = format!(
        "CREATE TABLE IF NOT EXISTS {} (id int PRIMARY KEY, val int)",
        table_name,
    );
    session.ddl(cql_create_table).await.unwrap();

    session
}

async fn check_query_unpaged(insert_num: u64, use_prepared_statements: bool) {
    let table_name = if use_prepared_statements {
        "execute_unpaged"
    } else {
        "query_unpaged"
    };
    let session: Session = create_session(table_name).await;

    for i in 0..insert_num {
        if use_prepared_statements {
            let prepared_statement = session
                .prepare(format!("INSERT INTO {}(id, val) VALUES (?, ?)", table_name))
                .await
                .unwrap();
            session
                .execute_unpaged(&prepared_statement, &vec![i as i32, i as i32])
                .await
                .unwrap();
        } else {
            let cql = format!("INSERT INTO {}(id, val) VALUES ({}, {})", table_name, i, i);
            session.query_unpaged(cql, &[]).await.unwrap();
        }
    }

    let query_result: QueryResult;
    if use_prepared_statements {
        let prepared_statement = session
            .prepare(format!("SELECT * FROM {}", table_name))
            .await
            .unwrap();
        query_result = session
            .execute_unpaged(&prepared_statement, &[])
            .await
            .unwrap();
    } else {
        let select_query = Query::new(format!("SELECT * FROM {}", table_name)).with_page_size(5);
        query_result = session.query_unpaged(select_query, &[]).await.unwrap();
    }
    let rows = query_result.into_rows_result().unwrap();

    // NOTE: check rows number using 'rows_num()' method.
    assert_eq!(rows.rows_num(), insert_num as usize);

    // NOTE: check actual rows number.
    let actual_rows = rows
        .rows::<(i32, i32)>()
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(actual_rows.len(), insert_num as usize);
}

async fn unpaged_error(use_prepared_statements: bool) {
    let table_name = if use_prepared_statements {
        "execute_unpaged"
    } else {
        "query_unpaged"
    };
    let session: Session = create_session(table_name).await;

    let query_result: Result<QueryResult, ExecutionError>;
    if use_prepared_statements {
        let prepared_statement = session
            .prepare(format!("SELECT * FROM {}", table_name))
            .await
            .unwrap();
        // NOTE: drop table to make the main query return error
        session
            .ddl(format!("DROP TABLE IF EXISTS {}", table_name))
            .await
            .unwrap();
        query_result = session.execute_unpaged(&prepared_statement, &[]).await;
    } else {
        let select_query = Query::new(format!("SELECT * FROM fake{}", table_name));
        query_result = session.query_unpaged(select_query, &[]).await;
    }
    match query_result {
        Ok(_) => panic!("Unexpected success"),
        Err(err) => println!("Table not found as expected: {:?}", err),
    }
}

#[tokio::test]
async fn test_query_unpaged_error() {
    unpaged_error(false).await
}

#[tokio::test]
async fn test_execute_unpaged_error() {
    unpaged_error(true).await
}

#[tokio::test]
async fn test_query_unpaged_no_rows() {
    check_query_unpaged(0, false).await;
}

#[tokio::test]
async fn test_query_unpaged_one_row() {
    check_query_unpaged(1, false).await;
}

#[tokio::test]
async fn test_query_unpaged_ten_rows() {
    check_query_unpaged(10, false).await;
}

#[tokio::test]
async fn test_query_unpaged_hundred_rows() {
    check_query_unpaged(100, false).await;
}

#[tokio::test]
async fn test_query_unpaged_thousand_rows() {
    check_query_unpaged(1000, false).await;
}

#[tokio::test]
async fn test_execute_unpaged_no_rows() {
    check_query_unpaged(0, true).await;
}

#[tokio::test]
async fn test_execute_unpaged_one_row() {
    check_query_unpaged(1, true).await;
}

#[tokio::test]
async fn test_execute_unpaged_ten_rows() {
    check_query_unpaged(10, true).await;
}

#[tokio::test]
async fn test_execute_unpaged_hundred_rows() {
    check_query_unpaged(100, true).await;
}

#[tokio::test]
async fn test_execute_unpaged_thousand_rows() {
    check_query_unpaged(1000, true).await;
}
