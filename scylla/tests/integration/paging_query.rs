use crate::utils::{setup_tracing, test_with_3_node_cluster, unique_keyspace_name, PerformDDL};
use scylla::client::session::Session;
use scylla::client::session_builder::SessionBuilder;
use scylla::query::Query;
use scylla_cql::frame::request::query::PagingState;
use scylla_cql::Consistency;
use scylla_proxy::{ProxyError, ShardAwareness, WorkerError};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::ops::ControlFlow;
use std::sync::Arc;

const PAGE_SIZE: usize = 10;
const ITEMS: usize = 20;

async fn prepare_data(
    proxy_uris: [String; 3],
    translation_map: HashMap<SocketAddr, SocketAddr>,
) -> (String, Session) {
    let session = SessionBuilder::new()
        .known_node(proxy_uris[0].as_str())
        .address_translator(Arc::new(translation_map))
        .build()
        .await
        .unwrap();

    let ks = unique_keyspace_name();

    session.ddl(
        format!("CREATE KEYSPACE IF NOT EXISTS {ks} WITH REPLICATION = {{'class' : 'NetworkTopologyStrategy', 'replication_factor' : 3}}")
    )
        .await.
        unwrap();
    session
        .ddl(format!(
            "CREATE TABLE IF NOT EXISTS {}.t (a int, primary key (a))",
            ks
        ))
        .await
        .unwrap();

    let mut prepared_insert = session
        .prepare(format!("INSERT INTO {ks}.t (a) VALUES (?)"))
        .await
        .unwrap();

    prepared_insert.set_consistency(Consistency::Quorum);

    for i in 0..ITEMS as i32 {
        session
            .execute_unpaged(&prepared_insert, (i,))
            .await
            .unwrap();
    }

    (ks, session)
}

#[tokio::test]
async fn test_paging_single_page_result() {
    setup_tracing();
    let result = test_with_3_node_cluster(
        ShardAwareness::QueryNode,
        |proxy_uris, translation_map, running_proxy| async move {
            let (ks, session) = prepare_data(proxy_uris, translation_map).await;

            let mut query = Query::from(format!("SELECT a FROM {}.t WHERE a = ?", ks));
            query.set_consistency(Consistency::Quorum);
            query.set_page_size(10);

            let state = PagingState::default();
            let result = session.query_single_page(query, (0,), state).await;

            assert!(result.is_ok());
            let (query_result, paging_state_response) = result.unwrap();

            assert_eq!(query_result.into_rows_result().unwrap().rows_num(), 1);
            assert!(paging_state_response.finished());

            running_proxy
        },
    )
    .await;

    match result {
        Ok(_) => {}
        Err(ProxyError::Worker(WorkerError::DriverDisconnected(_))) => {}
        Err(err) => panic!("{err:?}"),
    }
}
#[tokio::test]
async fn test_paging_single_page_result_prepared() {
    setup_tracing();
    let result = test_with_3_node_cluster(
        ShardAwareness::QueryNode,
        |proxy_uris, translation_map, running_proxy| async move {
            let (ks, session) = prepare_data(proxy_uris, translation_map).await;

            let mut query = session
                .prepare(format!("SELECT a FROM {}.t WHERE a = ?", ks))
                .await
                .unwrap();
            query.set_consistency(Consistency::Quorum);
            query.set_page_size(10);

            let state = PagingState::default();
            let result = session.execute_single_page(&query, (0,), state).await;

            assert!(result.is_ok());
            let (query_result, paging_state_response) = result.unwrap();

            assert_eq!(query_result.into_rows_result().unwrap().rows_num(), 1);
            assert!(paging_state_response.finished());

            running_proxy
        },
    )
    .await;

    match result {
        Ok(_) => {}
        Err(ProxyError::Worker(WorkerError::DriverDisconnected(_))) => {}
        Err(err) => panic!("{err:?}"),
    }
}

#[tokio::test]
async fn test_paging_multiple_no_errors() {
    setup_tracing();
    let result = test_with_3_node_cluster(
        ShardAwareness::QueryNode,
        |proxy_uris, translation_map, running_proxy| async move {
            let (ks, session) = prepare_data(proxy_uris, translation_map).await;

            let mut query = session
                .prepare(format!("SELECT a FROM {}.t WHERE a = ?", ks))
                .await
                .unwrap();
            query.set_consistency(Consistency::Quorum);
            query.set_page_size(10);

            let mut state = PagingState::default();

            let mut counter = 0;
            loop {
                let result = session
                    .execute_single_page(&query, &(), state.clone())
                    .await;
                let (query_result, paging_state_response) = result.unwrap();
                match paging_state_response.into_paging_control_flow() {
                    ControlFlow::Break(()) => {
                        break;
                    }
                    ControlFlow::Continue(new_paging_state) => {
                        assert_eq!(query_result.into_rows_result().unwrap().rows_num(), 10);
                        state = new_paging_state;
                    }
                }

                counter += 1;
            }

            assert_eq!(counter, ITEMS / PAGE_SIZE);

            running_proxy
        },
    )
    .await;

    match result {
        Ok(_) => {}
        Err(ProxyError::Worker(WorkerError::DriverDisconnected(_))) => {}
        Err(err) => panic!("{err:?}"),
    }
}

#[tokio::test]
async fn test_paging_multiple_no_errors_prepared() {
    setup_tracing();
    let result = test_with_3_node_cluster(
        ShardAwareness::QueryNode,
        |proxy_uris, translation_map, running_proxy| async move {
            let (ks, session) = prepare_data(proxy_uris, translation_map).await;

            let mut query = Query::from(format!("SELECT a FROM {}.t", ks));
            query.set_consistency(Consistency::Quorum);
            query.set_page_size(PAGE_SIZE as i32);

            let mut state = PagingState::default();

            let mut counter = 0;
            loop {
                let result = session
                    .query_single_page(query.clone(), &(), state.clone())
                    .await;
                let (query_result, paging_state_response) = result.unwrap();
                match paging_state_response.into_paging_control_flow() {
                    ControlFlow::Break(()) => {
                        break;
                    }
                    ControlFlow::Continue(new_paging_state) => {
                        assert_eq!(query_result.into_rows_result().unwrap().rows_num(), 10);
                        state = new_paging_state;
                    }
                }

                counter += 1;
            }

            assert_eq!(counter, ITEMS / PAGE_SIZE);

            running_proxy
        },
    )
    .await;

    match result {
        Ok(_) => {}
        Err(ProxyError::Worker(WorkerError::DriverDisconnected(_))) => {}
        Err(err) => panic!("{err:?}"),
    }
}

#[tokio::test]
async fn test_paging_error() {}

#[tokio::test]
async fn test_paging_error_prepared() {}

#[tokio::test]
async fn test_paging_error_on_next_page() {}

#[tokio::test]
async fn test_paging_error_on_next_page_prepared() {}

#[tokio::test]
async fn test_paging_wrong_page_state() {}

#[tokio::test]
async fn test_paging_wrong_page_state_prepared() {}
