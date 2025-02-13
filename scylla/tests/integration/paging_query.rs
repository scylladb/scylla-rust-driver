#[cfg(scylla_cloud_tests)]
use crate::utils::create_new_session_builder;
use crate::utils::{setup_tracing, test_with_3_node_cluster, unique_keyspace_name, PerformDDL};
use assert_matches::assert_matches;
use scylla::client::session::Session;
use scylla::client::session_builder::SessionBuilder;
use scylla::errors::{BadQuery, ExecutionError, RequestAttemptError};
use scylla::prepared_statement::PreparedStatement;
use scylla::query::Query;
use scylla::response::query_result::QueryResult;
use scylla_cql::frame::request::query::{PagingState, PagingStateResponse};
use scylla_cql::frame::request::RequestOpcode;
use scylla_cql::frame::response::error::DbError;
use scylla_cql::Consistency;
use scylla_cql::_macro_internal::SerializeRow;
use scylla_proxy::{
    Condition, ProxyError, RequestReaction, RequestRule, RunningProxy, ShardAwareness, WorkerError,
};
use std::error::Error;
use std::ops::ControlFlow;
use std::sync::Arc;

const PAGE_SIZE: i32 = 10;
const ITEMS: i32 = 20;

#[derive(Clone)]
enum Statement {
    Prepared(PreparedStatement),
    Simple(Query),
}

async fn prepare_data(session: impl AsRef<Session>) -> String {
    let ks = unique_keyspace_name();
    let session = session.as_ref();

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

    #[cfg(not(scylla_cloud_tests))]
    prepared_insert.set_consistency(Consistency::Quorum);

    for i in 0..ITEMS {
        session
            .execute_unpaged(&prepared_insert, (i,))
            .await
            .unwrap();
    }

    ks
}

async fn execute_statement(
    session: impl AsRef<Session>,
    statement: Statement,
    args: impl SerializeRow,
    paging_state: PagingState,
) -> Result<(QueryResult, PagingStateResponse), ExecutionError> {
    match statement {
        Statement::Simple(query) => {
            session
                .as_ref()
                .query_single_page(query, args, paging_state)
                .await
        }
        Statement::Prepared(prepared) => {
            session
                .as_ref()
                .execute_single_page(&prepared, args, paging_state)
                .await
        }
    }
}

async fn test_callback<F, Fut>(
    session: Arc<Session>,
    query: impl AsRef<str>,
    callback: F,
    running_proxy: Option<RunningProxy>,
) -> Option<RunningProxy>
where
    F: Fn(Arc<Session>, Option<RunningProxy>, Statement) -> Fut,
    Fut: std::future::Future<Output = Result<Option<RunningProxy>, Box<dyn Error>>>,
{
    let ks = prepare_data(Arc::clone(&session)).await;

    let query = query.as_ref().to_string().replace("%keyspace%", &ks);

    let mut query = Query::from(query.clone());
    #[cfg(not(scylla_cloud_tests))]
    query.set_consistency(Consistency::Quorum);
    query.set_page_size(PAGE_SIZE);

    let mut prepared = session.prepare(query.clone()).await.unwrap();
    #[cfg(not(scylla_cloud_tests))]
    prepared.set_consistency(Consistency::Quorum);
    prepared.set_page_size(PAGE_SIZE);

    let mut running_proxy = callback(
        Arc::clone(&session),
        running_proxy,
        Statement::Simple(query),
    )
    .await
    .unwrap();

    if let Some(ref mut running_proxy) = running_proxy {
        running_proxy.running_nodes.iter_mut().for_each(|node| {
            node.change_response_rules(None);
            node.change_request_rules(None);
        });
    }

    callback(
        Arc::clone(&session),
        running_proxy,
        Statement::Prepared(prepared),
    )
    .await
    .unwrap()
}

async fn execute_test<F, Fut>(query: impl AsRef<str>, callback: F)
where
    F: Fn(Arc<Session>, Option<RunningProxy>, Statement) -> Fut,
    Fut: std::future::Future<Output = Result<Option<RunningProxy>, Box<dyn Error>>>,
{
    setup_tracing();

    #[cfg(scylla_cloud_tests)]
    {
        let session = Arc::new(create_new_session_builder().build().await.unwrap());
        test_callback(session, query, callback, None).await;
    }

    #[cfg(not(scylla_cloud_tests))]
    let result = test_with_3_node_cluster(
        ShardAwareness::QueryNode,
        |proxy_uris, translation_map, running_proxy| async move {
            let session = Arc::new(
                SessionBuilder::new()
                    .known_node(proxy_uris[0].as_str())
                    .address_translator(Arc::new(translation_map))
                    .build()
                    .await
                    .unwrap(),
            );

            test_callback(session, query, callback, Some(running_proxy))
                .await
                .unwrap()
        },
    )
    .await;

    #[cfg(not(scylla_cloud_tests))]
    match result {
        Ok(_) => {}
        Err(ProxyError::Worker(WorkerError::DriverDisconnected(_))) => {}
        Err(err) => panic!("{err:?}"),
    }
}

#[tokio::test]
async fn test_paging_single_page_result() {
    execute_test(
        "SELECT a FROM %keyspace%.t WHERE a = ?",
        |session, running_proxy, statement| async move {
            let (query_result, paging_state_response) =
                execute_statement(&session, statement, (0,), PagingState::start()).await?;

            assert_eq!(query_result.into_rows_result()?.rows_num(), 1);
            assert!(paging_state_response.finished());

            Ok(running_proxy)
        },
    )
    .await;
}
#[tokio::test]
async fn test_paging_single_page_single_result() {
    execute_test(
        "SELECT a FROM %keyspace%.t WHERE a = ?",
        |session, running_proxy, statement| async move {
            let (query_result, paging_state_response) =
                execute_statement(&session, statement, (0,), PagingState::start()).await?;

            let results = query_result.into_rows_result()?;
            assert_eq!(results.rows_num(), 1);
            assert!(paging_state_response.finished());

            let (a,) = results.single_row::<(i32,)>()?;
            assert_eq!(a, 0);

            Ok(running_proxy)
        },
    )
    .await;
}

#[tokio::test]
async fn test_paging_multiple_no_errors() {
    execute_test(
        "SELECT a FROM %keyspace%.t",
        |session, running_proxy, statement| async move {
            let mut state = PagingState::start();

            for _ in 0..ITEMS / PAGE_SIZE {
                let (query_result, paging_state_response) =
                    execute_statement(&session, statement.clone(), &[], state.clone()).await?;
                match paging_state_response.into_paging_control_flow() {
                    ControlFlow::Break(_) => {
                        panic!("Unexpected break");
                    }
                    ControlFlow::Continue(new_paging_state) => {
                        assert_eq!(
                            query_result.into_rows_result()?.rows_num(),
                            PAGE_SIZE as usize
                        );
                        state = new_paging_state;
                    }
                }
            }

            Ok(running_proxy)
        },
    )
    .await;
}

#[tokio::test]
#[cfg(not(scylla_cloud_tests))]
async fn test_paging_error() {
    execute_test(
        "SELECT a FROM %keyspace%.t WHERE a = ?",
        |session, running_proxy, statement| async move {
            let result = execute_statement(
                &session,
                statement.clone(),
                ("hello world",),
                PagingState::start(),
            )
            .await
            .unwrap_err();

            match statement {
                Statement::Simple(_) => {
                    assert_matches!(
                        result,
                        ExecutionError::LastAttemptError(RequestAttemptError::SerializationError(
                            _
                        ))
                    );
                }
                Statement::Prepared(_) => {
                    assert_matches!(
                        result,
                        ExecutionError::BadQuery(BadQuery::SerializationError(_))
                    );
                }
            }

            Ok(running_proxy)
        },
    )
    .await;
}

#[tokio::test]
#[cfg(not(scylla_cloud_tests))]
async fn test_paging_error_on_next_page() {
    execute_test(
        "SELECT a FROM %keyspace%.t",
        |session, mut running_proxy, statement| async move {
            let mut state = PagingState::start();
            let (_, paging_state_resp) =
                execute_statement(&session, statement.clone(), (), state.clone()).await?;

            state = match paging_state_resp.into_paging_control_flow() {
                ControlFlow::Continue(x) => x,
                ControlFlow::Break(..) => panic!("Unexpected break"),
            };

            running_proxy
                .as_mut()
                .unwrap()
                .running_nodes
                .iter_mut()
                .for_each(|node| {
                    node.change_request_rules(Some(vec![
                        RequestRule(
                            Condition::RequestOpcode(RequestOpcode::Execute),
                            RequestReaction::forge_with_error(DbError::ServerError),
                        ),
                        RequestRule(
                            Condition::RequestOpcode(RequestOpcode::Query),
                            RequestReaction::forge_with_error(DbError::ServerError),
                        ),
                    ]))
                });

            let result = execute_statement(&session, statement.clone(), (), state.clone())
                .await
                .unwrap_err();

            assert_matches!(
                result,
                ExecutionError::LastAttemptError(RequestAttemptError::DbError(
                    DbError::ServerError,
                    ..
                ))
            );

            Ok(running_proxy)
        },
    )
    .await;
}
