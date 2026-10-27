use std::sync::Arc;
use std::time::Duration;

use assert_matches::assert_matches;
use futures::StreamExt as _;
use scylla::client::session::Session;
use scylla::errors::{ExecutionError, NextPageError, NextRowError};
use scylla::statement::Statement;
use tokio::task::JoinHandle;

use crate::utils::{
    PerformDDL as _, create_new_session_builder, setup_tracing, unique_keyspace_name,
};

const SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(10);

/// How long the writers run before a test shuts the session down.
const LOAD_TIME: Duration = Duration::from_millis(200);

/// Connects a session, and creates a keyspace with the table `t`.
async fn setup() -> (Arc<Session>, String) {
    setup_tracing();
    let session = create_new_session_builder().build().await.unwrap();
    let ks = unique_keyspace_name();
    session
        .ddl(format!(
            "CREATE KEYSPACE IF NOT EXISTS {ks} WITH \
             REPLICATION = {{'class' : 'NetworkTopologyStrategy', 'replication_factor' : 1}}"
        ))
        .await
        .unwrap();
    session
        .ddl(format!(
            "CREATE TABLE {ks}.t (pk int, ck int, PRIMARY KEY (pk, ck))"
        ))
        .await
        .unwrap();
    (Arc::new(session), ks)
}

/// Drops `ks`. The session of a test is shut down, so this uses a new one.
async fn drop_keyspace(ks: &str) {
    let session = create_new_session_builder().build().await.unwrap();
    session.ddl(format!("DROP KEYSPACE {ks}")).await.unwrap();
}

/// Starts writers that insert rows until the session rejects a request.
/// Each writer returns the number of its requests that succeeded.
///
/// If `allow_other_errors` is false, a writer panics on an error other than
/// `SessionShutDown`. A forced shutdown breaks the requests in flight, so
/// tests of it allow other errors.
async fn start_writers(
    session: &Arc<Session>,
    ks: &str,
    allow_other_errors: bool,
) -> Vec<JoinHandle<usize>> {
    let insert = session
        .prepare(format!("INSERT INTO {ks}.t (pk, ck) VALUES (?, ?)"))
        .await
        .unwrap();
    (0..8)
        .map(|pk: i32| {
            let session = Arc::clone(session);
            let insert = insert.clone();
            tokio::spawn(async move {
                let mut completed = 0;
                for ck in 0.. {
                    match session.execute_unpaged(&insert, (pk, ck)).await {
                        Ok(_) => completed += 1,
                        Err(ExecutionError::SessionShutDown(_)) => break,
                        Err(_) if allow_other_errors => {}
                        Err(err) => panic!("A request failed during the shutdown: {err}"),
                    }
                }
                completed
            })
        })
        .collect()
}

/// Waits until all writers stop, and returns their numbers of successful requests.
async fn join_writers(writers: Vec<JoinHandle<usize>>) -> Vec<usize> {
    let mut completed = Vec::new();
    for writer in writers {
        completed.push(
            tokio::time::timeout(SHUTDOWN_TIMEOUT, writer)
                .await
                .unwrap()
                .unwrap(),
        );
    }
    completed
}

fn assert_disconnected(session: &Session) {
    let cluster_state = session.get_cluster_state();
    assert!(
        cluster_state
            .get_nodes_info()
            .iter()
            .all(|node| !node.is_connected())
    );
}

/// A request that runs during a shutdown either completes, or fails because
/// the session rejected it. The shutdown does not break admitted requests.
#[tokio::test]
async fn test_shutdown_under_load() {
    let (session, ks) = setup().await;
    let writers = start_writers(&session, &ks, false).await;

    tokio::time::sleep(LOAD_TIME).await;
    tokio::time::timeout(SHUTDOWN_TIMEOUT, session.shutdown())
        .await
        .unwrap();

    assert!(join_writers(writers).await.iter().all(|&n| n > 0));
    assert_disconnected(&session);
    drop_keyspace(&ks).await;
}

/// The requests of a normal load complete well within the timeout, so the
/// shutdown is graceful, and it is not forced.
#[tokio::test]
async fn test_shutdown_timeout_under_load() {
    let (session, ks) = setup().await;
    let writers = start_writers(&session, &ks, false).await;

    tokio::time::sleep(LOAD_TIME).await;
    assert!(session.shutdown_timeout(SHUTDOWN_TIMEOUT).await);

    assert!(join_writers(writers).await.iter().all(|&n| n > 0));
    assert_disconnected(&session);
    drop_keyspace(&ks).await;
}

/// A forced shutdown closes all connections at once. The requests in flight
/// can fail, and all later requests fail because the session rejects them.
#[tokio::test]
async fn test_force_shutdown_under_load() {
    let (session, ks) = setup().await;
    let writers = start_writers(&session, &ks, true).await;

    tokio::time::sleep(LOAD_TIME).await;
    tokio::time::timeout(SHUTDOWN_TIMEOUT, session.force_shutdown())
        .await
        .unwrap();

    assert_disconnected(&session);
    join_writers(writers).await;
    drop_keyspace(&ks).await;
}

/// A pager that the user does not read does not delay the shutdown.
/// After the shutdown, it fails when it must fetch a new page.
#[tokio::test]
async fn test_shutdown_with_unread_pager() {
    let (session, ks) = setup().await;

    const ROWS: i32 = 10;
    for ck in 0..ROWS {
        session
            .query_unpaged(format!("INSERT INTO {ks}.t (pk, ck) VALUES (0, ?)"), (ck,))
            .await
            .unwrap();
    }

    let mut select = Statement::new(format!("SELECT ck FROM {ks}.t WHERE pk = 0"));
    select.set_page_size(1);
    let mut rows = session
        .query_iter(select, ())
        .await
        .unwrap()
        .rows_stream::<(i32,)>()
        .unwrap();

    tokio::time::timeout(SHUTDOWN_TIMEOUT, session.shutdown())
        .await
        .unwrap();

    let mut fetched_rows = 0;
    let error = loop {
        match rows.next().await {
            Some(Ok(_)) => fetched_rows += 1,
            Some(Err(error)) => break error,
            None => panic!("The pager returned all rows after the shutdown"),
        }
    };
    assert!((1..ROWS).contains(&fetched_rows));
    assert_matches!(
        error,
        NextRowError::NextPageError(NextPageError::SessionShutDown(_))
    );
    drop_keyspace(&ks).await;
}
