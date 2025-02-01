//! These tests check that all nodes are reachable, i.e. they can serve requests.

use scylla::client::session::Session;
use scylla::serialize::row::SerializeRow;

use crate::utils::{
    create_new_session_builder, execute_prepared_statement_everywhere, setup_tracing,
    unique_keyspace_name, PerformDDL as _,
};

/// Tests that all nodes are reachable and can serve requests.
#[tokio::test]
#[ntest::timeout(30000)]
async fn test_all_nodes_are_reachable_and_serving() {
    setup_tracing();

    let session = create_new_session_builder().build().await.unwrap();

    let ks = unique_keyspace_name();

    /* Prepare schema */
    prepare_schema(&session, &ks, "t").await;

    let prepared = session
        .prepare(format!(
            "INSERT INTO {}.t (a, b, c) VALUES (?, ?, 'abc')",
            ks
        ))
        .await
        .unwrap();

    let cluster_state = session.get_cluster_state();
    execute_prepared_statement_everywhere(
        &session,
        &cluster_state,
        &prepared,
        &(1, 2) as &dyn SerializeRow,
    )
    .await
    .unwrap();
}

async fn prepare_schema(session: &Session, ks: &str, table: &str) {
    session
        .ddl(format!(
            "CREATE KEYSPACE IF NOT EXISTS {}
            WITH REPLICATION = {{'class' : 'NetworkTopologyStrategy', 'replication_factor' : 1}}",
            ks
        ))
        .await
        .unwrap();
    session
        .ddl(format!(
            "CREATE TABLE IF NOT EXISTS {}.{} (a int, b int, c text, primary key (a, b))",
            ks, table
        ))
        .await
        .unwrap();
}
