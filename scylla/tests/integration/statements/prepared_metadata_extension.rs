use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::task::Poll;

use futures::StreamExt;
use itertools::Itertools;
use scylla::client::session::Session;
use scylla::client::session_builder::SessionBuilder;
use scylla::cluster::metadata::{ColumnType, NativeType};
use scylla::errors::DbError;
use scylla::frame::response::result::{ColumnSpec, TableSpec};
use scylla::statement::prepared::PreparedStatement;
use scylla_cql::frame::protocol_features::ProtocolFeatures;
use scylla_cql::frame::request::DeserializableRequest;
use scylla_cql::frame::request::execute::ExecuteV2;
use scylla_cql::frame::response::result::Result;
use scylla_cql::frame::response::{Response, Supported};
use scylla_cql::frame::{parse_response_body_extensions, types};
use scylla_proxy::{
    Condition, ProxyError, Reaction, RequestFrame, RequestReaction, RequestRule, ResponseFrame,
    ResponseReaction, ResponseRule, RunningProxy, WorkerError,
};

use crate::utils::{
    PerformDDL, fetch_negotiated_features, setup_tracing, test_with_3_node_cluster,
    unique_keyspace_name,
};

async fn prepare_schema_and_data(session: &Session, ks: &str) {
    tracing::info!("Preparing schema and data");
    session
        .ddl(format!(
            "CREATE KEYSPACE IF NOT EXISTS {ks} WITH REPLICATION =
        {{'class' : 'NetworkTopologyStrategy', 'replication_factor' : 1}}"
        ))
        .await
        .unwrap();
    session
        .ddl(format!(
            "CREATE TABLE IF NOT EXISTS {ks}.t (a int, b Text, primary key (a))"
        ))
        .await
        .unwrap();
    session
        .query_unpaged(
            format!("INSERT INTO {ks}.t (a, b) VALUES (?, ?)"),
            (1, "abc"),
        )
        .await
        .unwrap();
    tracing::info!("Preparing schema and data finished");
}

async fn drop_schema(session: &Session, ks: &str) {
    session.ddl(format!("DROP KEYSPACE  {ks}")).await.unwrap();
}

fn assert_old_schema(stmt: &PreparedStatement, ks: &str) {
    use ColumnType::*;
    use NativeType::*;
    let guard = stmt.get_current_result_set_col_specs();
    let table_spec = TableSpec::borrowed(ks, "t");

    assert_eq!(
        guard.get().as_slice()[0],
        ColumnSpec::borrowed("a", Native(Int), table_spec.clone())
    );
    assert_eq!(
        guard.get().as_slice()[1],
        ColumnSpec::borrowed("b", Native(Text), table_spec.clone())
    );
    assert_eq!(guard.get().as_slice().len(), 2);
}

fn assert_new_schema(stmt: &PreparedStatement, ks: &str) {
    use ColumnType::*;
    use NativeType::*;
    let guard = stmt.get_current_result_set_col_specs();
    let table_spec = TableSpec::borrowed(ks, "t");

    assert_eq!(
        guard.get().as_slice()[0],
        ColumnSpec::borrowed("a", Native(Int), table_spec.clone())
    );
    assert_eq!(
        guard.get().as_slice()[1],
        ColumnSpec::borrowed("b", Native(Text), table_spec.clone())
    );
    assert_eq!(
        guard.get().as_slice()[2],
        ColumnSpec::borrowed("c", Native(Text), table_spec.clone())
    );
    assert_eq!(guard.get().as_slice().len(), 3);
}

// Creates two sessions, and one `SELECT *` statement for each session.
// Tests that:
// - Statements initially have the same metadata.
// - After alter, statements still have same metadata.
// - After executing statement from session 1, its metadata is updated.
// - After that, statement 2 still has old metadata.
// - For executing statement 2 after that, reprepare is not needed and new metadata is sent.
async fn perform_test_for_proxy(
    proxy_uris: &[String; 3],
    translation_map: &HashMap<SocketAddr, SocketAddr>,
    mut running_proxy: RunningProxy,
    features: ProtocolFeatures,
    apply_prepare_rules: impl FnOnce(&mut RunningProxy),
    apply_execution_rules: impl FnOnce(&mut RunningProxy),
) -> RunningProxy {
    use Condition::*;
    use scylla_proxy::RequestOpcode::Prepare;

    let session_1 = SessionBuilder::new()
        .known_node(proxy_uris[0].as_str())
        .address_translator(Arc::new(translation_map.clone()))
        .build()
        .await
        .unwrap();
    let session_2 = SessionBuilder::new()
        .known_node(proxy_uris[0].as_str())
        .address_translator(Arc::new(translation_map.clone()))
        .build()
        .await
        .unwrap();
    let ks = unique_keyspace_name();
    prepare_schema_and_data(&session_1, &ks).await;

    apply_prepare_rules(&mut running_proxy);
    let query_str = format!("SELECT * FROM {ks}.t WHERE a = ?");
    tracing::info!("Preparing statement 1");
    let statement_1 = session_1.prepare(query_str.as_str()).await.unwrap();
    tracing::info!("Preparing statement 2");
    let statement_2 = session_2.prepare(query_str.as_str()).await.unwrap();

    running_proxy
        .running_nodes
        .iter_mut()
        .for_each(|node| node.change_request_rules(Some(vec![])));

    // Prepare again, to make sure statement is in cache on all nodes.
    tracing::info!("Preparing statements again");
    let _ = session_1.prepare(query_str.as_str()).await.unwrap();

    {
        // Nothing happened with schema yet, so both statements should have 2 columns in
        // result metadata.
        assert_old_schema(&statement_1, &ks);
        assert_old_schema(&statement_2, &ks);
    }

    tracing::info!("Altering table");
    session_1
        .query_unpaged(format!("ALTER TABLE {ks}.t ADD c text"), &())
        .await
        .unwrap();

    {
        // Statements are not being updated without being executed.
        assert_old_schema(&statement_1, &ks);
        assert_old_schema(&statement_2, &ks);
    }

    apply_execution_rules(&mut running_proxy);

    tracing::info!("Executing statement 1");
    let result_1 = session_1
        .execute_unpaged(&statement_1, &(1,))
        .await
        .unwrap()
        .into_rows_result()
        .unwrap();
    tracing::info!("Re-preparing statement");
    let _ = session_1.prepare(query_str.as_str()).await.unwrap();

    {
        // We Altered the schema, invalidating the cache.
        // Execution of statement 1 should give it new metadata.
        // Repreparation of statement on all nodes made sure it is in cache again,
        // so now if we execute statement 2 it should not need reprepare.
        assert_new_schema(&statement_1, &ks);
        assert_old_schema(&statement_2, &ks);

        let rows: Vec<_> = result_1
            .rows::<(i32, &str, Option<&str>)>()
            .unwrap()
            .try_collect()
            .unwrap();
        assert_eq!(rows.as_slice(), &[(1, "abc", None)]);
    }

    // Second request should not need reprepare.
    let (tx_prepare, rx_prepare) = tokio::sync::mpsc::unbounded_channel();
    running_proxy.running_nodes.iter_mut().for_each(|node| {
        node.prepend_request_rules(vec![RequestRule(
            Condition::not(ConnectionRegisteredAnyEvent).and(RequestOpcode(Prepare)),
            RequestReaction::noop().with_feedback_when_performed(tx_prepare.clone()),
        )])
    });

    // Response should contain new metadata id, let's check that.
    let (tx_result, mut rx_result) = tokio::sync::mpsc::unbounded_channel();
    running_proxy.running_nodes.iter_mut().for_each(|node| {
        node.prepend_response_rules(vec![ResponseRule(
            Condition::not(ConnectionRegisteredAnyEvent)
                .and(ResponseOpcode(scylla_proxy::ResponseOpcode::Result)),
            ResponseReaction::noop().with_feedback_when_performed(tx_result.clone()),
        )])
    });

    tracing::info!("Executing statement 2");
    let result_2 = session_2
        .execute_unpaged(&statement_2, &(1,))
        .await
        .unwrap()
        .into_rows_result()
        .unwrap();

    {
        assert!(rx_prepare.is_empty());
        assert_new_schema(&statement_2, &ks);

        let frame = rx_result.recv().await.unwrap().0;
        let body_with_extensions =
            parse_response_body_extensions(frame.params.flags, None, frame.body).unwrap();
        let response =
            Response::deserialize(&features, frame.opcode, body_with_extensions.body, None)
                .unwrap();
        let Response::Result(Result::Rows(raw_rows)) = response else {
            panic!("Wrong response type");
        };
        assert!(raw_rows.0.metadata_changed());

        let rows: Vec<_> = result_2
            .rows::<(i32, &str, Option<&str>)>()
            .unwrap()
            .try_collect()
            .unwrap();
        assert_eq!(rows.as_slice(), &[(1, "abc", None)]);
    }

    // If we execute the statement again, metadata should not change.
    let result_2_again = session_2
        .execute_unpaged(&statement_2, &(1,))
        .await
        .unwrap()
        .into_rows_result()
        .unwrap();

    {
        assert!(rx_prepare.is_empty());
        assert_new_schema(&statement_2, &ks);

        let frame = rx_result.recv().await.unwrap().0;
        let body_with_extensions =
            parse_response_body_extensions(frame.params.flags, None, frame.body).unwrap();
        let response =
            Response::deserialize(&features, frame.opcode, body_with_extensions.body, None)
                .unwrap();
        let Response::Result(Result::Rows(raw_rows)) = response else {
            panic!("Wrong response type");
        };
        assert!(!raw_rows.0.metadata_changed());
        assert!(raw_rows.0.no_metadata());

        let rows: Vec<_> = result_2_again
            .rows::<(i32, &str, Option<&str>)>()
            .unwrap()
            .try_collect()
            .unwrap();
        assert_eq!(rows.as_slice(), &[(1, "abc", None)]);
    }

    running_proxy.turn_off_rules();
    drop_schema(&session_1, &ks).await;
    running_proxy
}

// Test for the basic scenario that result metadata id extension is supposed to fix.
// We have 2 clients using 'SELECT *' query. Schema is changed. One of the clients
// re-inserts statement into the cache. The other client should have its metadata
// updated after executing the request.
#[tokio::test]
#[ntest::timeout(10000)]
async fn test_basic_metadata_update() {
    setup_tracing();

    let features = fetch_negotiated_features(None).await;
    if !features.scylla_metadata_id_supported {
        // Rust has no way to mark test as ignored at runtime.
        return;
    }

    let res = test_with_3_node_cluster(
        scylla_proxy::ShardAwareness::QueryNode,
        |proxy_uris, translation_map, running_proxy| async move {
            perform_test_for_proxy(
                &proxy_uris,
                &translation_map,
                running_proxy,
                features,
                |_| (),
                |_| (),
            )
            .await
        },
    )
    .await;

    match res {
        Ok(()) => (),
        Err(ProxyError::Worker(WorkerError::DriverDisconnected(_))) => (),
        Err(err) => panic!("{}", err),
    }
}

// One of the nodes is on DB version that does not support the extension.
// Verify that if we prepare statement on such node, and execute it on another,
// everything works.
#[tokio::test]
#[ntest::timeout(10000)]
async fn test_mixed_cluster() {
    use Condition::*;
    setup_tracing();

    let features = fetch_negotiated_features(None).await;
    if !features.scylla_metadata_id_supported {
        // Rust has no way to mark test as ignored at runtime.
        return;
    }

    let res = test_with_3_node_cluster(
        scylla_proxy::ShardAwareness::QueryNode,
        |proxy_uris, translation_map, mut running_proxy| async move {
            // Disable the extension on node 0
            running_proxy.running_nodes[0].change_response_rules(Some(vec![ResponseRule(
                ResponseOpcode(scylla_proxy::ResponseOpcode::Supported),
                ResponseReaction::transform_frame(Arc::new(|mut response: ResponseFrame| {
                    let mut msg = Supported::deserialize(&mut &*response.body).unwrap();
                    msg.options.remove("SCYLLA_USE_METADATA_ID");
                    // scylla-cql has no capability to serialize responses...
                    let mut new_body = Vec::new();
                    types::write_string_multimap(&msg.options, &mut new_body).unwrap();
                    response.body = new_body.into();
                    response
                })),
            )]));

            fn apply_prepare_rules(running_proxy: &mut RunningProxy) {
                // We want to only PREPARE on the node without extension,
                // in order to get PreparedStatement without metadata id.
                running_proxy.running_nodes[1..]
                    .iter_mut()
                    .for_each(|node| {
                        node.change_request_rules(Some(vec![RequestRule(
                            Condition::not(ConnectionRegisteredAnyEvent)
                                .and(RequestOpcode(scylla_proxy::RequestOpcode::Prepare)),
                            RequestReaction::forge_with_error(DbError::IsBootstrapping),
                        )]))
                    });
            }

            let (tx_execute, mut rx_execute) = tokio::sync::mpsc::unbounded_channel();

            let apply_execute_rules = move |running_proxy: &mut RunningProxy| {
                // We want to EXECUTE only on nodes with extension,
                // to be forced to send empty metadata id.
                // Need to forge the response that will cause a retry.
                running_proxy.running_nodes[0].change_request_rules(Some(vec![RequestRule(
                    Condition::not(ConnectionRegisteredAnyEvent)
                        .and(RequestOpcode(scylla_proxy::RequestOpcode::Execute)),
                    // Default retry policy always retries on next target for this error.
                    RequestReaction::forge_with_error(DbError::IsBootstrapping),
                )]));

                // Let's verify that empty metadata id is sent in first two requests.
                running_proxy.running_nodes[1..]
                    .iter_mut()
                    .for_each(|node| {
                        node.change_request_rules(Some(vec![RequestRule(
                            Condition::not(ConnectionRegisteredAnyEvent)
                                .and(RequestOpcode(scylla_proxy::RequestOpcode::Execute)),
                            // Default retry policy always retries on next target for this error.
                            RequestReaction::noop()
                                .with_feedback_when_performed(tx_execute.clone()),
                        )]))
                    });
            };

            let running_proxy = perform_test_for_proxy(
                &proxy_uris,
                &translation_map,
                running_proxy,
                features,
                &apply_prepare_rules,
                apply_execute_rules,
            )
            .await;

            let mut sent_ids =
                futures::stream::poll_fn(|ctx| -> Poll<Option<(RequestFrame, Option<u16>)>> {
                    rx_execute.poll_recv(ctx)
                })
                .map(|(frame, _)| {
                    let body_with_extensions =
                        parse_response_body_extensions(frame.params.flags, None, frame.body)
                            .unwrap();
                    let execute = ExecuteV2::deserialize_with_features(
                        &mut &*body_with_extensions.body,
                        &features,
                    )
                    .unwrap();

                    execute.result_metadata_id
                });

            // First request, before reprepare. It received empty id in PREPARED,
            // and was not executed / reprepared, so should send empty id.
            let Some(id) = sent_ids.next().await.unwrap() else {
                panic!("No ID in execute");
            };
            assert!(id.as_ref().is_empty());

            // Second request, after reprepare. Reprepare should update the result metadata.
            let Some(id) = sent_ids.next().await.unwrap() else {
                panic!("Empty id")
            };
            assert!(!id.as_ref().is_empty());

            // Third request (second statement). Statement is in cache, so no reprepare.
            // It received empty id in PREPARED, and was not executed / reprepared, so should send empty id.
            let Some(id) = sent_ids.next().await.unwrap() else {
                panic!("No ID in execute");
            };
            assert!(id.as_ref().is_empty());

            // Fourth request. Second statement again. Now it should have proper metadata.
            let Some(id) = sent_ids.next().await.unwrap() else {
                panic!("Empty id")
            };
            assert!(!id.as_ref().is_empty());

            running_proxy
        },
    )
    .await;

    match res {
        Ok(()) => (),
        Err(ProxyError::Worker(WorkerError::DriverDisconnected(_))) => (),
        Err(err) => panic!("{}", err),
    }
}
