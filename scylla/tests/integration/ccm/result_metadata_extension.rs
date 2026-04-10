use std::net::SocketAddr;
use std::sync::Arc;

use scylla::client::session::Session;
use scylla::client::session_builder::SessionBuilder;
use scylla::value::Row;
use scylla_cql::frame::parse_response_body_extensions;
use scylla_cql::frame::protocol_features::ProtocolFeatures;
use scylla_cql::frame::response::result::Result as CqlResult;
use scylla_cql::frame::response::{Response, Supported};
use scylla_proxy::{
    Condition, Node, Proxy, ProxyError, Reaction, RequestReaction, RequestRule, ResponseReaction,
    ResponseRule, RunningProxy, ShardAwareness, WorkerError,
};

use crate::ccm::lib::cluster::{Cluster, ClusterOptions};
use crate::ccm::lib::{CLUSTER_VERSION, run_ccm_test_with_configuration};
use crate::utils::setup_tracing;

fn cluster_1_node() -> ClusterOptions {
    ClusterOptions {
        name: "result_metadata_extension".to_string(),
        version: CLUSTER_VERSION.clone(),
        nodes_per_dc: vec![1],
        ..ClusterOptions::default()
    }
}

/// Sets up a proxy in front of a single CCM node and connects a session through it.
/// Returns the session, running proxy, and negotiated ProtocolFeatures.
async fn setup_proxy_and_session(cluster: &Cluster) -> (Session, RunningProxy, ProtocolFeatures) {
    let node = cluster.nodes().iter().next().expect("No nodes in cluster");
    let real_addr = SocketAddr::new(node.broadcast_rpc_address(), node.native_transport_port());
    let proxy_addr = SocketAddr::new(scylla_proxy::get_exclusive_local_address(), 9042);

    let proxy = Proxy::new([Node::builder()
        .real_address(real_addr)
        .proxy_address(proxy_addr)
        .shard_awareness(ShardAwareness::QueryNode)
        .build()]);

    let translation_map = proxy.translation_map();
    let mut running_proxy = proxy.run().await.unwrap();

    // Capture SUPPORTED response to determine ProtocolFeatures.
    let (tx_supported, mut rx_supported) = tokio::sync::mpsc::unbounded_channel();
    running_proxy.running_nodes[0].prepend_response_rules(vec![ResponseRule(
        Condition::ResponseOpcode(scylla_proxy::ResponseOpcode::Supported),
        ResponseReaction::noop().with_feedback_when_performed(tx_supported),
    )]);

    let session = SessionBuilder::new()
        .known_node(format!("{}", proxy_addr))
        .address_translator(Arc::new(translation_map))
        .user("cassandra", "cassandra")
        .build()
        .await
        .unwrap();

    let supported_frame = rx_supported.recv().await.unwrap().0;
    let supported = Supported::deserialize(&mut &*supported_frame.body)
        .unwrap()
        .options;
    let features = ProtocolFeatures::parse_from_supported(&supported);

    // Clear the SUPPORTED capture rule, no longer needed.
    running_proxy.running_nodes[0].change_response_rules(Some(vec![]));

    (session, running_proxy, features)
}

/// Helper: receive the next Result::Rows response from the proxy feedback channel,
/// skipping any non-Rows Result responses (e.g. Result::Prepared).
/// Returns the RawMetadataAndRawRows for inspection.
async fn recv_next_rows_response(
    rx: &mut tokio::sync::mpsc::UnboundedReceiver<(scylla_proxy::ResponseFrame, Option<u16>)>,
    features: &ProtocolFeatures,
) -> scylla_cql::frame::response::result::RawMetadataAndRawRows {
    loop {
        let (frame, _) = rx
            .recv()
            .await
            .expect("Response channel closed unexpectedly");
        let body_with_extensions =
            parse_response_body_extensions(frame.params.flags, None, frame.body).unwrap();
        let response =
            Response::deserialize(features, frame.opcode, body_with_extensions.body, None).unwrap();
        match response {
            Response::Result(CqlResult::Rows(raw_rows)) => return raw_rows.0,
            Response::Result(_) => {
                // Skip non-Rows results (e.g. Prepared from auto-reprepare).
                continue;
            }
            other => panic!("Unexpected response type: {other:?}"),
        }
    }
}

async fn test_special_query_behavior(
    session: &Session,
    running_proxy: &mut RunningProxy,
    features: &ProtocolFeatures,
    query_str: &str,
) {
    // --- Part A: Verify deserialization for special statements ---
    tracing::info!("Testing deserialization for: {}", query_str);
    let prepared = session.prepare(query_str).await.unwrap();
    for i in 0..3 {
        tracing::info!("  Executing iteration {}", i);
        let result = session.execute_unpaged(&prepared, &()).await.unwrap();
        let rows_result = result.into_rows_result().unwrap();
        assert!(
            rows_result.rows_num() > 0,
            "Expected rows for '{}', iteration {}",
            query_str,
            i
        );
        // Verify we can actually iterate/deserialize the rows.
        let count = rows_result
            .rows::<Row>()
            .unwrap()
            .map(|row| row.unwrap())
            // Both statements return multiple columns, let's make sure
            // we are not seeing the results as basically empty.
            .inspect(|row| assert!(row.columns.len() > 1))
            .count();
        assert_eq!(count, rows_result.rows_num());
    }

    // --- Part B: Protocol-level check for result metadata ---
    tracing::info!("Protocol-level check for result metadata");
    let prepared = session.prepare(query_str).await.unwrap();
    let has_real_metadata_in_prepared = prepared.get_current_result_set_col_specs().get().len() > 0;

    // Set up response feedback channel.
    let (tx_result, mut rx_result) = tokio::sync::mpsc::unbounded_channel();
    running_proxy.running_nodes[0].prepend_response_rules(vec![ResponseRule(
        Condition::not(Condition::ConnectionRegisteredAnyEvent).and(Condition::ResponseOpcode(
            scylla_proxy::ResponseOpcode::Result,
        )),
        ResponseReaction::noop().with_feedback_when_performed(tx_result.clone()),
    )]);

    // Execute #1: First execution after PREPARE.
    // If PREPARED had NO_METADATA, driver should send skip_metadata=false,
    // and receive real metadata here.
    // If PREPARED had real metadata, there should be no need to receive it here.
    tracing::info!("Execute #1 (expect metadata)");
    let result = session.execute_unpaged(&prepared, &()).await.unwrap();
    assert!(result.into_rows_result().unwrap().rows_num() > 0);
    {
        let raw_rows = recv_next_rows_response(&mut rx_result, features).await;
        assert_eq!(
            raw_rows.no_metadata(),
            has_real_metadata_in_prepared,
            "First execution should have no_metadata=true iff PREPARE returned real metadata (col_count > 0)"
        );
    }

    // Execute #2: Now the driver has surely cached result metadata (col_count > 0),
    // so it sends skip_metadata=true. Server should respond with no_metadata.
    // Let's try this multiple times to be sure
    tracing::info!("Execute #2 (expect no_metadata)");
    for _ in 0..3 {
        let result = session.execute_unpaged(&prepared, &()).await.unwrap();
        assert!(result.into_rows_result().unwrap().rows_num() > 0);
        {
            let raw_rows = recv_next_rows_response(&mut rx_result, features).await;
            assert!(
                raw_rows.no_metadata(),
                "Second execution should have no_metadata=true"
            );
            assert!(
                !raw_rows.metadata_changed(),
                "Second execution should have metadata_changed=false"
            );
        }
    }

    // --- Part C: After injected Unprepared error ---
    // Inject Unprepared for the next EXECUTE only (TrueForLimitedTimes(1)).
    // The driver should auto-reprepare and retry. Because the driver preserves
    // cached result metadata across reprepare, the retry should still have
    // skip_metadata=true, and the response should have no_metadata=true.
    tracing::info!("Injecting Unprepared error for next EXECUTE");
    running_proxy.running_nodes[0].prepend_request_rules(vec![RequestRule(
        Condition::not(Condition::ConnectionRegisteredAnyEvent)
            .and(Condition::RequestOpcode(
                scylla_proxy::RequestOpcode::Execute,
            ))
            .and(Condition::TrueForLimitedTimes(1)),
        RequestReaction::forge().unprepared(),
    )]);

    // Execute #4: triggers Unprepared → auto-reprepare → retry EXECUTE.
    // The retry's response should have no_metadata=true because the driver
    // preserved the cached result metadata from previous executions.
    tracing::info!("Execute #4 (after Unprepared, expect no_metadata on retry)");
    for _ in 0..3 {
        let result = session.execute_unpaged(&prepared, &()).await.unwrap();
        assert!(result.into_rows_result().unwrap().rows_num() > 0);
        {
            let raw_rows = recv_next_rows_response(&mut rx_result, features).await;
            assert!(
                raw_rows.no_metadata(),
                "Execution after auto-reprepare should have no_metadata=true \
                 (driver preserves cached result metadata)"
            );
            assert!(
                !raw_rows.metadata_changed(),
                "Execution after auto-reprepare should have metadata_changed=false"
            );
        }
    }
}

// Verify that special statements (LIST ROLES, DESCRIBE KEYSPACE) work correctly
// as prepared statements with the result metadata extension:
// 1. They can be prepared, executed multiple times, and results deserialized.
// 2. After the first execution, subsequent executions skip result metadata
//    (no_metadata=true, metadata_changed=false).
// 3. After an injected Unprepared error triggers auto-reprepare, the driver
//    preserves cached result metadata, so subsequent executions still skip metadata.
#[tokio::test]
async fn test_result_metadata_extension() {
    setup_tracing();
    fn process_proxy_result(res: Result<(), ProxyError>) {
        match res {
            Ok(()) => (),
            Err(ProxyError::Worker(WorkerError::DriverDisconnected(_))) => (),
            Err(err) => panic!("{}", err),
        }
    }
    async fn test(cluster: &mut Cluster) {
        let (session, mut running_proxy, features) = setup_proxy_and_session(cluster).await;

        if !features.scylla_metadata_id_supported {
            tracing::info!("Metadata ID extension not supported, skipping test");
            process_proxy_result(running_proxy.finish().await);
            return;
        }

        for query_str in ["LIST ROLES of cassandra", "DESCRIBE KEYSPACE system"] {
            test_special_query_behavior(&session, &mut running_proxy, &features, query_str).await
        }

        process_proxy_result(running_proxy.finish().await);
    }

    run_ccm_test_with_configuration(
        cluster_1_node,
        |mut cluster: Cluster| async move {
            cluster
                .enable_password_authentication()
                .await
                .expect("Failed to enable password authenticator");
            cluster
                .updateconf([("authorizer", "CassandraAuthorizer")])
                .await
                .expect("Failed to enable CassandraAuthorizer");
            cluster
        },
        test,
    )
    .await;
}
