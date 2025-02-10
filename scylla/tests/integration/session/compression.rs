use crate::utils::{PerformDDL, test_with_3_node_cluster, unique_keyspace_name};
use scylla::client::session::Session;
use scylla::client::session_builder::SessionBuilder;
use scylla::frame::Compression;
use scylla::statement::unprepared::Statement;

use scylla_proxy::{
    Condition, ProxyError, Reaction, RequestOpcode, RequestReaction, RequestRule, ShardAwareness,
    WorkerError,
};
use std::sync::Arc;
use tokio::sync::mpsc;

/// Tests the compression functionality of the Scylla driver by performing a series of operations
/// on a 3-node cluster with optional compression and verifying the total frame size of the requests.
///
/// # Arguments
///
/// * `compression` - An optional `Compression` enum value specifying the type of compression to use.
/// * `text_size` - The size of the text to be inserted into the test table.
/// * `expected_frame_total_size_range` - A range specifying the expected total size of the frames.
///
/// # Panics
///
/// This function will panic if the total frame size does not fall within the expected range or if
/// any of the operations (such as creating keyspace, table, or inserting/querying data) fail.
async fn test_compression(
    compression: Option<Compression>,
    text_size: usize,
    expected_frame_total_size_range: std::ops::Range<usize>,
) {
    let res = test_with_3_node_cluster(ShardAwareness::QueryNode, |proxy_uris, translation_map, mut running_proxy| async move {

        let request_rule = |tx| {
            RequestRule(
                    Condition::or(Condition::RequestOpcode(RequestOpcode::Query),
                        Condition::RequestOpcode(RequestOpcode::Execute)).and(
                    Condition::not(Condition::ConnectionRegisteredAnyEvent)),
                RequestReaction::noop().with_feedback_when_performed(tx),
            )
        };

        let (request_tx, mut request_rx) = mpsc::unbounded_channel();
        for running_node in running_proxy.running_nodes.iter_mut() {
            running_node.change_request_rules(Some(vec![request_rule(request_tx.clone())]));
        }

        let session: Session = SessionBuilder::new()
            .known_node(proxy_uris[0].as_str())
            .address_translator(Arc::new(translation_map))
            .compression(compression)
            .build()
            .await
            .unwrap();

        let ks = unique_keyspace_name();
        session.ddl(format!("CREATE KEYSPACE IF NOT EXISTS {} WITH REPLICATION = {{'class' : 'NetworkTopologyStrategy', 'replication_factor' : 3}}", ks)).await.unwrap();
        session.use_keyspace(ks, false).await.unwrap();
        session
            .ddl("CREATE TABLE test (k text PRIMARY KEY, t text, i int, f float)")
            .await
            .unwrap();

        let q = Statement::from("INSERT INTO test (k, t, i, f) VALUES (?, ?, ?, ?)");
        let large_string = "a".repeat(text_size);
        session.query_unpaged(q, ("key", large_string.as_str(), 42_i32, 24.03_f32)).await.unwrap();

        let result: Vec<(String, String, i32, f32)> = session
            .query_unpaged("SELECT k, t, i, f FROM test WHERE k = 'key'", &[])
            .await
            .unwrap()
            .into_rows_result()
            .unwrap()
            .rows::<(String, String, i32, f32)>()
            .unwrap()
            .collect::<Result<_, _>>()
            .unwrap();

        assert_eq!(result, vec![(String::from("key"), large_string, 42_i32, 24.03_f32)]);


        // The proxy hands out decompressed bodies, so `body.len()` would be the same
        // no matter the compression. `wire_body_len` is what actually travelled.
        let mut total_frame_size = 0;
        while let Ok((request_frame, _shard)) = request_rx.try_recv() {
            assert_eq!(
                request_frame.params.is_compressed(),
                compression.is_some(),
                "Frame compression flag does not match the configured compression ({compression:?})"
            );
            total_frame_size += request_frame.wire_body_len;
        }
        assert!(
            expected_frame_total_size_range.contains(&total_frame_size),
            "Total frame size {total_frame_size} not in expected range {expected_frame_total_size_range:?}"
        );

        running_proxy

    }).await;

    match res {
        Ok(()) => (),
        Err(ProxyError::Worker(WorkerError::DriverDisconnected(_))) => (),
        Err(err) => panic!("{}", err),
    }
}

#[tokio::test]
async fn should_execute_queries_without_compression() {
    test_compression(None, 1_000, 1_800..2_500).await;
}

#[tokio::test]
async fn should_execute_queries_without_compression_1mb() {
    test_compression(None, 1_000_000, 1_000_000..1_010_000).await;
}

#[tokio::test]
async fn should_execute_queries_with_snappy_compression() {
    test_compression(Some(Compression::Snappy), 1_000, 1_000..1_500).await;
}

#[tokio::test]
async fn should_execute_queries_with_snappy_compression_1mb() {
    test_compression(Some(Compression::Snappy), 1_000_000, 40_000..55_000).await;
}

#[tokio::test]
async fn should_execute_queries_with_lz4_compression() {
    test_compression(Some(Compression::Lz4), 1_000, 1_000..1_500).await;
}

#[tokio::test]
async fn should_execute_queries_with_lz4_compression_1mb() {
    test_compression(Some(Compression::Lz4), 1_000_000, 4_000..10_000).await;
}
