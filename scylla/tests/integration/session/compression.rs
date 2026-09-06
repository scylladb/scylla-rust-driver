//! Tests of frame compression: that the driver actually compresses the frames
//! it sends, and that requests and responses survive the round trip intact
//! with each of the supported algorithms.
//!
//! The frames are counted by a proxy sitting between the driver and the
//! cluster. The proxy hands out *decompressed* bodies, so the measurement
//! relies on [`RequestFrame::wire_body_len`](scylla_proxy::RequestFrame),
//! which records how many body bytes actually travelled over the network.
//!
//! All the cases live in one `#[tokio::test]`, sharing a single proxy and a
//! single keyspace, so that the shared cluster is not burdened with a keyspace
//! per case. Each case does need a session of its own, though - compression is
//! negotiated once, upon connecting.

use scylla::client::session::Session;
use scylla::client::session_builder::SessionBuilder;
use scylla::frame::Compression;
use scylla::statement::unprepared::Statement;

use scylla_proxy::{
    Condition, ProxyError, Reaction, RequestOpcode, RequestReaction, RequestRule, ShardAwareness,
    WorkerError,
};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::ops::Range;
use std::sync::Arc;
use tokio::sync::mpsc;

use crate::utils::{PerformDDL, setup_tracing, test_with_3_node_cluster, unique_keyspace_name};

/// A single compression setting to exercise, together with what it is expected
/// to do to the size of the frames sent.
struct Case {
    /// Compression to configure the session with, `None` for no compression.
    compression: Option<Compression>,
    /// Length of the (highly compressible) text inserted and read back.
    text_size: usize,
    /// Expected total size of the bodies of the request frames, as sent over
    /// the network.
    expected_wire_size: Range<usize>,
}

const CASES: &[Case] = &[
    Case {
        compression: None,
        text_size: 1_000,
        expected_wire_size: 1_050..1_300,
    },
    Case {
        compression: None,
        text_size: 1_000_000,
        expected_wire_size: 1_000_000..1_005_000,
    },
    Case {
        compression: Some(Compression::Snappy),
        text_size: 1_000,
        expected_wire_size: 100..400,
    },
    Case {
        compression: Some(Compression::Snappy),
        text_size: 1_000_000,
        expected_wire_size: 40_000..55_000,
    },
    Case {
        compression: Some(Compression::Lz4),
        text_size: 1_000,
        expected_wire_size: 100..400,
    },
    Case {
        compression: Some(Compression::Lz4),
        text_size: 1_000_000,
        expected_wire_size: 3_500..8_000,
    },
];

async fn connect(
    proxy_uris: &[String; 3],
    translation_map: &HashMap<SocketAddr, SocketAddr>,
    compression: Option<Compression>,
) -> Session {
    SessionBuilder::new()
        .known_node(proxy_uris[0].as_str())
        .address_translator(Arc::new(translation_map.clone()))
        .compression(compression)
        .build()
        .await
        .unwrap()
}

#[tokio::test]
async fn compression_shrinks_frames_and_preserves_data() {
    setup_tracing();

    let res = test_with_3_node_cluster(
        ShardAwareness::QueryNode,
        |proxy_uris, translation_map, mut running_proxy| async move {
            // Report every request the driver makes, except the ones on the
            // control connection - those are unrelated to the cases below and
            // would pollute the counts.
            let (request_tx, mut request_rx) = mpsc::unbounded_channel();
            for running_node in running_proxy.running_nodes.iter_mut() {
                running_node.change_request_rules(Some(vec![RequestRule(
                    Condition::or(
                        Condition::RequestOpcode(RequestOpcode::Query),
                        Condition::RequestOpcode(RequestOpcode::Execute),
                    )
                    .and(Condition::not(Condition::ConnectionRegisteredAnyEvent)),
                    RequestReaction::noop().with_feedback_when_performed(request_tx.clone()),
                )]));
            }

            // A session of its own for the schema, so that the DDL frames are
            // not counted against any of the cases.
            let setup_session = connect(&proxy_uris, &translation_map, None).await;
            let ks = unique_keyspace_name();
            setup_session.ddl(format!("CREATE KEYSPACE {ks} WITH REPLICATION = {{'class' : 'NetworkTopologyStrategy', 'replication_factor' : 3}}")).await.unwrap();
            setup_session
                .ddl(format!(
                    "CREATE TABLE {ks}.t (k text PRIMARY KEY, t text, i int, f float)"
                ))
                .await
                .unwrap();

            for (
                case_no,
                Case {
                    compression,
                    text_size,
                    expected_wire_size,
                },
            ) in CASES.iter().enumerate()
            {
                // Discard whatever the previous case (or the schema setup) left behind.
                while request_rx.try_recv().is_ok() {}

                let session = connect(&proxy_uris, &translation_map, *compression).await;

                // A partition of its own, so that the cases do not overwrite
                // each other's rows.
                let key = format!("key{case_no}");
                let text = "a".repeat(*text_size);

                session
                    .query_unpaged(
                        Statement::from(format!(
                            "INSERT INTO {ks}.t (k, t, i, f) VALUES (?, ?, ?, ?)"
                        )),
                        (&key, text.as_str(), 42_i32, 24.03_f32),
                    )
                    .await
                    .unwrap();

                let row = session
                    .query_unpaged(
                        Statement::from(format!("SELECT k, t, i, f FROM {ks}.t WHERE k = ?")),
                        (&key,),
                    )
                    .await
                    .unwrap()
                    .into_rows_result()
                    .unwrap()
                    .single_row::<(String, String, i32, f32)>()
                    .unwrap();
                assert_eq!(row, (key, text, 42_i32, 24.03_f32));

                // The proxy hands out decompressed bodies, so `body.len()` would
                // be the same no matter the compression; `wire_body_len` is what
                // actually travelled.
                let mut total_wire_size = 0;
                while let Ok((request_frame, _shard)) = request_rx.try_recv() {
                    assert_eq!(
                        request_frame.params.is_compressed(),
                        compression.is_some(),
                        "Case {case_no} ({compression:?}): frame compression flag does not match \
                         the configured compression"
                    );
                    total_wire_size += request_frame.wire_body_len;
                }
                assert!(
                    expected_wire_size.contains(&total_wire_size),
                    "Case {case_no} ({compression:?}, {text_size} B of text): total wire size \
                     {total_wire_size} not in expected range {expected_wire_size:?}"
                );
            }

            setup_session
                .ddl(format!("DROP KEYSPACE {ks}"))
                .await
                .unwrap();

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
