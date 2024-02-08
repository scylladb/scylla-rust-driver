use crate::utils::test_with_3_node_cluster;
use scylla::transport::session::Session;
use scylla::SessionBuilder;
use scylla::{prepared_statement::PreparedStatement, test_utils::unique_keyspace_name};
use scylla_cql::frame::types;
use scylla_proxy::{
    Condition, ProxyError, Reaction, ResponseFrame, ResponseReaction, ShardAwareness, TargetShard,
    WorkerError,
};
use std::sync::Arc;

#[tokio::test]
#[ntest::timeout(20000)]
#[cfg(not(scylla_cloud_tests))]
async fn test_skip_result_metadata() {
    use scylla_proxy::{ResponseOpcode, ResponseRule};

    const NO_METADATA_FLAG: i32 = 0x0004;

    let res = test_with_3_node_cluster(ShardAwareness::QueryNode, |proxy_uris, translation_map, mut running_proxy| async move {
        // DB preparation phase
        let session: Session = SessionBuilder::new()
            .known_node(proxy_uris[0].as_str())
            .address_translator(Arc::new(translation_map))
            .build()
            .await
            .unwrap();

        let ks = unique_keyspace_name();
        session.query(format!("CREATE KEYSPACE IF NOT EXISTS {} WITH REPLICATION = {{'class' : 'NetworkTopologyStrategy', 'replication_factor' : 3}}", ks), &[]).await.unwrap();
        session.use_keyspace(ks, false).await.unwrap();
        session
            .query("CREATE TABLE t (a int primary key, b int, c text)", &[])
            .await
            .unwrap();
        session.query("INSERT INTO t (a, b, c) VALUES (1, 2, 'foo_filter_data')", &[]).await.unwrap();

        let mut prepared = session.prepare("SELECT a, b, c FROM t").await.unwrap();

        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();

        // We inserted this string to filter responses
        let body_rows = b"foo_filter_data";
        for node in running_proxy.running_nodes.iter_mut() {
            let rule = ResponseRule(
                Condition::ResponseOpcode(ResponseOpcode::Result).and(Condition::BodyContainsCaseSensitive(Box::new(*body_rows))),
                ResponseReaction::noop().with_feedback_when_performed(tx.clone())
            );
            node.change_response_rules(Some(vec![rule]));
        }

        async fn test_with_flags_predicate(
            session: &Session,
            prepared: &PreparedStatement,
            rx: &mut tokio::sync::mpsc::UnboundedReceiver<(ResponseFrame, Option<TargetShard>)>,
            predicate: impl FnOnce(i32) -> bool
        ) {
            session.execute(prepared, &[]).await.unwrap();

            let (frame, _shard) = rx.recv().await.unwrap();
            let mut buf = &*frame.body;

            // FIXME: make use of scylla_cql::frame utilities, instead of deserializing frame manually.
            // This will probably be possible once https://github.com/scylladb/scylla-rust-driver/issues/462 is fixed.
            match types::read_int(&mut buf).unwrap() {
                0x0002 => (),
                _ => panic!("Invalid result type"),
            }
            let result_metadata_flags = types::read_int(&mut buf).unwrap();
            assert!(predicate(result_metadata_flags));
        }

        // Verify that server sends metadata when driver doesn't send SKIP_METADATA flag.
        prepared.set_use_cached_result_metadata(false);
        test_with_flags_predicate(&session, &prepared, &mut rx, |flags| flags & NO_METADATA_FLAG == 0).await;

        // Verify that server doesn't send metadata when driver sends SKIP_METADATA flag.
        prepared.set_use_cached_result_metadata(true);
        test_with_flags_predicate(&session, &prepared, &mut rx, |flags| flags & NO_METADATA_FLAG != 0).await;

        running_proxy
    }).await;

    match res {
        Ok(()) => (),
        Err(ProxyError::Worker(WorkerError::DriverDisconnected(_))) => (),
        Err(err) => panic!("{}", err),
    }
}
