use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;

use scylla::client::session::Session;
use scylla::client::session_builder::SessionBuilder;
use scylla::statement::Statement;
use scylla_proxy::{
    Condition, ProxyError, Reaction, RequestFrame, RequestOpcode, RequestReaction, RequestRule,
    RunningProxy, ShardAwareness, WorkerError,
};
use tokio::sync::mpsc;
use tracing::info;

use crate::utils::{setup_tracing, test_with_3_node_cluster};

#[tokio::test]
#[ntest::timeout(10000)]
async fn test_no_unprepared_internal_requests() {
    setup_tracing();

    let (feedback_tx, mut feedback_rx) = mpsc::unbounded_channel::<(RequestFrame, Option<u16>)>();

    let catch_query_rule = RequestRule(
        Condition::RequestOpcode(RequestOpcode::Query),
        RequestReaction::noop().with_feedback_when_performed(feedback_tx),
    );

    let test_fut = |proxy_uris: [String; 3],
                    translation_map: HashMap<SocketAddr, SocketAddr>,
                    mut running_proxy: RunningProxy| async move {
        // Add the rule to all proxy nodes to catch unprepared Query requests.
        for node in &mut running_proxy.running_nodes {
            node.change_request_rules(Some(vec![catch_query_rule.clone()]));
        }

        // Helper to check if any Query requests were issued.
        // We allow "USE " queries because they are intended to be unprepared and are not preparable in CQL.
        let check_no_unprepared_queries =
            |section: &str,
             feedback_rx: &mut mpsc::UnboundedReceiver<(RequestFrame, Option<u16>)>| {
                while let Ok((frame, _)) = feedback_rx.try_recv() {
                    let body_str = String::from_utf8_lossy(&frame.body);
                    if !body_str.starts_with("USE ") {
                        panic!(
                            "Section {}: Forbidden unprepared query detected: {}",
                            section, body_str
                        );
                    }
                }
            };

        info!("Starting section: session creation and metadata refresh");
        let session: Session = SessionBuilder::new()
            .known_node(proxy_uris[0].as_str())
            .address_translator(Arc::new(translation_map))
            .build()
            .await
            .unwrap();

        check_no_unprepared_queries("session connect", &mut feedback_rx);

        info!("Starting section: explicit metadata refresh");
        session.refresh_metadata().await.unwrap();
        check_no_unprepared_queries("metadata refresh", &mut feedback_rx);

        info!("Starting section: schema agreement");
        session.await_schema_agreement().await.unwrap();
        check_no_unprepared_queries("schema agreement", &mut feedback_rx);

        info!("Starting section: tracing info fetch");
        // Issue a query with tracing enabled.
        let stmt = Statement::new("SELECT * FROM system.local WHERE key = 'local'");
        let mut prepared = session.prepare(stmt).await.unwrap();
        prepared.set_tracing(true);

        let res = session.execute_unpaged(&prepared, ()).await.unwrap();
        let tracing_id = res.tracing_id().expect("Tracing ID should be present");

        // Clear feedback channel to ensure we only check queries issued by get_tracing_info.
        while feedback_rx.try_recv().is_ok() {}

        session.get_tracing_info(&tracing_id).await.unwrap();
        check_no_unprepared_queries("tracing info fetch", &mut feedback_rx);

        running_proxy
    };

    let res = test_with_3_node_cluster(ShardAwareness::QueryNode, test_fut).await;
    match res {
        Ok(()) => (),
        Err(ProxyError::Worker(WorkerError::DriverDisconnected(_))) => (),
        Err(err) => panic!("{}", err),
    }
}
