use std::net::SocketAddr;
use std::num::NonZeroUsize;
use std::sync::Arc;

use scylla::client::PoolSize;
use scylla::client::session_builder::SessionBuilder;
use scylla::policies::host_filter::AllowListHostFilter;
use scylla_proxy::{
    Condition, ProxyError, Reaction, RequestOpcode, RequestReaction, RequestRule, ShardAwareness,
    WorkerError,
};
use tokio::sync::mpsc;

use crate::utils::{setup_tracing, test_with_3_node_cluster};

/// A control connection opened to a contact point that the host filter rejects
/// is moved to an accepted node.
///
/// Only control connections send REGISTER, one per connection, so the REGISTER
/// frames received by each node tell where control connections were opened.
#[tokio::test]
async fn control_connection_leaves_contact_point_rejected_by_host_filter() {
    setup_tracing();

    let res = test_with_3_node_cluster(
        ShardAwareness::Unaware,
        async move |proxy_uris, translation_map, mut running_proxy| {
            let mut register_rxs = Vec::new();
            for node in running_proxy.running_nodes.iter_mut() {
                let (tx, rx) = mpsc::unbounded_channel();
                node.prepend_request_rules(vec![RequestRule(
                    Condition::RequestOpcode(RequestOpcode::Register),
                    RequestReaction::noop().with_feedback_when_performed(tx),
                )]);
                register_rxs.push(rx);
            }

            // The host filter sees the addresses that the nodes report, i.e. the
            // real ones, not the proxy ones.
            let accepted_proxies: [SocketAddr; 2] = [
                proxy_uris[1].parse().unwrap(),
                proxy_uris[2].parse().unwrap(),
            ];
            let host_filter = AllowListHostFilter::new(
                translation_map
                    .iter()
                    .filter(|(_real, proxy)| accepted_proxies.contains(proxy))
                    .map(|(real, _proxy)| *real),
            )
            .unwrap();

            // The only contact point is the rejected node.
            let session = SessionBuilder::new()
                .known_node(proxy_uris[0].as_str())
                .address_translator(Arc::new(translation_map.clone()))
                .host_filter(Arc::new(host_filter))
                .fetch_schema_metadata(false)
                .pool_size(PoolSize::PerHost(NonZeroUsize::new(1).unwrap()))
                .build()
                .await
                .unwrap();
            // Answered only after a fetch on an accepted control connection.
            session.refresh_metadata().await.unwrap();

            assert!(register_rxs[0].try_recv().is_ok());
            assert!(
                register_rxs[0].try_recv().is_err(),
                "a control connection was opened to the rejected node again"
            );
            let accepted_registers = register_rxs[1].len() + register_rxs[2].len();
            assert_eq!(
                accepted_registers, 1,
                "the control connection should have moved to exactly one accepted node"
            );

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
