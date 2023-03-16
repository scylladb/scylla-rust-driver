use crate::utils::{setup_tracing, test_with_3_node_cluster};
use scylla::{LegacySession, SessionBuilder};
use scylla_cql::frame::request::options;
use scylla_cql::frame::types;
use std::sync::Arc;
use tokio::sync::mpsc;

use scylla::transport::SelfIdentity;
use scylla_proxy::{
    Condition, ProxyError, Reaction, RequestOpcode, RequestReaction, RequestRule, ShardAwareness,
    WorkerError,
};

#[tokio::test]
#[ntest::timeout(20000)]
#[cfg(not(scylla_cloud_tests))]
async fn self_identity_is_set_properly_in_startup_message() {
    setup_tracing();

    let application_name = "test_self_identity";
    let application_version = "42.2137.0";
    let client_id = "blue18";
    let custom_driver_name = "ScyllaDB Rust Driver - test run";
    let custom_driver_version = "2137.42.0";

    let default_self_identity = SelfIdentity::new();

    let full_self_identity = SelfIdentity::new()
        .with_application_name(application_name)
        .with_application_version(application_version)
        .with_client_id(client_id)
        .with_custom_driver_name(custom_driver_name)
        .with_custom_driver_version(custom_driver_version);

    test_given_self_identity(default_self_identity).await;
    test_given_self_identity(full_self_identity).await;
}

async fn test_given_self_identity(self_identity: SelfIdentity<'static>) {
    let res = test_with_3_node_cluster(
        ShardAwareness::QueryNode,
        |proxy_uris, translation_map, mut running_proxy| async move {
            // We set up proxy, so that it informs us (via startup_rx) about driver's Startup message contents.

            let (startup_tx, mut startup_rx) = mpsc::unbounded_channel();

            running_proxy.running_nodes[0].change_request_rules(Some(vec![RequestRule(
                Condition::RequestOpcode(RequestOpcode::Startup),
                RequestReaction::noop().with_feedback_when_performed(startup_tx),
            )]));

            // DB preparation phase
            let _session: LegacySession = SessionBuilder::new()
                .known_node(proxy_uris[0].as_str())
                .address_translator(Arc::new(translation_map))
                .custom_identity(self_identity.clone())
                .build_legacy()
                .await
                .unwrap();

            let (startup_frame, _shard) = startup_rx.recv().await.unwrap();
            let startup_options = types::read_string_map(&mut &*startup_frame.body).unwrap();

            for (option_key, facultative_option) in [
                (
                    options::APPLICATION_NAME,
                    self_identity.get_application_name(),
                ),
                (
                    options::APPLICATION_VERSION,
                    self_identity.get_application_version(),
                ),
                (options::CLIENT_ID, self_identity.get_client_id()),
            ] {
                assert_eq!(
                    startup_options.get(option_key).map(String::as_str),
                    facultative_option
                );
            }

            for (option_key, default_mandatory_option, custom_mandatory_option) in [
                (
                    options::DRIVER_NAME,
                    options::DEFAULT_DRIVER_NAME,
                    self_identity.get_custom_driver_name(),
                ),
                (
                    options::DRIVER_VERSION,
                    options::DEFAULT_DRIVER_VERSION,
                    self_identity.get_custom_driver_version(),
                ),
            ] {
                assert_eq!(
                    startup_options.get(option_key).map(String::as_str),
                    Some(custom_mandatory_option.unwrap_or(default_mandatory_option))
                );
            }

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
