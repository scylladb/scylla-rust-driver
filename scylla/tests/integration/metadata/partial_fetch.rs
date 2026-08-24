//! Tests of partial metadata fetches: a server event announcing a change of one
//! metadata aspect must make the driver re-read that aspect alone, instead of
//! performing a full metadata fetch.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use scylla::client::session::Session;
use scylla::client::session_builder::SessionBuilder;
use scylla::cluster::ClusterState;
use scylla::cluster::metadata::PeriodicFetchMode;
use scylla_proxy::{
    Condition, ProxyError, Reaction, RequestFrame, RequestOpcode, RequestReaction, RequestRule,
    ResponseOpcode, ResponseReaction, ResponseRule, RunningProxy, ShardAwareness, WorkerError,
};
use tokio::sync::mpsc;
use tracing::info;

use crate::utils::{
    PerformDDL as _, inject_keyspace_drop_event, inject_status_change_down,
    inject_status_change_up, inject_topology_change_new_node, setup_tracing,
    test_with_3_node_cluster, unique_keyspace_name, wait_until_all_nodes_are_connected,
};

/// The requests a partial topology fetch issues: one for `system.peers` and one
/// for `system.local`. A full fetch would add at least one per schema table
/// (`system_schema.keyspaces`, `.tables`, `.columns`, ...).
const TOPOLOGY_FETCH_REQUESTS: usize = 2;

/// The requests a partial schema fetch issues at the full schema detail level:
/// one per schema table read for the affected keyspaces -
/// `system_schema.keyspaces`, `.types`, `.columns`, `.tables`, `.views`,
/// `.scylla_tables` (partitioners) and `.scylla_keyspaces` (tablet
/// information). A full fetch would add `system.peers` and `system.local`.
/// Last 2 tables are not present on Cassandra, so they won't be queried.
const SCHEMA_FETCH_REQUESTS: usize = if cfg!(cassandra_tests) { 5 } else { 7 };

/// How long [`full_metadata_mode_picks_up_schema_changes_without_events`] waits
/// for the periodic full fetch to publish the new keyspace: many
/// [`SCHEMA_REFRESH_INTERVAL`]s, so that only a fetch that never happens times
/// out.
const FETCH_WAIT_TIMEOUT: Duration = Duration::from_secs(5);

/// Short enough for the periodic tick that resolves the accumulated schema
/// changes to come quickly, so that the tests need not wait a minute for it.
const SCHEMA_REFRESH_INTERVAL: Duration = Duration::from_millis(100);

/// The proxy's feedback channel for the counted metadata requests.
type MetadataRequestFeedback = mpsc::UnboundedReceiver<(RequestFrame, Option<u16>)>;

/// A TOPOLOGY_CHANGE event can only have changed the peer list, so the driver
/// must react with a partial topology fetch: re-read `system.peers` and
/// `system.local`, and nothing else.
#[tokio::test]
async fn topology_change_event_triggers_only_a_topology_fetch() {
    assert_event_triggers_only_a_topology_fetch(inject_topology_change_new_node).await;
}

/// A STATUS_CHANGE UP event must trigger a partial topology fetch too.
///
/// A node's `rpc_address` can change while the node is neither added nor
/// removed - a restart under a new address, for instance - and no
/// TOPOLOGY_CHANGE announces that: the only events it produces are DOWN and UP.
/// Re-reading the peers on status changes is what keeps the driver from
/// addressing such a node by its stale address.
#[tokio::test]
async fn status_change_up_event_triggers_only_a_topology_fetch() {
    assert_event_triggers_only_a_topology_fetch(inject_status_change_up).await;
}

/// A STATUS_CHANGE DOWN event must trigger a partial topology fetch too, for
/// the reason given in [`status_change_up_event_triggers_only_a_topology_fetch`].
#[tokio::test]
async fn status_change_down_event_triggers_only_a_topology_fetch() {
    assert_event_triggers_only_a_topology_fetch(inject_status_change_down).await;
}

/// Injects one event with `inject_event` and requires the driver to react with
/// exactly one partial topology fetch: two metadata requests on the control
/// connection, and a new `ClusterState` that kept the schema metadata of the
/// previous one (the event cannot have affected the schema, and the fetch does
/// not re-read it).
async fn assert_event_triggers_only_a_topology_fetch(
    inject_event: impl FnOnce(&RunningProxy, SocketAddr),
) {
    setup_tracing();

    let res = test_with_3_node_cluster(
        ShardAwareness::QueryNode,
        |proxy_uris, translation_map, mut running_proxy| async move {
            let session = SessionBuilder::new()
                .known_node(proxy_uris[0].as_str())
                .address_translator(Arc::new(translation_map.clone()))
                // Far beyond the test duration: the only fetch on the
                // established control connection is the one the injected event
                // triggers.
                .cluster_metadata_refresh_interval(Duration::from_secs(10_000))
                .build()
                .await
                .unwrap();
            wait_until_all_nodes_are_connected(3, &session).await;

            let state_before_event = session.get_cluster_state();

            let mut metadata_request_rx = count_metadata_requests(&mut running_proxy);

            // The address named by the event does not matter for the fetch: the
            // driver reacts by re-reading the whole peer list anyway. An address
            // of no known node also keeps the status hints from provoking
            // anything else (a keepalive or a pool refill).
            inject_event(&running_proxy, SocketAddr::from(([127, 0, 0, 1], 9042)));

            // The fetched topology is published as a new `ClusterState`, which
            // must have kept the schema metadata of the previous one.
            let state_after_event =
                wait_for_state(&session, |state| !Arc::ptr_eq(state, &state_before_event)).await;

            let requests = drain_count(&mut metadata_request_rx);
            assert_eq!(
                requests, TOPOLOGY_FETCH_REQUESTS,
                "the control connection issued {requests} metadata requests, so the event \
                triggered something other than a partial topology fetch"
            );

            assert_eq!(state_after_event.get_nodes_info().len(), 3);

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

/// A SCHEMA_CHANGE event names the keyspace it concerns, so the driver must
/// react by re-reading that keyspace's schema alone - not the whole metadata,
/// as it used to on every periodic refresh.
/// On keyspace drop, no queries should be issued.
#[tokio::test]
async fn schema_change_event_triggers_only_a_schema_fetch() {
    setup_tracing();

    let res = test_with_3_node_cluster(
        ShardAwareness::QueryNode,
        |proxy_uris, translation_map, mut running_proxy| async move {
            let keyspace = unique_keyspace_name();
            let session = new_schema_watching_session(&proxy_uris, &translation_map, &keyspace)
                .build()
                .await
                .unwrap();

            info!("Part 1: Creating keyspace");

            let mut metadata_request_rx = count_metadata_requests(&mut running_proxy);

            let state_before_event = session.get_cluster_state();
            assert!(state_before_event.get_keyspace(&keyspace).is_none());

            // First schema change, creating the keyspace.
            create_keyspace(&session, &keyspace).await;

            let _state_after_event_1 =
                wait_for_state(&session, |state| state.get_keyspace(&keyspace).is_some()).await;

            let requests = drain_count(&mut metadata_request_rx);
            assert_eq!(
                requests, SCHEMA_FETCH_REQUESTS,
                "the control connection issued {requests} metadata requests, so the event \
                triggered something other than a partial schema fetch"
            );

            info!("Part 2: Creating table");

            // Another schema change, modifying the keyspace.
            session
                .ddl(format!("CREATE TABLE {keyspace}.tbl (a int PRIMARY KEY)"))
                .await
                .unwrap();

            // The event alone proves nothing - the driver must have re-read the
            // keyspace and published the result.
            let _state_after_event_2 = wait_for_state(&session, |state| {
                state
                    .get_keyspace(&keyspace)
                    .is_some_and(|ks| ks.tables.contains_key("tbl"))
            })
            .await;

            let requests = drain_count(&mut metadata_request_rx);
            assert_eq!(
                requests, SCHEMA_FETCH_REQUESTS,
                "the control connection issued {requests} metadata requests, so the event \
                triggered something other than a partial schema fetch"
            );

            info!("Part 3: Dropping keyspace");

            // Only one more thing left to test: Dropping a keyspace must
            // remove it from state without issuing any queries.

            // The checks are intentionally commented out.
            // When dropping a keyspace that contains some tables, Scylla can send the events in the wrong order:
            //     2026-08-21T16:12:16.255229Z DEBUG scylla::cluster::metadata::worker: Received server event: SchemaChange(KeyspaceChange { change_type: Dropped, keyspace_name: "test_rust_4cf8c3ab7e0940cc8970ef8c2397c1fe" })
            //     2026-08-21T16:12:16.255279Z DEBUG scylla::cluster::metadata::worker: Received server event: SchemaChange(TableChange { change_type: Dropped, keyspace_name: "test_rust_4cf8c3ab7e0940cc8970ef8c2397c1fe", object_name: "tbl" })
            //

            // let requests = drain_count(&mut metadata_request_rx).await;
            // assert_eq!(
            //     requests, 0,
            //     "No queries should be needed to drop a keyspace"
            // );

            // To avoid this, we can inject the event manually.

            inject_keyspace_drop_event(&running_proxy, &keyspace);

            let _state_after_event_3 =
                wait_for_state(&session, |state| state.get_keyspace(&keyspace).is_none()).await;

            let requests = drain_count(&mut metadata_request_rx);
            assert_eq!(
                requests, 0,
                "No requests should be needed for drop of a keyspace"
            );

            session
                .ddl(format!("DROP KEYSPACE {keyspace}"))
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

/// What [`PeriodicFetchMode::FullMetadata`] exists for: a cluster whose
/// `SCHEMA_CHANGE` events cannot be relied upon. With every EVENT frame dropped
/// by the proxy, the driver has nothing to react to, and only the periodic full
/// fetch can bring the new keyspace into the metadata.
#[tokio::test]
async fn full_metadata_mode_picks_up_schema_changes_without_events() {
    setup_tracing();

    let res = test_with_3_node_cluster(
        ShardAwareness::QueryNode,
        |proxy_uris, translation_map, mut running_proxy| async move {
            let keyspace = unique_keyspace_name();
            let session = new_schema_watching_session(&proxy_uris, &translation_map, &keyspace)
                .periodic_metadata_fetch_mode(PeriodicFetchMode::FullMetadata)
                .build()
                .await
                .unwrap();
            wait_until_all_nodes_are_connected(3, &session).await;

            // Installed after the session is up, so that the REGISTER handshake
            // is untouched and only the announcements are lost.
            drop_all_events(&mut running_proxy);

            assert!(
                session
                    .get_cluster_state()
                    .get_keyspace(&keyspace)
                    .is_none()
            );

            create_keyspace(&session, &keyspace).await;
            session
                .ddl(format!("CREATE TABLE {keyspace}.tbl (a int PRIMARY KEY)"))
                .await
                .unwrap();

            let _state = tokio::time::timeout(
                FETCH_WAIT_TIMEOUT,
                wait_for_state(&session, |state| {
                    state
                        .get_keyspace(&keyspace)
                        .is_some_and(|ks| ks.tables.contains_key("tbl"))
                }),
            )
            .await
            .expect(
                "the periodic full fetch did not pick up the new keyspace, so the mode depends \
                on the events it is meant to work without",
            );

            session
                .ddl(format!("DROP KEYSPACE {keyspace}"))
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

/// Builds a session that reacts to schema changes quickly and only for
/// `keyspace`.
///
/// The keyspace restriction is what makes the request counting of the schema
/// tests meaningful: the cluster is shared with the other tests, whose DDL
/// produces SCHEMA_CHANGE events of its own, and a restricted session fetches
/// nothing for the keyspaces it was told to ignore.
fn new_schema_watching_session(
    proxy_uris: &[String],
    translation_map: &std::collections::HashMap<SocketAddr, SocketAddr>,
    keyspace: &str,
) -> SessionBuilder {
    SessionBuilder::new()
        .known_node(proxy_uris[0].as_str())
        .address_translator(Arc::new(translation_map.clone()))
        .cluster_metadata_refresh_interval(SCHEMA_REFRESH_INTERVAL)
        // Off, so that the DDL of the tests is answered by the event handling
        // under test rather than by an explicit full refresh.
        .refresh_metadata_on_auto_schema_agreement(false)
        .keyspaces_to_fetch([keyspace])
}

async fn create_keyspace(session: &Session, keyspace: &str) {
    session
        .ddl(format!(
            "CREATE KEYSPACE {keyspace} WITH REPLICATION = \
            {{'class': 'NetworkTopologyStrategy', 'replication_factor': 1}}"
        ))
        .await
        .unwrap();
}

/// Starts counting the metadata requests of the control connection.
///
/// Only the control connection registers for events, hence the registration
/// condition. The driver prepares its metadata statements, so what identifies a
/// fetch is the number of EXECUTEs, not any query text. Installed on demand, so
/// that the requests preceding the behaviour under test are not counted.
fn count_metadata_requests(running_proxy: &mut RunningProxy) -> MetadataRequestFeedback {
    let (metadata_request_tx, metadata_request_rx) = mpsc::unbounded_channel();
    for node in running_proxy.running_nodes.iter_mut() {
        node.prepend_request_rules(vec![RequestRule(
            Condition::ConnectionRegisteredAnyEvent
                .and(Condition::RequestOpcode(RequestOpcode::Execute)),
            RequestReaction::noop().with_feedback_when_performed(metadata_request_tx.clone()),
        )]);
    }
    metadata_request_rx
}

/// Waits until the published `ClusterState` satisfies `is_expected`.
async fn wait_for_state(
    session: &Session,
    is_expected: impl Fn(&Arc<ClusterState>) -> bool,
) -> Arc<ClusterState> {
    loop {
        let state = session.get_cluster_state();
        if is_expected(&state) {
            return state;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// Counts everything the feedback channel holds.
fn drain_count(metadata_request_rx: &mut MetadataRequestFeedback) -> usize {
    let mut requests = 0;
    while metadata_request_rx.try_recv().is_ok() {
        requests += 1;
    }
    requests
}

/// Makes the cluster look mute to the driver: every EVENT frame the server
/// sends is dropped by the proxy.
fn drop_all_events(running_proxy: &mut RunningProxy) {
    for node in running_proxy.running_nodes.iter_mut() {
        node.prepend_response_rules(vec![ResponseRule(
            Condition::ResponseOpcode(ResponseOpcode::Event),
            ResponseReaction::drop_frame(),
        )]);
    }
}
