use std::fmt::Display;
use std::net::{IpAddr, SocketAddr};
use std::sync::{Arc, Mutex};

use scylla::client::session::Session;
use scylla::policies::host_filter::DcHostFilter;
use scylla::policies::host_listener::{HostEvent, HostEventContext, HostListener};
use tracing::info;
use uuid::Uuid;

use crate::ccm::lib::cluster::{Cluster, ClusterOptions};
use crate::ccm::lib::node::{Node, NodeStartOptions};
use crate::ccm::lib::{CLUSTER_VERSION, run_ccm_test_with_configuration};
use crate::utils::{check_session_works_and_fully_connected, setup_tracing};

/* LoggingListener HostListener implementation */

fn ip4_last_octet(ip: IpAddr) -> u8 {
    match ip {
        std::net::IpAddr::V4(ipv4) => ipv4.octets()[3],
        std::net::IpAddr::V6(_) => panic!("Expected IPv4 address, got IPv6: {ip}"),
    }
}

#[derive(PartialEq, Eq)]
enum EventKind {
    Added,
    Removed,
    Up,
    Down,
    Unknown,
}

impl std::fmt::Debug for EventKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            EventKind::Added => f.write_str("ADDED"),
            EventKind::Removed => f.write_str("REMOVED"),
            EventKind::Up => f.write_str("UP"),
            EventKind::Down => f.write_str("DOWN"),
            EventKind::Unknown => f.write_str("<UNKNOWN>"),
        }
    }
}
impl Display for EventKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Debug::fmt(self, f)
    }
}

#[derive(Debug, Default)]
struct EventLog {
    events: Vec<(Uuid, SocketAddr, EventKind)>,
}

impl EventLog {
    /// Validates the event log for consistency.
    ///
    /// This performs a set of checks, which must hold true for any valid sequence of events.
    fn validate(&self) {
        self.validate_state_graph();

        // The below checks are likely already covered by `validate_state_graph`,
        // but they are simpler and more direct, so I keep them for clarity.
        // We can remove them later if reviewers think they are redundant.

        #[derive(Debug, Clone, Copy)]
        enum SeenState {
            Up,
            Down,
            Unknown,
        }

        // No DOWN/UP event may be duplicated.
        let mut next_event_per_node = [SeenState::Unknown; 4];

        for (i, (_, addr, event)) in self.events.iter().enumerate().rev() {
            let node_index = ip4_last_octet(addr.ip()) as usize - 1;
            match (event, next_event_per_node[node_index]) {
                (EventKind::Down, SeenState::Down) => {
                    panic!("Duplicated DOWN event for address {} at index {}", addr, i);
                }
                (EventKind::Up, SeenState::Up) => {
                    panic!("Duplicated UP event for address {} at index {}", addr, i);
                }
                (EventKind::Down, _) => {
                    next_event_per_node[node_index] = SeenState::Down;
                }
                (EventKind::Up, _) => {
                    next_event_per_node[node_index] = SeenState::Up;
                }
                _ => {}
            }
        }

        // No DOWN event may come before an UP event for the same node.
        for (node_id, next_event) in next_event_per_node.iter().enumerate() {
            if let SeenState::Down = next_event {
                panic!("DOWN event came before UP event for node {}", node_id + 1);
            }
        }
    }

    /// Validates the event log for consistency, by following the state graph of each node
    /// and updating it according to the events received.
    ///
    /// This panics for any invalid transition found.
    fn validate_state_graph(&self) {
        #[derive(Debug, Clone, Copy)]
        enum NodeState {
            Unknown,
            JustAdded,
            Up,
            Down,
            Removed,
        }

        let mut node_state = [NodeState::Unknown; 4];

        for (i, (_, addr, event)) in self.events.iter().enumerate() {
            let node_index = ip4_last_octet(addr.ip()) as usize - 1;
            let state = &mut node_state[node_index];
            match (*state, event) {
                (_, EventKind::Unknown) => panic!("Received event of unknown kind"),

                // ADDED
                (NodeState::Unknown, EventKind::Added) => *state = NodeState::JustAdded,
                (NodeState::JustAdded, EventKind::Added) => {
                    panic!("Node {} ADDED duplicated, at index {}", node_index, i)
                }
                (NodeState::Up, EventKind::Added) => panic!(
                    "Node {} ADDED when being in UP state, at index {}",
                    node_index, i
                ),
                (NodeState::Down, EventKind::Added) => {
                    panic!(
                        "Node {} ADDED when being in DOWN state, at index {}",
                        node_index, i
                    );
                }
                (NodeState::Removed, EventKind::Added) => *state = NodeState::JustAdded,

                // REMOVED
                (NodeState::Unknown, EventKind::Removed) => {
                    panic!(
                        "Node {} REMOVED without being ADDED before, at index {}",
                        node_index, i
                    );
                }
                (NodeState::JustAdded, EventKind::Removed) => {
                    // TODO: decide if we require UP automatically after ADDED.
                    *state = NodeState::Removed
                }
                (NodeState::Up, EventKind::Removed) => {
                    panic!("Node {} REMOVED when being UP, at index {}", node_index, i)
                }
                (NodeState::Down, EventKind::Removed) => *state = NodeState::Removed,
                (NodeState::Removed, EventKind::Removed) => {
                    panic!("Node {} REMOVED duplicated, at index {}", node_index, i)
                }

                // UP
                (NodeState::Unknown, EventKind::Up) => panic!(
                    "Node {} UP without being ADDED before, at index {}",
                    node_index, i
                ),
                (NodeState::JustAdded, EventKind::Up) => *state = NodeState::Up,
                (NodeState::Up, EventKind::Up) => {
                    panic!("Node {} UP duplicated, at index {}", node_index, i)
                }
                (NodeState::Down, EventKind::Up) => *state = NodeState::Up,

                (NodeState::Unknown, EventKind::Down) => panic!(
                    "Node {} DOWN without being ADDED before, at index {}",
                    node_index, i
                ),
                (NodeState::Removed, EventKind::Up) => panic!(
                    "Node {} UP when being in REMOVED state, at index {}",
                    node_index, i
                ),

                // DOWN
                (NodeState::JustAdded, EventKind::Down) => {
                    // TODO: decide if we require UP automatically after ADDED.
                    *state = NodeState::Down
                }
                (NodeState::Up, EventKind::Down) => *state = NodeState::Down,

                (NodeState::Down, EventKind::Down) => {
                    panic!("Node {} DOWN duplicated, at index {}", node_index, i)
                }
                (NodeState::Removed, EventKind::Down) => panic!(
                    "Node {} DOWN when being in REMOVED state, at index {}",
                    node_index, i
                ),
            }
        }
    }

    #[track_caller]
    fn assert_log_subslice_contains(
        events: &[(Uuid, SocketAddr, EventKind)],
        node_id: u8,
        kind: EventKind,
    ) {
        events
            .iter()
            .find(|(_host_id, addr, event_kind)| {
                ip4_last_octet(addr.ip()) == node_id && *event_kind == kind
            })
            .unwrap_or_else(|| panic!("Expected {kind} event for node{node_id} not found"));
    }

    #[track_caller]
    fn assert_log_misses_node_events(events: &[(Uuid, SocketAddr, EventKind)], node_id: u8) {
        let _ = events
            .iter()
            .find(|(_host_id, addr, _event_kind)| ip4_last_octet(addr.ip()) == node_id)
            .is_none_or(|(_host_id, _addr, event_kind)| {
                panic!("Unexpected {event_kind} event for node{node_id} found")
            });
    }
}

impl Display for EventLog {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_list().entries(self.events.iter()).finish()
    }
}

#[derive(Debug, Default)]
struct LoggingListener {
    log: Mutex<EventLog>,
}

impl Display for LoggingListener {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        Display::fmt(&*self.log.lock().unwrap(), f)
    }
}

impl HostListener for LoggingListener {
    fn on_event(&self, context: &HostEventContext, event: &HostEvent) {
        let event = match *event {
            HostEvent::Added => EventKind::Added,
            HostEvent::Removed => EventKind::Removed,
            HostEvent::Up => EventKind::Up,
            HostEvent::Down => EventKind::Down,
            _ => EventKind::Unknown,
        };
        self.log
            .lock()
            .unwrap()
            .events
            .push((context.host_id(), context.addr(), event));
    }
}

/* CCM test helpers */

fn cluster_3_nodes() -> ClusterOptions {
    ClusterOptions {
        name: "cluster_3_node".to_string(),
        version: CLUSTER_VERSION.clone(),
        nodes_per_dc: vec![2, 1],
        ..ClusterOptions::default()
    }
}

async fn run_ccm_events_test(
    cluster_config: impl AsyncFnOnce(Cluster) -> Cluster,
    test: impl AsyncFnOnce(&mut Cluster) -> (),
) {
    run_ccm_test_with_configuration(
        cluster_3_nodes,
        {
            |cluster: Cluster| async move {
                // I wanted to call this at the end of the function,
                // but scylla-ccm can't handle calling `ccm updateconf` after using `ccm <node> updateconf`.
                // See: https://github.com/scylladb/scylla-ccm/issues/686
                cluster_config(cluster).await
            }
        },
        async |cluster: &mut Cluster| {
            test(cluster).await;
        },
    )
    .await
}

async fn do_with_node<T, E: std::fmt::Debug>(
    cluster: &mut Cluster,
    id: u16,
    f: impl AsyncFnOnce(&mut Node) -> Result<T, E>,
) -> T {
    f(cluster.nodes_mut().get_mut_by_id(id).unwrap())
        .await
        .inspect_err(|_| cluster.mark_as_failed())
        .expect("failed to execute ccm command")
}

/// Stops a node.
async fn stop(cluster: &mut Cluster, id: u16) {
    do_with_node(cluster, id, async |node| node.stop(None).await).await
}

/// Starts a node, waiting for it to be fully operational and noticed by other nodes.
async fn start(cluster: &mut Cluster, id: u16) {
    do_with_node(cluster, id, async |node| {
        node.start(Some(
            NodeStartOptions::new()
                .wait_for_binary_proto(true)
                .wait_other_notice(true),
        ))
        .await
    })
    .await
}

/// Removes a node gently, with decommission.
async fn decommission_remove(cluster: &mut Cluster, id: u16) {
    do_with_node(cluster, id, async |node| {
        node.decommission().await?;
        node.delete().await
    })
    .await
}

/// Removes a node suddenly, without decommission.
#[expect(dead_code)]
#[deprecated(
    note = "`ccm remove` without `decommission` does not perform `nodetool removenode`,
which leaves the cluster in an inconsistent state and may leave group 0 indecisive.
Prefer `decommission_remove` instead."
)]
async fn forcibly_remove(cluster: &mut Cluster, id: u16) {
    do_with_node(cluster, id, async |node| node.delete().await).await
}

/// Adds a node to the given DC.
async fn add(cluster: &mut Cluster, dc_id: Option<u16>) -> u16 {
    cluster
        .add_node(dc_id)
        .await
        .map(|node| node.id())
        .inspect_err(|_| cluster.mark_as_failed())
        .expect("failed to add node")
}

#[tokio::test]
#[cfg_attr(not(ccm_tests), ignore)]
async fn test_host_listener_with_status_changes() {
    setup_tracing();

    async fn test(cluster: &mut Cluster) {
        let listener = Arc::new(LoggingListener::default());
        let session_builder = cluster.make_session_builder().await;
        let mut config = session_builder.config;
        config.host_listener = Some(Arc::clone(&listener) as Arc<dyn HostListener>);

        {
            let session = Session::connect(config).await.unwrap();
            check_session_works_and_fully_connected(cluster.nodes().len(), &session).await;

            // This test tries to trigger multiple UP/DOWN events,
            // by stopping and starting nodes in a specific sequence.
            // It stops CC and non-CC nodes alternately,
            // to ensure the driver detects all state changes correctly.

            // Expected sequence of events:
            // 0.  - initial state - all nodes ADDED and UP
            // 1.  - node1 DOWN
            // 2.  - node2 DOWN
            // 3.  - node2 UP
            // 4.  - node3 DOWN
            // 5.  - node2 DOWN
            // 6.  - node3 UP
            // 7.  - node2 UP

            // 1.
            info!("1. Stopping node1");
            stop(cluster, 1).await;

            // 2.
            info!("2. Stopping node2");
            stop(cluster, 2).await;

            // 3.
            info!("3. Restarting node2");
            start(cluster, 2).await;

            // 4.
            info!("4. Stopping node3");
            stop(cluster, 3).await;

            // 5.
            info!("5. Stopping node2");
            stop(cluster, 2).await;

            // 6.
            info!("6. Restarting node3");
            start(cluster, 3).await;

            // 7.
            info!("7. Restarting node2");
            start(cluster, 2).await;

            session.await_schema_agreement().await.unwrap();
            // This is to ensure all events are triggered and processed before dropping the session.
            tokio::time::sleep(std::time::Duration::from_secs(10)).await;
        }
        // Session has been dropped. No new events should be logged now.

        info!("Host listener log:\n{:#}", listener);

        // Validations.
        {
            let log = &*listener.log.lock().unwrap();
            log.validate();

            let sublog_without_initial = &log.events[6..];

            EventLog::assert_log_subslice_contains(sublog_without_initial, 2, EventKind::Up);
            EventLog::assert_log_subslice_contains(sublog_without_initial, 3, EventKind::Up);
        }
    }
    run_ccm_events_test(async |c| c, test).await;
}

#[tokio::test]
#[cfg_attr(not(ccm_tests), ignore)]
async fn test_host_listener_with_topology_changes() {
    setup_tracing();

    async fn test(cluster: &mut Cluster) {
        let listener = Arc::new(LoggingListener::default());
        let session_builder = cluster.make_session_builder().await;
        let mut config = session_builder.config;
        config.host_listener = Some(Arc::clone(&listener) as Arc<dyn HostListener>);

        {
            let session = Session::connect(config).await.unwrap();
            check_session_works_and_fully_connected(cluster.nodes().len(), &session).await;

            // Test ideas:
            // 1. Just stop one non-CC node and see if the listener logged the DOWN event.
            // 2. Restart the node and see if the listener logged the UP event.
            // 3. Stop the CC node and see if the listener logged the DOWN event.
            // 4. Restart the node and see if the listener logged the UP event.
            // 5. Remove a non-CC node and see if the listener logged the DOWN and then REMOVED event.
            // 6. Remove the CC node and see if the listener logged the DOWN and then REMOVED event.
            // 7. Add a new node and see if the listener logged the ADDED and UP event.
            // 8. Stop the newly added node and see if the listener logged the DOWN event.
            // 9. Remove the stopped node and see if the listener logged the REMOVED event.
            // 10. Stop all nodes and see if the listener logged the DOWN events.
            // 11. Restart all nodes and see if the listener logged the UP events.
            //
            // Expected sequence of events:
            // 0.  - initial state - all nodes ADDED and UP
            // 1.  - node2 DOWN
            // 2.  - node2 UP
            // 3.  - node1 DOWN
            // 4   - node1 UP
            // 5.  - node1 node DOWN
            //     - node1 node REMOVED
            // 6.  - node2 DOWN
            //     - node2 REMOVED
            // 7.  - node4 ADDED
            //     - node4 UP
            // 8.  - node4 DOWN
            // 9.  - node4 REMOVED
            // 10. - node3 DOWN
            // 11. - node3 UP

            // 1. Just stop one non-CC node and see if the listener logged the DOWN event.
            info!("1. Stopping node2");
            stop(cluster, 2).await;

            // 2. Restart the node and see if the listener logged the UP event.
            info!("2. Restarting node2");
            start(cluster, 2).await;

            // 3. Stop the CC node and see if the listener logged the DOWN event.
            info!("3. Stopping node1");
            stop(cluster, 1).await;

            // 4. Restart the node and see if the listener logged the UP event.
            info!("4. Restarting node1");
            start(cluster, 1).await;

            // 5. Remove a non-CC node and see if the listener logged the DOWN and then REMOVED event.
            info!("5. Removing node1");
            decommission_remove(cluster, 1).await;

            // 6. Remove a CC node and see if the listener logged the DOWN and then REMOVED event.
            info!("6. Removing node2");
            decommission_remove(cluster, 2).await;

            // 7. Add a new node and see if the listener logged the ADDED and UP event.
            info!("7. Adding node4");
            let node4_id = add(cluster, None).await;
            info!("Starting node4");
            start(cluster, node4_id).await;

            // 8. Stop the newly added node and see if the listener logged the DOWN event.
            // info!("8. Stopping node4");
            // stop(cluster, node4_id).await;

            // 9. Remove the stopped node and see if the listener logged the REMOVED event.
            info!("9. Removing node4");
            decommission_remove(cluster, node4_id).await;

            // 10. Stop all nodes and see if the listener logged the DOWN events.
            info!("10. Stopping node3");
            stop(cluster, 3).await;

            // 11. Restart all nodes and see if the listener logged the UP events.
            info!("11. Restarting node3");
            start(cluster, 3).await;

            session.await_schema_agreement().await.unwrap();
        }
        // Session has been dropped. No new events should be logged now.

        info!("Host listener log:\n{:#}", listener);

        // Validations.
        {
            let log = &*listener.log.lock().unwrap();
            log.validate();
        }
    }

    run_ccm_events_test(async |c| c, test).await;
}

#[tokio::test]
#[cfg_attr(not(ccm_tests), ignore)]
async fn test_host_listener_with_host_filter() {
    setup_tracing();

    async fn test(cluster: &mut Cluster) {
        tracing::info!("HostFilter sub-test");
        let listener = Arc::new(LoggingListener::default());
        let mut session_builder = cluster.make_session_builder().await;
        session_builder =
            session_builder.host_filter(Arc::new(DcHostFilter::new("dc1".to_owned())));
        let mut config = session_builder.config;
        config.host_listener = Some(Arc::clone(&listener) as Arc<dyn HostListener>);

        {
            let _session = Session::connect(config).await.unwrap();

            // This test turns on DcHostFilter, which filters out node3 (in dc2).
            // Then, we check that events are still logged only for nodes in dc1 (node1 and node2).
            // Then we add another node to dc2 and check that its events are also filtered out.

            // Expected sequence of events:
            // 0.  - initial state - all nodes from dc2 ADDED and UP
            // 1.  - node1 DOWN
            // 2.  - node1 UP
            // 3.  - node3 DOWN filtered out
            // 4.  - node3 UP filtered out
            // 5.  - node4 ADDED filtered out
            //     - node4 UP filtered out
            // 6.  - node2 DOWN
            //     - node2 REMOVED
            // 7.  - node4 DOWN filtered out
            //     - node4 REMOVED filtered out

            info!("1. Stopping node1");
            stop(cluster, 1).await;

            info!("2. Restarting node1");
            start(cluster, 1).await;

            info!("3. Stopping node3");
            stop(cluster, 3).await;

            info!("4. Restarting node3");
            start(cluster, 3).await;

            info!("5. Adding node4");
            let node_4 = add(cluster, Some(2)).await;
            info!("Starting node4");
            start(cluster, node_4).await;

            info!("6. Removing node2");
            decommission_remove(cluster, 2).await;

            info!("7. Removing node4");
            decommission_remove(cluster, node_4).await;

            // This is to ensure all events are triggered and processed before dropping the session.
            tokio::time::sleep(std::time::Duration::from_secs(1)).await;
        }
        // Session has been dropped. No new events should be logged now.

        info!("Host listener log:\n{:#}", listener);

        // Validations.
        {
            let log = &*listener.log.lock().unwrap();
            log.validate();

            // Initial events for node3 are filtered out.
            let sublog_without_initial = &log.events[4..];

            EventLog::assert_log_subslice_contains(&log.events, 1, EventKind::Added);
            EventLog::assert_log_subslice_contains(&log.events, 1, EventKind::Up);
            EventLog::assert_log_subslice_contains(sublog_without_initial, 1, EventKind::Down);
            EventLog::assert_log_subslice_contains(sublog_without_initial, 1, EventKind::Up);
            EventLog::assert_log_subslice_contains(&log.events, 2, EventKind::Added);
            EventLog::assert_log_subslice_contains(&log.events, 2, EventKind::Up);
            EventLog::assert_log_subslice_contains(sublog_without_initial, 2, EventKind::Down);
            EventLog::assert_log_subslice_contains(sublog_without_initial, 2, EventKind::Removed);

            EventLog::assert_log_misses_node_events(&log.events, 3);
            EventLog::assert_log_misses_node_events(&log.events, 4);
        }
    }
    run_ccm_events_test(async |c| c, test).await;
}

// Other test ideas TODO:
// 1. IP change.
// 2. Node is removed, then another node is added with the same IP.
// 3. Driver connects when some nodes are down. Are they marked as UP optimistically,
//    before any connection attempt succeeds or fails?
