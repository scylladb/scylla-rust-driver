use std::fmt::Display;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};

use futures::StreamExt;
use scylla::client::session::Session;
use scylla::policies::host_listener::{HostEvent, HostEventContext, HostListener};
use tracing::info;
use uuid::Uuid;

use crate::ccm::lib::cluster::{Cluster, ClusterOptions};
use crate::ccm::lib::node::{Node, NodeStartOptions};
use crate::ccm::lib::{CLUSTER_VERSION, run_ccm_test_with_configuration};
use crate::utils::{execute_unprepared_statement_everywhere, setup_tracing};

fn cluster_3_nodes() -> ClusterOptions {
    ClusterOptions {
        name: "cluster_3_node".to_string(),
        version: CLUSTER_VERSION.clone(),
        nodes_per_dc: vec![3],
        ..ClusterOptions::default()
    }
}

#[derive(Debug, Default)]
struct LoggingListener {
    log: Mutex<Vec<(Uuid, SocketAddr, &'static str)>>,
}

impl Display for LoggingListener {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_list()
            .entries(self.log.lock().unwrap().iter())
            .finish()
    }
}

impl HostListener for LoggingListener {
    fn on_event(&self, context: &HostEventContext, event: &HostEvent) {
        let event_str = match *event {
            HostEvent::Added => "ADDED",
            HostEvent::Removed => "REMOVED",
            HostEvent::Up => "UP",
            HostEvent::Down => "DOWN",
            _ => "<UNKNOWN EVENT>",
        };
        self.log
            .lock()
            .unwrap()
            .push((context.host_id(), context.addr(), event_str));
    }
}

async fn run_ccm_events_test(
    cluster_config: impl AsyncFnOnce(Cluster) -> Cluster,
    test: impl AsyncFnOnce(&mut Cluster, Arc<dyn HostListener>) -> (),
) {
    let listener = Arc::new(LoggingListener::default());
    let listener_clone = Arc::clone(&listener);
    run_ccm_test_with_configuration(
        cluster_3_nodes,
        {
            |cluster: Cluster| async move {
                // I wanted to call this at the end of the function,
                // but scylla-ccm can't handle calling `ccm updateconf` after using `ccm <node> updateconf`.
                // See: https://github.com/scylladb/scylla-ccm/issues/686
                let mut cluster = cluster_config(cluster).await;

                futures::stream::iter(cluster.nodes_mut().iter_mut())
                    .for_each(|_node| std::future::ready::<()>(()))
                    .await;

                cluster
            }
        },
        async |cluster: &mut Cluster| {
            test(cluster, listener).await;
            println!("Host listener log:\n{:#}", listener_clone);
        },
    )
    .await
}

async fn check_session_works_and_fully_connected(expected_nodes: usize, session: &Session) {
    let state = session.get_cluster_state();
    assert_eq!(state.get_nodes_info().len(), expected_nodes);
    assert!(
        state
            .get_nodes_info()
            .iter()
            .inspect(|node| {
                tracing::debug!(
                    "Node {}, address: {}, connected: {}",
                    node.host_id,
                    node.address,
                    node.is_connected()
                )
            })
            .all(|node| node.is_connected())
    );
    execute_unprepared_statement_everywhere(
        session,
        &state,
        &"SELECT * FROM system.local WHERE key='local'".into(),
        &(),
    )
    .await
    .unwrap();
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
#[deprecated(note = "
    `ccm remove` without `decommission` does not perform `nodetool removenode`, which leaves the cluster\
    in an inconsistent state and may make group 0 undecisive. Prefer `decommission_remove` instead.
")]
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
async fn test_node_stop() {
    setup_tracing();

    async fn test(cluster: &mut Cluster, listener: Arc<dyn HostListener>) {
        {
            tracing::info!("Node stop sub-test");
            let session_builder = cluster.make_session_builder().await;
            let mut config = session_builder.config;
            config.host_listener = Some(listener);
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
    }

    run_ccm_events_test(async |c| c, test).await
}
