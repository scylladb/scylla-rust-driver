//! Integration tests for custom routing via `system.client_routes`.
//!
//! These tests require a CCM cluster and exercise the full client-routes flow:
//! driver →  NLB →  proxy →  Scylla, with `system.client_routes` providing
//! address translation rules.
//!
//! Each test uses per-node CQL-aware proxies with feedback channels to verify:
//! 1. The driver opens connections to ALL nodes
//! 2. The driver can query ALL nodes
//! 3. The driver connects through NLBs (address translation works)
//!
//! Proxy feedback alone proves all 3 requirements: if a proxy sees CQL traffic,
//! it necessarily went through the NLB (since the driver only knows NLB
//! addresses from `client_routes` and real node addresses, but not proxy addresses).

use std::collections::HashMap;
use std::time::Duration;

use tokio::sync::mpsc;

use crate::ccm::lib::CLUSTER_VERSION;
use crate::ccm::lib::client_routes::{
    ClientRoutesCluster, FeedbackItem, drain_feedback, run_client_routes_test,
};
use crate::ccm::lib::cluster::ClusterOptions;
use crate::ccm::lib::node::NodeId;
use crate::utils::{HEALTHCHECK_QUERY, setup_tracing};

use scylla::client::execution_profile::ExecutionProfile;
use scylla::client::session::Session;
use scylla::policies::load_balancing::{NodeIdentifier, SingleTargetLoadBalancingPolicy};
use scylla::statement::unprepared::Statement;
use tracing::info;

/// Timeout for waiting for the driver to open connections to all proxy nodes.
/// Must be generous enough for the driver to discover new/restarted nodes,
/// but short enough to fail promptly if the driver has a bug (e.g.,
/// `Untranslatable` marking prevents address translation for a node).
const CONNECTION_WAIT_TIMEOUT: Duration = Duration::from_secs(3);

// ---------------------------------------------------------------------------
// Cluster option factories
// ---------------------------------------------------------------------------

fn cluster_3_nodes() -> ClusterOptions {
    ClusterOptions {
        name: "client_routes_3_nodes".to_string(),
        version: CLUSTER_VERSION.clone(),
        nodes_per_dc: vec![3],
        ..ClusterOptions::default()
    }
}

fn cluster_2dc_2_2() -> ClusterOptions {
    ClusterOptions {
        name: "client_routes_2dc".to_string(),
        version: CLUSTER_VERSION.clone(),
        nodes_per_dc: vec![2, 2],
        ..ClusterOptions::default()
    }
}

// ---------------------------------------------------------------------------
// Helper: verify all active nodes receive queries via proxy feedback
// ---------------------------------------------------------------------------

/// Executes a query on every *active* node (i.e., nodes that still have a
/// running proxy chain) and verifies via proxy feedback that each such node
/// actually received traffic.
///
/// Only nodes whose host_id is known to `plc` AND present in the driver's
/// cluster state are targeted. This avoids sending queries to nodes that
/// have been decommissioned (proxy torn down) but whose removal the driver
/// hasn't yet observed via a topology event.
///
/// We deliberately target nodes rather than individual shards: after a client
/// routes update the pool may not be fully filled yet (only one shard
/// connected), and targeting a specific missing shard would fail with
/// `ConnectionPoolError(Broken)`.
async fn assert_queries_reach_all_nodes(
    session: &Session,
    plc: &ClientRoutesCluster,
    rxs: &mut HashMap<NodeId, mpsc::UnboundedReceiver<FeedbackItem>>,
) {
    let cluster = session.get_cluster_state();

    let tasks = plc.host_ids().values().map(|&host_id| {
        let cluster = &cluster;
        async move {
            let node = cluster.get_node_by_host_id(host_id).unwrap_or_else(|| {
                panic!(
                    "Node with host_id {} is active in the test harness \
                     but absent from driver's cluster state",
                    host_id,
                )
            });
            let policy =
                SingleTargetLoadBalancingPolicy::new(NodeIdentifier::Node(node.clone()), None);
            let execution_profile = ExecutionProfile::builder()
                .load_balancing_policy(policy)
                .build();
            let mut statement: Statement = HEALTHCHECK_QUERY.into();
            statement.set_execution_profile_handle(Some(execution_profile.into_handle()));
            session.query_unpaged(statement, &()).await.unwrap();
        }
    });

    futures::future::join_all(tasks).await;

    let (per_node, _total) = drain_feedback(rxs);
    for &node_id in rxs.keys() {
        let count = *per_node.get(&node_id).unwrap_or(&0);
        assert!(
            count >= 1,
            "Node {} received 0 queries — driver didn't reach it via proxy",
            node_id,
        );
    }
}

// ---------------------------------------------------------------------------
// Multi-DC topology change (2 DCs, decommission + add)
// ---------------------------------------------------------------------------

/// **Goal**: Verify the driver correctly handles decommissioning a node and
/// adding a new one in a multi-DC cluster, with route updates propagated
/// at each step.
///
/// **Added value**: Tests the full lifecycle of topology membership changes
/// under client-routes: contact-point NLB backend removal before
/// decommission (so no new control connections target the node), followed
/// by route update after decommission (so the driver learns the node is
/// gone), and route addition after a new node joins (so the driver
/// discovers and routes to it). Without this test, we wouldn't know if
/// the driver's route cache stays consistent across node removals and
/// additions in a multi-DC setup.
///
/// **Scenario** (2 DCs, 2+2 nodes):
/// 1. **Phase 1** — all 4 nodes: queries reach all 4.
/// 2. Remove the highest-ID node in DC2 from the contact-point NLB,
///    decommission it via CCM, then post routes without it.
/// 3. **Phase 2** — 3 nodes: queries reach remaining 3, decommissioned
///    node absent from feedback.
/// 4. Add a new node to DC2, discover its host ID, start its proxy chain.
/// 5. **Phase 3** — 4 nodes again: queries reach all 4 (including the
///    newly added node).
async fn multi_dc_topology_change(plc: &mut ClientRoutesCluster) {
    let session = plc
        .make_session_builder()
        .build()
        .await
        .expect("Failed to build client-routes session");

    // Wait for the driver to load routes and connect to all proxy nodes.
    plc.wait_for_pools_connected(&session, CONNECTION_WAIT_TIMEOUT)
        .await
        .expect("Driver did not connect to all nodes");

    // Identify nodes. In a 2+2 setup, CCM node IDs are 1,2 (DC1) and 3,4 (DC2).
    let initial_nodes = plc.active_node_ids();
    assert_eq!(initial_nodes.len(), 4, "Expected 4 initial nodes");
    info!("Initial nodes: {:?}", initial_nodes);

    // --- Phase 1: all 4 nodes ---
    info!("=== Phase 1: all 4 nodes ===");
    let mut rxs = plc.setup_query_feedback();
    assert_queries_reach_all_nodes(&session, plc, &mut rxs).await;

    // The node to decommission: the highest ID in DC2 (should be node 4).
    let node_to_decommission = *initial_nodes.iter().max().expect("non-empty");
    info!("Will decommission node {}", node_to_decommission);

    // --- Decommission node from DC2 ---
    // Contact-point NLB backend is removed before CCM decommission (prevents
    // new control connections); routes are posted after decommission completes.
    info!("Decommissioning node {}...", node_to_decommission);
    plc.decommission_node(node_to_decommission)
        .await
        .expect("Failed to decommission node");

    // --- Phase 2: 3 nodes remaining ---
    info!("=== Phase 2: 3 nodes remaining ===");
    let remaining_nodes = plc.active_node_ids();
    assert_eq!(remaining_nodes.len(), 3);

    let mut rxs = plc.setup_query_feedback();
    assert_queries_reach_all_nodes(&session, plc, &mut rxs).await;

    // --- Add new node to DC2 ---
    // Cluster::add_node() uses 1-based CCM DC naming: 2 = dc2.
    info!("Adding new node to DC2...");
    let new_node_id = plc.add_node(Some(2)).await.expect("Failed to add new node");
    info!("New node added: {}", new_node_id);
    // Wait for the driver to discover the new node and open a connection
    // through the new proxy chain.
    plc.wait_for_pools_connected(&session, CONNECTION_WAIT_TIMEOUT)
        .await
        .expect("Driver did not connect to all nodes after adding node");

    // --- Phase 3: 4 nodes again (with new node) ---
    info!("=== Phase 3: 4 nodes with new node ===");
    let final_nodes = plc.active_node_ids();
    assert_eq!(final_nodes.len(), 4);
    assert!(
        final_nodes.contains(&new_node_id),
        "New node {} should be in active list",
        new_node_id
    );
    let mut rxs = plc.setup_query_feedback();
    assert_queries_reach_all_nodes(&session, plc, &mut rxs).await;
}

#[tokio::test]
async fn test_client_routes_multi_dc_topology_change() {
    setup_tracing();
    run_client_routes_test(cluster_2dc_2_2, multi_dc_topology_change).await;
}

// ---------------------------------------------------------------------------
// Rolling restart (1 DC, 3 nodes)
// ---------------------------------------------------------------------------

/// **Goal**: Verify the driver survives a rolling restart of all nodes,
/// picking up new NLB ports after each restart and maintaining full-cluster
/// query coverage throughout.
///
/// **Added value**: Rolling restarts are a standard operational procedure.
/// Each restart gives the node's proxy chain a new OS-assigned NLB port,
/// so the driver must re-read `system.client_routes` and reconnect. This
/// test catches regressions where the driver fails to update its route
/// cache incrementally, or where stale connections to old NLB ports cause
/// query failures during the rolling window.
///
/// **Scenario** (1 DC, 3 nodes):
/// 1. **Phase 0** (baseline): queries reach all 3.
/// 2. For each node 1, 2, 3 in turn:
///    a. Stop the node via CCM, tear down its proxy chain.
///    b. Start the node via CCM, rebuild proxy chain (new NLB port).
///    c. Wait for the driver to connect through the new chain.
///    d. Queries reach all 3.
async fn rolling_restart(plc: &mut ClientRoutesCluster) {
    let session = plc
        .make_session_builder()
        .build()
        .await
        .expect("Failed to build client-routes session");

    plc.wait_for_pools_connected(&session, CONNECTION_WAIT_TIMEOUT)
        .await
        .expect("Driver did not connect to all nodes");

    // --- Phase 0: baseline ---
    info!("=== Phase 0: baseline (all 3 nodes) ===");
    let mut rxs = plc.setup_query_feedback();
    assert_queries_reach_all_nodes(&session, plc, &mut rxs).await;

    // --- Rolling restart: stop + start each node in turn ---
    for target_node in 1..=3u16 {
        info!("=== Rolling restart: stopping node {} ===", target_node);

        // Stop the CCM node.
        {
            let node = plc
                .cluster_mut()
                .nodes_mut()
                .get_mut_by_id(target_node)
                .expect("node not found");
            node.stop(None)
                .await
                .unwrap_or_else(|e| panic!("Failed to stop node {}: {}", target_node, e));
        }
        plc.stop_node_chain(target_node)
            .await
            .unwrap_or_else(|e| panic!("Failed to stop chain for node {}: {}", target_node, e));

        // Start the CCM node again.
        info!("Rolling restart: starting node {}...", target_node);
        {
            let node = plc
                .cluster_mut()
                .nodes_mut()
                .get_mut_by_id(target_node)
                .expect("node not found");
            node.start(None)
                .await
                .unwrap_or_else(|e| panic!("Failed to start node {}: {}", target_node, e));
        }
        plc.restart_node_chain(target_node)
            .await
            .unwrap_or_else(|e| panic!("Failed to restart chain for node {}: {}", target_node, e));

        plc.wait_for_pools_connected(&session, CONNECTION_WAIT_TIMEOUT)
            .await
            .unwrap_or_else(|e| {
                panic!(
                    "Driver did not reconnect to all nodes after restarting node {}: {}",
                    target_node, e
                )
            });

        // Verify all 3 nodes receive traffic after this restart.
        info!(
            "=== Phase after restarting node {}: verifying all 3 nodes ===",
            target_node
        );
        let mut rxs = plc.setup_query_feedback();
        assert_queries_reach_all_nodes(&session, plc, &mut rxs).await;
    }
}

#[tokio::test]
async fn test_client_routes_rolling_restart() {
    setup_tracing();
    run_client_routes_test(cluster_3_nodes, rolling_restart).await;
}

// ---------------------------------------------------------------------------
// NLB port remap without Scylla restart (1 DC, 3 nodes)
// ---------------------------------------------------------------------------

/// **Goal**: Verify the driver detects route changes and reconnects through
/// new NLB ports when the Scylla nodes themselves are NOT restarted — only
/// the proxy/NLB layer is rebuilt.
///
/// **Added value**: This is the purest client-routes test. All other
/// topology tests involve CCM node stop/start, which triggers Scylla-level
/// `STATUS_CHANGE` events that could mask route-change detection bugs. By
/// keeping Scylla nodes running and only changing the proxy chain (new NLB
/// ports + updated routes), this test isolates the driver's
/// `CLIENT_ROUTES_CHANGE` event handling and route-refresh logic from
/// node-lifecycle events. A failure here means the driver cannot react to
/// route-only changes.
///
/// **Scenario** (1 DC, 3 nodes):
/// 1. **Phase 1** (baseline): queries reach all 3.
/// 2. Rebuild proxy chains for nodes 1 and 2 (Scylla stays up). New
///    OS-assigned NLB ports, updated routes posted to ScyllaDB.
/// 3. Wait for the driver to connect through the new NLB ports.
/// 4. **Phase 2**: queries reach all 3.
async fn nlb_port_remap(plc: &mut ClientRoutesCluster) {
    let session = plc
        .make_session_builder()
        .build()
        .await
        .expect("Failed to build client-routes session");

    plc.wait_for_pools_connected(&session, CONNECTION_WAIT_TIMEOUT)
        .await
        .expect("Driver did not connect to all nodes");

    // --- Phase 1: baseline ---
    info!("=== Phase 1: baseline (all 3 nodes) ===");
    let mut rxs = plc.setup_query_feedback();
    assert_queries_reach_all_nodes(&session, plc, &mut rxs).await;

    // --- Remap: rebuild proxy chains for nodes 1 and 2 ---
    // Scylla nodes stay up; only the proxy + NLB are replaced, giving
    // them new OS-assigned ports.
    info!("=== Remapping NLB ports for nodes 1 and 2 ===");
    plc.restart_node_chain(1)
        .await
        .expect("Failed to remap chain for node 1");
    plc.restart_node_chain(2)
        .await
        .expect("Failed to remap chain for node 2");

    // No metadata refresh here, because we want to rely on the CLIENT_ROUTES_CHANGE event.

    // Wait for the driver to connect through the new NLB ports using
    // the freshly-read routes.
    plc.wait_for_pools_connected(&session, CONNECTION_WAIT_TIMEOUT)
        .await
        .expect("Driver did not reconnect through new NLB ports");

    // --- Phase 2: verify traffic goes through new ports ---
    info!("=== Phase 2: after NLB port remap ===");
    let mut rxs = plc.setup_query_feedback();
    assert_queries_reach_all_nodes(&session, plc, &mut rxs).await;
}

#[tokio::test]
async fn test_client_routes_nlb_port_remap() {
    setup_tracing();
    run_client_routes_test(cluster_3_nodes, nlb_port_remap).await;
}

// ---------------------------------------------------------------------------
// Scale out (1 DC, 3 →  6 nodes)
// ---------------------------------------------------------------------------

/// **Goal**: Verify the driver correctly discovers and routes to dynamically
/// added nodes, integrating them into the existing client-routes topology.
///
/// **Added value**: Scale-out (adding nodes to a live cluster) is a key
/// cloud operations scenario. Each new node requires host-ID discovery, a
/// fresh proxy chain, and a route update. This test verifies that the
/// driver's route cache, connection pool, and token ring are all updated
/// to include the new nodes. Without it, we wouldn't catch bugs where the
/// driver ignores newly added nodes or fails to translate their addresses.
///
/// **Scenario** (1 DC, starts with 3 nodes):
/// 1. **Phase 1** (3 nodes): queries reach all 3.
/// 2. Add 3 new nodes to DC1 one at a time (CCM add + start, discover
///    host ID, start proxy chain, post updated routes).
/// 3. Wait for the driver to connect to all 6 nodes.
/// 4. **Phase 2** (6 nodes): queries reach all 6.
async fn scale_out(plc: &mut ClientRoutesCluster) {
    let session = plc
        .make_session_builder()
        .build()
        .await
        .expect("Failed to build client-routes session");

    plc.wait_for_pools_connected(&session, CONNECTION_WAIT_TIMEOUT)
        .await
        .expect("Driver did not connect to all nodes");

    // --- Phase 1: initial 3 nodes ---
    info!("=== Phase 1: initial 3 nodes ===");
    let mut rxs = plc.setup_query_feedback();
    assert_queries_reach_all_nodes(&session, plc, &mut rxs).await;

    // --- Add 3 new nodes ---
    let mut new_node_ids = Vec::new();
    for i in 1..=3 {
        info!("Adding node {} of 3...", i);
        let new_id = plc
            .add_node(None)
            .await
            .unwrap_or_else(|e| panic!("Failed to add node {} of 3: {}", i, e));
        info!("Added node {}", new_id);
        new_node_ids.push(new_id);
    }

    // Wait for the driver to discover all 6 nodes, load their routes,
    // and open data pool connections.
    plc.wait_for_pools_connected(&session, CONNECTION_WAIT_TIMEOUT)
        .await
        .expect("Driver did not connect to all 6 nodes");

    // --- Phase 2: all 6 nodes ---
    info!("=== Phase 2: all 6 nodes ===");
    let active = plc.active_node_ids();
    assert_eq!(active.len(), 6, "Expected 6 active nodes, got {:?}", active);

    let mut rxs = plc.setup_query_feedback();
    assert_queries_reach_all_nodes(&session, plc, &mut rxs).await;
}

#[tokio::test]
async fn test_client_routes_scale_out() {
    setup_tracing();
    run_client_routes_test(cluster_3_nodes, scale_out).await;
}

// ---------------------------------------------------------------------------
// Event-driven reroute (2 DCs, 2+2 nodes)
// ---------------------------------------------------------------------------

/// Wait for any of the given feedback receivers to produce a message.
///
/// Only one proxy hosts the control connection, so only one receiver will
/// actually fire. This polls all receivers until any one gets a message.
async fn wait_for_any_feedback(
    receivers: &mut HashMap<NodeId, mpsc::UnboundedReceiver<FeedbackItem>>,
    timeout: Duration,
    context: &str,
) {
    let result = tokio::time::timeout(timeout, async {
        loop {
            for rx in receivers.values_mut() {
                match rx.try_recv() {
                    Ok(_feedback) => return,
                    Err(mpsc::error::TryRecvError::Empty) => {}
                    Err(mpsc::error::TryRecvError::Disconnected) => {}
                }
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await;

    assert!(
        result.is_ok(),
        "Timed out after {:?} waiting for feedback: {}",
        timeout,
        context
    );
}

/// **Goal**: Verify the driver correctly processes `CLIENT_ROUTES_CHANGE`
/// events injected via the proxy, re-reads routes, reconnects through
/// updated NLB ports, handles cross-DC reroutes, and recovers from
/// malformed events that kill the control connection.
///
/// **Added value**: This is the most comprehensive event-handling test.
/// It exercises the driver's event-driven route refresh path (as opposed
/// to polling) by injecting synthetic CQL events directly into the control
/// connection. It covers: (a) event reception and re-query trigger,
/// (b) full-DC reroute with real route changes, (c) cross-DC reroute,
/// and (d) resilience to malformed events. Without this test, we wouldn't
/// know if the driver's `CLIENT_ROUTES_CHANGE` event handler, CC
/// reconnection logic, and metadata refresh pipeline work end-to-end.
///
/// **Scenario** (2 DCs, 2+2 nodes):
/// 1. **Phase 1** (baseline): queries reach all 4.
/// 2. **Phase 2** (event injection, no route change): inject a
///    `CLIENT_ROUTES_CHANGE` event for DC1 nodes. Verify the driver
///    re-queries `system.client_routes` (detected via EXECUTE body
///    matching). Verify all 4 nodes still work.
/// 3. **Phase 3** (full-DC reroute): restart proxy chains for both DC1
///    nodes (new NLB ports). ScyllaDB emits a real event. Wait for
///    connections, verify all 4 receive queries.
/// 4. **Phase 4** (cross-DC reroute): restart one node from each DC.
///    Same verification.
/// 5. **Phase 5** (malformed event recovery): inject a malformed event
///    (mismatched array lengths). The CC dies, the driver reconnects and
///    performs a full metadata refresh (detected via EXECUTE for
///    `system.client_routes`). Verify all 4 nodes still work.
async fn event_driven_reroute(plc: &mut ClientRoutesCluster) {
    let session = plc
        .make_session_builder()
        .build()
        .await
        .expect("Failed to build client-routes session");

    plc.wait_for_pools_connected(&session, CONNECTION_WAIT_TIMEOUT)
        .await
        .expect("Driver did not connect to all nodes");

    // Identify nodes by DC. In a 2+2 setup, CCM node IDs are 1,2 (DC1)
    // and 3,4 (DC2).
    let all_nodes = plc.active_node_ids();
    assert_eq!(all_nodes.len(), 4, "Expected 4 initial nodes");
    let dc1_nodes: Vec<_> = all_nodes.iter().copied().filter(|&id| id <= 2).collect();
    let dc2_nodes: Vec<_> = all_nodes.iter().copied().filter(|&id| id > 2).collect();
    assert_eq!(dc1_nodes.len(), 2);
    assert_eq!(dc2_nodes.len(), 2);
    info!("DC1 nodes: {:?}, DC2 nodes: {:?}", dc1_nodes, dc2_nodes);

    let requery_timeout = Duration::from_secs(10);

    // ---------------------------------------------------------------
    // Phase 1: Baseline
    // ---------------------------------------------------------------
    info!("=== Phase 1: baseline ===");
    let mut rxs = plc.setup_query_feedback();
    assert_queries_reach_all_nodes(&session, plc, &mut rxs).await;

    // ---------------------------------------------------------------
    // Phase 2: Event injection — verify event processing
    // ---------------------------------------------------------------
    // Inject a CLIENT_ROUTES_CHANGE event for DC1 nodes (no actual route
    // change). The driver should re-query system.client_routes.
    info!(
        "=== Phase 2: event injection for DC1 nodes {:?} ===",
        dc1_nodes
    );
    let mut event_rxs = plc.setup_event_requery_detection();
    let injected = plc.inject_event(&dc1_nodes);
    assert!(injected >= 1, "Phase 2: event was not injected into any CC");

    wait_for_any_feedback(
        &mut event_rxs,
        requery_timeout,
        "Phase 2: driver did not re-query system.client_routes after injected event",
    )
    .await;
    info!("Phase 2: driver re-queried system.client_routes after injected event");

    // Verify all nodes still work after event processing.
    let mut rxs = plc.setup_query_feedback();
    assert_queries_reach_all_nodes(&session, plc, &mut rxs).await;

    // ---------------------------------------------------------------
    // Phase 3: Full-DC reroute (both DC1 nodes)
    // ---------------------------------------------------------------
    // Restart proxy chains for both DC1 nodes. This gives them new NLB
    // ports and posts updated routes. ScyllaDB emits a real
    // CLIENT_ROUTES_CHANGE event, causing the driver to re-read routes
    // and reconnect through the new ports.
    info!(
        "=== Phase 3: full-DC reroute (DC1 nodes {:?}) ===",
        dc1_nodes
    );
    for &node_id in &dc1_nodes {
        plc.restart_node_chain(node_id)
            .await
            .unwrap_or_else(|e| panic!("Failed to restart chain for node {}: {}", node_id, e));
    }

    // Wait for the driver to connect through the new NLB ports.
    plc.wait_for_pools_connected(&session, CONNECTION_WAIT_TIMEOUT)
        .await
        .unwrap_or_else(|e| {
            panic!(
                "Phase 3: driver did not reconnect to all nodes after dc1 chain restarts: {}",
                e
            )
        });

    // Verify traffic reaches all 4 nodes through the new ports.
    let mut rxs = plc.setup_query_feedback();
    assert_queries_reach_all_nodes(&session, plc, &mut rxs).await;

    // ---------------------------------------------------------------
    // Phase 4: Cross-DC reroute (one node from each DC)
    // ---------------------------------------------------------------
    let cross_dc_nodes = [dc1_nodes[0], dc2_nodes[1]];
    info!(
        "=== Phase 4: cross-DC reroute (nodes {:?}) ===",
        cross_dc_nodes
    );

    for &node_id in &cross_dc_nodes {
        plc.restart_node_chain(node_id)
            .await
            .unwrap_or_else(|e| panic!("Failed to restart chain for node {}: {}", node_id, e));
    }

    plc.wait_for_pools_connected(&session, CONNECTION_WAIT_TIMEOUT)
        .await
        .unwrap_or_else(|e| panic!("Phase 4: driver did not reconnect to cross-dc nodes: {}", e));

    let mut rxs = plc.setup_query_feedback();
    assert_queries_reach_all_nodes(&session, plc, &mut rxs).await;

    // ---------------------------------------------------------------
    // Phase 5: Malformed event recovery
    // ---------------------------------------------------------------
    info!("=== Phase 5: malformed event ===");

    // Set up detection for the metadata refresh that happens after the CC
    // reconnects, then inject the malformed event.
    let mut event_rxs = plc.setup_malformed_event_requery_detection();
    let injected = plc.inject_malformed_event();
    assert!(
        injected >= 1,
        "Phase 5: malformed event was not injected into any CC"
    );

    // Wait for the metadata refresh detection (EXECUTE for
    // system.client_routes on the new CC). After the CC breaks and
    // reconnects, the full metadata refresh re-fetches client_routes
    // via EXECUTE.
    // Note: in the current implementation of the control connection,
    // the prepared statement is reused from the shared
    // `ControlConnectionCache`, so no PREPARE is sent on the wire.
    // That's why it's better to check for EXECUTE.
    wait_for_any_feedback(
        &mut event_rxs,
        requery_timeout,
        "Phase 5: driver did not perform full system.client_routes fetch after malformed event",
    )
    .await;

    let mut rxs = plc.setup_query_feedback();
    assert_queries_reach_all_nodes(&session, plc, &mut rxs).await;

    info!("All phases passed — event-driven reroute works correctly");
}

#[tokio::test]
async fn test_client_routes_event_driven_reroute() {
    setup_tracing();
    run_client_routes_test(cluster_2dc_2_2, event_driven_reroute).await;
}
