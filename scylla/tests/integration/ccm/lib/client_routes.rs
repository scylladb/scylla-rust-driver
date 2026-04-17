//! Test helpers for `system.client_routes`-based integration tests.
//!
//! [`ClientRoutesCluster`] wraps a CCM [`Cluster`] and layers on:
//! - Per-node CQL-aware proxy instances (for feedback verification)
//! - Per-node NLBs (one per node, each pointing at that node's proxy)
//! - Host-ID discovery and REST-API route posting with DC-specific connection IDs
//!
//! ## Test chain per node
//!
//! ```text
//! Driver →  Per-node NLB →  Per-node Proxy (1 node) →  Real ScyllaDB
//! ```
//!
//! The proxy provides CQL-level feedback (proves queries reached nodes). Since
//! the driver only knows NLB addresses (from `client_routes`), if a proxy sees
//! CQL traffic, it necessarily went through the NLB. So proxy feedback alone
//! proves all 3 custom routing requirements:
//! 1. The driver opens connections to ALL nodes.
//! 2. The driver can query ALL nodes.
//! 3. The driver connects through NLBs (address translation works).
//!
//! ## Per-node proxy instances
//!
//! Each ScyllaDB node gets its own independent `Proxy` instance (with 1 `Node`).
//! This is required because `RunningProxy` cannot add/remove nodes at runtime
//! (topology is fixed at `Proxy::run()` time). For dynamic topology tests,
//! adding a node = start a new proxy; removing = finish one, without disrupting
//! others.

use std::collections::{BTreeMap, HashMap};
use std::net::{IpAddr, SocketAddr};
use std::panic::AssertUnwindSafe;
use std::time::Duration;

use anyhow::{Context, Error, bail};
use futures::FutureExt;
use futures::StreamExt as _;
use futures::TryStreamExt as _;
use scylla::client::client_routes::{ClientRoutesConfig, ClientRoutesProxy};
use scylla::client::session_builder::ClientRoutesSessionBuilder;
use scylla_proxy::nlb::{NlbFrontend, RunningNlbFrontend};
use scylla_proxy::{
    Condition, Node as ProxyNode, Proxy, Reaction, RequestOpcode, RequestReaction, RequestRule,
    RunningProxy, ShardAwareness, get_exclusive_local_address,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tracing::{debug, info};
use uuid::Uuid;

use super::cluster::{Cluster, ClusterOptions};
use super::node::NodeId;

/// Re-export the feedback frame type used by proxy feedback channels.
pub(crate) type FeedbackItem = (
    scylla_proxy::RequestFrame,
    Option<scylla_proxy::TargetShard>,
);

/// Base name for test connection IDs. Each DC gets `"{BASE}-dc{dc_id}"`.
const CONNECTION_ID_BASE: &str = "rust-driver-test";

// ---------------------------------------------------------------------------
// Per-node proxy chain
// ---------------------------------------------------------------------------

/// A per-node proxy chain: CQL-aware proxy + NLB frontend.
///
/// ```text
/// NLB (nlb_addr) →  Proxy (proxy_addr) →  Real ScyllaDB (real_addr)
/// ```
struct NodeChain {
    /// The running CQL-aware proxy (single-node). Provides feedback channels.
    running_proxy: RunningProxy,
    /// The proxy's listen address (used as backend for both per-node NLB
    /// and per-DC contact-point NLB).
    proxy_addr: SocketAddr,
    /// The NLB frontend that the driver connects to.
    nlb: RunningNlbFrontend,
}

impl NodeChain {
    /// The address that the driver (or contact-point NLB) should connect to.
    fn nlb_addr(&self) -> SocketAddr {
        self.nlb.listen_addr()
    }

    /// The proxy's listen address (used as per-DC NLB backend).
    fn proxy_addr(&self) -> SocketAddr {
        self.proxy_addr
    }
}

/// Start a proxy + NLB chain for a single Scylla node (plaintext).
///
/// 1. Allocate a unique proxy address via `get_exclusive_local_address()`
/// 2. Build and run a single-node `Proxy` (proxy_addr →  real_addr)
/// 3. Build and run an NLB frontend (OS-assigned port →  proxy_addr)
async fn start_node_chain(real_addr: SocketAddr) -> Result<NodeChain, Error> {
    let proxy_ip = get_exclusive_local_address();
    let proxy_addr = SocketAddr::new(proxy_ip, 9042);

    let node = ProxyNode::builder()
        .real_address(real_addr)
        .proxy_address(proxy_addr)
        .shard_awareness(ShardAwareness::QueryNode)
        .build();

    let running_proxy = Proxy::new([node])
        .run()
        .await
        .with_context(|| format!("Failed to start proxy for real node {}", real_addr))?;

    let nlb = NlbFrontend::builder()
        .listen_addr("127.0.0.1:0".parse().unwrap())
        .backend(proxy_addr)
        .build()
        .run()
        .await
        .with_context(|| format!("Failed to start NLB for proxy {}", proxy_addr))?;

    Ok(NodeChain {
        running_proxy,
        proxy_addr,
        nlb,
    })
}

/// Shut down a proxy chain for a node.
///
/// The NLB is shut down first, then the proxy. Proxy errors are expected
/// (the backend may already be dead) and are logged rather than propagated.
async fn shutdown_node_chain(chain: NodeChain, node_id: NodeId) {
    chain.nlb.finish().await;
    if let Err(e) = chain.running_proxy.finish().await {
        info!("Proxy for node {} reported (expected): {}", node_id, e);
    }
}

// ---------------------------------------------------------------------------
// Per-DC configuration
// ---------------------------------------------------------------------------

/// Per-datacenter NLB and route configuration.
struct DcConfig {
    /// Connection ID for this DC (e.g. `"rust-driver-test-dc0"`).
    connection_id: String,
    /// Per-node chains for nodes in this DC, keyed by CCM node ID.
    per_node_chains: HashMap<NodeId, NodeChain>,
    /// Per-DC round-robin NLB for contact points.
    ///
    /// Backends are the per-node proxy addresses.
    contact_point_nlb: RunningNlbFrontend,
}

impl DcConfig {
    /// Returns the per-DC contact-point NLB address.
    fn contact_point_addr(&self) -> SocketAddr {
        self.contact_point_nlb.listen_addr()
    }

    /// Update the per-DC contact-point NLB's backends to reflect the current
    /// set of per-node proxy addresses.
    ///
    /// Call this after any topology change (stop/restart/add/decommission)
    /// that alters which per-node chains are active in this DC.
    fn refresh_contact_point_backends(&self) {
        let backends: Vec<SocketAddr> = self
            .per_node_chains
            .values()
            .map(|chain| chain.proxy_addr())
            .collect();
        info!(
            "DC {} refreshing contact-point NLB backends: {:?}",
            self.connection_id, backends
        );
        self.contact_point_nlb.set_backends(backends);
    }
}

// ---------------------------------------------------------------------------
// ClientRoutesCluster
// ---------------------------------------------------------------------------

/// A CCM cluster wrapped with per-node proxy chains and `system.client_routes`
/// route management.
///
/// Provides everything needed to build a [`ClientRoutesSessionBuilder`] that
/// connects through simulated NLBs with CQL-level feedback verification.
pub(crate) struct ClientRoutesCluster {
    /// The underlying CCM cluster.
    cluster: Cluster,
    /// Per-DC configuration, keyed by datacenter_id (0-based).
    dc_configs: BTreeMap<u16, DcConfig>,
    /// Mapping from CCM node ID to its Scylla host_id UUID.
    host_ids: HashMap<NodeId, Uuid>,
}

impl ClientRoutesCluster {
    /// Build the cluster, discover host IDs, start proxy chains, and post routes.
    ///
    /// Returns `Ok(None)` if the cluster does not support `system.client_routes`
    /// (e.g., the ScyllaDB version is too old). The caller should skip the test
    /// in that case.
    ///
    /// Full lifecycle:
    /// 1. CCM cluster create + init
    /// 2. CCM cluster start
    /// 3. Check `system.client_routes` table existence (skip if absent)
    /// 4. Discover each node's `host_id` via a direct CQL connection
    /// 5. Start per-node proxy chains
    /// 6. POST client routes via Scylla's REST API on each node
    async fn setup(opts: ClusterOptions) -> Result<Option<Self>, Error> {
        let mut cluster = Cluster::new(opts)
            .await
            .context("Failed to create cluster")?;
        cluster
            .init()
            .await
            .inspect_err(|_| cluster.mark_as_failed())
            .context("Failed to init cluster")?;

        cluster
            .start(None)
            .await
            .inspect_err(|_| cluster.mark_as_failed())
            .context("Failed to start cluster")?;

        // Step 3: check that `system.client_routes` exists.
        if !client_routes_table_exists(&cluster).await? {
            info!(
                "system.client_routes table not found — \
                 this ScyllaDB version does not support client routes, skipping test"
            );
            return Ok(None);
        }

        // Step 4: discover host IDs (uses plaintext port 9042).
        info!("Starting host ID discovery. Starting a driver Session for that...");
        let host_ids = discover_host_ids(&cluster)
            .await
            .inspect_err(|_| cluster.mark_as_failed())
            .context("Failed to discover host IDs")?;
        info!("Discovered host IDs: {:?}", host_ids);

        // Step 4: start per-node proxy chains.
        info!("Starting per-node proxy chains");
        let dc_configs = build_dc_configs(&cluster)
            .await
            .inspect_err(|_| cluster.mark_as_failed())
            .context("Failed to build DC configs / start proxy chains")?;

        for (dc_id, dc_cfg) in &dc_configs {
            info!(
                "DC {}: connection_id={}, contact={}, {} per-node chains",
                dc_id,
                dc_cfg.connection_id,
                dc_cfg.contact_point_addr(),
                dc_cfg.per_node_chains.len(),
            );
        }

        let mut plc = ClientRoutesCluster {
            cluster,
            dc_configs,
            host_ids,
        };

        // Step 5: POST client routes to all nodes.
        info!("POSTing client routes to all nodes");
        plc.post_routes_to_all_nodes()
            .await
            .inspect_err(|_| plc.cluster.mark_as_failed())
            .context("Failed to post client routes")?;

        info!("Finished POSTing client routes to all nodes");

        Ok(Some(plc))
    }

    /// Build a [`ClientRoutesSessionBuilder`] configured to connect through the NLBs.
    ///
    /// Creates one [`ClientRoutesProxy`] per DC (each with its DC-specific
    /// connection ID) and uses the per-DC contact-point NLB as the contact point.
    pub(crate) fn make_session_builder(&self) -> ClientRoutesSessionBuilder {
        let (proxies, contact_points): (Vec<_>, Vec<_>) = self
            .dc_configs
            .values()
            .map(|dc_cfg| {
                let proxy = ClientRoutesProxy::new_with_connection_id(dc_cfg.connection_id.clone())
                    .with_overridden_hostname("127.0.0.1".to_string());
                let contact_point = dc_cfg.contact_point_addr().to_string();
                (proxy, contact_point)
            })
            .unzip();

        let config = ClientRoutesConfig::new(proxies).expect("valid config");
        ClientRoutesSessionBuilder::new(config).known_nodes(contact_points)
    }

    /// Set up QUERY feedback rules on proxy nodes.
    ///
    /// Returns a map from `NodeId` to the feedback receiver for that node.
    /// Each call replaces previous rules/senders, so new queries go to new
    /// channels. This allows "fresh" counting per test phase.
    ///
    /// The condition matches only user QUERY frames, excluding control
    /// connection traffic (REGISTER-ed connections).
    pub(crate) fn setup_query_feedback(
        &mut self,
    ) -> HashMap<NodeId, mpsc::UnboundedReceiver<FeedbackItem>> {
        let mut receivers = HashMap::new();

        for dc_cfg in self.dc_configs.values_mut() {
            for (&node_id, chain) in dc_cfg.per_node_chains.iter_mut() {
                let (tx, rx) = mpsc::unbounded_channel();

                let condition = Condition::RequestOpcode(RequestOpcode::Query)
                    .and(Condition::not(Condition::ConnectionRegisteredAnyEvent));
                let rule = RequestRule(
                    condition,
                    RequestReaction::noop().with_feedback_when_performed(tx),
                );

                // Each proxy has exactly 1 node (index 0).
                chain.running_proxy.running_nodes[0].change_request_rules(Some(vec![rule]));

                receivers.insert(node_id, rx);
            }
        }

        receivers
    }

    pub(crate) fn cluster_mut(&mut self) -> &mut Cluster {
        &mut self.cluster
    }

    /// Returns the mapping from CCM node ID to Scylla host_id.
    #[expect(dead_code)]
    pub(crate) fn host_ids(&self) -> &HashMap<NodeId, Uuid> {
        &self.host_ids
    }

    /// Returns all active node IDs across all DCs.
    pub(crate) fn active_node_ids(&self) -> Vec<NodeId> {
        self.dc_configs
            .values()
            .flat_map(|dc| dc.per_node_chains.keys().copied())
            .collect()
    }

    /// Waits until every active proxy node has at least one driver connection.
    ///
    /// After topology changes (restart, add node), the driver takes time to
    /// discover the new/restarted node and open a connection. Calling this
    /// before issuing queries prevents races where all queries miss the
    /// new node because the driver hasn't connected yet.
    ///
    /// Times out after `timeout` to avoid hanging forever if the driver fails to connect.
    pub(crate) async fn wait_for_connections_to_all_nodes(
        &self,
        timeout: Duration,
    ) -> Result<(), Error> {
        let futs: Vec<_> = self
            .dc_configs
            .values()
            .flat_map(|dc| {
                dc.per_node_chains.iter().map(move |(&node_id, chain)| {
                    let proxy = &chain.running_proxy;
                    async move {
                        proxy.wait_for_connection().await;
                        info!("Proxy for node {} has a driver connection", node_id);
                    }
                })
            })
            .collect();

        tokio::time::timeout(timeout, futures::future::join_all(futs))
            .await
            .map_err(|_| {
                anyhow::anyhow!(
                    "Timed out waiting for driver connections to all proxy nodes. \
                     Active node IDs: {:?}",
                    self.active_node_ids()
                )
            })?;

        Ok(())
    }

    /// Waits until the proxy responsible for given node has at least one driver
    /// connection.
    ///
    /// After topology changes (restart, add node), the driver takes time to
    /// discover the new/restarted node and open a connection. Calling this
    /// before issuing queries prevents races where all queries miss the
    /// new node because the driver hasn't connected yet.
    ///
    /// Times out after `timeout` to avoid hanging forever if the driver fails to connect.
    pub(crate) async fn wait_for_connections_to_node(
        &self,
        node_id: NodeId,
        timeout: Duration,
    ) -> Result<(), Error> {
        let fut = self
            .dc_configs
            .values()
            .find_map(|dc| {
                dc.per_node_chains.get(&node_id).map(|chain| {
                    let proxy = &chain.running_proxy;
                    async move {
                        proxy.wait_for_connection().await;
                        info!("Proxy for node {} has a driver connection", node_id);
                    }
                })
            })
            .unwrap_or_else(|| panic!("Node {} not present in the DcConfigs", node_id));

        tokio::time::timeout(timeout, fut).await.map_err(|_| {
            anyhow::anyhow!(
                "Timed out waiting for driver connection to node {}. \
                     Active node IDs: {:?}",
                node_id,
                self.active_node_ids()
            )
        })?;

        Ok(())
    }

    async fn post_routes_to_all_nodes(&self) -> Result<(), Error> {
        let routes = self.build_route_entries();
        let route_json = serde_json_routes(&routes);
        info!("Posting client routes: {}", route_json);

        // Collect the set of node IDs that have active proxy chains.
        let active_ids: std::collections::HashSet<NodeId> = self
            .dc_configs
            .values()
            .flat_map(|dc| dc.per_node_chains.keys().copied())
            .collect();

        for node in self.cluster.nodes().iter() {
            if !active_ids.contains(&node.id()) {
                debug!(
                    "Skipping route POST to node {} ({}) — no active chain",
                    node.id(),
                    node.broadcast_rpc_address()
                );
                continue;
            }
            let node_ip = node.broadcast_rpc_address();
            post_client_routes_raw(node_ip, &route_json)
                .await
                .with_context(|| {
                    format!(
                        "Failed to POST client routes to node {} ({})",
                        node.id(),
                        node_ip
                    )
                })?;
        }

        Ok(())
    }

    /// Build the JSON-serializable route entries from current state.
    ///
    /// Each node's route uses the connection ID of its DC. The `port` field
    /// points to the NLB. `tls_port` is set to the same value as `port`
    /// because the REST API rejects port 0; the driver never uses TLS in
    /// these tests, so the value is irrelevant.
    fn build_route_entries(&self) -> Vec<RouteEntry> {
        let mut entries = Vec::new();

        for (dc_id, dc_cfg) in &self.dc_configs {
            for (&node_id, chain) in &dc_cfg.per_node_chains {
                if let Some(&host_id) = self.host_ids.get(&node_id) {
                    let nlb = chain.nlb_addr();
                    entries.push(RouteEntry {
                        connection_id: dc_cfg.connection_id.clone(),
                        host_id,
                        address: nlb.ip().to_string(),
                        port: nlb.port(),
                        // For now, both are at the same port. TLS is anyway not tested, as not yet
                        // supported. Once it's supported, this needs to be adjusted to a different port
                        // than plaintext.
                        tls_port: nlb.port(),
                    });
                } else {
                    debug!(
                        "Skipping route for node {} in DC {} — no host_id",
                        node_id, dc_id
                    );
                }
            }
        }

        entries
    }

    /// Shut down all proxy chains and per-DC contact-point NLBs.
    async fn shutdown_all(self) {
        for (dc_id, dc_cfg) in self.dc_configs {
            debug!("Shutting down per-DC contact-point NLB for DC {}", dc_id);
            dc_cfg.contact_point_nlb.finish().await;

            for (node_id, chain) in dc_cfg.per_node_chains {
                debug!("Shutting down chain for DC {} node {}", dc_id, node_id);
                shutdown_node_chain(chain, node_id).await;
            }
        }
        info!("All proxy chains and NLBs shut down");
    }
}

// ---------------------------------------------------------------------------
// DC config builder
// ---------------------------------------------------------------------------

/// Group cluster nodes by DC and start per-node proxy chains + per-DC
/// contact-point NLBs.
///
/// For each DC:
/// - One proxy chain per node
/// - One per-DC round-robin NLB (used as contact point)
/// - A connection ID of the form `"rust-driver-test-dc{dc_id}"`
///
/// ```text
/// Driver →  Per-DC NLB →  Proxy →  ScyllaDB :9042
/// ```
async fn build_dc_configs(cluster: &Cluster) -> Result<BTreeMap<u16, DcConfig>, Error> {
    // Group nodes by DC: DcId -> {(NodeId, IpAddr)}.
    let dc_nodes: BTreeMap<u16, Vec<(NodeId, IpAddr)>> =
        cluster
            .nodes()
            .iter()
            .fold(BTreeMap::new(), |mut acc, node| {
                acc.entry(node.datacenter_id())
                    .or_default()
                    .push((node.id(), node.broadcast_rpc_address()));
                acc
            });

    futures::stream::iter(dc_nodes)
        .then(|(dc_id, nodes)| async move {
            let connection_id = format!("{}-dc{}", CONNECTION_ID_BASE, dc_id);

            // Start per-node proxy chains.
            let mut per_node_chains = HashMap::new();
            for (node_id, real_ip) in &nodes {
                let chain = start_node_chain(SocketAddr::new(*real_ip, 9042))
                    .await
                    .with_context(|| {
                        format!("Failed to start chain for DC {} node {}", dc_id, node_id)
                    })?;
                per_node_chains.insert(*node_id, chain);
            }

            // Start per-DC round-robin NLB.
            let backends: Vec<SocketAddr> = per_node_chains
                .values()
                .map(|chain| chain.proxy_addr())
                .collect();

            let contact_point_nlb = NlbFrontend::builder()
                .listen_addr("127.0.0.1:0".parse().unwrap())
                .backends(backends.iter().copied())
                .build()
                .run()
                .await
                .with_context(|| {
                    format!("Failed to start per-DC contact-point NLB for DC {}", dc_id)
                })?;

            info!(
                "DC {} contact-point NLB: {} →  {:?}",
                dc_id,
                contact_point_nlb.listen_addr(),
                backends,
            );

            Ok::<_, Error>((
                dc_id,
                DcConfig {
                    connection_id,
                    per_node_chains,
                    contact_point_nlb,
                },
            ))
        })
        .try_collect()
        .await
}

// ---------------------------------------------------------------------------
// Feature detection
// ---------------------------------------------------------------------------

/// Check whether the `system.client_routes` table exists on this cluster.
///
/// Opens a short-lived session to the first node and queries
/// `system_schema.tables`. Returns `false` if the table is absent
/// (ScyllaDB version too old), `Err` on connection/query failure.
async fn client_routes_table_exists(cluster: &Cluster) -> Result<bool, Error> {
    use scylla::client::session_builder::SessionBuilder;

    let first_node = cluster
        .nodes()
        .iter()
        .next()
        .context("Cluster has no nodes")?;
    let contact = SocketAddr::new(
        first_node.broadcast_rpc_address(),
        first_node.native_transport_port(),
    );
    let session = SessionBuilder::new()
        .known_node(contact.to_string())
        .build()
        .await
        .with_context(|| format!("Failed to connect to cluster via {}", contact))?;

    let result = session
        .query_unpaged(
            "SELECT table_name FROM system_schema.tables \
             WHERE keyspace_name = 'system' AND table_name = 'client_routes'",
            &[],
        )
        .await
        .context("Failed to query system_schema.tables")?;

    let rows = result.into_rows_result().context("Expected rows result")?;
    Ok(rows.rows_num() > 0)
}

// ---------------------------------------------------------------------------
// Host-ID discovery
// ---------------------------------------------------------------------------

/// Uses the driver's cluster metadata (via `Session::get_cluster_state()`) to
/// reliably discover host_ids for all CCM nodes.
///
/// This avoids querying `system.local` / `system.peers` directly, which is
/// unreliable because queries go through the load balancer and `system.local`
/// returns the *queried* node's data (not necessarily the contact point's).
/// The Session already discovers all nodes during initialization, so we can
/// simply read the metadata it has collected.
async fn discover_host_ids(cluster: &Cluster) -> Result<HashMap<NodeId, Uuid>, Error> {
    use scylla::client::session_builder::SessionBuilder;

    // Connect to the first node — the session will discover the whole cluster.
    let first_node = cluster
        .nodes()
        .iter()
        .next()
        .context("Cluster has no nodes")?;
    let contact = SocketAddr::new(
        first_node.broadcast_rpc_address(),
        first_node.native_transport_port(),
    );
    let session = SessionBuilder::new()
        .known_node(contact.to_string())
        .build()
        .await
        .with_context(|| format!("Failed to connect to cluster via {}", contact))?;

    // Read the cluster metadata that the session has already collected.
    let state = session.get_cluster_state();
    let nodes_info = state.get_nodes_info();

    // Build a map: IP address →  host_id from the driver's metadata.
    let addr_to_host_id: HashMap<IpAddr, Uuid> = nodes_info
        .iter()
        .map(|node| (node.address.ip(), node.host_id))
        .collect();

    debug!(
        "Cluster metadata address →  host_id map ({} nodes): {:?}",
        addr_to_host_id.len(),
        addr_to_host_id
    );

    // Match CCM nodes by their broadcast_rpc_address.
    let mut host_ids = HashMap::new();
    for node in cluster.nodes().iter() {
        let rpc_ip = node.broadcast_rpc_address();
        let host_id = addr_to_host_id.get(&rpc_ip).with_context(|| {
            format!(
                "Node {} ({}) not found in driver cluster metadata — cluster may not be fully up. \
                 Known addresses: {:?}",
                node.id(),
                rpc_ip,
                addr_to_host_id.keys().collect::<Vec<_>>()
            )
        })?;
        host_ids.insert(node.id(), *host_id);
        debug!("Node {} ({}): host_id = {}", node.id(), rpc_ip, host_id);
    }

    Ok(host_ids)
}

/// Connect to a single Scylla node and retrieve its `host_id`.
///
/// Uses the driver's cluster metadata (same approach as [`discover_host_ids`]).
/// Retries with backoff because a newly-added CCM node may not be ready
/// immediately after `node.start()`.
async fn discover_single_host_id(addr: SocketAddr) -> Result<Uuid, Error> {
    use scylla::client::session_builder::SessionBuilder;

    let max_attempts = 10;
    let mut last_err = None;

    for attempt in 1..=max_attempts {
        match SessionBuilder::new()
            .known_node(addr.to_string())
            .build()
            .await
        {
            Ok(session) => {
                let state = session.get_cluster_state();
                let nodes_info = state.get_nodes_info();
                // Find the node matching our address.
                if let Some(node) = nodes_info.iter().find(|n| n.address.ip() == addr.ip()) {
                    return Ok(node.host_id);
                }
                // If the target node isn't in metadata, fall back to looking
                // for any single node (the session only knows one contact point).
                if nodes_info.len() == 1 {
                    return Ok(nodes_info[0].host_id);
                }
                last_err = Some(anyhow::anyhow!(
                    "Node {} not found in session metadata ({} nodes known)",
                    addr,
                    nodes_info.len()
                ));
            }
            Err(e) => {
                last_err = Some(anyhow::anyhow!("Attempt {}: {}", attempt, e));
            }
        }

        if attempt < max_attempts {
            info!(
                "discover_single_host_id: attempt {}/{} for {} failed, retrying...",
                attempt, max_attempts, addr
            );
            tokio::time::sleep(Duration::from_secs(2)).await;
        }
    }

    Err(last_err.unwrap_or_else(|| anyhow::anyhow!("No attempts made")))
}

// ---------------------------------------------------------------------------
// Feedback drain helpers
// ---------------------------------------------------------------------------

/// Drain all feedback channels and return per-node counts and total count.
///
/// This is non-blocking: it drains whatever is currently buffered in each
/// channel. Call this after all queries have completed and a short delay.
pub(crate) fn drain_feedback(
    receivers: &mut HashMap<NodeId, mpsc::UnboundedReceiver<FeedbackItem>>,
) -> (HashMap<NodeId, usize>, usize) {
    let mut per_node = HashMap::new();
    let mut total = 0usize;

    for (&node_id, rx) in receivers.iter_mut() {
        let mut count = 0usize;
        while let Ok(_feedback) = rx.try_recv() {
            count += 1;
        }
        per_node.insert(node_id, count);
        total += count;
    }

    (per_node, total)
}

// ---------------------------------------------------------------------------
// Route posting (raw HTTP)
// ---------------------------------------------------------------------------

/// A single route entry for the REST API JSON payload.
struct RouteEntry {
    connection_id: String,
    host_id: Uuid,
    address: String,
    port: u16,
    tls_port: u16,
}

/// Serialize route entries to JSON without pulling in serde_json.
fn serde_json_routes(routes: &[RouteEntry]) -> String {
    let entries: Vec<String> = routes
        .iter()
        .map(|r| {
            format!(
                r#"{{"connection_id":"{}","host_id":"{}","address":"{}","port":{},"tls_port":{}}}"#,
                r.connection_id, r.host_id, r.address, r.port, r.tls_port
            )
        })
        .collect();
    format!("[{}]", entries.join(","))
}

/// POST client routes to a single Scylla node via its REST API.
///
/// Uses raw HTTP/1.1 over TCP to avoid adding an HTTP client dependency.
/// Retries up to `MAX_POST_ATTEMPTS` times with backoff because a freshly
/// started node's REST API may not be ready immediately (returns HTTP 500).
async fn post_client_routes_raw(node_ip: IpAddr, json_body: &str) -> Result<(), Error> {
    const MAX_POST_ATTEMPTS: u32 = 10;
    const RETRY_DELAY: Duration = Duration::from_secs(2);

    let api_addr = SocketAddr::new(node_ip, 10000);
    let mut last_err = None;

    for attempt in 1..=MAX_POST_ATTEMPTS {
        match post_client_routes_raw_once(api_addr, node_ip, json_body).await {
            Ok(()) => return Ok(()),
            Err(e) => {
                info!(
                    "POST routes to {} attempt {}/{} failed: {}",
                    api_addr, attempt, MAX_POST_ATTEMPTS, e
                );
                last_err = Some(e);
                if attempt < MAX_POST_ATTEMPTS {
                    tokio::time::sleep(RETRY_DELAY).await;
                }
            }
        }
    }

    Err(last_err.unwrap_or_else(|| anyhow::anyhow!("No POST attempts made")))
}

/// Single attempt to POST client routes. Called by [`post_client_routes_raw`].
async fn post_client_routes_raw_once(
    api_addr: SocketAddr,
    node_ip: IpAddr,
    json_body: &str,
) -> Result<(), Error> {
    let mut stream = TcpStream::connect(api_addr)
        .await
        .with_context(|| format!("Failed to connect to REST API at {}", api_addr))?;

    let request = format!(
        "POST /v2/client-routes HTTP/1.1\r\n\
         Host: {}:{}\r\n\
         Content-Type: application/json\r\n\
         Content-Length: {}\r\n\
         Connection: close\r\n\
         \r\n\
         {}",
        node_ip,
        10000,
        json_body.len(),
        json_body
    );

    stream
        .write_all(request.as_bytes())
        .await
        .context("Failed to send HTTP request")?;

    let mut response = Vec::new();
    stream
        .read_to_end(&mut response)
        .await
        .context("Failed to read HTTP response")?;
    let response_str = String::from_utf8_lossy(&response);

    let status_line = response_str.lines().next().unwrap_or("(empty response)");
    debug!("REST API response from {}: {}", api_addr, status_line);

    if !status_line.contains("200") && !status_line.contains("201") {
        bail!(
            "REST API at {} returned unexpected status: {}\nFull response:\n{}",
            api_addr,
            status_line,
            response_str
        );
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// Test runner
// ---------------------------------------------------------------------------

/// Run a `system.client_routes`-based integration test.
///
/// This is the client-routes equivalent of [`super::run_ccm_test`]. It:
/// 1. Creates and starts a CCM cluster with the given options
/// 2. Discovers host IDs, starts per-node proxy chains, and posts client routes
/// 3. Runs the test body with a [`ClientRoutesCluster`]
/// 4. Shuts down all proxy chains and NLBs, cleans up the cluster
pub(crate) async fn run_client_routes_test<C, T>(make_cluster_options: C, test_body: T)
where
    C: FnOnce() -> ClusterOptions,
    T: AsyncFnOnce(&mut ClientRoutesCluster) -> (),
{
    let opts = make_cluster_options();
    let Some(mut plc) = ClientRoutesCluster::setup(opts)
        .await
        .expect("Failed to set up client-routes test cluster")
    else {
        info!("Skipping client-routes test (unsupported by this cluster version)");
        return;
    };

    let result = AssertUnwindSafe(test_body(&mut plc)).catch_unwind().await;

    let cluster_failed = result.is_err();
    if cluster_failed {
        plc.cluster.mark_as_failed();
    }

    // Shut down proxy chains + NLBs first, then let Cluster's Drop handle CCM.
    plc.shutdown_all().await;

    if let Err(err) = result {
        std::panic::resume_unwind(err);
    }
}
