use crate::client::session::TABLET_CHANNEL_SIZE;
use crate::cluster::{KnownNode, Node};
use crate::errors::{MetadataError, NewSessionError, RequestAttemptError, UseKeyspaceError};
use crate::frame::response::event::Event;
use crate::network::{ConnectivityChangeEvent, PoolConfig, VerifiedKeyspaceName};
#[cfg(feature = "metrics")]
use crate::observability::metrics::Metrics;
use crate::policies::host_filter::HostFilter;
use crate::policies::host_listener::{HostEvent, HostEventContext, HostListener};
use crate::routing::locator::tablets::{RawTablet, TabletsInfo};

use arc_swap::ArcSwap;
use futures::future::join_all;
use futures::{FutureExt, future::RemoteHandle};
use scylla_cql::frame::response::result::TableSpec;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tracing::{debug, error, info, trace};
use uuid::Uuid;

use super::metadata::MetadataReader;
use super::state::{ClusterState, ClusterStateNeatDebug};

/// Cluster manages up to date information and connections to database nodes.
/// All state can be accessed by cloning Arc<ClusterState> in the `state` field
pub(crate) struct Cluster {
    // `ArcSwap<ClusterState>` is wrapped in `Arc` to support sharing cluster state
    // between `Cluster` and `ClusterWorker`
    state: Arc<ArcSwap<ClusterState>>,

    refresh_channel: tokio::sync::mpsc::Sender<RefreshRequest>,
    use_keyspace_channel: tokio::sync::mpsc::Sender<UseKeyspaceRequest>,

    _worker_handle: RemoteHandle<()>,
}

/// Enables printing [Cluster] struct in a neat way, by skipping the rather useless
/// print of channels state and printing [ClusterState] neatly.
pub(crate) struct ClusterNeatDebug<'a>(pub(crate) &'a Cluster);
impl std::fmt::Debug for ClusterNeatDebug<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let cluster = self.0;
        f.debug_struct("Cluster")
            .field("data", &ClusterStateNeatDebug(&cluster.state.load()))
            .finish_non_exhaustive()
    }
}

/// Used to track node status changes, i.e. whether a node is reachable or not.
///
/// Mainly used to deduplicate [ConnectivityChangeEvent]s received from `PoolRefiller`
/// before notifying [HostListener] about node status changes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum NodeConnectivityStatus {
    Connected,
    Unreachable,
}

// Works in the background to keep the cluster updated
struct ClusterWorker {
    // Cluster state to keep updated:
    cluster_state: Arc<ArcSwap<ClusterState>>,

    /// Node status map.
    ///
    /// Maps host_id to connectivity status.
    /// Used to track node status in order to deduplicate HostListener events.
    node_status: HashMap<Uuid, NodeConnectivityStatus>,

    // Cluster connections
    metadata_reader: MetadataReader,
    pool_config: PoolConfig,

    // To listen for refresh requests
    refresh_channel: tokio::sync::mpsc::Receiver<RefreshRequest>,

    // Channel used to receive use keyspace requests
    use_keyspace_channel: tokio::sync::mpsc::Receiver<UseKeyspaceRequest>,

    // Channel used to receive server events
    server_events_channel: tokio::sync::mpsc::Receiver<Event>,

    // Channel used to receive signals that node is no longer reachable or became reachable.
    connectivity_events_receiver: tokio::sync::mpsc::UnboundedReceiver<ConnectivityChangeEvent>,
    // Sender part of that channel to pass to `PoolRefiller`s.
    connectivity_events_sender: tokio::sync::mpsc::UnboundedSender<ConnectivityChangeEvent>,

    // Channel used to receive signals that control connection is broken
    control_connection_repair_channel: tokio::sync::mpsc::Receiver<()>,

    // Channel used to receive info about new tablets from custom payload in responses
    // sent by server.
    tablets_channel: tokio::sync::mpsc::Receiver<(TableSpec<'static>, RawTablet)>,

    // Keyspace send in "USE <keyspace name>" when opening each connection
    used_keyspace: Option<VerifiedKeyspaceName>,

    // The host filter determines towards which nodes we should open
    // connections
    host_filter: Option<Arc<dyn HostFilter>>,

    // The host listener allows to listen for topology and node status changes.
    host_listener: Option<Arc<dyn HostListener>>,

    // This value determines how frequently the cluster
    // worker will refresh the cluster metadata
    cluster_metadata_refresh_interval: Duration,

    #[cfg(feature = "metrics")]
    metrics: Arc<Metrics>,
}

#[derive(Debug)]
struct RefreshRequest {
    response_chan: tokio::sync::oneshot::Sender<Result<(), MetadataError>>,
}

#[derive(Debug)]
struct UseKeyspaceRequest {
    keyspace_name: VerifiedKeyspaceName,
    response_chan: tokio::sync::oneshot::Sender<Result<(), UseKeyspaceError>>,
}

impl Cluster {
    #[expect(clippy::too_many_arguments)]
    pub(crate) async fn new(
        known_nodes: Vec<KnownNode>,
        pool_config: PoolConfig,
        keyspaces_to_fetch: Vec<String>,
        fetch_schema_metadata: bool,
        metadata_request_serverside_timeout: Option<Duration>,
        hostname_resolution_timeout: Option<Duration>,
        host_filter: Option<Arc<dyn HostFilter>>,
        host_listener: Option<Arc<dyn HostListener>>,
        cluster_metadata_refresh_interval: Duration,
        tablet_receiver: tokio::sync::mpsc::Receiver<(TableSpec<'static>, RawTablet)>,
        #[cfg(feature = "metrics")] metrics: Arc<Metrics>,
    ) -> Result<Cluster, NewSessionError> {
        let (refresh_sender, refresh_receiver) = tokio::sync::mpsc::channel(32);
        let (use_keyspace_sender, use_keyspace_receiver) = tokio::sync::mpsc::channel(32);
        let (server_events_sender, server_events_receiver) = tokio::sync::mpsc::channel(32);
        // This is unbounded, because there is possibility that many events will be sent quickly,
        // for example when driver is connected to a large cluster and it loses network connectivity.
        //
        // If the channel were bounded, then we would either block PoolRefillers (if we decide to send blockingly)
        // or drop events (if we decide to do so if the channel is full). Both options are bad.
        let (connectivity_events_sender, connectivity_events_receiver) =
            tokio::sync::mpsc::unbounded_channel();
        let (control_connection_repair_sender, control_connection_repair_receiver) =
            tokio::sync::mpsc::channel(32);

        let mut metadata_reader = MetadataReader::new(
            known_nodes,
            hostname_resolution_timeout,
            control_connection_repair_sender,
            pool_config.connection_config.clone(),
            metadata_request_serverside_timeout,
            server_events_sender,
            keyspaces_to_fetch,
            fetch_schema_metadata,
            &host_filter,
            #[cfg(feature = "metrics")]
            Arc::clone(&metrics),
            Arc::clone(&pool_config.reconnect_policy),
        )
        .await?;

        let mut node_status = HashMap::new();

        let metadata = metadata_reader.read_metadata(true).await?;
        let cluster_state = ClusterState::new(
            metadata,
            &pool_config,
            &HashMap::new(),
            &mut |old_nodes, new_nodes| {
                ClusterWorker::handle_topology_changes(
                    old_nodes,
                    new_nodes,
                    host_listener.as_deref(),
                    &mut node_status,
                )
            },
            &None,
            host_filter.as_deref(),
            &connectivity_events_sender,
            TabletsInfo::new(),
            &HashMap::new(),
            #[cfg(feature = "metrics")]
            &metrics,
        )
        .await;
        cluster_state.wait_until_all_pools_are_initialized().await;

        let cluster_state: Arc<ArcSwap<ClusterState>> =
            Arc::new(ArcSwap::from(Arc::new(cluster_state)));

        let worker = ClusterWorker {
            cluster_state: cluster_state.clone(),
            node_status,

            metadata_reader,
            pool_config,

            refresh_channel: refresh_receiver,
            server_events_channel: server_events_receiver,
            connectivity_events_sender,
            connectivity_events_receiver,
            control_connection_repair_channel: control_connection_repair_receiver,
            tablets_channel: tablet_receiver,

            use_keyspace_channel: use_keyspace_receiver,
            used_keyspace: None,

            host_filter,
            host_listener,
            cluster_metadata_refresh_interval,

            #[cfg(feature = "metrics")]
            metrics,
        };

        let (fut, worker_handle) = worker.work().remote_handle();
        tokio::spawn(fut);

        let result = Cluster {
            state: cluster_state,
            refresh_channel: refresh_sender,
            use_keyspace_channel: use_keyspace_sender,
            _worker_handle: worker_handle,
        };

        Ok(result)
    }

    pub(crate) fn get_state(&self) -> Arc<ClusterState> {
        self.state.load_full()
    }

    pub(crate) async fn refresh_metadata(&self) -> Result<(), MetadataError> {
        let (response_sender, response_receiver) = tokio::sync::oneshot::channel();

        self.refresh_channel
            .send(RefreshRequest {
                response_chan: response_sender,
            })
            .await
            .expect("Bug in Cluster::refresh_metadata sending");
        // Other end of this channel is in ClusterWorker, can't be dropped while we have &self to Cluster with _worker_handle

        response_receiver
            .await
            .expect("Bug in Cluster::refresh_metadata receiving")
        // ClusterWorker always responds
    }

    pub(crate) async fn use_keyspace(
        &self,
        keyspace_name: VerifiedKeyspaceName,
    ) -> Result<(), UseKeyspaceError> {
        let (response_sender, response_receiver) = tokio::sync::oneshot::channel();

        self.use_keyspace_channel
            .send(UseKeyspaceRequest {
                keyspace_name,
                response_chan: response_sender,
            })
            .await
            .expect("Bug in Cluster::use_keyspace sending");
        // Other end of this channel is in ClusterWorkers, can't be dropped while we have &self to Cluster with _worker_handle

        response_receiver.await.unwrap() // ClusterWorker always responds
    }
}

impl ClusterWorker {
    pub(crate) async fn work(mut self) {
        use tokio::time::Instant;

        let control_connection_repair_duration = Duration::from_secs(1); // Attempt control connection repair every second
        let mut last_refresh_time = Instant::now();
        let mut control_connection_works = true;

        loop {
            let mut cur_request: Option<RefreshRequest> = None;

            // Wait until it's time for the next refresh
            let sleep_until: Instant = last_refresh_time
                .checked_add(if control_connection_works {
                    self.cluster_metadata_refresh_interval
                } else {
                    control_connection_repair_duration
                })
                .unwrap_or_else(Instant::now);

            let mut tablets = Vec::new();

            let sleep_future = tokio::time::sleep_until(sleep_until);
            tokio::pin!(sleep_future);

            tokio::select! {
                _sleep_finished = sleep_future => {
                    // Time to do periodic refresh.
                },

                maybe_refresh_request = self.refresh_channel.recv() => {
                    match maybe_refresh_request {
                        Some(request) => cur_request = Some(request),
                        None => return, // If refresh_channel was closed then cluster was dropped, we can stop working
                    }
                }

                tablets_count = self.tablets_channel.recv_many(&mut tablets, TABLET_CHANNEL_SIZE) => {
                    tracing::trace!("Performing tablets update - received {} tablets", tablets_count);
                    if tablets_count == 0 {
                        // If the channel was closed then the cluster was dropped, we can stop working
                        return;
                    }
                    // The current tablet implementation collects tablet feedback in a channel
                    // and then clones the whole ClusterState, updates it with new tablets and replaces
                    // the old ClusterState - this update procedure happens below.
                    // This fits the general model of how ClusterState is handled in the driver:
                    // - ClusterState remains a "simple" struct - without locks etc (apart from Node).
                    // - Topology information update is similar to tablet update - it creates a new ClusterState
                    //   and replaces the old one.
                    // The disadvantage is that we need to have 2 copies of ClusterState, but this happens
                    // anyway during topology update.
                    //
                    // An alternative solution would be to use some synchronization primitives to update tablet info
                    // in place. This solution avoids ClusterState cloning but:
                    // - ClusterState would be much more complicated
                    // - Requires using locks in hot path (when sending request)
                    // - Makes maintenance (which happens during topology update) more complicated and error-prone.
                    //
                    // I decided to stick with the approach that fits with the driver.
                    // Apart from the reasons above, it is much easier to reason about concurrency etc
                    // when reading the code in other parts of the driver.
                    let mut new_cluster_state: ClusterState = self.cluster_state.load().as_ref().clone();
                    new_cluster_state.update_tablets(tablets);
                    self.update_cluster_state(Arc::new(new_cluster_state));

                    continue;
                }

                maybe_cql_event = self.server_events_channel.recv() => {
                    if let Some(event) = maybe_cql_event {
                        debug!("Received server event: {:?}", event);

                        match event {
                            Event::TopologyChange(_) => (), // Refresh immediately
                            Event::StatusChange(_status) => {
                                // TODO: Tracking status using events is unreliable because of
                                // the possibility of losing events when control connection is broken.
                                // Maybe a better thing to do here is to treat those events as hints?
                                // What I mean by that?
                                // - Don't store the status at all.
                                // - When receiving down event, and the driver still sees the node
                                //   as connected, then try to send a keepalive query to its connections.
                                // - When receiving up event, and we have no connections to the node,
                                //   then try to open new connections.
                                continue;
                            },
                            _ => continue, // Don't go to refreshing
                        }
                    } else {
                        // If server_events_channel was closed, than MetadataReader was dropped,
                        // so we can probably stop working too
                        return;
                    }
                }

                maybe_connectivity_event = self.connectivity_events_receiver.recv() => {
                    let Some(event) = maybe_connectivity_event else {
                        // connectivity_events_channel should never be closed while ClusterWorker is alive,
                        // because ClusterWorker owns the other end of the channel.
                        // However, if it is closed, we can't do anything useful, so just stop working.
                        return;
                    };
                    debug!("Received connectivity event: {:?}", event);

                    self.handle_connectivity_change_event(&event);

                    continue; // Don't go to refreshing.
                }

                maybe_use_keyspace_request = self.use_keyspace_channel.recv() => {
                    match maybe_use_keyspace_request {
                        Some(request) => {
                            self.used_keyspace = Some(request.keyspace_name.clone());

                            let cluster_state = self.cluster_state.load_full();
                            let use_keyspace_future = Self::handle_use_keyspace_request(cluster_state, request);
                            tokio::spawn(use_keyspace_future);
                        },
                        None => return, // If use_keyspace_channel was closed then cluster was dropped, we can stop working
                    }

                    continue; // Don't go to refreshing, wait for the next event
                }

                maybe_control_connection_failed = self.control_connection_repair_channel.recv() => {
                    match maybe_control_connection_failed {
                        Some(()) => {
                            // The control connection was broken. Acknowledge that and start attempting to reconnect.
                            // The first reconnect attempt will be immediate (by attempting metadata refresh below),
                            // and if it does not succeed, then `control_connection_works` will be set to `false`,
                            // so subsequent attempts will be issued every second.
                        }
                        None => {
                            // If control_connection_repair_channel was closed then MetadataReader was dropped,
                            // we can stop working.
                            return;
                        }
                    }
                }
            }

            // Perform the refresh
            debug!("Requesting metadata refresh");
            last_refresh_time = Instant::now();
            let refresh_res = self.perform_refresh().await;

            control_connection_works = refresh_res.is_ok();

            // Send refresh result if there was a request
            if let Some(request) = cur_request {
                // We can ignore sending error - if no one waits for the response we can drop it
                let _ = request.response_chan.send(refresh_res);
            }
        }
    }

    async fn handle_use_keyspace_request(
        cluster_state: Arc<ClusterState>,
        request: UseKeyspaceRequest,
    ) {
        let result = Self::send_use_keyspace(cluster_state, &request.keyspace_name).await;

        // Don't care if nobody wants request result
        let _ = request.response_chan.send(result);
    }

    async fn send_use_keyspace(
        cluster_state: Arc<ClusterState>,
        keyspace_name: &VerifiedKeyspaceName,
    ) -> Result<(), UseKeyspaceError> {
        let use_keyspace_futures = cluster_state
            .known_peers
            .values()
            .map(|node| node.use_keyspace(keyspace_name.clone()));
        let use_keyspace_results: Vec<Result<(), UseKeyspaceError>> =
            join_all(use_keyspace_futures).await;

        use_keyspace_result(use_keyspace_results.into_iter())
    }

    async fn perform_refresh(&mut self) -> Result<(), MetadataError> {
        // Read latest Metadata
        let metadata = self.metadata_reader.read_metadata(false).await?;
        let cluster_state: Arc<ClusterState> = self.cluster_state.load_full();

        let new_cluster_state = Arc::new(
            ClusterState::new(
                metadata,
                &self.pool_config,
                &cluster_state.known_peers,
                &mut |old_nodes, new_nodes| {
                    ClusterWorker::handle_topology_changes(
                        old_nodes,
                        new_nodes,
                        self.host_listener.as_deref(),
                        &mut self.node_status,
                    )
                },
                &self.used_keyspace,
                self.host_filter.as_deref(),
                &self.connectivity_events_sender,
                cluster_state.locator.tablets.clone(),
                &cluster_state.keyspaces,
                #[cfg(feature = "metrics")]
                &self.metrics,
            )
            .await,
        );

        new_cluster_state
            .wait_until_all_pools_are_initialized()
            .await;

        self.update_cluster_state(new_cluster_state);

        Ok(())
    }

    fn update_cluster_state(&mut self, new_cluster_state: Arc<ClusterState>) {
        self.cluster_state.store(new_cluster_state);
    }

    /// Handle node addition/removal/address changes.
    ///
    /// Emit respective events to the [HostListener], if configured.
    fn handle_topology_changes(
        known_peers: &HashMap<Uuid, Arc<Node>>,
        new_known_peers: &HashMap<Uuid, Arc<Node>>,
        host_listener: Option<&dyn HostListener>,
        node_status: &mut HashMap<Uuid, NodeConnectivityStatus>,
    ) {
        // Nodes that were previously in the cluster but are not present anymore.
        let removed_nodes =
            hash_map_difference(known_peers, new_known_peers).filter(|(_host_id, node)| {
                // If a host filter is configured, we only consider nodes that passed the filter
                // as removed. Nodes that were filtered out are not considered part of the cluster,
                // so their removal is not signaled.
                node.is_enabled()
            });
        // Nodes that weren't previously in the cluster but are present now.
        let added_nodes =
            hash_map_difference(new_known_peers, known_peers).filter(|(_host_id, node)| {
                // If a host filter is configured, we only consider nodes that passed the filter
                // as added. Nodes that were filtered out are not considered part of the cluster,
                // so their addition is not signaled.
                node.is_enabled()
            });
        // Nodes that were present in both old and new cluster state, but have changed address.
        let nodes_with_changed_address = known_peers
            .iter()
            .filter(|(_host_id, old_node)| {
                // If a host filter is configured, we only consider nodes that passed the filter
                // as candidates for address change notification. Nodes that were filtered out
                // are not considered part of the cluster, so their address changes are not signaled.
                old_node.is_enabled()
            })
            .filter_map(|(host_id, old_node)| {
                new_known_peers
                    .get(host_id)
                    // We only consider nodes with changed SocketAddr. If only NodeAddr variant changed
                    // (which happens mainly when control connection moves from one node to another),
                    // we don't notify the host listener about that, as it operates on SocketAddr only.
                    //
                    // We must compare only by SocketAddr and ignore NodeAddr variant, because otherwise
                    // the following sequence of events could be issued when node 127.0.0.1 is removed
                    // and control connection is moved to 127.0.0.2, resulting in NodeAddr variant change
                    // from `Translatable(127.0.0.2)` to `Untranslatable(127.0.0.2)`:
                    //
                    // ```
                    // Host 127.0.0.1 is DOWN
                    // Host 127.0.0.2 is DOWN
                    // Host 127.0.0.2 has been REMOVED
                    // Host 127.0.0.1 has been REMOVED
                    // Host 127.0.0.2 has been ADDED
                    // Host 127.0.0.2 is UP
                    // ```
                    .filter(|new_node| {
                        old_node.address.into_inner() != new_node.address.into_inner()
                    })
                    .map(|new_node| (old_node, new_node))
            });

        // Handle node removal.
        for (host_id, node) in removed_nodes {
            info!(
                "Node removed from cluster: {} - {}",
                node.host_id, node.address,
            );

            let Some(connectivity) = node_status.remove(host_id) else {
                error!(
                    "BUG: Inconsistent node status: missing entry for removed node {} - {}",
                    node.host_id, node.address
                );
                continue;
            };

            let ctx = HostEventContext {
                host_id: node.host_id,
                addr: node.address.into_inner(),
            };
            // Notify listener about node removal.
            let Some(host_listener) = host_listener else {
                // No listener configured, nothing to do.
                continue;
            };

            // First signal DOWN event, if needed.
            match connectivity {
                NodeConnectivityStatus::Connected => {
                    host_listener.on_event(&ctx, &HostEvent::Down);
                }
                NodeConnectivityStatus::Unreachable => { /* No need to signal anything */ }
            }

            // Then signal REMOVED event.
            host_listener.on_event(&ctx, &HostEvent::Removed);
        }

        // Handle node address changes.
        for (old_node, new_node) in nodes_with_changed_address {
            info!(
                "Node address changed in cluster: {} - {} -> {}",
                old_node.host_id, old_node.address, new_node.address,
            );

            // Update node address in node_status map.
            let Some(connectivity) = node_status.get_mut(&old_node.host_id) else {
                error!(
                    "BUG: Inconsistent node status: missing entry for node with changed address {} - {}",
                    new_node.host_id, new_node.address
                );
                // If the entry is missing, we skip notifying the host listener about the address change,
                // to avoid emitting inconsistent events.
                continue;
            };

            // Notify listener about node address change.
            let Some(host_listener) = host_listener else {
                // No listener configured, nothing to do.
                continue;
            };

            // We need to make sure that this event is only signaled when the node is DOWN.
            // Otherwise, we need to first emit DOWN event, then ADDRESS_CHANGED event, then UP event.

            if *connectivity == NodeConnectivityStatus::Connected {
                // First signal DOWN event.
                let down_ctx = HostEventContext {
                    host_id: new_node.host_id,
                    addr: old_node.address.into_inner(),
                };
                host_listener.on_event(&down_ctx, &HostEvent::Down);
            }

            let ctx = HostEventContext {
                host_id: new_node.host_id,
                // We need to decide which address to send in the context - old or new.
                // I decided to send the new address, as it is more useful - after the address change
                // the driver will use the new address to connect to the node.
                // Both addresses are sent in the AddressChanged event itself.
                addr: new_node.address.into_inner(),
            };
            // Signal ADDRESS_CHANGED event.
            host_listener.on_event(
                &ctx,
                &HostEvent::AddressChanged {
                    old_address: old_node.address.into_inner(),
                    new_address: new_node.address.into_inner(),
                },
            );

            if *connectivity == NodeConnectivityStatus::Connected {
                // We first signaled artificial DOWN event, so now we must signal UP event.
                let up_ctx = HostEventContext {
                    host_id: new_node.host_id,
                    addr: new_node.address.into_inner(),
                };
                host_listener.on_event(&up_ctx, &HostEvent::Up);
            }
        }

        // Handle node addition.
        for (&host_id, node) in added_nodes {
            info!("Node added to cluster: {} - {}", node.host_id, node.address,);

            // Update node_status map.
            // New nodes are always initially marked as Connected.
            let prev = node_status.insert(host_id, NodeConnectivityStatus::Connected);
            if prev.is_some() {
                error!(
                    "BUG: Inconsistent node status: entry for newly added node {} - {} already existed",
                    node.host_id, node.address
                );
                // If the entry already existed, we skip notifying the host listener about the addition,
                // to avoid duplicate events.
                continue;
            }

            // Notify listener about new nodes in the cluster.
            let Some(host_listener) = host_listener else {
                continue;
            };

            let ctx = HostEventContext {
                host_id: node.host_id,
                addr: node.address.into_inner(),
            };

            // First signal ADDED event.
            host_listener.on_event(&ctx, &HostEvent::Added);
            host_listener.on_event(&ctx, &HostEvent::Up);
        }
    }

    /// Handles connectivity change events received from connection pools.
    ///
    /// When a node becomes unreachable or reachable again, notifies the [HostListener]
    /// about the change, if a host listener is configured. Otherwise, if the node status
    /// has not changed, does nothing.
    fn handle_connectivity_change_event(&mut self, event: &ConnectivityChangeEvent) {
        let host_id = event.host_id();
        let cluster_state = self.cluster_state.load();

        let (Some(node), Some(connectivity)) = (
            cluster_state.known_peers.get(&host_id),
            self.node_status.get_mut(&host_id),
        ) else {
            trace!("Received connectivity change event for unknown host_id: {host_id}");
            return;
        };

        let addr = node.address.into_inner();
        let maybe_event: Option<HostEvent> = match (*connectivity, event) {
            (NodeConnectivityStatus::Connected, ConnectivityChangeEvent::Lost { .. }) => {
                debug!("Node is no longer reachable: {}", addr);
                *connectivity = NodeConnectivityStatus::Unreachable;
                Some(HostEvent::Down)
            }
            (NodeConnectivityStatus::Unreachable, ConnectivityChangeEvent::Established { .. }) => {
                debug!("Node is now reachable again: {}", addr);
                *connectivity = NodeConnectivityStatus::Connected;
                Some(HostEvent::Up)
            }
            _ => {
                /* No status change */
                None
            }
        };

        let Some(host_listener) = self.host_listener.as_deref() else {
            // No host listener configured, nothing to do.
            return;
        };
        let Some(event) = maybe_event else {
            // No event to signal, nothing to do.
            return;
        };

        let ctx = HostEventContext { host_id, addr };
        host_listener.on_event(&ctx, &event);
    }
}

/// Returns a result of use_keyspace operation, based on the query results
/// returned from given node/connection.
///
/// This function assumes that `use_keyspace_results` iterator is NON-EMPTY!
pub(crate) fn use_keyspace_result(
    use_keyspace_results: impl Iterator<Item = Result<(), UseKeyspaceError>>,
) -> Result<(), UseKeyspaceError> {
    // If there was at least one Ok and the rest were broken connection errors we can return Ok
    // keyspace name is correct and will be used on broken connection on the next reconnect

    // If there were only broken connection errors then return broken connection error.
    // If there was an error different than broken connection error return this error - something is wrong

    let mut was_ok: bool = false;
    let mut broken_conn_error: Option<UseKeyspaceError> = None;

    for result in use_keyspace_results {
        match result {
            Ok(()) => was_ok = true,
            Err(err) => match err {
                UseKeyspaceError::RequestError(RequestAttemptError::BrokenConnectionError(_)) => {
                    broken_conn_error = Some(err)
                }
                _ => return Err(err),
            },
        }
    }

    if was_ok {
        return Ok(());
    }

    // We can unwrap conn_broken_error because use_keyspace_results must be nonempty
    Err(broken_conn_error.unwrap())
}

/// Computes the difference between two hash maps, analogous to set difference.
fn hash_map_difference<'present, 'absent, K, V>(
    present_here: &'present HashMap<K, V>,
    absent_here: &'absent HashMap<K, V>,
) -> impl Iterator<Item = (&'present K, &'present V)> + use<'present, 'absent, K, V>
where
    K: std::hash::Hash + Eq,
{
    present_here
        .iter()
        .filter(|(k, _v)| !absent_here.contains_key(k))
}
