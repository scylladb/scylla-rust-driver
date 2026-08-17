use crate::client::client_routes::{
    ClientRoutesAddressTranslator, ClientRoutesConfig, ClientRoutesSubscriber,
};
use crate::client::session::TABLET_CHANNEL_SIZE;
use crate::cluster::control_connection::MetadataRequestTimeouts;
use crate::cluster::metadata::SchemaMetadataFetchMode;
use crate::cluster::metadata::update::{
    MetadataChanges, MetadataUpdate, PartialMetadataChanges, RefreshRequest, StatusHint,
};
use crate::cluster::state::NodeConfig;
use crate::cluster::{KnownNode, Node};
use crate::errors::{MetadataError, NewSessionError, RequestAttemptError, UseKeyspaceError};
use crate::network::{ConnectivityChangeEvent, PoolConfig, VerifiedKeyspaceName};
use crate::observability::metrics::Metrics;
use crate::policies::address_translator::AddressTranslator;
use crate::policies::host_filter::HostFilter;
use crate::policies::host_listener::{HostEvent, HostEventContext, HostListener};
use crate::routing::locator::tablets::{RawTablet, TabletsInfo};

use crate::frame::response::result::TableSpec;
use arc_swap::ArcSwap;
use futures::future::join_all;
use futures::{FutureExt, future::RemoteHandle};

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;
use tracing::{debug, error, info, trace};
use uuid::Uuid;

use super::metadata::cc_establisher::ControlConnectionEstablisher;
use super::metadata::merge_channel::{self, merge_channel};
use super::metadata::worker::MetadataWorker;
use super::state::ClusterState;

/// Cluster manages up to date information and connections to database nodes.
/// All state can be accessed by cloning Arc<ClusterState> in the `state` field
pub(crate) struct Cluster {
    // `ArcSwap<ClusterState>` is wrapped in `Arc` to support sharing cluster state
    // between `Cluster` and `ClusterWorker`
    state: Arc<ArcSwap<ClusterState>>,

    refresh_channel: tokio::sync::mpsc::Sender<RefreshRequest>,
    use_keyspace_channel: tokio::sync::mpsc::Sender<UseKeyspaceRequest>,

    _worker_handle: RemoteHandle<()>,
    _metadata_worker_handle: RemoteHandle<()>,
}

/// Enables printing [Cluster] struct in a neat way, by skipping the rather useless
/// print of channels state and printing [ClusterState] neatly.
pub(crate) struct ClusterNeatDebug<'a>(pub(crate) &'a Cluster);
impl std::fmt::Debug for ClusterNeatDebug<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let cluster = self.0;
        f.debug_struct("Cluster")
            .field("data", &cluster.state.load())
            .finish_non_exhaustive()
    }
}

impl Cluster {
    #[expect(clippy::too_many_arguments)]
    pub(crate) async fn new(
        known_nodes: Vec<KnownNode>,
        mut pool_config: PoolConfig,
        keyspaces_to_fetch: Vec<String>,
        schema_metadata_fetch_mode: SchemaMetadataFetchMode,
        metadata_request_timeouts: MetadataRequestTimeouts,
        hostname_resolution_timeout: Option<Duration>,
        host_filter: Option<Arc<dyn HostFilter>>,
        host_listener: Option<Arc<dyn HostListener>>,
        cluster_metadata_refresh_interval: Duration,
        tablet_receiver: tokio::sync::mpsc::Receiver<(TableSpec<'static>, RawTablet)>,
        metrics: Metrics,
        client_routes_config: Option<ClientRoutesConfig>,
    ) -> Result<Cluster, NewSessionError> {
        let (refresh_sender, refresh_receiver) = tokio::sync::mpsc::channel(32);
        let (use_keyspace_sender, use_keyspace_receiver) = tokio::sync::mpsc::channel(32);
        // This is unbounded, because there is possibility that many events will be sent quickly,
        // for example when driver is connected to a large cluster and it loses network connectivity.
        //
        // If the channel were bounded, then we would either block PoolRefillers (if we decide to send blockingly)
        // or drop events (if we decide to do so if the channel is full). Both options are bad.
        let (connectivity_events_sender, connectivity_events_receiver) =
            tokio::sync::mpsc::unbounded_channel();

        let client_routes_address_translator = client_routes_config.as_ref().map(|config| {
            let translator = Arc::new(ClientRoutesAddressTranslator::new(
                config.clone(),
                hostname_resolution_timeout,
                pool_config.connection_config.tls_provider.is_some(),
            ));
            pool_config.connection_config.address_translator =
                Some(Arc::clone(&translator) as Arc<dyn AddressTranslator>);

            translator
        });

        let client_routes_subscriber = client_routes_address_translator
            .map(|translator| translator as Arc<dyn ClientRoutesSubscriber>);

        let cc_establisher = ControlConnectionEstablisher::new(
            known_nodes,
            hostname_resolution_timeout,
            pool_config.connection_config.clone(),
            metadata_request_timeouts,
            keyspaces_to_fetch,
            schema_metadata_fetch_mode,
            &host_filter,
            client_routes_subscriber.as_ref().map(Arc::clone),
        )
        .await?;

        let mut node_status = HashMap::new();

        let (metadata_updates_sender, metadata_updates_receiver) = merge_channel();

        let mut metadata_worker = MetadataWorker::new(
            cc_establisher,
            cluster_metadata_refresh_interval,
            refresh_receiver,
            metadata_updates_sender,
        );

        let (cc, mut metadata) = metadata_worker.establish(true).await?;

        // The initial metadata is fetched before the metadata worker is spawned, so the routes
        // must be applied here - `ClusterState::new` below creates connection pools, which
        // translate addresses through the subscriber.
        if let (Some(subscriber), Some(routes)) = (
            client_routes_subscriber.as_ref(),
            metadata.client_routes.take(),
        ) {
            // The returned host ids are irrelevant here: the pools are being created fresh anyway.
            let _ = subscriber.replace_client_routes(routes);
        }

        let node_config = NodeConfig {
            pool_config,
            used_keyspace: None,
            connectivity_events_sender,
            metrics,
        };

        let cluster_state = ClusterState::new_updated(
            metadata,
            &node_config,
            &HashMap::new(),
            host_filter.as_deref(),
            TabletsInfo::new(),
            &HashMap::new(),
        )
        .await;
        ClusterWorker::handle_topology_changes(
            &HashMap::new(),
            &cluster_state.known_nodes,
            host_listener.as_deref(),
            &mut node_status,
        );

        cluster_state.wait_until_all_pools_are_initialized().await;

        let cluster_state: Arc<ArcSwap<ClusterState>> =
            Arc::new(ArcSwap::from(Arc::new(cluster_state)));

        let worker = ClusterWorker {
            cluster_state: cluster_state.clone(),
            node_status,

            client_routes_subscriber,
            node_config,

            metadata_updates: metadata_updates_receiver,
            connectivity_events_receiver,
            tablets_channel: tablet_receiver,

            use_keyspace_channel: use_keyspace_receiver,

            host_filter,
            host_listener,
        };

        let (fut, worker_handle) = worker.work().remote_handle();
        tokio::spawn(fut);

        let (metadata_fut, metadata_worker_handle) = metadata_worker.work(cc).remote_handle();
        tokio::spawn(metadata_fut);

        let result = Cluster {
            state: cluster_state,
            refresh_channel: refresh_sender,
            use_keyspace_channel: use_keyspace_sender,
            _worker_handle: worker_handle,
            _metadata_worker_handle: metadata_worker_handle,
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
        // Other end of this channel is in MetadataWorker, can't be dropped while we have &self to Cluster with _metadata_worker_handle

        response_receiver
            .await
            .expect("Bug in Cluster::refresh_metadata receiving")
        // The workers always respond
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

    /// The applier of client routes snapshots fetched from `system.client_routes`.
    /// `None` if client routes are not configured.
    client_routes_subscriber: Option<Arc<dyn ClientRoutesSubscriber>>,

    /// Used to create all the `Node` objects.
    node_config: NodeConfig,

    // Channel over which the metadata worker delivers everything it learned
    // about the cluster.
    metadata_updates: merge_channel::Receiver<MetadataUpdate>,

    // Channel used to receive use keyspace requests
    use_keyspace_channel: tokio::sync::mpsc::Receiver<UseKeyspaceRequest>,

    // Channel used to receive signals that node is no longer reachable or became reachable.
    connectivity_events_receiver: tokio::sync::mpsc::UnboundedReceiver<ConnectivityChangeEvent>,

    // Channel used to receive info about new tablets from custom payload in responses
    // sent by server.
    tablets_channel: tokio::sync::mpsc::Receiver<(TableSpec<'static>, RawTablet)>,

    // The host filter determines towards which nodes we should open
    // connections
    host_filter: Option<Arc<dyn HostFilter>>,

    // The host listener allows to listen for topology and node status changes.
    host_listener: Option<Arc<dyn HostListener>>,
}

#[derive(Debug)]
struct UseKeyspaceRequest {
    keyspace_name: VerifiedKeyspaceName,
    response_chan: tokio::sync::oneshot::Sender<Result<(), UseKeyspaceError>>,
}

impl ClusterWorker {
    pub(crate) async fn work(mut self) {
        loop {
            let mut tablets = Vec::new();

            tokio::select! {
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
                }

                maybe_metadata_update = self.metadata_updates.recv() => {
                    let Some(update) = maybe_metadata_update else {
                        // If the channel was closed then the metadata worker is gone,
                        // so there is nothing left to keep the cluster state updated with.
                        debug!("The metadata worker is gone. Shutting down ClusterWorker.");
                        return;
                    };

                    self.apply_metadata_update(update).await;
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
                }

                maybe_use_keyspace_request = self.use_keyspace_channel.recv() => {
                    match maybe_use_keyspace_request {
                        Some(request) => {
                            self.node_config.used_keyspace = Some(request.keyspace_name.clone());

                            let cluster_state = self.cluster_state.load_full();
                            let use_keyspace_future = Self::handle_use_keyspace_request(cluster_state, request);
                            tokio::spawn(use_keyspace_future);
                        },
                        None => return, // If use_keyspace_channel was closed then cluster was dropped, we can stop working
                    }
                }
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
            .known_nodes
            .values()
            .map(|node| node.use_keyspace(keyspace_name.clone()));
        let use_keyspace_results: Vec<Result<(), UseKeyspaceError>> =
            join_all(use_keyspace_futures).await;

        use_keyspace_result(use_keyspace_results.into_iter())
    }

    /// Applies everything that the metadata worker learned since the previous update.
    ///
    /// See [`MetadataUpdate`] for what an update can contain and why several
    /// discoveries can arrive merged into one.
    async fn apply_metadata_update(&mut self, mut update: MetadataUpdate) {
        // Apply the client routes BEFORE constructing the new `ClusterState` below,
        // because `ClusterState::new` creates connection pools, which translate addresses
        // through the subscriber.
        let client_routes_hosts_to_refill = self.handle_client_route_update(&mut update);

        let cluster_state = self.cluster_state.load_full();

        // Unlike UP hints, DOWN hints are applied to the *current* state: a keepalive
        // query only makes sense for a node the driver still holds connections to, so
        // there is nothing a freshly built state could add, and waiting for the refresh
        // (which awaits pool initialization) would only delay the liveness probe.
        update
            .status_hints
            .iter()
            .filter(|(_k, v)| **v == StatusHint::Down)
            .for_each(|(addr, _)| cluster_state.trigger_keepalive_for_addr(*addr));

        let process_up_hints = |state: &ClusterState| {
            state.trigger_pool_refills_for_hosts(client_routes_hosts_to_refill.into_iter());
            update
                .status_hints
                .iter()
                .filter(|(_k, v)| **v == StatusHint::Up)
                .for_each(|(addr, _)| state.trigger_pool_refill_for_addr(*addr));
        };

        let new_state_with_requests = match update.metadata_changes {
            Some(MetadataChanges::Full {
                metadata,
                refresh_responses,
            }) => {
                let new_cluster_state = Arc::new(
                    ClusterState::new_updated(
                        metadata,
                        &self.node_config,
                        &cluster_state.known_nodes,
                        self.host_filter.as_deref(),
                        cluster_state.locator.tablets.clone(),
                        &cluster_state.keyspaces,
                    )
                    .await,
                );
                Some((new_cluster_state, refresh_responses))
            }
            None | Some(MetadataChanges::Partial(_)) => {
                // For now there is nothing that requires publishing new ClusterState.
                None
            }
        };

        // Regardless of wheter we have a new state or not, we need to publish UP hints.
        // If no new state - publish using new one.
        let Some((new_cluster_state, refresh_responses)) = new_state_with_requests else {
            process_up_hints(&cluster_state);
            return;
        };

        process_up_hints(&new_cluster_state);

        ClusterWorker::handle_topology_changes(
            &cluster_state.known_nodes,
            &new_cluster_state.known_nodes,
            self.host_listener.as_deref(),
            &mut self.node_status,
        );

        new_cluster_state
            .wait_until_all_pools_are_initialized()
            .await;

        self.update_cluster_state(new_cluster_state);

        // The new state is published, so the awaited refreshes are complete.
        for response_chan in refresh_responses {
            // We can ignore sending error - if no one waits for the response we can drop it
            let _ = response_chan.send(Ok(()));
        }
    }

    /// Applies the provided `update` to client route subscriber, if any.
    ///
    /// Returns a set of hosts to which UP hint should be applied.
    fn handle_client_route_update(&self, update: &mut MetadataUpdate) -> HashSet<Uuid> {
        let (Some(subscriber), Some(metadata_changes)) = (
            self.client_routes_subscriber.as_ref(),
            update.metadata_changes.as_mut(),
        ) else {
            return HashSet::new();
        };

        match metadata_changes {
            MetadataChanges::Full {
                metadata,
                refresh_responses: _,
            } => {
                if let Some(client_routes) = metadata.client_routes.take() {
                    subscriber.replace_client_routes(client_routes)
                } else {
                    HashSet::new()
                }
            }
            MetadataChanges::Partial(PartialMetadataChanges {
                client_routes_updates,
            }) => match client_routes_updates.take() {
                Some(client_routes_update) => {
                    subscriber.merge_client_routes_update(client_routes_update)
                }
                None => HashSet::new(),
            },
        }
    }

    fn update_cluster_state(&mut self, new_cluster_state: Arc<ClusterState>) {
        self.cluster_state.store(new_cluster_state);
    }

    /// Handle node addition/removal/address changes.
    ///
    /// Emit respective events to the [HostListener], if configured.
    fn handle_topology_changes(
        known_nodes: &HashMap<Uuid, Arc<Node>>,
        new_known_nodes: &HashMap<Uuid, Arc<Node>>,
        host_listener: Option<&dyn HostListener>,
        node_status: &mut HashMap<Uuid, NodeConnectivityStatus>,
    ) {
        // Nodes that were previously in the cluster but are not present anymore.
        let removed_nodes =
            hash_map_difference(known_nodes, new_known_nodes).filter(|(_host_id, node)| {
                // If a host filter is configured, we only consider nodes that passed the filter
                // as removed. Nodes that were filtered out are not considered part of the cluster,
                // so their removal is not signaled.
                node.is_enabled()
            });
        // Nodes that weren't previously in the cluster but are present now.
        let added_nodes =
            hash_map_difference(new_known_nodes, known_nodes).filter(|(_host_id, node)| {
                // If a host filter is configured, we only consider nodes that passed the filter
                // as added. Nodes that were filtered out are not considered part of the cluster,
                // so their addition is not signaled.
                node.is_enabled()
            });
        // Nodes that were present in both old and new cluster state, but have changed address.
        let nodes_with_changed_address = known_nodes
            .iter()
            .filter(|(_host_id, old_node)| {
                // If a host filter is configured, we only consider nodes that passed the filter
                // as candidates for address change notification. Nodes that were filtered out
                // are not considered part of the cluster, so their address changes are not signaled.
                old_node.is_enabled()
            })
            .filter_map(|(host_id, old_node)| {
                new_known_nodes
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
            cluster_state.known_nodes.get(&host_id),
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
