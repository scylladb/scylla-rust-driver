use crate::client::session::TABLET_CHANNEL_SIZE;
use crate::errors::{MetadataError, NewSessionError, RequestAttemptError, UseKeyspaceError};
use crate::frame::response::event::{Event, StatusChangeEvent};
use crate::network::{PoolConfig, VerifiedKeyspaceName};
#[cfg(feature = "metrics")]
use crate::observability::metrics::Metrics;
use crate::policies::host_filter::HostFilter;
use crate::routing::locator::tablets::{RawTablet, TabletsInfo};

use arc_swap::ArcSwap;
use futures::future::join_all;
use futures::{future::RemoteHandle, FutureExt};
use scylla_cql::frame::response::result::TableSpec;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tracing::{debug, warn};

use super::metadata::MetadataReader;
use super::node::{InternalKnownNode, NodeAddr};
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

// Works in the background to keep the cluster updated
struct ClusterWorker {
    // Cluster state to keep updated:
    cluster_state: Arc<ArcSwap<ClusterState>>,

    // Cluster connections
    metadata_reader: MetadataReader,
    pool_config: PoolConfig,

    // To listen for refresh requests
    refresh_channel: tokio::sync::mpsc::Receiver<RefreshRequest>,

    // Channel used to receive use keyspace requests
    use_keyspace_channel: tokio::sync::mpsc::Receiver<UseKeyspaceRequest>,

    // Channel used to receive server events
    server_events_channel: tokio::sync::mpsc::Receiver<Event>,

    // Channel used to receive signals that control connection is broken
    control_connection_repair_channel: tokio::sync::broadcast::Receiver<()>,

    // Channel used to receive info about new tablets from custom payload in responses
    // sent by server.
    tablets_channel: tokio::sync::mpsc::Receiver<(TableSpec<'static>, RawTablet)>,

    // Keyspace send in "USE <keyspace name>" when opening each connection
    used_keyspace: Option<VerifiedKeyspaceName>,

    // The host filter determines towards which nodes we should open
    // connections
    host_filter: Option<Arc<dyn HostFilter>>,

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
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn new(
        known_nodes: Vec<InternalKnownNode>,
        pool_config: PoolConfig,
        keyspaces_to_fetch: Vec<String>,
        fetch_schema_metadata: bool,
        metadata_request_serverside_timeout: Option<Duration>,
        host_filter: Option<Arc<dyn HostFilter>>,
        cluster_metadata_refresh_interval: Duration,
        tablet_receiver: tokio::sync::mpsc::Receiver<(TableSpec<'static>, RawTablet)>,
        #[cfg(feature = "metrics")] metrics: Arc<Metrics>,
    ) -> Result<Cluster, NewSessionError> {
        let (refresh_sender, refresh_receiver) = tokio::sync::mpsc::channel(32);
        let (use_keyspace_sender, use_keyspace_receiver) = tokio::sync::mpsc::channel(32);
        let (server_events_sender, server_events_receiver) = tokio::sync::mpsc::channel(32);
        let (control_connection_repair_sender, control_connection_repair_receiver) =
            tokio::sync::broadcast::channel(32);

        let mut metadata_reader = MetadataReader::new(
            known_nodes,
            control_connection_repair_sender,
            pool_config.connection_config.clone(),
            metadata_request_serverside_timeout,
            server_events_sender,
            keyspaces_to_fetch,
            fetch_schema_metadata,
            &host_filter,
            #[cfg(feature = "metrics")]
            Arc::clone(&metrics),
        )
        .await?;

        let metadata = metadata_reader.read_metadata(true).await?;
        let cluster_state = ClusterState::new(
            metadata,
            &pool_config,
            &HashMap::new(),
            &None,
            host_filter.as_deref(),
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

            metadata_reader,
            pool_config,

            refresh_channel: refresh_receiver,
            server_events_channel: server_events_receiver,
            control_connection_repair_channel: control_connection_repair_receiver,
            tablets_channel: tablet_receiver,

            use_keyspace_channel: use_keyspace_receiver,
            used_keyspace: None,

            host_filter,
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
                _ = sleep_future => {},
                recv_res = self.refresh_channel.recv() => {
                    match recv_res {
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
                recv_res = self.server_events_channel.recv() => {
                    if let Some(event) = recv_res {
                        debug!("Received server event: {:?}", event);
                        match event {
                            Event::TopologyChange(_) => (), // Refresh immediately
                            Event::StatusChange(status) => {
                                // If some node went down/up, update it's marker and refresh
                                // later as planned.

                                // TODO: Tracking status using events is unreliable because of
                                // the possibility of losing events when control connection is broken.
                                // Maybe a better thing to do here is to treat those events as hints?
                                // What I mean by that?
                                // - Don't store the status at all.
                                // - When receiving down event, and the driver still sees the node
                                //   as connected, then try to send a keepalive query to its connections.
                                // - When receiving up event, and we have no connections to the node,
                                //   then try to open new connections.

                                match status {
                                    StatusChangeEvent::Down(addr) => self.change_node_down_marker(addr, true),
                                    StatusChangeEvent::Up(addr) => self.change_node_down_marker(addr, false),
                                }
                                continue;
                            },
                            _ => continue, // Don't go to refreshing
                        }
                    } else {
                        // If server_events_channel was closed, than TopologyReader was dropped,
                        // so we can probably stop working too
                        return;
                    }
                }
                recv_res = self.use_keyspace_channel.recv() => {
                    match recv_res {
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
                recv_res = self.control_connection_repair_channel.recv() => {
                    match recv_res {
                        Ok(()) => {
                            // The control connection was broken. Acknowledge that and start attempting to reconnect.
                            // The first reconnect attempt will be immediate (by attempting metadata refresh below),
                            // and if it does not succeed, then `control_connection_works` will be set to `false`,
                            // so subsequent attempts will be issued every second.
                        }
                        Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {
                            // This is very unlikely; we would have to have a lot of concurrent
                            // control connections opened and broken at the same time.
                            // The best we can do is ignoring this.
                        }
                        Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                            // If control_connection_repair_channel was closed then MetadataReader was dropped,
                            // we can stop working.
                            return;
                        }
                    }
                }
            }

            // Perform the refresh
            debug!("Requesting topology refresh");
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

    fn change_node_down_marker(&mut self, addr: SocketAddr, is_down: bool) {
        let cluster_state = self.cluster_state.load_full();

        // We need to iterate through the whole map here, but there will rarely more than ~100 nodes,
        // and changes of down marker are infrequent enough to afford this. As an important tradeoff,
        // we only keep one hashmap of known_peers, which is indexed by host IDs for node identification.
        let node = match cluster_state
            .known_peers
            .values()
            .find(|&peer| peer.address == NodeAddr::Translatable(addr))
        {
            Some(node) => node,
            None => {
                warn!("Unknown node address {}", addr);
                return;
            }
        };

        node.change_down_marker(is_down);
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
                &self.used_keyspace,
                self.host_filter.as_deref(),
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
