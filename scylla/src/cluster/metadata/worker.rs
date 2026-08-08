use std::ops::ControlFlow;
use std::time::Duration;

use tokio::time::Instant;
use tracing::{debug, error};

use crate::cluster::control_connection::{
    ControlConnection, ControlConnectionEvent, ControlConnectionEvents,
};
use crate::cluster::metadata::update::{MetadataUpdate, RefreshRequest};
use crate::frame::response::event::EventV2 as Event;
use crate::frame::response::event::StatusChangeEvent;

use super::merge_channel;
use super::reader::MetadataReader;

/// How often the worker attempts to establish a control connection while it has none.
const CONTROL_CONNECTION_REPAIR_INTERVAL: Duration = Duration::from_secs(1);

/// Works in the background to keep learning about the cluster: fetches metadata
/// periodically and on request, and drains CQL server events from the control
/// connection. All of its output is put into the `merge_channel` that the
/// caller of `new` provides.
///
/// The worker alternates between two loops:
/// - [`work_on_cc`](Self::work_on_cc), which serves a specific control connection
///   until it breaks, and
/// - [`work_without_cc`](Self::work_without_cc), which establishes a new one.
///
/// The control connection is therefore not a field, but a value owned by the
/// currently running loop.
pub(in super::super) struct MetadataWorker {
    /// Fetches metadata and (re-)establishes control connections. Also keeps the
    /// known peers and the client-routes subscriber, which it needs for
    /// connection ids and event registration.
    metadata_reader: MetadataReader,

    // This value determines how frequently the metadata
    // worker will refresh the cluster metadata
    cluster_metadata_refresh_interval: Duration,

    // To listen for refresh requests
    refresh_channel: tokio::sync::mpsc::Receiver<RefreshRequest>,

    /// Produces updates for the cluster worker.
    updates: merge_channel::Sender<MetadataUpdate>,

    /// A received refresh request whose refresh has not been performed yet.
    ///
    /// This is worker state rather than a local of either loop: a request may
    /// arrive on a control connection that then turns out to be defunct, in which
    /// case it must survive the switch to [`work_without_cc`](Self::work_without_cc),
    /// which retries the refresh and ultimately answers it.
    ///
    /// Invariant: at most one request is pending at a time - both loops take it
    /// (answering it, directly or through the cluster worker) before awaiting
    /// the next one.
    pending_request: Option<RefreshRequest>,
}

impl MetadataWorker {
    pub(in super::super) fn new(
        metadata_reader: MetadataReader,
        cluster_metadata_refresh_interval: Duration,
        refresh_channel: tokio::sync::mpsc::Receiver<RefreshRequest>,
        updates: merge_channel::Sender<MetadataUpdate>,
    ) -> Self {
        Self {
            metadata_reader,
            cluster_metadata_refresh_interval,
            refresh_channel,
            updates,
            pending_request: None,
        }
    }

    /// Runs the worker until the cluster (or the runtime) is gone.
    ///
    /// `initial_cc` is the control connection that the initial metadata was fetched
    /// on; `None` means none could be kept, in which case one is established first.
    pub(in super::super) async fn work(
        mut self,
        initial_cc: Option<(ControlConnection, ControlConnectionEvents)>,
    ) {
        let mut next_cc = initial_cc;

        loop {
            let (cc, cc_events) = match next_cc.take() {
                Some(cc) => cc,
                None => match self.work_without_cc().await {
                    ControlFlow::Break(()) => return,
                    ControlFlow::Continue(cc) => cc,
                },
            };

            match self.work_on_cc(cc, cc_events).await {
                ControlFlow::Break(()) => return,
                // The control connection is defunct - establish a new one.
                ControlFlow::Continue(()) => (),
            }
        }
    }

    /// Attempts to establish a control connection until one is available.
    ///
    /// The first attempt is made immediately; further ones are spaced by
    /// [`CONTROL_CONNECTION_REPAIR_INTERVAL`], with an incoming refresh request
    /// triggering an attempt right away.
    ///
    /// Each attempt fetches metadata, too, so a successful one both satisfies the
    /// pending refresh request (through the cluster worker) and publishes an update.
    ///
    /// Returns [`ControlFlow::Break`] if the worker should stop.
    async fn work_without_cc(
        &mut self,
    ) -> ControlFlow<(), (ControlConnection, ControlConnectionEvents)> {
        loop {
            debug!("Attempting to establish a new control connection");
            let attempt_time = Instant::now();

            match self
                .metadata_reader
                .establish_cc_and_fetch_metadata(false)
                .await
            {
                Ok((cc, metadata)) => {
                    // The refresh request, if any, is answered by the cluster worker, once the
                    // state resulting from this metadata is published - this is what makes
                    // `Cluster::refresh_metadata` return only after the new state is visible.
                    let response_chan = self
                        .pending_request
                        .take()
                        .map(|request| request.response_chan);
                    if self
                        .send_update(|slot| {
                            MetadataUpdate::merge_metadata(slot, metadata, response_chan)
                        })
                        .is_err()
                    {
                        return ControlFlow::Break(());
                    }

                    if let Some(cc) = cc {
                        return ControlFlow::Continue(cc);
                    }
                    // Metadata was fetched, but no control connection could be kept
                    // (e.g. every reachable node is rejected by the host filter).
                    // Keep trying at the repair cadence.
                }
                Err(err) => {
                    debug!(
                        error = %err,
                        "Failed to establish a control connection and fetch metadata"
                    );
                    // Nobody else can act on a failed fetch, so the error only goes to the
                    // requester, if there is one.
                    if let Some(request) = self.pending_request.take() {
                        // We can ignore sending error - if no one waits for the response we can drop it
                        let _ = request.response_chan.send(Err(err));
                    }
                }
            }

            // Wait until it's time for the next attempt, unless a refresh request
            // makes us attempt earlier.
            let sleep_until = attempt_time
                .checked_add(CONTROL_CONNECTION_REPAIR_INTERVAL)
                .unwrap_or_else(Instant::now);

            tokio::select! {
                _sleep_finished = tokio::time::sleep_until(sleep_until) => (),

                maybe_refresh_request = self.refresh_channel.recv() => {
                    match maybe_refresh_request {
                        Some(request) => self.set_pending_request(request),
                        None => return ControlFlow::Break(()), // If refresh_channel was closed then cluster was dropped, we can stop working
                    }
                }
            }
        }
    }

    /// Serves the given control connection: refreshes metadata on it (periodically,
    /// on request and in reaction to server events) and drains its events.
    ///
    /// Returns [`ControlFlow::Continue`] once the control connection is deemed
    /// defunct and should be replaced, or [`ControlFlow::Break`] if the worker
    /// should stop.
    async fn work_on_cc(
        &mut self,
        cc: ControlConnection,
        mut cc_events: ControlConnectionEvents,
    ) -> ControlFlow<()> {
        let mut last_refresh_time = Instant::now();

        loop {
            // Wait until it's time for the next refresh
            let sleep_until: Instant = last_refresh_time
                .checked_add(self.cluster_metadata_refresh_interval)
                .unwrap_or_else(Instant::now);

            let sleep_future = tokio::time::sleep_until(sleep_until);
            tokio::pin!(sleep_future);

            tokio::select! {
                _sleep_finished = sleep_future => {
                    // Time to do periodic refresh.
                },

                maybe_refresh_request = self.refresh_channel.recv() => {
                    match maybe_refresh_request {
                        Some(request) => self.set_pending_request(request),
                        None => return ControlFlow::Break(()), // If refresh_channel was closed then cluster was dropped, we can stop working
                    }
                }

                control_connection_event = cc_events.wait_for_event() => {
                    match control_connection_event {
                        ControlConnectionEvent::Shutdown => {
                            // The runtime is shutting down. We can stop working.
                            debug!("Got shutdown control connection event. Shutting down MetadataWorker.");
                            return ControlFlow::Break(());
                        },
                        ControlConnectionEvent::Broken(_err) => {
                            // The control connection was broken. Have a new one established;
                            // the first attempt will be immediate, and if it does not succeed,
                            // subsequent attempts will be issued every second.
                            return ControlFlow::Continue(());
                        },
                        ControlConnectionEvent::ServerEvent(event) => {
                            debug!("Received server event: {:?}", event);
                            match event {
                                Event::TopologyChange(_) => (), // Refresh immediately
                                Event::ClientRoutesChange(evt) => {
                                    let res = self.metadata_reader.fetch_client_routes_update_on_event(&cc, &evt).await;
                                    match res {
                                        Ok(None) => continue, // Nothing to apply; don't go to refreshing.
                                        Ok(Some(routes)) => {
                                            if self.send_update(|slot| MetadataUpdate::merge_client_routes_update(slot, routes)).is_err() {
                                                return ControlFlow::Break(());
                                            }
                                            continue; // Don't go to refreshing.
                                        }
                                        Err(err) =>
                                        {
                                            error!(
                                                "Error when fetching client route updates: {err}. \
                                                Proceeding with metadata refresh, because the control connection is likely defunct."
                                            );
                                            // Refresh immediately.
                                        }
                                    }
                                }
                                Event::StatusChange(status) => {
                                    // Tracking node status using events is unreliable because of the possibility of losing events
                                    // when control connection is broken. A better thing to do here is to treat those events as hints
                                    // for:
                                    // - PoolRefiller - UP triggers immediate pool refill attempt, and
                                    // - Keepaliver - DOWN triggers immediate keepalive query attempt.

                                    match status {
                                        StatusChangeEvent::Up(addr) => {
                                            // When receiving an UP event, it is likely that the node just came back up and is now reachable.
                                            // We optimistically trigger pool refill for this node.
                                            // This is not guaranteed to be correct. It is for example possible that a network partition happened,
                                            // the node lost connectivity to the cluster and driver; then it regained connectivity to the cluster,
                                            // but not to the driver, and thus is still unreachable from the driver's perspective.
                                            // However, in this case triggering pool refill is not harmful - if the node is actually reachable,
                                            // then new connections will be opened to it, and if it is not reachable,
                                            // then connection attempts will fail and the node will be marked as unreachable by `PoolRefiller`,
                                            // so it won't be targeted by the load balancing policy.
                                            if self.send_update(|slot| MetadataUpdate::merge_up_hint(slot, addr)).is_err() {
                                                return ControlFlow::Break(());
                                            }
                                        },
                                        StatusChangeEvent::Down(addr) => {
                                            // When receiving a DOWN event, and the driver still sees the node as connected,
                                            // we send a keepalive query on its connections to verify their liveness.
                                            // The node is supposedly DOWN, so connections to this node are likely defunct.
                                            // We expect that the keepalive query fails, in which case connections will be closed.
                                            // As a result, the connection pool will report 0 connections to this node,
                                            // and thus the node will not be targeted by the LoadBalancingPolicy,
                                            // which is the desired behaviour. However, if the keepalive query succeeds,
                                            // then the node is likely still alive (got stale event?), and we keep targeting it.
                                            if self.send_update(|slot| MetadataUpdate::merge_down_hint(slot, addr)).is_err() {
                                                return ControlFlow::Break(());
                                            }
                                        },
                                    }
                                    continue; // Don't go to refreshing.
                                },
                                _ => continue, // Don't go to refreshing.
                            }
                        }
                    }
                }
            }

            // Perform the refresh
            debug!("Requesting metadata refresh");
            last_refresh_time = Instant::now();

            match self.metadata_reader.fetch_metadata_on_cc(&cc).await {
                Ok(metadata) => {
                    // The refresh request, if any, is answered by the cluster worker, once the
                    // state resulting from this metadata is published - this is what makes
                    // `Cluster::refresh_metadata` return only after the new state is visible.
                    let response_chan = self
                        .pending_request
                        .take()
                        .map(|request| request.response_chan);
                    if self
                        .send_update(|slot| {
                            MetadataUpdate::merge_metadata(slot, metadata, response_chan)
                        })
                        .is_err()
                    {
                        return ControlFlow::Break(());
                    }
                }
                Err(err) => {
                    debug!(
                        error = %err,
                        "Failed to fetch metadata on the current control connection. \
                        Will try to establish a new one."
                    );
                    // The control connection is considered defunct - drop it. The pending
                    // request, if any, is retried (and answered) while establishing a new one.
                    return ControlFlow::Continue(());
                }
            }
        }
    }

    /// Stores a freshly received refresh request as the pending one.
    ///
    /// Upholds the `pending_request` invariant: both loops take the pending request
    /// before awaiting the next one, so there can never be one already stored.
    fn set_pending_request(&mut self, request: RefreshRequest) {
        debug_assert!(self.pending_request.is_none());
        self.pending_request = Some(request);
    }

    /// Merges an update into the pending one, to be picked up by the cluster worker.
    ///
    /// An error means that the cluster worker is gone, in which case this worker
    /// has nothing left to work for and should end.
    fn send_update(
        &mut self,
        f: impl FnOnce(&mut Option<MetadataUpdate>),
    ) -> Result<(), merge_channel::SendError> {
        self.updates.modify(f).inspect_err(|_| {
            debug!("The cluster worker is gone. Shutting down MetadataWorker.");
        })
    }
}
