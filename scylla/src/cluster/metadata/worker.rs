use std::time::Duration;

use tracing::{debug, error};

use crate::cluster::control_connection::{
    ControlConnection, ControlConnectionEvent, ControlConnectionEvents,
};
use crate::cluster::metadata::update::{MetadataUpdate, RefreshRequest};
use crate::errors::MetadataError;
use crate::frame::response::event::EventV2 as Event;
use crate::frame::response::event::StatusChangeEvent;

use super::Metadata;
use super::merge_channel;
use super::reader::MetadataReader;

/// Works in the background to keep learning about the cluster: fetches metadata
/// periodically and on request, and drains CQL server events from the control
/// connection. All of its output is put into the `merge_channel` that the
/// caller of `new` provides.
pub(in super::super) struct MetadataWorker {
    /// Fetches metadata and (re-)establishes control connections. Also keeps the
    /// known peers and the client-routes subscriber, which it needs for
    /// connection ids and event registration.
    metadata_reader: MetadataReader,

    /// The control connection, on which metadata is fetched and CQL server
    /// events are received, together with the channels the events arrive on.
    /// `None` means it is currently broken (or was never established) and needs
    /// to be re-established, which happens during the next metadata refresh.
    control_connection: Option<(ControlConnection, ControlConnectionEvents)>,

    // This value determines how frequently the metadata
    // worker will refresh the cluster metadata
    cluster_metadata_refresh_interval: Duration,

    // To listen for refresh requests
    refresh_channel: tokio::sync::mpsc::Receiver<RefreshRequest>,

    /// Produces updates for the cluster worker.
    updates: merge_channel::Sender<MetadataUpdate>,
}

impl MetadataWorker {
    pub(in super::super) fn new(
        metadata_reader: MetadataReader,
        initial_control_connection: Option<(ControlConnection, ControlConnectionEvents)>,
        cluster_metadata_refresh_interval: Duration,
        refresh_channel: tokio::sync::mpsc::Receiver<RefreshRequest>,
        updates: merge_channel::Sender<MetadataUpdate>,
    ) -> Self {
        Self {
            metadata_reader,
            control_connection: initial_control_connection,
            cluster_metadata_refresh_interval,
            refresh_channel,
            updates,
        }
    }

    pub(in super::super) async fn work(mut self) {
        use tokio::time::Instant;

        let control_connection_repair_duration = Duration::from_secs(1); // Attempt control connection repair every second
        let mut last_refresh_time = Instant::now();

        loop {
            let mut cur_request: Option<RefreshRequest> = None;

            // Wait until it's time for the next refresh
            let sleep_until: Instant = last_refresh_time
                .checked_add(if self.control_connection.is_some() {
                    self.cluster_metadata_refresh_interval
                } else {
                    control_connection_repair_duration
                })
                .unwrap_or_else(Instant::now);

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

                control_connection_event = Self::wait_for_control_connection_event(&mut self.control_connection) => {
                    match control_connection_event {
                        ControlConnectionEvent::Shutdown => {
                            // The runtime is shutting down. We can stop working.
                            debug!("Got shutdown control connection event. Shutting down MetadataWorker.");
                            return;
                        },
                        ControlConnectionEvent::Broken(_err) => {
                            // The control connection was broken. Drop it and start attempting to reconnect.
                            // The first reconnect attempt will be immediate (by attempting metadata refresh below),
                            // and if it does not succeed, then the control connection will stay `None`, so
                            // subsequent attempts will be issued every second.
                            self.control_connection = None;
                        },
                        ControlConnectionEvent::ServerEvent(event) => {
                            debug!("Received server event: {:?}", event);
                            match event {
                                Event::TopologyChange(_) => (), // Refresh immediately
                                Event::ClientRoutesChange(evt) => {
                                    // We received this event on the control connection, so it must be present.
                                    let Some((cc, _events)) = self.control_connection.as_ref() else {
                                        error!("BUG: Received a server event without a control connection.");
                                        continue;
                                    };
                                    let res = self.metadata_reader.fetch_client_routes_update_on_event(cc, &evt).await;
                                    match res {
                                        Ok(None) => continue, // Nothing to apply; don't go to refreshing.
                                        Ok(Some(routes)) => {
                                            if self.send_update(|slot| MetadataUpdate::merge_client_routes_update(slot, routes)).is_err() {
                                                return;
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
                                                return;
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
                                                return;
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
            let refresh_res = self.read_metadata().await;

            match refresh_res {
                Ok(metadata) => {
                    // The refresh request, if any, is answered by the cluster worker, once the
                    // state resulting from this metadata is published - this is what makes
                    // `Cluster::refresh_metadata` return only after the new state is visible.
                    let response_chan = cur_request.map(|request| request.response_chan);
                    if self
                        .send_update(|slot| {
                            MetadataUpdate::merge_metadata(slot, metadata, response_chan)
                        })
                        .is_err()
                    {
                        return;
                    }
                }
                Err(err) => {
                    // Nobody else can act on a failed fetch, so the error only goes to the
                    // requester, if there is one.
                    if let Some(request) = cur_request {
                        // We can ignore sending error - if no one waits for the response we can drop it
                        let _ = request.response_chan.send(Err(err));
                    }
                }
            }
        }
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

    /// Waits for the next control connection event.
    ///
    /// If there is no working control connection (it is broken and awaiting
    /// re-establishment), this never resolves — the worker will rely on the
    /// periodic refresh timer to attempt re-establishment instead.
    async fn wait_for_control_connection_event(
        control_connection: &mut Option<(ControlConnection, ControlConnectionEvents)>,
    ) -> ControlConnectionEvent {
        match control_connection {
            None => std::future::pending().await,
            Some((_cc, events)) => events.wait_for_event().await,
        }
    }

    /// Fetches the latest metadata, (re-)establishing the control connection if needed.
    ///
    /// If a working control connection is present, metadata is fetched on it. If that
    /// fails, the connection is dropped and a fresh one is established (iterating over
    /// known peers and, as a last resort, the initial contact points). The (possibly
    /// new) working control connection is stored back into `self.control_connection`.
    async fn read_metadata(&mut self) -> Result<Metadata, MetadataError> {
        if let Some((cc, _events)) = self.control_connection.as_ref() {
            match self.metadata_reader.fetch_metadata_on_cc(cc).await {
                Ok(metadata) => return Ok(metadata),
                Err(err) => {
                    debug!(
                        error = %err,
                        "Failed to fetch metadata on the current control connection. \
                        Will try to establish a new one."
                    );
                    // The control connection is considered defunct - drop it.
                    self.control_connection = None;
                }
            }
        }

        // We have no working control connection - establish a new one and fetch metadata on it.
        let (cc, metadata) = self
            .metadata_reader
            .establish_cc_and_fetch_metadata(false)
            .await?;
        self.control_connection = cc;
        Ok(metadata)
    }
}
