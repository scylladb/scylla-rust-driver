//! This module contains the [`ControlConnectionEstablisher`] struct, which is responsible for
//! creating control connections and fetching cluster metadata through them.
//!
//! The control connection is a dedicated connection to one of the cluster nodes
//! that is used to:
//! - Fetch cluster metadata (topology, schema, token ring information)
//! - Receive server-side events (topology changes, schema changes, status changes)
//!
//! [`ControlConnectionEstablisher`] establishes control connections, including:
//! - Connection establishment to contact points or known peers
//! - Iterating over known peers and initial contact points on connection failure
//! - Host filtering to ensure the control connection is established to an accepted node
//!
//! Ownership of the established control connection lives outside the establisher (in the
//! metadata worker); the establisher only knows how to create one. The metadata queries
//! themselves are methods on [`ControlConnection`], configured at its creation.

use std::sync::Arc;
use std::time::Duration;

use rand::rng;
use rand::seq::SliceRandom;
use tracing::{debug, error, warn};

use crate::client::client_routes::ClientRoutesSubscriber;
use crate::cluster::KnownNode;
use crate::cluster::control_connection::{
    ControlConnection, ControlConnectionCache, ControlConnectionConfig, ControlConnectionEvents,
    MetadataRequestTimeouts,
};
use crate::cluster::metadata::{
    Metadata, Peer, PeerEndpoint, SchemaMetadataFetchMode, UntranslatedEndpoint,
};
use crate::cluster::node::resolve_contact_points;
use crate::errors::{ConnectionPoolError, MetadataError, NewSessionError};
use crate::frame::server_event_type::EventTypeV2 as EventType;
use crate::network::{ConnectionConfig, open_connection};
use crate::policies::host_filter::HostFilter;
use crate::utils::safe_format::IteratorSafeFormatExt;

/// Maintains the persistent state needed to create control connections and
/// fetch cluster metadata. The established control connection itself is owned by
/// the caller (the metadata worker), not by the establisher.
pub(crate) struct ControlConnectionEstablisher {
    // =======================================================================================
    // Configuration values - they will stay the same during whole lifetime of ControlConnectionEstablisher.
    // =======================================================================================
    control_connection_config: ConnectionConfig,
    /// Configuration stamped onto every control connection this establisher creates;
    /// governs what the control connection's metadata queries fetch.
    cc_config: ControlConnectionConfig,
    hostname_resolution_timeout: Option<Duration>,
    host_filter: Option<Arc<dyn HostFilter>>,
    // When no known peer is reachable, initial known nodes are resolved once again as a fallback
    // and establishing control connection to them is attempted.
    initial_known_nodes: Vec<KnownNode>,

    // ====================================================================
    // Mutable state of ControlConnectionEstablisher. It will change during its lifetime.
    // ====================================================================
    // when a control connection fails, ControlConnectionEstablisher tries to connect to one of known_peers
    known_peers: Vec<UntranslatedEndpoint>,
    cc_cache: Arc<ControlConnectionCache>,
}

/// The per-candidate metadata fetch that
/// [`ControlConnectionEstablisher::establish_cc_and_fetch_metadata`] runs on each candidate
/// connection - implemented by the metadata worker.
///
/// When `ControlConnectionEstablisher` open a new `Connection`, it needs to fetch initial
/// `Metadata` on it before returning it as a new CC. A candidate is such
/// potential CC until the `Metadata` is done fetching.
///
/// This trait allows the caller to customize the behavior of the fetch
/// and return a custom type alongisde the new CC. Intended usage is draining
/// the event channel concurrently to the fetch to avoid a deadlock.
///
/// A trait rather than an `AsyncFnMut` bound: the implementor lends state
/// (e.g. the updates channel) to each call, and a capturing async closure
/// defeats `Send` inference for every future built on top of it (rustc's
/// "implementation of `Send` is not general enough").
pub(super) trait FetchOnCandidate {
    /// Whatever the fetch produces besides the metadata itself; handed back
    /// with the kept connection.
    type Payload;

    async fn fetch(
        &mut self,
        cc: &ControlConnection,
        cc_events: &mut ControlConnectionEvents,
    ) -> Result<(TopologyUpdateGuard, Self::Payload), MetadataError>;
}

impl ControlConnectionEstablisher {
    /// Creates a new ControlConnectionEstablisher.
    ///
    /// Resolves the initial contact points and populates the initial known peers
    /// list. Does **not** establish a control connection — use
    /// [`establish_cc_and_fetch_metadata`](Self::establish_cc_and_fetch_metadata)
    /// for that.
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn new(
        initial_known_nodes: Vec<KnownNode>,
        hostname_resolution_timeout: Option<Duration>,
        connection_config: ConnectionConfig,
        request_timeouts: MetadataRequestTimeouts,
        keyspaces_to_fetch: Vec<String>,
        schema_metadata_fetch_mode: SchemaMetadataFetchMode,
        host_filter: &Option<Arc<dyn HostFilter>>,
        client_routes_subscriber: Option<Arc<dyn ClientRoutesSubscriber>>,
    ) -> Result<Self, NewSessionError> {
        let (initial_peers, resolved_hostnames) =
            resolve_contact_points(&initial_known_nodes, hostname_resolution_timeout).await;
        // Ensure there is at least one resolved node
        if initial_peers.is_empty() {
            return Err(NewSessionError::FailedToResolveAnyHostname(
                resolved_hostnames,
            ));
        }

        let cc_cache = Arc::new(ControlConnectionCache::new());

        Ok(ControlConnectionEstablisher {
            control_connection_config: connection_config,
            cc_config: ControlConnectionConfig {
                keyspaces_to_fetch,
                schema_metadata_fetch_mode,
                client_routes_subscriber,
                request_timeouts,
            },
            hostname_resolution_timeout,
            known_peers: initial_peers
                .into_iter()
                .map(UntranslatedEndpoint::ContactPoint)
                .collect(),
            host_filter: host_filter.clone(),
            initial_known_nodes,
            cc_cache,
        })
    }

    /// Establishes a control connection and fetches metadata in one go.
    ///
    /// Iterates over known peers (shuffled), trying to connect and run
    /// `fetcher` on each. If `initial` is false and all known peers are
    /// exhausted, falls back to re-resolving the initial contact points.
    ///
    /// `fetcher` is invoked once per candidate connection and must perform the
    /// metadata fetch on it; it is also free to consume the connection's server
    /// events meanwhile. Its payload is returned alongside the connection
    /// it was produced on, so a payload never outlives its candidate. On
    /// success the connection is handed onward together with its event
    /// channels, which must remain pollable: `fetcher` must not consume a
    /// lifecycle event (`Broken`/`Shutdown`) and still return `Ok`.
    ///
    /// On success, updates `known_peers` and returns the fetched metadata together
    /// with a control connection to use going forward. The returned control
    /// connection is `None` when metadata was obtained but no usable control
    /// connection remains:
    /// - on an `initial` read whose metadata fetch failed (dummy metadata is
    ///   returned so the session can still start), or
    /// - when every node that yielded metadata is rejected by the host filter.
    ///
    /// In both of these cases the caller is expected to re-establish the control
    /// connection at the repair cadence.
    pub(super) async fn establish_cc_and_fetch_metadata<F: FetchOnCandidate>(
        &mut self,
        initial: bool,
        fetcher: &mut F,
    ) -> Result<
        (
            Option<(ControlConnection, ControlConnectionEvents, F::Payload)>,
            Metadata,
        ),
        MetadataError,
    > {
        // shuffle known_peers to iterate through them in random order
        self.known_peers.shuffle(&mut rng());
        debug!(
            "Known peers: {:?}",
            self.known_peers.iter().safe_format(", ")
        );

        // `try_establish_on_nodes` returns `Err(None)` if the node iterator was empty
        // (e.g. all known peers were rejected by the host filter, or contact points
        // failed to resolve). We carry the most recent error across attempts and only
        // synthesize a fallback error if no connection was ever attempted.
        let known_peers_err = match self
            .try_establish_on_nodes(initial, self.known_peers.clone().into_iter(), fetcher)
            .await
        {
            Ok(result) => return Ok(result),
            Err(err) => err,
        };

        if initial {
            // No point in falling back as this is an initial connection attempt.
            let err = known_peers_err.unwrap_or_else(no_nodes_available_error);
            error!(
                error = ?err,
                "Could not establish control connection and fetch metadata"
            );
            return Err(err);
        }

        // If no known peer is reachable, try falling back to initial contact points, in hope that
        // there are some hostnames there which will resolve to reachable new addresses.
        warn!(
            "Failed to establish control connection and fetch metadata on all known peers. Falling back to initial contact points."
        );
        let (initial_peers, _hostnames) =
            resolve_contact_points(&self.initial_known_nodes, self.hostname_resolution_timeout)
                .await;
        match self
            .try_establish_on_nodes(
                initial,
                initial_peers
                    .into_iter()
                    .map(UntranslatedEndpoint::ContactPoint),
                fetcher,
            )
            .await
        {
            Ok(result) => Ok(result),
            Err(fallback_err) => {
                let err = fallback_err
                    .or(known_peers_err)
                    .unwrap_or_else(no_nodes_available_error);
                error!(
                    error = ?err,
                    "Could not establish control connection and fetch metadata"
                );
                Err(err)
            }
        }
    }

    /// Tries to establish a control connection and fetch metadata (through the
    /// caller-provided `fetch`) on each node from the given iterator.
    ///
    /// Returns the first working, host-filter-accepted control connection
    /// together with its metadata and the payload `fetch` produced on it. Two
    /// situations yield metadata but no control connection (`Ok((None, metadata))`):
    /// - every node that could be queried is rejected by the host filter — the
    ///   metadata is valid cluster-wide, but none of the connections may be kept;
    /// - `initial` is true and a connection was established but its metadata fetch
    ///   failed — dummy metadata is returned so the session can still start.
    ///
    /// Returns `Err(None)` if the iterator was empty (no connection was ever
    /// attempted), or `Err(Some(err))` with the most recent error otherwise.
    async fn try_establish_on_nodes<F: FetchOnCandidate>(
        &mut self,
        initial: bool,
        nodes: impl Iterator<Item = UntranslatedEndpoint>,
        fetcher: &mut F,
    ) -> Result<
        (
            Option<(ControlConnection, ControlConnectionEvents, F::Payload)>,
            Metadata,
        ),
        Option<MetadataError>,
    > {
        let mut last_err: Option<MetadataError> = None;
        // Metadata fetched from a host-filter-rejected node. It is valid cluster-wide,
        // so it is kept as a fallback in case no accepted node can be reached, while we
        // keep looking for an accepted node to host the control connection on.
        let mut rejected_metadata: Option<Metadata> = None;

        for peer in nodes {
            let peer_address = peer.address();
            debug!("Trying to establish control connection on {peer_address}");

            let (cc, mut cc_events) = match Self::make_control_connection(
                peer,
                self.control_connection_config.clone(),
                self.cc_config.clone(),
                Arc::clone(&self.cc_cache),
            )
            .await
            {
                Ok(cc) => cc,
                Err(err) => {
                    warn!(
                        control_connection_address = %peer_address,
                        error = %err,
                        "Failed to establish control connection"
                    );
                    last_err = Some(err);
                    continue;
                }
            };

            let (topology_update, payload) = match fetcher.fetch(&cc, &mut cc_events).await {
                Ok(fetched) => fetched,
                Err(err) => {
                    if initial {
                        // The control connection was established, but the initial
                        // metadata fetch failed. Prefer any valid metadata already
                        // obtained from a rejected node; otherwise fall back to dummy
                        // metadata so the session can still start. Either way, drop the
                        // control connection so it is re-established at the repair cadence.
                        let metadata = rejected_metadata.take().unwrap_or_else(|| {
                            warn!(
                                error = ?err,
                                "Initial metadata read failed, proceeding with metadata \
                                consisting only of the initial peer list and dummy tokens. \
                                This might result in suboptimal performance and schema \
                                information not being available."
                            );
                            Metadata::new_dummy(&self.known_peers)
                        });
                        return Ok((None, metadata));
                    }
                    warn!(
                        control_connection_address = %peer_address,
                        error = %err,
                        "Failed to fetch metadata using current control connection"
                    );
                    last_err = Some(err);
                    // CC is dropped here, we continue to the next peer.
                    continue;
                }
            };

            let metadata = topology_update.apply(self);
            debug!("Fetched new metadata");

            if self.is_cc_endpoint_rejected(cc.endpoint(), &metadata) {
                // The node hosting this control connection is rejected by the host
                // filter. The metadata is valid cluster-wide, so remember it, but drop
                // the connection and keep looking for a host-filter-accepted node.
                rejected_metadata = Some(metadata);
                continue;
            }

            return Ok((Some((cc, cc_events, payload)), metadata));
        }

        match rejected_metadata {
            Some(metadata) => Ok((None, metadata)),
            None => Err(last_err),
        }
    }

    fn update_known_peers(&mut self, peers: &[Peer]) {
        let host_filter = self.host_filter.as_ref();
        self.known_peers = peers
            .iter()
            .filter(|peer| host_filter.is_none_or(|f| f.accept(peer)))
            .map(|peer| UntranslatedEndpoint::Peer(peer.to_peer_endpoint()))
            .collect();

        // Check if the host filter isn't accidentally too restrictive,
        // and print an error message about this fact
        if !peers.is_empty() && self.known_peers.is_empty() {
            error!(
                node_ips = tracing::field::display(
                    peers.iter().map(|peer| peer.address).safe_format(", ")
                ),
                "The host filter rejected all nodes in the cluster, \
                no connections that can serve user queries have been \
                established. The session cannot serve any queries!"
            )
        }
    }

    /// Returns true if the control connection endpoint is on a node rejected
    /// by the host filter, meaning the caller should re-establish the CC on
    /// an accepted node.
    fn is_cc_endpoint_rejected(
        &self,
        endpoint: &UntranslatedEndpoint,
        metadata: &Metadata,
    ) -> bool {
        let control_connection_peer = metadata
            .peers
            .iter()
            .find(|peer| matches!(endpoint, UntranslatedEndpoint::Peer(PeerEndpoint{address, ..}) if *address == peer.address));
        if let Some(peer) = control_connection_peer
            && !self.host_filter.as_ref().is_none_or(|f| f.accept(peer))
        {
            warn!(
                filtered_node_ips = tracing::field::display(metadata
                    .peers
                    .iter()
                    .filter(|peer| self.host_filter.as_ref().is_none_or(|p| p.accept(peer)))
                    .map(|peer| peer.address)
                    .safe_format(", ")
                ),
                control_connection_address = ?endpoint.address(),
                "The node that the control connection is established to \
                is not accepted by the host filter. Please verify that \
                the nodes in your initial peers list are accepted by the \
                host filter. The driver will try to re-establish the \
                control connection to a different node."
            );
            return true;
        }
        false
    }

    async fn make_control_connection(
        endpoint: UntranslatedEndpoint,
        mut config: ConnectionConfig,
        cc_config: ControlConnectionConfig,
        cache: Arc<ControlConnectionCache>,
    ) -> Result<(ControlConnection, ControlConnectionEvents), MetadataError> {
        let (sender, receiver) = tokio::sync::mpsc::channel(32);
        // setting event_sender field in connection config will cause control connection to
        // - send REGISTER message to receive server events
        // - send received events via server_event_sender
        let mut events_to_register_for = vec![
            EventType::TopologyChange,
            EventType::StatusChange,
            EventType::SchemaChange,
        ];
        if cc_config.client_routes_subscriber.is_some() {
            events_to_register_for.push(EventType::ClientRoutesChange);
        }

        config.event_sender = Some((sender, events_to_register_for));
        let open_result = open_connection(
            &endpoint,
            None,
            &config.to_host_connection_config(&endpoint),
        )
        .await;

        match open_result {
            Ok((con, recv)) => Ok(ControlConnection::new(
                Arc::new(con),
                endpoint,
                cc_config,
                cache,
                recv,
                receiver,
            )),
            Err(conn_err) => Err(MetadataError::ConnectionPoolError(
                ConnectionPoolError::Broken {
                    last_connection_error: conn_err,
                },
            )),
        }
    }
}

/// Freshly fetched [`Metadata`] that the [`ControlConnectionEstablisher`] has not yet absorbed.
///
/// [`ControlConnection::query_metadata`] returns its result wrapped in this
/// guard, so that the fetched metadata cannot be used without the establisher
/// updating its known peers from it first: the only way to extract the
/// [`Metadata`] is [`apply`](Self::apply).
#[must_use = "the fetched metadata must be applied to the ControlConnectionEstablisher"]
pub(super) struct TopologyUpdateGuard {
    metadata: Metadata,
}

impl TopologyUpdateGuard {
    pub(super) fn new(metadata: Metadata) -> Self {
        Self { metadata }
    }

    /// Updates the establisher's known peers from the fetched metadata and releases
    /// the metadata itself.
    pub(super) fn apply(self, establisher: &mut ControlConnectionEstablisher) -> Metadata {
        establisher.update_known_peers(&self.metadata.peers);
        self.metadata
    }
}

/// Error to report when there was not a single node to even attempt a control
/// connection on (e.g. all known peers were rejected by the host filter and the
/// initial contact points failed to resolve to any address).
fn no_nodes_available_error() -> MetadataError {
    MetadataError::ConnectionPoolError(ConnectionPoolError::NodeDisabledByHostFilter)
}
