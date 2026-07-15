//! This module contains the [`MetadataReader`] struct, which is responsible for
//! fetching and maintaining cluster metadata through a control connection.
//!
//! The control connection is a dedicated connection to one of the cluster nodes
//! that is used to:
//! - Fetch cluster metadata (topology, schema, token ring information)
//! - Receive server-side events (topology changes, schema changes, status changes)
//!
//! [`MetadataReader`] handles control connection lifecycle, including:
//! - Initial connection establishment to contact points
//! - Automatic reconnection to other known peers on connection failure
//! - Fallback to initial contact points when all known peers are unreachable
//! - Host filtering to ensure the control connection is established to an accepted node
//!

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use rand::rng;
use rand::seq::{IndexedRandom, SliceRandom};
use tracing::{debug, error, warn};
use uuid::Uuid;

use crate::client::client_routes::ClientRoutesSubscriber;
use crate::cluster::KnownNode;
use crate::cluster::control_connection::{
    ControlConnection, ControlConnectionCache, ControlConnectionEvent,
};
use crate::cluster::metadata::{Metadata, PeerEndpoint, UntranslatedEndpoint};
use crate::cluster::node::resolve_contact_points;
use crate::errors::{ConnectionPoolError, MetadataError, NewSessionError};
use crate::frame::response::event::ClientRoutesChangeEvent;
use crate::frame::server_event_type::EventTypeV2 as EventType;
use crate::network::{ConnectionConfig, open_connection};
use crate::policies::host_filter::HostFilter;
use crate::utils::safe_format::IteratorSafeFormatExt;

enum ControlConnectionState {
    Working(ControlConnection),
    Broken {
        last_error: MetadataError,
        last_endpoint: UntranslatedEndpoint,
    },
}

impl ControlConnectionState {
    fn endpoint(&self) -> &UntranslatedEndpoint {
        match self {
            ControlConnectionState::Working(c) => c.endpoint(),
            ControlConnectionState::Broken { last_endpoint, .. } => last_endpoint,
        }
    }
}

/// Allows to read current metadata from the cluster
pub(crate) struct MetadataReader {
    // =======================================================================================
    // Configuration values - they will stay the same during whole lifetime of MetadataReader.
    // =======================================================================================
    control_connection_config: ConnectionConfig,
    request_serverside_timeout: Option<Duration>,
    hostname_resolution_timeout: Option<Duration>,
    keyspaces_to_fetch: Vec<String>,
    fetch_schema: bool,
    host_filter: Option<Arc<dyn HostFilter>>,
    // When no known peer is reachable, initial known nodes are resolved once again as a fallback
    // and establishing control connection to them is attempted.
    initial_known_nodes: Vec<KnownNode>,
    client_routes_subscriber: Option<Arc<dyn ClientRoutesSubscriber>>,

    // ====================================================================
    // Mutable state of MetadataReader. It will change during its lifetime.
    // ====================================================================
    control_connection_state: ControlConnectionState,
    // when control connection fails, MetadataReader tries to connect to one of known_peers
    known_peers: Vec<UntranslatedEndpoint>,
    cc_cache: Arc<ControlConnectionCache>,
}

impl MetadataReader {
    /// Creates new MetadataReader, which connects to initially_known_peers in the background
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn new(
        initial_known_nodes: Vec<KnownNode>,
        hostname_resolution_timeout: Option<Duration>,
        connection_config: ConnectionConfig,
        request_serverside_timeout: Option<Duration>,
        keyspaces_to_fetch: Vec<String>,
        fetch_schema: bool,
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

        let control_connection_endpoint = UntranslatedEndpoint::ContactPoint(
            initial_peers
                .choose(&mut rng())
                .expect("Tried to initialize MetadataReader with empty initial_known_nodes list!")
                .clone(),
        );

        let cc_cache = Arc::new(ControlConnectionCache::new());

        let control_connection_state = Self::make_control_connection_state(
            control_connection_endpoint,
            connection_config.clone(),
            request_serverside_timeout,
            Arc::clone(&cc_cache),
            client_routes_subscriber.as_ref().map(Arc::clone),
        )
        .await;

        Ok(MetadataReader {
            control_connection_config: connection_config,
            control_connection_state,
            request_serverside_timeout,
            hostname_resolution_timeout,
            known_peers: initial_peers
                .into_iter()
                .map(UntranslatedEndpoint::ContactPoint)
                .collect(),
            keyspaces_to_fetch,
            fetch_schema,
            host_filter: host_filter.clone(),
            initial_known_nodes,
            cc_cache,
            client_routes_subscriber,
        })
    }

    pub(crate) async fn wait_for_control_connection_event(&mut self) -> ControlConnectionEvent {
        match &mut self.control_connection_state {
            ControlConnectionState::Broken { .. } => std::future::pending().await,
            ControlConnectionState::Working(working_connection) => {
                let event = working_connection.wait_for_event().await;
                if let ControlConnectionEvent::Broken(err) = &event {
                    self.control_connection_state = ControlConnectionState::Broken {
                        last_error: MetadataError::ConnectionPoolError(
                            ConnectionPoolError::Broken {
                                last_connection_error: err.clone(),
                            },
                        ),
                        last_endpoint: working_connection.endpoint().clone(),
                    };
                }

                event
            }
        }
    }

    pub(crate) fn control_connection_works(&self) -> bool {
        matches!(
            self.control_connection_state,
            ControlConnectionState::Working(_)
        )
    }

    /// Fetches current metadata from the cluster
    pub(crate) async fn read_metadata(&mut self, initial: bool) -> Result<Metadata, MetadataError> {
        let mut result = self.fetch_metadata(initial).await;
        let prev_err = match result {
            Ok(metadata) => {
                debug!("Fetched new metadata");
                self.update_known_peers(&metadata);
                if initial {
                    self.handle_unaccepted_host_in_control_connection(&metadata)
                        .await;
                }
                return Ok(metadata);
            }
            Err(err) => err,
        };

        // At this point, we known that fetching metadata on current control connection failed.
        // Therefore, we try to fetch metadata from other known peers, in order.

        // shuffle known_peers to iterate through them in random order later
        self.known_peers.shuffle(&mut rng());
        debug!(
            "Known peers: {:?}",
            self.known_peers.iter().safe_format(", ")
        );

        // if fetching metadata on current control connection failed,
        // try to fetch metadata from other known peer
        result = self
            .retry_fetch_metadata_on_nodes(initial, self.known_peers.clone().into_iter(), prev_err)
            .await;

        if let Err(prev_err) = result {
            if !initial {
                // If no known peer is reachable, try falling back to initial contact points, in hope that
                // there are some hostnames there which will resolve to reachable new addresses.
                warn!(
                    "Failed to establish control connection and fetch metadata on all known peers. Falling back to initial contact points."
                );
                let (initial_peers, _hostnames) = resolve_contact_points(
                    &self.initial_known_nodes,
                    self.hostname_resolution_timeout,
                )
                .await;
                result = self
                    .retry_fetch_metadata_on_nodes(
                        initial,
                        initial_peers
                            .into_iter()
                            .map(UntranslatedEndpoint::ContactPoint),
                        prev_err,
                    )
                    .await;
            } else {
                // No point in falling back as this is an initial connection attempt.
                result = Err(prev_err);
            }
        }

        match &result {
            Ok(metadata) => {
                self.update_known_peers(metadata);
                self.handle_unaccepted_host_in_control_connection(metadata)
                    .await;
                debug!("Fetched new metadata");
            }
            Err(error) => {
                error!(
                    error = %error,
                    "Could not fetch metadata"
                )
            }
        }

        result
    }

    async fn retry_fetch_metadata_on_nodes(
        &mut self,
        initial: bool,
        nodes: impl Iterator<Item = UntranslatedEndpoint>,
        prev_err: MetadataError,
    ) -> Result<Metadata, MetadataError> {
        let mut last_err = prev_err;
        for peer in nodes {
            let peer_address = peer.address();
            debug!("Retrying to establish the control connection on {peer_address}");

            self.control_connection_state = Self::make_control_connection_state(
                peer,
                self.control_connection_config.clone(),
                self.request_serverside_timeout,
                Arc::clone(&self.cc_cache),
                self.client_routes_subscriber.as_ref().map(Arc::clone),
            )
            .await;

            let err = match self.fetch_metadata(initial).await {
                Ok(metadata) => return Ok(metadata),
                Err(err) => err,
            };

            warn!(
                control_connection_address = %peer_address,
                error = %err,
                "Failed to fetch metadata using current control connection"
            );

            last_err = err;
        }

        Err(last_err)
    }

    async fn fetch_metadata(&mut self, initial: bool) -> Result<Metadata, MetadataError> {
        let (endpoint, res) = match &self.control_connection_state {
            ControlConnectionState::Working(working_connection) => {
                let endpoint = working_connection.endpoint().clone();
                let res = self.query_metadata_on_cc(working_connection).await;
                (endpoint, res)
            }
            ControlConnectionState::Broken { last_error: e, .. } => {
                return Err(e.clone());
            }
        };

        // If metadata fetch failed, we consider the connection broken.
        if let Err(err) = &res {
            self.control_connection_state = ControlConnectionState::Broken {
                last_error: err.clone(),
                last_endpoint: endpoint,
            }
        }

        if initial && let Err(err) = res {
            warn!(
                error = ?err,
                "Initial metadata read failed, proceeding with metadata \
                consisting only of the initial peer list and dummy tokens. \
                This might result in suboptimal performance and schema \
                information not being available."
            );
            return Ok(Metadata::new_dummy(&self.known_peers));
        }

        res
    }

    /// Queries metadata on the given control connection.
    ///
    /// This is a thin wrapper over [`ControlConnection::query_metadata`] that fills in
    /// the reader's configuration (keyspaces to fetch, whether to fetch schema). It does
    /// **not** update `known_peers` nor touch the control connection state.
    async fn query_metadata_on_cc(
        &self,
        cc: &ControlConnection,
    ) -> Result<Metadata, MetadataError> {
        cc.query_metadata(
            cc.endpoint().address().port(),
            &self.keyspaces_to_fetch,
            self.fetch_schema,
        )
        .await
    }

    fn update_known_peers(&mut self, metadata: &Metadata) {
        let host_filter = self.host_filter.as_ref();
        self.known_peers = metadata
            .peers
            .iter()
            .filter(|peer| host_filter.is_none_or(|f| f.accept(peer)))
            .map(|peer| UntranslatedEndpoint::Peer(peer.to_peer_endpoint()))
            .collect();

        // Check if the host filter isn't accidentally too restrictive,
        // and print an error message about this fact
        if !metadata.peers.is_empty() && self.known_peers.is_empty() {
            error!(
                node_ips = tracing::field::display(
                    metadata
                        .peers
                        .iter()
                        .map(|peer| peer.address)
                        .safe_format(", ")
                ),
                "The host filter rejected all nodes in the cluster, \
                no connections that can serve user queries have been \
                established. The session cannot serve any queries!"
            )
        }
    }

    async fn handle_unaccepted_host_in_control_connection(&mut self, metadata: &Metadata) {
        let endpoint = self.control_connection_state.endpoint().clone();
        if self.is_cc_endpoint_rejected(&endpoint, metadata) {
            // Assuming here that known_peers are up-to-date
            if !self.known_peers.is_empty() {
                let control_connection_endpoint = self
                    .known_peers
                    .choose(&mut rng())
                    .expect("known_peers is empty - should be impossible")
                    .clone();

                self.control_connection_state = Self::make_control_connection_state(
                    control_connection_endpoint,
                    self.control_connection_config.clone(),
                    self.request_serverside_timeout,
                    Arc::clone(&self.cc_cache),
                    self.client_routes_subscriber.as_ref().map(Arc::clone),
                )
                .await;
            }
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

    /// Opens a control connection to `endpoint`, wrapping the outcome in a
    /// [`ControlConnectionState`] (either `Working` or `Broken`).
    async fn make_control_connection_state(
        endpoint: UntranslatedEndpoint,
        config: ConnectionConfig,
        request_serverside_timeout: Option<Duration>,
        cache: Arc<ControlConnectionCache>,
        client_routes_subscriber: Option<Arc<dyn ClientRoutesSubscriber>>,
    ) -> ControlConnectionState {
        let last_endpoint = endpoint.clone();
        match Self::make_control_connection(
            endpoint,
            config,
            request_serverside_timeout,
            cache,
            client_routes_subscriber,
        )
        .await
        {
            Ok(cc) => ControlConnectionState::Working(cc),
            Err(last_error) => ControlConnectionState::Broken {
                last_error,
                last_endpoint,
            },
        }
    }

    async fn make_control_connection(
        endpoint: UntranslatedEndpoint,
        mut config: ConnectionConfig,
        request_serverside_timeout: Option<Duration>,
        cache: Arc<ControlConnectionCache>,
        client_routes_subscriber: Option<Arc<dyn ClientRoutesSubscriber>>,
    ) -> Result<ControlConnection, MetadataError> {
        let (sender, receiver) = tokio::sync::mpsc::channel(32);
        // setting event_sender field in connection config will cause control connection to
        // - send REGISTER message to receive server events
        // - send received events via server_event_sender
        let mut events_to_register_for = vec![
            EventType::TopologyChange,
            EventType::StatusChange,
            EventType::SchemaChange,
        ];
        if client_routes_subscriber.is_some() {
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
                cache,
                client_routes_subscriber,
                recv,
                receiver,
            )
            .override_serverside_timeout(request_serverside_timeout)),
            Err(conn_err) => Err(MetadataError::ConnectionPoolError(
                ConnectionPoolError::Broken {
                    last_connection_error: conn_err,
                },
            )),
        }
    }

    /// Performs a partial fetch of `system.client_routes`. Partial means that filtering is done
    /// not only by connection ids known to the driver (which is always the case), but also
    /// by host ids - only for the hosts whose ids are present in the event payload.
    ///
    /// Then, the updates are fed to the [`ClientRoutesSubscriber`] for merging with previous knowledge.
    pub(in super::super) async fn fetch_client_route_updates_on_event(
        &mut self,
        evt: &ClientRoutesChangeEvent,
    ) -> Result<HashSet<Uuid>, MetadataError> {
        let working_connection = match &self.control_connection_state {
            ControlConnectionState::Working(working_connection) => working_connection,
            ControlConnectionState::Broken { last_error: e, .. } => {
                return Err(e.clone());
            }
        };

        let Some(subscriber) = &self.client_routes_subscriber else {
            // No subscriber, but received an event? Strange enough, but nothing to be done here.
            warn!("BUG: Received ClientRoutesChange event, but no ClientRoutesSubscriber was set!");
            return Ok(HashSet::new());
        };

        #[deny(clippy::wildcard_enum_match_arm)]
        let (connection_ids, host_ids) = match evt {
            ClientRoutesChangeEvent::UpdateNodes {
                connection_ids,
                host_ids,
            } => (connection_ids, host_ids),
            _ => unreachable!("clippy testifies that the match is exhaustive"),
        };

        // TODO: this is wasteful - it allocates both strings and a vec.
        // This won't be a performance problem, because UPDATE_NODES events are not frequent.
        // As an optimization, we can implement ser/de for some special new iterator type,
        // to avoid the need to allocate when serializing collections.
        let connection_ids: Vec<String> = connection_ids
            .iter()
            .filter(|&conn_id| subscriber.get_connection_ids().contains(conn_id))
            .cloned()
            .collect();

        if connection_ids.is_empty() {
            // The event contained no relevant connection IDs.
            // Nothing to be done.
            return Ok(HashSet::new());
        }

        // Although this is vaguely documented, the semantics of an event with connection ids [A, B, C] and host ids [X, Y, Z]
        // is that the following entries were added/updated/removed: `[(A, X), (B, Y), (C, Z)]`.
        // Unfortunately, we can't really query Scylla this way. Therefore, we do the query: `WHERE connection id IN ? AND host id IN ?`,
        // which fetches possibly more routes than necessary, for example `(A, Z)` or `(C, Y)`.
        // This is a tradeoff - the only alternative is issuing multiple queries, one per connection id.
        // I believe the tradeoff here is correct.
        let client_routes = working_connection
            .query_client_routes(&connection_ids, host_ids)
            .await?;

        let updated_hosts = subscriber.merge_client_routes_update(evt, client_routes);

        Ok(updated_hosts)
    }
}
