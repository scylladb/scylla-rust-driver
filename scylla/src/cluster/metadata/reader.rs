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

use std::sync::Arc;
use std::time::Duration;

use rand::rng;
use rand::seq::{IndexedRandom, SliceRandom};
use tokio::sync::{mpsc, oneshot};
use tracing::{debug, error, warn};

use crate::cluster::KnownNode;
use crate::cluster::control_connection::ControlConnection;
use crate::cluster::metadata::{Metadata, PeerEndpoint, UntranslatedEndpoint};
use crate::cluster::node::resolve_contact_points;
use crate::errors::{ConnectionError, ConnectionPoolError, MetadataError, NewSessionError};
use crate::frame::response::event::Event;
use crate::network::{ConnectionConfig, open_connection};
use crate::policies::host_filter::HostFilter;
use crate::utils::safe_format::IteratorSafeFormatExt;

pub(crate) enum ControlConnectionEvent {
    Broken,
    ServerEvent(Event),
}

struct WorkingControlConnection {
    connection: ControlConnection,
    endpoint: UntranslatedEndpoint,
    error_channel: oneshot::Receiver<ConnectionError>,
    events_channel: mpsc::Receiver<Event>,
}

enum ControlConnectionState {
    Working(WorkingControlConnection),
    Broken {
        last_error: MetadataError,
        last_endpoint: UntranslatedEndpoint,
    },
}

impl ControlConnectionState {
    fn endpoint(&self) -> &UntranslatedEndpoint {
        match self {
            ControlConnectionState::Working(c) => &c.endpoint,
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

    // ====================================================================
    // Mutable state of MetadataReader. It will change during its lifetime.
    // ====================================================================
    control_connection_state: ControlConnectionState,
    // when control connection fails, MetadataReader tries to connect to one of known_peers
    known_peers: Vec<UntranslatedEndpoint>,
}

impl MetadataReader {
    /// Creates new MetadataReader, which connects to initially_known_peers in the background
    pub(crate) async fn new(
        initial_known_nodes: Vec<KnownNode>,
        hostname_resolution_timeout: Option<Duration>,
        connection_config: ConnectionConfig,
        request_serverside_timeout: Option<Duration>,
        keyspaces_to_fetch: Vec<String>,
        fetch_schema: bool,
        host_filter: &Option<Arc<dyn HostFilter>>,
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

        let control_connection_state = Self::make_control_connection(
            control_connection_endpoint,
            connection_config.clone(),
            request_serverside_timeout,
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
        })
    }

    pub(crate) async fn wait_for_control_connection_event(&mut self) -> ControlConnectionEvent {
        match &mut self.control_connection_state {
            ControlConnectionState::Broken { .. } => std::future::pending().await,
            ControlConnectionState::Working(working_connection) => {
                tokio::select! {
                    // Why only `Some`? `None` means that event channel was dropped.
                    // In current implementation (as of writing this comment)
                    // this should not be possible: events sender is stored in HostConnectionConfig,
                    // which is a field of Connection that we own. If we got `None`, then most likely
                    // two things happened:
                    //  - The implementation changed, for example by moving event sender to router.
                    //  - Connection was closed, router shutdown.
                    //  - `tokio::select!` chose this branch instead of error channel.
                    // The best thing we can imo do is ignore this `None`. `error_channel` should receive
                    // info about connection shutdown very soon.
                    Some(cql_event) = working_connection.events_channel.recv() => {
                        ControlConnectionEvent::ServerEvent(cql_event)
                    },
                    maybe_control_connection_failed = &mut working_connection.error_channel => {
                        let err = match maybe_control_connection_failed {
                            Ok(err) => err,
                            Err(_recv_error) => {
                                // If we got here then error channel, in a Connection that we own,
                                // was dropped without sending anything. This is definitely a bug in the driver!
                                // We could theoretically recover by dropping a connection and creating new one,
                                // but we would need to add an error variant to `BrokenConnectionErrorKind` that
                                // could basically never happen. Let's panic instead.
                                panic!("Error sender of control connection unexpectedly dropped. This is a bug in the driver, please open an issue!");
                            },
                        };
                        self.control_connection_state = ControlConnectionState::Broken {
                            last_error: MetadataError::ConnectionPoolError(ConnectionPoolError::Broken { last_connection_error: err }),
                            last_endpoint: working_connection.endpoint.clone()
                        };
                        ControlConnectionEvent::Broken
                    }
                }
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

        let address_of_failed_control_connection =
            self.control_connection_state.endpoint().address();
        let filtered_known_peers = self
            .known_peers
            .clone()
            .into_iter()
            .filter(|peer| peer.address() != address_of_failed_control_connection);

        // if fetching metadata on current control connection failed,
        // try to fetch metadata from other known peer
        result = self
            .retry_fetch_metadata_on_nodes(initial, filtered_known_peers, prev_err)
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
                let target = self
                    .control_connection_state
                    .endpoint()
                    .address()
                    .into_inner();
                error!(
                    error = %error,
                    target = %target,
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
        let mut result = Err(prev_err);
        for peer in nodes {
            let err = match result {
                Ok(_) => break,
                Err(err) => err,
            };

            warn!(
                control_connection_address = tracing::field::display(self
                    .control_connection_state.endpoint()
                    .address()),
                error = %err,
                "Failed to fetch metadata using current control connection"
            );

            debug!(
                "Retrying to establish the control connection on {}",
                peer.address()
            );

            self.control_connection_state = Self::make_control_connection(
                peer,
                self.control_connection_config.clone(),
                self.request_serverside_timeout,
            )
            .await;

            result = self.fetch_metadata(initial).await;
        }
        result
    }

    async fn fetch_metadata(&mut self, initial: bool) -> Result<Metadata, MetadataError> {
        let working_connection = match &self.control_connection_state {
            ControlConnectionState::Working(working_connection) => working_connection,
            ControlConnectionState::Broken { last_error: e, .. } => {
                return Err(e.clone());
            }
        };
        let endpoint = working_connection.endpoint.clone();

        let res = working_connection
            .connection
            .query_metadata(
                endpoint.address().port(),
                &self.keyspaces_to_fetch,
                self.fetch_schema,
            )
            .await;

        // If metadata fetch failed, we consider the connection broken.
        if let Err(err) = &res {
            self.control_connection_state = ControlConnectionState::Broken {
                last_error: err.clone(),
                last_endpoint: endpoint,
            }
        }

        if initial {
            if let Err(err) = res {
                warn!(
                    error = ?err,
                    "Initial metadata read failed, proceeding with metadata \
                    consisting only of the initial peer list and dummy tokens. \
                    This might result in suboptimal performance and schema \
                    information not being available."
                );
                return Ok(Metadata::new_dummy(&self.known_peers));
            }
        }

        res
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
        let control_connection_peer = metadata
            .peers
            .iter()
            .find(|peer| matches!(self.control_connection_state.endpoint(), UntranslatedEndpoint::Peer(PeerEndpoint{address, ..}) if *address == peer.address));
        if let Some(peer) = control_connection_peer {
            if !self.host_filter.as_ref().is_none_or(|f| f.accept(peer)) {
                warn!(
                    filtered_node_ips = tracing::field::display(metadata
                        .peers
                        .iter()
                        .filter(|peer| self.host_filter.as_ref().is_none_or(|p| p.accept(peer)))
                        .map(|peer| peer.address)
                        .safe_format(", ")
                    ),
                    control_connection_address = ?self.control_connection_state.endpoint().address(),
                    "The node that the control connection is established to \
                    is not accepted by the host filter. Please verify that \
                    the nodes in your initial peers list are accepted by the \
                    host filter. The driver will try to re-establish the \
                    control connection to a different node."
                );

                // Assuming here that known_peers are up-to-date
                if !self.known_peers.is_empty() {
                    let control_connection_endpoint = self
                        .known_peers
                        .choose(&mut rng())
                        .expect("known_peers is empty - should be impossible")
                        .clone();

                    self.control_connection_state = Self::make_control_connection(
                        control_connection_endpoint,
                        self.control_connection_config.clone(),
                        self.request_serverside_timeout,
                    )
                    .await;
                }
            }
        }
    }

    async fn make_control_connection(
        endpoint: UntranslatedEndpoint,
        mut config: ConnectionConfig,
        request_serverside_timeout: Option<Duration>,
    ) -> ControlConnectionState {
        let (sender, receiver) = tokio::sync::mpsc::channel(32);
        // setting event_sender field in connection config will cause control connection to
        // - send REGISTER message to receive server events
        // - send received events via server_event_sender
        config.event_sender = Some(sender);
        let open_result = open_connection(
            &endpoint,
            None,
            &config.to_host_connection_config(&endpoint),
        )
        .await;

        match open_result {
            Ok((con, recv)) => ControlConnectionState::Working(WorkingControlConnection {
                connection: ControlConnection::new(Arc::new(con))
                    .override_serverside_timeout(request_serverside_timeout),
                error_channel: recv,
                events_channel: receiver,
                endpoint,
            }),
            Err(conn_err) => ControlConnectionState::Broken {
                last_error: MetadataError::ConnectionPoolError(ConnectionPoolError::Broken {
                    last_connection_error: conn_err,
                }),
                last_endpoint: endpoint,
            },
        }
    }
}
