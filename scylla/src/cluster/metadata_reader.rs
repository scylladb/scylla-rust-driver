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

pub(crate) struct WorkingControlConnection {
    connection: ControlConnection,
    pub(crate) error_channel: oneshot::Receiver<ConnectionError>,
}

pub(crate) enum ControlConnectionState {
    Working(WorkingControlConnection),
    Broken { last_error: ConnectionError },
}

/// Allows to read current metadata from the cluster
pub(crate) struct MetadataReader {
    control_connection_config: ConnectionConfig,
    request_serverside_timeout: Option<Duration>,
    hostname_resolution_timeout: Option<Duration>,

    control_connection_endpoint: UntranslatedEndpoint,
    pub(crate) control_connection_state: ControlConnectionState,

    // when control connection fails, MetadataReader tries to connect to one of known_peers
    known_peers: Vec<UntranslatedEndpoint>,
    keyspaces_to_fetch: Vec<String>,
    fetch_schema: bool,
    host_filter: Option<Arc<dyn HostFilter>>,

    // When no known peer is reachable, initial known nodes are resolved once again as a fallback
    // and establishing control connection to them is attempted.
    initial_known_nodes: Vec<KnownNode>,
}

impl MetadataReader {
    /// Creates new MetadataReader, which connects to initially_known_peers in the background
    #[expect(clippy::too_many_arguments)]
    pub(crate) async fn new(
        initial_known_nodes: Vec<KnownNode>,
        hostname_resolution_timeout: Option<Duration>,
        mut connection_config: ConnectionConfig,
        request_serverside_timeout: Option<Duration>,
        server_event_sender: mpsc::Sender<Event>,
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

        // setting event_sender field in connection config will cause control connection to
        // - send REGISTER message to receive server events
        // - send received events via server_event_sender
        connection_config.event_sender = Some(server_event_sender);

        let control_connection_state = match Self::make_control_connection(
            control_connection_endpoint.clone(),
            &connection_config,
            request_serverside_timeout,
        )
        .await
        {
            Ok(working_connection) => ControlConnectionState::Working(working_connection),
            Err(e) => ControlConnectionState::Broken { last_error: e },
        };

        Ok(MetadataReader {
            control_connection_config: connection_config,
            control_connection_endpoint,
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

        let address_of_failed_control_connection = self.control_connection_endpoint.address();
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
                let target = self.control_connection_endpoint.address().into_inner();
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
                    .control_connection_endpoint
                    .address()),
                error = %err,
                "Failed to fetch metadata using current control connection"
            );

            self.control_connection_endpoint = peer.clone();
            debug!(
                "Retrying to establish the control connection on {}",
                self.control_connection_endpoint.address()
            );

            self.control_connection_state = match Self::make_control_connection(
                self.control_connection_endpoint.clone(),
                &self.control_connection_config,
                self.request_serverside_timeout,
            )
            .await
            {
                Ok(working_connection) => ControlConnectionState::Working(working_connection),
                Err(e) => ControlConnectionState::Broken { last_error: e },
            };

            result = self.fetch_metadata(initial).await;
        }
        result
    }

    async fn fetch_metadata(&self, initial: bool) -> Result<Metadata, MetadataError> {
        let conn = match &self.control_connection_state {
            ControlConnectionState::Working(working_connection) => &working_connection.connection,
            ControlConnectionState::Broken { last_error: e } => {
                return Err(MetadataError::ConnectionPoolError(
                    ConnectionPoolError::Broken {
                        last_connection_error: e.clone(),
                    },
                ));
            }
        };

        let res = conn
            .query_metadata(
                self.control_connection_endpoint.address().port(),
                &self.keyspaces_to_fetch,
                self.fetch_schema,
            )
            .await;

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
            .find(|peer| matches!(self.control_connection_endpoint, UntranslatedEndpoint::Peer(PeerEndpoint{address, ..}) if address == peer.address));
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
                    control_connection_address = ?self.control_connection_endpoint.address(),
                    "The node that the control connection is established to \
                    is not accepted by the host filter. Please verify that \
                    the nodes in your initial peers list are accepted by the \
                    host filter. The driver will try to re-establish the \
                    control connection to a different node."
                );

                // Assuming here that known_peers are up-to-date
                if !self.known_peers.is_empty() {
                    self.control_connection_endpoint = self
                        .known_peers
                        .choose(&mut rng())
                        .expect("known_peers is empty - should be impossible")
                        .clone();

                    self.control_connection_state = match Self::make_control_connection(
                        self.control_connection_endpoint.clone(),
                        &self.control_connection_config,
                        self.request_serverside_timeout,
                    )
                    .await
                    {
                        Ok(working_connection) => {
                            ControlConnectionState::Working(working_connection)
                        }
                        Err(e) => ControlConnectionState::Broken { last_error: e },
                    };
                }
            }
        }
    }

    async fn make_control_connection(
        endpoint: UntranslatedEndpoint,
        config: &ConnectionConfig,
        request_serverside_timeout: Option<Duration>,
    ) -> Result<WorkingControlConnection, ConnectionError> {
        open_connection(
            &endpoint,
            None,
            &config.to_host_connection_config(&endpoint),
        )
        .await
        .map(|(con, recv)| WorkingControlConnection {
            connection: ControlConnection::new(Arc::new(con))
                .override_serverside_timeout(request_serverside_timeout),
            error_channel: recv,
        })
    }
}
