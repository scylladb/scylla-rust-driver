//! This module holds entities that represent the cluster metadata,
//! which includes:
//! - topology metadata:
//!   - [Peer],
//! - schema metadata:
//!   - [Keyspace],
//!   - [Strategy] - replication strategy employed by a keyspace,
//!   - [Table],
//!   - [Column],
//!   - [ColumnKind],
//!   - [MaterializedView],
//!   - CQL types (re-exported from scylla-cql):
//!     - [ColumnType],
//!     - [NativeType],
//!     - [UserDefinedType],
//!     - [CollectionType],
//!

use crate::cluster::KnownNode;
use crate::cluster::node::resolve_contact_points;
use crate::errors::NewSessionError;
use crate::frame::response::event::Event;
use crate::network::{ConnectionConfig, NodeConnectionPool, PoolConfig, PoolSize};
#[cfg(feature = "metrics")]
use crate::observability::metrics::Metrics;
use crate::policies::host_filter::HostFilter;
use crate::policies::reconnect::ReconnectPolicy;
use crate::routing::Token;
use crate::utils::safe_format::IteratorSafeFormatExt;

use rand::rng;
use rand::seq::{IndexedRandom, SliceRandom};
use scylla_cql::frame::response::result::ColumnSpec;
use std::collections::HashMap;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::Duration;
use thiserror::Error;
use tokio::sync::mpsc;
use tracing::{debug, error, warn};
use uuid::Uuid;

use crate::cluster::node::{NodeAddr, ResolvedContactPoint};
use crate::errors::MetadataError;

// Re-export of CQL types.
pub use scylla_cql::frame::response::result::{
    CollectionType, ColumnType, NativeType, UserDefinedType,
};

use super::control_connection::ControlConnection;

/// Indicates that reading metadata failed, but in a way
/// that we can handle, by throwing out data for a keyspace.
/// It is possible that some of the errors could be handled in even
/// more granular way (e.g. throwing out a single table), but keyspace
/// granularity seems like a good choice given how independent keyspaces
/// are from each other.
#[derive(Clone, Debug, Error)]
pub(crate) enum SingleKeyspaceMetadataError {
    #[error(transparent)]
    MissingUDT(MissingUserDefinedType),
    #[error("Partition key column with position {0} is missing from metadata")]
    IncompletePartitionKey(i32),
    #[error("Clustering key column with position {0} is missing from metadata")]
    IncompleteClusteringKey(i32),
}

/// Allows to read current metadata from the cluster
pub(crate) struct MetadataReader {
    control_connection_pool_config: PoolConfig,
    request_serverside_timeout: Option<Duration>,
    hostname_resolution_timeout: Option<Duration>,

    control_connection_endpoint: UntranslatedEndpoint,
    control_connection: NodeConnectionPool,

    // when control connection fails, MetadataReader tries to connect to one of known_peers
    known_peers: Vec<UntranslatedEndpoint>,
    keyspaces_to_fetch: Vec<String>,
    fetch_schema: bool,
    host_filter: Option<Arc<dyn HostFilter>>,

    // When no known peer is reachable, initial known nodes are resolved once again as a fallback
    // and establishing control connection to them is attempted.
    initial_known_nodes: Vec<KnownNode>,

    // When a control connection breaks, the PoolRefiller of its pool uses the requester
    // to signal ClusterWorker that an immediate metadata refresh is advisable.
    control_connection_repair_requester: mpsc::Sender<()>,

    #[cfg(feature = "metrics")]
    metrics: Arc<Metrics>,
}

/// Describes all metadata retrieved from the cluster
pub(crate) struct Metadata {
    pub(crate) peers: Vec<Peer>,
    pub(crate) keyspaces: HashMap<String, Result<Keyspace, SingleKeyspaceMetadataError>>,
}

/// Represents a node in the cluster, as fetched from the `system.{peers,local}` tables.
#[non_exhaustive] // <- so that we can add more fields in a backwards-compatible way
pub struct Peer {
    /// Unique identifier of the node.
    pub host_id: Uuid,
    /// Address of the node, which may be translatable by the driver or not,
    /// depending on whether the node is a contact point or a peer.
    pub address: NodeAddr,
    /// Tokens owned by this node.
    pub tokens: Vec<Token>,
    /// Datacenter this node is in, if known.
    pub datacenter: Option<String>,
    /// Rack this node is in, if known.
    pub rack: Option<String>,
}

/// An endpoint for a node that the driver is to issue connections to,
/// possibly after prior address translation.
#[derive(Clone, Debug)]
pub(crate) enum UntranslatedEndpoint {
    /// Provided by user in SessionConfig (initial contact points).
    ContactPoint(ResolvedContactPoint),
    /// Fetched in Metadata with `query_peers()`
    Peer(PeerEndpoint),
}

impl UntranslatedEndpoint {
    pub(crate) fn address(&self) -> NodeAddr {
        match *self {
            UntranslatedEndpoint::ContactPoint(ResolvedContactPoint { address, .. }) => {
                NodeAddr::Untranslatable(address)
            }
            UntranslatedEndpoint::Peer(PeerEndpoint { address, .. }) => address,
        }
    }
    pub(crate) fn set_port(&mut self, port: u16) {
        let inner_addr = match self {
            UntranslatedEndpoint::ContactPoint(ResolvedContactPoint { address, .. }) => address,
            UntranslatedEndpoint::Peer(PeerEndpoint { address, .. }) => address.inner_mut(),
        };
        inner_addr.set_port(port);
    }
}

/// Data used to issue connections to a node.
///
/// Fetched from the cluster in Metadata.
#[derive(Clone, Debug)]
pub(crate) struct PeerEndpoint {
    pub(crate) host_id: Uuid,
    pub(crate) address: NodeAddr,
    pub(crate) datacenter: Option<String>,
    pub(crate) rack: Option<String>,
}

impl Peer {
    pub(crate) fn to_peer_endpoint(&self) -> PeerEndpoint {
        PeerEndpoint {
            host_id: self.host_id,
            address: self.address,
            datacenter: self.datacenter.clone(),
            rack: self.rack.clone(),
        }
    }

    pub(crate) fn into_peer_endpoint_and_tokens(self) -> (PeerEndpoint, Vec<Token>) {
        (
            PeerEndpoint {
                host_id: self.host_id,
                address: self.address,
                datacenter: self.datacenter,
                rack: self.rack,
            },
            self.tokens,
        )
    }
}

/// Describes a keyspace in the cluster.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct Keyspace {
    /// Replication strategy used by the keyspace.
    pub strategy: Strategy,
    /// Tables in the keyspace.
    ///
    /// Empty HashMap may as well mean that the client disabled schema fetching in SessionConfig.
    pub tables: HashMap<String, Table>,
    /// Materialized views in the keyspace.
    ///
    /// Empty HashMap may as well mean that the client disabled schema fetching in SessionConfig.
    pub views: HashMap<String, MaterializedView>,
    /// User defined types in the keyspace.
    ///
    /// Empty HashMap may as well mean that the client disabled schema fetching in SessionConfig.
    pub user_defined_types: HashMap<String, Arc<UserDefinedType<'static>>>,
}

/// Describes a table in the cluster.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct Table {
    /// Columns that constitute the table.
    pub columns: HashMap<String, Column>,
    /// Names of the columns that constitute the partition key.
    /// All of the names are guaranteed to be present in `columns` field.
    pub partition_key: Vec<String>,
    /// Names of the columns that constitute the clustering key.
    /// All of the names are guaranteed to be present in `columns` field.
    pub clustering_key: Vec<String>,
    /// Name of the partitioner used by the table.
    pub partitioner: Option<String>,
    /// Column specs for the partition key columns.
    pub(crate) pk_column_specs: Vec<ColumnSpec<'static>>,
}

/// Describes a materialized view in the cluster.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct MaterializedView {
    /// As materialized views are a special kind of table,
    /// they have the same metadata as a table.
    pub view_metadata: Table,
    /// The name of a table that the materialized view is an index of.
    pub base_table_name: String,
}

/// Describes a column of the table.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct Column {
    /// CQL type that the value stored in this column has.
    pub typ: ColumnType<'static>,
    /// Describes role of the column in the table.
    pub kind: ColumnKind,
}

/// Represents a user defined type whose definition is missing from the metadata.
#[derive(Clone, Debug, Error)]
#[error("Missing UDT: {keyspace}, {name}")]
pub(crate) struct MissingUserDefinedType {
    pub(crate) name: String,
    pub(crate) keyspace: String,
}

/// Some columns have a specific meaning in the context of a table,
/// and this meaning is represented by the [ColumnKind] enum.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum ColumnKind {
    /// Just a regular column.
    Regular,
    /// Column that has the same value for all rows in a partition.
    Static,
    /// Column that is part of the clustering key.
    Clustering,
    /// Column that is part of the partition key.
    PartitionKey,
}

/// [ColumnKind] parse error
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ColumnKindFromStrError;

impl std::str::FromStr for ColumnKind {
    type Err = ColumnKindFromStrError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "regular" => Ok(Self::Regular),
            "static" => Ok(Self::Static),
            "clustering" => Ok(Self::Clustering),
            "partition_key" => Ok(Self::PartitionKey),
            _ => Err(ColumnKindFromStrError),
        }
    }
}

/// Replication strategy used by a keyspace.
///
/// This specifies how data is replicated across the cluster.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
// Check triggers because all variants end with "Strategy".
// TODO(2.0): Remove the "Strategy" postfix from variants.
#[expect(clippy::enum_variant_names)]
pub enum Strategy {
    /// _Deprecated in ScyllaDB._
    /// **Use only for a single datacenter and one rack.**
    /// Places the first replica on a node determined by the partitioner.
    /// Additional replicas are placed on the next nodes clockwise in the ring
    /// without considering topology (rack or datacenter location).
    SimpleStrategy {
        /// Replication factor, i.e. how many replicas of each piece of data there are.
        replication_factor: usize,
    },
    /// Use this strategy when you have (or plan to have) your cluster deployed across
    /// multiple datacenters. This strategy specifies how many replicas you want in each
    /// datacenter.
    ///
    /// `NetworkTopologyStrategy` places replicas in the same datacenter by walking the ring
    /// clockwise until reaching the first node in another rack. It attempts to place replicas
    /// on distinct racks because nodes in the same rack (or similar physical grouping) often
    /// fail at the same time due to power, cooling, or network issues.
    NetworkTopologyStrategy {
        /// Replication factors of datacenters with given names, i.e. how many replicas of each piece
        /// of data there are in each datacenter.
        datacenter_repfactors: HashMap<String, usize>,
    },
    /// Used for internal purposes, e.g. for system tables.
    LocalStrategy, // replication_factor == 1
    /// Unknown other strategy, which is not supported by the driver.
    Other {
        /// Name of the strategy.
        name: String,
        /// Additional parameters of the strategy, which the driver does not understand.
        data: HashMap<String, String>,
    },
}

impl Metadata {
    /// Creates new, dummy metadata from a given list of peers.
    ///
    /// It can be used as a replacement for real metadata when initial
    /// metadata read fails.
    pub(crate) fn new_dummy(initial_peers: &[UntranslatedEndpoint]) -> Self {
        let peers = initial_peers
            .iter()
            .enumerate()
            .map(|(id, endpoint)| {
                // Given N nodes, divide the ring into N roughly equal parts
                // and assign them to each node.
                let token = ((id as u128) << 64) / initial_peers.len() as u128;

                Peer {
                    address: endpoint.address(),
                    tokens: vec![Token::new(token as i64)],
                    datacenter: None,
                    rack: None,
                    host_id: Uuid::new_v4(),
                }
            })
            .collect();

        Metadata {
            peers,
            keyspaces: HashMap::new(),
        }
    }
}

impl MetadataReader {
    /// Creates new MetadataReader, which connects to initially_known_peers in the background
    #[expect(clippy::too_many_arguments)]
    pub(crate) async fn new(
        initial_known_nodes: Vec<KnownNode>,
        hostname_resolution_timeout: Option<Duration>,
        control_connection_repair_requester: tokio::sync::mpsc::Sender<()>,
        mut connection_config: ConnectionConfig,
        request_serverside_timeout: Option<Duration>,
        server_event_sender: mpsc::Sender<Event>,
        keyspaces_to_fetch: Vec<String>,
        fetch_schema: bool,
        host_filter: &Option<Arc<dyn HostFilter>>,
        #[cfg(feature = "metrics")] metrics: Arc<Metrics>,
        reconnect_policy: Arc<dyn ReconnectPolicy>,
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

        let control_connection_pool_config = PoolConfig {
            connection_config,

            // We want to have only one connection to receive events from
            pool_size: PoolSize::PerHost(NonZeroUsize::new(1).unwrap()),

            // The shard-aware port won't be used with PerHost pool size anyway,
            // so explicitly disable it here
            can_use_shard_aware_port: false,
            reconnect_policy,
        };

        let control_connection = Self::make_control_connection_pool(
            control_connection_endpoint.clone(),
            &control_connection_pool_config,
            control_connection_repair_requester.clone(),
            #[cfg(feature = "metrics")]
            metrics.clone(),
        );

        Ok(MetadataReader {
            control_connection_pool_config,
            control_connection_endpoint,
            control_connection,
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
            control_connection_repair_requester,
            #[cfg(feature = "metrics")]
            metrics,
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
                    self.handle_unaccepted_host_in_control_connection(&metadata);
                }
                return Ok(metadata);
            }
            Err(err) => err,
        };

        // At this point, we known that fetching metadata on currect control connection failed.
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
                self.handle_unaccepted_host_in_control_connection(metadata);
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
            self.control_connection = Self::make_control_connection_pool(
                self.control_connection_endpoint.clone(),
                &self.control_connection_pool_config,
                self.control_connection_repair_requester.clone(),
                #[cfg(feature = "metrics")]
                Arc::clone(&self.metrics),
            );

            debug!(
                "Retrying to establish the control connection on {}",
                self.control_connection_endpoint.address()
            );
            result = self.fetch_metadata(initial).await;
        }
        result
    }

    async fn fetch_metadata(&self, initial: bool) -> Result<Metadata, MetadataError> {
        // TODO: Timeouts?
        self.control_connection.wait_until_initialized().await;
        let conn = ControlConnection::new(self.control_connection.random_connection()?)
            .override_serverside_timeout(self.request_serverside_timeout);

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

    fn handle_unaccepted_host_in_control_connection(&mut self, metadata: &Metadata) {
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

                    self.control_connection = Self::make_control_connection_pool(
                        self.control_connection_endpoint.clone(),
                        &self.control_connection_pool_config,
                        self.control_connection_repair_requester.clone(),
                        #[cfg(feature = "metrics")]
                        Arc::clone(&self.metrics),
                    );
                }
            }
        }
    }

    fn make_control_connection_pool(
        endpoint: UntranslatedEndpoint,
        pool_config: &PoolConfig,
        refresh_requester: mpsc::Sender<()>,
        #[cfg(feature = "metrics")] metrics: Arc<Metrics>,
    ) -> NodeConnectionPool {
        NodeConnectionPool::new(
            endpoint,
            pool_config,
            None,
            None,
            refresh_requester,
            #[cfg(feature = "metrics")]
            metrics,
        )
    }
}
