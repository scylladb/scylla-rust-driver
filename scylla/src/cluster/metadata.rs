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

use crate::client::pager::{NextPageError, NextRowError, QueryPager};
use crate::cluster::node::resolve_contact_points;
use crate::deserialize::DeserializeOwnedRow;
use crate::errors::{
    DbError, MetadataFetchError, MetadataFetchErrorKind, NewSessionError, RequestAttemptError,
};
use crate::frame::response::event::Event;
use crate::network::{ConnectionConfig, NodeConnectionPool, PoolConfig, PoolSize};
#[cfg(feature = "metrics")]
use crate::observability::metrics::Metrics;
use crate::policies::host_filter::HostFilter;
use crate::routing::Token;
use crate::statement::unprepared::Statement;
use crate::DeserializeRow;
use scylla_cql::utils::parse::{ParseErrorCause, ParseResult, ParserState};

use futures::future::{self, FutureExt};
use futures::stream::{self, StreamExt, TryStreamExt};
use futures::Stream;
use itertools::Itertools;
use rand::seq::{IndexedRandom, SliceRandom};
use rand::{rng, Rng};
use scylla_cql::frame::response::result::{ColumnSpec, TableSpec};
use std::borrow::BorrowMut;
use std::cell::Cell;
use std::collections::HashMap;
use std::fmt::{self, Formatter};
use std::net::{IpAddr, SocketAddr};
use std::num::NonZeroUsize;
use std::str::FromStr;
use std::sync::Arc;
use std::time::{Duration, Instant};
use thiserror::Error;
use tokio::sync::{broadcast, mpsc};
use tracing::{debug, error, trace, warn};
use uuid::Uuid;

use crate::cluster::node::{InternalKnownNode, NodeAddr, ResolvedContactPoint};
use crate::errors::{
    KeyspaceStrategyError, KeyspacesMetadataError, MetadataError, PeersMetadataError, RequestError,
    TablesMetadataError, UdtMetadataError,
};

// Re-export of CQL types.
pub use scylla_cql::frame::response::result::{
    CollectionType, ColumnType, NativeType, UserDefinedType,
};

use super::control_connection::ControlConnection;

type PerKeyspace<T> = HashMap<String, T>;
type PerKeyspaceResult<T, E> = PerKeyspace<Result<T, E>>;
type PerTable<T> = HashMap<String, T>;
type PerKsTable<T> = HashMap<(String, String), T>;
type PerKsTableResult<T, E> = PerKsTable<Result<T, E>>;

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

    control_connection_endpoint: UntranslatedEndpoint,
    control_connection: NodeConnectionPool,

    // when control connection fails, MetadataReader tries to connect to one of known_peers
    known_peers: Vec<UntranslatedEndpoint>,
    keyspaces_to_fetch: Vec<String>,
    fetch_schema: bool,
    host_filter: Option<Arc<dyn HostFilter>>,

    // When no known peer is reachable, initial known nodes are resolved once again as a fallback
    // and establishing control connection to them is attempted.
    initial_known_nodes: Vec<InternalKnownNode>,

    // When a control connection breaks, the PoolRefiller of its pool uses the requester
    // to signal ClusterWorker that an immediate metadata refresh is advisable.
    control_connection_repair_requester: broadcast::Sender<()>,

    #[cfg(feature = "metrics")]
    metrics: Arc<Metrics>,
}

/// Describes all metadata retrieved from the cluster
pub(crate) struct Metadata {
    pub(crate) peers: Vec<Peer>,
    pub(crate) keyspaces: HashMap<String, Result<Keyspace, SingleKeyspaceMetadataError>>,
}

#[non_exhaustive] // <- so that we can add more fields in a backwards-compatible way
pub struct Peer {
    pub host_id: Uuid,
    pub address: NodeAddr,
    pub tokens: Vec<Token>,
    pub datacenter: Option<String>,
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
    pub strategy: Strategy,
    /// Empty HashMap may as well mean that the client disabled schema fetching in SessionConfig
    pub tables: HashMap<String, Table>,
    /// Empty HashMap may as well mean that the client disabled schema fetching in SessionConfig
    pub views: HashMap<String, MaterializedView>,
    /// Empty HashMap may as well mean that the client disabled schema fetching in SessionConfig
    pub user_defined_types: HashMap<String, Arc<UserDefinedType<'static>>>,
}

/// Describes a table in the cluster.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct Table {
    pub columns: HashMap<String, Column>,
    /// Names of the column of partition key.
    /// All of the names are guaranteed to be present in `columns` field.
    pub partition_key: Vec<String>,
    /// Names of the column of clustering key.
    /// All of the names are guaranteed to be present in `columns` field.
    pub clustering_key: Vec<String>,
    pub partitioner: Option<String>,
    pub(crate) pk_column_specs: Vec<ColumnSpec<'static>>,
}

/// Describes a materialized view in the cluster.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct MaterializedView {
    pub view_metadata: Table,
    pub base_table_name: String,
}

/// Describes a column of the table.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct Column {
    pub typ: ColumnType<'static>,
    pub kind: ColumnKind,
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum PreColumnType {
    Native(NativeType),
    Collection {
        frozen: bool,
        typ: PreCollectionType,
    },
    Tuple(Vec<PreColumnType>),
    Vector {
        typ: Box<PreColumnType>,
        dimensions: u16,
    },
    UserDefinedType {
        frozen: bool,
        name: String,
    },
}

impl PreColumnType {
    pub(crate) fn into_cql_type(
        self,
        keyspace_name: &String,
        keyspace_udts: &PerTable<Arc<UserDefinedType<'static>>>,
    ) -> Result<ColumnType<'static>, MissingUserDefinedType> {
        match self {
            PreColumnType::Native(n) => Ok(ColumnType::Native(n)),
            PreColumnType::Collection { frozen, typ: type_ } => type_
                .into_collection_type(keyspace_name, keyspace_udts)
                .map(|inner| ColumnType::Collection { frozen, typ: inner }),
            PreColumnType::Tuple(t) => t
                .into_iter()
                .map(|t| t.into_cql_type(keyspace_name, keyspace_udts))
                .collect::<Result<Vec<ColumnType>, MissingUserDefinedType>>()
                .map(ColumnType::Tuple),
            PreColumnType::Vector {
                typ: type_,
                dimensions,
            } => type_
                .into_cql_type(keyspace_name, keyspace_udts)
                .map(|inner| ColumnType::Vector {
                    typ: Box::new(inner),
                    dimensions,
                }),
            PreColumnType::UserDefinedType { frozen, name } => {
                let definition = match keyspace_udts.get(&name) {
                    Some(def) => def.clone(),
                    None => {
                        return Err(MissingUserDefinedType {
                            name,
                            keyspace: keyspace_name.clone(),
                        })
                    }
                };
                Ok(ColumnType::UserDefinedType { frozen, definition })
            }
        }
    }
}

/// Represents a user defined type whose definition is missing from the metadata.
#[derive(Clone, Debug, Error)]
#[error("Missing UDT: {keyspace}, {name}")]
pub(crate) struct MissingUserDefinedType {
    name: String,
    keyspace: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum PreCollectionType {
    List(Box<PreColumnType>),
    Map(Box<PreColumnType>, Box<PreColumnType>),
    Set(Box<PreColumnType>),
}

impl PreCollectionType {
    pub(crate) fn into_collection_type(
        self,
        keyspace_name: &String,
        keyspace_udts: &PerTable<Arc<UserDefinedType<'static>>>,
    ) -> Result<CollectionType<'static>, MissingUserDefinedType> {
        match self {
            PreCollectionType::List(t) => t
                .into_cql_type(keyspace_name, keyspace_udts)
                .map(|inner| CollectionType::List(Box::new(inner))),
            PreCollectionType::Map(tk, tv) => Ok(CollectionType::Map(
                Box::new(tk.into_cql_type(keyspace_name, keyspace_udts)?),
                Box::new(tv.into_cql_type(keyspace_name, keyspace_udts)?),
            )),
            PreCollectionType::Set(t) => t
                .into_cql_type(keyspace_name, keyspace_udts)
                .map(|inner| CollectionType::Set(Box::new(inner))),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum ColumnKind {
    Regular,
    Static,
    Clustering,
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

#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
#[allow(clippy::enum_variant_names)]
pub enum Strategy {
    SimpleStrategy {
        replication_factor: usize,
    },
    NetworkTopologyStrategy {
        // Replication factors of datacenters with given names
        datacenter_repfactors: HashMap<String, usize>,
    },
    LocalStrategy, // replication_factor == 1
    Other {
        name: String,
        data: HashMap<String, String>,
    },
}

#[derive(Clone, Debug)]
struct InvalidCqlType {
    typ: String,
    position: usize,
    reason: String,
}

impl fmt::Display for InvalidCqlType {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", self)
    }
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
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn new(
        initial_known_nodes: Vec<InternalKnownNode>,
        control_connection_repair_requester: broadcast::Sender<()>,
        mut connection_config: ConnectionConfig,
        request_serverside_timeout: Option<Duration>,
        server_event_sender: mpsc::Sender<Event>,
        keyspaces_to_fetch: Vec<String>,
        fetch_schema: bool,
        host_filter: &Option<Arc<dyn HostFilter>>,
        #[cfg(feature = "metrics")] metrics: Arc<Metrics>,
    ) -> Result<Self, NewSessionError> {
        let (initial_peers, resolved_hostnames) =
            resolve_contact_points(&initial_known_nodes).await;
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
        debug!("Known peers: {:?}", self.known_peers.iter().format(", "));

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
                warn!("Failed to establish control connection and fetch metadata on all known peers. Falling back to initial contact points.");
                let (initial_peers, _hostnames) =
                    resolve_contact_points(&self.initial_known_nodes).await;
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
            Err(error) => error!(
                error = %error,
                "Could not fetch metadata"
            ),
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
            .filter(|peer| host_filter.map_or(true, |f| f.accept(peer)))
            .map(|peer| UntranslatedEndpoint::Peer(peer.to_peer_endpoint()))
            .collect();

        // Check if the host filter isn't accidentally too restrictive,
        // and print an error message about this fact
        if !metadata.peers.is_empty() && self.known_peers.is_empty() {
            error!(
                node_ips = tracing::field::display(
                    metadata.peers.iter().map(|peer| peer.address).format(", ")
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
            if !self.host_filter.as_ref().map_or(true, |f| f.accept(peer)) {
                warn!(
                    filtered_node_ips = tracing::field::display(metadata
                        .peers
                        .iter()
                        .filter(|peer| self.host_filter.as_ref().map_or(true, |p| p.accept(peer)))
                        .map(|peer| peer.address)
                        .format(", ")
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
        refresh_requester: broadcast::Sender<()>,
        #[cfg(feature = "metrics")] metrics: Arc<Metrics>,
    ) -> NodeConnectionPool {
        NodeConnectionPool::new(
            endpoint,
            pool_config,
            None,
            refresh_requester,
            #[cfg(feature = "metrics")]
            metrics,
        )
    }
}

impl ControlConnection {
    async fn query_metadata(
        &self,
        connect_port: u16,
        keyspace_to_fetch: &[String],
        fetch_schema: bool,
    ) -> Result<Metadata, MetadataError> {
        let peers_query = self.query_peers(connect_port);
        let keyspaces_query = self.query_keyspaces(keyspace_to_fetch, fetch_schema);

        let (peers, keyspaces) = tokio::try_join!(peers_query, keyspaces_query)?;

        // There must be at least one peer
        if peers.is_empty() {
            return Err(MetadataError::Peers(PeersMetadataError::EmptyPeers));
        }

        // At least one peer has to have some tokens
        if peers.iter().all(|peer| peer.tokens.is_empty()) {
            return Err(MetadataError::Peers(PeersMetadataError::EmptyTokenLists));
        }

        Ok(Metadata { peers, keyspaces })
    }
}

#[derive(DeserializeRow)]
#[scylla(crate = "scylla_cql")]
struct NodeInfoRow {
    host_id: Option<Uuid>,
    #[scylla(rename = "rpc_address")]
    untranslated_ip_addr: IpAddr,
    #[scylla(rename = "data_center")]
    datacenter: Option<String>,
    rack: Option<String>,
    tokens: Option<Vec<String>>,
}

#[derive(Clone, Copy)]
enum NodeInfoSource {
    Local,
    Peer,
}

impl NodeInfoSource {
    fn describe(&self) -> &'static str {
        match self {
            Self::Local => "local node",
            Self::Peer => "peer",
        }
    }
}

const METADATA_QUERY_PAGE_SIZE: i32 = 1024;

impl ControlConnection {
    async fn query_peers(&self, connect_port: u16) -> Result<Vec<Peer>, MetadataError> {
        let mut peers_query = Statement::new(
            "select host_id, rpc_address, data_center, rack, tokens from system.peers",
        );
        peers_query.set_page_size(METADATA_QUERY_PAGE_SIZE);
        let peers_query_stream = self
            .query_iter(peers_query)
            .map(|pager_res| {
                let pager = pager_res?;
                let rows_stream = pager.rows_stream::<NodeInfoRow>()?;
                Ok::<_, MetadataFetchErrorKind>(rows_stream)
            })
            .into_stream()
            .map(|result| result.map(|stream| stream.map_err(MetadataFetchErrorKind::NextRowError)))
            .try_flatten()
            // Add table context to the error.
            .map_err(|error| MetadataFetchError {
                error,
                table: "system.peers",
            })
            .and_then(|row_result| future::ok((NodeInfoSource::Peer, row_result)));

        let mut local_query =
        Statement::new("select host_id, rpc_address, data_center, rack, tokens from system.local WHERE key='local'");
        local_query.set_page_size(METADATA_QUERY_PAGE_SIZE);
        let local_query_stream = self
            .query_iter(local_query)
            .map(|pager_res| {
                let pager = pager_res?;
                let rows_stream = pager.rows_stream::<NodeInfoRow>()?;
                Ok::<_, MetadataFetchErrorKind>(rows_stream)
            })
            .into_stream()
            .map(|result| result.map(|stream| stream.map_err(MetadataFetchErrorKind::NextRowError)))
            .try_flatten()
            // Add table context to the error.
            .map_err(|error| MetadataFetchError {
                error,
                table: "system.local",
            })
            .and_then(|row_result| future::ok((NodeInfoSource::Local, row_result)));

        let untranslated_rows = stream::select(peers_query_stream, local_query_stream);

        let local_ip: IpAddr = self.get_connect_address().ip();
        let local_address = SocketAddr::new(local_ip, connect_port);

        let translated_peers_futures = untranslated_rows.map(|row_result| async {
            match row_result {
                Ok((source, row)) => Self::create_peer_from_row(source, row, local_address).await,
                Err(err) => {
                    warn!(
                        "system.peers or system.local has an invalid row, skipping it: {}",
                        err
                    );
                    None
                }
            }
        });

        let peers = translated_peers_futures
            .buffer_unordered(256)
            .filter_map(std::future::ready)
            .collect::<Vec<_>>()
            .await;
        Ok(peers)
    }

    async fn create_peer_from_row(
        source: NodeInfoSource,
        row: NodeInfoRow,
        local_address: SocketAddr,
    ) -> Option<Peer> {
        let NodeInfoRow {
            host_id,
            untranslated_ip_addr,
            datacenter,
            rack,
            tokens,
        } = row;

        let host_id = match host_id {
            Some(host_id) => host_id,
            None => {
                warn!("{} (untranslated ip: {}, dc: {:?}, rack: {:?}) has Host ID set to null; skipping node.", source.describe(), untranslated_ip_addr, datacenter, rack);
                return None;
            }
        };

        let connect_port = local_address.port();
        let untranslated_address = SocketAddr::new(untranslated_ip_addr, connect_port);

        let node_addr = match source {
            NodeInfoSource::Local => {
                // For the local node we should use connection's address instead of rpc_address.
                // (The reason is that rpc_address in system.local can be wrong.)
                // Thus, we replace address in local_rows with connection's address.
                // We need to replace rpc_address with control connection address.
                NodeAddr::Untranslatable(local_address)
            }
            NodeInfoSource::Peer => {
                // The usual case - no translation.
                NodeAddr::Translatable(untranslated_address)
            }
        };

        let tokens_str: Vec<String> = tokens.unwrap_or_default();

        // Parse string representation of tokens as integer values
        let tokens: Vec<Token> = match tokens_str
            .iter()
            .map(|s| Token::from_str(s))
            .collect::<Result<Vec<Token>, _>>()
        {
            Ok(parsed) => parsed,
            Err(e) => {
                // FIXME: we could allow the users to provide custom partitioning information
                // in order for it to work with non-standard token sizes.
                // Also, we could implement support for Cassandra's other standard partitioners
                // like RandomPartitioner or ByteOrderedPartitioner.
                trace!("Couldn't parse tokens as 64-bit integers: {}, proceeding with a dummy token. If you're using a partitioner with different token size, consider migrating to murmur3", e);
                vec![Token::new(rand::rng().random::<i64>())]
            }
        };

        Some(Peer {
            host_id,
            address: node_addr,
            tokens,
            datacenter,
            rack,
        })
    }

    fn query_filter_keyspace_name<'a, R>(
        &'a self,
        query_str: &'a str,
        keyspaces_to_fetch: &'a [String],
    ) -> impl Stream<Item = Result<R, MetadataFetchErrorKind>> + 'a
    where
        R: DeserializeOwnedRow + 'static,
    {
        // This function is extracted to reduce monomorphisation penalty:
        // query_filter_keyspace_name() is going to be monomorphised into 5 distinct functions,
        // so it's better to extract the common part.
        async fn make_keyspace_filtered_query_pager(
            conn: &ControlConnection,
            query_str: &str,
            keyspaces_to_fetch: &[String],
        ) -> Result<QueryPager, MetadataFetchErrorKind> {
            if keyspaces_to_fetch.is_empty() {
                let mut query = Statement::new(query_str);
                query.set_page_size(METADATA_QUERY_PAGE_SIZE);

                conn.query_iter(query)
                    .await
                    .map_err(MetadataFetchErrorKind::NextRowError)
            } else {
                let keyspaces = &[keyspaces_to_fetch] as &[&[String]];
                let query_str = format!("{query_str} where keyspace_name in ?");

                let mut query = Statement::new(query_str);
                query.set_page_size(METADATA_QUERY_PAGE_SIZE);

                let prepared = conn.prepare(query).await?;
                let serialized_values = prepared.serialize_values(&keyspaces)?;
                conn.execute_iter(prepared, serialized_values)
                    .await
                    .map_err(MetadataFetchErrorKind::NextRowError)
            }
        }

        let fut = async move {
            let pager =
                make_keyspace_filtered_query_pager(self, query_str, keyspaces_to_fetch).await?;
            let stream: crate::client::pager::TypedRowStream<R> = pager.rows_stream::<R>()?;
            Ok::<_, MetadataFetchErrorKind>(stream)
        };
        fut.into_stream()
            .map(|result| result.map(|stream| stream.map_err(MetadataFetchErrorKind::NextRowError)))
            .try_flatten()
    }

    async fn query_keyspaces(
        &self,
        keyspaces_to_fetch: &[String],
        fetch_schema: bool,
    ) -> Result<PerKeyspaceResult<Keyspace, SingleKeyspaceMetadataError>, MetadataError> {
        let rows = self
            .query_filter_keyspace_name::<(String, HashMap<String, String>)>(
                "select keyspace_name, replication from system_schema.keyspaces",
                keyspaces_to_fetch,
            )
            .map_err(|error| MetadataFetchError {
                error,
                table: "system_schema.keyspaces",
            });

        let (mut all_tables, mut all_views, mut all_user_defined_types) = if fetch_schema {
            let udts = self.query_user_defined_types(keyspaces_to_fetch).await?;
            let mut tables_schema = self.query_tables_schema(keyspaces_to_fetch, &udts).await?;
            (
                // We pass the mutable reference to the same map to the both functions.
                // First function fetches `system_schema.tables`, and removes found
                // table from `tables_schema`.
                // Second does the same for `system_schema.views`.
                // The assumption here is that no keys (table names) can appear in both
                // of those schema table.
                // As far as we know this assumption is true for Scylla and Cassandra.
                self.query_tables(keyspaces_to_fetch, &mut tables_schema)
                    .await?,
                self.query_views(keyspaces_to_fetch, &mut tables_schema)
                    .await?,
                udts,
            )
        } else {
            (HashMap::new(), HashMap::new(), HashMap::new())
        };

        rows.map(|row_result| {
            let (keyspace_name, strategy_map) = row_result?;

            let strategy: Strategy = strategy_from_string_map(strategy_map).map_err(|error| {
                KeyspacesMetadataError::Strategy {
                    keyspace: keyspace_name.clone(),
                    error,
                }
            })?;
            let tables = all_tables
                .remove(&keyspace_name)
                .unwrap_or_else(|| Ok(HashMap::new()));
            let views = all_views
                .remove(&keyspace_name)
                .unwrap_or_else(|| Ok(HashMap::new()));
            let user_defined_types = all_user_defined_types
                .remove(&keyspace_name)
                .unwrap_or_else(|| Ok(HashMap::new()));

            // As you can notice, in this file we generally operate on two layers of errors:
            // - Outer (MetadataError) if something went wrong with querying the cluster.
            // - Inner (SingleKeyspaceMetadataError) if the fetched metadata turned out to not be fully consistent.
            // If there is an inner error, we want to drop metadata for the whole keyspace.
            // This logic checks if either tables, views, or UDTs have such inner error, and returns it if so.
            // Notice that in the error branch, return value is wrapped in `Ok` - but this is the
            // outer error, so it just means there was no error while querying the cluster.
            let (tables, views, user_defined_types) = match (tables, views, user_defined_types) {
                (Ok(t), Ok(v), Ok(u)) => (t, v, u),
                (Err(e), _, _) | (_, Err(e), _) => return Ok((keyspace_name, Err(e))),
                (_, _, Err(e)) => {
                    return Ok((
                        keyspace_name,
                        Err(SingleKeyspaceMetadataError::MissingUDT(e)),
                    ))
                }
            };

            let keyspace = Keyspace {
                strategy,
                tables,
                views,
                user_defined_types,
            };

            Ok((keyspace_name, Ok(keyspace)))
        })
        .try_collect()
        .await
    }
}

#[derive(DeserializeRow, Debug)]
#[scylla(crate = "crate")]
struct UdtRow {
    keyspace_name: String,
    type_name: String,
    field_names: Vec<String>,
    field_types: Vec<String>,
}

#[derive(Debug)]
struct UdtRowWithParsedFieldTypes {
    keyspace_name: String,
    type_name: String,
    field_names: Vec<String>,
    field_types: Vec<PreColumnType>,
}

impl TryFrom<UdtRow> for UdtRowWithParsedFieldTypes {
    type Error = InvalidCqlType;
    fn try_from(udt_row: UdtRow) -> Result<Self, InvalidCqlType> {
        let UdtRow {
            keyspace_name,
            type_name,
            field_names,
            field_types,
        } = udt_row;
        let field_types = field_types
            .into_iter()
            .map(|type_| map_string_to_cql_type(&type_))
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Self {
            keyspace_name,
            type_name,
            field_names,
            field_types,
        })
    }
}

impl ControlConnection {
    async fn query_user_defined_types(
        &self,
        keyspaces_to_fetch: &[String],
    ) -> Result<
        PerKeyspaceResult<PerTable<Arc<UserDefinedType<'static>>>, MissingUserDefinedType>,
        MetadataError,
    > {
        let rows = self.query_filter_keyspace_name::<UdtRow>(
        "select keyspace_name, type_name, field_names, field_types from system_schema.types",
        keyspaces_to_fetch,
    )
    .map_err(|error| MetadataFetchError {
        error,
        table: "system_schema.types",
    });

        let mut udt_rows: Vec<UdtRowWithParsedFieldTypes> = rows
            .map(|row_result| {
                let udt_row = row_result?.try_into().map_err(|err: InvalidCqlType| {
                    MetadataError::Udts(UdtMetadataError::InvalidCqlType {
                        typ: err.typ,
                        position: err.position,
                        reason: err.reason,
                    })
                })?;

                Ok::<_, MetadataError>(udt_row)
            })
            .try_collect()
            .await?;

        let instant_before_toposort = Instant::now();
        topo_sort_udts(&mut udt_rows)?;
        let toposort_elapsed = instant_before_toposort.elapsed();
        debug!(
            "Toposort of UDT definitions took {:.2} ms (udts len: {})",
            toposort_elapsed.as_secs_f64() * 1000.,
            udt_rows.len(),
        );

        let mut udts = HashMap::new();
        'udts_loop: for udt_row in udt_rows {
            let UdtRowWithParsedFieldTypes {
                keyspace_name,
                type_name,
                field_names,
                field_types,
            } = udt_row;

            let keyspace_name_clone = keyspace_name.clone();
            let keyspace_udts_result = udts
                .entry(keyspace_name)
                .or_insert_with(|| Ok(HashMap::new()));

            // If there was previously an error in this keyspace then it makes no sense to process this UDT.
            let keyspace_udts = match keyspace_udts_result {
                Ok(udts) => udts,
                Err(_) => continue,
            };

            let mut fields = Vec::with_capacity(field_names.len());

            for (field_name, field_type) in field_names.into_iter().zip(field_types.into_iter()) {
                match field_type.into_cql_type(&keyspace_name_clone, keyspace_udts) {
                    Ok(cql_type) => fields.push((field_name.into(), cql_type)),
                    Err(e) => {
                        *keyspace_udts_result = Err(e);
                        continue 'udts_loop;
                    }
                }
            }

            let udt = Arc::new(UserDefinedType {
                name: type_name.clone().into(),
                keyspace: keyspace_name_clone.into(),
                field_types: fields,
            });

            keyspace_udts.insert(type_name, udt);
        }

        Ok(udts)
    }
}

fn topo_sort_udts(udts: &mut Vec<UdtRowWithParsedFieldTypes>) -> Result<(), UdtMetadataError> {
    fn do_with_referenced_udts(what: &mut impl FnMut(&str), pre_cql_type: &PreColumnType) {
        match pre_cql_type {
            PreColumnType::Native(_) => (),
            PreColumnType::Collection { typ: type_, .. } => match type_ {
                PreCollectionType::List(t) | PreCollectionType::Set(t) => {
                    do_with_referenced_udts(what, t)
                }
                PreCollectionType::Map(t1, t2) => {
                    do_with_referenced_udts(what, t1);
                    do_with_referenced_udts(what, t2);
                }
            },
            PreColumnType::Tuple(types) => types
                .iter()
                .for_each(|type_| do_with_referenced_udts(what, type_)),
            PreColumnType::Vector { typ: type_, .. } => do_with_referenced_udts(what, type_),
            PreColumnType::UserDefinedType { name, .. } => what(name),
        }
    }

    // Build an indegree map: for each node in the graph, how many directly depending types it has.
    let mut indegs = udts
        .drain(..)
        .map(|def| {
            (
                (def.keyspace_name.clone(), def.type_name.clone()),
                (def, Cell::new(0u32)),
            )
        })
        .collect::<HashMap<_, _>>();

    // For each node in the graph...
    for (def, _) in indegs.values() {
        let mut increment_referred_udts = |type_name: &str| {
            let deg = indegs
                .get(&(def.keyspace_name.clone(), type_name.to_string()))
                .map(|(_, count)| count);

            if let Some(deg_cell) = deg {
                deg_cell.set(deg_cell.get() + 1);
            }
        };

        // For each type referred by the node...
        for field_type in def.field_types.iter() {
            do_with_referenced_udts(&mut increment_referred_udts, field_type);
        }
    }

    let mut sorted = Vec::with_capacity(indegs.len());
    let mut next_idx = 0;

    // Schedule keys that had an initial indeg of 0
    for (key, _) in indegs.iter().filter(|(_, (_, deg))| deg.get() == 0) {
        sorted.push(key);
    }

    while let Some(key @ (keyspace, _type_name)) = sorted.get(next_idx).copied() {
        next_idx += 1;
        // Decrement the counters of all UDTs that this UDT depends upon
        // and then schedule them if their counter drops to 0
        let mut decrement_referred_udts = |type_name: &str| {
            let key_value = indegs.get_key_value(&(keyspace.clone(), type_name.to_string()));

            if let Some((ref_key, (_, cnt))) = key_value {
                let new_cnt = cnt.get() - 1;
                cnt.set(new_cnt);
                if new_cnt == 0 {
                    sorted.push(ref_key);
                }
            }
        };

        let def = &indegs.get(key).unwrap().0;
        // For each type referred by the node...
        for field_type in def.field_types.iter() {
            do_with_referenced_udts(&mut decrement_referred_udts, field_type);
        }
    }

    if sorted.len() < indegs.len() {
        // Some UDTs could not become leaves in the graph, which implies cycles.
        return Err(UdtMetadataError::CircularTypeDependency);
    }

    let owned_sorted = sorted.into_iter().cloned().collect::<Vec<_>>();
    assert!(udts.is_empty());
    for key in owned_sorted.into_iter().rev() {
        udts.push(indegs.remove(&key).unwrap().0);
    }

    Ok(())
}

#[cfg(test)]
mod toposort_tests {
    use crate::test_utils::setup_tracing;

    use super::{topo_sort_udts, UdtRow, UdtRowWithParsedFieldTypes};

    const KEYSPACE1: &str = "KEYSPACE1";
    const KEYSPACE2: &str = "KEYSPACE2";

    fn make_udt_row(
        keyspace_name: String,
        type_name: String,
        field_types: Vec<String>,
    ) -> UdtRowWithParsedFieldTypes {
        UdtRow {
            keyspace_name,
            type_name,
            field_names: vec!["udt_field".into(); field_types.len()],
            field_types,
        }
        .try_into()
        .unwrap()
    }

    fn get_udt_idx(
        toposorted: &[UdtRowWithParsedFieldTypes],
        keyspace_name: &str,
        type_name: &str,
    ) -> usize {
        toposorted
            .iter()
            .enumerate()
            .find_map(|(idx, def)| {
                (def.type_name == type_name && def.keyspace_name == keyspace_name).then_some(idx)
            })
            .unwrap()
    }

    #[test]
    #[ntest::timeout(1000)]
    fn test_udt_topo_sort_valid_case() {
        setup_tracing();
        // UDTs dependencies on each other (arrow A -> B signifies that type B is composed of type A):
        //
        // KEYSPACE1
        //      F -->+
        //      ^    |
        //      |    |
        // A -> B -> C
        // |    ^
        // + -> D
        //
        // E   (E is an independent UDT)
        //
        // KEYSPACE2
        // B -> A -> C
        // ^    ^
        // D -> E

        let mut udts = vec![
            make_udt_row(KEYSPACE1.into(), "A".into(), vec!["blob".into()]),
            make_udt_row(KEYSPACE1.into(), "B".into(), vec!["A".into(), "D".into()]),
            make_udt_row(
                KEYSPACE1.into(),
                "C".into(),
                vec!["blob".into(), "B".into(), "list<map<F, text>>".into()],
            ),
            make_udt_row(
                KEYSPACE1.into(),
                "D".into(),
                vec!["A".into(), "blob".into()],
            ),
            make_udt_row(KEYSPACE1.into(), "E".into(), vec!["blob".into()]),
            make_udt_row(
                KEYSPACE1.into(),
                "F".into(),
                vec!["B".into(), "blob".into()],
            ),
            make_udt_row(
                KEYSPACE2.into(),
                "A".into(),
                vec!["B".into(), "tuple<E, E>".into()],
            ),
            make_udt_row(KEYSPACE2.into(), "B".into(), vec!["map<text, D>".into()]),
            make_udt_row(KEYSPACE2.into(), "C".into(), vec!["frozen<A>".into()]),
            make_udt_row(KEYSPACE2.into(), "D".into(), vec!["blob".into()]),
            make_udt_row(KEYSPACE2.into(), "E".into(), vec!["D".into()]),
        ];

        topo_sort_udts(&mut udts).unwrap();

        assert!(get_udt_idx(&udts, KEYSPACE1, "A") < get_udt_idx(&udts, KEYSPACE1, "B"));
        assert!(get_udt_idx(&udts, KEYSPACE1, "A") < get_udt_idx(&udts, KEYSPACE1, "D"));
        assert!(get_udt_idx(&udts, KEYSPACE1, "B") < get_udt_idx(&udts, KEYSPACE1, "C"));
        assert!(get_udt_idx(&udts, KEYSPACE1, "B") < get_udt_idx(&udts, KEYSPACE1, "F"));
        assert!(get_udt_idx(&udts, KEYSPACE1, "F") < get_udt_idx(&udts, KEYSPACE1, "C"));
        assert!(get_udt_idx(&udts, KEYSPACE1, "D") < get_udt_idx(&udts, KEYSPACE1, "B"));

        assert!(get_udt_idx(&udts, KEYSPACE2, "B") < get_udt_idx(&udts, KEYSPACE2, "A"));
        assert!(get_udt_idx(&udts, KEYSPACE2, "D") < get_udt_idx(&udts, KEYSPACE2, "B"));
        assert!(get_udt_idx(&udts, KEYSPACE2, "D") < get_udt_idx(&udts, KEYSPACE2, "E"));
        assert!(get_udt_idx(&udts, KEYSPACE2, "E") < get_udt_idx(&udts, KEYSPACE2, "A"));
        assert!(get_udt_idx(&udts, KEYSPACE2, "A") < get_udt_idx(&udts, KEYSPACE2, "C"));
    }

    #[test]
    #[ntest::timeout(1000)]
    fn test_udt_topo_sort_detects_cycles() {
        setup_tracing();
        const KEYSPACE1: &str = "KEYSPACE1";
        let tests = [
            // test 1
            // A depends on itself.
            vec![make_udt_row(
                KEYSPACE1.into(),
                "A".into(),
                vec!["blob".into(), "A".into()],
            )],
            // test 2
            // A depends on B, which depends on A; also, there is an independent E.
            vec![
                make_udt_row(
                    KEYSPACE1.into(),
                    "A".into(),
                    vec!["blob".into(), "B".into()],
                ),
                make_udt_row(
                    KEYSPACE1.into(),
                    "B".into(),
                    vec!["int".into(), "map<text, A>".into()],
                ),
                make_udt_row(KEYSPACE1.into(), "E".into(), vec!["text".into()]),
            ],
        ];

        for mut udts in tests {
            topo_sort_udts(&mut udts).unwrap_err();
        }
    }

    #[test]
    #[ntest::timeout(1000)]
    fn test_udt_topo_sort_ignores_invalid_metadata() {
        setup_tracing();
        // A depends on B, which depends on unknown C; also, there is an independent E.
        let mut udts = vec![
            make_udt_row(
                KEYSPACE1.into(),
                "A".into(),
                vec!["blob".into(), "B".into()],
            ),
            make_udt_row(
                KEYSPACE1.into(),
                "B".into(),
                vec!["int".into(), "map<text, C>".into()],
            ),
            make_udt_row(KEYSPACE1.into(), "E".into(), vec!["text".into()]),
        ];

        topo_sort_udts(&mut udts).unwrap();

        assert!(get_udt_idx(&udts, KEYSPACE1, "B") < get_udt_idx(&udts, KEYSPACE1, "A"));
    }
}

impl ControlConnection {
    async fn query_tables(
        &self,
        keyspaces_to_fetch: &[String],
        tables: &mut PerKsTableResult<Table, SingleKeyspaceMetadataError>,
    ) -> Result<PerKeyspaceResult<PerTable<Table>, SingleKeyspaceMetadataError>, MetadataError>
    {
        let rows = self
            .query_filter_keyspace_name::<(String, String)>(
                "SELECT keyspace_name, table_name FROM system_schema.tables",
                keyspaces_to_fetch,
            )
            .map_err(|error| MetadataFetchError {
                error,
                table: "system_schema.tables",
            });
        let mut result = HashMap::new();

        rows.map(|row_result| {
            let keyspace_and_table_name = row_result?;

            let table = tables.remove(&keyspace_and_table_name).unwrap_or(Ok(Table {
                columns: HashMap::new(),
                partition_key: vec![],
                clustering_key: vec![],
                partitioner: None,
                pk_column_specs: vec![],
            }));

            let mut entry = result
                .entry(keyspace_and_table_name.0)
                .or_insert_with(|| Ok(HashMap::new()));
            match (&mut entry, table) {
                (Ok(tables), Ok(table)) => {
                    let _ = tables.insert(keyspace_and_table_name.1, table);
                }
                (Err(_), _) => (),
                (Ok(_), Err(e)) => *entry = Err(e),
            };

            Ok::<_, MetadataError>(())
        })
        .try_for_each(|_| future::ok(()))
        .await?;

        Ok(result)
    }

    async fn query_views(
        &self,
        keyspaces_to_fetch: &[String],
        tables: &mut PerKsTableResult<Table, SingleKeyspaceMetadataError>,
    ) -> Result<
        PerKeyspaceResult<PerTable<MaterializedView>, SingleKeyspaceMetadataError>,
        MetadataError,
    > {
        let rows = self
            .query_filter_keyspace_name::<(String, String, String)>(
                "SELECT keyspace_name, view_name, base_table_name FROM system_schema.views",
                keyspaces_to_fetch,
            )
            .map_err(|error| MetadataFetchError {
                error,
                table: "system_schema.views",
            });

        let mut result = HashMap::new();

        rows.map(|row_result| {
            let (keyspace_name, view_name, base_table_name) = row_result?;

            let keyspace_and_view_name = (keyspace_name, view_name);

            let materialized_view = tables
                .remove(&keyspace_and_view_name)
                .unwrap_or(Ok(Table {
                    columns: HashMap::new(),
                    partition_key: vec![],
                    clustering_key: vec![],
                    partitioner: None,
                    pk_column_specs: vec![],
                }))
                .map(|table| MaterializedView {
                    view_metadata: table,
                    base_table_name,
                });

            let mut entry = result
                .entry(keyspace_and_view_name.0)
                .or_insert_with(|| Ok(HashMap::new()));

            match (&mut entry, materialized_view) {
                (Ok(views), Ok(view)) => {
                    let _ = views.insert(keyspace_and_view_name.1, view);
                }
                (Err(_), _) => (),
                (Ok(_), Err(e)) => *entry = Err(e),
            };

            Ok::<_, MetadataError>(())
        })
        .try_for_each(|_| future::ok(()))
        .await?;

        Ok(result)
    }

    async fn query_tables_schema(
        &self,
        keyspaces_to_fetch: &[String],
        udts: &PerKeyspaceResult<PerTable<Arc<UserDefinedType<'static>>>, MissingUserDefinedType>,
    ) -> Result<PerKsTableResult<Table, SingleKeyspaceMetadataError>, MetadataError> {
        // Upon migration from thrift to CQL, Cassandra internally creates a surrogate column "value" of
        // type EmptyType for dense tables. This resolves into this CQL type name.
        // This column shouldn't be exposed to the user but is currently exposed in system tables.
        const THRIFT_EMPTY_TYPE: &str = "empty";

        type RowType = (String, String, String, String, i32, String);

        let rows = self.query_filter_keyspace_name::<RowType>(
        "select keyspace_name, table_name, column_name, kind, position, type from system_schema.columns",
        keyspaces_to_fetch
    ).map_err(|error| MetadataFetchError {
        error,
        table: "system_schema.columns",
    });

        let empty_ok_map = Ok(HashMap::new());

        let mut tables_schema: HashMap<_, Result<_, SingleKeyspaceMetadataError>> = HashMap::new();

        rows.map(|row_result| {
            let (keyspace_name, table_name, column_name, kind, position, type_) = row_result?;

            if type_ == THRIFT_EMPTY_TYPE {
                return Ok::<_, MetadataError>(());
            }

            let keyspace_udts: &PerTable<Arc<UserDefinedType<'static>>> =
                match udts.get(&keyspace_name).unwrap_or(&empty_ok_map) {
                    Ok(udts) => udts,
                    Err(e) => {
                        // There are two things we could do here
                        // 1. Not inserting, just returning. In that case the keyspaces containing
                        //    tables that have a column with a broken UDT will not be present in
                        //    the output of this function at all.
                        // 2. Inserting an error (which requires cloning it). In that case,
                        //    keyspace containing a table with broken UDT will have the error
                        //    cloned from this UDT.
                        //
                        // Solution number 1 seems weird because it can be seen as silencing
                        // the error: we have data for a keyspace, but we just don't put
                        // it in the result at all.
                        // Solution 2 is also not perfect because it:
                        // - Returns error for the keyspace even if the broken UDT is not used in any table.
                        // - Doesn't really distinguish between a table using a broken UDT and
                        //   a keyspace just containing some broken UDTs.
                        //
                        // I chose solution 2. Its first problem is not really important because
                        // the caller will error out the entire keyspace anyway. The second problem
                        // is minor enough to ignore. Note that the first issue also applies to
                        // solution 1: but the keyspace won't be present in the result at all,
                        // which is arguably worse.
                        tables_schema.insert(
                            (keyspace_name, table_name),
                            Err(SingleKeyspaceMetadataError::MissingUDT(e.clone())),
                        );
                        return Ok::<_, MetadataError>(());
                    }
                };
            let pre_cql_type = map_string_to_cql_type(&type_).map_err(|err: InvalidCqlType| {
                TablesMetadataError::InvalidCqlType {
                    typ: err.typ,
                    position: err.position,
                    reason: err.reason,
                }
            })?;
            let cql_type = match pre_cql_type.into_cql_type(&keyspace_name, keyspace_udts) {
                Ok(t) => t,
                Err(e) => {
                    tables_schema.insert(
                        (keyspace_name, table_name),
                        Err(SingleKeyspaceMetadataError::MissingUDT(e)),
                    );
                    return Ok::<_, MetadataError>(());
                }
            };

            let kind = ColumnKind::from_str(&kind).map_err(|_| {
                TablesMetadataError::UnknownColumnKind {
                    keyspace_name: keyspace_name.clone(),
                    table_name: table_name.clone(),
                    column_name: column_name.clone(),
                    column_kind: kind,
                }
            })?;

            let Ok(entry) = tables_schema
                .entry((keyspace_name, table_name))
                .or_insert(Ok((
                    HashMap::new(), // columns
                    Vec::new(),     // partition key
                    Vec::new(),     // clustering key
                )))
            else {
                // This table was previously marked as broken, no way to insert anything.
                return Ok::<_, MetadataError>(());
            };

            if kind == ColumnKind::PartitionKey || kind == ColumnKind::Clustering {
                let key_list: &mut Vec<(i32, String)> = if kind == ColumnKind::PartitionKey {
                    entry.1.borrow_mut()
                } else {
                    entry.2.borrow_mut()
                };
                key_list.push((position, column_name.clone()));
            }

            entry.0.insert(
                column_name,
                Column {
                    typ: cql_type,
                    kind,
                },
            );

            Ok::<_, MetadataError>(())
        })
        .try_for_each(|_| future::ok(()))
        .await?;

        let mut all_partitioners = self.query_table_partitioners().await?;
        let mut result = HashMap::new();

        'tables_loop: for ((keyspace_name, table_name), table_result) in tables_schema {
            let keyspace_and_table_name = (keyspace_name, table_name);

            #[allow(clippy::type_complexity)]
            let (columns, partition_key_columns, clustering_key_columns): (
                HashMap<String, Column>,
                Vec<(i32, String)>,
                Vec<(i32, String)>,
            ) = match table_result {
                Ok(table) => table,
                Err(e) => {
                    let _ = result.insert(keyspace_and_table_name, Err(e));
                    continue;
                }
            };

            fn validate_key_columns(
                mut key_columns: Vec<(i32, String)>,
            ) -> Result<Vec<String>, i32> {
                key_columns.sort_unstable_by_key(|(position, _)| *position);

                key_columns
                    .into_iter()
                    .enumerate()
                    .map(|(idx, (position, column_name))| {
                        // unwrap: I don't see the point of handling the scenario of fetching over
                        // 2 * 10^9 columns.
                        let idx: i32 = idx.try_into().unwrap();
                        if idx == position {
                            Ok(column_name)
                        } else {
                            Err(idx)
                        }
                    })
                    .collect::<Result<Vec<_>, _>>()
            }

            let partition_key = match validate_key_columns(partition_key_columns) {
                Ok(partition_key_columns) => partition_key_columns,
                Err(position) => {
                    result.insert(
                        keyspace_and_table_name,
                        Err(SingleKeyspaceMetadataError::IncompletePartitionKey(
                            position,
                        )),
                    );
                    continue 'tables_loop;
                }
            };

            let clustering_key = match validate_key_columns(clustering_key_columns) {
                Ok(clustering_key_columns) => clustering_key_columns,
                Err(position) => {
                    result.insert(
                        keyspace_and_table_name,
                        Err(SingleKeyspaceMetadataError::IncompleteClusteringKey(
                            position,
                        )),
                    );
                    continue 'tables_loop;
                }
            };

            let partitioner = all_partitioners
                .remove(&keyspace_and_table_name)
                .unwrap_or_default();

            // unwrap of get() result: all column names in `partition_key` are at this
            // point guaranteed to be present in `columns`. See the construction of `partition_key`
            let pk_column_specs = partition_key
                .iter()
                .map(|column_name| (column_name, columns.get(column_name).unwrap().clone().typ))
                .map(|(name, typ)| {
                    let table_spec = TableSpec::owned(
                        keyspace_and_table_name.0.clone(),
                        keyspace_and_table_name.1.clone(),
                    );
                    ColumnSpec::owned(name.to_owned(), typ, table_spec)
                })
                .collect();

            result.insert(
                keyspace_and_table_name,
                Ok(Table {
                    columns,
                    partition_key,
                    clustering_key,
                    partitioner,
                    pk_column_specs,
                }),
            );
        }

        Ok(result)
    }
}

fn map_string_to_cql_type(typ: &str) -> Result<PreColumnType, InvalidCqlType> {
    match parse_cql_type(ParserState::new(typ)) {
        Err(err) => Err(InvalidCqlType {
            typ: typ.to_string(),
            position: err.calculate_position(typ).unwrap_or(0),
            reason: err.get_cause().to_string(),
        }),
        Ok((_, p)) if !p.is_at_eof() => Err(InvalidCqlType {
            typ: typ.to_string(),
            position: p.calculate_position(typ).unwrap_or(0),
            reason: "leftover characters".to_string(),
        }),
        Ok((typ, _)) => Ok(typ),
    }
}

fn parse_cql_type(p: ParserState<'_>) -> ParseResult<(PreColumnType, ParserState<'_>)> {
    if let Ok(p) = p.accept("frozen<") {
        let (inner_type, p) = parse_cql_type(p)?;
        let p = p.accept(">")?;

        let frozen_type = freeze_type(inner_type);

        Ok((frozen_type, p))
    } else if let Ok(p) = p.accept("map<") {
        let (key, p) = parse_cql_type(p)?;
        let p = p.accept(",")?.skip_white();
        let (value, p) = parse_cql_type(p)?;
        let p = p.accept(">")?;

        let typ = PreColumnType::Collection {
            frozen: false,
            typ: PreCollectionType::Map(Box::new(key), Box::new(value)),
        };

        Ok((typ, p))
    } else if let Ok(p) = p.accept("list<") {
        let (inner_type, p) = parse_cql_type(p)?;
        let p = p.accept(">")?;

        let typ = PreColumnType::Collection {
            frozen: false,
            typ: PreCollectionType::List(Box::new(inner_type)),
        };

        Ok((typ, p))
    } else if let Ok(p) = p.accept("set<") {
        let (inner_type, p) = parse_cql_type(p)?;
        let p = p.accept(">")?;

        let typ = PreColumnType::Collection {
            frozen: false,
            typ: PreCollectionType::Set(Box::new(inner_type)),
        };

        Ok((typ, p))
    } else if let Ok(p) = p.accept("tuple<") {
        let mut types = Vec::new();
        let p = p.parse_while(|p| {
            let (inner_type, p) = parse_cql_type(p)?;
            types.push(inner_type);

            if let Ok(p) = p.accept(",") {
                let p = p.skip_white();
                Ok((true, p))
            } else if let Ok(p) = p.accept(">") {
                Ok((false, p))
            } else {
                Err(p.error(ParseErrorCause::Other("expected \",\" or \">\"")))
            }
        })?;

        Ok((PreColumnType::Tuple(types), p))
    } else if let Ok(p) = p.accept("vector<") {
        let (inner_type, p) = parse_cql_type(p)?;

        let p = p.skip_white();
        let p = p.accept(",")?;
        let p = p.skip_white();
        let (size, p) = p.parse_u16()?;
        let p = p.skip_white();
        let p = p.accept(">")?;

        let typ = PreColumnType::Vector {
            typ: Box::new(inner_type),
            dimensions: size,
        };

        Ok((typ, p))
    } else if let Ok((typ, p)) = parse_native_type(p) {
        Ok((PreColumnType::Native(typ), p))
    } else if let Ok((name, p)) = parse_user_defined_type(p) {
        let typ = PreColumnType::UserDefinedType {
            frozen: false,
            name: name.to_string(),
        };
        Ok((typ, p))
    } else {
        Err(p.error(ParseErrorCause::Other("invalid cql type")))
    }
}

fn parse_native_type(p: ParserState) -> ParseResult<(NativeType, ParserState)> {
    let (tok, p) = p.take_while(|c| c.is_alphanumeric() || c == '_');
    let typ = match tok {
        "ascii" => NativeType::Ascii,
        "boolean" => NativeType::Boolean,
        "blob" => NativeType::Blob,
        "counter" => NativeType::Counter,
        "date" => NativeType::Date,
        "decimal" => NativeType::Decimal,
        "double" => NativeType::Double,
        "duration" => NativeType::Duration,
        "float" => NativeType::Float,
        "int" => NativeType::Int,
        "bigint" => NativeType::BigInt,
        "text" => NativeType::Text,
        "timestamp" => NativeType::Timestamp,
        "inet" => NativeType::Inet,
        "smallint" => NativeType::SmallInt,
        "tinyint" => NativeType::TinyInt,
        "time" => NativeType::Time,
        "timeuuid" => NativeType::Timeuuid,
        "uuid" => NativeType::Uuid,
        "varint" => NativeType::Varint,
        _ => return Err(p.error(ParseErrorCause::Other("invalid native type"))),
    };
    Ok((typ, p))
}

fn parse_user_defined_type(p: ParserState) -> ParseResult<(&str, ParserState)> {
    // Java identifiers allow letters, underscores and dollar signs at any position
    // and digits in non-first position. Dots are accepted here because the names
    // are usually fully qualified.
    let (tok, p) = p.take_while(|c| c.is_alphanumeric() || c == '.' || c == '_' || c == '$');
    if tok.is_empty() {
        return Err(p.error(ParseErrorCause::Other("invalid user defined type")));
    }
    Ok((tok, p))
}

fn freeze_type(typ: PreColumnType) -> PreColumnType {
    match typ {
        PreColumnType::Collection { typ: type_, .. } => PreColumnType::Collection {
            frozen: true,
            typ: type_,
        },
        PreColumnType::UserDefinedType { name, .. } => {
            PreColumnType::UserDefinedType { frozen: true, name }
        }
        other => other,
    }
}

impl ControlConnection {
    async fn query_table_partitioners(
        &self,
    ) -> Result<PerKsTable<Option<String>>, MetadataFetchError> {
        fn create_err(err: impl Into<MetadataFetchErrorKind>) -> MetadataFetchError {
            MetadataFetchError {
                error: err.into(),
                table: "system_schema.scylla_tables",
            }
        }

        let mut partitioner_query = Statement::new(
            "select keyspace_name, table_name, partitioner from system_schema.scylla_tables",
        );
        partitioner_query.set_page_size(METADATA_QUERY_PAGE_SIZE);

        let rows = self
            .query_iter(partitioner_query)
            .map(|pager_res| {
                let pager = pager_res.map_err(create_err)?;
                let stream = pager
                    .rows_stream::<(String, String, Option<String>)>()
                    // Map the error of Result<TypedRowStream, TypecheckError>
                    .map_err(create_err)?
                    // Map the error of single stream iteration (NextRowError)
                    .map_err(create_err);
                Ok::<_, MetadataFetchError>(stream)
            })
            .into_stream()
            .try_flatten();

        let result = rows
            .map(|row_result| {
                let (keyspace_name, table_name, partitioner) = row_result?;
                Ok::<_, MetadataFetchError>(((keyspace_name, table_name), partitioner))
            })
            .try_collect::<HashMap<_, _>>()
            .await;

        match result {
            // FIXME: This match catches all database errors with this error code despite the fact
            // that we are only interested in the ones resulting from non-existent table
            // system_schema.scylla_tables.
            // For more information please refer to https://github.com/scylladb/scylla-rust-driver/pull/349#discussion_r762050262
            Err(MetadataFetchError {
                error:
                    MetadataFetchErrorKind::NextRowError(NextRowError::NextPageError(
                        NextPageError::RequestFailure(RequestError::LastAttemptError(
                            RequestAttemptError::DbError(DbError::Invalid, _),
                        )),
                    )),
                ..
            }) => Ok(HashMap::new()),
            result => result,
        }
    }
}

fn strategy_from_string_map(
    mut strategy_map: HashMap<String, String>,
) -> Result<Strategy, KeyspaceStrategyError> {
    let strategy_name: String = strategy_map
        .remove("class")
        .ok_or(KeyspaceStrategyError::MissingClassForStrategyDefinition)?;

    let strategy: Strategy = match strategy_name.as_str() {
        "org.apache.cassandra.locator.SimpleStrategy" | "SimpleStrategy" => {
            let rep_factor_str: String = strategy_map
                .remove("replication_factor")
                .ok_or(KeyspaceStrategyError::MissingReplicationFactorForSimpleStrategy)?;

            let replication_factor: usize = usize::from_str(&rep_factor_str)
                .map_err(KeyspaceStrategyError::ReplicationFactorParseError)?;

            Strategy::SimpleStrategy { replication_factor }
        }
        "org.apache.cassandra.locator.NetworkTopologyStrategy" | "NetworkTopologyStrategy" => {
            let mut datacenter_repfactors: HashMap<String, usize> =
                HashMap::with_capacity(strategy_map.len());

            for (key, value) in strategy_map.drain() {
                let rep_factor: usize = usize::from_str(&value).map_err(|_| {
                    // Unexpected NTS option.
                    // We only expect 'class' (which is resolved above)
                    // and replication factors per dc.
                    KeyspaceStrategyError::UnexpectedNetworkTopologyStrategyOption {
                        key: key.clone(),
                        value,
                    }
                })?;

                datacenter_repfactors.insert(key, rep_factor);
            }

            Strategy::NetworkTopologyStrategy {
                datacenter_repfactors,
            }
        }
        "org.apache.cassandra.locator.LocalStrategy" | "LocalStrategy" => Strategy::LocalStrategy,
        _ => Strategy::Other {
            name: strategy_name,
            data: strategy_map,
        },
    };

    Ok(strategy)
}

#[cfg(test)]
mod tests {
    use crate::test_utils::setup_tracing;

    use super::*;

    #[test]
    fn test_cql_type_parsing() {
        setup_tracing();
        let test_cases = [
            ("bigint", PreColumnType::Native(NativeType::BigInt)),
            (
                "list<int>",
                PreColumnType::Collection {
                    frozen: false,
                    typ: PreCollectionType::List(Box::new(PreColumnType::Native(NativeType::Int))),
                },
            ),
            (
                "set<ascii>",
                PreColumnType::Collection {
                    frozen: false,
                    typ: PreCollectionType::Set(Box::new(PreColumnType::Native(NativeType::Ascii))),
                },
            ),
            (
                "map<blob, boolean>",
                PreColumnType::Collection {
                    frozen: false,
                    typ: PreCollectionType::Map(
                        Box::new(PreColumnType::Native(NativeType::Blob)),
                        Box::new(PreColumnType::Native(NativeType::Boolean)),
                    ),
                },
            ),
            (
                "frozen<map<text, text>>",
                PreColumnType::Collection {
                    frozen: true,
                    typ: PreCollectionType::Map(
                        Box::new(PreColumnType::Native(NativeType::Text)),
                        Box::new(PreColumnType::Native(NativeType::Text)),
                    ),
                },
            ),
            (
                "tuple<tinyint, smallint, int, bigint, varint>",
                PreColumnType::Tuple(vec![
                    PreColumnType::Native(NativeType::TinyInt),
                    PreColumnType::Native(NativeType::SmallInt),
                    PreColumnType::Native(NativeType::Int),
                    PreColumnType::Native(NativeType::BigInt),
                    PreColumnType::Native(NativeType::Varint),
                ]),
            ),
            (
                "vector<int, 5>",
                PreColumnType::Vector {
                    typ: Box::new(PreColumnType::Native(NativeType::Int)),
                    dimensions: 5,
                },
            ),
            (
                "vector<text, 1234>",
                PreColumnType::Vector {
                    typ: Box::new(PreColumnType::Native(NativeType::Text)),
                    dimensions: 1234,
                },
            ),
            (
                "com.scylladb.types.AwesomeType",
                PreColumnType::UserDefinedType {
                    frozen: false,
                    name: "com.scylladb.types.AwesomeType".to_string(),
                },
            ),
            (
                "frozen<ks.my_udt>",
                PreColumnType::UserDefinedType {
                    frozen: true,
                    name: "ks.my_udt".to_string(),
                },
            ),
            (
                "map<text, frozen<map<text, text>>>",
                PreColumnType::Collection {
                    frozen: false,
                    typ: PreCollectionType::Map(
                        Box::new(PreColumnType::Native(NativeType::Text)),
                        Box::new(PreColumnType::Collection {
                            frozen: true,
                            typ: PreCollectionType::Map(
                                Box::new(PreColumnType::Native(NativeType::Text)),
                                Box::new(PreColumnType::Native(NativeType::Text)),
                            ),
                        }),
                    ),
                },
            ),
            (
                "map<\
                    frozen<list<int>>, \
                    set<\
                        list<\
                            tuple<\
                                list<list<text>>, \
                                map<text, map<ks.my_type, blob>>, \
                                frozen<set<set<int>>>\
                            >\
                        >\
                    >\
                >",
                // map<...>
                PreColumnType::Collection {
                    frozen: false,
                    typ: PreCollectionType::Map(
                        Box::new(PreColumnType::Collection {
                            // frozen<list<int>>
                            frozen: true,
                            typ: PreCollectionType::List(Box::new(PreColumnType::Native(
                                NativeType::Int,
                            ))),
                        }),
                        Box::new(PreColumnType::Collection {
                            // set<...>
                            frozen: false,
                            typ: PreCollectionType::Set(Box::new(PreColumnType::Collection {
                                // list<tuple<...>>
                                frozen: false,
                                typ: PreCollectionType::List(Box::new(PreColumnType::Tuple(vec![
                                    PreColumnType::Collection {
                                        // list<list<text>>
                                        frozen: false,
                                        typ: PreCollectionType::List(Box::new(
                                            PreColumnType::Collection {
                                                frozen: false,
                                                typ: PreCollectionType::List(Box::new(
                                                    PreColumnType::Native(NativeType::Text),
                                                )),
                                            },
                                        )),
                                    },
                                    PreColumnType::Collection {
                                        // map<text, map<ks.my_type, blob>>
                                        frozen: false,
                                        typ: PreCollectionType::Map(
                                            Box::new(PreColumnType::Native(NativeType::Text)),
                                            Box::new(PreColumnType::Collection {
                                                frozen: false,
                                                typ: PreCollectionType::Map(
                                                    Box::new(PreColumnType::UserDefinedType {
                                                        frozen: false,
                                                        name: "ks.my_type".to_string(),
                                                    }),
                                                    Box::new(PreColumnType::Native(
                                                        NativeType::Blob,
                                                    )),
                                                ),
                                            }),
                                        ),
                                    },
                                    PreColumnType::Collection {
                                        // frozen<set<set<int>>>
                                        frozen: true,
                                        typ: PreCollectionType::Set(Box::new(
                                            PreColumnType::Collection {
                                                frozen: false,
                                                typ: PreCollectionType::Set(Box::new(
                                                    PreColumnType::Native(NativeType::Int),
                                                )),
                                            },
                                        )),
                                    },
                                ]))),
                            })),
                        }),
                    ),
                },
            ),
        ];

        for (s, expected) in test_cases {
            let parsed = map_string_to_cql_type(s).unwrap();
            assert_eq!(parsed, expected);
        }
    }
}
