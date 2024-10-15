use crate::frame::response::event::Event;
use crate::routing::Token;
use crate::statement::query::Query;
use crate::transport::connection::{Connection, ConnectionConfig};
use crate::transport::connection_pool::{NodeConnectionPool, PoolConfig, PoolSize};
use crate::transport::errors::{DbError, NewSessionError, QueryError};
use crate::transport::host_filter::HostFilter;
use crate::transport::node::resolve_contact_points;
use crate::utils::parse::{ParseErrorCause, ParseResult, ParserState};

use futures::future::{self, FutureExt};
use futures::stream::{self, StreamExt, TryStreamExt};
use futures::Stream;
use rand::seq::SliceRandom;
use rand::{thread_rng, Rng};
use scylla_cql::frame::response::result::Row;
use scylla_macros::FromRow;
use std::borrow::BorrowMut;
use std::cell::Cell;
use std::collections::HashMap;
use std::fmt;
use std::fmt::Formatter;
use std::net::{IpAddr, SocketAddr};
use std::num::NonZeroUsize;
use std::str::FromStr;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{broadcast, mpsc};
use tracing::{debug, error, trace, warn};
use uuid::Uuid;

use super::errors::{
    KeyspaceStrategyError, KeyspacesMetadataError, MetadataError, PeersMetadataError,
    ProtocolError, TablesMetadataError, UdtMetadataError, ViewsMetadataError,
};
use super::node::{KnownNode, NodeAddr, ResolvedContactPoint};

/// Allows to read current metadata from the cluster
pub(crate) struct MetadataReader {
    connection_config: ConnectionConfig,
    keepalive_interval: Option<Duration>,

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
    control_connection_repair_requester: broadcast::Sender<()>,
}

/// Describes all metadata retrieved from the cluster
pub(crate) struct Metadata {
    pub(crate) peers: Vec<Peer>,
    pub(crate) keyspaces: HashMap<String, Keyspace>,
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
#[non_exhaustive] // <- so that we can add more fields in a backwards-compatible way
#[derive(Clone, Debug)]
pub enum UntranslatedEndpoint {
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
#[non_exhaustive] // <- so that we can add more fields in a backwards-compatible way
#[derive(Clone, Debug)]
pub struct PeerEndpoint {
    pub host_id: Uuid,
    pub address: NodeAddr,
    pub datacenter: Option<String>,
    pub rack: Option<String>,
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

/// Data used to issue connections to a node that is possibly subject to address translation.
///
/// Built from `PeerEndpoint` if its `NodeAddr` variant implies address translation possibility.
#[non_exhaustive] // <- so that we can add more fields in a backwards-compatible way
pub struct UntranslatedPeer {
    pub host_id: Uuid,
    pub untranslated_address: SocketAddr,
    pub datacenter: Option<String>,
    pub rack: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Keyspace {
    pub strategy: Strategy,
    /// Empty HashMap may as well mean that the client disabled schema fetching in SessionConfig
    pub tables: HashMap<String, Table>,
    /// Empty HashMap may as well mean that the client disabled schema fetching in SessionConfig
    pub views: HashMap<String, MaterializedView>,
    /// Empty HashMap may as well mean that the client disabled schema fetching in SessionConfig
    pub user_defined_types: HashMap<String, Arc<UserDefinedType>>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Table {
    pub columns: HashMap<String, Column>,
    pub partition_key: Vec<String>,
    pub clustering_key: Vec<String>,
    pub partitioner: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MaterializedView {
    pub view_metadata: Table,
    pub base_table_name: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Column {
    pub type_: CqlType,
    pub kind: ColumnKind,
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum PreCqlType {
    Native(NativeType),
    Collection {
        frozen: bool,
        type_: PreCollectionType,
    },
    Tuple(Vec<PreCqlType>),
    Vector {
        type_: Box<PreCqlType>,
        /// matches the datatype used by the java driver:
        /// <https://github.com/apache/cassandra-java-driver/blob/85bb4065098b887d2dda26eb14423ce4fc687045/core/src/main/java/com/datastax/oss/driver/api/core/type/DataTypes.java#L77>
        dimensions: i32,
    },
    UserDefinedType {
        frozen: bool,
        name: String,
    },
}

impl PreCqlType {
    pub(crate) fn into_cql_type(
        self,
        keyspace_name: &String,
        udts: &HashMap<String, HashMap<String, Arc<UserDefinedType>>>,
    ) -> CqlType {
        match self {
            PreCqlType::Native(n) => CqlType::Native(n),
            PreCqlType::Collection { frozen, type_ } => CqlType::Collection {
                frozen,
                type_: type_.into_collection_type(keyspace_name, udts),
            },
            PreCqlType::Tuple(t) => CqlType::Tuple(
                t.into_iter()
                    .map(|t| t.into_cql_type(keyspace_name, udts))
                    .collect(),
            ),
            PreCqlType::Vector { type_, dimensions } => CqlType::Vector {
                type_: Box::new(type_.into_cql_type(keyspace_name, udts)),
                dimensions,
            },
            PreCqlType::UserDefinedType { frozen, name } => {
                let definition = match udts
                    .get(keyspace_name)
                    .and_then(|per_keyspace_udts| per_keyspace_udts.get(&name))
                {
                    Some(def) => Ok(def.clone()),
                    None => Err(MissingUserDefinedType {
                        name,
                        keyspace: keyspace_name.clone(),
                    }),
                };
                CqlType::UserDefinedType { frozen, definition }
            }
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CqlType {
    Native(NativeType),
    Collection {
        frozen: bool,
        type_: CollectionType,
    },
    Tuple(Vec<CqlType>),
    Vector {
        type_: Box<CqlType>,
        /// matches the datatype used by the java driver:
        /// <https://github.com/apache/cassandra-java-driver/blob/85bb4065098b887d2dda26eb14423ce4fc687045/core/src/main/java/com/datastax/oss/driver/api/core/type/DataTypes.java#L77>
        dimensions: i32,
    },
    UserDefinedType {
        frozen: bool,
        // Using Arc here in order not to have many copies of the same definition
        definition: Result<Arc<UserDefinedType>, MissingUserDefinedType>,
    },
}

/// Definition of a user-defined type
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct UserDefinedType {
    pub name: String,
    pub keyspace: String,
    pub field_types: Vec<(String, CqlType)>,
}

/// Represents a user defined type whose definition is missing from the metadata.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MissingUserDefinedType {
    pub name: String,
    pub keyspace: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum NativeType {
    Ascii,
    Boolean,
    Blob,
    Counter,
    Date,
    Decimal,
    Double,
    Duration,
    Float,
    Int,
    BigInt,
    Text,
    Timestamp,
    Inet,
    SmallInt,
    TinyInt,
    Time,
    Timeuuid,
    Uuid,
    Varint,
}

/// [NativeType] parse error
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct NativeTypeFromStrError;

impl std::str::FromStr for NativeType {
    type Err = NativeTypeFromStrError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "ascii" => Ok(Self::Ascii),
            "boolean" => Ok(Self::Boolean),
            "blob" => Ok(Self::Blob),
            "counter" => Ok(Self::Counter),
            "date" => Ok(Self::Date),
            "decimal" => Ok(Self::Decimal),
            "double" => Ok(Self::Double),
            "duration" => Ok(Self::Duration),
            "float" => Ok(Self::Float),
            "int" => Ok(Self::Int),
            "bigint" => Ok(Self::BigInt),
            "text" => Ok(Self::Text),
            "timestamp" => Ok(Self::Timestamp),
            "inet" => Ok(Self::Inet),
            "smallint" => Ok(Self::SmallInt),
            "tinyint" => Ok(Self::TinyInt),
            "time" => Ok(Self::Time),
            "timeuuid" => Ok(Self::Timeuuid),
            "uuid" => Ok(Self::Uuid),
            "varint" => Ok(Self::Varint),
            _ => Err(NativeTypeFromStrError),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum PreCollectionType {
    List(Box<PreCqlType>),
    Map(Box<PreCqlType>, Box<PreCqlType>),
    Set(Box<PreCqlType>),
}

impl PreCollectionType {
    pub(crate) fn into_collection_type(
        self,
        keyspace_name: &String,
        udts: &HashMap<String, HashMap<String, Arc<UserDefinedType>>>,
    ) -> CollectionType {
        match self {
            PreCollectionType::List(t) => {
                CollectionType::List(Box::new(t.into_cql_type(keyspace_name, udts)))
            }
            PreCollectionType::Map(tk, tv) => CollectionType::Map(
                Box::new(tk.into_cql_type(keyspace_name, udts)),
                Box::new(tv.into_cql_type(keyspace_name, udts)),
            ),
            PreCollectionType::Set(t) => {
                CollectionType::Set(Box::new(t.into_cql_type(keyspace_name, udts)))
            }
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CollectionType {
    List(Box<CqlType>),
    Map(Box<CqlType>, Box<CqlType>),
    Set(Box<CqlType>),
}

#[derive(Clone, Debug, PartialEq, Eq)]
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
    type_: String,
    position: usize,
    reason: String,
}

impl fmt::Display for InvalidCqlType {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl From<InvalidCqlType> for QueryError {
    fn from(e: InvalidCqlType) -> Self {
        ProtocolError::InvalidCqlType {
            typ: e.type_,
            position: e.position,
            reason: e.reason,
        }
        .into()
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
        initial_known_nodes: Vec<KnownNode>,
        control_connection_repair_requester: broadcast::Sender<()>,
        mut connection_config: ConnectionConfig,
        keepalive_interval: Option<Duration>,
        server_event_sender: mpsc::Sender<Event>,
        keyspaces_to_fetch: Vec<String>,
        fetch_schema: bool,
        host_filter: &Option<Arc<dyn HostFilter>>,
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
                .choose(&mut thread_rng())
                .expect("Tried to initialize MetadataReader with empty initial_known_nodes list!")
                .clone(),
        );

        // setting event_sender field in connection config will cause control connection to
        // - send REGISTER message to receive server events
        // - send received events via server_event_sender
        connection_config.event_sender = Some(server_event_sender);

        let control_connection = Self::make_control_connection_pool(
            control_connection_endpoint.clone(),
            connection_config.clone(),
            keepalive_interval,
            control_connection_repair_requester.clone(),
        );

        Ok(MetadataReader {
            control_connection_endpoint,
            control_connection,
            keepalive_interval,
            connection_config,
            known_peers: initial_peers
                .into_iter()
                .map(UntranslatedEndpoint::ContactPoint)
                .collect(),
            keyspaces_to_fetch,
            fetch_schema,
            host_filter: host_filter.clone(),
            initial_known_nodes,
            control_connection_repair_requester,
        })
    }

    /// Fetches current metadata from the cluster
    pub(crate) async fn read_metadata(&mut self, initial: bool) -> Result<Metadata, QueryError> {
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
        self.known_peers.shuffle(&mut thread_rng());
        debug!(
            "Known peers: {}",
            self.known_peers
                .iter()
                .map(|endpoint| format!("{:?}", endpoint))
                .collect::<Vec<String>>()
                .join(", ")
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
        prev_err: QueryError,
    ) -> Result<Metadata, QueryError> {
        let mut result = Err(prev_err);
        for peer in nodes {
            let err = match result {
                Ok(_) => break,
                Err(err) => err,
            };

            warn!(
                control_connection_address = self
                    .control_connection_endpoint
                    .address()
                    .to_string()
                    .as_str(),
                error = %err,
                "Failed to fetch metadata using current control connection"
            );

            self.control_connection_endpoint = peer.clone();
            self.control_connection = Self::make_control_connection_pool(
                self.control_connection_endpoint.clone(),
                self.connection_config.clone(),
                self.keepalive_interval,
                self.control_connection_repair_requester.clone(),
            );

            debug!(
                "Retrying to establish the control connection on {}",
                self.control_connection_endpoint.address()
            );
            result = self.fetch_metadata(initial).await;
        }
        result
    }

    async fn fetch_metadata(&self, initial: bool) -> Result<Metadata, QueryError> {
        // TODO: Timeouts?
        self.control_connection.wait_until_initialized().await;
        let conn = &self.control_connection.random_connection()?;

        let res = query_metadata(
            conn,
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
                node_ips = ?metadata
                    .peers
                    .iter()
                    .map(|peer| peer.address)
                    .collect::<Vec<_>>(),
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
                    filtered_node_ips = ?metadata
                        .peers
                        .iter()
                        .filter(|peer| self.host_filter.as_ref().map_or(true, |p| p.accept(peer)))
                        .map(|peer| peer.address)
                        .collect::<Vec<_>>(),
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
                        .choose(&mut thread_rng())
                        .expect("known_peers is empty - should be impossible")
                        .clone();

                    self.control_connection = Self::make_control_connection_pool(
                        self.control_connection_endpoint.clone(),
                        self.connection_config.clone(),
                        self.keepalive_interval,
                        self.control_connection_repair_requester.clone(),
                    );
                }
            }
        }
    }

    fn make_control_connection_pool(
        endpoint: UntranslatedEndpoint,
        connection_config: ConnectionConfig,
        keepalive_interval: Option<Duration>,
        refresh_requester: broadcast::Sender<()>,
    ) -> NodeConnectionPool {
        let pool_config = PoolConfig {
            connection_config,
            keepalive_interval,

            // We want to have only one connection to receive events from
            pool_size: PoolSize::PerHost(NonZeroUsize::new(1).unwrap()),

            // The shard-aware port won't be used with PerHost pool size anyway,
            // so explicitly disable it here
            can_use_shard_aware_port: false,
        };

        NodeConnectionPool::new(endpoint, pool_config, None, refresh_requester)
    }
}

async fn query_metadata(
    conn: &Arc<Connection>,
    connect_port: u16,
    keyspace_to_fetch: &[String],
    fetch_schema: bool,
) -> Result<Metadata, QueryError> {
    let peers_query = query_peers(conn, connect_port);
    let keyspaces_query = query_keyspaces(conn, keyspace_to_fetch, fetch_schema);

    let (peers, keyspaces) = tokio::try_join!(peers_query, keyspaces_query)?;

    // There must be at least one peer
    if peers.is_empty() {
        return Err(MetadataError::Peers(PeersMetadataError::EmptyPeers).into());
    }

    // At least one peer has to have some tokens
    if peers.iter().all(|peer| peer.tokens.is_empty()) {
        return Err(MetadataError::Peers(PeersMetadataError::EmptyTokenLists).into());
    }

    Ok(Metadata { peers, keyspaces })
}

#[derive(FromRow)]
#[scylla_crate = "scylla_cql"]
struct NodeInfoRow {
    host_id: Option<Uuid>,
    untranslated_ip_addr: IpAddr,
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

async fn query_peers(conn: &Arc<Connection>, connect_port: u16) -> Result<Vec<Peer>, QueryError> {
    let mut peers_query =
        Query::new("select host_id, rpc_address, data_center, rack, tokens from system.peers");
    peers_query.set_page_size(METADATA_QUERY_PAGE_SIZE);
    let peers_query_stream = conn
        .clone()
        .query_iter(peers_query)
        .into_stream()
        .try_flatten()
        .and_then(|row_result| future::ok((NodeInfoSource::Peer, row_result)));

    let mut local_query =
        Query::new("select host_id, rpc_address, data_center, rack, tokens from system.local");
    local_query.set_page_size(METADATA_QUERY_PAGE_SIZE);
    let local_query_stream = conn
        .clone()
        .query_iter(local_query)
        .into_stream()
        .try_flatten()
        .and_then(|row_result| future::ok((NodeInfoSource::Local, row_result)));

    let untranslated_rows = stream::select(peers_query_stream, local_query_stream);

    let local_ip: IpAddr = conn.get_connect_address().ip();
    let local_address = SocketAddr::new(local_ip, connect_port);

    let translated_peers_futures = untranslated_rows.map(|row_result| async {
        let (source, raw_row) = row_result?;
        match raw_row.into_typed() {
            Ok(row) => create_peer_from_row(source, row, local_address).await,
            Err(err) => {
                warn!(
                    "system.peers or system.local has an invalid row, skipping it: {}",
                    err
                );
                Ok(None)
            }
        }
    });

    let peers = translated_peers_futures
        .buffer_unordered(256)
        .try_collect::<Vec<_>>()
        .await?;
    Ok(peers.into_iter().flatten().collect())
}

async fn create_peer_from_row(
    source: NodeInfoSource,
    row: NodeInfoRow,
    local_address: SocketAddr,
) -> Result<Option<Peer>, QueryError> {
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
            return Ok(None);
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
            vec![Token::new(rand::thread_rng().gen::<i64>())]
        }
    };

    Ok(Some(Peer {
        host_id,
        address: node_addr,
        tokens,
        datacenter,
        rack,
    }))
}

fn query_filter_keyspace_name<'a>(
    conn: &Arc<Connection>,
    query_str: &'a str,
    keyspaces_to_fetch: &'a [String],
) -> impl Stream<Item = Result<Row, QueryError>> + 'a {
    let conn = conn.clone();

    let fut = async move {
        if keyspaces_to_fetch.is_empty() {
            let mut query = Query::new(query_str);
            query.set_page_size(METADATA_QUERY_PAGE_SIZE);

            conn.query_iter(query).await
        } else {
            let keyspaces = &[keyspaces_to_fetch] as &[&[String]];
            let query_str = format!("{query_str} where keyspace_name in ?");

            let mut query = Query::new(query_str);
            query.set_page_size(METADATA_QUERY_PAGE_SIZE);

            let prepared = conn.prepare(&query).await?;
            let serialized_values = prepared.serialize_values(&keyspaces)?;
            conn.execute_iter(prepared, serialized_values).await
        }
    };
    fut.into_stream().try_flatten()
}

async fn query_keyspaces(
    conn: &Arc<Connection>,
    keyspaces_to_fetch: &[String],
    fetch_schema: bool,
) -> Result<HashMap<String, Keyspace>, QueryError> {
    let rows = query_filter_keyspace_name(
        conn,
        "select keyspace_name, replication from system_schema.keyspaces",
        keyspaces_to_fetch,
    );

    let (mut all_tables, mut all_views, mut all_user_defined_types) = if fetch_schema {
        let udts = query_user_defined_types(conn, keyspaces_to_fetch).await?;
        (
            query_tables(conn, keyspaces_to_fetch, &udts).await?,
            query_views(conn, keyspaces_to_fetch, &udts).await?,
            udts,
        )
    } else {
        (HashMap::new(), HashMap::new(), HashMap::new())
    };

    rows.map(|row_result| {
        let row = row_result?;
        let (keyspace_name, strategy_map) = row.into_typed::<(String, _)>().map_err(|err| {
            MetadataError::Keyspaces(KeyspacesMetadataError::SchemaKeyspacesInvalidColumnType(
                err,
            ))
        })?;

        let strategy: Strategy = strategy_from_string_map(strategy_map).map_err(|error| {
            MetadataError::Keyspaces(KeyspacesMetadataError::Strategy {
                keyspace: keyspace_name.clone(),
                error,
            })
        })?;
        let tables = all_tables.remove(&keyspace_name).unwrap_or_default();
        let views = all_views.remove(&keyspace_name).unwrap_or_default();
        let user_defined_types = all_user_defined_types
            .remove(&keyspace_name)
            .unwrap_or_default();

        let keyspace = Keyspace {
            strategy,
            tables,
            views,
            user_defined_types,
        };

        Ok((keyspace_name, keyspace))
    })
    .try_collect()
    .await
}

#[derive(FromRow, Debug)]
#[scylla_crate = "crate"]
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
    field_types: Vec<PreCqlType>,
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

async fn query_user_defined_types(
    conn: &Arc<Connection>,
    keyspaces_to_fetch: &[String],
) -> Result<HashMap<String, HashMap<String, Arc<UserDefinedType>>>, QueryError> {
    let rows = query_filter_keyspace_name(
        conn,
        "select keyspace_name, type_name, field_names, field_types from system_schema.types",
        keyspaces_to_fetch,
    );

    let mut udt_rows: Vec<UdtRowWithParsedFieldTypes> = rows
        .map(|row_result| {
            let row = row_result?;
            let udt_row = row
                .into_typed::<UdtRow>()
                .map_err(|err| {
                    MetadataError::Udts(UdtMetadataError::SchemaTypesInvalidColumnType(err))
                })?
                .try_into()?;

            Ok::<_, QueryError>(udt_row)
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
    for udt_row in udt_rows {
        let UdtRowWithParsedFieldTypes {
            keyspace_name,
            type_name,
            field_names,
            field_types,
        } = udt_row;

        let mut fields = Vec::with_capacity(field_names.len());

        for (field_name, field_type) in field_names.into_iter().zip(field_types.into_iter()) {
            let cql_type = field_type.into_cql_type(&keyspace_name, &udts);
            fields.push((field_name, cql_type));
        }

        let udt = Arc::new(UserDefinedType {
            name: type_name.clone(),
            keyspace: keyspace_name.clone(),
            field_types: fields,
        });

        udts.entry(keyspace_name)
            .or_insert_with(HashMap::new)
            .insert(type_name, udt);
    }

    Ok(udts)
}

fn topo_sort_udts(udts: &mut Vec<UdtRowWithParsedFieldTypes>) -> Result<(), QueryError> {
    fn do_with_referenced_udts(what: &mut impl FnMut(&str), pre_cql_type: &PreCqlType) {
        match pre_cql_type {
            PreCqlType::Native(_) => (),
            PreCqlType::Collection { type_, .. } => match type_ {
                PreCollectionType::List(t) | PreCollectionType::Set(t) => {
                    do_with_referenced_udts(what, t)
                }
                PreCollectionType::Map(t1, t2) => {
                    do_with_referenced_udts(what, t1);
                    do_with_referenced_udts(what, t2);
                }
            },
            PreCqlType::Tuple(types) => types
                .iter()
                .for_each(|type_| do_with_referenced_udts(what, type_)),
            PreCqlType::Vector { type_, .. } => do_with_referenced_udts(what, type_),
            PreCqlType::UserDefinedType { name, .. } => what(name),
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
        return Err(MetadataError::Udts(UdtMetadataError::CircularTypeDependency).into());
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

async fn query_tables(
    conn: &Arc<Connection>,
    keyspaces_to_fetch: &[String],
    udts: &HashMap<String, HashMap<String, Arc<UserDefinedType>>>,
) -> Result<HashMap<String, HashMap<String, Table>>, QueryError> {
    let rows = query_filter_keyspace_name(
        conn,
        "SELECT keyspace_name, table_name FROM system_schema.tables",
        keyspaces_to_fetch,
    );
    let mut result = HashMap::new();
    let mut tables = query_tables_schema(conn, keyspaces_to_fetch, udts).await?;

    rows.map(|row_result| {
        let row = row_result?;
        let (keyspace_name, table_name) = row.into_typed().map_err(|err| {
            MetadataError::Tables(TablesMetadataError::SchemaTablesInvalidColumnType(err))
        })?;

        let keyspace_and_table_name = (keyspace_name, table_name);

        let table = tables.remove(&keyspace_and_table_name).unwrap_or(Table {
            columns: HashMap::new(),
            partition_key: vec![],
            clustering_key: vec![],
            partitioner: None,
        });

        result
            .entry(keyspace_and_table_name.0)
            .or_insert_with(HashMap::new)
            .insert(keyspace_and_table_name.1, table);

        Ok::<_, QueryError>(())
    })
    .try_for_each(|_| future::ok(()))
    .await?;

    Ok(result)
}

async fn query_views(
    conn: &Arc<Connection>,
    keyspaces_to_fetch: &[String],
    udts: &HashMap<String, HashMap<String, Arc<UserDefinedType>>>,
) -> Result<HashMap<String, HashMap<String, MaterializedView>>, QueryError> {
    let rows = query_filter_keyspace_name(
        conn,
        "SELECT keyspace_name, view_name, base_table_name FROM system_schema.views",
        keyspaces_to_fetch,
    );

    let mut result = HashMap::new();
    let mut tables = query_tables_schema(conn, keyspaces_to_fetch, udts).await?;

    rows.map(|row_result| {
        let row = row_result?;
        let (keyspace_name, view_name, base_table_name) = row.into_typed().map_err(|err| {
            MetadataError::Views(ViewsMetadataError::SchemaViewsInvalidColumnType(err))
        })?;

        let keyspace_and_view_name = (keyspace_name, view_name);

        let table = tables.remove(&keyspace_and_view_name).unwrap_or(Table {
            columns: HashMap::new(),
            partition_key: vec![],
            clustering_key: vec![],
            partitioner: None,
        });
        let materialized_view = MaterializedView {
            view_metadata: table,
            base_table_name,
        };

        result
            .entry(keyspace_and_view_name.0)
            .or_insert_with(HashMap::new)
            .insert(keyspace_and_view_name.1, materialized_view);

        Ok::<_, QueryError>(())
    })
    .try_for_each(|_| future::ok(()))
    .await?;

    Ok(result)
}

async fn query_tables_schema(
    conn: &Arc<Connection>,
    keyspaces_to_fetch: &[String],
    udts: &HashMap<String, HashMap<String, Arc<UserDefinedType>>>,
) -> Result<HashMap<(String, String), Table>, QueryError> {
    // Upon migration from thrift to CQL, Cassandra internally creates a surrogate column "value" of
    // type EmptyType for dense tables. This resolves into this CQL type name.
    // This column shouldn't be exposed to the user but is currently exposed in system tables.
    const THRIFT_EMPTY_TYPE: &str = "empty";

    let rows = query_filter_keyspace_name(conn,
        "select keyspace_name, table_name, column_name, kind, position, type from system_schema.columns", keyspaces_to_fetch
    );

    let mut tables_schema = HashMap::new();

    rows.map(|row_result| {
        let row = row_result?;
        let (keyspace_name, table_name, column_name, kind, position, type_): (
            String,
            String,
            String,
            String,
            i32,
            String,
        ) = row.into_typed().map_err(|err| {
            MetadataError::Tables(TablesMetadataError::SchemaColumnsInvalidColumnType(err))
        })?;

        if type_ == THRIFT_EMPTY_TYPE {
            return Ok::<_, QueryError>(());
        }

        let pre_cql_type = map_string_to_cql_type(&type_)?;
        let cql_type = pre_cql_type.into_cql_type(&keyspace_name, udts);

        let kind = ColumnKind::from_str(&kind).map_err(|_| {
            MetadataError::Tables(TablesMetadataError::UnknownColumnKind {
                keyspace_name: keyspace_name.clone(),
                table_name: table_name.clone(),
                column_name: column_name.clone(),
                column_kind: kind,
            })
        })?;

        let entry = tables_schema.entry((keyspace_name, table_name)).or_insert((
            HashMap::new(), // columns
            HashMap::new(), // partition key
            HashMap::new(), // clustering key
        ));

        if kind == ColumnKind::PartitionKey || kind == ColumnKind::Clustering {
            let key_map = if kind == ColumnKind::PartitionKey {
                entry.1.borrow_mut()
            } else {
                entry.2.borrow_mut()
            };
            key_map.insert(position, column_name.clone());
        }

        entry.0.insert(
            column_name,
            Column {
                type_: cql_type,
                kind,
            },
        );

        Ok::<_, QueryError>(())
    })
    .try_for_each(|_| future::ok(()))
    .await?;

    let mut all_partitioners = query_table_partitioners(conn).await?;
    let mut result = HashMap::new();

    for ((keyspace_name, table_name), (columns, partition_key_columns, clustering_key_columns)) in
        tables_schema
    {
        let mut partition_key = vec!["".to_string(); partition_key_columns.len()];
        for (position, column_name) in partition_key_columns {
            partition_key[position as usize] = column_name;
        }

        let mut clustering_key = vec!["".to_string(); clustering_key_columns.len()];
        for (position, column_name) in clustering_key_columns {
            clustering_key[position as usize] = column_name;
        }

        let keyspace_and_table_name = (keyspace_name, table_name);

        let partitioner = all_partitioners
            .remove(&keyspace_and_table_name)
            .unwrap_or_default();

        result.insert(
            keyspace_and_table_name,
            Table {
                columns,
                partition_key,
                clustering_key,
                partitioner,
            },
        );
    }

    Ok(result)
}

fn map_string_to_cql_type(type_: &str) -> Result<PreCqlType, InvalidCqlType> {
    match parse_cql_type(ParserState::new(type_)) {
        Err(err) => Err(InvalidCqlType {
            type_: type_.to_string(),
            position: err.calculate_position(type_).unwrap_or(0),
            reason: err.get_cause().to_string(),
        }),
        Ok((_, p)) if !p.is_at_eof() => Err(InvalidCqlType {
            type_: type_.to_string(),
            position: p.calculate_position(type_).unwrap_or(0),
            reason: "leftover characters".to_string(),
        }),
        Ok((typ, _)) => Ok(typ),
    }
}

fn parse_cql_type(p: ParserState<'_>) -> ParseResult<(PreCqlType, ParserState<'_>)> {
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

        let typ = PreCqlType::Collection {
            frozen: false,
            type_: PreCollectionType::Map(Box::new(key), Box::new(value)),
        };

        Ok((typ, p))
    } else if let Ok(p) = p.accept("list<") {
        let (inner_type, p) = parse_cql_type(p)?;
        let p = p.accept(">")?;

        let typ = PreCqlType::Collection {
            frozen: false,
            type_: PreCollectionType::List(Box::new(inner_type)),
        };

        Ok((typ, p))
    } else if let Ok(p) = p.accept("set<") {
        let (inner_type, p) = parse_cql_type(p)?;
        let p = p.accept(">")?;

        let typ = PreCqlType::Collection {
            frozen: false,
            type_: PreCollectionType::Set(Box::new(inner_type)),
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

        Ok((PreCqlType::Tuple(types), p))
    } else if let Ok(p) = p.accept("vector<") {
        let (inner_type, p) = parse_cql_type(p)?;

        let p = p.skip_white();
        let p = p.accept(",")?;
        let p = p.skip_white();
        let (size, p) = p.parse_i32()?;
        let p = p.skip_white();
        let p = p.accept(">")?;

        let typ = PreCqlType::Vector {
            type_: Box::new(inner_type),
            dimensions: size,
        };

        Ok((typ, p))
    } else if let Ok((typ, p)) = parse_native_type(p) {
        Ok((PreCqlType::Native(typ), p))
    } else if let Ok((name, p)) = parse_user_defined_type(p) {
        let typ = PreCqlType::UserDefinedType {
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
    let typ = NativeType::from_str(tok)
        .map_err(|_| p.error(ParseErrorCause::Other("invalid native type")))?;
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

fn freeze_type(type_: PreCqlType) -> PreCqlType {
    match type_ {
        PreCqlType::Collection { type_, .. } => PreCqlType::Collection {
            frozen: true,
            type_,
        },
        PreCqlType::UserDefinedType { name, .. } => {
            PreCqlType::UserDefinedType { frozen: true, name }
        }
        other => other,
    }
}

async fn query_table_partitioners(
    conn: &Arc<Connection>,
) -> Result<HashMap<(String, String), Option<String>>, QueryError> {
    let mut partitioner_query = Query::new(
        "select keyspace_name, table_name, partitioner from system_schema.scylla_tables",
    );
    partitioner_query.set_page_size(METADATA_QUERY_PAGE_SIZE);

    let rows = conn
        .clone()
        .query_iter(partitioner_query)
        .into_stream()
        .try_flatten();

    let result = rows
        .map(|row_result| {
            let (keyspace_name, table_name, partitioner) =
                row_result?.into_typed().map_err(|err| {
                    MetadataError::Tables(TablesMetadataError::SchemaTablesInvalidColumnType(err))
                })?;
            Ok::<_, QueryError>(((keyspace_name, table_name), partitioner))
        })
        .try_collect::<HashMap<_, _>>()
        .await;

    match result {
        // FIXME: This match catches all database errors with this error code despite the fact
        // that we are only interested in the ones resulting from non-existent table
        // system_schema.scylla_tables.
        // For more information please refer to https://github.com/scylladb/scylla-rust-driver/pull/349#discussion_r762050262
        Err(QueryError::DbError(DbError::Invalid, _)) => Ok(HashMap::new()),
        result => result,
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
            ("bigint", PreCqlType::Native(NativeType::BigInt)),
            (
                "list<int>",
                PreCqlType::Collection {
                    frozen: false,
                    type_: PreCollectionType::List(Box::new(PreCqlType::Native(NativeType::Int))),
                },
            ),
            (
                "set<ascii>",
                PreCqlType::Collection {
                    frozen: false,
                    type_: PreCollectionType::Set(Box::new(PreCqlType::Native(NativeType::Ascii))),
                },
            ),
            (
                "map<blob, boolean>",
                PreCqlType::Collection {
                    frozen: false,
                    type_: PreCollectionType::Map(
                        Box::new(PreCqlType::Native(NativeType::Blob)),
                        Box::new(PreCqlType::Native(NativeType::Boolean)),
                    ),
                },
            ),
            (
                "frozen<map<text, text>>",
                PreCqlType::Collection {
                    frozen: true,
                    type_: PreCollectionType::Map(
                        Box::new(PreCqlType::Native(NativeType::Text)),
                        Box::new(PreCqlType::Native(NativeType::Text)),
                    ),
                },
            ),
            (
                "tuple<tinyint, smallint, int, bigint, varint>",
                PreCqlType::Tuple(vec![
                    PreCqlType::Native(NativeType::TinyInt),
                    PreCqlType::Native(NativeType::SmallInt),
                    PreCqlType::Native(NativeType::Int),
                    PreCqlType::Native(NativeType::BigInt),
                    PreCqlType::Native(NativeType::Varint),
                ]),
            ),
            (
                "vector<int, 5>",
                PreCqlType::Vector {
                    type_: Box::new(PreCqlType::Native(NativeType::Int)),
                    dimensions: 5,
                },
            ),
            (
                "vector<text, 1234>",
                PreCqlType::Vector {
                    type_: Box::new(PreCqlType::Native(NativeType::Text)),
                    dimensions: 1234,
                },
            ),
            (
                "com.scylladb.types.AwesomeType",
                PreCqlType::UserDefinedType {
                    frozen: false,
                    name: "com.scylladb.types.AwesomeType".to_string(),
                },
            ),
            (
                "frozen<ks.my_udt>",
                PreCqlType::UserDefinedType {
                    frozen: true,
                    name: "ks.my_udt".to_string(),
                },
            ),
            (
                "map<text, frozen<map<text, text>>>",
                PreCqlType::Collection {
                    frozen: false,
                    type_: PreCollectionType::Map(
                        Box::new(PreCqlType::Native(NativeType::Text)),
                        Box::new(PreCqlType::Collection {
                            frozen: true,
                            type_: PreCollectionType::Map(
                                Box::new(PreCqlType::Native(NativeType::Text)),
                                Box::new(PreCqlType::Native(NativeType::Text)),
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
                PreCqlType::Collection {
                    frozen: false,
                    type_: PreCollectionType::Map(
                        Box::new(PreCqlType::Collection {
                            // frozen<list<int>>
                            frozen: true,
                            type_: PreCollectionType::List(Box::new(PreCqlType::Native(
                                NativeType::Int,
                            ))),
                        }),
                        Box::new(PreCqlType::Collection {
                            // set<...>
                            frozen: false,
                            type_: PreCollectionType::Set(Box::new(PreCqlType::Collection {
                                // list<tuple<...>>
                                frozen: false,
                                type_: PreCollectionType::List(Box::new(PreCqlType::Tuple(vec![
                                    PreCqlType::Collection {
                                        // list<list<text>>
                                        frozen: false,
                                        type_: PreCollectionType::List(Box::new(
                                            PreCqlType::Collection {
                                                frozen: false,
                                                type_: PreCollectionType::List(Box::new(
                                                    PreCqlType::Native(NativeType::Text),
                                                )),
                                            },
                                        )),
                                    },
                                    PreCqlType::Collection {
                                        // map<text, map<ks.my_type, blob>>
                                        frozen: false,
                                        type_: PreCollectionType::Map(
                                            Box::new(PreCqlType::Native(NativeType::Text)),
                                            Box::new(PreCqlType::Collection {
                                                frozen: false,
                                                type_: PreCollectionType::Map(
                                                    Box::new(PreCqlType::UserDefinedType {
                                                        frozen: false,
                                                        name: "ks.my_type".to_string(),
                                                    }),
                                                    Box::new(PreCqlType::Native(NativeType::Blob)),
                                                ),
                                            }),
                                        ),
                                    },
                                    PreCqlType::Collection {
                                        // frozen<set<set<int>>>
                                        frozen: true,
                                        type_: PreCollectionType::Set(Box::new(
                                            PreCqlType::Collection {
                                                frozen: false,
                                                type_: PreCollectionType::Set(Box::new(
                                                    PreCqlType::Native(NativeType::Int),
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
